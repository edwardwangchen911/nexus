"""Non-authoritative Webhook receipts for the simulation execution boundary."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Literal, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.ids import new_uuid7
from nexus.domain.time import require_utc
from nexus.persistence.models.broker_simulation import (
    BrokerAccountBindingRecord,
    BrokerWebhookReceiptRecord,
)

_MAX_BODY_BYTES = 65_536


class WebhookContractError(ValueError):
    """Stable Webhook receipt or schema failure."""


class WebhookCandidate(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: str = Field(min_length=1, max_length=255)
    occurred_at: datetime
    provider_order_ref: str = Field(min_length=1, max_length=128)
    order_status: Literal[
        "acknowledged",
        "working",
        "partially_filled",
        "filled",
        "cancelled",
        "rejected",
        "expired",
    ]
    safe_code: Literal["WEBHOOK_CANDIDATE", "WEBHOOK_STALE_HINT"] = "WEBHOOK_CANDIDATE"
    authoritative: Literal[False] = False

    @field_validator("occurred_at")
    @classmethod
    def _occurred_at_is_utc(cls, value: datetime) -> datetime:
        return require_utc(value)


class WebhookAuthenticator(Protocol):
    async def authenticate(
        self, headers: Mapping[str, str], raw_body: bytes
    ) -> Literal["authenticated", "rejected", "not_admitted"]: ...


class WebhookEvidenceStore(Protocol):
    async def put(self, digest: str, raw_body: bytes) -> str: ...


class NotAdmittedWebhookAuthenticator:
    async def authenticate(
        self, headers: Mapping[str, str], raw_body: bytes
    ) -> Literal["not_admitted"]:
        del headers, raw_body
        return "not_admitted"


class InMemoryWebhookEvidenceStore:
    """Test-only immutable content-addressed byte store."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    async def put(self, digest: str, raw_body: bytes) -> str:
        existing = self.objects.get(digest)
        if existing is not None and existing != raw_body:
            raise WebhookContractError("WEBHOOK_EVIDENCE_CONFLICT")
        self.objects[digest] = raw_body
        return f"memory://webhook/{digest}"


class WebhookReceiptResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    receipt_id: UUID
    event_id: str
    body_hash: str
    authentication_status: Literal["authenticated", "rejected", "not_admitted"]
    safe_code: str
    duplicate: bool
    authoritative: Literal[False] = False


def parse_webhook_candidate(raw_body: bytes, *, received_at: datetime) -> WebhookCandidate:
    received_at = require_utc(received_at)
    if len(raw_body) > _MAX_BODY_BYTES:
        raise WebhookContractError("WEBHOOK_TOO_LARGE")
    try:
        decoded = json.loads(raw_body)
        if not isinstance(decoded, dict):
            raise ValueError
        candidate = WebhookCandidate.model_validate(decoded)
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError, ValidationError):
        raise WebhookContractError("WEBHOOK_VALIDATION_ERROR") from None
    if (
        candidate.occurred_at < received_at - timedelta(days=1)
        or candidate.occurred_at > received_at + timedelta(minutes=5)
    ):
        candidate = candidate.model_copy(update={"safe_code": "WEBHOOK_STALE_HINT"})
    return candidate


async def receive_webhook(
    session: AsyncSession,
    *,
    account_binding_id: UUID | None,
    raw_body: bytes,
    headers: Mapping[str, str],
    received_at: datetime,
    authenticator: WebhookAuthenticator,
    evidence_store: WebhookEvidenceStore,
) -> WebhookReceiptResult:
    """Archive exact bytes and store a sanitized hint that never finalizes broker state."""
    received_at = require_utc(received_at)
    candidate = parse_webhook_candidate(raw_body, received_at=received_at)
    authentication_status = await authenticator.authenticate(headers, raw_body)
    digest = hashlib.sha256(raw_body).hexdigest()
    await evidence_store.put(digest, raw_body)
    existing = await session.scalar(
        select(BrokerWebhookReceiptRecord)
        .where(BrokerWebhookReceiptRecord.event_id == candidate.event_id)
        .with_for_update()
    )
    if existing is not None:
        if existing.body_hash != digest:
            if account_binding_id is not None:
                binding = await session.scalar(
                    select(BrokerAccountBindingRecord)
                    .where(BrokerAccountBindingRecord.id == account_binding_id)
                    .with_for_update()
                )
                if binding is not None:
                    binding.locked = True
                    binding.lock_reason = "webhook_event_collision"
                    binding.reconciliation_status = "conflict"
                    binding.version += 1
                    await session.flush()
            raise WebhookContractError("WEBHOOK_EVENT_COLLISION")
        return WebhookReceiptResult(
            receipt_id=existing.id,
            event_id=existing.event_id,
            body_hash=existing.body_hash,
            authentication_status=existing.authentication_status,  # type: ignore[arg-type]
            safe_code=existing.safe_code,
            duplicate=True,
        )
    safe_code = (
        candidate.safe_code
        if authentication_status == "authenticated"
        else "WEBHOOK_NOT_ADMITTED"
    )
    receipt = BrokerWebhookReceiptRecord(
        id=new_uuid7(),
        account_binding_id=account_binding_id,
        event_id=candidate.event_id,
        body_hash=digest,
        body_length=len(raw_body),
        received_at=received_at,
        authentication_status=authentication_status,
        safe_code=safe_code,
    )
    session.add(receipt)
    await session.flush()
    return WebhookReceiptResult(
        receipt_id=receipt.id,
        event_id=receipt.event_id,
        body_hash=receipt.body_hash,
        authentication_status=authentication_status,
        safe_code=safe_code,
        duplicate=False,
    )
