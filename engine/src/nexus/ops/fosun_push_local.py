"""Prepare one explicit read-only authority before a bounded Fosun push probe."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.canonical import canonical_json
from nexus.domain.time import require_utc
from nexus.persistence.models.data import ProviderCapability
from nexus.persistence.models.runtime import IntegrationStatus
from nexus.quality.source_policy import load_fosun_market_admission

_OPERATION = "fosun_subscribe_quotes"
_MARKET_SCOPE = "US"
_CAPABILITY_DAYS = 365


@dataclass(frozen=True)
class PreparedPushAuthority:
    """Canonical authority document and the digest bound into a v3 archive."""

    payload: bytes
    sha256: str


def _utc_text(value: datetime) -> str:
    return require_utc(value).isoformat().replace("+00:00", "Z")


async def prepare_push_authority(
    session: AsyncSession,
    *,
    prepared_at: datetime,
) -> PreparedPushAuthority:
    """Create/reuse subscription authority before opening the WebSocket."""
    checked_at = require_utc(prepared_at)
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {
            "key": (
                "provider-capability-scope:v1:fosun:READ_ONLY:"
                f"{_OPERATION}:{_MARKET_SCOPE}"
            )
        },
    )
    status_id = await session.scalar(
        select(IntegrationStatus.id).where(IntegrationStatus.provider == "fosun")
    )
    if status_id is None:
        raise ValueError("FOSUN_PUSH_AUTHORITY_REQUIRED")
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {"key": f"fosun-integration-status:{status_id}"},
    )
    status = await session.scalar(
        select(IntegrationStatus)
        .where(IntegrationStatus.id == status_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if status is None or (
        status.status != "read_only"
        or status.created_at > checked_at
        or status.mode_changed_at > checked_at
        or status.admitted_contract_hash is None
        or status.capability_invalidated_at is not None
    ):
        raise ValueError("FOSUN_PUSH_AUTHORITY_REQUIRED")

    capability = await session.scalar(
        select(ProviderCapability)
        .where(
            ProviderCapability.provider == "fosun",
            ProviderCapability.capability == "READ_ONLY",
            ProviderCapability.contract_hash == status.admitted_contract_hash,
            ProviderCapability.payload["operation"].as_string() == _OPERATION,
            ProviderCapability.payload["market_scope"].as_string() == _MARKET_SCOPE,
        )
        .order_by(ProviderCapability.created_at.desc(), ProviderCapability.id.desc())
        .limit(1)
    )
    if capability is not None and (
        capability.schema_version != "1"
        or capability.created_at > checked_at
        or capability.expires_at <= checked_at
        or capability.payload.get("delivery_mode") != "unknown"
    ):
        raise ValueError("FOSUN_PUSH_AUTHORITY_INVALID")
    if capability is None:
        capability = ProviderCapability(
            provider="fosun",
            capability="READ_ONLY",
            contract_hash=status.admitted_contract_hash,
            expires_at=checked_at + timedelta(days=_CAPABILITY_DAYS),
            schema_version="1",
            payload={
                "operation": _OPERATION,
                "market_scope": _MARKET_SCOPE,
                "delivery_mode": "unknown",
            },
            created_at=checked_at,
        )
        session.add(capability)
        await session.flush()

    admission = await load_fosun_market_admission(
        session,
        decision_time=checked_at,
        operation=_OPERATION,
        market_scope=_MARKET_SCOPE,
    )
    if admission is None or admission.capability_id != capability.id:
        raise ValueError("FOSUN_PUSH_AUTHORITY_REQUIRED")
    document = {
        "schema": "nexus_fosun_push_authority_v1",
        "provider": "fosun",
        "operation": _OPERATION,
        "delivery_mode": admission.delivery_mode,
        "capability_id": str(admission.capability_id),
        "capability_expires_at": _utc_text(admission.expires_at),
        "status_id": str(admission.status_id),
        "status_version": admission.status_version,
        "contract_hash": admission.contract_hash,
        "prepared_at": _utc_text(checked_at),
    }
    payload = canonical_json(document).encode()
    return PreparedPushAuthority(payload=payload, sha256=sha256(payload).hexdigest())
