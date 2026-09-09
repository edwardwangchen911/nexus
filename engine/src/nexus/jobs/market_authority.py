"""Frozen read-only Fosun market authority carried by provider jobs."""

from __future__ import annotations

import re
from collections.abc import Sequence
from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.time import require_utc
from nexus.quality.source_policy import load_fosun_market_admission

_HASH = re.compile(r"^[0-9a-f]{64}$")


class FrozenFosunMarketAuthority(BaseModel):
    """Immutable capability/status identity safe to embed in a durable Job."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    source: Literal["fosun"] = "fosun"
    operation: Literal[
        "fosun_get_quotes", "fosun_get_bars", "fosun_subscribe_quotes"
    ]
    market_scope: Literal["US"] = "US"
    delivery_mode: Literal["unknown", "real_time", "delayed"]
    capability_id: UUID
    capability_schema_version: Literal["1"]
    capability_created_at: datetime
    capability_expires_at: datetime
    status_id: UUID
    status_version: int
    status_created_at: datetime
    status_mode_changed_at: datetime
    capability_invalidated_at: datetime | None
    contract_hash: str

    @field_validator(
        "capability_created_at",
        "capability_expires_at",
        "status_created_at",
        "status_mode_changed_at",
        "capability_invalidated_at",
    )
    @classmethod
    def _utc(cls, value: datetime | None) -> datetime | None:
        return None if value is None else require_utc(value)

    @field_validator("status_version")
    @classmethod
    def _positive_version(cls, value: int) -> int:
        if isinstance(value, bool) or value <= 0:
            raise ValueError("provider status version is invalid")
        return value

    @field_validator("contract_hash")
    @classmethod
    def _canonical_hash(cls, value: str) -> str:
        if _HASH.fullmatch(value) is None:
            raise ValueError("provider contract hash is invalid")
        return value


class ProviderMarketBinding(BaseModel):
    """The five immutable columns attached to one normalized Fosun fact set."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    provider_capability_id: UUID
    provider_status_id: UUID
    provider_status_version: int
    provider_contract_hash: str
    provider_authorized_at: datetime

    @field_validator("provider_status_version")
    @classmethod
    def _positive_version(cls, value: int) -> int:
        if isinstance(value, bool) or value <= 0:
            raise ValueError("provider status version is invalid")
        return value

    @field_validator("provider_contract_hash")
    @classmethod
    def _canonical_hash(cls, value: str) -> str:
        if _HASH.fullmatch(value) is None:
            raise ValueError("provider contract hash is invalid")
        return value

    @field_validator("provider_authorized_at")
    @classmethod
    def _authorized_utc(cls, value: datetime) -> datetime:
        return require_utc(value)


async def load_frozen_fosun_market_authority(
    session: AsyncSession,
    *,
    check_time: datetime,
    operation: Literal[
        "fosun_get_quotes", "fosun_get_bars", "fosun_subscribe_quotes"
    ],
    delivery_mode: Literal["unknown", "real_time", "delayed"] | None = None,
) -> FrozenFosunMarketAuthority | None:
    """Load one exact current authority at the caller's actual wall-clock time."""
    checked_at = require_utc(check_time)
    admission = await load_fosun_market_admission(
        session,
        decision_time=checked_at,
        operation=operation,
        market_scope="US",
    )
    if admission is None or (
        delivery_mode is not None and admission.delivery_mode != delivery_mode
    ):
        return None
    return FrozenFosunMarketAuthority(
        operation=admission.operation,
        delivery_mode=admission.delivery_mode,
        capability_id=admission.capability_id,
        capability_schema_version=admission.capability_schema_version,
        capability_created_at=admission.capability_created_at,
        capability_expires_at=admission.expires_at,
        status_id=admission.status_id,
        status_version=admission.status_version,
        status_created_at=admission.status_created_at,
        status_mode_changed_at=admission.status_mode_changed_at,
        capability_invalidated_at=admission.capability_invalidated_at,
        contract_hash=admission.contract_hash,
    )


async def acquire_fosun_market_authority_locks(
    session: AsyncSession,
    authorities: Sequence[FrozenFosunMarketAuthority],
) -> None:
    """Serialize frozen reads with capability-scope and status mutations."""
    if not authorities:
        raise ValueError("at least one Fosun market authority is required")
    for operation in sorted({authority.operation for authority in authorities}):
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
            {"key": (f"provider-capability-scope:v1:fosun:READ_ONLY:{operation}:US")},
        )
    for status_id in sorted({str(authority.status_id) for authority in authorities}):
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
            {"key": f"fosun-integration-status:{status_id}"},
        )


def binding_at(
    authority: FrozenFosunMarketAuthority,
    *,
    authorized_at: datetime,
) -> ProviderMarketBinding:
    """Create the database binding only after a matching post-call revalidation."""
    checked_at = require_utc(authorized_at)
    if not authority.capability_created_at <= checked_at < authority.capability_expires_at:
        raise ValueError("provider authority is not valid at binding time")
    return ProviderMarketBinding(
        provider_capability_id=authority.capability_id,
        provider_status_id=authority.status_id,
        provider_status_version=authority.status_version,
        provider_contract_hash=authority.contract_hash,
        provider_authorized_at=checked_at,
    )
