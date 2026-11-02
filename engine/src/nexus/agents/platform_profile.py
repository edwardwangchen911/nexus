"""Separate historical experiments and normal Sol profile identities.

Registration or price calculation alone never grants funding/dispatch authority.
"""

import hashlib
import json
from dataclasses import asdict
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.platform_pricing import sol_price_record
from nexus.agents.platform_usage import (
    CURRENT_PLATFORM_PRICE_VERSION,
    MEDIUM_PLATFORM_PRICE_VERSION,
    PLATFORM_PRICE_VERSION,
)
from nexus.agents.platform_wire import platform_wire_profile
from nexus.persistence.models.agents import AgentModelConfigRecord

SOL_BUSINESS_CONTRACT = "platform-sol-business-v1"


def sol_business_model_fields(price_record: object) -> dict[str, object]:
    """One supported capability, with renewable immutable price data.

    This is deliberately not a comparison profile or a default selector.
    Normal owners must freeze the exact returned identity in their own prepare.
    """
    pricing = sol_price_record(price_record)
    payload = {
        "contract": SOL_BUSINESS_CONTRACT,
        "price_record": pricing.model_dump(mode="json"),
        "wire_profile": asdict(platform_wire_profile(pricing.version, price_record=pricing)),
        "max_input_tokens": 816000,
        "max_output_tokens": 8192,
        "max_tool_calls": 0,
        "business_consumption_allowed": True,
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return {
        "model_id": pricing.model_id,
        "version": SOL_BUSINESS_CONTRACT + "-" + digest,
        "config_hash": digest,
        "max_input_tokens": 816000,
        "max_output_tokens": 8192,
        "max_tool_calls": 0,
        "input_usd_per_million": Decimal(f"{max(pricing.token_picodollars[:3])}e-6"),
        "output_usd_per_million": Decimal(pricing.output_usd_per_million),
        "pricing_available_at": pricing.verified_at,
        "pricing_expires_at": pricing.expires_at,
        "platform_profile": payload,
    }


def sol_business_model_matches(model: AgentModelConfigRecord) -> bool:
    payload = getattr(model, "platform_profile", None)
    if not isinstance(payload, dict):
        return False
    try:
        fields = sol_business_model_fields(payload.get("price_record"))
    except ValueError:
        return False
    return all(getattr(model, key, None) == value for key, value in fields.items())


async def register_sol_business_profile(
    session: AsyncSession, *, price_record: object, source_evidence: bytes
) -> AgentModelConfigRecord:
    """Operator-verified source registration; no remote price/balance assertion.

    The caller commits. A lost commit acknowledgement requires readback of the
    deterministic identity, never new timestamps or an implicit re-registration.
    """
    from nexus.audit.service import append_audit_event

    pricing = sol_price_record(price_record)
    if (
        type(source_evidence) is not bytes
        or not 0 < len(source_evidence) <= 2 * 1024 * 1024
        or hashlib.sha256(source_evidence).hexdigest() != pricing.source_evidence_sha256
    ):
        raise ValueError("PLATFORM_PRICE_SOURCE_MISMATCH")
    await session.execute(text("SET LOCAL statement_timeout='5s'"))
    await session.execute(text("SET LOCAL lock_timeout='1s'"))
    now = await session.scalar(text("SELECT clock_timestamp()"))
    if not isinstance(now, datetime) or not pricing.verified_at <= now < pricing.expires_at:
        raise ValueError("PLATFORM_PRICE_WINDOW_INVALID")
    fields = sol_business_model_fields(pricing)
    inserted_id = await session.scalar(
        insert(AgentModelConfigRecord)
        .values(**fields)
        .on_conflict_do_nothing(index_elements=["model_id", "version"])
        .returning(AgentModelConfigRecord.id)
    )
    row = await session.scalar(
        select(AgentModelConfigRecord).where(
            AgentModelConfigRecord.model_id == pricing.model_id,
            AgentModelConfigRecord.version == fields["version"],
        )
    )
    if row is None or not sol_business_model_matches(row):
        raise ValueError("PLATFORM_PROFILE_EXISTING_INVALID")
    if inserted_id is not None:
        await append_audit_event(
            session,
            actor_id="local:platform-profile",
            action="platform.sol_profile_registered",
            schema_version=SOL_BUSINESS_CONTRACT,
            occurred_at=now,
            payload={
                "model_config_id": str(row.id),
                "config_hash": row.config_hash,
                "price_version": pricing.version,
                "source_evidence_sha256": pricing.source_evidence_sha256,
                "dispatch_authority": False,
            },
        )
    return row


async def read_sol_business_profile(
    session: AsyncSession, *, model_config_id: UUID
) -> AgentModelConfigRecord | None:
    """Historical inspection never extends a price window or needs a live Key."""
    row = await session.get(AgentModelConfigRecord, model_config_id)
    if row is not None and not sol_business_model_matches(row):
        raise ValueError("PLATFORM_PROFILE_EXISTING_INVALID")
    return row


def platform_profile(version: str = PLATFORM_PRICE_VERSION) -> dict[str, object]:
    # Historical callers retain v1 bytes. Only the normal new-work entry opts into v2.
    windows = {
        PLATFORM_PRICE_VERSION: ("2026-10-31", "2026-11-01"),
        CURRENT_PLATFORM_PRICE_VERSION: ("2026-11-01", "2026-11-02"),
        MEDIUM_PLATFORM_PRICE_VERSION: ("2026-11-01", "2026-11-02"),
    }
    if version not in windows:
        raise ValueError("PLATFORM_PROFILE_INVALID")
    start, end = windows[version]
    return {
        **asdict(platform_wire_profile(version)),
        "model_id": "gpt-5.6-terra",
        "version": version,
        "max_input_tokens": 816000,  # 3 stages; each independently capped at 272000.
        "max_output_tokens": 8192,
        "max_tool_calls": 0,
        "input_usd_per_million": "2.50",  # reservation bound, NOT exact settlement.
        "output_usd_per_million": "12",
        "pricing_available_at": start + "T00:00:00+00:00",
        "pricing_expires_at": end + "T00:00:00+00:00",
        "settlement": "platform-cache-classification-v1",
        "business_consumption_allowed": False,
    }


def platform_profile_hash(version: str = PLATFORM_PRICE_VERSION) -> str:
    return hashlib.sha256(
        json.dumps(platform_profile(version), sort_keys=True).encode()
    ).hexdigest()


def platform_model_fields(version: str = PLATFORM_PRICE_VERSION) -> dict[str, object]:
    p = platform_profile(version)
    fields = {
        k: p[k]
        for k in ("model_id", "version", "max_input_tokens", "max_output_tokens", "max_tool_calls")
    }
    fields["config_hash"] = platform_profile_hash(version)
    for key in ("input_usd_per_million", "output_usd_per_million"):
        fields[key] = Decimal(str(p[key]))
    for key in ("pricing_available_at", "pricing_expires_at"):
        fields[key] = datetime.fromisoformat(str(p[key]))
    return fields


def platform_model_matches(model: AgentModelConfigRecord) -> bool:
    try:
        fields = platform_model_fields(model.version)
    except ValueError:
        return False
    return all(getattr(model, k) == v for k, v in fields.items())


async def register_platform_profile(
    session: AsyncSession, *, version: str = PLATFORM_PRICE_VERSION
) -> AgentModelConfigRecord:
    """Caller commits; records price authority, never acquires send/funding rights."""
    fields = platform_model_fields(version)
    now = await session.scalar(text("SELECT clock_timestamp()"))
    start, end = fields["pricing_available_at"], fields["pricing_expires_at"]
    if not (
        isinstance(now, datetime)
        and isinstance(start, datetime)
        and isinstance(end, datetime)
        and start <= now < end
    ):
        raise ValueError("PLATFORM_PRICE_WINDOW_INVALID")
    await session.execute(
        insert(AgentModelConfigRecord)
        .values(**fields)
        .on_conflict_do_nothing(index_elements=["model_id", "version"])
    )
    row = await session.scalar(
        select(AgentModelConfigRecord).where(
            AgentModelConfigRecord.version == version,
            AgentModelConfigRecord.model_id == "gpt-5.6-terra",
        )
    )
    if row is None or not platform_model_matches(row):
        raise ValueError("PLATFORM_PROFILE_EXISTING_INVALID")
    return row


async def read_platform_profile(
    session: AsyncSession, *, version: str = PLATFORM_PRICE_VERSION
) -> AgentModelConfigRecord | None:
    platform_profile(version)
    row = await session.scalar(
        select(AgentModelConfigRecord).where(
            AgentModelConfigRecord.version == version,
            AgentModelConfigRecord.model_id == "gpt-5.6-terra",
        )
    )
    if row is not None and not platform_model_matches(row):
        raise ValueError("PLATFORM_PROFILE_EXISTING_INVALID")
    return row
