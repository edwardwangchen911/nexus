"""Fixed, opt-in Platform model registration; never a business default or funding proof."""

import hashlib
import json
from dataclasses import asdict
from datetime import datetime
from decimal import Decimal

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.platform_usage import (
    CURRENT_PLATFORM_PRICE_VERSION,
    MEDIUM_PLATFORM_PRICE_VERSION,
    PLATFORM_PRICE_VERSION,
)
from nexus.agents.platform_wire import platform_wire_profile
from nexus.persistence.models.agents import AgentModelConfigRecord


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
