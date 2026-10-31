"""Register the approved subscription comparison profile, never a business default."""

import hashlib
import json
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_bounded_budget import bounded_stage_config, bounded_stage_config_hash
from nexus.agents.codex_capacity import capacity_model_matches
from nexus.persistence.models.agents import AgentModelConfigRecord

API_PROFILE_VERSION = "responses-comparison-terra-v11-v1"


def api_profile() -> dict[str, str | int | bool]:
    profile = bounded_stage_config()
    profile.update(
        version=API_PROFILE_VERSION,
        source_sdk_config_hash=bounded_stage_config_hash(),
        transport="responses-http-v1",
        endpoint="https://api.openai.com/v1/responses",
        reasoning_effort="low",
        stream=True,
        store=False,
        schema_mode="json_schema_strict",
        authorization="nexus_existing_siwc_grant",
        business_consumption_allowed=False,
    )
    return profile


def api_profile_hash() -> str:
    return hashlib.sha256(json.dumps(api_profile(), sort_keys=True).encode()).hexdigest()


def is_comparison_profile(model: AgentModelConfigRecord) -> bool:
    from nexus.agents.platform_profile import platform_profile_hash
    from nexus.agents.platform_usage import PLATFORM_PRICE_VERSION

    return model.version in (API_PROFILE_VERSION, PLATFORM_PRICE_VERSION) or model.config_hash in (
        api_profile_hash(),
        platform_profile_hash(),
    )


def _fields() -> dict[str, object]:
    profile = api_profile()
    fields: dict[str, object] = {
        key: profile[key]
        for key in (
            "model_id",
            "version",
            "max_input_tokens",
            "max_output_tokens",
            "max_tool_calls",
        )
    }
    fields["config_hash"] = api_profile_hash()
    for key in ("input_usd_per_million", "output_usd_per_million"):
        fields[key] = Decimal(str(profile[key]))
    for key in ("pricing_available_at", "pricing_expires_at"):
        fields[key] = datetime.fromisoformat(str(profile[key]))
    return fields


async def read_api_profile(session: AsyncSession) -> AgentModelConfigRecord | None:
    fields = _fields()
    row = await session.scalar(
        select(AgentModelConfigRecord)
        .where(
            AgentModelConfigRecord.model_id == fields["model_id"],
            AgentModelConfigRecord.version == API_PROFILE_VERSION,
        )
        .execution_options(populate_existing=True)
    )
    if row is not None and any(getattr(row, key) != value for key, value in fields.items()):
        raise ValueError("COMPARISON_PROFILE_EXISTING_INVALID")
    return row


async def register_api_profile(
    session: AsyncSession, *, sdk_model_config_id: UUID
) -> AgentModelConfigRecord:
    """Caller owns commit; preserve the canonical SDK price window and all budgets.

    This records existing subscription bookkeeping, not new API billing authority
    or proof of provider eligibility. A verified grant is still required by the runner.
    Concurrent registration reads the same immutable profile; conflicts never overwrite.
    """
    source = await session.get(AgentModelConfigRecord, sdk_model_config_id)
    now = await session.scalar(text("SELECT clock_timestamp()"))
    if (
        source is None
        or not isinstance(now, datetime)
        or not capacity_model_matches(source, bounded_stage_budget=True)
        or source.created_at > now
        or not source.pricing_available_at <= now < source.pricing_expires_at
    ):
        raise ValueError("COMPARISON_PROFILE_SOURCE_INVALID")
    await session.execute(
        insert(AgentModelConfigRecord)
        .values(**_fields())
        .on_conflict_do_nothing(index_elements=["model_id", "version"])
    )
    row = await read_api_profile(session)
    if row is None:
        raise ValueError("COMPARISON_PROFILE_EXISTING_INVALID")
    return row
