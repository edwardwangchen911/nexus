"""Resolve a single non-trading selection from the protected Sol batch."""

from datetime import date, datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_formal_stage import require_formal_stage_model_profile
from nexus.agents.persistence import _lock_budget_day, _shared_daily_budget_used
from nexus.agents.platform_native import native_runtime_matches
from nexus.agents.platform_profile import sol_business_model_matches
from nexus.agents.research_usage import require_research_accounting_clear
from nexus.jobs.models import job_input_hash
from nexus.paper import preparation_unknown_ack, scheduled_unknown_acceptance
from nexus.paper.candidate_batch import validate_agent_paper_candidate_batch
from nexus.paper.formal_budget_protection import latest_binding
from nexus.paper.manual_entry import manual_host_scope
from nexus.paper.research_request import database_now
from nexus.paper.target_service import lock_portfolio_account
from nexus.settings import EngineSettings


class SingleSolSelection(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    contract: Literal["single_sol_research_v1"] = "single_sol_research_v1"
    selected_instrument_id: UUID
    selection_hash: str
    batch_member_count: int
    checked_at: datetime
    budget_day: date
    pricing_version: str
    pricing_expires_at: datetime
    single_maximum_usd: Decimal
    natural_research_maximum_usd: Decimal
    synthesis_maximum_usd: Decimal
    increment_maximum_usd: Decimal
    daily_limit_usd: Decimal
    used_usd: Decimal
    full_session_capacity_sufficient: bool
    submit_authorized: Literal[False] = False
    budget_reserved: Literal[False] = False
    execution_locked: Literal[True] = True


async def inspect_single_sol(
    session: AsyncSession, settings: EngineSettings, *, instrument_id: UUID
) -> SingleSolSelection:
    """Read selection and full-session capacity; never issue a request or a budget credit."""
    account = settings.agent_paper_account_id
    if account is None or not settings.agent_paper_candidate_batch_id:
        raise ValueError("MANUAL_CONFIGURATION_UNAVAILABLE")
    if not settings.agent_paper_manual_enabled or not settings.agent_paper_cycle_enabled:
        raise ValueError("MANUAL_ENTRY_DISABLED")
    if not native_runtime_matches(settings):
        raise ValueError("MANUAL_CONFIGURATION_UNAVAILABLE")
    await lock_portfolio_account(session, account)
    day = (await database_now(session)).date()
    await _lock_budget_day(session, day.isoformat())
    now = await database_now(session)
    if (
        now.date() != day
        or await session.scalar(text("SHOW transaction_isolation")) != "read committed"
    ):
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    if settings.formal_budget_protection_enabled or await latest_binding(
        session, business_date=day
    ):
        raise ValueError("MANUAL_BUDGET_PROTECTED")
    batch, _ = await validate_agent_paper_candidate_batch(
        session, account_id=account, batch_id=settings.agent_paper_candidate_batch_id, as_of=now
    )
    if instrument_id not in batch.instrument_ids:
        raise ValueError("SINGLE_SOL_NOT_BATCH_MEMBER")
    model = await require_formal_stage_model_profile(
        session, model_config_id=settings.agent_paper_formal_model_config_id, now=now
    )
    if not sol_business_model_matches(model) or model.max_tool_calls != 0:
        raise ValueError("MANUAL_CONFIGURATION_UNAVAILABLE")
    proofs = {}
    for action, audits, validate in (
        (
            preparation_unknown_ack.ACTION,
            preparation_unknown_ack._audits,
            preparation_unknown_ack._validate,
        ),
        (
            scheduled_unknown_acceptance.ACTION,
            scheduled_unknown_acceptance._audits,
            scheduled_unknown_acceptance._validate,
        ),
    ):
        records = await audits(session, account)
        if len(records) > 1 or any(
            row.occurred_at >= preparation_unknown_ack._FORMAL_SUCCESSOR_POLICY_AT
            for row in records
        ):
            raise ValueError("MANUAL_PREPARATION_ACK_BLOCKED")
        for row in records:
            await validate(session, row, now=now)
            proofs[action] = row.event_hash
    await require_research_accounting_clear(session)
    used = await _shared_daily_budget_used(session, business_date=day)
    maximum = model.worst_case_cost_usd
    daily = settings.agent_daily_budget_usd
    natural = maximum * len(batch.instrument_ids)
    # Each consumer uses this same configured price. An increment has no selected stock yet.
    # This preview grants no credit for an obligation that might already be covered.
    full_session = natural + maximum + maximum  # batch research, synthesis, one increment
    checked_at = await database_now(session)
    if (
        checked_at.date() != day
        or not model.pricing_available_at <= checked_at < model.pricing_expires_at
    ):
        raise ValueError("MANUAL_CONFIGURATION_UNAVAILABLE")
    digest = job_input_hash(
        {
            "contract": "single_sol_research_v1",
            "scope": manual_host_scope(settings),
            "batch": batch.batch_hash,
            "instrument": str(instrument_id),
            "model": model.config_hash,
            "pricing": model.version,
            "budget_day": day.isoformat(),
            "daily_limit": format(daily.normalize(), "f"),
            "legacy_proofs": proofs,
        }
    )
    return SingleSolSelection(
        selected_instrument_id=instrument_id,
        selection_hash=digest,
        batch_member_count=len(batch.instrument_ids),
        checked_at=checked_at,
        budget_day=day,
        pricing_version=model.version,
        pricing_expires_at=model.pricing_expires_at,
        single_maximum_usd=maximum,
        natural_research_maximum_usd=natural,
        synthesis_maximum_usd=maximum,
        increment_maximum_usd=maximum,
        daily_limit_usd=daily,
        used_usd=used,
        full_session_capacity_sufficient=used + full_session + maximum <= daily,
    )
