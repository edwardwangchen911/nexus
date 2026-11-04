"""Resolve a single non-trading selection from the protected Sol batch."""

from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.budget import ResearchAccountingBusy
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
from nexus.paper.manual_request import ManualRequestView
from nexus.paper.research_request import database_now
from nexus.paper.single_sol_owner import SingleSolBinding, binding_for
from nexus.paper.target_service import lock_portfolio_account
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.settings import EngineSettings


class SingleSolSelection(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    contract: Literal["single_sol_research_v1"] = "single_sol_research_v1"
    history_proofs: dict[str, str] = Field(default_factory=dict, exclude=True)
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
        history_proofs=proofs,
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


async def single_sol_authority(
    session: AsyncSession, settings: EngineSettings, *, instrument_id: UUID, funded: bool = False
) -> SingleSolBinding:
    """The same non-trading identity must still be admissible before each new effect."""
    from nexus.agents.persistence import try_lock_budget_day
    from nexus.monitoring.host import (
        BATCH_PREPARATION_HEADROOM_SECONDS,
        INCREMENT_ACTION_HEADROOM_SECONDS,
    )
    from nexus.paper.agent_cycle_authority import select_agent_paper_service_calendar_authority
    from nexus.paper.economic_budget_hold import held_pair_budget
    from nexus.paper.target_service import try_lock_portfolio_account

    now = await database_now(session)
    account = settings.agent_paper_account_id
    if account is None or not await try_lock_portfolio_account(session, account):
        raise ResearchAccountingBusy()
    if not await try_lock_budget_day(session, now.date().isoformat()):
        raise ResearchAccountingBusy()
    selection = await inspect_single_sol(session, settings, instrument_id=instrument_id)
    if await held_pair_budget(session, business_date=selection.budget_day):
        raise ValueError("SINGLE_SOL_ECONOMIC_SCOPE_CONFLICT")
    calendar = await select_agent_paper_service_calendar_authority(
        session, now=selection.checked_at
    )
    from nexus.paper.single_sol_budget import has_guard
    from nexus.paper.single_sol_obligations import funded_obligations, original_slot

    if not funded and await has_guard(session, selection.budget_day):
        raise ValueError("SINGLE_SOL_BUDGET_SCOPE_ALREADY_EXISTS")
    slot = original_slot(selection.budget_day)
    post_slot = selection.checked_at >= slot
    deadline = calendar.market_close_at if post_slot else calendar.market_open_at
    checked = await database_now(session)
    if (
        selection.budget_day != calendar.session
        or checked.date() != selection.budget_day
        or checked >= deadline
        or not funded
        and checked
        + timedelta(seconds=BATCH_PREPARATION_HEADROOM_SECONDS + INCREMENT_ACTION_HEADROOM_SECONDS)
        >= deadline
        or selection.pricing_expires_at <= calendar.market_close_at
    ):
        raise ValueError("SINGLE_SOL_NATURAL_WINDOW_PROTECTED")
    if not funded or post_slot:
        batch, _ = await validate_agent_paper_candidate_batch(
            session,
            account_id=account,
            batch_id=settings.agent_paper_candidate_batch_id,
            as_of=checked,
        )
        model_id = settings.agent_paper_formal_model_config_id
        if model_id is None:
            raise ValueError("MANUAL_CONFIGURATION_UNAVAILABLE")
        covered = await funded_obligations(
            session,
            now=checked,
            account=account,
            batch=batch.batch_id,
            model=model_id,
            calendar=calendar.calendar_authority_id,
            members=batch.instrument_ids,
            maximum=selection.single_maximum_usd,
        )
        remaining = len(batch.instrument_ids) + 2 - len(covered)
        if not funded and (
            selection.used_usd + (remaining + 1) * selection.single_maximum_usd
            > selection.daily_limit_usd
        ):
            raise ValueError("SINGLE_SOL_NATURAL_BUDGET_PROTECTED")
    checked = await database_now(session)
    required_seconds = (
        0 if funded else BATCH_PREPARATION_HEADROOM_SECONDS + INCREMENT_ACTION_HEADROOM_SECONDS
    )
    if (
        checked.date() != selection.budget_day
        or checked + timedelta(seconds=required_seconds) >= deadline
    ):
        raise ValueError("SINGLE_SOL_NATURAL_WINDOW_PROTECTED")
    return SingleSolBinding(
        preparation_proof=selection.history_proofs.get(preparation_unknown_ack.ACTION),
        scheduled_proof=selection.history_proofs.get(scheduled_unknown_acceptance.ACTION),
        instrument_id=instrument_id,
        selection_hash=selection.selection_hash,
        budget_day=selection.budget_day,
        calendar_id=calendar.calendar_authority_id,
        deadline=deadline,
    )


async def require_single_sol_first_claim_window(
    session: AsyncSession, row: ManualPaperRequest
) -> None:
    from nexus.monitoring.host import INCREMENT_ACTION_HEADROOM_SECONDS

    frozen = binding_for(row)
    now = await database_now(session)
    if (
        now.date() != frozen.budget_day
        or now + timedelta(seconds=INCREMENT_ACTION_HEADROOM_SECONDS) >= frozen.deadline
    ):
        raise ValueError("SINGLE_SOL_NATURAL_WINDOW_PROTECTED")


async def recheck_single_sol(
    session: AsyncSession,
    settings: EngineSettings,
    row: ManualPaperRequest,
    *,
    run_id: UUID | None = None,
    cleanup_deadline: datetime | None = None,
) -> None:
    from nexus.paper.manual_request import require_manual_account_available
    from nexus.paper.single_sol_budget import require_single_sol_capacity

    frozen = binding_for(row)
    current = await single_sol_authority(
        session, settings, instrument_id=frozen.instrument_id, funded=run_id is not None
    )
    if current != frozen:
        raise ValueError("SINGLE_SOL_REQUEST_BINDING_INVALID")
    await require_manual_account_available(
        session,
        account_id=row.account_id,
        now=await database_now(session),
        excluding_request_id=row.id,
        read_only=True,
    )
    from nexus.paper.manual_entry import require_configured_manual_host

    await require_configured_manual_host(session, settings, now=await database_now(session))
    if run_id is not None:
        from nexus.paper.manual_request import read_manual_request

        receipt = await read_manual_request(session, row)
        if (
            receipt.status != "active"
            or receipt.research is None
            or len(receipt.research.members) != 1
            or receipt.research.members[0].run_id != run_id
        ):
            raise ValueError("SINGLE_SOL_REQUEST_BINDING_INVALID")
        source = await session.scalar(
            text("""SELECT source_run_id FROM single_sol_budget_scopes
            WHERE budget_day=:day AND account_id=:account AND batch_id=:batch AND model_id=:model
            AND selected_id=:selected AND calendar_id=:cal"""),
            dict(
                day=frozen.budget_day,
                account=row.account_id,
                batch=row.candidate_batch_id,
                model=row.model_config_id,
                selected=frozen.instrument_id,
                cal=frozen.calendar_id,
            ),
        )
        now = await database_now(session)
        if (
            source != run_id
            or cleanup_deadline is None
            or cleanup_deadline >= frozen.deadline
            or not await require_single_sol_capacity(
                session,
                kind="dispatch",
                run_id=run_id,
                now=now,
                daily_limit_usd=settings.agent_daily_budget_usd,
                amount=Decimal(0),
            )
        ):
            raise ValueError("SINGLE_SOL_BUDGET_SCOPE_INVALID")
    if await database_now(session) >= current.deadline:
        raise ValueError("SINGLE_SOL_NATURAL_WINDOW_PROTECTED")


async def start_single_sol(
    session: AsyncSession,
    settings: EngineSettings,
    *,
    instrument_id: UUID,
    idempotency_key: str,
    expected_intent_hash: str,
) -> ManualRequestView:
    from nexus.paper.manual_request import admit_manual_request
    from nexus.paper.target_service import try_lock_portfolio_account

    account = settings.agent_paper_account_id
    if account is None:
        raise ValueError("MANUAL_CONFIGURATION_UNAVAILABLE")
    if not await try_lock_portfolio_account(session, account):
        raise ResearchAccountingBusy()
    batch = settings.agent_paper_candidate_batch_id
    model = settings.agent_paper_formal_model_config_id
    if batch is None or model is None:
        raise ValueError("MANUAL_CONFIGURATION_UNAVAILABLE")
    return await admit_manual_request(
        session,
        account_id=account,
        batch_id=batch,
        model_config_id=model,
        mode="research_only",
        contract_version=4,
        idempotency_key=idempotency_key,
        runtime_mode="responses",
        daily_limit_usd=settings.agent_daily_budget_usd,
        single_sol_settings=settings,
        selected_instrument_id=instrument_id,
        expected_intent_hash=expected_intent_hash,
    )
