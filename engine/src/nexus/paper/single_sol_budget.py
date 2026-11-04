"""Protect remaining natural obligations in the native UTC-day allocation transaction."""

from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import RowMapping, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.budget import BudgetExhausted
from nexus.agents.persistence import _lock_budget_day, _shared_daily_budget_used
from nexus.agents.platform_profile import sol_business_model_matches
from nexus.paper.agent_cycle_authority import load_agent_paper_service_calendar_authority
from nexus.paper.candidate_batch import validate_agent_paper_candidate_batch
from nexus.paper.portfolio_policy import PORTFOLIO_ALLOCATION_V1
from nexus.paper.research_request import database_now
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentModelConfigRecord,
    AgentPortfolioSynthesisRunRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.runtime import IdempotencyRecord
from nexus.settings import EngineSettings


async def has_guard(session: AsyncSession, day: date) -> bool:
    # The advisory lock cannot refresh an older repeatable-read snapshot. Check
    # isolation before believing that the scope is absent.
    schema, isolation = (
        await session.execute(
            text(
                "SELECT to_regclass('public.single_sol_budget_scopes'), "
                "current_setting('transaction_isolation')"
            )
        )
    ).one()
    if isolation != "read committed":
        raise ValueError("SINGLE_SOL_BUDGET_STALE_SNAPSHOT")
    if schema is None:
        return False
    return bool(
        await session.scalar(
            text("SELECT 1 FROM single_sol_budget_scopes WHERE budget_day=:day"), {"day": day}
        )
    )


async def require_no_single_sol_guard(session: AsyncSession, day: date) -> None:
    if await has_guard(session, day):
        raise ValueError("SINGLE_SOL_ECONOMIC_SCOPE_CONFLICT")


async def freeze_single_sol_budget(
    session: AsyncSession,
    settings: EngineSettings,
    *,
    run_id: UUID,
    instrument_id: UUID,
    calendar_authority_id: UUID,
) -> None:
    """Install in the same transaction as the source run and its one native reserve."""
    from nexus.agents.research_usage import require_research_accounting_clear
    from nexus.paper.economic_budget_hold import held_pair_budget
    from nexus.paper.formal_budget_protection import latest_binding

    account = settings.agent_paper_account_id
    if account is None:
        raise ValueError("SINGLE_SOL_BUDGET_SCOPE_INVALID")
    from nexus.paper.target_service import try_lock_portfolio_account

    if not await try_lock_portfolio_account(session, account):
        raise ValueError("SINGLE_SOL_ACCOUNT_BUSY")
    day = (await database_now(session)).date()
    await _lock_budget_day(session, day.isoformat())
    now = await database_now(session)
    slot = datetime.combine(
        day,
        PORTFOLIO_ALLOCATION_V1.decision_local_time,
        ZoneInfo(PORTFOLIO_ALLOCATION_V1.decision_timezone),
    ).astimezone(UTC)
    if (
        now.date() != day
        or now >= slot
        or await session.scalar(text("SHOW transaction_isolation")) != "read committed"
    ):
        raise ValueError("SINGLE_SOL_BUDGET_WINDOW_CLOSED")
    if await session.scalar(
        select(IdempotencyRecord.id).where(
            IdempotencyRecord.actor_id == f"local-agent-paper-schwab:{account}",
            IdempotencyRecord.route == "agent-paper-schwab-trigger-v2",
            IdempotencyRecord.idempotency_key == slot.isoformat(),
        )
    ) or await session.scalar(
        select(AgentPaperCycleRecord.id).where(
            AgentPaperCycleRecord.account_id == account,
            AgentPaperCycleRecord.decision_slot_at == slot,
        )
    ):
        raise ValueError("SINGLE_SOL_NATURAL_SLOT_CLAIMED")
    if (
        settings.formal_budget_protection_enabled
        or await latest_binding(session, business_date=day) is not None
        or await held_pair_budget(session, business_date=day)
    ):
        raise ValueError("SINGLE_SOL_ECONOMIC_SCOPE_CONFLICT")
    await require_research_accounting_clear(session)
    batch, _ = await validate_agent_paper_candidate_batch(
        session, account_id=account, batch_id=settings.agent_paper_candidate_batch_id, as_of=now
    )
    model = await session.get(AgentModelConfigRecord, settings.agent_paper_formal_model_config_id)
    source = await session.get(AgentResearchRunRecord, run_id)
    # An earlier committed reservation cannot be adopted as a new owner's source.
    reservation = await session.scalar(
        select(AgentDailyBudgetReservationRecord).where(
            AgentDailyBudgetReservationRecord.run_id == run_id,
            text("pg_xact_status(xmin::text::xid8)='in progress'"),
        )
    )
    if (
        source is None
        or model is None
        or reservation is None
        or not sol_business_model_matches(model)
        or source.actor_id != "local-single-sol-research-v1"
        or source.decision_time.date() != day
        or source.decision_time > now
        or source.reserved_cost_usd != model.worst_case_cost_usd
        or source.instrument_id != instrument_id
        or instrument_id not in batch.instrument_ids
        or source.model_config_id != model.id
        or source.model_hash != model.config_hash
        or source.status != "prepared"
        or source.pricing_version != model.version
        or reservation.pricing_version != model.version
        or reservation.business_date != day
        or reservation.state != "reserved"
        or reservation.settled_usd is not None
        or reservation.reserved_usd != model.worst_case_cost_usd
        or not model.pricing_available_at <= now < slot < model.pricing_expires_at
    ):
        raise ValueError("SINGLE_SOL_BUDGET_SCOPE_INVALID")
    calendar = await load_agent_paper_service_calendar_authority(
        session, now=now, expected_calendar_authority_id=calendar_authority_id
    )
    if (
        calendar.session != day
        or not calendar.market_open_at <= slot < calendar.market_close_at
        or model.pricing_expires_at <= calendar.market_close_at
        or await has_guard(session, day)
    ):
        raise ValueError("SINGLE_SOL_BUDGET_SCOPE_INVALID")
    maximum = model.worst_case_cost_usd
    covered_increment = await session.scalar(
        text("""SELECT i.run_id FROM research_monitor_increments i
            WHERE i.account_id=:account AND i.session_date=:day AND i.research_as_of<=:now
            AND single_sol_increment_valid(:day,:account,:batch,:model,:calendar,:maximum,i.run_id)
            ORDER BY i.research_as_of,i.run_id LIMIT 1"""),
        dict(
            day=day,
            account=account,
            batch=batch.batch_id,
            model=model.id,
            calendar=calendar_authority_id,
            maximum=maximum,
            now=now,
        ),
    )
    used = await _shared_daily_budget_used(session, business_date=day)
    remaining = len(batch.instrument_ids) + 2 - int(covered_increment is not None)
    if used + remaining * maximum > settings.agent_daily_budget_usd:
        raise BudgetExhausted("SINGLE_SOL_NATURAL_BUDGET_PROTECTED")
    checked = await database_now(session)
    if checked.date() != day or checked >= slot:
        raise ValueError("SINGLE_SOL_BUDGET_WINDOW_CLOSED")
    await session.execute(
        text("""INSERT INTO single_sol_budget_scopes
        (budget_day,source_run_id,account_id,batch_id,model_id,selected_id,calendar_id,
         slot,created_at,daily_limit,maximum)
        VALUES (:day,:run,:account,:batch,:model, :instrument,:calendar,:slot,
                :now,:limit,:maximum)"""),
        dict(
            day=day,
            run=run_id,
            account=account,
            batch=batch.batch_id,
            model=model.id,
            instrument=instrument_id,
            calendar=calendar_authority_id,
            slot=slot,
            now=checked,
            limit=settings.agent_daily_budget_usd,
            maximum=maximum,
        ),
    )

    if covered_increment is not None:
        await _convert(session, day, "increment", covered_increment)


async def require_single_sol_capacity(
    session: AsyncSession,
    *,
    kind: str,
    run_id: UUID,
    now: datetime,
    daily_limit_usd: Decimal,
    amount: Decimal,
) -> bool:
    """Called under the native day lock. Commit validates every provisional conversion."""
    day = now.date()
    guarded = await has_guard(session, day)
    observed = (await database_now(session)).date()
    if observed != day and (guarded or await has_guard(session, observed)):
        raise ValueError("SINGLE_SOL_BUDGET_WINDOW_CLOSED")
    if not guarded:
        return False
    guard = (
        (
            await session.execute(
                text("""SELECT g.*, b.candidate_instrument_ids AS members
       FROM single_sol_budget_scopes g JOIN agent_paper_candidate_batches b
       ON b.id=g.batch_id AND b.account_id=g.account_id WHERE g.budget_day=:day"""),
                {"day": day},
            )
        )
        .mappings()
        .one()
    )
    if daily_limit_usd != guard["daily_limit"]:
        raise ValueError("SINGLE_SOL_BUDGET_SCOPE_INVALID")
    credits = set(
        await session.scalars(
            text("SELECT task FROM single_sol_budget_credits WHERE budget_day=:day"), {"day": day}
        )
    )
    tasks = {"research:" + str(i) for i in guard["members"]} | {"synthesis", "increment"}
    if not credits <= tasks:
        raise ValueError("SINGLE_SOL_BUDGET_SCOPE_INVALID")
    remaining = tasks - credits
    task = (
        await _original_obligation(session, guard, kind, run_id, now)
        if amount == guard["maximum"]
        else None
    )
    credit = task is not None and task in remaining
    used = await _shared_daily_budget_used(session, business_date=day)
    if used + amount + (len(remaining) - int(credit)) * guard["maximum"] > daily_limit_usd:
        raise BudgetExhausted("SINGLE_SOL_NATURAL_BUDGET_PROTECTED")
    if (await database_now(session)).date() != day:
        raise ValueError("SINGLE_SOL_BUDGET_WINDOW_CLOSED")
    if credit and task is not None:
        await _convert(session, day, task, run_id)
    return True


async def _convert(session: AsyncSession, day: date, task: str, run_id: UUID) -> None:
    await session.execute(
        text(
            "INSERT INTO single_sol_budget_credits(budget_day,task,run_id) VALUES(:day,:task,:run)"
        ),
        dict(day=day, task=task, run=run_id),
    )


async def _original_obligation(
    session: AsyncSession, guard: RowMapping, kind: str, run_id: UUID, now: datetime
) -> str | None:
    if kind not in {"research", "synthesis"}:
        return None
    if kind == "research":
        run = await session.get(AgentResearchRunRecord, run_id)
        if (
            run is None
            or run.model_config_id != guard["model_id"]
            or run.instrument_id not in guard["members"]
        ):
            return None
        if run.actor_id == f"monitor:{guard['account_id']}":
            # A still-valid predecessor may predate this day's natural research.
            match = await session.scalar(
                text("""SELECT 1 FROM research_monitor_triggers t
                    JOIN research_monitor_checks k ON k.id=t.check_id
                    JOIN research_monitor_plans p ON p.id=k.plan_id
                    JOIN data_snapshots src ON src.id=:snapshot
                    AND src.payload#>>'{monitor_context,parent_run_id}'=p.source_run_id::text
                    AND src.payload#>>'{monitor_context,parent_output_id}'=p.source_output_id::text
                    AND src.payload#>>'{monitor_context,authority,calendar_authority_id}'=:calendar
                    WHERE t.account_id=:account AND t.instrument_id=:instrument
                    AND k.result='true' AND k.checked_at>=p.effective_at
                    AND k.checked_at<p.valid_until AND k.checked_at<=:now
                    AND (:key='monitor:'||t.id::text OR :key='monitor-batch:'||t.batch_id::text)
                    LIMIT 1"""),
                dict(
                    account=guard["account_id"],
                    instrument=run.instrument_id,
                    snapshot=run.data_snapshot_id,
                    calendar=str(guard["calendar_id"]),
                    now=now,
                    key=run.idempotency_key,
                ),
            )
            return "increment" if match else None
    if now < guard["slot"]:
        return None
    cycle = await session.scalar(
        text("""SELECT c.id FROM agent_paper_cycles c JOIN idempotency_records claim
                ON claim.actor_id='local-agent-paper-schwab:'||c.account_id::text
                AND claim.route='agent-paper-schwab-trigger-v2'
                AND claim.schema_version='agent_paper_schwab_trigger_v2'
                AND claim.idempotency_key=:key AND claim.formal_budget_binding_id IS NULL
                WHERE c.account_id=:account AND c.decision_slot_at=:slot
                AND c.candidate_batch_id=:batch AND c.formal_budget_binding_id IS NULL
                AND NOT EXISTS(SELECT FROM manual_paper_request_events e WHERE e.cycle_id=c.id)"""),
        dict(
            account=guard["account_id"],
            slot=guard["slot"],
            batch=guard["batch_id"],
            key=guard["slot"].isoformat(),
        ),
    )
    if cycle is None:
        return None
    if kind == "synthesis":
        synth = await session.get(AgentPortfolioSynthesisRunRecord, run_id)
        if (
            synth is not None
            and synth.cycle_id == cycle
            and synth.model_config_id == guard["model_id"]
            and synth.candidate_batch_id == guard["batch_id"]
            and synth.frozen_input.get("calendar_authority_id") == str(guard["calendar_id"])
        ):
            return "synthesis"
    elif (
        run is not None
        and run.actor_id == "local-agent-paper-cycle"
        and run.idempotency_key == f"agent-paper:{cycle}:{run.instrument_id}"
    ):
        return "research:" + str(run.instrument_id)
    return None
