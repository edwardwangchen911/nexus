"""Match already-funded same-session obligations only to their original native owners."""

from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.paper.portfolio_policy import PORTFOLIO_ALLOCATION_V1 as POLICY


def original_slot(day: date) -> datetime:
    return datetime.combine(
        day, POLICY.decision_local_time, ZoneInfo(POLICY.decision_timezone)
    ).astimezone(UTC)


async def funded_obligations(
    session: AsyncSession,
    *,
    now: datetime,
    account: UUID,
    batch: UUID,
    model: UUID,
    calendar: UUID,
    members: tuple[UUID, ...],
    maximum: Decimal,
) -> dict[str, UUID]:
    slot = original_slot(now.date())
    args = dict(
        account=account,
        batch=batch,
        model=model,
        calendar=calendar,
        maximum=maximum,
        day=now.date(),
        slot=slot,
        key=slot.isoformat(),
        now=now,
    )
    credits: dict[str, UUID] = {}
    increment = await session.scalar(
        text("""SELECT i.run_id FROM research_monitor_increments i
        JOIN agent_daily_budget_reservations b ON b.run_id=i.run_id
        AND b.state IN ('reserved','settled')
        WHERE i.account_id=:account AND i.session_date=:day AND i.research_as_of<=:now
        AND single_sol_increment_valid(:day,:account,:batch,:model,:calendar,:maximum,i.run_id)
        ORDER BY i.research_as_of,i.run_id LIMIT 1"""),
        args,
    )
    if increment is not None:
        credits["increment"] = increment
    if now < slot:
        return credits
    # A clock past 10:00 does not prove the original slot has finished. Its committed
    # claim, exact cycle, frozen owners, and known native settlements must all exist.
    cycle = await session.scalar(
        text("""SELECT c.id FROM agent_paper_cycles c JOIN idempotency_records t
        ON t.actor_id='local-agent-paper-schwab:'||c.account_id::text
        AND t.route='agent-paper-schwab-trigger-v2'
        AND t.schema_version='agent_paper_schwab_trigger_v2'
        AND t.idempotency_key=:key AND t.formal_budget_binding_id IS NULL
        AND t.response_payload->>'outcome'='completed'
        AND t.response_payload#>>'{result,cycle_id}'=c.id::text
        WHERE c.account_id=:account AND c.candidate_batch_id=:batch AND c.decision_slot_at=:slot
        AND c.decision_session=:day AND c.execution_locked AND c.formal_budget_binding_id IS NULL
        AND c.decision_time>=c.decision_slot_at
        AND c.decision_time<c.decision_slot_at+interval '5 minutes'
        AND NOT EXISTS(SELECT FROM manual_paper_request_events e WHERE e.cycle_id=c.id)
        AND (SELECT e.to_status FROM agent_paper_cycle_events e WHERE e.cycle_id=c.id
             ORDER BY e.sequence DESC LIMIT 1) IN ('completed','unavailable')"""),
        args,
    )
    if cycle is None:
        raise ValueError("SINGLE_SOL_NATURAL_WINDOW_PROTECTED")
    rows = (
        await session.execute(
            text("""SELECT r.instrument_id, r.id
        FROM research_stage_route rt JOIN agent_research_runs r ON r.id=rt.run_id
        JOIN agent_daily_budget_reservations b ON b.run_id=r.id
        JOIN data_snapshots s ON s.id=r.data_snapshot_id
        JOIN agent_model_configs m ON r.model_config_id=m.id
        WHERE rt.kind='formal' AND rt.scope_id=:cycle AND rt.platform_binding IS NOT NULL
        AND m.id=:model AND r.model_hash=m.config_hash AND r.pricing_version=m.version
        AND r.actor_id='local-agent-paper-cycle'
        AND r.idempotency_key='agent-paper:'||rt.scope_id::text||':'||r.instrument_id::text
        AND s.payload#>>'{agent_paper_context,cycle_id}'=rt.scope_id::text
        AND s.payload#>>'{agent_paper_context,instrument_id}'=r.instrument_id::text
        AND b.business_date=:day AND b.reserved_usd=:maximum AND b.pricing_version=m.version
        AND b.state='settled' AND b.settled_usd=r.settled_cost_usd
        AND r.status IN ('completed','abstained')"""),
            args | {"cycle": cycle},
        )
    ).all()
    synth = await session.scalar(
        text("""SELECT s.id FROM agent_portfolio_synthesis_runs s
        JOIN agent_portfolio_synthesis_budget_reservations b ON b.run_id=s.id
        JOIN agent_model_configs m ON s.model_config_id=m.id
        WHERE s.cycle_id=:cycle AND s.candidate_batch_id=:batch
        AND s.idempotency_key='portfolio-synthesis:'||s.cycle_id::text
        AND s.frozen_input->>'calendar_authority_id'=CAST(:calendar AS text)
        AND m.id=:model AND s.model_hash=m.config_hash AND s.pricing_version=m.version
        AND b.business_date=:day AND b.pricing_version=m.version AND b.reserved_usd=:maximum
        AND b.state='settled' AND b.settled_usd=s.settled_cost_usd AND s.status='completed'"""),
        args | {"cycle": cycle, "calendar": str(calendar)},
    )
    if (
        synth is None
        or len(rows) != len(members)
        or {r.instrument_id for r in rows} != set(members)
    ):
        raise ValueError("SINGLE_SOL_ORIGINAL_SETTLEMENT_UNPROVEN")
    return credits | {"research:" + str(r.instrument_id): r.id for r in rows} | {"synthesis": synth}
