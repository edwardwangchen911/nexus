"""A prospective v3 scope for exact accepted history, never an unknown disposition."""

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.budget import ResearchAccountingBusy
from nexus.agents.codex_formal_stage import require_formal_stage_model_profile
from nexus.agents.persistence import try_lock_budget_day
from nexus.agents.platform_profile import sol_business_model_matches
from nexus.paper.target_service import try_lock_portfolio_account
from nexus.persistence.models.manual_paper import ManualPaperRequest

POLICY_AT = datetime(2026, 11, 6, 2, 40, tzinfo=UTC)
SCOPE = "INDEPENDENT_HISTORY_SCOPE_UNSUPPORTED"
PROOF = "INDEPENDENT_HISTORY_PROOF_INVALID"
BUDGET = "INDEPENDENT_NATURAL_BUDGET_PROTECTED"
WINDOW = "INDEPENDENT_NATURAL_WINDOW_PROTECTED"
ECONOMIC = "INDEPENDENT_ECONOMIC_SCOPE_CONFLICT"
UNAVAILABLE = "INDEPENDENT_RECHECK_UNAVAILABLE"
SAFE_CODES = frozenset(
    {
        SCOPE,
        PROOF,
        BUDGET,
        WINDOW,
        ECONOMIC,
        UNAVAILABLE,
        "INDEPENDENT_PRIORITY_INVALID",
        "INDEPENDENT_SHARED_BUDGET_EXHAUSTED",
    }
)


class AcceptedProof(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    audit_id: UUID
    event_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class HistoricalException(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    version: Literal["independent_accepted_history_v1"] = "independent_accepted_history_v1"
    preparation: AcceptedProof | None
    scheduled: AcceptedProof | None


def binding(row: ManualPaperRequest) -> HistoricalException | None:
    profile = row.research_profile
    if not isinstance(profile, dict) or "historical_exception" not in profile:
        return None
    try:
        result = HistoricalException.model_validate(profile["historical_exception"])
    except ValidationError:
        raise ValueError(SCOPE) from None
    if (
        row.contract_version != 3
        or row.mode != "research_and_monitor"
        or profile.get("runtime_mode") != "responses"
        or row.requested_at < POLICY_AT
        or result.preparation is None
        and result.scheduled is None
    ):
        raise ValueError(SCOPE)
    return result


async def _locks(session: AsyncSession, account_id: UUID, now: datetime) -> None:
    if not await try_lock_portfolio_account(session, account_id) or not await try_lock_budget_day(
        session, now.date().isoformat()
    ):
        raise ResearchAccountingBusy()
    if await session.scalar(text("SHOW transaction_isolation")) != "read committed":
        raise ValueError(PROOF)


async def capture(
    session: AsyncSession,
    *,
    account_id: UUID,
    model_config_id: UUID,
    now: datetime,
) -> HistoricalException | None:
    """Called under the normal account/day fence; does not create or accept history."""
    from nexus.paper import preparation_unknown_ack as preparation
    from nexus.paper import scheduled_unknown_acceptance as scheduled

    await _locks(session, account_id, now)
    rows = [
        await preparation._audits(session, account_id),
        await scheduled._audits(session, account_id),
    ]
    if not any(rows):
        return None
    model = await require_formal_stage_model_profile(
        session, model_config_id=model_config_id, now=now
    )
    # Existing zero-price SDK behavior is unchanged; it never receives this policy.
    if model.input_usd_per_million == 0 and model.output_usd_per_million == 0:
        return None
    if now < POLICY_AT or not sol_business_model_matches(model) or model.max_tool_calls != 0:
        raise ValueError(SCOPE)
    from nexus.paper.economic_budget_hold import held_pair_budget
    from nexus.paper.formal_budget_protection import latest_binding

    if await latest_binding(session, business_date=now.date()) or await held_pair_budget(
        session, business_date=now.date()
    ):
        raise ValueError(ECONOMIC)
    proofs: list[AcceptedProof | None] = []
    for records, validator in zip(rows, (preparation._validate, scheduled._validate), strict=True):
        if len(records) > 1 or any(
            a.occurred_at >= preparation._FORMAL_SUCCESSOR_POLICY_AT for a in records
        ):
            raise ValueError(PROOF)
        if not records:
            proofs.append(None)
            continue
        audit = records[0]
        # Normal historical writers share the account fence. Also protect the exact
        # existing claims against a late receipt while validating this send right.
        claim_ids = audit.payload.get("claim_ids", [audit.payload.get("claim_id")])
        if not isinstance(claim_ids, list) or not claim_ids:
            raise ValueError(PROOF)
        from sqlalchemy.exc import DBAPIError

        from nexus.persistence.models.runtime import IdempotencyRecord

        try:
            parsed_ids = [UUID(str(value)) for value in claim_ids]
        except (TypeError, ValueError):
            raise ValueError(PROOF) from None
        try:
            statement = select(IdempotencyRecord.id).where(IdempotencyRecord.id.in_(parsed_ids))
            if await session.scalar(text("SHOW transaction_read_only")) != "on":
                statement = statement.with_for_update(read=True, nowait=True)
            await session.execute(statement)
        except DBAPIError as error:
            if getattr(error.orig, "sqlstate", None) == "55P03":
                raise ResearchAccountingBusy() from error
            raise
        try:
            await validator(session, audit, now=now)
        except ValueError as error:
            if str(error) in {preparation.BLOCKED, scheduled.BLOCKED}:
                raise ValueError(PROOF) from error
            raise
        proofs.append(AcceptedProof(audit_id=audit.id, event_hash=audit.event_hash))
    return HistoricalException(preparation=proofs[0], scheduled=proofs[1])


async def recheck(
    session: AsyncSession,
    row: ManualPaperRequest,
    *,
    now: datetime,
    run_id: UUID | None = None,
    cleanup_deadline: datetime | None = None,
) -> None:
    if row.contract_version != 3:
        return
    from nexus.paper import preparation_unknown_ack as preparation
    from nexus.paper.manual_request import read_manual_request, require_manual_request_history
    from nexus.paper.research_request import read_research_manifest

    expected = binding(row)
    actual = await capture(
        session, account_id=row.account_id, model_config_id=row.model_config_id, now=now
    )
    if actual is None and expected is None:
        return
    if expected is None:
        raise ValueError(SCOPE)
    if actual != expected:
        raise ValueError(PROOF)
    view = await read_manual_request(session, row)
    if view.status not in {"queued", "preparing", "active"} or view.cycle_id is not None:
        raise ValueError(SCOPE)
    if run_id is not None:
        manifest = await read_research_manifest(session, row)
        if (
            view.status != "active"
            or manifest is None
            or run_id not in {m.run_id for m in manifest.members}
        ):
            raise ValueError(SCOPE)
        # A terminal unknown within this same root cannot be hidden by self exclusion.
        from nexus.persistence.models.agents import AgentResearchRunRecord

        own = (
            await session.scalars(
                select(AgentResearchRunRecord).where(
                    AgentResearchRunRecord.id.in_([m.run_id for m in manifest.members])
                )
            )
        ).all()
        if any(
            r.status in {"failed", "abstained", "completed"} and r.settled_cost_usd is None
            for r in own
        ):
            raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")
    await require_manual_request_history(
        session, account_id=row.account_id, now=now, excluding_request_id=row.id
    )
    await preparation.require_acknowledged_native_claims(
        session, account_id=row.account_id, now=now, excluding_request_id=row.id
    )
    assert row.research_profile is not None
    await require_capacity(
        session,
        account_id=row.account_id,
        batch_id=row.candidate_batch_id,
        model_id=row.model_config_id,
        daily_limit=Decimal(str(row.research_profile["daily_limit_usd"])),
        now=now,
        row=row,
        cleanup_deadline=cleanup_deadline,
    )


async def require_capacity(
    session: AsyncSession,
    *,
    account_id: UUID,
    batch_id: UUID,
    model_id: UUID,
    daily_limit: Decimal,
    now: datetime,
    row: ManualPaperRequest | None = None,
    cleanup_deadline: datetime | None = None,
    key_hash: str | None = None,
) -> dict[str, object] | None:
    """Independent reserves never pay a natural obligation, even without an older guard."""
    from nexus.agents.persistence import _shared_daily_budget_used
    from nexus.agents.research_usage import require_research_accounting_clear
    from nexus.monitoring.host import (
        BATCH_PREPARATION_HEADROOM_SECONDS,
        INCREMENT_ACTION_HEADROOM_SECONDS,
    )
    from nexus.paper.agent_cycle_authority import select_agent_paper_service_calendar_authority
    from nexus.paper.candidate_batch import validate_agent_paper_candidate_batch
    from nexus.paper.research_request import read_research_manifest
    from nexus.paper.single_sol_obligations import funded_obligations, original_slot

    await _locks(session, account_id, now)
    batch, _ = await validate_agent_paper_candidate_batch(
        session, account_id=account_id, batch_id=batch_id, as_of=now
    )
    model = await require_formal_stage_model_profile(session, model_config_id=model_id, now=now)
    if len(batch.instrument_ids) != 3:
        raise ValueError(SCOPE)
    calendar = await select_agent_paper_service_calendar_authority(session, now=now)
    deadline = execution_boundary(
        now=now,
        market_open_at=calendar.market_open_at,
        session_date=calendar.session,
        pricing_expires_at=model.pricing_expires_at,
    )
    from nexus.paper.independent_priority import EXHAUSTED, INVALID, resolve

    priority = await resolve(
        session,
        account_id=account_id,
        batch_id=batch_id,
        batch_hash=batch.batch_hash,
        model_id=model_id,
        model_hash=model.config_hash,
        daily_limit=daily_limit,
        maximum=model.worst_case_cost_usd,
        now=now,
        key_hash=key_hash,
        row=row,
    )
    if priority is not None:
        deadline = min(deadline, priority[1].expires_at)
    manifest = None if row is None else await read_research_manifest(session, row)
    if priority is not None and manifest is not None:
        from nexus.persistence.models.agents import AgentDailyBudgetReservationRecord

        holds = list(
            await session.scalars(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id.in_(
                        [m.run_id for m in manifest.members]
                    )
                )
            )
        )
        if (
            len(manifest.members) != 3
            or len(holds) != 3
            or any(
                h.business_date != priority[1].budget_day
                or h.reserved_usd != model.worst_case_cost_usd
                or h.pricing_version != model.version
                or h.state not in {"reserved", "settled"}
                or h.state == "reserved"
                and h.settled_usd is not None
                or h.state == "settled"
                and h.settled_usd is None
                for h in holds
            )
        ):
            raise ValueError(INVALID)
    headroom = BATCH_PREPARATION_HEADROOM_SECONDS + 3 * INCREMENT_ACTION_HEADROOM_SECONDS
    if (
        row is not None
        and row.requested_at.date() != now.date()
        or now >= deadline
        or manifest is None
        and now + timedelta(seconds=headroom) >= deadline
        or cleanup_deadline is not None
        and cleanup_deadline >= deadline
    ):
        raise ValueError(WINDOW)
    await require_research_accounting_clear(session)
    from nexus.paper.single_sol_budget import remaining_obligations
    from nexus.paper.single_sol_compat import budget_model_compatible

    guard = (
        (
            await session.execute(
                text("SELECT * FROM single_sol_budget_scopes WHERE budget_day=:day"),
                {"day": now.date()},
            )
        )
        .mappings()
        .one_or_none()
    )
    if guard is not None:
        if (
            guard["account_id"] != account_id
            or guard["batch_id"] != batch_id
            or guard["daily_limit"] != daily_limit
            or guard["maximum"] != model.worst_case_cost_usd
            or guard["slot"] != original_slot(now.date())
            or not await budget_model_compatible(
                session, expected=guard["model_id"], candidate=model.id, maximum=guard["maximum"]
            )
        ):
            raise ValueError(BUDGET)
    try:
        covered = await funded_obligations(
            session,
            now=now,
            account=account_id,
            batch=batch_id,
            model=model_id if guard is None else guard["model_id"],
            calendar=calendar.calendar_authority_id if guard is None else guard["calendar_id"],
            members=batch.instrument_ids,
            maximum=model.worst_case_cost_usd,
        )
    except ValueError as error:
        if str(error) == "SINGLE_SOL_NATURAL_WINDOW_PROTECTED":
            raise ValueError(WINDOW) from error
        if str(error) == "SINGLE_SOL_ORIGINAL_SETTLEMENT_UNPROVEN":
            raise ValueError(UNAVAILABLE) from error
        raise
    remaining = len(batch.instrument_ids) + 2 - len(covered)
    if guard is not None:
        obligations, _ = await remaining_obligations(
            session, day=now.date(), members=list(batch.instrument_ids), now=now
        )
        remaining = len(obligations)
    if priority is not None:
        remaining = 0
    additional = 3 if manifest is None else 0
    used = await _shared_daily_budget_used(session, business_date=now.date())
    if used + (remaining + additional) * model.worst_case_cost_usd > daily_limit:
        raise ValueError(EXHAUSTED if priority is not None else BUDGET)
    from nexus.paper.research_request import database_now

    checked = await database_now(session)
    if (
        checked.date() != now.date()
        or checked >= deadline
        or manifest is None
        and checked + timedelta(seconds=headroom) >= deadline
    ):
        raise ValueError(WINDOW)

    return None if priority is None else priority[0].model_dump(mode="json")


def execution_boundary(
    *,
    now: datetime,
    market_open_at: datetime,
    session_date: date,
    pricing_expires_at: datetime,
) -> datetime:
    """Keep the next natural boundary without inventing an after-close embargo."""
    from nexus.paper.single_sol_obligations import original_slot

    budget_end = datetime.combine(now.date() + timedelta(days=1), datetime.min.time(), UTC)
    protected = [budget_end, pricing_expires_at]
    if market_open_at > now:
        protected.append(market_open_at)
    slot = original_slot(session_date)
    if slot > now:
        protected.append(slot)
    return min(protected)
