"""Read-only formal budget demand; a snapshot never authorizes economic execution."""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from typing import Literal, Protocol, cast
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_versions import ACTIONABLE_CONFIG_VERSIONS
from nexus.agents.paper_context import read_agent_paper_research_context
from nexus.agents.portfolio_synthesis_persistence import _frozen_run
from nexus.jobs.daily_research import AGENT_VERSION_CHALLENGER_ACTOR, AGENT_VERSION_INCUMBENT_ACTOR
from nexus.paper.agent_cycle_models import (
    CandidateManifestEntry,
    CycleStatus,
    candidate_manifest_hash,
    canonical_cycle_event_hash,
    parse_candidate_manifest,
)
from nexus.paper.candidate_batch import validate_agent_paper_candidate_batch
from nexus.paper.economic_budget_hold import held_pair_budget
from nexus.paper.portfolio_policy import PORTFOLIO_ALLOCATION_V1
from nexus.persistence.models.agent_versions import (
    AgentVersionEvaluationDecisionRecord,
    AgentVersionEvaluationRecord,
)
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentModelConfigRecord,
    AgentPortfolioSynthesisBudgetReservationRecord,
    AgentPortfolioSynthesisRunRecord,
    AgentResearchRunRecord,
    PortfolioDecisionBudgetReservationRecord,
)
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.paper import (
    PAPER_DECISION_SOURCE_POLICY_ID,
    AgentPaperCycleEventRecord,
    AgentPaperCycleRecord,
)
from nexus.persistence.models.runtime import IdempotencyRecord


class Reservation(Protocol):
    state: str
    reserved_usd: Decimal
    settled_usd: Decimal | None


def reservation_usage(reservation: Reservation) -> tuple[Decimal, bool]:
    reserved, settled = reservation.reserved_usd, reservation.settled_usd
    if not reserved.is_finite() or reserved < 0:
        raise ValueError("FORMAL_BUDGET_LEDGER_INVALID")
    if reservation.state == "reserved" and settled is None:
        return reserved, True
    if (
        reservation.state == "settled"
        and settled is not None
        and settled.is_finite()
        and 0 <= settled <= reserved
    ):
        return settled, True
    if reservation.state == "released" and settled == 0:
        return Decimal(0), True
    return reserved, False


def describe_coverage(
    *,
    maximum: Decimal | None,
    now: datetime,
    slot: datetime,
    run_status: str | None,
    reservation: Reservation | None,
    no_start_confirmed: bool,
) -> tuple[str, Decimal, Decimal | None]:
    """Describe an exact association; this never grants a new budget reservation."""
    if reservation is not None:
        usage, valid = reservation_usage(reservation)
        if not valid or run_status is None:
            return "unknown", usage, None
        if reservation.state == "reserved":
            state = (
                "reserved_usage_unsettled" if run_status in {"completed", "failed"} else "reserved"
            )
            return state, usage, Decimal(0)
        if run_status not in {"completed", "failed"}:
            return "unknown", usage, None
        return (
            ("settled" if reservation.state == "settled" else "released_terminal"),
            usage,
            Decimal(0),
        )
    if run_status is not None or maximum is None or not no_start_confirmed:
        return "unknown", Decimal(0), None
    if now >= slot + timedelta(minutes=5):
        return "window_passed_no_start", Decimal(0), Decimal(0)
    return "future_not_created", Decimal(0), maximum


class TaskObservation(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    task_ref: str
    source: str
    kind: str
    slot_at: datetime
    coverage: str
    native_reservation_ref: str | None
    native_occupied_usd: Decimal
    observed_uncovered_usd: Decimal | None


class FormalBudgetInventory(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    as_of: datetime
    budget_day_utc: str
    configuration_hash: str
    source_completeness: Literal["observed_only"] = "observed_only"
    protection_status: Literal["protection_not_enforced"] = "protection_not_enforced"
    activation_permitted: Literal[False] = False
    execution_locked: Literal[True] = True
    ordinary_candidate_count: int | None
    registered_evaluation_count: int
    native_reservation_count: int
    unassigned_native_reservation_count: int
    conservative_native_occupied_usd: Decimal
    economic_remaining_hold_usd: Decimal | None
    observed_uncovered_formal_usd: Decimal | None
    tasks: tuple[TaskObservation, ...]
    blocking_codes: tuple[str, ...]


def reference(*parts: object) -> str:
    return sha256(json.dumps(parts, default=str, separators=(",", ":")).encode()).hexdigest()


async def inspect_inventory(
    session: AsyncSession,
    *,
    account_id: UUID,
    batch_id: UUID,
    enabled: bool,
    now: datetime | None = None,
    configuration_hash: str,
) -> FormalBudgetInventory:
    if now is None:
        now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() != timedelta(0):
        raise ValueError("FORMAL_BUDGET_CLOCK_INVALID")
    day = now.date()
    start = datetime.combine(day, datetime.min.time(), tzinfo=UTC)
    end = start + timedelta(days=1)
    codes = {
        "FORMAL_CONFIGURATION_NOT_FROZEN",
        "FORMAL_PRIORITY_NOT_ENFORCED",
        "WORKER_CAPACITY_NOT_VERIFIED",
        "COMMON_WINDOW_NOT_VERIFIED",
    }
    native: dict[tuple[str, UUID], Reservation] = {}
    native_ids: dict[tuple[str, UUID], UUID] = {}
    native_total = Decimal(0)
    for kind, table in (
        ("research", AgentDailyBudgetReservationRecord),
        ("synthesis", AgentPortfolioSynthesisBudgetReservationRecord),
        ("portfolio_decision", PortfolioDecisionBudgetReservationRecord),
    ):
        for raw in await session.scalars(select(table).where(table.business_date == day)):
            row = cast(
                AgentDailyBudgetReservationRecord
                | AgentPortfolioSynthesisBudgetReservationRecord
                | PortfolioDecisionBudgetReservationRecord,
                raw,
            )
            usage, valid = reservation_usage(row)
            native_total += usage
            if not valid:
                codes.add("NATIVE_USAGE_UNKNOWN")
            native[kind, row.run_id] = row
            native_ids[kind, row.run_id] = row.id
    try:
        remaining_hold = await held_pair_budget(session, business_date=day)
    except ValueError:
        remaining_hold = None
        codes.add("ECONOMIC_HOLD_ASSOCIATION_UNKNOWN")
    tasks: list[TaskObservation] = []
    assigned: set[tuple[str, UUID]] = set()
    cycles = tuple(
        await session.scalars(
            select(AgentPaperCycleRecord).where(
                AgentPaperCycleRecord.decision_slot_at >= start,
                AgentPaperCycleRecord.decision_slot_at < end,
            )
        )
    )
    by_account_slot = {(cycle.account_id, cycle.decision_slot_at): cycle for cycle in cycles}

    seen: set[tuple[UUID, datetime, str, UUID | None]] = set()

    async def task(
        source: str,
        owner: UUID,
        slot: datetime,
        kind: str,
        instrument: UUID | None,
        maximum: Decimal | None,
        actor: str | None,
        key: str | None,
        cycle: AgentPaperCycleRecord | None,
    ) -> None:
        identity = (owner, slot, kind, instrument)
        if identity in seen:
            return
        seen.add(identity)
        run: AgentResearchRunRecord | AgentPortfolioSynthesisRunRecord | None = None
        exact = True
        if cycle is not None:
            try:
                manifest = parse_candidate_manifest(
                    cycle.candidate_manifest,
                    allow_legacy=cycle.decision_price_policy_id == PAPER_DECISION_SOURCE_POLICY_ID,
                )
                exact = candidate_manifest_hash(manifest) == cycle.candidate_hash
            except ValueError:
                manifest, exact = (), False
            if kind == "research":
                run = await session.scalar(
                    select(AgentResearchRunRecord).where(
                        AgentResearchRunRecord.actor_id == actor,
                        AgentResearchRunRecord.idempotency_key == key,
                    )
                )
                events = tuple(
                    await session.scalars(
                        select(AgentPaperCycleEventRecord).where(
                            AgentPaperCycleEventRecord.cycle_id == cycle.id,
                            AgentPaperCycleEventRecord.result_code == "RESEARCH_JOBS_PREPARED",
                        )
                    )
                )
                if isinstance(run, AgentResearchRunRecord):
                    candidate = next(
                        (entry for entry in manifest if entry.instrument_id == instrument), None
                    )
                    snapshot_bound = (
                        candidate is not None
                        and candidate.research_snapshot_id == run.data_snapshot_id
                    )
                    if cycle.execution_session == cycle.decision_session:
                        if not isinstance(candidate, CandidateManifestEntry):
                            exact = False
                        # Same-session research intentionally freezes a new snapshot;
                        # the immutable context, not equality with the slow base ID,
                        # binds that snapshot to this cycle and the run's evidence.
                        context = await read_agent_paper_research_context(
                            session,
                            snapshot_id=run.data_snapshot_id,
                            instrument_id=run.instrument_id,
                            as_of=run.decision_time,
                        )
                        snapshot_bound = False
                        if context is not None:
                            evidence = context[1]
                            frozen_evidence = {
                                "id": str(evidence.id),
                                "source": evidence.source,
                                "provider_object_id": evidence.provider_object_id,
                                "revision": evidence.revision,
                                "available_at": evidence.available_at.isoformat().replace(
                                    "+00:00", "Z"
                                ),
                                "location": evidence.location,
                                "content_hash": evidence.content_hash,
                            }
                            snapshot_bound = (
                                context[0].cycle_id == cycle.id
                                and context[0].decision_time == cycle.decision_time
                                and run.decision_time == cycle.decision_time
                                and frozen_evidence in run.evidence_manifest
                            )
                    exact = (
                        exact
                        and run.instrument_id == instrument
                        and snapshot_bound
                        and any(
                            isinstance(event.fact_ids.get("research_run_ids"), list)
                            and str(run.id)
                            in cast(list[object], event.fact_ids["research_run_ids"])
                            and event.event_hash
                            == canonical_cycle_event_hash(
                                cycle_id=cycle.id,
                                sequence=event.sequence,
                                from_status=cast(CycleStatus | None, event.from_status),
                                to_status=cast(CycleStatus, event.to_status),
                                result_code=event.result_code,
                                fact_ids=event.fact_ids,
                                occurred_at=event.occurred_at,
                            )
                            for event in events
                        )
                    )
            else:
                run = await session.scalar(
                    select(AgentPortfolioSynthesisRunRecord).where(
                        AgentPortfolioSynthesisRunRecord.cycle_id == cycle.id,
                    )
                )
                if isinstance(run, AgentPortfolioSynthesisRunRecord):
                    try:
                        frozen = _frozen_run(run)
                        exact = exact and (
                            run.idempotency_key == f"portfolio-synthesis:{cycle.id}"
                            and frozen.cycle_id == cycle.id
                            and frozen.account_id == owner
                            and run.candidate_batch_id == cycle.candidate_batch_id
                            and set(frozen.candidate_instrument_ids)
                            == {entry.instrument_id for entry in manifest}
                        )
                    except ValueError:
                        exact = False
        reservation = None if run is None or not exact else native.get((kind, run.id))
        if reservation is not None and run is not None:
            assigned.add((kind, run.id))
        claim = await session.scalar(
            select(IdempotencyRecord.id).where(
                IdempotencyRecord.actor_id == f"local-agent-paper-schwab:{owner}",
                IdempotencyRecord.route == "agent-paper-schwab-trigger-v2",
                IdempotencyRecord.idempotency_key == slot.isoformat(),
            )
        )
        state, occupied, uncovered = describe_coverage(
            maximum=maximum,
            now=now,
            slot=slot,
            run_status=None if run is None else run.status,
            reservation=reservation,
            no_start_confirmed=cycle is None and claim is None,
        )
        if not exact or (run is None and cycle is not None):
            state, uncovered = "unknown", None
            codes.add("TASK_ASSOCIATION_UNKNOWN")
        if state == "unknown":
            codes.add("TASK_COVERAGE_UNKNOWN")
        tasks.append(
            TaskObservation(
                task_ref=reference("formal-paper", owner, slot, kind, instrument),
                source=source,
                kind=kind,
                slot_at=slot,
                coverage=state,
                native_reservation_ref=None
                if reservation is None or run is None
                else reference(kind, native_ids[kind, run.id]),
                native_occupied_usd=occupied,
                observed_uncovered_usd=uncovered,
            )
        )

    count = None
    if enabled:
        try:
            batch, _ = await validate_agent_paper_candidate_batch(
                session,
                account_id=account_id,
                batch_id=batch_id,
                as_of=now,
            )
            count = len(batch.instrument_ids)
            local_day = now.astimezone(ZoneInfo("America/New_York")).date()
            calendar = await session.scalar(
                select(ExchangeCalendarAuthorityRecord)
                .where(
                    ExchangeCalendarAuthorityRecord.exchange == "XNYS",
                    ExchangeCalendarAuthorityRecord.completeness_status == "complete",
                    ExchangeCalendarAuthorityRecord.range_start <= local_day,
                    ExchangeCalendarAuthorityRecord.range_end >= local_day,
                    ExchangeCalendarAuthorityRecord.published_at <= now,
                    ExchangeCalendarAuthorityRecord.created_at <= now,
                )
                .order_by(
                    ExchangeCalendarAuthorityRecord.published_at.desc(),
                    ExchangeCalendarAuthorityRecord.id.desc(),
                )
                .limit(1)
            )
            if calendar is None:
                raise ValueError("FORMAL_CALENDAR_UNAVAILABLE")
            model = await session.scalar(
                select(AgentModelConfigRecord)
                .where(
                    AgentModelConfigRecord.version.in_(ACTIONABLE_CONFIG_VERSIONS),
                    AgentModelConfigRecord.created_at <= now,
                    AgentModelConfigRecord.pricing_available_at <= now,
                    AgentModelConfigRecord.pricing_expires_at > now,
                )
                .order_by(
                    AgentModelConfigRecord.created_at.desc(), AgentModelConfigRecord.id.desc()
                )
                .limit(1)
            )
            maximum = None if model is None else model.worst_case_cost_usd
            codes.add("FUTURE_MODEL_SELECTION_NOT_FROZEN")
            rows = await session.scalars(
                select(ExchangeCalendarSessionRecord)
                .where(
                    ExchangeCalendarSessionRecord.calendar_authority_id == calendar.id,
                    ExchangeCalendarSessionRecord.session
                    >= start.astimezone(ZoneInfo("America/New_York")).date(),
                    ExchangeCalendarSessionRecord.session
                    <= end.astimezone(ZoneInfo("America/New_York")).date(),
                    ExchangeCalendarSessionRecord.created_at <= now,
                )
                .order_by(ExchangeCalendarSessionRecord.session)
            )
            for calendar_session in rows:
                slot = datetime.combine(
                    calendar_session.session,
                    PORTFOLIO_ALLOCATION_V1.decision_local_time,
                    tzinfo=ZoneInfo(PORTFOLIO_ALLOCATION_V1.decision_timezone),
                ).astimezone(UTC)
                if slot.date() != day:
                    continue
                cycle = by_account_slot.get((account_id, slot))
                if cycle is not None and cycle.candidate_batch_id != batch_id:
                    codes.add("FORMAL_BATCH_BINDING_CHANGED")
                    cycle = None
                    maximum = None
                for instrument in batch.instrument_ids:
                    await task(
                        "ordinary_slot",
                        account_id,
                        slot,
                        "research",
                        instrument,
                        maximum,
                        "local-agent-paper-cycle",
                        None if cycle is None else f"agent-paper:{cycle.id}:{instrument}",
                        cycle,
                    )
                await task(
                    "ordinary_slot", account_id, slot, "synthesis", None, maximum, None, None, cycle
                )
        except ValueError:
            codes.add("FORMAL_SCOPE_OR_CALENDAR_UNAVAILABLE")
    evaluations = tuple(
        await session.scalars(
            select(AgentVersionEvaluationRecord)
            .outerjoin(
                AgentVersionEvaluationDecisionRecord,
                AgentVersionEvaluationDecisionRecord.evaluation_id
                == AgentVersionEvaluationRecord.id,
            )
            .where(AgentVersionEvaluationDecisionRecord.id.is_(None))
        )
    )
    if len(evaluations) > 1:
        codes.add("REGISTERED_EVALUATIONS_AMBIGUOUS")
    for evaluation in evaluations:
        for slot in tuple(evaluation.train_slots) + tuple(evaluation.oos_slots):
            if slot.astimezone(UTC).date() != day:
                continue
            for lane, owner, actor in (
                ("incumbent", evaluation.incumbent_account_id, AGENT_VERSION_INCUMBENT_ACTOR),
                ("challenger", evaluation.challenger_account_id, AGENT_VERSION_CHALLENGER_ACTOR),
            ):
                await task(
                    "registered_evaluation",
                    owner,
                    slot,
                    "research",
                    evaluation.instrument_id,
                    None,
                    actor,
                    f"agent-version:{evaluation.id}:{lane}:{slot.isoformat()}:{evaluation.instrument_id}",
                    by_account_slot.get((owner, slot)),
                )
            codes.add("REGISTERED_EVALUATION_SOURCE_NOT_FULLY_BOUND")
    # Include actual configured-account cycles outside the ordinary slot without
    # converting them into replacement scheduled opportunities.
    for cycle in cycles:
        if cycle.account_id != account_id:
            continue
        try:
            manifest = parse_candidate_manifest(
                cycle.candidate_manifest,
                allow_legacy=cycle.decision_price_policy_id == PAPER_DECISION_SOURCE_POLICY_ID,
            )
        except ValueError:
            codes.add("TASK_ASSOCIATION_UNKNOWN")
            continue
        for entry in manifest:
            await task(
                "persisted_cycle",
                account_id,
                cycle.decision_slot_at,
                "research",
                entry.instrument_id,
                None,
                "local-agent-paper-cycle",
                f"agent-paper:{cycle.id}:{entry.instrument_id}",
                cycle,
            )
        if cycle.candidate_batch_id is not None:
            await task(
                "persisted_cycle",
                account_id,
                cycle.decision_slot_at,
                "synthesis",
                None,
                None,
                None,
                None,
                cycle,
            )
    # Preserve all native commitments, including tasks outside the configured slot.
    # Unassigned records are visible as a count; they are never dropped from N_d.
    unassigned = len(set(native) - assigned)
    if unassigned:
        codes.add("UNASSIGNED_NATIVE_RESERVATIONS")
    uncovered = (
        None
        if "FORMAL_SCOPE_OR_CALENDAR_UNAVAILABLE" in codes
        or any(t.observed_uncovered_usd is None for t in tasks)
        else sum((cast(Decimal, t.observed_uncovered_usd) for t in tasks), Decimal(0))
    )
    return FormalBudgetInventory(
        as_of=now,
        budget_day_utc=day.isoformat(),
        configuration_hash=configuration_hash,
        ordinary_candidate_count=count,
        registered_evaluation_count=len(evaluations),
        native_reservation_count=len(native),
        unassigned_native_reservation_count=unassigned,
        conservative_native_occupied_usd=native_total,
        economic_remaining_hold_usd=remaining_hold,
        observed_uncovered_formal_usd=uncovered,
        tasks=tuple(tasks),
        blocking_codes=tuple(sorted(codes)),
    )
