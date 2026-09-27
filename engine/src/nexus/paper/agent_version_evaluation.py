"""Atomic registration for one isolated paired Agent-version evaluation."""

import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Literal, cast
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_sdk import MODEL_ID
from nexus.agents.codex_versions import ACTIONABLE_CONFIG_VERSIONS, aggregate_input_limit
from nexus.agents.committee import ROLE_ORDER, AgentRole
from nexus.agents.execution_authority import load_frozen_prompts
from nexus.agents.persistence import load_complete_prompt_authority
from nexus.audit.canonical import canonical_json
from nexus.domain.ids import new_uuid7
from nexus.paper.agent_version_models import (
    MIN_OOS_SESSIONS,
    POLICY_VERSION,
    AgentVersionEvaluationDecision,
    AgentVersionEvaluationRegistration,
    AgentVersionEvaluationRequest,
    AgentVersionMetricFacts,
    FrozenAgentVersionSelection,
    evaluate_agent_version_metrics,
    evaluation_policy_hash,
    evaluation_request_hash,
    has_unsafe_same_session_gap,
)
from nexus.persistence.models.agent_versions import (
    AgentVersionCyclePairRecord,
    AgentVersionEvaluationDecisionRecord,
    AgentVersionEvaluationRecord,
)
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.market import InstrumentAlias
from nexus.persistence.models.paper import (
    AgentPaperCycleEventRecord,
    AgentPaperCycleRecord,
    PaperAccountRecord,
    PaperFillRecord,
    PaperLedgerEntryRecord,
    PaperLedgerTransactionRecord,
    PaperOrderRecord,
    PaperPerformanceSnapshotRecord,
    RiskEvaluationRecord,
    StrategyDecisionAgentOutputRecord,
    StrategyDecisionRecord,
)
from nexus.persistence.models.portfolio import Account, Portfolio
from nexus.reconciliation.admission_claim import claim_or_lock
from nexus.reconciliation.persistence_errors import IdempotencyConflict

_ROUTE = "agent-version-evaluation:register"
_SCHEMA_VERSION = "agent_version_eval_reg_v1"
_NEW_YORK = ZoneInfo("America/New_York")
_UNRECOVERABLE_SLOT_REASONS = frozenset(
    {
        "PAIR_AUTHORITY_UNAVAILABLE",
        "PAIR_START_STATE_MISMATCH",
        "PAIR_PREPARATION_UNAVAILABLE",
        "PAIR_PRIOR_DAILY_UNAVAILABLE",
        "PAIR_CURRENT_TRENDS_UNAVAILABLE",
        "PAIR_RISK_INPUT_REQUIRED",
        "PAIR_SCHWAB_L1_FRESHNESS_UNPROVEN",
        "PAIR_SCHWAB_L1_INCOMPLETE",
        "PAIR_CYCLES_UNAVAILABLE",
        "PAIR_RUN_BINDING_INVALID",
        "PAIR_EVIDENCE_MISMATCH",
        "PAIR_LANE_TERMINAL_FAILURE",
        "PAIR_PREVIOUS_SLOT_INCOMPLETE",
    }
)


async def register_agent_version_evaluation(
    session: AsyncSession,
    *,
    request: AgentVersionEvaluationRequest,
    actor_id: str,
    idempotency_key: str,
    now: datetime,
) -> AgentVersionEvaluationRegistration:
    """Register frozen authority and two equal internal accounts without model work."""
    if now.tzinfo is None or now.utcoffset() != UTC.utcoffset(now):
        raise ValueError("AGENT_VERSION_TIME_INVALID")
    request_hash = evaluation_request_hash(request)
    claim, owner = await claim_or_lock(
        session,
        actor_id,
        _ROUTE,
        idempotency_key,
        request_hash,
        _SCHEMA_VERSION,
    )
    if not owner:
        if claim.request_hash != request_hash:
            raise IdempotencyConflict(
                "idempotency key Agent-version request conflicts with prior request"
            )
        return _registration_from_payload(claim.response_payload, replayed=True)

    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 1417))"),
        {"scope": "agent-version-evaluation-registration"},
    )
    incumbent_run = await session.get(
        AgentResearchRunRecord,
        request.incumbent_version_source_run_id,
        populate_existing=True,
    )
    if (
        incumbent_run is None
        or incumbent_run.status != "completed"
        or incumbent_run.instrument_id != request.instrument_id
    ):
        raise ValueError("AGENT_VERSION_INCUMBENT_INVALID")
    incumbent_prompts = await load_frozen_prompts(session, incumbent_run)
    incumbent_model = await session.get(
        AgentModelConfigRecord, incumbent_run.model_config_id, populate_existing=True
    )
    if (
        incumbent_prompts is None
        or incumbent_model is None
        or incumbent_model.config_hash != incumbent_run.model_hash
        or incumbent_model.version != incumbent_run.pricing_version
    ):
        raise ValueError("AGENT_VERSION_INCUMBENT_INVALID")

    challenger_prompts = await load_complete_prompt_authority(
        session, orchestrator_id=request.challenger_prompt_version_id
    )
    challenger_model = await session.get(
        AgentModelConfigRecord,
        request.challenger_model_config_id,
        populate_existing=True,
    )
    if challenger_prompts is None or challenger_model is None:
        raise ValueError("AGENT_VERSION_CHALLENGER_INVALID")
    if not _model_is_agent_paper_compatible(
        incumbent_model
    ) or not _model_is_agent_paper_compatible(challenger_model):
        raise ValueError("AGENT_VERSION_MODEL_INCOMPATIBLE")

    last_slot = request.oos_slots[-1]
    if not _pricing_covers(incumbent_model, now=now, last_slot=last_slot) or not _pricing_covers(
        challenger_model, now=now, last_slot=last_slot
    ):
        raise ValueError("AGENT_VERSION_PRICING_INVALID")
    challenger_prompt_hash = _prompt_manifest_hash(challenger_prompts)
    if (
        incumbent_run.prompt_hash == challenger_prompt_hash
        and incumbent_run.model_hash == challenger_model.config_hash
    ):
        raise ValueError("AGENT_VERSION_IDENTITY_UNCHANGED")

    if not await _is_aapl(session, request, now=now):
        raise ValueError("AGENT_VERSION_INSTRUMENT_INVALID")
    calendar = await _select_calendar(session, request=request, now=now)
    if calendar is None:
        raise ValueError("AGENT_VERSION_CALENDAR_INVALID")

    policy_hash = evaluation_policy_hash()
    authority_hash = _authority_hash(
        request=request,
        calendar=calendar,
        incumbent_prompt_hash=incumbent_run.prompt_hash,
        incumbent_model_hash=incumbent_run.model_hash,
        challenger_prompt_hash=challenger_prompt_hash,
        challenger_model_hash=challenger_model.config_hash,
        policy_hash=policy_hash,
    )
    existing = await session.scalar(
        select(AgentVersionEvaluationRecord).where(
            AgentVersionEvaluationRecord.authority_hash == authority_hash
        )
    )
    if existing is not None:
        registration = _registration(existing, replayed=True)
        claim.response_payload = registration.model_dump(mode="json")
        await session.flush()
        return registration

    open_evaluation = await session.scalar(
        select(AgentVersionEvaluationRecord)
        .outerjoin(
            AgentVersionEvaluationDecisionRecord,
            AgentVersionEvaluationDecisionRecord.evaluation_id == AgentVersionEvaluationRecord.id,
        )
        .where(AgentVersionEvaluationDecisionRecord.evaluation_id.is_(None))
        .limit(1)
    )
    if open_evaluation is not None:
        raise ValueError("AGENT_VERSION_EVALUATION_ALREADY_OPEN")

    evaluation_id = new_uuid7()
    incumbent_account = _evaluation_account(
        evaluation_id=evaluation_id,
        lane="incumbent",
        initial_capital=request.initial_capital,
    )
    challenger_account = _evaluation_account(
        evaluation_id=evaluation_id,
        lane="challenger",
        initial_capital=request.initial_capital,
    )
    session.add_all((incumbent_account[0], challenger_account[0]))
    await session.flush()
    session.add_all((incumbent_account[1], challenger_account[1]))
    await session.flush()
    session.add_all((incumbent_account[2], challenger_account[2]))
    await session.flush()
    evaluation = AgentVersionEvaluationRecord(
        id=evaluation_id,
        incumbent_account_id=incumbent_account[2].id,
        challenger_account_id=challenger_account[2].id,
        instrument_id=request.instrument_id,
        calendar_authority_id=calendar.id,
        incumbent_version_source_run_id=incumbent_run.id,
        challenger_prompt_version_id=request.challenger_prompt_version_id,
        challenger_model_config_id=challenger_model.id,
        incumbent_prompt_hash=incumbent_run.prompt_hash,
        incumbent_model_hash=incumbent_run.model_hash,
        challenger_prompt_hash=challenger_prompt_hash,
        challenger_model_hash=challenger_model.config_hash,
        train_slots=list(request.train_slots),
        oos_slots=list(request.oos_slots),
        policy_version=POLICY_VERSION,
        policy_hash=policy_hash,
        authority_hash=authority_hash,
    )
    session.add(evaluation)
    registration = _registration(evaluation, replayed=False)
    claim.response_payload = registration.model_dump(mode="json")
    await session.flush()
    return registration


async def finalize_agent_version_evaluation(
    session: AsyncSession,
    *,
    evaluation_id: UUID,
    now: datetime,
) -> AgentVersionEvaluationDecision:
    """Persist one deterministic OOS decision without model or provider work."""
    if now.tzinfo is None or now.utcoffset() != UTC.utcoffset(now):
        raise ValueError("AGENT_VERSION_TIME_INVALID")
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 0))"),
        {"scope": f"nexus:agent-version-evaluation:{evaluation_id}"},
    )
    evaluation = await session.get(
        AgentVersionEvaluationRecord,
        evaluation_id,
        populate_existing=True,
    )
    if evaluation is None:
        raise ValueError("AGENT_VERSION_EVALUATION_UNKNOWN")
    existing = await session.scalar(
        select(AgentVersionEvaluationDecisionRecord).where(
            AgentVersionEvaluationDecisionRecord.evaluation_id == evaluation.id
        )
    )
    if existing is not None:
        return _decision_result(existing, evaluation=evaluation, replayed=True)
    session_closes = await load_oos_session_closes(session, evaluation=evaluation)
    if not session_closes or now < session_closes[-1]:
        raise ValueError("AGENT_VERSION_WINDOW_OPEN")

    facts = await _load_agent_version_metric_facts(
        session,
        evaluation=evaluation,
        now=now,
    )
    report = evaluate_agent_version_metrics(facts)
    selected_run_id = evaluation.incumbent_version_source_run_id
    if report.status == "PASS":
        challenger_run_id = await session.scalar(
            select(AgentVersionCyclePairRecord.challenger_run_id)
            .where(
                AgentVersionCyclePairRecord.evaluation_id == evaluation.id,
                AgentVersionCyclePairRecord.phase == "oos",
            )
            .order_by(
                AgentVersionCyclePairRecord.decision_slot_at,
                AgentVersionCyclePairRecord.id,
            )
            .limit(1)
        )
        if challenger_run_id is None:
            raise ValueError("AGENT_VERSION_DECISION_AUTHORITY_INVALID")
        selected_run_id = challenger_run_id
    return await _persist_evaluation_decision(
        session,
        evaluation=evaluation,
        status=report.status,
        reason_code=report.reason_code,
        metric_report=report.model_dump(mode="json"),
        selected_run_id=selected_run_id,
    )


async def invalidate_agent_version_evaluation(
    session: AsyncSession,
    *,
    evaluation_id: UUID,
    now: datetime,
) -> AgentVersionEvaluationDecision:
    """Close one legacy, structurally invalid evaluation without replaying work."""
    if now.tzinfo is None or now.utcoffset() != UTC.utcoffset(now):
        raise ValueError("AGENT_VERSION_TIME_INVALID")
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 0))"),
        {"scope": f"nexus:agent-version-evaluation:{evaluation_id}"},
    )
    evaluation = await session.get(
        AgentVersionEvaluationRecord,
        evaluation_id,
        populate_existing=True,
    )
    if evaluation is None:
        raise ValueError("AGENT_VERSION_EVALUATION_UNKNOWN")
    existing = await session.scalar(
        select(AgentVersionEvaluationDecisionRecord).where(
            AgentVersionEvaluationDecisionRecord.evaluation_id == evaluation.id
        )
    )
    if existing is not None:
        return _decision_result(existing, evaluation=evaluation, replayed=True)
    first_session = min(_session(slot) for slot in evaluation.train_slots)
    has_prior_session = await _calendar_has_prior_session(
        session,
        calendar_id=evaluation.calendar_authority_id,
        before=first_session,
    )
    schedule_unsafe = has_unsafe_same_session_gap(
        tuple(evaluation.train_slots)
    ) or has_unsafe_same_session_gap(tuple(evaluation.oos_slots))
    if has_prior_session and not schedule_unsafe:
        raise ValueError("AGENT_VERSION_EVALUATION_NOT_INVALID")
    pair_count = int(
        await session.scalar(
            select(func.count())
            .select_from(AgentVersionCyclePairRecord)
            .where(AgentVersionCyclePairRecord.evaluation_id == evaluation.id)
        )
        or 0
    )
    cycle_count = int(
        await session.scalar(
            select(func.count())
            .select_from(AgentPaperCycleRecord)
            .where(
                AgentPaperCycleRecord.account_id.in_(
                    (evaluation.incumbent_account_id, evaluation.challenger_account_id)
                )
            )
        )
        or 0
    )
    account_ids = (evaluation.incumbent_account_id, evaluation.challenger_account_id)
    order_count = int(
        await session.scalar(
            select(func.count())
            .select_from(PaperOrderRecord)
            .where(PaperOrderRecord.account_id.in_(account_ids))
        )
        or 0
    )
    fill_count = int(
        await session.scalar(
            select(func.count())
            .select_from(PaperFillRecord)
            .where(PaperFillRecord.account_id.in_(account_ids))
        )
        or 0
    )
    if pair_count or cycle_count or order_count or fill_count:
        raise ValueError("AGENT_VERSION_INVALIDATION_UNSAFE")
    facts = await _load_agent_version_metric_facts(
        session,
        evaluation=evaluation,
        now=now,
    )
    report = evaluate_agent_version_metrics(facts)
    return await _persist_evaluation_decision(
        session,
        evaluation=evaluation,
        status="UNKNOWN",
        reason_code=(
            "CALENDAR_PRIOR_SESSION_MISSING"
            if not has_prior_session
            else "EVALUATION_SCHEDULE_UNSAFE"
        ),
        metric_report=report.model_dump(mode="json"),
        selected_run_id=evaluation.incumbent_version_source_run_id,
    )


async def close_incomplete_agent_version_evaluation(
    session: AsyncSession,
    *,
    evaluation_id: UUID,
    now: datetime,
) -> AgentVersionEvaluationDecision:
    """Close an evaluation after an unstarted registered slot was missed."""
    if now.tzinfo is None or now.utcoffset() != UTC.utcoffset(now):
        raise ValueError("AGENT_VERSION_TIME_INVALID")
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 0))"),
        {"scope": f"nexus:agent-version-evaluation:{evaluation_id}"},
    )
    evaluation = await session.get(
        AgentVersionEvaluationRecord,
        evaluation_id,
        populate_existing=True,
    )
    if evaluation is None:
        raise ValueError("AGENT_VERSION_EVALUATION_UNKNOWN")
    existing = await session.scalar(
        select(AgentVersionEvaluationDecisionRecord).where(
            AgentVersionEvaluationDecisionRecord.evaluation_id == evaluation.id
        )
    )
    if existing is not None:
        return _decision_result(existing, evaluation=evaluation, replayed=True)

    paired_slots = set(
        await session.scalars(
            select(AgentVersionCyclePairRecord.decision_slot_at).where(
                AgentVersionCyclePairRecord.evaluation_id == evaluation.id
            )
        )
    )
    registered_slots = tuple(evaluation.train_slots) + tuple(evaluation.oos_slots)
    missing_slot = next((slot for slot in registered_slots if slot not in paired_slots), None)
    if missing_slot is None or now <= missing_slot + timedelta(seconds=60):
        raise ValueError("AGENT_VERSION_SLOT_NOT_MISSED")
    cycle_count = int(
        await session.scalar(
            select(func.count())
            .select_from(AgentPaperCycleRecord)
            .where(
                AgentPaperCycleRecord.account_id.in_(
                    (evaluation.incumbent_account_id, evaluation.challenger_account_id)
                ),
                AgentPaperCycleRecord.decision_slot_at == missing_slot,
            )
        )
        or 0
    )
    if cycle_count:
        raise ValueError("AGENT_VERSION_INCOMPLETE_CLOSE_UNSAFE")

    facts = await _load_agent_version_metric_facts(
        session,
        evaluation=evaluation,
        now=now,
    )
    report = evaluate_agent_version_metrics(facts)
    return await _persist_evaluation_decision(
        session,
        evaluation=evaluation,
        status="UNKNOWN",
        reason_code="REGISTERED_SLOT_MISSED",
        metric_report=report.model_dump(mode="json"),
        selected_run_id=evaluation.incumbent_version_source_run_id,
    )


async def close_unrecoverable_agent_version_evaluation(
    session: AsyncSession,
    *,
    evaluation_id: UUID,
    decision_slot_at: datetime,
    reason_code: str,
    now: datetime,
) -> AgentVersionEvaluationDecision:
    """Close one current slot whose paired runner found a terminal invariant failure."""
    if (
        now.tzinfo is None
        or now.utcoffset() != UTC.utcoffset(now)
        or reason_code not in _UNRECOVERABLE_SLOT_REASONS
    ):
        raise ValueError("AGENT_VERSION_UNRECOVERABLE_CLOSE_INVALID")
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 0))"),
        {"scope": f"nexus:agent-version-evaluation:{evaluation_id}"},
    )
    evaluation = await session.get(
        AgentVersionEvaluationRecord,
        evaluation_id,
        populate_existing=True,
    )
    if evaluation is None:
        raise ValueError("AGENT_VERSION_EVALUATION_UNKNOWN")
    existing = await session.scalar(
        select(AgentVersionEvaluationDecisionRecord).where(
            AgentVersionEvaluationDecisionRecord.evaluation_id == evaluation.id
        )
    )
    if existing is not None:
        return _decision_result(existing, evaluation=evaluation, replayed=True)
    paired_slots = set(
        await session.scalars(
            select(AgentVersionCyclePairRecord.decision_slot_at).where(
                AgentVersionCyclePairRecord.evaluation_id == evaluation.id
            )
        )
    )
    registered_slots = tuple(evaluation.train_slots) + tuple(evaluation.oos_slots)
    current_slot = next((slot for slot in registered_slots if slot not in paired_slots), None)
    if current_slot != decision_slot_at:
        raise ValueError("AGENT_VERSION_SLOT_NOT_CURRENT")
    facts = await _load_agent_version_metric_facts(
        session,
        evaluation=evaluation,
        now=now,
    )
    report = evaluate_agent_version_metrics(facts)
    metric_report = report.model_dump(mode="json")
    metric_report["terminal_slot"] = {
        "decision_slot_at": decision_slot_at.isoformat(),
        "reason_code": reason_code,
    }
    return await _persist_evaluation_decision(
        session,
        evaluation=evaluation,
        status="UNKNOWN",
        reason_code=reason_code,
        metric_report=metric_report,
        selected_run_id=evaluation.incumbent_version_source_run_id,
    )


async def _persist_evaluation_decision(
    session: AsyncSession,
    *,
    evaluation: AgentVersionEvaluationRecord,
    status: Literal["PASS", "FAIL", "UNKNOWN"],
    reason_code: str,
    metric_report: dict[str, object],
    selected_run_id: UUID,
) -> AgentVersionEvaluationDecision:
    metric_hash = hashlib.sha256(canonical_json(metric_report).encode()).hexdigest()
    decision_hash = hashlib.sha256(
        canonical_json(
            {
                "evaluation_id": str(evaluation.id),
                "metric_hash": metric_hash,
                "policy_hash": evaluation.policy_hash,
                "reason_code": reason_code,
                "selected_version_source_run_id": str(selected_run_id),
                "status": status,
            }
        ).encode()
    ).hexdigest()
    record = AgentVersionEvaluationDecisionRecord(
        id=new_uuid7(),
        evaluation_id=evaluation.id,
        status=status,
        metric_report=metric_report,
        metric_hash=metric_hash,
        selected_version_source_run_id=selected_run_id,
        reason_code=reason_code,
        decision_hash=decision_hash,
    )
    session.add(record)
    await session.flush()
    return _decision_result(record, evaluation=evaluation, replayed=False)


async def load_selected_internal_agent_version(
    session: AsyncSession,
    *,
    now: datetime,
) -> FrozenAgentVersionSelection | None:
    """Return only the newest hash-valid PASS selection for future internal cycles."""
    if now.tzinfo is None or now.utcoffset() != UTC.utcoffset(now):
        raise ValueError("AGENT_VERSION_TIME_INVALID")
    row = (
        await session.execute(
            select(AgentVersionEvaluationDecisionRecord, AgentVersionEvaluationRecord)
            .join(
                AgentVersionEvaluationRecord,
                AgentVersionEvaluationRecord.id
                == AgentVersionEvaluationDecisionRecord.evaluation_id,
            )
            .where(
                AgentVersionEvaluationDecisionRecord.status == "PASS",
                AgentVersionEvaluationDecisionRecord.created_at <= now,
            )
            .order_by(
                AgentVersionEvaluationDecisionRecord.created_at.desc(),
                AgentVersionEvaluationDecisionRecord.id.desc(),
            )
            .limit(1)
        )
    ).one_or_none()
    if row is None:
        return None
    decision, evaluation = row
    run = await session.get(
        AgentResearchRunRecord,
        decision.selected_version_source_run_id,
        populate_existing=True,
    )
    prompts = None if run is None else await load_frozen_prompts(session, run)
    model = (
        None
        if run is None
        else await session.get(
            AgentModelConfigRecord,
            run.model_config_id,
            populate_existing=True,
        )
    )
    first_oos_run = await session.scalar(
        select(AgentVersionCyclePairRecord.challenger_run_id)
        .where(
            AgentVersionCyclePairRecord.evaluation_id == evaluation.id,
            AgentVersionCyclePairRecord.phase == "oos",
        )
        .order_by(
            AgentVersionCyclePairRecord.decision_slot_at,
            AgentVersionCyclePairRecord.id,
        )
        .limit(1)
    )
    if (
        run is None
        or run.status != "completed"
        or prompts is None
        or model is None
        or run.id != first_oos_run
        or run.prompt_hash != evaluation.challenger_prompt_hash
        or run.model_hash != evaluation.challenger_model_hash
        or _prompt_manifest_hash(prompts) != run.prompt_hash
        or model.config_hash != run.model_hash
    ):
        raise ValueError("AGENT_VERSION_SELECTED_AUTHORITY_INVALID")
    return FrozenAgentVersionSelection(
        source_run_id=run.id,
        prompt_hash=run.prompt_hash,
        model_hash=run.model_hash,
    )


async def _load_agent_version_metric_facts(
    session: AsyncSession,
    *,
    evaluation: AgentVersionEvaluationRecord,
    now: datetime,
) -> AgentVersionMetricFacts:
    pairs = tuple(
        await session.scalars(
            select(AgentVersionCyclePairRecord)
            .where(
                AgentVersionCyclePairRecord.evaluation_id == evaluation.id,
                AgentVersionCyclePairRecord.phase == "oos",
            )
            .order_by(AgentVersionCyclePairRecord.decision_slot_at)
        )
    )
    cycle_ids = tuple(
        cycle_id
        for pair in pairs
        for cycle_id in (pair.incumbent_cycle_id, pair.challenger_cycle_id)
    )
    cycles = (
        tuple(
            await session.scalars(
                select(AgentPaperCycleRecord).where(AgentPaperCycleRecord.id.in_(cycle_ids))
            )
        )
        if cycle_ids
        else ()
    )
    decisions = (
        tuple(
            await session.scalars(
                select(StrategyDecisionRecord).where(
                    StrategyDecisionRecord.agent_paper_cycle_id.in_(cycle_ids)
                )
            )
        )
        if cycle_ids
        else ()
    )
    decision_by_cycle = {row.agent_paper_cycle_id: row for row in decisions}
    decision_ids = tuple(row.id for row in decisions)
    bindings = (
        tuple(
            await session.scalars(
                select(StrategyDecisionAgentOutputRecord).where(
                    StrategyDecisionAgentOutputRecord.strategy_decision_id.in_(decision_ids)
                )
            )
        )
        if decision_ids
        else ()
    )
    action_by_decision = {row.strategy_decision_id: row.sizing_action for row in bindings}
    divergence_count = 0
    for pair in pairs:
        incumbent_decision = decision_by_cycle.get(pair.incumbent_cycle_id)
        challenger_decision = decision_by_cycle.get(pair.challenger_cycle_id)
        if incumbent_decision is None or challenger_decision is None:
            continue
        incumbent_action = action_by_decision.get(incumbent_decision.id)
        challenger_action = action_by_decision.get(challenger_decision.id)
        if incumbent_action is not None and challenger_action is not None:
            divergence_count += int(incumbent_action != challenger_action)

    account_ids = (evaluation.incumbent_account_id, evaluation.challenger_account_id)
    fills = tuple(
        await session.scalars(
            select(PaperFillRecord).where(
                PaperFillRecord.account_id.in_(account_ids),
                PaperFillRecord.occurred_at >= evaluation.oos_slots[0],
                PaperFillRecord.occurred_at <= now,
            )
        )
    )
    orders = tuple(
        await session.scalars(
            select(PaperOrderRecord).where(
                PaperOrderRecord.account_id.in_(account_ids),
                PaperOrderRecord.created_at >= evaluation.oos_slots[0],
                PaperOrderRecord.created_at <= now,
            )
        )
    )
    order_ids = tuple(row.id for row in orders)
    risk_rows = (
        tuple(
            await session.scalars(
                select(RiskEvaluationRecord).where(RiskEvaluationRecord.order_id.in_(order_ids))
            )
        )
        if order_ids
        else ()
    )
    risk_by_order = {row.order_id: row for row in risk_rows}
    hard_risk_violations = sum(
        1
        for order in orders
        if order.id not in risk_by_order or risk_by_order[order.id].outcome != order.risk_outcome
    )

    session_closes = await load_oos_session_closes(session, evaluation=evaluation)
    snapshots = (
        tuple(
            await session.scalars(
                select(PaperPerformanceSnapshotRecord)
                .where(
                    PaperPerformanceSnapshotRecord.account_id.in_(account_ids),
                    PaperPerformanceSnapshotRecord.as_of.in_(session_closes),
                    PaperPerformanceSnapshotRecord.as_of <= now,
                )
                .order_by(
                    PaperPerformanceSnapshotRecord.account_id,
                    PaperPerformanceSnapshotRecord.as_of,
                )
            )
        )
        if session_closes
        else ()
    )
    snapshot_by_key = {(row.account_id, row.as_of): row for row in snapshots}
    required_snapshot_keys = {
        (account_id, close) for account_id in account_ids for close in session_closes
    }

    accounts = tuple(
        await session.scalars(
            select(PaperAccountRecord).where(PaperAccountRecord.id.in_(account_ids))
        )
    )
    capital_by_account = {row.id: row.initial_capital for row in accounts}
    comparable_performance = (
        len(session_closes) == MIN_OOS_SESSIONS
        and set(snapshot_by_key) == required_snapshot_keys
        and set(capital_by_account) == set(account_ids)
    )
    cumulative_excess_return = Decimal(0)
    positive_sessions = 0
    drawdown_degradation = Decimal(0)
    if comparable_performance:
        incumbent_rows = [
            snapshot_by_key[(evaluation.incumbent_account_id, close)]
            for close in session_closes
        ]
        challenger_rows = [
            snapshot_by_key[(evaluation.challenger_account_id, close)]
            for close in session_closes
        ]
        cumulative_excess_return = (
            challenger_rows[-1].nav / capital_by_account[evaluation.challenger_account_id]
            - incumbent_rows[-1].nav / capital_by_account[evaluation.incumbent_account_id]
        )
        previous_incumbent = capital_by_account[evaluation.incumbent_account_id]
        previous_challenger = capital_by_account[evaluation.challenger_account_id]
        for incumbent_row, challenger_row in zip(
            incumbent_rows, challenger_rows, strict=True
        ):
            incumbent_nav = incumbent_row.nav
            challenger_nav = challenger_row.nav
            positive_sessions += int(
                challenger_nav / previous_challenger > incumbent_nav / previous_incumbent
            )
            previous_incumbent = incumbent_nav
            previous_challenger = challenger_nav
        drawdown_degradation = max(row.drawdown_ratio for row in challenger_rows) - max(
            row.drawdown_ratio for row in incumbent_rows
        )

    transactions = tuple(
        await session.scalars(
            select(PaperLedgerTransactionRecord).where(
                PaperLedgerTransactionRecord.account_id.in_(account_ids),
                PaperLedgerTransactionRecord.occurred_at <= now,
            )
        )
    )
    transaction_ids = tuple(row.id for row in transactions)
    entries = (
        tuple(
            await session.scalars(
                select(PaperLedgerEntryRecord).where(
                    PaperLedgerEntryRecord.transaction_id.in_(transaction_ids)
                )
            )
        )
        if transaction_ids
        else ()
    )
    ledger_conserved = all(
        sum(
            (entry.debit_amount for entry in entries if entry.transaction_id == transaction.id),
            Decimal(0),
        )
        == sum(
            (entry.credit_amount for entry in entries if entry.transaction_id == transaction.id),
            Decimal(0),
        )
        for transaction in transactions
    )
    latest_events: dict[UUID, AgentPaperCycleEventRecord] = {}
    if cycle_ids:
        events = tuple(
            await session.scalars(
                select(AgentPaperCycleEventRecord)
                .where(AgentPaperCycleEventRecord.cycle_id.in_(cycle_ids))
                .order_by(
                    AgentPaperCycleEventRecord.cycle_id,
                    AgentPaperCycleEventRecord.sequence,
                )
            )
        )
        for event in events:
            latest_events[event.cycle_id] = event
    authority_complete = (
        len(pairs) == len(evaluation.oos_slots)
        and len(cycles) == len(evaluation.oos_slots) * 2
        and len(decisions) == len(evaluation.oos_slots) * 2
        and len(bindings) == len(evaluation.oos_slots) * 2
        and comparable_performance
        and all(cycle.execution_locked for cycle in cycles)
        and all(
            latest_events.get(cycle.id) is not None
            and latest_events[cycle.id].to_status == "completed"
            for cycle in cycles
        )
    )
    duplicate_count = (
        len(orders)
        - len({(row.account_id, row.client_order_id) for row in orders})
        + len(fills)
        - len({(row.order_id, row.fill_seq) for row in fills})
    )
    execution_model_hashes = {row.execution_model_hash for row in fills}
    cost_authority_complete = comparable_performance and len(execution_model_hashes) <= 1
    return AgentVersionMetricFacts(
        oos_pair_count=len(pairs),
        action_divergence_count=divergence_count,
        challenger_fill_count=sum(
            row.account_id == evaluation.challenger_account_id for row in fills
        ),
        cumulative_excess_return=cumulative_excess_return,
        positive_excess_session_count=positive_sessions,
        drawdown_degradation=drawdown_degradation,
        hard_risk_violation_count=hard_risk_violations,
        ledger_conserved=ledger_conserved,
        duplicate_execution_fact_count=duplicate_count,
        cost_authority_complete=cost_authority_complete,
        comparison_authority_complete=False,
        authority_complete=authority_complete,
    )


async def load_oos_session_closes(
    session: AsyncSession,
    *,
    evaluation: AgentVersionEvaluationRecord,
) -> tuple[datetime, ...]:
    session_dates = tuple(sorted({_session(slot) for slot in evaluation.oos_slots}))
    if len(session_dates) != MIN_OOS_SESSIONS:
        return ()
    rows = tuple(
        await session.scalars(
            select(ExchangeCalendarSessionRecord)
            .where(
                ExchangeCalendarSessionRecord.calendar_authority_id
                == evaluation.calendar_authority_id,
                ExchangeCalendarSessionRecord.session.in_(session_dates),
            )
            .order_by(ExchangeCalendarSessionRecord.session)
        )
    )
    if tuple(row.session for row in rows) != session_dates:
        return ()
    closes = tuple(row.market_close_at for row in rows)
    return closes if len(set(closes)) == len(closes) else ()


def _decision_result(
    record: AgentVersionEvaluationDecisionRecord,
    *,
    evaluation: AgentVersionEvaluationRecord,
    replayed: bool,
) -> AgentVersionEvaluationDecision:
    expected_metric_hash = hashlib.sha256(canonical_json(record.metric_report).encode()).hexdigest()
    if expected_metric_hash != record.metric_hash:
        raise ValueError("AGENT_VERSION_DECISION_AUTHORITY_INVALID")
    expected_decision_hash = hashlib.sha256(
        canonical_json(
            {
                "evaluation_id": str(record.evaluation_id),
                "metric_hash": record.metric_hash,
                "policy_hash": evaluation.policy_hash,
                "reason_code": record.reason_code,
                "selected_version_source_run_id": str(record.selected_version_source_run_id),
                "status": record.status,
            }
        ).encode()
    ).hexdigest()
    if expected_decision_hash != record.decision_hash:
        raise ValueError("AGENT_VERSION_DECISION_AUTHORITY_INVALID")
    if record.status not in {"PASS", "FAIL", "UNKNOWN"}:
        raise ValueError("AGENT_VERSION_DECISION_AUTHORITY_INVALID")
    return AgentVersionEvaluationDecision(
        evaluation_id=record.evaluation_id,
        status=cast(Literal["PASS", "FAIL", "UNKNOWN"], record.status),
        selected_version_source_run_id=record.selected_version_source_run_id,
        reason_code=record.reason_code,
        metric_hash=record.metric_hash,
        decision_hash=record.decision_hash,
        replayed=replayed,
    )


def _evaluation_account(
    *, evaluation_id: UUID, lane: str, initial_capital: Decimal
) -> tuple[Account, Portfolio, PaperAccountRecord]:
    account_id = new_uuid7()
    portfolio_id = new_uuid7()
    paper_account_id = new_uuid7()
    account = Account(
        id=account_id,
        provider="internal_paper_evaluation",
        status="ACTIVE",
    )
    portfolio = Portfolio(
        id=portfolio_id,
        account_id=account_id,
        name=f"Agent version {lane} {evaluation_id}",
    )
    paper = PaperAccountRecord(
        id=paper_account_id,
        account_id=account_id,
        portfolio_id=portfolio_id,
        code=f"ave-{lane[:3]}-{evaluation_id.hex}",
        currency="USD",
        account_model="cash",
        position_model="long_only",
        initial_capital=initial_capital,
        capability="simulation_write_internal",
    )
    return account, portfolio, paper


async def _is_aapl(
    session: AsyncSession,
    request: AgentVersionEvaluationRequest,
    *,
    now: datetime,
) -> bool:
    first_date = _session(request.train_slots[0])
    last_date = _session(request.oos_slots[-1])
    alias = await session.scalar(
        select(InstrumentAlias.id)
        .where(
            InstrumentAlias.instrument_id == request.instrument_id,
            InstrumentAlias.symbol == "AAPL",
            InstrumentAlias.alias_type == "ticker",
            InstrumentAlias.created_at <= now,
            InstrumentAlias.valid_from <= first_date,
            (InstrumentAlias.valid_to.is_(None) | (InstrumentAlias.valid_to >= last_date)),
        )
        .limit(1)
    )
    return alias is not None


async def _select_calendar(
    session: AsyncSession,
    *,
    request: AgentVersionEvaluationRequest,
    now: datetime,
) -> ExchangeCalendarAuthorityRecord | None:
    slots = request.train_slots + request.oos_slots
    dates = {_session(slot) for slot in slots}
    candidates = tuple(
        await session.scalars(
            select(ExchangeCalendarAuthorityRecord)
            .where(
                ExchangeCalendarAuthorityRecord.exchange == "XNYS",
                ExchangeCalendarAuthorityRecord.completeness_status == "complete",
                ExchangeCalendarAuthorityRecord.range_start <= min(dates),
                ExchangeCalendarAuthorityRecord.range_end >= max(dates),
                ExchangeCalendarAuthorityRecord.published_at <= now,
            )
            .order_by(
                ExchangeCalendarAuthorityRecord.published_at.desc(),
                ExchangeCalendarAuthorityRecord.id.desc(),
            )
        )
    )
    for candidate in candidates:
        if not await _calendar_has_prior_session(
            session,
            calendar_id=candidate.id,
            before=min(dates),
        ):
            continue
        rows = tuple(
            await session.scalars(
                select(ExchangeCalendarSessionRecord).where(
                    ExchangeCalendarSessionRecord.calendar_authority_id == candidate.id,
                    ExchangeCalendarSessionRecord.session.in_(dates),
                )
            )
        )
        by_date = {row.session: row for row in rows}
        if set(by_date) != dates:
            continue
        if all(
            by_date[_session(slot)].market_open_at <= slot
            and slot < by_date[_session(slot)].market_close_at
            for slot in slots
        ):
            return candidate
    return None


async def _calendar_has_prior_session(
    session: AsyncSession,
    *,
    calendar_id: UUID,
    before: date,
) -> bool:
    prior_session = await session.scalar(
        select(ExchangeCalendarSessionRecord.session)
        .where(
            ExchangeCalendarSessionRecord.calendar_authority_id == calendar_id,
            ExchangeCalendarSessionRecord.session < before,
        )
        .order_by(ExchangeCalendarSessionRecord.session.desc())
        .limit(1)
    )
    return prior_session is not None


def _pricing_covers(model: AgentModelConfigRecord, *, now: datetime, last_slot: datetime) -> bool:
    return model.pricing_available_at <= now and model.pricing_expires_at > last_slot


def _model_is_agent_paper_compatible(model: AgentModelConfigRecord) -> bool:
    return (
        model.model_id == MODEL_ID
        and model.version in ACTIONABLE_CONFIG_VERSIONS
        and model.max_input_tokens == aggregate_input_limit(model.version)
        and model.max_output_tokens == 8192
        and model.input_usd_per_million == 0
        and model.output_usd_per_million == 0
    )


def _prompt_manifest_hash(
    prompts: Mapping[AgentRole, AgentPromptVersionRecord],
) -> str:
    manifest = {
        role: {
            "id": str(prompts[role].id),
            "version": prompts[role].version,
            "content_hash": prompts[role].content_hash,
        }
        for role in ROLE_ORDER
    }
    return hashlib.sha256(
        json.dumps(manifest, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _authority_hash(
    *,
    request: AgentVersionEvaluationRequest,
    calendar: ExchangeCalendarAuthorityRecord,
    incumbent_prompt_hash: str,
    incumbent_model_hash: str,
    challenger_prompt_hash: str,
    challenger_model_hash: str,
    policy_hash: str,
) -> str:
    value = {
        "calendar_authority_id": str(calendar.id),
        "calendar_content_hash": calendar.content_hash,
        "challenger_model_hash": challenger_model_hash,
        "challenger_prompt_hash": challenger_prompt_hash,
        "incumbent_model_hash": incumbent_model_hash,
        "incumbent_prompt_hash": incumbent_prompt_hash,
        "initial_capital": format(request.initial_capital.normalize(), "f"),
        "instrument_id": str(request.instrument_id),
        "oos_slots": [_timestamp(slot) for slot in request.oos_slots],
        "policy_hash": policy_hash,
        "train_slots": [_timestamp(slot) for slot in request.train_slots],
    }
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def _registration(
    evaluation: AgentVersionEvaluationRecord, *, replayed: bool
) -> AgentVersionEvaluationRegistration:
    return AgentVersionEvaluationRegistration(
        evaluation_id=evaluation.id,
        incumbent_account_id=evaluation.incumbent_account_id,
        challenger_account_id=evaluation.challenger_account_id,
        authority_hash=evaluation.authority_hash,
        replayed=replayed,
    )


def _registration_from_payload(
    payload: dict[str, object], *, replayed: bool
) -> AgentVersionEvaluationRegistration:
    try:
        if payload.get("model_jobs_created") != 0 or payload.get("execution_locked") is not True:
            raise ValueError
        return AgentVersionEvaluationRegistration.model_validate({**payload, "replayed": replayed})
    except (TypeError, ValueError) as error:
        raise RuntimeError("Agent-version idempotency response is incomplete") from error


def _session(value: datetime) -> date:
    return value.astimezone(_NEW_YORK).date()


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
