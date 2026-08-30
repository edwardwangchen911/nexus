"""Real database authority paths through the restricted Strategy R&D executor."""

from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from uuid import uuid4

import exchange_calendars
import pytest
from helpers.evolution_shadow import (
    seed_complete_strategy_shadow_history,
    seed_strategy_shadow_ready,
    seed_strategy_shadow_session_evidence,
)
from nexus.backtest.authority_loader import (
    LoadedPointInTimeBacktest,
    load_point_in_time_dataset,
)
from nexus.backtest.engine import (
    BacktestBar,
    BacktestProtocol,
    authority_collection_content_hash,
    run_backtest,
)
from nexus.backtest.persistence import BacktestPersistenceAuthority, persist_backtest_result
from nexus.evolution.agent import load_strategy_rnd_prompt
from nexus.evolution.executor import build_strategy_rnd_executor
from nexus.evolution.jobs import (
    build_strategy_rnd_job_payload,
    enqueue_strategy_rnd_job,
)
from nexus.evolution.registry import activate_pending_transition, request_registry_rollback
from nexus.evolution.service import admit_experiment_family
from nexus.evolution.shadow.service import start_strategy_shadow
from nexus.evolution.worker import run_strategy_rnd_worker_once
from nexus.persistence.models.agents import (
    AgentDataSnapshotEvidenceRecord,
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
)
from nexus.persistence.models.data import QualityReport
from nexus.persistence.models.evolution import (
    StrategyCandidateValidationResultRecord,
    StrategyEvolutionPolicyRecord,
    StrategyExperimentCandidateProjectionRecord,
    StrategyExperimentFamilyRecord,
    StrategyPromotionDecisionRecord,
    StrategyRegistryHeadRecord,
    StrategyResearchLineageRecord,
)
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.persistence.models.runtime import Job
from nexus.persistence.session import session_factory
from nexus.quality.source_policy import SOURCE_POLICY_ID
from nexus.settings import EngineSettings
from nexus.strategy.dsl import baseline_feature_registry, baseline_v1_document, compile_strategy
from sqlalchemy import func, select, text

from engine.tests.integration.test_backtest_writer import (
    AUTHORITY_BUNDLE_ID,
    BACKTEST_COST_MODEL,
    COMPLETED_AT,
    DATA_SNAPSHOT_ID,
    _dataset,
    _raw_inputs,
    _seed_data_snapshot,
    _seed_upstream_authorities,
)
from engine.tests.integration.test_backtest_writer import (
    INSTRUMENT_ID as BACKTEST_INSTRUMENT_ID,
)
from engine.tests.integration.test_evolution_admission_pg import (
    INSTRUMENT_ID,
    NOW,
    RND_OUTPUT_ID,
    _seed_authority,
    _seed_bound_agent_output,
)


@pytest.fixture(autouse=True)
def _legacy_adjusted_authority_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep legacy executor mechanics covered while the current gate is closed."""
    for target in (
        "nexus.evolution.jobs.current_strategy_rnd_adjusted_authority_available",
        "nexus.evolution.authority.current_strategy_rnd_adjusted_authority_available",
        "nexus.evolution.executor.current_strategy_rnd_adjusted_authority_available",
    ):
        monkeypatch.setattr(target, lambda: True)


@pytest.mark.asyncio
async def test_real_generate_family_creates_exactly_twenty_four_bounded_candidates(
    migrated_database_url: str,
) -> None:
    settings = EngineSettings(database_url=migrated_database_url)
    factory = session_factory(settings)
    baseline = await _seed_long_generation_baseline(factory)
    generation_time = datetime.now(UTC) + timedelta(minutes=2)
    evidence_id = uuid4()
    quality_id = uuid4()
    prompt_id = uuid4()
    model_id = uuid4()
    lineage_id = uuid4()
    prompt = load_strategy_rnd_prompt()
    async with factory() as session, session.begin():
        search = await session.scalar(
            select(StrategyEvolutionPolicyRecord).where(
                StrategyEvolutionPolicyRecord.policy_kind == "evolution_search_space"
            )
        )
        assert search is not None
        session.add_all(
            (
                ResearchEvidenceRef(
                    id=evidence_id,
                    source="fixture",
                    provider_object_id="phase2-generation-evidence",
                    revision="v1",
                    available_at=COMPLETED_AT - timedelta(minutes=1),
                    location=f"memory://sha256/{'9' * 64}",
                    content_hash="9" * 64,
                    byte_length=42,
                ),
                QualityReport(
                    id=quality_id,
                    data_snapshot_id=DATA_SNAPSHOT_ID,
                    status="PASS",
                    schema_version="v1",
                    payload={},
                ),
                AgentPromptVersionRecord(
                    id=prompt_id,
                    role=prompt.role,
                    version=prompt.version,
                    content_hash=prompt.content_hash,
                    content=prompt.content,
                ),
                AgentModelConfigRecord(
                    id=model_id,
                    model_id="fixture-strategy-rnd",
                    version="fixture-v1",
                    config_hash="8" * 64,
                    max_input_tokens=10_000,
                    max_tool_calls=4,
                    max_output_tokens=1_000,
                    input_usd_per_million=Decimal("0"),
                    output_usd_per_million=Decimal("0"),
                    pricing_available_at=generation_time - timedelta(days=1),
                    pricing_expires_at=generation_time + timedelta(days=1),
                ),
                StrategyResearchLineageRecord(
                    id=lineage_id,
                    lineage_key="phase2-real-generation",
                    evolution_policy_id=search.id,
                    cumulative_oos_trial_count=0,
                    status="active",
                ),
            )
        )
        await session.flush()
        session.add(
            AgentDataSnapshotEvidenceRecord(
                data_snapshot_id=DATA_SNAPSHOT_ID,
                instrument_id=baseline.data.spy_instrument_id,
                evidence_ref_id=evidence_id,
                authority_kind="research",
                selected_at=None,
                source="fixture",
                provider_object_id="phase2-generation-evidence",
                revision="v1",
                available_at=COMPLETED_AT - timedelta(minutes=1),
                location=f"memory://sha256/{'9' * 64}",
                content_hash="9" * 64,
            )
        )
        untouched_before = await _execution_fact_counts(session)

    payload = build_strategy_rnd_job_payload(
        operation="generate_family",
        family_id=None,
        experiment_id=None,
        expected_champion_generation=1,
    )
    async with factory() as session, session.begin():
        job = await enqueue_strategy_rnd_job(session, payload=payload, effective_at=generation_time)
    worked = await run_strategy_rnd_worker_once(
        settings,
        executor=build_strategy_rnd_executor(settings),
        worker_id="strategy-rnd-real-generation-test",
        worker_started_at=generation_time,
        clock=lambda: generation_time + timedelta(seconds=1),
    )

    async with factory() as session:
        stored = await session.get(Job, job.id)
        family = await session.scalar(select(StrategyExperimentFamilyRecord))
        projection_count = int(
            await session.scalar(
                select(func.count()).select_from(StrategyExperimentCandidateProjectionRecord)
            )
            or 0
        )
        untouched_after = await _execution_fact_counts(session)

    assert worked is True
    assert stored is not None and stored.status == "succeeded", (
        None if stored is None else stored.last_error_code
    )
    assert stored.result_code == "FAMILY_GENERATED"
    assert family is not None and family.candidate_count == 24
    assert family.baseline_backtest_run_id == baseline.baseline_run_id
    assert projection_count == 24
    assert untouched_after == untouched_before == (0, 0)


@pytest.mark.asyncio
async def test_finalize_operation_builds_handover_and_activates_at_boundary(
    migrated_database_url: str,
) -> None:
    settings = EngineSettings(database_url=migrated_database_url)
    factory = session_factory(settings)
    shadow = await seed_strategy_shadow_ready(migrated_database_url, factory)
    async with factory() as session, session.begin():
        started = await start_strategy_shadow(
            session,
            experiment_id=shadow.pair.experiment_id,
            now=shadow.now,
        )
    async with factory() as session, session.begin():
        await seed_complete_strategy_shadow_history(
            session,
            group_id=started.group_id,
            calendar_sessions=shadow.calendar_sessions,
        )
        await seed_strategy_shadow_session_evidence(
            session,
            group_id=started.group_id,
            session_date=shadow.calendar_sessions[62],
            ordinal=63,
            observed_at=datetime.combine(
                shadow.calendar_sessions[62], time(23), tzinfo=UTC
            ),
        )
    finalized_at = datetime.combine(shadow.calendar_sessions[62], time(23), tzinfo=UTC)
    payload = build_strategy_rnd_job_payload(
        operation="finalize_promotion",
        family_id=shadow.pair.family_id,
        experiment_id=shadow.pair.experiment_id,
        expected_champion_generation=1,
    )
    async with factory() as session, session.begin():
        first_job = await enqueue_strategy_rnd_job(
            session, payload=payload, effective_at=finalized_at
        )

    first_worked = await run_strategy_rnd_worker_once(
        settings,
        executor=build_strategy_rnd_executor(settings),
        worker_id="strategy-rnd-handover-test",
        worker_started_at=finalized_at,
        clock=lambda: finalized_at,
    )
    async with factory() as session:
        first_stored = await session.get(Job, first_job.id)
        handover = await session.scalar(
            select(StrategyPromotionDecisionRecord).where(
                StrategyPromotionDecisionRecord.stage == "handover"
            )
        )
        head = await session.get(StrategyRegistryHeadRecord, "internal_paper")

    assert first_worked is True
    assert first_stored is not None and first_stored.status == "succeeded", (
        None if first_stored is None else first_stored.last_error_code
    )
    assert first_stored.result_code == "PROMOTION_PENDING_BOUNDARY"
    assert handover is not None and handover.status == "PASS"
    assert head is not None and head.pending_event_id is not None

    boundary_time = datetime.combine(shadow.calendar_sessions[63], time(23), tzinfo=UTC)
    async with factory() as session, session.begin():
        second_job = await enqueue_strategy_rnd_job(
            session, payload=payload, effective_at=boundary_time
        )
    second_worked = await run_strategy_rnd_worker_once(
        settings,
        executor=build_strategy_rnd_executor(settings),
        worker_id="strategy-rnd-activation-test",
        worker_started_at=boundary_time,
        clock=lambda: boundary_time,
    )
    async with factory() as session:
        second_stored = await session.get(Job, second_job.id)
        effective_head = await session.get(StrategyRegistryHeadRecord, "internal_paper")

    assert second_worked is True
    assert second_stored is not None and second_stored.status == "succeeded", (
        None if second_stored is None else second_stored.last_error_code
    )
    assert second_stored.result_code == "REGISTRY_TRANSITION_EFFECTIVE"
    assert effective_head is not None
    assert effective_head.active_generation == 2
    assert effective_head.active_definition_id == shadow.pair.challenger_definition_id

    rollback_session = shadow.calendar_sessions[64]
    rollback_time = datetime.combine(rollback_session, time(23), tzinfo=UTC)
    async with factory() as session, session.begin():
        rollback_pending = await request_registry_rollback(
            session,
            expected_generation=effective_head.active_generation,
            expected_version=effective_head.version,
            effective_session=rollback_session,
            idempotency_key="strategy-rnd-real-executor-rollback",
            reason_code="USER_REQUESTED",
            now=rollback_time,
        )
    async with factory() as session, session.begin():
        rolled_back = await activate_pending_transition(
            session,
            expected_generation=rollback_pending.active_generation,
            expected_version=rollback_pending.version,
            decision_session=rollback_session,
            now=rollback_time,
        )

    assert rollback_pending.reason_code == "ROLLBACK_PENDING_BOUNDARY"
    assert rolled_back.reason_code == "REGISTRY_TRANSITION_EFFECTIVE"
    assert rolled_back.active_generation == 3
    assert rolled_back.active_definition_id == shadow.pair.champion_definition_id
    assert effective_head.pending_event_id is None


@pytest.mark.asyncio
async def test_real_executor_loads_and_fails_closed_on_incomplete_validation_windows(
    migrated_database_url: str,
) -> None:
    settings = EngineSettings(database_url=migrated_database_url)
    factory = session_factory(settings)
    strategy, bars, sessions, protocol = _raw_inputs()
    price_authorities = await _seed_upstream_authorities(factory, bars, sessions)
    data = _dataset(bars, sessions, price_authorities)
    baseline_result = run_backtest(strategy, data, protocol)
    await _seed_data_snapshot(factory, data, protocol, baseline_result)
    async with factory() as session, session.begin():
        baseline = await persist_backtest_result(
            session,
            strategy,
            data,
            protocol,
            baseline_result,
            BacktestPersistenceAuthority(
                authority_bundle_id=AUTHORITY_BUNDLE_ID,
                data_snapshot_id=DATA_SNAPSHOT_ID,
                source_policy_id=SOURCE_POLICY_ID,
                cost_policy_id=data.authority.cost_policy_id,
                completed_at=COMPLETED_AT,
            ),
        )

    proposal, original_authority = await _seed_authority(factory, bind_agent_output=False)
    quality_id = uuid4()
    executor_evidence_id = uuid4()
    proposal = proposal.model_copy(update={"evidence_refs": (executor_evidence_id,)})
    async with factory() as session, session.begin():
        session.add(
            ResearchEvidenceRef(
                id=executor_evidence_id,
                source="fixture",
                provider_object_id="phase2-executor-evidence",
                revision="v1",
                available_at=COMPLETED_AT - timedelta(minutes=1),
                location=f"memory://sha256/{'f' * 64}",
                content_hash="f" * 64,
                byte_length=42,
            )
        )
        await session.flush()
        session.add_all(
            [
                QualityReport(
                    id=quality_id,
                    data_snapshot_id=DATA_SNAPSHOT_ID,
                    status="PASS",
                    schema_version="v1",
                    payload={},
                ),
                AgentDataSnapshotEvidenceRecord(
                    data_snapshot_id=DATA_SNAPSHOT_ID,
                    instrument_id=INSTRUMENT_ID,
                    evidence_ref_id=executor_evidence_id,
                    authority_kind="research",
                    selected_at=None,
                    source="fixture",
                    provider_object_id="phase2-executor-evidence",
                    revision="v1",
                    available_at=COMPLETED_AT - timedelta(minutes=1),
                    location=f"memory://sha256/{'f' * 64}",
                    content_hash="f" * 64,
                ),
            ]
        )
    await _seed_bound_agent_output(
        factory,
        parent_id=proposal.parent_strategy_definition_id,
        proposal=proposal,
        data_snapshot_id=DATA_SNAPSHOT_ID,
    )
    authority = original_authority.model_copy(
        update={
            "data_snapshot_id": DATA_SNAPSHOT_ID,
            "quality_report_id": quality_id,
            "authority_hash": baseline.authority_hash,
            "protocol_hash": baseline.protocol_hash,
            "code_hash": baseline.code_hash,
            "baseline_backtest_run_id": baseline.id,
            "rnd_output_id": RND_OUTPUT_ID,
        }
    )
    async with factory() as session, session.begin():
        family = await admit_experiment_family(
            session,
            proposal=proposal,
            authority=authority,
            now=NOW,
            idempotency_key="phase2-real-executor-validation",
        )
        projection = await session.scalar(
            select(StrategyExperimentCandidateProjectionRecord)
            .where(StrategyExperimentCandidateProjectionRecord.family_id == family.family_id)
            .order_by(StrategyExperimentCandidateProjectionRecord.experiment_id)
            .limit(1)
        )
        assert projection is not None
        payload = build_strategy_rnd_job_payload(
            operation="evaluate_candidate",
            family_id=family.family_id,
            experiment_id=projection.experiment_id,
            expected_champion_generation=family.champion_generation,
            candidate_state=projection.current_state,
            candidate_version=projection.version,
        )
        job = await enqueue_strategy_rnd_job(
            session, payload=payload, effective_at=NOW + timedelta(seconds=1)
        )
        untouched_before = await _execution_fact_counts(session)

    execution_time = baseline.authority_frozen_at + timedelta(seconds=2)
    worked = await run_strategy_rnd_worker_once(
        settings,
        executor=build_strategy_rnd_executor(settings),
        worker_id="strategy-rnd-real-executor-test",
        worker_started_at=NOW,
        clock=lambda: execution_time,
    )

    async with factory() as session:
        stored = await session.get(Job, job.id)
        validation = await session.scalar(
            select(StrategyCandidateValidationResultRecord).where(
                StrategyCandidateValidationResultRecord.experiment_id == projection.experiment_id
            )
        )
        refreshed = await session.get(
            StrategyExperimentCandidateProjectionRecord, projection.experiment_id
        )
        untouched_after = await _execution_fact_counts(session)

    assert worked is True
    assert stored is not None and stored.status == "succeeded", (
        None if stored is None else stored.status,
        None if stored is None else stored.last_error_code,
    )
    assert stored.result_code == "CANDIDATE_VALIDATION_INCOMPLETE"
    assert validation is not None and validation.authority_complete is False
    assert refreshed is not None and refreshed.current_state == "VALIDATION_FAILED"
    assert untouched_after == untouched_before == (0, 0)


async def _execution_fact_counts(session) -> tuple[int, int]:
    paper = int(await session.scalar(select(func.count()).select_from(text("paper_orders"))) or 0)
    external = int(
        await session.scalar(select(func.count()).select_from(text("broker_order_snapshots"))) or 0
    )
    return paper, external


async def _seed_long_generation_baseline(factory) -> LoadedPointInTimeBacktest:
    calendar = exchange_calendars.get_calendar("XNYS")
    sessions = tuple(
        value.date() for value in calendar.sessions_in_range(date(2014, 1, 2), date(2025, 12, 31))
    )
    bars = tuple(
        BacktestBar(
            instrument_id=BACKTEST_INSTRUMENT_ID,
            session=session,
            open=Decimal("10"),
            high=Decimal("10"),
            low=Decimal("10"),
            close=Decimal("10"),
            volume=Decimal("7000000"),
            observed_at=calendar.session_close(session).to_pydatetime(),
            available_at=calendar.session_close(session).to_pydatetime(),
            received_at=calendar.session_close(session).to_pydatetime(),
        )
        for session in sessions
    )
    strategy = compile_strategy(baseline_v1_document(), baseline_feature_registry())
    protocol = BacktestProtocol(
        start=sessions[200],
        end=sessions[-1],
        initial_nav=Decimal("10000"),
        cost_model=BACKTEST_COST_MODEL,
        walk_forward=True,
    )
    price_authorities = await _seed_upstream_authorities(factory, bars, sessions)
    data = _dataset(bars, sessions, price_authorities)
    collection_updates = {}
    for field in (
        "universe_collection",
        "sector_collection",
        "corporate_action_collection",
    ):
        collection = getattr(data.authority, field)
        collection_updates[field] = collection.model_copy(
            update={
                "range_start": protocol.start,
                "range_end": protocol.end,
                "content_hash": authority_collection_content_hash(
                    kind=collection.kind,
                    source=collection.source,
                    range_start=protocol.start,
                    range_end=protocol.end,
                    query_parameters=collection.query_parameters,
                    member_ids=collection.member_ids,
                ),
            }
        )
    data = data.model_copy(
        update={
            "authority": data.authority.model_copy(update=collection_updates),
        }
    )
    result = run_backtest(strategy, data, protocol)
    assert result.strategy_data_complete, result.strategy_incomplete_reasons
    await _seed_data_snapshot(factory, data, protocol, result)
    async with factory() as session, session.begin():
        record = await persist_backtest_result(
            session,
            strategy,
            data,
            protocol,
            result,
            BacktestPersistenceAuthority(
                authority_bundle_id=AUTHORITY_BUNDLE_ID,
                data_snapshot_id=DATA_SNAPSHOT_ID,
                source_policy_id=SOURCE_POLICY_ID,
                cost_policy_id=data.authority.cost_policy_id,
                completed_at=COMPLETED_AT,
            ),
        )
        run_id = record.id
        as_of = record.authority_frozen_at
    async with factory() as session:
        return await load_point_in_time_dataset(session, run_id, as_of)
