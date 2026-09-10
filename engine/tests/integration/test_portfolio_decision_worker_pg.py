"""Real-PostgreSQL proof for the dedicated portfolio decision worker."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.agents.portfolio_decision_contracts import (
    AgentScenarioProjectionV1,
    PortfolioDecisionInputV1,
    PortfolioDecisionModelContextV1,
)
from nexus.agents.portfolio_decision_execution import (
    PortfolioDecisionExecutionAuthority,
    PortfolioDecisionExecutionError,
)
from nexus.agents.portfolio_decision_persistence import admit_portfolio_decision_run
from nexus.agents.portfolio_decision_tools import PortfolioCitationAuthority
from nexus.agents.portfolio_decision_worker import (
    reconcile_orphaned_portfolio_decisions,
)
from nexus.agents.portfolio_decision_worker import (
    run_portfolio_decision_worker_once as run_core_worker_once,
)
from nexus.agents.responses import ResponsesModelConfig
from nexus.diagnostics.models import PortfolioDiagnosticReport
from nexus.diagnostics.policy import (
    REAL_ADVISORY_POLICY_ID,
    REAL_ADVISORY_POLICY_VERSION,
    RealAdvisoryPolicy,
)
from nexus.optimizer.scenario_contracts import (
    AdjustmentScenarioConstraintsV1,
    PortfolioScenarioRequestV1,
)
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    PortfolioDecisionBudgetReservationRecord,
    PortfolioDecisionOutputRecord,
    PortfolioDecisionRunRecord,
)
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.portfolio import (
    Account,
    BrokerSyncRun,
    Portfolio,
    PortfolioSnapshotRecord,
)
from nexus.persistence.models.runtime import Job, JobAttempt, JobDependency
from nexus.persistence.session import session_factory
from nexus.providers.broker.base import PortfolioSnapshot
from nexus.settings import EngineSettings
from nexus.worker import run_portfolio_decision_worker_once
from sqlalchemy import func, select, text

NOW = datetime(2026, 9, 10, 8, tzinfo=UTC)
PORTFOLIO_ID = UUID("0198d500-2000-7000-8000-000000002001")
SNAPSHOT_ID = UUID("0198d500-2000-7000-8000-000000002002")
REPORT_ID = UUID("0198d500-2000-7000-8000-000000002003")
PROMPT_ID = UUID("0198d500-2000-7000-8000-000000002004")
MODEL_ID = UUID("0198d500-2000-7000-8000-000000002005")
RESULT_HASH = "e" * 64


@pytest.mark.asyncio
async def test_worker_completes_two_turn_decision_without_a_long_transaction(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = EngineSettings(database_url=migrated_database_url)
    factory = session_factory(settings)
    await _seed_authority(factory)
    await _admit(factory, monkeypatch)
    async with factory() as session:
        run = await session.scalar(select(PortfolioDecisionRunRecord))
        assert run is not None
        authority = await _authority(session, run)
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_worker.load_portfolio_decision_execution_authority",
        AsyncMock(return_value=authority),
    )
    client = _Client(_intent(authority), factory)

    worked = await run_portfolio_decision_worker_once(
        settings,
        responses_client=client,
    )

    async with factory() as session:
        stored_run = await session.get(PortfolioDecisionRunRecord, run.id)
        output = await session.scalar(
            select(PortfolioDecisionOutputRecord).where(
                PortfolioDecisionOutputRecord.run_id == run.id
            )
        )
        reservation = await session.scalar(
            select(PortfolioDecisionBudgetReservationRecord).where(
                PortfolioDecisionBudgetReservationRecord.run_id == run.id
            )
        )
        job = await session.scalar(select(Job).where(Job.job_type == "agent.portfolio_decision"))
        attempt = await session.scalar(select(JobAttempt).where(JobAttempt.job_id == job.id))
    assert worked is True
    assert len(client.responses.calls) == 2
    assert stored_run is not None
    assert (stored_run.status, stored_run.provider_phase) == ("completed", "second_requested")
    assert output is not None and output.status == "completed"
    assert reservation is not None
    assert (reservation.state, reservation.settled_usd) == (
        "settled",
        Decimal("0.000250000000"),
    )
    assert job is not None and (job.status, job.result_code) == (
        "succeeded",
        "PORTFOLIO_DECISION_COMPLETED",
    )
    assert attempt is not None and attempt.outcome == "succeeded"


@pytest.mark.asyncio
@pytest.mark.parametrize("client_type", ("incompatible", "raising"))
async def test_worker_rejects_an_incompatible_transport_before_provider_call(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
    client_type: str,
) -> None:
    settings = EngineSettings(database_url=migrated_database_url)
    factory = session_factory(settings)
    await _seed_authority(factory)
    await _admit(factory, monkeypatch)
    async with factory() as session:
        run = await session.scalar(select(PortfolioDecisionRunRecord))
        assert run is not None
        authority = await _authority(session, run)
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_worker.load_portfolio_decision_execution_authority",
        AsyncMock(return_value=authority),
    )
    client = (
        _IncompatibleClient(_intent(authority), factory)
        if client_type == "incompatible"
        else _RaisingCompatibilityClient(_intent(authority), factory)
    )

    assert await run_portfolio_decision_worker_once(settings, responses_client=client) is True

    async with factory() as session:
        stored = await session.get(PortfolioDecisionRunRecord, run.id)
        reservation = await session.scalar(
            select(PortfolioDecisionBudgetReservationRecord).where(
                PortfolioDecisionBudgetReservationRecord.run_id == run.id
            )
        )
    assert stored is not None
    assert (stored.status, stored.provider_phase) == ("failed", "none")
    assert reservation is not None and reservation.state == "released"
    assert client.responses.calls == []


@pytest.mark.asyncio
async def test_worker_rejects_an_unbound_transport_before_provider_call(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = EngineSettings(database_url=migrated_database_url)
    factory = session_factory(settings)
    await _seed_authority(factory)
    await _admit(factory, monkeypatch)
    async with factory() as session:
        run = await session.scalar(select(PortfolioDecisionRunRecord))
        assert run is not None
        authority = await _authority(session, run)
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_worker.load_portfolio_decision_execution_authority",
        AsyncMock(return_value=authority),
    )
    client = _UnboundClient(_intent(authority), factory)

    assert await run_portfolio_decision_worker_once(settings, responses_client=client) is True

    async with factory() as session:
        stored = await session.get(PortfolioDecisionRunRecord, run.id)
    assert stored is not None
    assert (stored.status, stored.provider_phase) == ("failed", "none")
    assert client.responses.calls == []


@pytest.mark.asyncio
async def test_reconcile_after_output_checkpoint_crash_never_replays_provider(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    await _admit(factory, monkeypatch)
    async with factory() as session:
        run = await session.scalar(select(PortfolioDecisionRunRecord))
        assert run is not None
        authority = await _authority(session, run)
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_worker.load_portfolio_decision_execution_authority",
        AsyncMock(return_value=authority),
    )
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_worker.finalize_job",
        AsyncMock(side_effect=RuntimeError("injected output checkpoint crash")),
    )
    client = _Client(_intent(authority), factory)

    with pytest.raises(RuntimeError, match="output checkpoint crash"):
        await run_core_worker_once(
            factory,
            responses_client=client,
            worker_id="portfolio-decision-crash",
            worker_started_at=NOW,
            process_version="test-worker-v1",
            migration_head="0049_pd_terminal_phases",
            clock=lambda: NOW,
        )
    assert len(client.responses.calls) == 2

    from nexus.jobs.models import QueueName
    from nexus.jobs.repository import reclaim_expired_jobs

    async with factory() as session, session.begin():
        reclaimed = await reclaim_expired_jobs(
            session,
            queue=QueueName.GENERAL,
            now=NOW + timedelta(seconds=121),
        )
        reconciled = await reconcile_orphaned_portfolio_decisions(
            session, now=NOW + timedelta(seconds=121)
        )

    async with factory() as session:
        stored_run = await session.get(PortfolioDecisionRunRecord, run.id)
        job = await session.scalar(select(Job).where(Job.job_type == "agent.portfolio_decision"))
        output = await session.scalar(
            select(PortfolioDecisionOutputRecord).where(
                PortfolioDecisionOutputRecord.run_id == run.id
            )
        )
        reservation = await session.scalar(
            select(PortfolioDecisionBudgetReservationRecord).where(
                PortfolioDecisionBudgetReservationRecord.run_id == run.id
            )
        )
        output_count = await session.scalar(
            select(func.count()).select_from(PortfolioDecisionOutputRecord)
        )
    assert (reclaimed, reconciled, output_count) == (1, 1, 1)
    assert len(client.responses.calls) == 2
    assert stored_run is not None and stored_run.status == "failed"
    assert job is not None and job.status == "dead_letter"
    assert output is not None and output.details == {
        "error_code": "PORTFOLIO_DECISION_PERSISTENCE_UNCERTAIN",
        "execution_locked": True,
    }
    assert reservation is not None
    assert (reservation.state, reservation.settled_usd) == (
        "settled",
        reservation.reserved_usd,
    )


@pytest.mark.asyncio
async def test_worker_abstains_immediately_when_first_turn_omits_tool(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    await _admit(factory, monkeypatch)
    async with factory() as session:
        run = await session.scalar(select(PortfolioDecisionRunRecord))
        assert run is not None
        authority = await _authority(session, run)
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_worker.load_portfolio_decision_execution_authority",
        AsyncMock(return_value=authority),
    )
    client = _Client(_intent(authority), factory, mode="no_tool")

    worked = await run_core_worker_once(
        factory,
        responses_client=client,
        worker_id="portfolio-decision-abstain",
        worker_started_at=NOW,
        process_version="test-worker-v1",
        migration_head="0049_pd_terminal_phases",
        clock=lambda: NOW,
    )

    async with factory() as session:
        stored_run = await session.get(PortfolioDecisionRunRecord, run.id)
        job = await session.scalar(select(Job).where(Job.job_type == "agent.portfolio_decision"))
        output = await session.scalar(select(PortfolioDecisionOutputRecord))
        reservation = await session.scalar(select(PortfolioDecisionBudgetReservationRecord))
    assert worked is True and len(client.responses.calls) == 1
    assert stored_run is not None
    assert (stored_run.status, stored_run.provider_phase) == ("abstained", "first_requested")
    assert job is not None and (job.status, job.result_code) == (
        "skipped",
        "PORTFOLIO_DECISION_ABSTAINED",
    )
    assert output is not None and output.details == {
        "error_code": "PORTFOLIO_DECISION_TOOL_REQUIRED",
        "execution_locked": True,
    }
    assert reservation is not None
    assert (reservation.state, reservation.settled_usd) == (
        "settled",
        Decimal("0.000150000000"),
    )


@pytest.mark.asyncio
async def test_worker_fails_once_and_settles_full_when_transport_usage_is_unknown(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    await _admit(factory, monkeypatch)
    async with factory() as session:
        run = await session.scalar(select(PortfolioDecisionRunRecord))
        assert run is not None
        authority = await _authority(session, run)
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_worker.load_portfolio_decision_execution_authority",
        AsyncMock(return_value=authority),
    )
    client = _Client(_intent(authority), factory, mode="transport_error")

    worked = await run_core_worker_once(
        factory,
        responses_client=client,
        worker_id="portfolio-decision-failed",
        worker_started_at=NOW,
        process_version="test-worker-v1",
        migration_head="0049_pd_terminal_phases",
        clock=lambda: NOW,
    )

    async with factory() as session:
        stored_run = await session.get(PortfolioDecisionRunRecord, run.id)
        job = await session.scalar(select(Job).where(Job.job_type == "agent.portfolio_decision"))
        output = await session.scalar(select(PortfolioDecisionOutputRecord))
        reservation = await session.scalar(select(PortfolioDecisionBudgetReservationRecord))
    assert worked is True and len(client.responses.calls) == 1
    assert stored_run is not None and stored_run.status == "failed"
    assert job is not None and (job.status, job.last_error_code) == (
        "failed",
        "PORTFOLIO_DECISION_MODEL_FAILED",
    )
    assert output is not None and output.status == "failed"
    assert reservation is not None
    assert (reservation.state, reservation.settled_usd) == (
        "settled",
        reservation.reserved_usd,
    )


@pytest.mark.asyncio
async def test_worker_terminalizes_invalid_authority_without_calling_provider(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    await _admit(factory, monkeypatch)
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_worker.load_portfolio_decision_execution_authority",
        AsyncMock(
            side_effect=PortfolioDecisionExecutionError("PORTFOLIO_DECISION_AUTHORITY_INVALID")
        ),
    )
    client = _Client({}, factory)

    worked = await run_core_worker_once(
        factory,
        responses_client=client,
        worker_id="portfolio-decision-invalid-authority",
        worker_started_at=NOW,
        process_version="test-worker-v1",
        migration_head="0049_pd_terminal_phases",
        clock=lambda: NOW,
    )

    async with factory() as session:
        run = await session.scalar(select(PortfolioDecisionRunRecord))
        job = await session.scalar(select(Job).where(Job.job_type == "agent.portfolio_decision"))
        reservation = await session.scalar(select(PortfolioDecisionBudgetReservationRecord))
    assert worked is True and client.responses.calls == []
    assert run is not None and (run.status, run.provider_phase) == ("failed", "none")
    assert job is not None and (job.status, job.last_error_code) == (
        "failed",
        "PORTFOLIO_DECISION_AUTHORITY_INVALID",
    )
    assert reservation is not None and reservation.state == "released"


@pytest.mark.asyncio
async def test_worker_skips_failed_dependency_without_calling_provider(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    await _admit(factory, monkeypatch)
    async with factory() as session, session.begin():
        child = await session.scalar(select(Job).where(Job.job_type == "agent.portfolio_decision"))
        assert child is not None
        parent = Job(
            job_key="test:failed-parent",
            input_hash="9" * 64,
            status="failed",
            schema_version="test_v1",
            payload={},
            job_type="daily.pipeline",
            queue="general",
            scope_kind="global",
            scope_id="test",
            effective_at=NOW,
            priority=280,
            retry_policy="never",
            max_attempts=1,
            attempt_count=1,
            manual_retry_count=0,
            available_at=NOW,
            started_at=NOW,
            finished_at=NOW,
            last_error_code="TEST_PARENT_FAILED",
            result_code=None,
            version=2,
        )
        session.add(parent)
        await session.flush()
        session.add(
            JobDependency(
                job_id=child.id,
                depends_on_job_id=parent.id,
                required_outcome="succeeded",
            )
        )
    client = _Client({}, factory)

    worked = await run_core_worker_once(
        factory,
        responses_client=client,
        worker_id="portfolio-decision-dependency",
        worker_started_at=NOW,
        process_version="test-worker-v1",
        migration_head="0049_pd_terminal_phases",
        clock=lambda: NOW,
    )

    async with factory() as session:
        run = await session.scalar(select(PortfolioDecisionRunRecord))
        child = await session.scalar(select(Job).where(Job.job_type == "agent.portfolio_decision"))
        reservation = await session.scalar(select(PortfolioDecisionBudgetReservationRecord))
    assert worked is True and client.responses.calls == []
    assert run is not None and run.status == "failed"
    assert child is not None and (child.status, child.result_code) == (
        "skipped",
        "DEPENDENCY_NOT_SATISFIED",
    )
    assert reservation is not None and reservation.state == "released"


@pytest.mark.asyncio
async def test_reconcile_limit_applies_only_to_nonterminal_runs(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    await _admit(factory, monkeypatch)
    await _admit(
        factory,
        monkeypatch,
        actor_id="fixture-actor-2",
        idempotency_key="worker-orphan",
        request_hash="b" * 64,
        now=NOW + timedelta(seconds=2),
    )
    async with factory() as session, session.begin():
        await session.execute(text("SET LOCAL session_replication_role = replica"))
        runs = tuple(
            await session.scalars(
                select(PortfolioDecisionRunRecord).order_by(
                    PortfolioDecisionRunRecord.decision_time
                )
            )
        )
        jobs = tuple(
            await session.scalars(
                select(Job)
                .where(Job.job_type == "agent.portfolio_decision")
                .order_by(Job.effective_at)
            )
        )
        assert len(runs) == len(jobs) == 2
        settled_run, orphan_run = runs
        settled_job, orphan_job = jobs
        settled_run.status = "failed"
        for job, finished_at in (
            (settled_job, NOW),
            (orphan_job, NOW + timedelta(seconds=2)),
        ):
            job.status = "failed"
            job.attempt_count = 1
            job.started_at = finished_at
            job.finished_at = finished_at
            job.last_error_code = "PORTFOLIO_DECISION_PERSISTENCE_UNCERTAIN"
            job.version += 1
        session.add_all(
            Job(
                job_key=f"test:settled:{index}",
                input_hash=f"{index % 16:x}" * 64,
                status="failed",
                schema_version="portfolio_decision_job_v1",
                payload={"run_id": str(settled_run.id)},
                job_type="agent.portfolio_decision",
                queue="general",
                scope_kind="portfolio",
                scope_id=str(PORTFOLIO_ID),
                effective_at=NOW,
                priority=180,
                retry_policy="never",
                max_attempts=1,
                attempt_count=1,
                manual_retry_count=0,
                available_at=NOW,
                started_at=NOW,
                finished_at=NOW,
                last_error_code="PORTFOLIO_DECISION_PERSISTENCE_UNCERTAIN",
                result_code=None,
                version=2,
            )
            for index in range(99)
        )

    async with factory() as session, session.begin():
        reconciled = await reconcile_orphaned_portfolio_decisions(
            session,
            now=NOW + timedelta(seconds=3),
            limit=100,
        )

    async with factory() as session:
        stored_orphan = await session.get(PortfolioDecisionRunRecord, orphan_run.id)
    assert reconciled == 1
    assert stored_orphan is not None and stored_orphan.status == "failed"


async def _seed_authority(factory) -> None:
    account_id = UUID("0198d500-2000-7000-8000-000000002011")
    sync_id = UUID("0198d500-2000-7000-8000-000000002012")
    prompt = "bounded portfolio decision prompt"
    async with factory() as session, session.begin():
        session.add_all(
            (
                Account(id=account_id, provider="fosun", status="ACTIVE", created_at=NOW),
                Portfolio(id=PORTFOLIO_ID, account_id=account_id, name="fixture", created_at=NOW),
                AgentPromptVersionRecord(
                    id=PROMPT_ID,
                    role="portfolio_decision",
                    version="portfolio-decision-v1",
                    content_hash=hashlib.sha256(prompt.encode()).hexdigest(),
                    content=prompt,
                    created_at=NOW - timedelta(minutes=2),
                ),
                AgentModelConfigRecord(
                    id=MODEL_ID,
                    model_id="fixture-model",
                    version="fixture-portfolio-v1",
                    config_hash="d" * 64,
                    max_input_tokens=100_000,
                    max_output_tokens=1_000,
                    max_tool_calls=1,
                    input_usd_per_million=Decimal("1"),
                    output_usd_per_million=Decimal("1"),
                    pricing_available_at=NOW - timedelta(days=1),
                    pricing_expires_at=NOW + timedelta(days=1),
                    created_at=NOW - timedelta(minutes=2),
                ),
            )
        )
        await session.flush()
        session.add(
            BrokerSyncRun(
                id=sync_id,
                account_id=account_id,
                portfolio_id=PORTFOLIO_ID,
                trigger_kind="MANUAL",
                actor_id="fixture",
                route="fixture",
                idempotency_key="fixture",
                request_hash="c" * 64,
                status="READY",
                accepted_at=NOW - timedelta(minutes=2),
                completed_at=NOW - timedelta(minutes=1),
                attempt_count=1,
                failure_reason=None,
                cash_flow_quality="PASS",
                created_at=NOW - timedelta(minutes=2),
            )
        )
        await session.flush()
        session.add(
            PortfolioSnapshotRecord(
                id=SNAPSHOT_ID,
                portfolio_id=PORTFOLIO_ID,
                sync_run_id=sync_id,
                stale_from_snapshot_id=None,
                status="READY",
                as_of=NOW - timedelta(minutes=1),
                position_hash_version="broker_position_hash_v1",
                position_hash="4" * 64,
                order_hash_version="broker_order_hash_v1",
                order_hash="5" * 64,
                source_request_ids=[],
                source_observations=[],
                created_at=NOW - timedelta(minutes=1),
            )
        )
        await session.flush()
        session.add(
            PortfolioDiagnosticReportRecord(
                id=REPORT_ID,
                portfolio_id=PORTFOLIO_ID,
                portfolio_snapshot_id=SNAPSHOT_ID,
                snapshot_as_of=NOW - timedelta(minutes=1),
                trading_day=NOW.date(),
                policy_id=REAL_ADVISORY_POLICY_ID,
                policy_version=REAL_ADVISORY_POLICY_VERSION,
                optimization_need="NO_ACTION",
                diagnostic_input_hash="f" * 64,
                created_at=NOW - timedelta(seconds=30),
            )
        )


async def _admit(
    factory,
    monkeypatch: pytest.MonkeyPatch,
    *,
    actor_id: str = "fixture-actor",
    idempotency_key: str = "worker-success",
    request_hash: str = "a" * 64,
    now: datetime = NOW,
) -> None:
    snapshot = PortfolioSnapshot(
        id=SNAPSHOT_ID,
        portfolio_id=PORTFOLIO_ID,
        status="READY",
        cash=(),
        positions=(),
        open_orders=(),
        as_of=NOW - timedelta(minutes=1),
        stale_from_snapshot_id=None,
        source_request_ids=(),
    )
    report = PortfolioDiagnosticReport(
        portfolio_snapshot_id=SNAPSHOT_ID,
        snapshot_as_of=NOW - timedelta(minutes=1),
        trading_day=NOW.date(),
        policy_version=REAL_ADVISORY_POLICY_VERSION,
        optimization_need="NO_ACTION",
        items=(),
    )
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_persistence.load_frozen_snapshot",
        AsyncMock(return_value=snapshot),
    )
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_persistence.read_report",
        AsyncMock(return_value=report),
    )
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_persistence.load_instrument_identities",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_persistence.load_real_advisory_policy_v1",
        AsyncMock(return_value=RealAdvisoryPolicy.v1()),
    )
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_persistence.evaluate_adjustment_scenario",
        lambda **_: type("Evaluation", (), {"result_hash": RESULT_HASH})(),
    )
    request = PortfolioScenarioRequestV1(
        schema_version="portfolio_scenario_request_v1",
        portfolio_snapshot_id=SNAPSHOT_ID,
        diagnostic_report_id=REPORT_ID,
        as_of=NOW - timedelta(minutes=1),
        constraints=AdjustmentScenarioConstraintsV1(
            schema_version="adjustment_scenario_constraints_v1",
            objective="reduce_security_concentration",
            max_turnover=Decimal("0.10"),
            minimum_cash=Decimal("0.10"),
            universe_policy="current_holdings_only",
            allow_new_positions=False,
        ),
    )
    async with factory() as session, session.begin():
        await admit_portfolio_decision_run(
            session,
            actor_id=actor_id,
            route="/internal/v1/portfolio-decisions",
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            request=request,
            expected_scenario_result_hash=RESULT_HASH,
            prompt_version_id=PROMPT_ID,
            model_config_id=MODEL_ID,
            daily_limit_usd=Decimal("1"),
            now=now,
        )


async def _authority(
    session, run: PortfolioDecisionRunRecord
) -> PortfolioDecisionExecutionAuthority:
    frozen = run.frozen_input
    citations = PortfolioCitationAuthority.from_manifest(
        run_id=run.id,
        manifest=run.evidence_manifest,
        evidence_hash=run.evidence_hash,
        decision_time=run.decision_time,
    )
    citation_id = citations.citation_ids[0]
    context = PortfolioDecisionModelContextV1.model_validate(
        {
            "schema_version": "portfolio_decision_model_context_v1",
            "run_id": str(run.id),
            "portfolio_id": str(run.portfolio_id),
            "portfolio_snapshot_id": str(run.portfolio_snapshot_id),
            "diagnostic_report_id": str(run.diagnostic_report_id),
            "decision_time": run.decision_time.isoformat(),
            "objective": frozen["objective"],
            "constraints": {
                "schema_version": "adjustment_scenario_constraints_v1",
                "objective": frozen["objective"],
                "max_turnover": frozen["max_turnover"],
                "minimum_cash": frozen["minimum_cash"],
                "universe_policy": frozen["universe_policy"],
                "allow_new_positions": frozen["allow_new_positions"],
            },
            "current_diagnostics": [
                {
                    "code": code,
                    "severity": "warning",
                    "metric": _known(value),
                    "unit": "ratio",
                    "citation_ids": [citation_id],
                }
                for code, value in (
                    ("CONCENTRATION_TOP1", "0.4"),
                    ("CONCENTRATION_TOP3", "0.8"),
                    ("CONCENTRATION_HHI", "0.22"),
                )
            ],
            "citation_ids": [citation_id],
            "execution_locked": True,
        }
    )
    projection = _projection(citation_id)
    model = await session.get(AgentModelConfigRecord, run.model_config_id)
    prompt = await session.get(AgentPromptVersionRecord, run.prompt_version_id)
    assert model is not None and prompt is not None
    return PortfolioDecisionExecutionAuthority(
        frozen=PortfolioDecisionInputV1.model_validate(frozen),
        context=context,
        prompt=prompt.content,
        model=model,
        model_config=ResponsesModelConfig(
            model_id=model.model_id,
            pricing_version=model.version,
            max_input_tokens=model.max_input_tokens,
            max_output_tokens=model.max_output_tokens,
            max_tool_calls=1,
            absolute_timeout_seconds=Decimal("45"),
            input_usd_per_million=model.input_usd_per_million,
            output_usd_per_million=model.output_usd_per_million,
        ),
        tools=_Tools(projection),
    )


def _known(value: str) -> dict[str, object]:
    return {"status": "known", "value": value, "reason": None}


def _projection(citation_id: str) -> AgentScenarioProjectionV1:
    unknown = {"status": "unknown", "value": None, "reason": "COST_EVIDENCE_INCOMPLETE"}
    metrics = {
        "security_top1": _known("0.4"),
        "security_top3": _known("0.8"),
        "security_hhi": _known("0.22"),
        "cash_weight": _known("0.1"),
        "one_way_turnover": _known("0.05"),
        "estimated_cost": unknown,
    }
    return AgentScenarioProjectionV1.model_validate(
        {
            "schema_version": "agent_scenario_projection_v1",
            "objective": "reduce_security_concentration",
            "objective_status": "improved_not_met",
            "analysis_status": "partial",
            "baseline": metrics,
            "candidate": metrics,
            "unknown_metrics": ["estimated_cost"],
            "hard_constraint_reasons": [],
            "changed_position_count": 1,
            "result_hash": RESULT_HASH,
            "citation_ids": [citation_id],
            "execution_locked": True,
        }
    )


def _intent(authority: PortfolioDecisionExecutionAuthority) -> dict[str, object]:
    citation_id = authority.context.citation_ids[0]
    return {
        "schema_version": "portfolio_research_intent_v1",
        "run_id": str(authority.context.run_id),
        "portfolio_id": str(authority.context.portfolio_id),
        "portfolio_snapshot_id": str(authority.context.portfolio_snapshot_id),
        "diagnostic_report_id": str(authority.context.diagnostic_report_id),
        "decision_time": authority.context.decision_time.isoformat(),
        "assessment": "explore_adjustment",
        "objective": authority.context.objective,
        "horizon": "medium_term",
        "confidence": "medium",
        "rationale": [{"text": "Concentration improves.", "citation_ids": [citation_id]}],
        "invalidation_conditions": [
            {
                "signal": "portfolio.scenario_objective_status",
                "operator": "eq",
                "threshold": "unchanged",
                "unit": "state",
                "citation_ids": [citation_id],
            }
        ],
        "limitations": [{"code": "estimated_cost", "text": "Estimated cost remains unknown."}],
        "scenario_result_hash": RESULT_HASH,
        "citation_ids": [citation_id],
        "execution_locked": True,
    }


class _Tools:
    names = ("evaluate_adjustment_scenario",)

    def __init__(self, projection: AgentScenarioProjectionV1) -> None:
        self._projection = projection

    async def execute(self, name: str, payload: dict[str, object]) -> dict[str, object]:
        assert name == self.names[0]
        assert payload == {"objective": "reduce_security_concentration"}
        return self._projection.model_dump(mode="json")


class _Responses:
    def __init__(self, intent: dict[str, object], factory, *, mode: str = "success") -> None:
        self.intent = intent
        self.factory = factory
        self.mode = mode
        self.calls: list[dict[str, object]] = []

    async def create(self, **kwargs: object) -> dict[str, object]:
        async with self.factory() as session, session.begin():
            await session.execute(
                text("LOCK TABLE portfolio_decision_runs IN ACCESS EXCLUSIVE MODE NOWAIT")
            )
        self.calls.append(dict(kwargs))
        if self.mode == "transport_error":
            raise RuntimeError("provider response usage unknown")
        if len(self.calls) == 1:
            if self.mode == "no_tool":
                return {
                    "output": [],
                    "output_text": None,
                    "usage": {"input_tokens": 100, "output_tokens": 50},
                }
            return {
                "output": [
                    {
                        "type": "function_call",
                        "call_id": "worker-call",
                        "name": "evaluate_adjustment_scenario",
                        "arguments": json.dumps({"objective": "reduce_security_concentration"}),
                    }
                ],
                "usage": {"input_tokens": 100, "output_tokens": 50},
            }
        return {
            "output": [],
            "output_text": json.dumps(self.intent),
            "usage": {"input_tokens": 50, "output_tokens": 50},
        }


class _Client:
    max_retries = 0

    def __init__(self, intent: dict[str, object], factory, *, mode: str = "success") -> None:
        self.responses = _Responses(intent, factory, mode=mode)

    def supports_model_config(self, config: ResponsesModelConfig) -> bool:
        del config
        return True


class _IncompatibleClient(_Client):
    def supports_model_config(self, config: ResponsesModelConfig) -> bool:
        del config
        return False


class _RaisingCompatibilityClient(_Client):
    def supports_model_config(self, config: ResponsesModelConfig) -> bool:
        del config
        raise RuntimeError("injected compatibility failure")


class _UnboundClient:
    max_retries = 0

    def __init__(self, intent: dict[str, object], factory) -> None:
        self.responses = _Responses(intent, factory)
