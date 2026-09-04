"""Normal optimizer producer/worker with synthetic persisted research, never live research."""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from helpers.diagnostic_artifacts import FixtureArtifactReader
from integration.test_strategy_snapshot_reader_pg import CUTOFF, ID, seed
from nexus.agents.portfolio_context import read_portfolio_context
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.agents.synthesis import domain_evidence
from nexus.diagnostics.service import diagnose_and_persist
from nexus.jobs.models import JobKeyV1, JobType, QueueName, RetryPolicy, job_input_hash
from nexus.jobs.repository import claim_next_job, enqueue_job, finalize_job
from nexus.jobs.worker import run_job_worker_once
from nexus.ops.openai_local import prepare_database
from nexus.optimizer.formal_inputs import FormalResearchOutputReference, canonical_hash
from nexus.optimizer.formal_producer import FormalOptimizationRequest, admit_formal_optimization
from nexus.optimizer.jobs import build_optimization_registry
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    AgentResearchOutputEvidenceRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot, QualityReport
from nexus.persistence.models.diagnostics import (
    PortfolioDiagnosticEvidenceRefRecord,
    PortfolioDiagnosticItemRecord,
    PortfolioDiagnosticReportRecord,
)
from nexus.persistence.models.optimization import (
    BaselineSelectionAuthorityRecord,
    OptimizationAdmissionAuthorityRecord,
    OptimizationProposalGroupRecord,
    OptimizationProposalRecord,
    ProposalComparisonRecord,
    ProposalPositionRecord,
)
from nexus.persistence.models.portfolio import (
    Account,
    BrokerSyncRun,
    CashSnapshot,
    Portfolio,
    PortfolioSnapshotRecord,
)
from nexus.persistence.models.research import Filing, ResearchEvidenceRef
from nexus.persistence.models.runtime import Job
from nexus.persistence.session import session_factory
from nexus.quality.source_policy import SOURCE_POLICY_ID
from nexus.reconciliation.persistence_errors import IdempotencyConflict
from nexus.settings import EngineSettings
from nexus.strategy.intents import ResearchIntent
from sqlalchemy import func, select


async def setup(url, *, late_cash=False, research_quality="complete", output_delay=False):
    settings = EngineSettings(database_url=url)
    await prepare_database(settings)
    factory = session_factory(settings)
    references = await seed(factory)
    at = CUTOFF - timedelta(minutes=2)
    research_at = CUTOFF - timedelta(minutes=1)
    async with factory() as session, session.begin():
        account = Account(provider="fosun", status="ACTIVE")
        session.add(account)
        await session.flush()
        portfolio = Portfolio(account_id=account.id, name="synthetic formal")
        session.add(portfolio)
        await session.flush()
        sync = BrokerSyncRun(
            account_id=account.id,
            portfolio_id=portfolio.id,
            trigger_kind="MANUAL",
            actor_id="synthetic",
            route="formal-test",
            idempotency_key="cash",
            request_hash="a" * 64,
            status="READY",
            accepted_at=at,
            completed_at=at,
            cash_flow_quality="PASS",
            attempt_count=1,
        )
        session.add(sync)
        await session.flush()
        p = PortfolioSnapshotRecord(
            portfolio_id=portfolio.id,
            sync_run_id=sync.id,
            status="READY",
            as_of=at,
            position_hash_version="broker_position_hash_v1",
            position_hash="b" * 64,
            order_hash_version="broker_order_hash_v1",
            order_hash="c" * 64,
            source_request_ids=[],
            source_observations=[],
        )
        session.add(p)
        await session.flush()
        session.add(
            CashSnapshot(
                portfolio_snapshot_id=p.id,
                currency="USD",
                cash_balance=100,
                available_cash=100,
                provider_as_of=at,
                created_at=CUTOFF + timedelta(microseconds=1) if late_cash else at,
            )
        )
        await session.flush()
        report = await diagnose_and_persist(
            session,
            portfolio_id=portfolio.id,
            snapshot_id=p.id,
            artifact_reader=FixtureArtifactReader({}),
        )
        assert report.optimization_need == "RESEARCH_REQUIRED"
        diagnostic = await session.scalar(
            select(PortfolioDiagnosticReportRecord).where(
                PortfolioDiagnosticReportRecord.portfolio_snapshot_id == p.id,
            )
        )
        if late_cash:
            # Isolate formal baseline admission from the already strict context reader.
            return factory, FormalOptimizationRequest(
                portfolio_snapshot_id=p.id,
                diagnostic_report_id=diagnostic.id,
                decision_cutoff_at=CUTOFF,
                strategy_references=references,
                research_outputs=(
                    FormalResearchOutputReference(run_id=uuid4(), output_id=uuid4()),
                ),
            )
        # Legal persisted research fixture: its content, original P and evidence are
        # fully validated by the optimizer. This does not claim native model generation.
        filing = ResearchEvidenceRef(
            source="sec",
            provider_object_id="synthetic-formal-filing",
            revision="1",
            available_at=at,
            created_at=at,
            content_hash="e" * 64,
            location="memory://sha256/" + "e" * 64,
            byte_length=10,
        )
        session.add(filing)
        await session.flush()
        session.add(
            Filing(
                instrument_id=ID,
                evidence_ref_id=filing.id,
                source="sec",
                provider_object_id=filing.provider_object_id,
                revision="1",
                accession_number="0000000001-26-000001",
                form="10-Q",
                filed_at=at,
                accepted_at=at,
            )
        )
        await session.flush()
        r = await prepare_research_snapshot(
            session, ID, as_of=research_at, portfolio_snapshot_id=p.id
        )
        assert r is not None
        context = await read_portfolio_context(session, r.id, ID, research_at)
        assert context is not None and context.portfolio_snapshot_id == p.id
        evidence = await session.get(ResearchEvidenceRef, context.evidence_ref_id)
        evidence_rows = (filing, evidence)
        manifest = [
            {
                "id": str(evidence.id),
                "source": evidence.source,
                "provider_object_id": evidence.provider_object_id,
                "revision": evidence.revision,
                "available_at": evidence.available_at.isoformat().replace("+00:00", "Z"),
                "location": evidence.location,
                "content_hash": evidence.content_hash,
            }
            for evidence in evidence_rows
        ]
        run = AgentResearchRunRecord(
            instrument_id=ID,
            data_snapshot_id=r.id,
            prompt_version_id=await session.scalar(select(AgentPromptVersionRecord.id).limit(1)),
            model_config_id=await session.scalar(select(AgentModelConfigRecord.id).limit(1)),
            decision_time=research_at,
            actor_id="formal-fixture",
            idempotency_key="research",
            status="completed",
            retry_count=0,
            schema_hash="a" * 64,
            model_hash="a" * 64,
            prompt_hash="a" * 64,
            prompt_manifest={},
            evidence_hash=hashlib.sha256(
                json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
            evidence_manifest=manifest,
            tool_hash="a" * 64,
            input_hash="a" * 64,
            reserved_cost_usd=0,
            settled_cost_usd=0,
            pricing_version="configured",
        )
        session.add(run)
        await session.flush()
        intent = ResearchIntent(
            schema_version="research_intent_v1",
            decision_id=uuid4(),
            instrument_id=ID,
            display_symbol="TEST",
            decision_time=research_at,
            data_snapshot_id=r.id,
            horizon="long_term",
            stance="hold",
            exposure_action="maintain",
            theses=(),
            counter_theses=(),
            uncertainties=(),
            invalidation_conditions=(),
            entry_conditions=(),
            exit_conditions=(),
            valid_until=CUTOFF + timedelta(days=1),
            quality_status=research_quality,
            evidence_audit="complete",
            role_votes=({"role": "portfolio", "vote": "support"},),
            disagreement="Synthetic disagreement retained",
            strategy_version="baseline_v1",
            model_version="synthetic",
            prompt_version="synthetic",
            data_version="synthetic",
            evidence_refs=tuple(domain_evidence(evidence) for evidence in evidence_rows),
        )
        output_payload = {
            "status": "completed",
            "role": None,
            "payload": {},
            "intent": intent.model_dump(mode="json"),
        }
        output = AgentResearchOutputRecord(
            run_id=run.id,
            sequence=1,
            content_hash=canonical_hash(output_payload),
            **output_payload,
            created_at=CUTOFF + timedelta(microseconds=1)
            if output_delay
            else CUTOFF - timedelta(seconds=1),
        )
        session.add(output)
        await session.flush()
        session.add_all(
            [
                AgentResearchOutputEvidenceRecord(
                    output_id=output.id,
                    evidence_ref_id=evidence.id,
                    **{
                        name: getattr(evidence, name)
                        for name in (
                            "source",
                            "provider_object_id",
                            "revision",
                            "available_at",
                            "content_hash",
                            "location",
                        )
                    },
                )
                for evidence in evidence_rows
            ]
        )
        payload = {"run_id": str(run.id)}
        job = await enqueue_job(
            session,
            key=JobKeyV1(
                job_type=JobType.AGENT_RESEARCH,
                scope_kind="instrument",
                scope_id=str(ID),
                effective_at=research_at,
                input_hash=job_input_hash(payload),
            ),
            schema_version="agent_research_job_v1",
            payload=payload,
            queue=QueueName.GENERAL,
            retry_policy=RetryPolicy.NEVER,
            max_attempts=1,
        )
        claim = await claim_next_job(
            session,
            queue=QueueName.GENERAL,
            worker_id="persisted-research-fixture",
            now=CUTOFF - timedelta(seconds=2),
            job_ids=(job.id,),
        )
        assert claim is not None
        await finalize_job(
            session,
            claim=claim,
            outcome="succeeded",
            result_code="RESEARCH_COMPLETED",
            now=CUTOFF - timedelta(seconds=1),
        )
        quality = QualityReport(
            data_snapshot_id=r.id,
            status="PASS",
            schema_version="1",
            payload={
                "required_instrument_ids": [str(ID)],
                "decision_session": references.market.decision_session.isoformat(),
            },
        )
        session.add(quality)
        await session.flush()
        references = references.model_copy(
            update={
                "market": references.market.model_copy(
                    update={
                        "source_policy_id": SOURCE_POLICY_ID,
                        "quality_report_id": quality.id,
                    }
                )
            }
        )
        request = FormalOptimizationRequest(
            portfolio_snapshot_id=p.id,
            diagnostic_report_id=diagnostic.id,
            decision_cutoff_at=CUTOFF,
            strategy_references=references,
            research_outputs=(FormalResearchOutputReference(run_id=run.id, output_id=output.id),),
        )
    return factory, request


@pytest.mark.asyncio
async def test_normal_producer_and_worker_keep_research_required_costs_unknown_and_replay(
    migrated_database_url,
):
    factory, request = await setup(migrated_database_url)
    async with factory() as session, session.begin():
        admitted = await admit_formal_optimization(
            session, request=request, actor_id="test", idempotency_key="one"
        )
    async with factory() as session, session.begin():
        replay = await admit_formal_optimization(
            session, request=request, actor_id="test", idempotency_key="one"
        )
        assert replay.replayed and replay.job_id == admitted.job_id
        another_key = await admit_formal_optimization(
            session, request=request, actor_id="test", idempotency_key="another-key"
        )
        assert another_key.replayed
        assert another_key.job_id == admitted.job_id
        assert another_key.data_snapshot_id == admitted.data_snapshot_id
        assert another_key.admission_authority_id == admitted.admission_authority_id
        with pytest.raises(IdempotencyConflict):
            await admit_formal_optimization(
                session,
                request=request.model_copy(update={"cost_policy_id": uuid4()}),
                actor_id="test",
                idempotency_key="one",
            )
    assert await run_job_worker_once(
        factory,
        registry=build_optimization_registry(),
        queue=QueueName.GENERAL,
        worker_id="formal-test",
        worker_started_at=datetime.now(UTC),
        process_version="test",
        migration_head="test",
        clock=lambda: datetime.now(UTC),
        claim_registered_only=True,
        job_ids=(admitted.job_id,),
    )
    async with factory() as session:
        job = await session.get(Job, admitted.job_id)
        assert job.status == "succeeded", job.last_error_code
        group = await session.scalar(select(OptimizationProposalGroupRecord))
        assert group is not None and group.decision_cutoff_at == CUTOFF
        assert group.valuation_time < CUTOFF and group.partial and not group.diagnostic_complete
        assert not group.cost_complete and group.portfolio_scope == "real_read_only"
        comparisons = tuple(await session.scalars(select(ProposalComparisonRecord)))
        assert len(comparisons) == 3 and all(
            c.non_executable and not c.cost_complete for c in comparisons
        )
        assert all(
            c.metrics["cost"] is None and not c.metrics["ranking_eligible"] for c in comparisons
        )
        # The evaluator selects ID, but the frozen hold/maintain research forbids
        # increasing an empty position. This proves the real consumer used R.
        selection = await session.scalar(select(BaselineSelectionAuthorityRecord))
        assert selection.selected_instrument_ids == [str(ID)]
        target = await session.scalar(
            select(ProposalPositionRecord)
            .join(
                OptimizationProposalRecord,
                OptimizationProposalRecord.id == ProposalPositionRecord.proposal_id,
            )
            .where(OptimizationProposalRecord.variant == "strategy_target")
        )
        assert target.instrument_id == ID and target.target_weight == 0 and target.action == "none"
        frozen = await session.get(DataSnapshot, admitted.data_snapshot_id)
        output = await session.get(AgentResearchOutputRecord, request.research_outputs[0].output_id)
        assert frozen.revision == canonical_hash(frozen.payload)
        binding = frozen.payload["research_bindings"][0]
        assert binding["output_hash"] == output.content_hash
        assert binding["research_data_snapshot_id"] == output.intent["data_snapshot_id"]
        assert output.intent["data_snapshot_id"] != str(frozen.id)
        assert frozen.observed_at == group.valuation_time
        assert CUTOFF <= selection.available_at <= frozen.available_at == frozen.received_at
        assert (
            await session.scalar(select(func.count()).select_from(BaselineSelectionAuthorityRecord))
            == 1
        )
        assert (
            await session.scalar(
                select(func.count()).select_from(OptimizationAdmissionAuthorityRecord)
            )
            == 1
        )


async def _assert_no_formal_writes(session):
    assert (
        await session.scalar(
            select(func.count())
            .select_from(DataSnapshot)
            .where(DataSnapshot.source == "nexus_optimization_formal")
        )
        == 0
    )
    for record in (
        BaselineSelectionAuthorityRecord,
        OptimizationAdmissionAuthorityRecord,
        OptimizationProposalGroupRecord,
    ):
        assert await session.scalar(select(func.count()).select_from(record)) == 0
    assert (
        await session.scalar(
            select(func.count()).select_from(Job).where(Job.job_type == "optimization.propose")
        )
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["output", "portfolio", "cost", "cutoff", "duplicate"])
async def test_invalid_formal_request_has_zero_writes(migrated_database_url, invalid):
    factory, request = await setup(migrated_database_url)
    changes = {
        "output": {
            "research_outputs": (
                FormalResearchOutputReference(
                    run_id=request.research_outputs[0].run_id, output_id=uuid4()
                ),
            )
        },
        "portfolio": {"portfolio_snapshot_id": uuid4()},
        "cost": {"cost_policy_id": uuid4()},
        "cutoff": {"decision_cutoff_at": CUTOFF - timedelta(seconds=2)},
        "duplicate": {"research_outputs": request.research_outputs * 2},
    }
    async with factory() as session, session.begin():
        with pytest.raises(ValueError):
            await admit_formal_optimization(
                session,
                request=request.model_copy(update=changes[invalid]),
                actor_id="test",
                idempotency_key="invalid",
            )
        await _assert_no_formal_writes(session)


@pytest.mark.asyncio
async def test_late_cash_is_not_backfilled_into_formal_baseline(migrated_database_url):
    factory, request = await setup(migrated_database_url, late_cash=True)
    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="OPTIMIZATION_FORMAL_BASELINE_NOT_READY"):
            await admit_formal_optimization(
                session, request=request, actor_id="test", idempotency_key="late-cash"
            )
        await _assert_no_formal_writes(session)


@pytest.mark.asyncio
@pytest.mark.parametrize("child", ["item", "evidence"])
async def test_late_diagnostic_children_are_not_backfilled(migrated_database_url, child):
    factory, request = await setup(migrated_database_url)
    async with factory() as session, session.begin():
        item = await session.scalar(
            select(PortfolioDiagnosticItemRecord).where(
                PortfolioDiagnosticItemRecord.report_id == request.diagnostic_report_id
            )
        )
        if child == "item":
            late_item_id = uuid4()
            session.add(
                PortfolioDiagnosticItemRecord(
                    id=late_item_id,
                    report_id=request.diagnostic_report_id,
                    diagnostic_code="LATE_SYNTHETIC",
                    metric_value=None,
                    unit="ratio",
                    reference_limit=None,
                    policy_scope="synthetic",
                    severity="info",
                    actionability="monitor",
                    unknown_reason="UNKNOWN",
                    created_at=CUTOFF + timedelta(microseconds=1),
                )
            )
            await session.flush()
            session.add(
                PortfolioDiagnosticEvidenceRefRecord(
                    item_id=late_item_id,
                    source="synthetic",
                    object_id="late-item-evidence",
                    available_at=CUTOFF - timedelta(days=1),
                    location="memory://synthetic",
                    content_hash="f" * 64,
                    created_at=CUTOFF - timedelta(days=1),
                )
            )
        else:
            session.add(
                PortfolioDiagnosticEvidenceRefRecord(
                    item_id=item.id,
                    source="synthetic",
                    object_id="late-fact",
                    available_at=CUTOFF - timedelta(days=1),
                    location="memory://synthetic",
                    content_hash="f" * 64,
                    created_at=CUTOFF + timedelta(microseconds=1),
                )
            )
    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="OPTIMIZATION_FORMAL_DIAGNOSTIC_NOT_READY"):
            await admit_formal_optimization(
                session, request=request, actor_id="test", idempotency_key="late-diagnostic"
            )
        await _assert_no_formal_writes(session)


@pytest.mark.asyncio
@pytest.mark.parametrize("changes", [{"research_quality": "partial"}, {"output_delay": True}])
async def test_partial_or_late_research_does_not_gain_formal_admission(
    migrated_database_url, changes
):
    factory, request = await setup(migrated_database_url, **changes)
    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="OPTIMIZATION_FORMAL_RESEARCH_NOT_READY"):
            await admit_formal_optimization(
                session, request=request, actor_id="test", idempotency_key="research-not-ready"
            )
        await _assert_no_formal_writes(session)


@pytest.mark.asyncio
async def test_enqueue_failure_rolls_back_manifest_selection_claim_and_admission(
    migrated_database_url,
    monkeypatch,
):
    from nexus.optimizer import formal_producer

    factory, request = await setup(migrated_database_url)
    original = formal_producer.admit_queued_job

    async def failed_enqueue(*args, **kwargs):
        raise ValueError("injected admission failure")

    monkeypatch.setattr(formal_producer, "admit_queued_job", failed_enqueue)
    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="injected admission failure"):
            await admit_formal_optimization(
                session, request=request, actor_id="test", idempotency_key="atomic"
            )
        await _assert_no_formal_writes(session)
    monkeypatch.setattr(formal_producer, "admit_queued_job", original)
    async with factory() as session, session.begin():
        admitted = await admit_formal_optimization(
            session, request=request, actor_id="test", idempotency_key="atomic"
        )
        assert not admitted.replayed


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["evidence_refs", "candidates"])
async def test_formal_loader_rejects_corrupt_selection_provenance(migrated_database_url, field):
    from nexus.optimizer.formal_producer import load_formal_admission_authority
    from sqlalchemy.orm.attributes import set_committed_value

    factory, request = await setup(migrated_database_url)
    async with factory() as session, session.begin():
        admitted = await admit_formal_optimization(
            session, request=request, actor_id="test", idempotency_key="provenance"
        )
    async with factory() as session:
        authority = await session.get(
            OptimizationAdmissionAuthorityRecord, admitted.admission_authority_id
        )
        data = await session.get(DataSnapshot, admitted.data_snapshot_id)
        selection = await session.get(
            BaselineSelectionAuthorityRecord, authority.selection_authority_id
        )
        # Simulate a malformed storage-reader result without rewriting immutable history.
        value = (
            ["forged-ref"]
            if field == "evidence_refs"
            else [
                {"instrument_id": row["instrument_id"], "provenance_refs": ["forged-ref"]}
                for row in selection.candidates
            ]
        )
        set_committed_value(selection, field, value)
        with pytest.raises(ValueError, match="OPTIMIZATION_FORMAL_SELECTION_INVALID"):
            await load_formal_admission_authority(session, authority, data)


@pytest.mark.asyncio
async def test_other_valid_portfolio_snapshot_cannot_reuse_original_research(migrated_database_url):
    factory, request = await setup(migrated_database_url)
    async with factory() as session, session.begin():
        original = await session.get(PortfolioSnapshotRecord, request.portfolio_snapshot_id)
        sync = await session.get(BrokerSyncRun, original.sync_run_id)
        # Same account, cash and valuation clock; only the immutable P identity differs.
        later_sync = BrokerSyncRun(
            **{
                column.key: getattr(sync, column.key)
                for column in BrokerSyncRun.__table__.columns
                if column.key not in {"id", "idempotency_key"}
            },
            idempotency_key="second-synthetic-snapshot",
        )
        session.add(later_sync)
        await session.flush()
        other = PortfolioSnapshotRecord(
            **{
                column.key: getattr(original, column.key)
                for column in PortfolioSnapshotRecord.__table__.columns
                if column.key not in {"id", "sync_run_id"}
            },
            sync_run_id=later_sync.id,
        )
        session.add(other)
        await session.flush()
        session.add(
            CashSnapshot(
                portfolio_snapshot_id=other.id,
                currency="USD",
                cash_balance=100,
                available_cash=100,
                provider_as_of=other.as_of,
            )
        )
        await session.flush()
        await diagnose_and_persist(
            session,
            portfolio_id=other.portfolio_id,
            snapshot_id=other.id,
            artifact_reader=FixtureArtifactReader({}),
        )
        report = await session.scalar(
            select(PortfolioDiagnosticReportRecord).where(
                PortfolioDiagnosticReportRecord.portfolio_snapshot_id == other.id
            )
        )
        mismatch = request.model_copy(
            update={"portfolio_snapshot_id": other.id, "diagnostic_report_id": report.id}
        )
    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="OPTIMIZATION_FORMAL_PORTFOLIO_MISMATCH"):
            await admit_formal_optimization(
                session, request=mismatch, actor_id="test", idempotency_key="other-p"
            )
        await _assert_no_formal_writes(session)
