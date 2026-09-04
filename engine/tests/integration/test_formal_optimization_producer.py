"""Normal optimizer producer/worker with synthetic persisted research, never live research."""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from helpers.diagnostic_artifacts import FixtureArtifactReader
from nexus.agents.portfolio_context import read_portfolio_context
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.agents.synthesis import domain_evidence
from nexus.diagnostics.service import diagnose_and_persist
from nexus.jobs.models import JobKeyV1, JobType, QueueName, RetryPolicy, job_input_hash
from nexus.jobs.repository import enqueue_job, claim_next_job, finalize_job
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
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.optimization import (
    BaselineSelectionAuthorityRecord,
    OptimizationAdmissionAuthorityRecord,
    OptimizationProposalGroupRecord,
    ProposalComparisonRecord,
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
from nexus.settings import EngineSettings
from nexus.strategy.intents import ResearchIntent
from sqlalchemy import func, select

from integration.test_strategy_snapshot_reader_pg import CUTOFF, ID, seed


async def setup(url):
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
        # Legal persisted research fixture: its content, original P and evidence are
        # fully validated by the optimizer. This does not claim native model generation.
        filing = ResearchEvidenceRef(
            source="sec", provider_object_id="synthetic-formal-filing", revision="1",
            available_at=at, created_at=at, content_hash="e" * 64,
            location="memory://sha256/" + "e" * 64, byte_length=10,
        )
        session.add(filing)
        await session.flush()
        session.add(Filing(
            instrument_id=ID, evidence_ref_id=filing.id, source="sec",
            provider_object_id=filing.provider_object_id, revision="1",
            accession_number="0000000001-26-000001", form="10-Q", filed_at=at, accepted_at=at,
        ))
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
            quality_status="complete",
            evidence_audit="complete",
            role_votes=(
                {"role": "portfolio", "vote": "support"},
            ),
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
            created_at=CUTOFF - timedelta(seconds=1),
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
            session, queue=QueueName.GENERAL, worker_id="persisted-research-fixture",
            now=CUTOFF - timedelta(seconds=2), job_ids=(job.id,),
        )
        assert claim is not None
        await finalize_job(
            session, claim=claim, outcome="succeeded", result_code="RESEARCH_COMPLETED",
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
