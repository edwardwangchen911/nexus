"""Normal research consumers with synthetic sources, never live-model qualification.

The positive case deliberately uses the legacy offline FixtureCommittee. Neither
synthesis nor formal research loading is mocked; no final research rows are seeded.
"""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock

import httpx
import pytest
from helpers.diagnostic_artifacts import FixtureArtifactReader
from helpers.portfolio_api import app, headers
from integration import test_agent_execution_pg as research_fixtures
from integration.test_strategy_snapshot_reader_pg import CUTOFF, ID, seed
from nexus.agents import codex_staged
from nexus.agents.committee import ROLE_ORDER
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import admit_research_run
from nexus.agents.responses_budget import UsageSnapshot
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.diagnostics.service import diagnose_and_persist
from nexus.jobs.models import JobType, QueueName
from nexus.jobs.worker import run_job_worker_once
from nexus.ops.openai_local import prepare_database
from nexus.optimizer.formal_inputs import FormalResearchOutputReference, canonical_hash
from nexus.optimizer.formal_producer import FormalOptimizationRequest, admit_formal_optimization
from nexus.optimizer.jobs import build_optimization_registry
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentModelConfigRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot, QualityReport
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.market import InstrumentAlias
from nexus.persistence.models.optimization import (
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
from nexus.persistence.models.runtime import Job
from nexus.persistence.session import session_factory
from nexus.quality.source_policy import SOURCE_POLICY_ID
from nexus.settings import EngineSettings
from nexus.worker import run_agent_worker_once
from sqlalchemy import func, select


async def _prepare(url, monkeypatch, *, structured=False, portfolio_after_market=False):
    settings = EngineSettings(
        database_url=url, agent_runtime_mode="codex_sdk" if structured else "fixture"
    )
    await prepare_database(settings, structured_synthesis=structured)
    factory = session_factory(settings)
    references = await seed(factory)
    at = (datetime.now(UTC) if portfolio_after_market else CUTOFF) - timedelta(minutes=2)
    async with factory.begin() as session:
        if not structured:
            # The same explicitly legacy fixture config used by research-worker
            # tests, never a claim that a paid model generated qualified research.
            session.add(
                AgentModelConfigRecord(
                    model_id="fixture-model",
                    version="fixture-pricing-v1",
                    config_hash="2" * 64,
                    max_input_tokens=1_000_000,
                    max_tool_calls=8,
                    max_output_tokens=10_000,
                    input_usd_per_million=Decimal("1"),
                    output_usd_per_million=Decimal("2"),
                    pricing_available_at=at - timedelta(days=1),
                    pricing_expires_at=datetime.now(UTC) + timedelta(days=30),
                )
            )
        # Adapt only a test-source fixture to the strategy instrument. This dummy
        # snapshot is not the research R admitted below; prepare creates exact R/P.
        session.add(
            DataSnapshot(
                id=research_fixtures._SNAPSHOT_ID,
                source="fixture",
                provider_object_id="unused-source-fixture",
                revision="1",
                observed_at=at,
                available_at=at,
                received_at=at,
                schema_version="1",
                payload={},
            )
        )
        session.add(
            InstrumentAlias(
                instrument_id=ID,
                symbol="SYNTH",
                alias_type="ticker",
                valid_from=at.date() - timedelta(days=365),
            )
        )
    with monkeypatch.context() as source_patch:
        source_patch.setattr(research_fixtures, "_INSTRUMENT_ID", ID)
        await research_fixtures._seed_database_tool_authority(
            factory, link_price=False, price_source="fosun"
        )
    async with factory.begin() as session:
        account = Account(provider="fosun", status="ACTIVE")
        session.add(account)
        await session.flush()
        portfolio = Portfolio(account_id=account.id, name="synthetic native research")
        session.add(portfolio)
        await session.flush()
        sync = BrokerSyncRun(
            account_id=account.id,
            portfolio_id=portfolio.id,
            trigger_kind="MANUAL",
            actor_id="synthetic",
            route="native-formal-test",
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
        portfolio_snapshot = PortfolioSnapshotRecord(
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
        session.add(portfolio_snapshot)
        await session.flush()
        session.add(
            CashSnapshot(
                portfolio_snapshot_id=portfolio_snapshot.id,
                currency="USD",
                cash_balance=100,
                available_cash=100,
                provider_as_of=at,
                created_at=at,
            )
        )
        await session.flush()
        report = await diagnose_and_persist(
            session,
            portfolio_id=portfolio.id,
            snapshot_id=portfolio_snapshot.id,
            artifact_reader=FixtureArtifactReader({}),
        )
        assert report.optimization_need == "RESEARCH_REQUIRED"
        diagnostic = await session.scalar(select(PortfolioDiagnosticReportRecord))
        research_at = datetime.now(UTC)
        snapshot = await prepare_research_snapshot(
            session, ID, as_of=research_at, portfolio_snapshot_id=portfolio_snapshot.id
        )
        assert snapshot is not None
        model = await session.scalar(
            select(AgentModelConfigRecord).where(
                AgentModelConfigRecord.version
                == ("codex-sdk-chatgpt-v3" if structured else "fixture-pricing-v1")
            )
        )
        admission = await admit_research_run(
            session,
            actor_id="test",
            route="native-formal-test",
            idempotency_key="research",
            request_hash="d" * 64,
            request=ResearchRequest(
                instrument_id=ID, data_snapshot_id=snapshot.id, decision_time=research_at
            ),
            prompt_version_id=None,
            model_config_id=model.id,
            daily_limit_usd=Decimal("3"),
            now=datetime.now(UTC),
        )
        assert any(
            item["provider_object_id"].startswith("price-bar-set:")
            for item in admission.run.evidence_manifest
        )
        # Market quality belongs to the original synthetic market snapshot, not
        # to later R. Only source facts are seeded; research clocks stay real.
        market_snapshot = DataSnapshot(
            source="fixture",
            provider_object_id="synthetic-market-quality",
            revision="1",
            observed_at=CUTOFF,
            available_at=CUTOFF,
            received_at=CUTOFF,
            schema_version="1",
            payload={},
        )
        session.add(market_snapshot)
        await session.flush()
        quality = QualityReport(
            data_snapshot_id=market_snapshot.id,
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
        return settings, factory, references, portfolio_snapshot.id, diagnostic.id, admission.run_id


async def _completed(factory, run_id):
    async with factory() as session:
        run = await session.get(AgentResearchRunRecord, run_id)
        outputs = tuple(
            await session.scalars(
                select(AgentResearchOutputRecord).where(AgentResearchOutputRecord.run_id == run_id)
            )
        )
        assert run.status == "completed", [(row.status, row.payload) for row in outputs]
        assert all(item["source"] != "fixture" for item in run.evidence_manifest)
        expected_usage = (900, 240) if run.pricing_version == "codex-sdk-chatgpt-v3" else (80, 40)
        assert (run.input_tokens, run.output_tokens) == expected_usage
        reservation = await session.scalar(
            select(AgentDailyBudgetReservationRecord).where(
                AgentDailyBudgetReservationRecord.run_id == run_id
            )
        )
        assert reservation is not None and reservation.state == "settled"
        assert {row.role for row in outputs if row.role} == set(ROLE_ORDER)
        final = next(row for row in outputs if row.status == "completed")
        assert final.intent is not None
        assert final.content_hash == canonical_hash(
            {
                "status": final.status,
                "role": final.role,
                "payload": final.payload,
                "intent": final.intent,
            }
        )
        job = await session.scalar(select(Job).where(Job.payload["run_id"].astext == str(run_id)))
        assert job.status == "succeeded" and job.attempt_count == 1
        return final


def _request(references, portfolio_id, diagnostic_id, output, *, preserve_calendar=True):
    cutoff = datetime.now(UTC)
    return FormalOptimizationRequest(
        portfolio_snapshot_id=portfolio_id,
        diagnostic_report_id=diagnostic_id,
        decision_cutoff_at=cutoff,
        strategy_references=references
        if preserve_calendar
        else references.model_copy(
            update={"market": references.market.model_copy(update={"decision_time": cutoff})}
        ),
        research_outputs=(
            FormalResearchOutputReference(run_id=output.run_id, output_id=output.id),
        ),
    )


@pytest.mark.asyncio
async def test_legacy_offline_fixture_native_research_reaches_formal_worker(
    migrated_database_url, monkeypatch
):
    settings, factory, refs, portfolio_id, diagnostic_id, run_id = await _prepare(
        migrated_database_url, monkeypatch
    )
    assert await run_agent_worker_once(settings)
    output = await _completed(factory, run_id)
    assert output.intent["quality_status"] == "complete"
    assert output.intent["evidence_audit"] == "complete"
    assert output.intent["stance"] == "buy_candidate"
    request = _request(refs, portfolio_id, diagnostic_id, output)
    assert request.strategy_references.market.decision_time < request.decision_cutoff_at
    async with factory.begin() as session:
        admitted = await admit_formal_optimization(
            session, request=request, actor_id="test", idempotency_key="native-formal"
        )
    async with factory.begin() as session:
        replay = await admit_formal_optimization(
            session, request=request, actor_id="test", idempotency_key="native-formal"
        )
        assert replay.replayed and replay.job_id == admitted.job_id
    assert await run_job_worker_once(
        factory,
        registry=build_optimization_registry(),
        queue=QueueName.GENERAL,
        worker_id="formal-native-test",
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
        assert group.partial and not group.diagnostic_complete and not group.cost_complete
        comparisons = tuple(await session.scalars(select(ProposalComparisonRecord)))
        assert len(comparisons) == 3
        assert all(
            row.non_executable and not row.metrics["ranking_eligible"] for row in comparisons
        )
        target = await session.scalar(
            select(ProposalPositionRecord)
            .join(
                OptimizationProposalRecord,
                OptimizationProposalRecord.id == ProposalPositionRecord.proposal_id,
            )
            .where(OptimizationProposalRecord.variant == "strategy_target")
        )
        assert target.instrument_id == ID and target.target_weight == Decimal("0.125")
        frozen = await session.get(DataSnapshot, admitted.data_snapshot_id)
        binding = frozen.payload["research_bindings"][0]
        assert binding["output_id"] == str(output.id)
        assert binding["output_hash"] == output.content_hash
        assert binding["research_data_snapshot_id"] == output.intent["data_snapshot_id"]
        retained = await session.get(AgentResearchOutputRecord, output.id)
        assert retained.content_hash == output.content_hash and retained.intent == output.intent

    # Read the normally generated group through the actual authenticated API;
    # source fixtures must not turn into economic or execution permission.
    path = f"/internal/v1/optimization-proposals/{group.id}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url, now=datetime.now(UTC))),
        base_url="http://test",
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["proposal_group_id"] == str(group.id)
    assert data["data_snapshot_id"] == str(admitted.data_snapshot_id)
    assert len(data["proposals"]) == 3
    assert not data["shadow_eligibility"]["eligible"]
    assert all(
        proposal["comparison"]["non_executable"]
        and not proposal["comparison"]["metrics"]["ranking_eligible"]
        for proposal in data["proposals"]
    )


@pytest.mark.asyncio
async def test_native_v3_self_sufficient_remains_partial_and_formal_rejects(
    migrated_database_url, monkeypatch
):
    settings, factory, refs, portfolio_id, diagnostic_id, run_id = await _prepare(
        migrated_database_url, monkeypatch, structured=True
    )

    async def invoke(payload, *, output_model):
        if payload["stage"] == "synthesis":
            response = {
                "synthesis_assessment": {
                    "stance": "watch",
                    "claims": [{"text": "Synthetic conclusion.", "citations": [0]}],
                    "reviews": [
                        {
                            "claim_id": item["claim_id"],
                            "status": "accepted",
                            "rationale": "Synthetic resolved review.",
                            "citations": [0],
                            "resolved": True,
                            "missing_evidence": [],
                            "prevented_conclusion": None,
                        }
                        for item in payload["previous_claims"]
                        if item["role"] in {"bear", "evidence_audit"}
                    ],
                    "review_conclusion": "sufficient",
                    "uncertainties": [],
                    "conflicting_facts": False,
                }
            }
        else:
            response = {
                "roles": [
                    {
                        "role": role,
                        "vote": "support",
                        "conflicting_facts": False,
                        "claims": [{"text": "Synthetic source claim.", "citations": [0]}],
                        "reviewed_claim_ids": [] if payload["stage"] == "research" else ["bull:0"],
                    }
                    for role in payload["requested_roles"]
                ]
            }
        raw = json.dumps(response)
        output_model.model_validate_json(raw)
        return raw, UsageSnapshot(input_tokens=300, output_tokens=80)

    sdk = AsyncMock(side_effect=invoke)
    monkeypatch.setattr(codex_staged, "invoke_codex", sdk)
    assert await run_agent_worker_once(settings)
    assert sdk.await_count == 3
    output = await _completed(factory, run_id)
    assert output.intent["quality_status"] == "partial"
    assert output.intent["evidence_audit"] == "partial"
    assert output.intent["synthesis_assessment"]["review_conclusion"] == "sufficient"
    assert output.intent["stance"] == "watch" and output.intent["exposure_action"] == "none"
    request = _request(refs, portfolio_id, diagnostic_id, output)
    async with factory.begin() as session:
        with pytest.raises(ValueError, match="^OPTIMIZATION_FORMAL_RESEARCH_NOT_READY$"):
            await admit_formal_optimization(
                session, request=request, actor_id="test", idempotency_key="v3"
            )
        assert (
            await session.scalar(
                select(func.count()).select_from(OptimizationAdmissionAuthorityRecord)
            )
            == 0
        )
        assert (
            await session.scalar(select(func.count()).select_from(OptimizationProposalGroupRecord))
            == 0
        )
        assert (
            await session.scalar(
                select(func.count())
                .select_from(Job)
                .where(Job.job_type == JobType.OPTIMIZATION_PROPOSE)
            )
            == 0
        )


@pytest.mark.asyncio
async def test_native_complete_research_cannot_relabel_market_calendar_to_finish_time(
    migrated_database_url, monkeypatch
):
    settings, factory, refs, portfolio_id, diagnostic_id, run_id = await _prepare(
        migrated_database_url, monkeypatch
    )
    assert await run_agent_worker_once(settings)
    output = await _completed(factory, run_id)
    assert output.intent["quality_status"] == "complete"
    request = _request(refs, portfolio_id, diagnostic_id, output, preserve_calendar=False)
    async with factory.begin() as session:
        with pytest.raises(ValueError, match="^STRATEGY_SNAPSHOT_FEATURES_UNAVAILABLE$"):
            await admit_formal_optimization(
                session, request=request, actor_id="test", idempotency_key="bad-market-clock"
            )
        assert (
            await session.scalar(select(func.count()).select_from(OptimizationProposalGroupRecord))
            == 0
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_clock", ["portfolio_after_market", "market_after_cutoff", "old_equal"]
)
async def test_native_research_keeps_market_and_completion_clock_boundaries(
    migrated_database_url, monkeypatch, bad_clock
):
    settings, factory, refs, portfolio_id, diagnostic_id, run_id = await _prepare(
        migrated_database_url,
        monkeypatch,
        portfolio_after_market=bad_clock == "portfolio_after_market",
    )
    assert await run_agent_worker_once(settings)
    output = await _completed(factory, run_id)
    assert output.intent["quality_status"] == "complete"
    request = _request(refs, portfolio_id, diagnostic_id, output)
    if bad_clock == "market_after_cutoff":
        request = request.model_copy(
            update={
                "strategy_references": refs.model_copy(
                    update={
                        "market": refs.market.model_copy(
                            update={
                                "decision_time": request.decision_cutoff_at
                                + timedelta(microseconds=1)
                            }
                        )
                    }
                )
            }
        )
    elif bad_clock == "old_equal":
        request = request.model_copy(update={"decision_cutoff_at": refs.market.decision_time})
    expected = (
        "OPTIMIZATION_FORMAL_RESEARCH_NOT_READY"
        if bad_clock == "old_equal"
        else "OPTIMIZATION_FORMAL_BINDING_INVALID"
    )
    async with factory.begin() as session:
        with pytest.raises(ValueError, match=f"^{expected}$"):
            await admit_formal_optimization(
                session, request=request, actor_id="test", idempotency_key=bad_clock
            )
        assert (
            await session.scalar(
                select(func.count()).select_from(OptimizationAdmissionAuthorityRecord)
            )
            == 0
        )
