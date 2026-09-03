"""A later research context is frozen, not backdated into scenario valuation."""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from uuid import UUID

import httpx
import pytest
from helpers.diagnostic_artifacts import FixtureArtifactReader
from helpers.initial_draft import seed_initial_draft
from helpers.portfolio_api import app, headers, settings
from helpers.research_snapshots import seed_filing
from nexus.diagnostics.repository import read_report
from nexus.jobs.daily_manual import (
    ManualDailyRequest,
    build_manual_daily_registry,
    enqueue_manual_daily_root,
    manual_daily_job_ids,
)
from nexus.jobs.daily_research import admit_daily_research, resolve_daily_research_job
from nexus.jobs.models import QueueName
from nexus.jobs.worker import run_job_worker_once
from nexus.ops.daily_once import DailyOnceSettings, DailyProposalBuildRequest, run_once
from nexus.ops.openai_local import prepare_database
from nexus.optimizer.research_context import freeze_daily_research_context
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentResearchOutputEvidenceRecord,
    AgentResearchOutputRecord,
)
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.optimization import OptimizationProposalGroupRecord
from nexus.persistence.models.runtime import Job
from nexus.persistence.session import session_factory
from nexus.worker import run_agent_worker_once
from sqlalchemy import func, select


async def _case(url, monkeypatch):
    baseline, diagnostic = await seed_initial_draft(url)
    configured = settings(url)
    await prepare_database(configured)
    factory = session_factory(configured)

    # This suite consumes the existing synthetic valued/liquid draft fixture.
    # Real source-to-diagnostic computation has separate daily pipeline coverage.
    async def fixture_diagnostic(session, **kwargs):
        report = await session.get(PortfolioDiagnosticReportRecord, diagnostic)
        return await read_report(session, report)

    monkeypatch.setattr("nexus.jobs.daily_manual.diagnose_and_persist", fixture_diagnostic)
    close = datetime(2026, 8, 21, 20, tzinfo=UTC)
    async with factory() as session, session.begin():
        await seed_filing(session, instrument_id=UUID(int=801), seed_instrument=False)
        calendar = ExchangeCalendarAuthorityRecord(
            exchange="XNYS",
            version="researched-draft-test",
            source="fixture",
            range_start=close.date(),
            range_end=close.date(),
            query_parameters={},
            completeness_status="complete",
            content_hash="e" * 64,
            published_at=close,
        )
        session.add(calendar)
        await session.flush()
        session.add(
            ExchangeCalendarSessionRecord(
                calendar_authority_id=calendar.id,
                session=close.date(),
                market_open_at=close - timedelta(hours=6, minutes=30),
                market_close_at=close,
                decision_at=close + timedelta(hours=1),
            )
        )
    async with factory() as session, session.begin():
        root = await enqueue_manual_daily_root(
            session,
            request=ManualDailyRequest(
                snapshot_id=baseline,
                calendar_authority_id=calendar.id,
                session=close.date(),
                cutoff_at=datetime.now(UTC),
            ),
            now=datetime.now(UTC),
        )
        model_id = await session.scalar(select(AgentModelConfigRecord.id))
    for _ in range(3):
        async with factory() as session:
            ids = await manual_daily_job_ids(session, root_id=root.id)
        assert await run_job_worker_once(
            factory,
            registry=build_manual_daily_registry(artifact_reader=FixtureArtifactReader({})),
            queue=QueueName.GENERAL,
            worker_id="researched-draft",
            worker_started_at=close,
            process_version="test",
            migration_head=CURRENT_MIGRATION_HEAD,
            clock=lambda: datetime.now(UTC),
            job_ids=ids,
        )
    async with factory() as session, session.begin():
        research = await admit_daily_research(
            session,
            root_id=root.id,
            instrument_id=UUID(int=801),
            research_as_of=datetime.now(UTC),
            model_config_id=model_id,
            daily_limit_usd=configured.agent_daily_budget_usd,
            now=datetime.now(UTC),
        )
        job = await resolve_daily_research_job(session, root_id=root.id, run_id=research.run_id)
    assert await run_agent_worker_once(configured, job_ids=(job.id,))
    return (
        factory,
        baseline,
        diagnostic,
        {
            "root_id": str(root.id),
            "run_id": str(research.run_id),
            "context_as_of": datetime.now(UTC).isoformat(),
        },
    )


async def _post(client, baseline, diagnostic, context=None, key="draft"):
    path = "/internal/v1/optimization-proposal-drafts"
    body = {"portfolio_snapshot_id": str(baseline), "diagnostic_report_id": str(diagnostic)}
    if context is not None:
        body["research_context"] = context
    raw = json.dumps(body).encode()
    signed = headers("POST", path, raw)
    signed["Idempotency-Key"] = key
    return await client.post(path, content=raw, headers=signed)


@pytest.mark.asyncio
async def test_researched_draft_freezes_context_api_and_preserves_targets(
    migrated_database_url, monkeypatch
):
    factory, baseline, diagnostic, context = await _case(migrated_database_url, monkeypatch)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url, now=datetime.now(UTC))),
        base_url="http://testserver",
    ) as client:
        old_response = await _post(client, baseline, diagnostic, key="before-research")
        assert old_response.status_code == 201, old_response.text
        old = old_response.json()["data"]
        created = await _post(client, baseline, diagnostic, context)
        assert created.status_code == 201, created.text
        result = created.json()["data"]
        frozen = result["research_context"]
        assert frozen["root_id"] == context["root_id"] and frozen["run_id"] == context["run_id"]
        assert frozen["context_as_of"] != frozen["valuation_time"]
        assert frozen["mode"] == "context_only" and frozen["execution_locked"]
        assert frozen["quality_status"] == "partial"
        assert result["shadow_eligibility"]["eligible"] is False
        assert result["data_snapshot_id"] != old["data_snapshot_id"]
        assert [p["positions"] for p in result["proposals"]] == [
            p["positions"] for p in old["proposals"]
        ]
        replay = await _post(client, baseline, diagnostic, context)
        assert replay.status_code == 201 and replay.json()["data"] == result
        path = f"/internal/v1/optimization-proposals/{old['proposal_group_id']}"
        old_read = await client.get(path, headers=headers("GET", path))
        assert old_read.status_code == 200 and old_read.json()["data"] == old
        async with factory() as session, session.begin():
            sequence = await session.scalar(
                select(func.max(AgentResearchOutputRecord.sequence)).where(
                    AgentResearchOutputRecord.run_id == UUID(context["run_id"])
                )
            )
            # Later malformed output must not replace the already frozen output.
            session.add(
                AgentResearchOutputRecord(
                    run_id=UUID(context["run_id"]),
                    sequence=sequence + 1,
                    status="completed",
                    role=None,
                    prompt_hash=None,
                    content_hash="f" * 64,
                    payload={},
                    intent=None,
                    created_at=datetime.now(UTC),
                )
            )
            jobs_before = await session.scalar(select(func.count()).select_from(Job))
        path = f"/internal/v1/optimization-proposals/{result['proposal_group_id']}"
        read = await client.get(path, headers=headers("GET", path))
        assert read.status_code == 200 and read.json()["data"] == result
        path = "/internal/v1/optimization-proposals"
        body = json.dumps(
            {
                "portfolio_snapshot_id": str(baseline),
                "data_snapshot_id": result["data_snapshot_id"],
                "cost_policy_id": result["cost_policy_id"],
            }
        ).encode()
        signed = headers("POST", path, body)
        signed["Idempotency-Key"] = "cannot-promote-context"
        blocked = await client.post(path, content=body, headers=signed)
        assert blocked.status_code == 409
    async with factory() as session:
        data = await session.get(DataSnapshot, UUID(result["data_snapshot_id"]))
        assert data.schema_version == "optimization_draft_inputs_v2"
        assert data.payload["research_context"] == frozen
        original = await session.get(DataSnapshot, UUID(old["data_snapshot_id"]))
        assert original.schema_version == "optimization_draft_inputs_v1"
        assert "research_context" not in original.payload
        assert await session.scalar(select(func.count()).select_from(Job)) == jobs_before


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["early", "root", "run", "future"])
async def test_researched_draft_rejects_output_before_available_without_new_draft(
    migrated_database_url,
    monkeypatch,
    invalid,
):
    factory, baseline, diagnostic, context = await _case(migrated_database_url, monkeypatch)
    if invalid == "early":
        context["context_as_of"] = datetime(2026, 8, 24, tzinfo=UTC).isoformat()
    elif invalid == "future":
        context["context_as_of"] = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    else:
        context[f"{invalid}_id"] = str(UUID(int=999999))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url, now=datetime.now(UTC))),
        base_url="http://testserver",
    ) as client:
        response = await _post(client, baseline, diagnostic, context)
        assert response.status_code == 409
    async with factory() as session:
        assert (
            await session.scalar(select(func.count()).select_from(OptimizationProposalGroupRecord))
            == 0
        )
        assert (
            await session.scalar(
                select(func.count())
                .select_from(DataSnapshot)
                .where(DataSnapshot.source == "nexus_optimizer_draft")
            )
            == 0
        )


@pytest.mark.asyncio
async def test_daily_proposal_once_uses_stored_context_without_model_or_new_jobs(
    migrated_database_url, monkeypatch
):
    factory, _, _, context = await _case(migrated_database_url, monkeypatch)

    class Store:
        def __init__(self, settings):
            self.read_normalized = FixtureArtifactReader({})

    def forbidden(*args, **kwargs):
        pytest.fail("proposal-build must never execute model or job worker")

    monkeypatch.setattr("nexus.ops.daily_once.S3ProviderEvidenceStore", Store)
    monkeypatch.setattr("nexus.ops.daily_once.run_agent_worker_once", forbidden)
    monkeypatch.setattr("nexus.ops.daily_once.run_job_worker_once", forbidden)
    configured = DailyOnceSettings(
        database_url=migrated_database_url,
        object_store_endpoint="http://127.0.0.1:19000",
        object_store_region="test-region",
        object_store_bucket="synthetic-test",
        object_store_access_key_id="synthetic-key",
        object_store_secret_access_key="synthetic-secret",
    )
    request = DailyProposalBuildRequest(action="proposal-build", idempotency_key="once", **context)
    async with factory() as session:
        jobs_before = await session.scalar(select(func.count()).select_from(Job))
    result = await run_once(configured, request)
    assert result["status"] == "draft_created" and result["execution_locked"] is True
    assert await run_once(configured, request) == result
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Job)) == jobs_before
        assert (
            await session.scalar(select(func.count()).select_from(OptimizationProposalGroupRecord))
            == 1
        )
        group = await session.get(
            OptimizationProposalGroupRecord, UUID(result["proposal_group_id"])
        )
        data = await session.get(DataSnapshot, group.data_snapshot_id)
        assert data.payload["research_context"]["run_id"] == context["run_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_group", [None, "claims", "reviews"])
async def test_context_rejects_unbound_structured_synthesis_evidence(
    migrated_database_url, monkeypatch, bad_group
):
    factory, _, _, context = await _case(migrated_database_url, monkeypatch)
    async with factory() as session, session.begin():
        original = await session.scalar(
            select(AgentResearchOutputRecord)
            .where(
                AgentResearchOutputRecord.run_id == UUID(context["run_id"]),
                AgentResearchOutputRecord.role.is_(None),
            )
            .order_by(AgentResearchOutputRecord.sequence.desc())
        )
        intent = dict(original.intent)
        ref = intent["evidence_refs"][0]
        assessment = {
            "stance": "watch",
            "claims": [{"text": "Synthetic support", "evidence_refs": [ref]}],
            "reviews": [
                {
                    "claim_id": "bear:0",
                    "status": "unresolved",
                    "rationale": "Synthetic unknown",
                    "evidence_refs": [ref],
                    "resolved": False,
                    "missing_evidence": ["Synthetic missing"],
                    "prevented_conclusion": "Cannot conclude",
                }
            ],
            "review_conclusion": "limited",
            "uncertainties": ["Synthetic unknown"],
        }
        if bad_group is not None:
            assessment[bad_group][0]["evidence_refs"] = [{**ref, "content_hash": "e" * 64}]
        intent["synthesis_assessment"] = assessment
        content = {
            "status": original.status,
            "role": None,
            "payload": original.payload,
            "intent": intent,
        }
        later = AgentResearchOutputRecord(
            run_id=original.run_id,
            sequence=original.sequence + 1,
            prompt_hash=None,
            content_hash=hashlib.sha256(
                json.dumps(content, sort_keys=True, separators=(",", ":"), default=str).encode()
            ).hexdigest(),
            created_at=datetime.now(UTC),
            **content,
        )
        session.add(later)
        await session.flush()
        bindings = list(
            await session.scalars(
                select(AgentResearchOutputEvidenceRecord).where(
                    AgentResearchOutputEvidenceRecord.output_id == original.id,
                )
            )
        )
        for binding in bindings:
            session.add(
                AgentResearchOutputEvidenceRecord(
                    output_id=later.id,
                    **{
                        name: getattr(binding, name)
                        for name in (
                            "evidence_ref_id",
                            "source",
                            "provider_object_id",
                            "revision",
                            "available_at",
                            "location",
                            "content_hash",
                        )
                    },
                )
            )
    async with factory() as session:
        if bad_group is None:
            frozen = await freeze_daily_research_context(
                session,
                root_id=UUID(context["root_id"]),
                run_id=UUID(context["run_id"]),
                context_as_of=datetime.now(UTC),
            )
            assert frozen.intent.synthesis_assessment is not None
            return
        with pytest.raises(ValueError, match="DRAFT_RESEARCH_OUTPUT_INVALID"):
            await freeze_daily_research_context(
                session,
                root_id=UUID(context["root_id"]),
                run_id=UUID(context["run_id"]),
                context_as_of=datetime.now(UTC),
            )
