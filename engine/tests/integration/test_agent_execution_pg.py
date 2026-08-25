"""Real-PostgreSQL durable fixture committee lifecycle tests."""

import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import httpx
import pytest
from helpers.portfolio_api import app, headers
from nexus.agents.committee import (
    ROLE_ORDER,
    AgentRole,
    CommitteeRoleOutput,
    FixtureCommittee,
    PromptAsset,
    load_prompt_assets,
)
from nexus.agents.database_tools import database_read_only_tools
from nexus.agents.execution import LIFECYCLE, execute_research_run
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import (
    ResearchAdmission,
    admit_research_run,
    release_daily_budget,
)
from nexus.agents.tools import EXPECTED_READ_ONLY_TOOL_NAMES, ToolInputError
from nexus.domain.evidence import EvidenceRef
from nexus.market.price_bar_authority import finalize_price_bar_set
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentDataSnapshotEvidenceRecord,
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    AgentResearchEventRecord,
    AgentResearchOutputEvidenceRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import (
    DataArtifact,
    DataSnapshot,
    MacroObservation,
    PriceBarRecord,
    PriceBarSetAuthority,
    PriceSeriesManifest,
)
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.research import Filing, FinancialFactRecord, ResearchEvidenceRef
from nexus.persistence.models.runtime import Job
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings, Environment
from nexus.worker import run_agent_worker_once
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

_NOW = datetime(2026, 8, 24, 12, tzinfo=UTC)
_INSTRUMENT_ID = UUID("0198d500-1000-7000-8000-000000001601")
_SNAPSHOT_ID = UUID("0198d500-1000-7000-8000-000000001602")
_MODEL_ID = UUID("0198d500-1000-7000-8000-000000001603")
_EVIDENCE_ID = UUID("0198d500-1000-7000-8000-000000001604")
_PRICE_MANIFEST_ID = UUID("0198d500-1000-7000-8000-000000001707")
_PARQUET_PRICE_EVIDENCE_ID = UUID("0198d500-1000-7000-8000-000000001708")


@pytest.mark.asyncio
async def test_fixture_worker_persists_exact_lifecycle_roles_evidence_and_usage(
    migrated_database_url: str,
) -> None:
    """Skipping a role/state or failing to settle usage would leave an incomplete durable run."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    async with factory() as session, session.begin():
        admission = await admit_research_run(
            session,
            actor_id="fixture-worker",
            route="/internal/v1/research-runs",
            idempotency_key="durable-run",
            request_hash="a" * 64,
            request=ResearchRequest(
                instrument_id=_INSTRUMENT_ID,
                data_snapshot_id=_SNAPSHOT_ID,
                decision_time=_NOW,
            ),
            prompt_version_id=prompt_ids["orchestrator"],
            model_config_id=_MODEL_ID,
            daily_limit_usd=Decimal("3.00"),
            now=_NOW,
        )
    async with factory() as session, session.begin():
        result = await execute_research_run(
            session,
            run_id=admission.run_id,
            committee=FixtureCommittee(),
        )
    assert result.status == "completed"
    assert result.intent is not None
    assert (result.intent.stance, result.intent.exposure_action) == ("watch", "none")
    assert result.intent.quality_status == "partial"
    assert "Price authority is unavailable." in result.intent.uncertainties
    assert next(
        vote.vote for vote in result.intent.role_votes if vote.role == "market_state"
    ) == "abstain"
    assert result.intent.counter_theses
    assert result.intent.invalidation_conditions
    assert result.intent.entry_conditions
    assert result.intent.exit_conditions
    async with factory() as session:
        run = await session.get(AgentResearchRunRecord, admission.run_id)
        events = list(
            await session.scalars(
                select(AgentResearchEventRecord)
                .where(AgentResearchEventRecord.run_id == admission.run_id)
                .order_by(AgentResearchEventRecord.sequence)
            )
        )
        outputs = list(
            await session.scalars(
                select(AgentResearchOutputRecord)
                .where(AgentResearchOutputRecord.run_id == admission.run_id)
                .order_by(AgentResearchOutputRecord.sequence)
            )
        )
        evidence_count = await session.scalar(
            select(func.count())
            .select_from(AgentResearchOutputEvidenceRecord)
            .join(
                AgentResearchOutputRecord,
                AgentResearchOutputRecord.id == AgentResearchOutputEvidenceRecord.output_id,
            )
            .where(AgentResearchOutputRecord.run_id == admission.run_id)
        )
        reservation = await session.scalar(
            select(AgentDailyBudgetReservationRecord).where(
                AgentDailyBudgetReservationRecord.run_id == admission.run_id
            )
        )
    assert run is not None
    assert (run.status, run.input_tokens, run.output_tokens) == ("completed", 80, 40)
    assert tuple(event.state for event in events) == LIFECYCLE
    assert tuple(output.role for output in outputs[:8]) == ROLE_ORDER
    prompt_assets = load_prompt_assets()
    assert tuple(output.prompt_hash for output in outputs[:8]) == tuple(
        prompt_assets[role].content_hash for role in ROLE_ORDER
    )
    assert outputs[-2].status == "synthesized"
    assert outputs[-1].status == "completed"
    assert outputs[-1].intent == result.intent.model_dump(mode="json")
    assert evidence_count == 1
    assert reservation is not None and reservation.state == "settled"


@pytest.mark.asyncio
async def test_worker_abstains_and_releases_budget_when_fixture_evidence_is_disallowed(
    migrated_database_url: str,
) -> None:
    """Production composition cannot treat fixture evidence as executable authority."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    async with factory() as session, session.begin():
        admission = await admit_research_run(
            session,
            actor_id="fixture-worker",
            route="/internal/v1/research-runs",
            idempotency_key="missing-evidence",
            request_hash="b" * 64,
            request=ResearchRequest(
                instrument_id=_INSTRUMENT_ID,
                data_snapshot_id=_SNAPSHOT_ID,
                decision_time=_NOW,
            ),
            prompt_version_id=prompt_ids["orchestrator"],
            model_config_id=_MODEL_ID,
            daily_limit_usd=Decimal("3.00"),
            now=_NOW,
        )
    async with factory() as session, session.begin():
        result = await execute_research_run(
            session,
            run_id=admission.run_id,
            committee=FixtureCommittee(),
            allow_fixture_evidence=False,
        )
    assert result.status == "abstained" and result.intent is None
    async with factory() as session:
        run = await session.get(AgentResearchRunRecord, admission.run_id)
        reservation = await session.scalar(
            select(AgentDailyBudgetReservationRecord).where(
                AgentDailyBudgetReservationRecord.run_id == admission.run_id
            )
        )
        events = tuple(
            await session.scalars(
                select(AgentResearchEventRecord.state)
                .where(AgentResearchEventRecord.run_id == admission.run_id)
                .order_by(AgentResearchEventRecord.sequence)
            )
        )
    assert run is not None and run.status == "abstained"
    assert reservation is not None and reservation.state == "released"
    assert events == ("prepared", "abstained")


@pytest.mark.asyncio
async def test_conflicting_fixture_roles_abstain_before_synthesis(
    migrated_database_url: str,
) -> None:
    """Contradictory facts may be audited but cannot become a strategy-supporting intent."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    async with factory() as session, session.begin():
        admission = await admit_research_run(
            session,
            actor_id="fixture-worker",
            route="/internal/v1/research-runs",
            idempotency_key="conflicting-facts",
            request_hash="c" * 64,
            request=ResearchRequest(
                instrument_id=_INSTRUMENT_ID,
                data_snapshot_id=_SNAPSHOT_ID,
                decision_time=_NOW,
            ),
            prompt_version_id=prompt_ids["orchestrator"],
            model_config_id=_MODEL_ID,
            daily_limit_usd=Decimal("3.00"),
            now=_NOW,
        )
    async with factory() as session, session.begin():
        result = await execute_research_run(
            session,
            run_id=admission.run_id,
            committee=FixtureCommittee(conflicting_facts=True),
        )
    assert result.status == "abstained"
    async with factory() as session:
        events = tuple(
            await session.scalars(
                select(AgentResearchEventRecord.state)
                .where(AgentResearchEventRecord.run_id == admission.run_id)
                .order_by(AgentResearchEventRecord.sequence)
            )
        )
    assert events == ("prepared", "running", "validated", "audited", "abstained")


@pytest.mark.asyncio
async def test_worker_persists_failed_terminal_and_releases_budget_on_model_error(
    migrated_database_url: str,
) -> None:
    """A model exception must not strand a run in running with reserved budget."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    async with factory() as session, session.begin():
        admission = await admit_research_run(
            session,
            actor_id="fixture-worker",
            route="/internal/v1/research-runs",
            idempotency_key="model-error",
            request_hash="f" * 64,
            request=ResearchRequest(
                instrument_id=_INSTRUMENT_ID,
                data_snapshot_id=_SNAPSHOT_ID,
                decision_time=_NOW,
            ),
            prompt_version_id=prompt_ids["orchestrator"],
            model_config_id=_MODEL_ID,
            daily_limit_usd=Decimal("3.00"),
            now=_NOW,
        )
    committee = _FailingCommittee()
    async with factory() as session, session.begin():
        result = await execute_research_run(
            session,
            run_id=admission.run_id,
            committee=committee,
        )
    assert result.status == "failed"
    async with factory() as session:
        run = await session.get(AgentResearchRunRecord, admission.run_id)
        reservation = await session.scalar(
            select(AgentDailyBudgetReservationRecord).where(
                AgentDailyBudgetReservationRecord.run_id == admission.run_id
            )
        )
        events = tuple(
            await session.scalars(
                select(AgentResearchEventRecord.state)
                .where(AgentResearchEventRecord.run_id == admission.run_id)
                .order_by(AgentResearchEventRecord.sequence)
            )
        )
    assert run is not None and (run.status, run.retry_count) == ("failed", 1)
    assert committee.call_count == 2
    assert reservation is not None and reservation.state == "released"
    assert events == ("prepared", "running", "failed")


@pytest.mark.asyncio
async def test_worker_retries_once_with_the_identical_frozen_input_then_completes(
    migrated_database_url: str,
) -> None:
    """Rebuilding request/evidence/prompt on retry could change the point-in-time decision."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    async with factory() as session, session.begin():
        admission = await admit_research_run(
            session,
            actor_id="fixture-worker",
            route="/internal/v1/research-runs",
            idempotency_key="retry-once",
            request_hash="7" * 64,
            request=ResearchRequest(
                instrument_id=_INSTRUMENT_ID,
                data_snapshot_id=_SNAPSHOT_ID,
                decision_time=_NOW,
            ),
            prompt_version_id=prompt_ids["orchestrator"],
            model_config_id=_MODEL_ID,
            daily_limit_usd=Decimal("3.00"),
            now=_NOW,
        )
    replacement_content = "new prompt that must not enter an admitted run"
    async with factory() as session, session.begin():
        session.add(
            AgentPromptVersionRecord(
                id=UUID("0198d500-1000-7000-8000-000000001698"),
                role="bear",
                version="fixture-v2",
                content_hash=hashlib.sha256(replacement_content.encode()).hexdigest(),
                content=replacement_content,
            )
        )
    committee = _FailOnceCommittee()
    async with factory() as session, session.begin():
        result = await execute_research_run(
            session,
            run_id=admission.run_id,
            committee=committee,
        )
    async with factory() as session:
        run = await session.get(AgentResearchRunRecord, admission.run_id)
    assert result.status == "completed"
    assert run is not None and run.retry_count == 1
    assert committee.calls[0][0] is committee.calls[1][0]
    assert committee.calls[0][1] is committee.calls[1][1]
    assert committee.calls[0][2] is committee.calls[1][2]
    assert all(call[2].version == "fixture-v1" for call in committee.calls)


@pytest.mark.asyncio
async def test_worker_terminalizes_usage_limit_and_settlement_failures(
    migrated_database_url: str,
) -> None:
    """Accounting rejection must not strand audited work or an open reservation."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    admissions = []
    for suffix in ("usage-limit", "settlement-state"):
        async with factory() as session, session.begin():
            admissions.append(
                await admit_research_run(
                    session,
                    actor_id="fixture-worker",
                    route="/internal/v1/research-runs",
                    idempotency_key=suffix,
                    request_hash=("8" if suffix == "usage-limit" else "9") * 64,
                    request=ResearchRequest(
                        instrument_id=_INSTRUMENT_ID,
                        data_snapshot_id=_SNAPSHOT_ID,
                        decision_time=_NOW,
                    ),
                    prompt_version_id=prompt_ids["orchestrator"],
                    model_config_id=_MODEL_ID,
                    daily_limit_usd=Decimal("3.00"),
                    now=_NOW,
                )
            )
    async with factory() as session, session.begin():
        await release_daily_budget(session, run_id=admissions[1].run_id)
    results = []
    for admission, committee in zip(
        admissions, (_OversizedUsageCommittee(), FixtureCommittee()), strict=True
    ):
        async with factory() as session, session.begin():
            results.append(
                await execute_research_run(
                    session,
                    run_id=admission.run_id,
                    committee=committee,
                )
            )
    assert tuple(result.status for result in results) == ("failed", "failed")
    async with factory() as session:
        runs = tuple(
            await session.scalars(
                select(AgentResearchRunRecord).where(
                    AgentResearchRunRecord.id.in_(tuple(item.run_id for item in admissions))
                )
            )
        )
        reservations = tuple(
            await session.scalars(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id.in_(
                        tuple(item.run_id for item in admissions)
                    )
                )
            )
        )
    assert {run.status for run in runs} == {"failed"}
    states = {reservation.run_id: reservation.state for reservation in reservations}
    assert states[admissions[0].run_id] == "reserved"
    assert states[admissions[1].run_id] == "released"
    oversized = next(run for run in runs if run.id == admissions[0].run_id)
    assert oversized.input_tokens == 8_000_008


@pytest.mark.asyncio
async def test_responses_parser_failure_settles_all_known_provider_usage(
    migrated_database_url: str,
) -> None:
    """A response billed before strict parsing fails must never release its reservation."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    async with factory() as session, session.begin():
        admission = await admit_research_run(
            session,
            actor_id="responses-worker",
            route="/internal/v1/research-runs",
            idempotency_key="responses-parser-usage",
            request_hash="4" * 64,
            request=ResearchRequest(
                instrument_id=_INSTRUMENT_ID,
                data_snapshot_id=_SNAPSHOT_ID,
                decision_time=_NOW,
            ),
            prompt_version_id=prompt_ids["orchestrator"],
            model_config_id=_MODEL_ID,
            daily_limit_usd=Decimal("3.00"),
            now=_NOW,
        )
    settings = EngineSettings(
        database_url=migrated_database_url,
        agent_runtime_mode="responses",
        openai_api_key="fixture-key",
        openai_extraction_model="fixture-extraction",
        openai_reasoning_model="fixture-model",
    )
    fake = _InvalidDurableResponsesClient()
    assert await run_agent_worker_once(settings, responses_client=fake) is True
    async with factory() as session:
        run = await session.get(AgentResearchRunRecord, admission.run_id)
        reservation = await session.scalar(
            select(AgentDailyBudgetReservationRecord).where(
                AgentDailyBudgetReservationRecord.run_id == admission.run_id
            )
        )
    assert run is not None and reservation is not None
    assert (run.status, run.retry_count) == ("failed", 1)
    assert (run.input_tokens, run.output_tokens) == (24, 14)
    assert run.settled_cost_usd == Decimal("0.000052000000")
    assert (reservation.state, reservation.settled_usd) == (
        "settled",
        Decimal("0.000052000000"),
    )
    assert len(fake.responses.calls) == 2


@pytest.mark.asyncio
async def test_research_and_brief_apis_bind_as_of_url_and_authority_only(
    migrated_database_url: str,
) -> None:
    """Reads must not leak role payloads or select a run after the requested point in time."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    async with factory() as session, session.begin():
        admission = await admit_research_run(
            session,
            actor_id="fixture-worker",
            route="/internal/v1/research-runs",
            idempotency_key="api-run",
            request_hash="d" * 64,
            request=ResearchRequest(
                instrument_id=_INSTRUMENT_ID,
                data_snapshot_id=_SNAPSHOT_ID,
                decision_time=_NOW,
            ),
            prompt_version_id=prompt_ids["orchestrator"],
            model_config_id=_MODEL_ID,
            daily_limit_usd=Decimal("3.00"),
            now=_NOW,
        )
    async with factory() as session, session.begin():
        await execute_research_run(
            session,
            run_id=admission.run_id,
            committee=FixtureCommittee(),
        )
    instrument_path = (
        f"/internal/v1/instruments/{_INSTRUMENT_ID}/research?as_of=2026-08-24T12%3A00%3A00Z"
    )
    before_path = (
        f"/internal/v1/instruments/{_INSTRUMENT_ID}/research?as_of=2026-08-24T11%3A59%3A59Z"
    )
    brief_path = "/internal/v1/briefs/2026-08-24"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url, now=_NOW)),
        base_url="http://testserver",
    ) as client:
        research = await client.get(instrument_path, headers=headers("GET", instrument_path))
        before = await client.get(before_path, headers=headers("GET", before_path))
        brief = await client.get(brief_path, headers=headers("GET", brief_path))
    assert research.status_code == 200
    data = research.json()["data"]
    assert data["run_id"] == str(admission.run_id)
    assert data["instrument_id"] == str(_INSTRUMENT_ID)
    assert data["evidence"] == [
        {
            "source": "fixture",
            "object_id": "execution-evidence",
            "available_at": "2026-08-24T11:59:00Z",
            "location": f"memory://sha256/{'e' * 64}",
            "content_hash": "e" * 64,
        }
    ]
    serialized = json.dumps(data, sort_keys=True)
    assert "payload" not in serialized and "prompt_hash" not in serialized
    assert before.status_code == 404
    assert brief.status_code == 200
    brief_data = brief.json()["data"]
    assert set(brief_data) == {
        "session_date",
        "generated_at",
        "candidates",
        "holding_risks",
        "rejected",
        "budget_exhausted_watch",
        "evidence_completeness",
        "disagreements",
        "invalidations",
        "truncated",
    }
    assert brief_data["evidence_completeness"] == {
        "total_runs": 1,
        "complete": 0,
        "partial": 1,
        "failed": 0,
    }
    assert len(brief_data["disagreements"]) == 1


@pytest.mark.asyncio
async def test_research_post_api_is_atomic_idempotent_and_returns_one_safe_location(
    migrated_database_url: str,
) -> None:
    """The authenticated POST must replay one run and reject a changed body for the same key."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    path = "/internal/v1/research-runs"
    body = json.dumps(
        {
            "instrument_id": str(_INSTRUMENT_ID),
            "data_snapshot_id": str(_SNAPSHOT_ID),
            "decision_time": "2026-08-24T12:00:00Z",
        },
        separators=(",", ":"),
    ).encode()
    changed = body.replace(b"12:00:00Z", b"12:00:01Z")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        first = await client.post(
            path,
            content=body,
            headers={**headers("POST", path, body), "Idempotency-Key": "api-idempotency"},
        )
        replay = await client.post(
            path,
            content=body,
            headers={**headers("POST", path, body), "Idempotency-Key": "api-idempotency"},
        )
        conflict = await client.post(
            path,
            content=changed,
            headers={
                **headers("POST", path, changed),
                "Idempotency-Key": "api-idempotency",
            },
        )
    assert first.status_code == replay.status_code == 202
    assert first.json()["data"]["run_id"] == replay.json()["data"]["run_id"]
    assert first.headers["Location"] == replay.headers["Location"]
    assert first.headers["Location"].startswith("/api/v1/research-runs/")
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 1


@pytest.mark.asyncio
async def test_posted_fixture_run_reaches_terminal_through_worker_composition(
    migrated_database_url: str,
) -> None:
    """The production POST/worker composition must not leave admitted work permanently prepared."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    path = "/internal/v1/research-runs"
    body = json.dumps(
        {
            "instrument_id": str(_INSTRUMENT_ID),
            "data_snapshot_id": str(_SNAPSHOT_ID),
            "decision_time": "2026-08-24T12:00:00Z",
        },
        separators=(",", ":"),
    ).encode()
    engine_settings = EngineSettings(database_url=migrated_database_url)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        created = await client.post(
            path,
            content=body,
            headers={**headers("POST", path, body), "Idempotency-Key": "worker-composition"},
        )
        assert created.status_code == 202
        assert await run_agent_worker_once(engine_settings) is True
        run_id = created.json()["data"]["run_id"]
        detail_path = f"/internal/v1/research-runs/{run_id}"
        detail = await client.get(detail_path, headers=headers("GET", detail_path))
    assert detail.status_code == 200
    assert detail.json()["data"]["status"] == "completed"
    async with factory() as session:
        job_status = await session.scalar(
            select(Job.status).where(Job.payload["run_id"].as_string() == run_id)
        )
    assert job_status == "COMPLETED"


@pytest.mark.asyncio
async def test_responses_worker_reaches_terminal_with_injected_zero_network_transport(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    async with factory() as session, session.begin():
        admission = await admit_research_run(
            session,
            actor_id="responses-worker",
            route="/internal/v1/research-runs",
            idempotency_key="responses-durable",
            request_hash="8" * 64,
            request=ResearchRequest(
                instrument_id=_INSTRUMENT_ID,
                data_snapshot_id=_SNAPSHOT_ID,
                decision_time=_NOW,
            ),
            prompt_version_id=prompt_ids["orchestrator"],
            model_config_id=_MODEL_ID,
            daily_limit_usd=Decimal("3.00"),
            now=_NOW,
        )
    fake = _DurableResponsesClient()
    settings = EngineSettings(
        database_url=migrated_database_url,
        agent_runtime_mode="responses",
        openai_api_key="fixture-key",
        openai_extraction_model="fixture-extraction",
        openai_reasoning_model="fixture-model",
    )
    assert await run_agent_worker_once(settings, responses_client=fake) is True
    async with factory() as session:
        run = await session.get(AgentResearchRunRecord, admission.run_id)
    assert run is not None
    assert run.status == "completed"
    assert len(fake.responses.calls) == len(ROLE_ORDER)
    assert all(
        len(json.loads(call["input"][1]["content"])["evidence_refs"]) == 1
        for call in fake.responses.calls
    )


@pytest.mark.asyncio
async def test_production_fixture_authority_terminalizes_and_worker_continues_next_job(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    run_ids: list[UUID] = []
    for index in range(2):
        async with factory() as session, session.begin():
            admission = await admit_research_run(
                session,
                actor_id="production-worker",
                route="/internal/v1/research-runs",
                idempotency_key=f"production-fixture-authority-{index}",
                request_hash=str(index + 6) * 64,
                request=ResearchRequest(
                    instrument_id=_INSTRUMENT_ID,
                    data_snapshot_id=_SNAPSHOT_ID,
                    decision_time=_NOW,
                ),
                prompt_version_id=prompt_ids["orchestrator"],
                model_config_id=_MODEL_ID,
                daily_limit_usd=Decimal("3.00"),
                now=_NOW,
            )
            run_ids.append(admission.run_id)
    settings = EngineSettings(
        environment=Environment.PRODUCTION,
        database_url=migrated_database_url,
        object_store_endpoint="https://object-store.invalid",
        object_store_region="us-east-1",
        object_store_bucket="fixture",
        object_store_access_key_id="fixture-access",
        object_store_secret_access_key="fixture-secret",
        engine_hmac_current_key_id="fixture-current",
        engine_hmac_current_secret="fixture-hmac",
        agent_runtime_mode="responses",
        openai_api_key="fixture-key",
        openai_extraction_model="fixture-extraction",
        openai_reasoning_model="fixture-model",
    )
    fake = _DurableResponsesClient()
    assert await run_agent_worker_once(settings, responses_client=fake) is True
    assert await run_agent_worker_once(settings, responses_client=fake) is True
    async with factory() as session:
        runs = tuple(
            await session.scalars(
                select(AgentResearchRunRecord).where(AgentResearchRunRecord.id.in_(run_ids))
            )
        )
        jobs = tuple(
            await session.scalars(
                select(Job).where(Job.payload["run_id"].as_string().in_(map(str, run_ids)))
            )
        )
        reservations = tuple(
            await session.scalars(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id.in_(run_ids)
                )
            )
        )
    assert {run.status for run in runs} == {"abstained"}
    assert {job.status for job in jobs} == {"ABSTAINED"}
    assert {reservation.state for reservation in reservations} == {"released"}
    assert fake.responses.calls == []


@pytest.mark.asyncio
async def test_database_tools_are_complete_and_reject_authority_escape(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    await _seed_database_tool_authority(factory)
    async with factory() as session, session.begin():
        admission = await admit_research_run(
            session,
            actor_id="database-tools",
            route="/internal/v1/research-runs",
            idempotency_key="database-tools",
            request_hash="3" * 64,
            request=ResearchRequest(
                instrument_id=_INSTRUMENT_ID,
                data_snapshot_id=_SNAPSHOT_ID,
                decision_time=_NOW,
            ),
            prompt_version_id=prompt_ids["orchestrator"],
            model_config_id=_MODEL_ID,
            daily_limit_usd=Decimal("3.00"),
            now=_NOW,
            allow_fixture_price_authority=True,
        )
        price_authority = await session.scalar(select(PriceBarSetAuthority))
        assert price_authority is not None
        tools = await database_read_only_tools(session, admission.run, allow_fixture_evidence=True)
        assert tools.configured_names == EXPECTED_READ_ONLY_TOOL_NAMES
        result = await tools.execute(
            "get_security_snapshot",
            {
                "instrument_id": str(_INSTRUMENT_ID),
                "data_snapshot_id": str(_SNAPSHOT_ID),
                "as_of": "2026-08-24T12:00:00Z",
            },
        )
        assert {item["evidence_ref_id"] for item in result["items"]} == {
            str(_EVIDENCE_ID),
            "0198d500-1000-7000-8000-000000001701",
            "0198d500-1000-7000-8000-000000001702",
            "0198d500-1000-7000-8000-000000001703",
            str(price_authority.evidence_ref_id),
        }
        financial = await tools.execute(
            "get_financial_facts",
            {
                "instrument_id": str(_INSTRUMENT_ID),
                "data_snapshot_id": str(_SNAPSHOT_ID),
                "as_of": "2026-08-24T12:00:00Z",
                "fields": ["revenue"],
            },
        )
        assert [(item["fact"], item["value"]) for item in financial["items"]] == [
            ("revenue", "100.000000000000")
        ]
        filings = await tools.execute(
            "search_filing_evidence",
            {
                "instrument_id": str(_INSTRUMENT_ID),
                "data_snapshot_id": str(_SNAPSHOT_ID),
                "as_of": "2026-08-24T12:00:00Z",
                "forms": ["10-Q"],
                "filed_from": "2026-01-01",
                "filed_to": "2026-08-24",
            },
        )
        assert [item["accession_number"] for item in filings["items"]] == [
            "0000000001-26-000001"
        ]
        price = await tools.execute(
            "get_price_features",
            {
                "instrument_id": str(_INSTRUMENT_ID),
                "data_snapshot_id": str(_SNAPSHOT_ID),
                "as_of": "2026-08-24T12:00:00Z",
                "features": ["close", "sma_20", "sma_200", "rsi_14", "volatility_20"],
            },
        )
        assert price["status"] == "ok"
        assert price["items"] == [
            {
                "evidence_ref_id": str(price_authority.evidence_ref_id),
                "source": "fixture",
                "feed": "adjusted-daily",
                "adjustment": "split_dividend",
                "as_of_session": "2026-08-23",
                "features": {
                    "close": "100.000000000000",
                    "sma_20": "100.000000000000",
                    "sma_200": "100.000000000000",
                    "rsi_14": "50.000000000000",
                    "volatility_20": "0.000000000000",
                },
            }
        ]
        assert price["items"][0]["evidence_ref_id"] != str(
            _PARQUET_PRICE_EVIDENCE_ID
        )
        macro = await tools.execute(
            "get_macro_snapshot",
            {
                "data_snapshot_id": str(_SNAPSHOT_ID),
                "as_of": "2026-08-24T12:00:00Z",
                "series": ["DGS10"],
            },
        )
        assert [(item["series"], item["value"]) for item in macro["items"]] == [
            ("DGS10", "4.250000000000")
        ]
        unfrozen_macro = await tools.execute(
            "get_macro_snapshot",
            {
                "data_snapshot_id": str(_SNAPSHOT_ID),
                "as_of": "2026-08-24T12:00:00Z",
                "series": ["DFF"],
            },
        )
        assert (unfrozen_macro["status"], unfrozen_macro["items"]) == ("unavailable", [])
        with pytest.raises(ToolInputError, match="exceeds frozen"):
            await tools.execute(
                "get_security_snapshot",
                {
                    "instrument_id": "0198d500-1000-7000-8000-000000001699",
                    "data_snapshot_id": str(_SNAPSHOT_ID),
                    "as_of": "2026-08-24T12:00:00Z",
                },
            )
        with pytest.raises(ToolInputError, match="decision-time"):
            await tools.execute(
                "get_security_snapshot",
                {
                    "instrument_id": str(_INSTRUMENT_ID),
                    "data_snapshot_id": str(_SNAPSHOT_ID),
                    "as_of": "2026-08-24T12:00:01Z",
                },
            )
        portfolio = await tools.execute(
            "get_portfolio_exposure",
            {
                "portfolio_snapshot_id": "0198d500-1000-7000-8000-000000001699",
                "instrument_id": str(_INSTRUMENT_ID),
                "as_of": "2026-08-24T12:00:00Z",
            },
        )
        diagnostics = await tools.execute(
            "get_portfolio_diagnostics",
            {
                "portfolio_snapshot_id": "0198d500-1000-7000-8000-000000001699",
                "diagnostic_run_id": "0198d500-1000-7000-8000-000000001698",
                "as_of": "2026-08-24T12:00:00Z",
            },
        )
        comparison = await tools.execute(
            "get_proposal_comparison",
            {
                "proposal_group_id": "0198d500-1000-7000-8000-000000001697",
                "as_of": "2026-08-24T12:00:00Z",
            },
        )
        assert {
            (portfolio["status"], portfolio["reason"]),
            (diagnostics["status"], diagnostics["reason"]),
            (comparison["status"], comparison["reason"]),
        } == {("unavailable", "authority_not_frozen_for_research_run")}


@pytest.mark.asyncio
async def test_admission_selects_and_freezes_db_resident_price_authority(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    await _seed_database_tool_authority(factory, link_price=False)
    async with factory() as session, session.begin():
        admission = await _admit_price_run(
            session,
            prompt_ids,
            allow_fixture_price_authority=True,
        )
    price_items = [
        item
        for item in admission.run.evidence_manifest
        if item["provider_object_id"].startswith("price-bar-set:")
    ]
    assert len(price_items) == 1
    assert price_items[0]["location"].startswith("db://price-bar-set/")
    assert "@sha256:" in price_items[0]["location"]


async def _admit_price_run(
    session: AsyncSession,
    prompt_ids: Mapping[AgentRole, UUID],
    *,
    allow_fixture_price_authority: bool,
) -> ResearchAdmission:
    return await admit_research_run(
        session,
        actor_id=f"price-authority-{allow_fixture_price_authority}",
        route="/internal/v1/research-runs",
        idempotency_key=f"price-authority-{allow_fixture_price_authority}",
        request_hash=("8" if allow_fixture_price_authority else "9") * 64,
        request=ResearchRequest(
            instrument_id=_INSTRUMENT_ID,
            data_snapshot_id=_SNAPSHOT_ID,
            decision_time=_NOW,
        ),
        prompt_version_id=prompt_ids["orchestrator"],
        model_config_id=_MODEL_ID,
        daily_limit_usd=Decimal("3.00"),
        now=_NOW,
        allow_fixture_price_authority=allow_fixture_price_authority,
    )


def _price_tool_payload() -> dict[str, object]:
    return {
        "instrument_id": str(_INSTRUMENT_ID),
        "data_snapshot_id": str(_SNAPSHOT_ID),
        "as_of": _NOW,
        "features": ["close", "sma_20", "rsi_14", "volatility_20"],
    }


async def _prepared_price_run(database_url: str):
    factory = session_factory(EngineSettings(database_url=database_url))
    prompt_ids = await _seed_authority(factory)
    await _seed_database_tool_authority(factory, link_price=False, bar_count=200)
    async with factory() as session, session.begin():
        admission = await _admit_price_run(
            session,
            prompt_ids,
            allow_fixture_price_authority=True,
        )
    return factory, admission.run


async def _corrupt_price_evidence_content(factory) -> None:
    corrupted_hash = "f" * 64
    async with factory() as session, session.begin():
        await session.execute(text("ALTER TABLE research_evidence_refs DISABLE TRIGGER USER"))
        try:
            await session.execute(
                text(
                    "UPDATE research_evidence_refs "
                    "SET content_hash=:content_hash, location=:location "
                    "WHERE id = ("
                    "SELECT evidence_ref_id FROM price_bar_set_authorities "
                    "WHERE price_series_manifest_id=:manifest_id)"
                ),
                {
                    "content_hash": corrupted_hash,
                    "location": (
                        f"db://price-bar-set/{_PRICE_MANIFEST_ID}"
                        f"@sha256:{corrupted_hash}"
                    ),
                    "manifest_id": _PRICE_MANIFEST_ID,
                },
            )
        finally:
            await session.execute(text("ALTER TABLE research_evidence_refs ENABLE TRIGGER USER"))


@pytest.mark.asyncio
async def test_price_tool_refreshes_evidence_changed_after_tool_construction(
    migrated_database_url: str,
) -> None:
    factory, run = await _prepared_price_run(migrated_database_url)
    async with factory() as session:
        tools = await database_read_only_tools(session, run, allow_fixture_evidence=True)
        await _corrupt_price_evidence_content(factory)
        result = await tools.execute("get_price_features", _price_tool_payload())
    assert (result["status"], result["reason"], result["items"]) == (
        "unavailable",
        "invalid_price_bar_set_authority",
        [],
    )


@pytest.mark.asyncio
async def test_price_tool_degrades_when_evidence_is_corrupt_before_construction(
    migrated_database_url: str,
) -> None:
    factory, run = await _prepared_price_run(migrated_database_url)
    await _corrupt_price_evidence_content(factory)
    async with factory() as session:
        tools = await database_read_only_tools(session, run, allow_fixture_evidence=True)
        result = await tools.execute("get_price_features", _price_tool_payload())
        financial = await tools.execute(
            "get_financial_facts",
            {
                "instrument_id": str(_INSTRUMENT_ID),
                "data_snapshot_id": str(_SNAPSHOT_ID),
                "as_of": _NOW,
                "fields": ["revenue"],
            },
        )
    assert (result["status"], result["reason"], result["items"]) == (
        "unavailable",
        "invalid_price_bar_set_authority",
        [],
    )
    assert financial["items"] == []


@pytest.mark.parametrize(
    "damage_statement",
    (
        "DELETE FROM agent_data_snapshot_evidence WHERE authority_kind='price'",
        "UPDATE agent_data_snapshot_evidence "
        "SET authority_kind='research', selected_at=NULL "
        "WHERE authority_kind='price'",
    ),
    ids=("deleted", "retagged"),
)
@pytest.mark.asyncio
async def test_price_tool_treats_missing_expected_binding_as_corruption(
    migrated_database_url: str,
    damage_statement: str,
) -> None:
    factory, run = await _prepared_price_run(migrated_database_url)
    async with factory() as session, session.begin():
        await session.execute(
            text("ALTER TABLE agent_data_snapshot_evidence DISABLE TRIGGER USER")
        )
        try:
            await session.execute(text(damage_statement))
        finally:
            await session.execute(
                text("ALTER TABLE agent_data_snapshot_evidence ENABLE TRIGGER USER")
            )
    async with factory() as session:
        tools = await database_read_only_tools(session, run, allow_fixture_evidence=True)
        result = await tools.execute("get_price_features", _price_tool_payload())
    assert (result["status"], result["reason"], result["items"]) == (
        "unavailable",
        "invalid_price_bar_set_authority",
        [],
    )


@pytest.mark.asyncio
async def test_price_tool_never_returns_ok_for_flagged_stored_bars(
    migrated_database_url: str,
) -> None:
    factory, run = await _prepared_price_run(migrated_database_url)
    async with factory() as session, session.begin():
        await session.execute(text("ALTER TABLE price_bar_records DISABLE TRIGGER USER"))
        try:
            await session.execute(
                text(
                    "UPDATE price_bar_records SET quality_flags='[\"suspect\"]'::jsonb "
                    "WHERE price_series_manifest_id=:manifest_id"
                ),
                {"manifest_id": _PRICE_MANIFEST_ID},
            )
        finally:
            await session.execute(text("ALTER TABLE price_bar_records ENABLE TRIGGER USER"))
    async with factory() as session:
        tools = await database_read_only_tools(session, run, allow_fixture_evidence=True)
        result = await tools.execute("get_price_features", _price_tool_payload())
    assert (result["status"], result["reason"]) == (
        "unavailable",
        "flagged_price_bars",
    )
    assert result["items"] == []


@pytest.mark.asyncio
async def test_price_tool_degrades_when_stored_bar_set_hash_is_corrupted(
    migrated_database_url: str,
) -> None:
    factory, run = await _prepared_price_run(migrated_database_url)
    async with factory() as session, session.begin():
        await session.execute(text("ALTER TABLE price_bar_records DISABLE TRIGGER USER"))
        try:
            await session.execute(
                text(
                    "UPDATE price_bar_records "
                    "SET close = close + 0.010000000000 "
                    "WHERE id = ("
                    "SELECT id FROM price_bar_records "
                    "WHERE price_series_manifest_id = :manifest_id "
                    "ORDER BY session DESC LIMIT 1)"
                ),
                {"manifest_id": _PRICE_MANIFEST_ID},
            )
        finally:
            await session.execute(text("ALTER TABLE price_bar_records ENABLE TRIGGER USER"))
    async with factory() as session:
        tools = await database_read_only_tools(session, run, allow_fixture_evidence=True)
        result = await tools.execute("get_price_features", _price_tool_payload())
    assert (result["status"], result["reason"]) == (
        "unavailable",
        "invalid_price_bar_set_authority",
    )
    assert result["items"] == []


@pytest.mark.asyncio
async def test_price_tool_retains_input_errors_for_caller_scope_violations(
    migrated_database_url: str,
) -> None:
    factory, run = await _prepared_price_run(migrated_database_url)
    invalid_payloads = []
    for key, value in (
        ("instrument_id", UUID("0198d500-1000-7000-8000-000000009999")),
        ("data_snapshot_id", UUID("0198d500-1000-7000-8000-000000009998")),
        ("as_of", _NOW + timedelta(seconds=1)),
    ):
        payload = _price_tool_payload()
        payload[key] = value
        invalid_payloads.append(payload)
    async with factory() as session:
        tools = await database_read_only_tools(session, run, allow_fixture_evidence=True)
        for payload in invalid_payloads:
            with pytest.raises(ToolInputError, match="exceeds frozen"):
                await tools.execute("get_price_features", payload)


@pytest.mark.asyncio
async def test_price_features_report_unavailable_without_frozen_authority(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    async with factory() as session, session.begin():
        admission = await _admit_price_run(
            session,
            prompt_ids,
            allow_fixture_price_authority=True,
        )
        tools = await database_read_only_tools(
            session, admission.run, allow_fixture_evidence=True
        )
        result = await tools.execute("get_price_features", _price_tool_payload())
    assert (result["status"], result["reason"], result["items"]) == (
        "unavailable",
        "no_frozen_price_authority",
        [],
    )


@pytest.mark.asyncio
async def test_fixture_price_authority_requires_explicit_fixture_admission(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    await _seed_database_tool_authority(factory, link_price=False, price_source="fixture")
    with pytest.raises(ValueError, match="price authority"):
        async with factory() as session, session.begin():
            await _admit_price_run(
                session, prompt_ids, allow_fixture_price_authority=False
            )
    async with factory() as session, session.begin():
        admission = await _admit_price_run(
            session, prompt_ids, allow_fixture_price_authority=True
        )
    assert any(
        item["provider_object_id"].startswith("price-bar-set:")
        for item in admission.run.evidence_manifest
    )


@pytest.mark.asyncio
async def test_existing_fixture_price_authority_is_revalidated_for_runtime(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    await _seed_database_tool_authority(factory, price_source="fixture")
    with pytest.raises(ValueError, match="existing price authority is invalid"):
        async with factory() as session, session.begin():
            await _admit_price_run(
                session, prompt_ids, allow_fixture_price_authority=False
            )
    async with factory() as session:
        assert (
            await session.scalar(select(func.count()).select_from(AgentResearchRunRecord))
            == 0
        )
        assert await session.scalar(select(func.count()).select_from(Job)) == 0
        assert (
            await session.scalar(
                select(func.count()).select_from(AgentDailyBudgetReservationRecord)
            )
            == 0
        )


@pytest.mark.asyncio
async def test_flagged_price_bars_leave_admission_without_price_authority(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    await _seed_database_tool_authority(
        factory,
        link_price=False,
        price_source="fixture",
        price_quality_flags=("suspect",),
    )
    async with factory() as session, session.begin():
        admission = await _admit_price_run(
            session,
            prompt_ids,
            allow_fixture_price_authority=True,
        )
    assert not any(
        item["provider_object_id"].startswith("price-bar-set:")
        for item in admission.run.evidence_manifest
    )


@pytest.mark.asyncio
async def test_price_features_report_partial_when_history_warmup_is_insufficient(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    await _seed_database_tool_authority(factory, bar_count=10)
    async with factory() as session, session.begin():
        admission = await admit_research_run(
            session,
            actor_id="price-warmup",
            route="/internal/v1/research-runs",
            idempotency_key="price-warmup",
            request_hash="8" * 64,
            request=ResearchRequest(
                instrument_id=_INSTRUMENT_ID,
                data_snapshot_id=_SNAPSHOT_ID,
                decision_time=_NOW,
            ),
            prompt_version_id=prompt_ids["orchestrator"],
            model_config_id=_MODEL_ID,
            daily_limit_usd=Decimal("3.00"),
            now=_NOW,
            allow_fixture_price_authority=True,
        )
        tools = await database_read_only_tools(
            session, admission.run, allow_fixture_evidence=True
        )
        price = await tools.execute(
            "get_price_features",
            {
                "instrument_id": str(_INSTRUMENT_ID),
                "data_snapshot_id": str(_SNAPSHOT_ID),
                "as_of": _NOW,
                "features": ["close", "sma_20", "rsi_14", "volatility_20"],
            },
        )
    assert (price["status"], price["reason"]) == (
        "partial",
        "insufficient_price_history",
    )
    assert price["items"][0]["features"] == {"close": "100.000000000000"}
    assert price["items"][0]["missing_features"] == [
        "sma_20",
        "rsi_14",
        "volatility_20",
    ]


@pytest.mark.asyncio
async def test_price_owner_trigger_rejects_nonprice_cross_scope_and_future_authority(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    await _seed_database_tool_authority(factory, link_price=False)
    async with factory() as session:
        price_authority = await session.scalar(select(PriceBarSetAuthority))
        assert price_authority is not None
        price_evidence_id = price_authority.evidence_ref_id
    other_instrument = UUID("0198d500-1000-7000-8000-000000001709")
    async with factory() as session, session.begin():
        session.add(
            Instrument(
                id=other_instrument,
                asset_type="equity",
                exchange="NASDAQ",
                currency="USD",
                tick_size=Decimal("0.01"),
                risk_bucket="equity",
            )
        )
    cases = (
        (_EVIDENCE_ID, _INSTRUMENT_ID, "fixture", _NOW),
        (
            price_evidence_id,
            _INSTRUMENT_ID,
            "alpaca",
            _NOW,
        ),
        (
            price_evidence_id,
            _INSTRUMENT_ID,
            "fixture",
            _NOW + timedelta(seconds=1),
        ),
        (
            price_evidence_id,
            other_instrument,
            "fixture",
            _NOW,
        ),
    )
    for evidence_id, instrument_id, source, selected_at in cases:
        with pytest.raises(DBAPIError):
            async with factory() as session, session.begin():
                evidence = await session.get(ResearchEvidenceRef, evidence_id)
                assert evidence is not None
                session.add(
                    AgentDataSnapshotEvidenceRecord(
                        data_snapshot_id=_SNAPSHOT_ID,
                        instrument_id=instrument_id,
                        evidence_ref_id=evidence.id,
                        authority_kind="price",
                        selected_at=selected_at,
                        source=source,
                        provider_object_id=evidence.provider_object_id,
                        revision=evidence.revision,
                        available_at=evidence.available_at,
                        location=evidence.location,
                        content_hash=evidence.content_hash,
                    )
                )
                await session.flush()


@pytest.mark.asyncio
async def test_price_owner_trigger_rejects_parquet_evidence_as_price_authority(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    await _seed_database_tool_authority(factory, link_price=False)
    with pytest.raises(DBAPIError):
        async with factory() as session, session.begin():
            manifest = await session.get(
                PriceSeriesManifest,
                UUID("0198d500-1000-7000-8000-000000001707"),
            )
            assert manifest is not None and manifest.evidence_ref_id is not None
            evidence = await session.get(ResearchEvidenceRef, manifest.evidence_ref_id)
            assert evidence is not None
            session.add(
                AgentDataSnapshotEvidenceRecord(
                    data_snapshot_id=_SNAPSHOT_ID,
                    instrument_id=_INSTRUMENT_ID,
                    evidence_ref_id=evidence.id,
                    authority_kind="price",
                    selected_at=_NOW,
                    source=evidence.source,
                    provider_object_id=evidence.provider_object_id,
                    revision=evidence.revision,
                    available_at=evidence.available_at,
                    location=evidence.location,
                    content_hash=evidence.content_hash,
                )
            )
            await session.flush()


@pytest.mark.asyncio
async def test_price_owner_trigger_rejects_a_future_bar(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    await _seed_database_tool_authority(
        factory, link_price=False, future_last_bar=True
    )
    async with factory() as session:
        price_authority = await session.scalar(select(PriceBarSetAuthority))
        assert price_authority is not None
        evidence = await session.get(ResearchEvidenceRef, price_authority.evidence_ref_id)
        assert evidence is not None
        session.add(
            AgentDataSnapshotEvidenceRecord(
                data_snapshot_id=_SNAPSHOT_ID,
                instrument_id=_INSTRUMENT_ID,
                evidence_ref_id=evidence.id,
                authority_kind="price",
                selected_at=_NOW,
                source=evidence.source,
                provider_object_id=evidence.provider_object_id,
                revision=evidence.revision,
                available_at=evidence.available_at,
                location=evidence.location,
                content_hash=evidence.content_hash,
            )
        )
        with pytest.raises(DBAPIError, match="price evidence authority is invalid"):
            await session.flush()


@pytest.mark.asyncio
async def test_nonfixture_price_features_use_only_frozen_postgres_without_transport(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory, include_base_evidence=False)
    await _seed_database_tool_authority(
        factory, link_price=False, price_source="alpaca"
    )
    async with factory() as session, session.begin():
        admission = await admit_research_run(
            session,
            actor_id="production-price",
            route="/internal/v1/research-runs",
            idempotency_key="production-price",
            request_hash="7" * 64,
            request=ResearchRequest(
                instrument_id=_INSTRUMENT_ID,
                data_snapshot_id=_SNAPSHOT_ID,
                decision_time=_NOW,
            ),
            prompt_version_id=prompt_ids["orchestrator"],
            model_config_id=_MODEL_ID,
            daily_limit_usd=Decimal("3.00"),
            now=_NOW,
        )
        tools = await database_read_only_tools(
            session, admission.run, allow_fixture_evidence=False
        )
        result = await tools.execute(
            "get_price_features",
            {
                "instrument_id": str(_INSTRUMENT_ID),
                "data_snapshot_id": str(_SNAPSHOT_ID),
                "as_of": _NOW,
                "features": ["close", "sma_20"],
            },
        )
    assert result["status"] == "ok"
    assert result["items"][0]["source"] == "alpaca"
    assert result["items"][0]["features"] == {
        "close": "100.000000000000",
        "sma_20": "100.000000000000",
    }


@pytest.mark.asyncio
async def test_frozen_price_series_rejects_late_bar_growth(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    await _seed_database_tool_authority(factory, link_price=False)
    async with factory() as session, session.begin():
        await admit_research_run(
            session,
            actor_id="price-late-bar",
            route="/internal/v1/research-runs",
            idempotency_key="price-late-bar",
            request_hash="6" * 64,
            request=ResearchRequest(
                instrument_id=_INSTRUMENT_ID,
                data_snapshot_id=_SNAPSHOT_ID,
                decision_time=_NOW,
            ),
            prompt_version_id=prompt_ids["orchestrator"],
            model_config_id=_MODEL_ID,
            daily_limit_usd=Decimal("3.00"),
            now=_NOW,
            allow_fixture_price_authority=True,
        )
    observed_at = _NOW - timedelta(hours=12)
    with pytest.raises(DBAPIError, match="frozen price series"):
        async with factory() as session, session.begin():
            session.add(
                PriceBarRecord(
                    price_series_manifest_id=UUID(
                        "0198d500-1000-7000-8000-000000001707"
                    ),
                    instrument_id=_INSTRUMENT_ID,
                    session=observed_at.date(),
                    open=Decimal("100"),
                    high=Decimal("101"),
                    low=Decimal("99"),
                    close=Decimal("100"),
                    volume=Decimal("1000"),
                    observed_at=observed_at,
                    available_at=observed_at + timedelta(minutes=1),
                    received_at=observed_at + timedelta(minutes=2),
                    revision="late-r1",
                    quality_flags=[],
                )
            )
            await session.flush()
    authority_mutations = (
        "UPDATE price_series_manifests SET feed = 'changed' "
        "WHERE id = '0198d500-1000-7000-8000-000000001707'",
        "UPDATE data_artifacts SET byte_size = byte_size + 1 "
        "WHERE id = '0198d500-1000-7000-8000-000000001706'",
        "DELETE FROM price_series_manifests "
        "WHERE id = '0198d500-1000-7000-8000-000000001707'",
        "DELETE FROM data_artifacts "
        "WHERE id = '0198d500-1000-7000-8000-000000001706'",
    )
    for statement in authority_mutations:
        with pytest.raises(DBAPIError, match="frozen price authority is immutable"):
            async with factory() as session, session.begin():
                await session.execute(text(statement))


@pytest.mark.asyncio
async def test_responses_function_loop_uses_default_frozen_database_executor(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    async with factory() as session, session.begin():
        admission = await admit_research_run(
            session,
            actor_id="responses-worker",
            route="/internal/v1/research-runs",
            idempotency_key="responses-tool-loop",
            request_hash="2" * 64,
            request=ResearchRequest(
                instrument_id=_INSTRUMENT_ID,
                data_snapshot_id=_SNAPSHOT_ID,
                decision_time=_NOW,
            ),
            prompt_version_id=prompt_ids["orchestrator"],
            model_config_id=_MODEL_ID,
            daily_limit_usd=Decimal("3.00"),
            now=_NOW,
        )
    fake = _ToolLoopResponsesClient()
    settings = EngineSettings(
        database_url=migrated_database_url,
        agent_runtime_mode="responses",
        openai_api_key="fixture-key",
        openai_extraction_model="fixture-extraction",
        openai_reasoning_model="fixture-model",
    )
    assert await run_agent_worker_once(settings, responses_client=fake) is True
    async with factory() as session:
        run = await session.get(AgentResearchRunRecord, admission.run_id)
    assert run is not None and run.status == "completed"
    assert len(fake.responses.calls) == 16
    tool_outputs = [
        item
        for call in fake.responses.calls
        for item in call["input"]
        if item.get("type") == "function_call_output"
    ]
    assert len({item["call_id"] for item in tool_outputs}) == 8
    assert all(str(_EVIDENCE_ID) in item["output"] for item in tool_outputs)


@pytest.mark.asyncio
async def test_admission_freezes_exact_evidence_manifest_and_hash(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    async with factory() as session, session.begin():
        admission = await admit_research_run(
            session,
            actor_id="evidence-manifest",
            route="/internal/v1/research-runs",
            idempotency_key="freeze-evidence",
            request_hash="6" * 64,
            request=ResearchRequest(
                instrument_id=_INSTRUMENT_ID,
                data_snapshot_id=_SNAPSHOT_ID,
                decision_time=_NOW,
            ),
            prompt_version_id=prompt_ids["orchestrator"],
            model_config_id=_MODEL_ID,
            daily_limit_usd=Decimal("3.00"),
            now=_NOW,
        )
    manifest = admission.run.evidence_manifest
    assert manifest == [
        {
            "id": str(_EVIDENCE_ID),
            "source": "fixture",
            "provider_object_id": "execution-evidence",
            "revision": "v1",
            "available_at": "2026-08-24T11:59:00Z",
            "location": f"memory://sha256/{'e' * 64}",
            "content_hash": "e" * 64,
        }
    ]
    assert (
        admission.run.evidence_hash
        == hashlib.sha256(
            json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
    )


@pytest.mark.asyncio
async def test_post_admission_snapshot_link_cannot_change_worker_evidence_membership(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    async with factory() as session, session.begin():
        admission = await admit_research_run(
            session,
            actor_id="evidence-manifest",
            route="/internal/v1/research-runs",
            idempotency_key="ignore-late-link",
            request_hash="5" * 64,
            request=ResearchRequest(
                instrument_id=_INSTRUMENT_ID,
                data_snapshot_id=_SNAPSHOT_ID,
                decision_time=_NOW,
            ),
            prompt_version_id=prompt_ids["orchestrator"],
            model_config_id=_MODEL_ID,
            daily_limit_usd=Decimal("3.00"),
            now=_NOW,
        )
    late_id = UUID("0198d500-1000-7000-8000-000000001697")
    async with factory() as session, session.begin():
        session.add(
            ResearchEvidenceRef(
                id=late_id,
                source="fixture",
                provider_object_id="late-evidence",
                revision="v1",
                available_at=_NOW - timedelta(seconds=30),
                location=f"memory://sha256/{'4' * 64}",
                content_hash="4" * 64,
                byte_length=42,
            )
        )
        await session.flush()
        session.add(
            AgentDataSnapshotEvidenceRecord(
                data_snapshot_id=_SNAPSHOT_ID,
                instrument_id=_INSTRUMENT_ID,
                evidence_ref_id=late_id,
                source="fixture",
                provider_object_id="late-evidence",
                revision="v1",
                available_at=_NOW - timedelta(seconds=30),
                location=f"memory://sha256/{'4' * 64}",
                content_hash="4" * 64,
            )
        )
    assert await run_agent_worker_once(EngineSettings(database_url=migrated_database_url)) is True
    async with factory() as session:
        bound_ids = set(
            await session.scalars(
                select(AgentResearchOutputEvidenceRecord.evidence_ref_id)
                .join(
                    AgentResearchOutputRecord,
                    AgentResearchOutputRecord.id == AgentResearchOutputEvidenceRecord.output_id,
                )
                .where(AgentResearchOutputRecord.run_id == admission.run_id)
            )
        )
    assert bound_ids == {_EVIDENCE_ID}


@pytest.mark.asyncio
async def test_server_authoritative_first_run_admission_selects_frozen_snapshot_and_time(
    migrated_database_url: str,
) -> None:
    """A first-run client must obtain snapshot and decision authority instead of inventing IDs."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    path = (
        f"/internal/v1/instruments/{_INSTRUMENT_ID}/research-admission"
        "?as_of=2026-08-24T12%3A00%3A00Z"
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url, now=_NOW)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
        future_path = (
            f"/internal/v1/instruments/{_INSTRUMENT_ID}/research-admission"
            "?as_of=2036-08-24T12%3A00%3A00Z"
        )
        future = await client.get(future_path, headers=headers("GET", future_path))
    assert response.status_code == 200
    assert response.json()["data"] == {
        "eligible": True,
        "reason": None,
        "instrument_id": str(_INSTRUMENT_ID),
        "data_snapshot_id": str(_SNAPSHOT_ID),
        "decision_time": "2026-08-24T12:00:00Z",
    }
    assert future.status_code == 200
    assert future.json()["data"] == {
        "eligible": False,
        "reason": "decision_time_in_future",
        "instrument_id": str(_INSTRUMENT_ID),
        "data_snapshot_id": None,
        "decision_time": "2036-08-24T12:00:00Z",
    }


@pytest.mark.asyncio
async def test_admission_rejects_incomplete_eight_role_prompt_authority(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory, omitted_role="bear")
    path = (
        f"/internal/v1/instruments/{_INSTRUMENT_ID}/research-admission"
        "?as_of=2026-08-24T12%3A00%3A00Z"
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url, now=_NOW)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
    assert response.json()["data"]["eligible"] is False
    assert response.json()["data"]["reason"] == "prompt_authority_unavailable"


@pytest.mark.asyncio
async def test_admission_ignores_newer_snapshot_without_instrument_evidence_authority(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    unrelated = UUID("0198d500-1000-7000-8000-000000001699")
    async with factory() as session, session.begin():
        session.add(
            DataSnapshot(
                id=unrelated,
                source="fixture",
                provider_object_id="unrelated-newer",
                revision="v1",
                observed_at=_NOW,
                available_at=_NOW,
                received_at=_NOW,
                schema_version="1",
                payload={},
            )
        )
    path = (
        f"/internal/v1/instruments/{_INSTRUMENT_ID}/research-admission"
        "?as_of=2026-08-24T12%3A00%3A00Z"
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url, now=_NOW)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
    assert response.json()["data"]["eligible"] is True
    assert response.json()["data"]["data_snapshot_id"] == str(_SNAPSHOT_ID)


@pytest.mark.asyncio
async def test_00656_hk_worker_suppresses_fundamental_stance_and_records_missing_authority(
    migrated_database_url: str,
) -> None:
    """The read-only HK holding cannot receive a fundamental buy or sell stance."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory, symbol="00656.HK")
    async with factory() as session, session.begin():
        admission = await admit_research_run(
            session,
            actor_id="fixture-worker",
            route="/internal/v1/research-runs",
            idempotency_key="hk-read-only",
            request_hash="e" * 64,
            request=ResearchRequest(
                instrument_id=_INSTRUMENT_ID,
                data_snapshot_id=_SNAPSHOT_ID,
                decision_time=_NOW,
            ),
            prompt_version_id=prompt_ids["orchestrator"],
            model_config_id=_MODEL_ID,
            daily_limit_usd=Decimal("3.00"),
            now=_NOW,
        )
    async with factory() as session, session.begin():
        result = await execute_research_run(
            session,
            run_id=admission.run_id,
            committee=FixtureCommittee(),
        )
    assert result.intent is not None
    assert (result.intent.stance, result.intent.exposure_action) == ("watch", "none")
    fundamental = next(vote for vote in result.intent.role_votes if vote.role == "fundamental")
    assert fundamental.vote == "abstain"
    assert result.intent.quality_status == "partial"
    assert "HKEX fundamental authority is unavailable." in result.intent.uncertainties


async def _seed_authority(
    factory,
    *,
    symbol: str = "ABC",
    omitted_role: AgentRole | None = None,
    include_base_evidence: bool = True,
) -> dict[str, UUID]:
    prompts = load_prompt_assets()
    prompt_ids = {
        role: UUID(f"0198d500-1000-7000-8000-0000000016{10 + index:02d}")
        for index, role in enumerate(ROLE_ORDER)
    }
    async with factory() as session, session.begin():
        records: list[object] = [
                Instrument(
                    id=_INSTRUMENT_ID,
                    asset_type="equity",
                    exchange="NASDAQ",
                    currency="USD",
                    tick_size=Decimal("0.01"),
                    risk_bucket="equity",
                ),
                InstrumentAlias(
                    instrument_id=_INSTRUMENT_ID,
                    symbol=symbol,
                    alias_type="ticker",
                    valid_from=date(2020, 1, 1),
                    valid_to=None,
                ),
                DataSnapshot(
                    id=_SNAPSHOT_ID,
                    source="fixture",
                    provider_object_id="execution-snapshot",
                    revision="v1",
                    observed_at=_NOW,
                    available_at=_NOW,
                    received_at=_NOW,
                    schema_version="1",
                    payload={},
                ),
                AgentModelConfigRecord(
                    id=_MODEL_ID,
                    model_id="fixture-model",
                    version="fixture-pricing-v1",
                    config_hash="2" * 64,
                    max_input_tokens=1_000_000,
                    max_tool_calls=8,
                    max_output_tokens=10_000,
                    input_usd_per_million=Decimal("1.00"),
                    output_usd_per_million=Decimal("2.00"),
                    pricing_available_at=_NOW - timedelta(days=1),
                    pricing_expires_at=_NOW + timedelta(days=30),
                ),
        ]
        if include_base_evidence:
            records.append(
                ResearchEvidenceRef(
                    id=_EVIDENCE_ID,
                    source="fixture",
                    provider_object_id="execution-evidence",
                    revision="v1",
                    available_at=_NOW - timedelta(minutes=1),
                    location=f"memory://sha256/{'e' * 64}",
                    content_hash="e" * 64,
                    byte_length=42,
                )
            )
        session.add_all(records)
        await session.flush()
        if include_base_evidence:
            session.add(
                AgentDataSnapshotEvidenceRecord(
                    data_snapshot_id=_SNAPSHOT_ID,
                    instrument_id=_INSTRUMENT_ID,
                    evidence_ref_id=_EVIDENCE_ID,
                    source="fixture",
                    provider_object_id="execution-evidence",
                    revision="v1",
                    available_at=_NOW - timedelta(minutes=1),
                    location=f"memory://sha256/{'e' * 64}",
                    content_hash="e" * 64,
                )
            )
        session.add_all(
            AgentPromptVersionRecord(
                id=prompt_ids[role],
                role=role,
                version=prompts[role].version,
                content_hash=prompts[role].content_hash,
                content=prompts[role].content,
            )
            for role in ROLE_ORDER
            if role != omitted_role
        )
    return prompt_ids


async def _seed_database_tool_authority(
    factory,
    *,
    link_price: bool = True,
    bar_count: int = 200,
    price_source: str = "fixture",
    future_last_bar: bool = False,
    price_quality_flags: tuple[str, ...] = (),
) -> None:
    sec_evidence_id = UUID("0198d500-1000-7000-8000-000000001701")
    macro_value_id = UUID("0198d500-1000-7000-8000-000000001702")
    macro_calendar_id = UUID("0198d500-1000-7000-8000-000000001703")
    unfrozen_value_id = UUID("0198d500-1000-7000-8000-000000001704")
    filing_id = UUID("0198d500-1000-7000-8000-000000001705")
    price_evidence_id = UUID("0198d500-1000-7000-8000-000000001708")

    def bar_time(index: int) -> datetime:
        if future_last_bar and index == bar_count - 1:
            return _NOW + timedelta(seconds=1)
        return _NOW - timedelta(days=bar_count - index)

    async with factory() as session, session.begin():
        evidence = (
            ResearchEvidenceRef(
                id=sec_evidence_id,
                source="sec",
                provider_object_id="0000000001-26-000001",
                revision="v1",
                available_at=_NOW - timedelta(minutes=4),
                location=f"memory://sha256/{'1' * 64}",
                content_hash="1" * 64,
                byte_length=10,
            ),
            ResearchEvidenceRef(
                id=macro_value_id,
                source="fred",
                provider_object_id="DGS10:2026-08-23",
                revision="2026-08-24",
                available_at=_NOW - timedelta(minutes=3),
                location=f"memory://sha256/{'2' * 64}",
                content_hash="2" * 64,
                byte_length=10,
            ),
            ResearchEvidenceRef(
                id=macro_calendar_id,
                source="fred",
                provider_object_id="calendar:2026-08-24",
                revision="2026-08-24",
                available_at=_NOW - timedelta(minutes=2),
                location=f"memory://sha256/{'3' * 64}",
                content_hash="3" * 64,
                byte_length=10,
            ),
            ResearchEvidenceRef(
                id=unfrozen_value_id,
                source="fred",
                provider_object_id="DFF:2026-08-23",
                revision="2026-08-24",
                available_at=_NOW - timedelta(minutes=2),
                location=f"memory://sha256/{'4' * 64}",
                content_hash="4" * 64,
                byte_length=10,
            ),
            ResearchEvidenceRef(
                id=price_evidence_id,
                source=price_source,
                provider_object_id=(
                    "price-series-manifest:0198d500-1000-7000-8000-000000001707"
                ),
                revision="e" * 64,
                available_at=_NOW - timedelta(minutes=1),
                location=f"memory://sha256/{'e' * 64}",
                content_hash="e" * 64,
                byte_length=10,
            ),
        )
        session.add_all(evidence)
        await session.flush()
        filing = Filing(
            id=filing_id,
            instrument_id=_INSTRUMENT_ID,
            evidence_ref_id=sec_evidence_id,
            source="sec",
            provider_object_id="0000000001-26-000001",
            accession_number="0000000001-26-000001",
            form="10-Q",
            filed_at=_NOW - timedelta(days=2),
            accepted_at=_NOW - timedelta(days=2),
            revision="v1",
        )
        session.add(filing)
        await session.flush()
        session.add(
            FinancialFactRecord(
                filing_id=filing_id,
                evidence_ref_id=sec_evidence_id,
                instrument_id=_INSTRUMENT_ID,
                canonical_fact="revenue",
                raw_taxonomy="us-gaap",
                raw_tag="RevenueFromContractWithCustomerExcludingAssessedTax",
                unit="USD",
                value=Decimal("100"),
                period_kind="duration",
                period_start=date(2026, 4, 1),
                period_end=date(2026, 6, 30),
                fiscal_year=2026,
                fiscal_period="Q2",
                form="10-Q",
                filed_at=_NOW - timedelta(days=2),
                accepted_at=_NOW - timedelta(days=2),
                accession_number="0000000001-26-000001",
                frame=None,
                revision="v1",
            )
        )
        session.add_all(
            (
                MacroObservation(
                    source="fred",
                    series="DGS10",
                    observed_at=_NOW - timedelta(days=1),
                    available_at=_NOW - timedelta(minutes=3),
                    revision="2026-08-24",
                    value=Decimal("4.25"),
                    unit="percent",
                    provider_object_id="DGS10:2026-08-23",
                    authority_version="research_macro_v1",
                    real_time_start=date(2026, 8, 24),
                    real_time_end=date(9999, 12, 31),
                    evidence_ref_id=macro_value_id,
                    calendar_evidence_ref_id=macro_calendar_id,
                    calendar_source="fred",
                    calendar_provider_object_id="calendar:2026-08-24",
                    calendar_revision="2026-08-24",
                    display_sanity_only=False,
                ),
                MacroObservation(
                    source="fred",
                    series="DFF",
                    observed_at=_NOW - timedelta(days=1),
                    available_at=_NOW - timedelta(minutes=2),
                    revision="2026-08-24",
                    value=Decimal("5.00"),
                    unit="percent",
                    provider_object_id="DFF:2026-08-23",
                    authority_version="research_macro_v1",
                    real_time_start=date(2026, 8, 24),
                    real_time_end=date(9999, 12, 31),
                    evidence_ref_id=unfrozen_value_id,
                    calendar_evidence_ref_id=macro_calendar_id,
                    calendar_source="fred",
                    calendar_provider_object_id="calendar:2026-08-24",
                    calendar_revision="2026-08-24",
                    display_sanity_only=False,
                ),
            )
        )
        session.add(
            DataArtifact(
                id=UUID("0198d500-1000-7000-8000-000000001706"),
                uri=f"memory://sha256/{'e' * 64}",
                sha256="e" * 64,
                byte_size=10,
                mime_type="application/vnd.apache.parquet",
                schema_version="price_bars_v1",
                source=price_source,
                observed_at=_NOW - timedelta(days=bar_count),
                available_at=_NOW - timedelta(minutes=1),
                received_at=_NOW - timedelta(minutes=1),
            )
        )
        await session.flush()
        manifest = PriceSeriesManifest(
            id=UUID("0198d500-1000-7000-8000-000000001707"),
            evidence_ref_id=price_evidence_id,
            data_artifact_id=UUID("0198d500-1000-7000-8000-000000001706"),
            instrument=str(_INSTRUMENT_ID),
            source=price_source,
            feed=("sip" if price_source == "alpaca" else "adjusted-daily"),
            delivery_mode=("delayed" if price_source == "alpaca" else "fixture"),
            adjustment="split_dividend",
            interval="1Day",
            start_at=_NOW - timedelta(days=bar_count),
            end_at=bar_time(bar_count - 1),
            row_count=bar_count,
        )
        session.add(manifest)
        await session.flush()
        session.add_all(
            PriceBarRecord(
                price_series_manifest_id=UUID(
                    "0198d500-1000-7000-8000-000000001707"
                ),
                instrument_id=_INSTRUMENT_ID,
                session=bar_time(index).date(),
                open=Decimal("100"),
                high=Decimal("101"),
                low=Decimal("99"),
                close=Decimal("100"),
                volume=Decimal("1000"),
                observed_at=bar_time(index),
                available_at=bar_time(index) + timedelta(minutes=1),
                received_at=bar_time(index) + timedelta(minutes=2),
                revision="fixture-r1",
                quality_flags=list(price_quality_flags),
            )
            for index in range(bar_count)
        )
        await session.flush()
        finalized = await finalize_price_bar_set(session, manifest)
        price_evidence = None if finalized is None else finalized[1]
        linked = [evidence[0], evidence[1], evidence[2]]
        if link_price and price_evidence is not None:
            linked.append(price_evidence)
        session.add_all(
            AgentDataSnapshotEvidenceRecord(
                data_snapshot_id=_SNAPSHOT_ID,
                instrument_id=_INSTRUMENT_ID,
                evidence_ref_id=record.id,
                authority_kind=("price" if record is price_evidence else "research"),
                selected_at=(_NOW if record is price_evidence else None),
                source=record.source,
                provider_object_id=record.provider_object_id,
                revision=record.revision,
                available_at=record.available_at,
                location=record.location,
                content_hash=record.content_hash,
            )
            for record in linked
        )


class _FailingCommittee:
    def __init__(self) -> None:
        self.call_count = 0

    async def analyze(
        self,
        role: AgentRole,
        request: ResearchRequest,
        evidence_refs: tuple[EvidenceRef, ...],
        prompt: PromptAsset,
    ) -> CommitteeRoleOutput:
        del role, request, evidence_refs, prompt
        self.call_count += 1
        raise RuntimeError("deterministic fixture failure")


class _DurableResponses:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def create(self, **kwargs: object) -> dict[str, object]:
        self.calls.append(dict(kwargs))
        evidence = {
            "source": "fixture",
            "object_id": "execution-evidence",
            "available_at": "2026-08-24T11:59:00Z",
            "location": f"memory://sha256/{'e' * 64}",
            "content_hash": "e" * 64,
        }
        return {
            "output": [{"type": "message"}],
            "output_text": json.dumps(
                {
                    "vote": "support",
                    "claims": [
                        {
                            "text": "Frozen evidence supports review.",
                            "evidence_refs": [evidence],
                        }
                    ],
                    "conflicting_facts": False,
                }
            ),
            "usage": {"input_tokens": 10, "output_tokens": 5},
        }


class _DurableResponsesClient:
    def __init__(self) -> None:
        self.responses = _DurableResponses()


class _InvalidDurableResponses(_DurableResponses):
    async def create(self, **kwargs: object) -> dict[str, object]:
        self.calls.append(dict(kwargs))
        return {
            "output": [{"type": "message"}],
            "output_text": "{}",
            "usage": {"input_tokens": 12, "output_tokens": 7},
        }


class _InvalidDurableResponsesClient:
    def __init__(self) -> None:
        self.responses = _InvalidDurableResponses()


class _ToolLoopResponses(_DurableResponses):
    async def create(self, **kwargs: object) -> dict[str, object]:
        self.calls.append(dict(kwargs))
        if len(self.calls) % 2:
            return {
                "output": [
                    {
                        "type": "function_call",
                        "call_id": f"call-{len(self.calls)}",
                        "name": "get_security_snapshot",
                        "arguments": json.dumps(
                            {
                                "instrument_id": str(_INSTRUMENT_ID),
                                "data_snapshot_id": str(_SNAPSHOT_ID),
                                "as_of": "2026-08-24T12:00:00Z",
                            }
                        ),
                    }
                ],
                "usage": {"input_tokens": 10, "output_tokens": 5},
            }
        evidence = {
            "source": "fixture",
            "object_id": "execution-evidence",
            "available_at": "2026-08-24T11:59:00Z",
            "location": f"memory://sha256/{'e' * 64}",
            "content_hash": "e" * 64,
        }
        return {
            "output": [{"type": "message"}],
            "output_text": json.dumps(
                {
                    "vote": "support",
                    "claims": [
                        {
                            "text": "Frozen evidence supports review.",
                            "evidence_refs": [evidence],
                        }
                    ],
                    "conflicting_facts": False,
                }
            ),
            "usage": {"input_tokens": 10, "output_tokens": 5},
        }


class _ToolLoopResponsesClient:
    def __init__(self) -> None:
        self.responses = _ToolLoopResponses()


class _FailOnceCommittee:
    def __init__(self) -> None:
        self.calls: list[tuple[ResearchRequest, tuple[EvidenceRef, ...], PromptAsset]] = []
        self.fixture = FixtureCommittee()

    async def analyze(
        self,
        role: AgentRole,
        request: ResearchRequest,
        evidence_refs: tuple[EvidenceRef, ...],
        prompt: PromptAsset,
    ) -> CommitteeRoleOutput:
        self.calls.append((request, evidence_refs, prompt))
        if len(self.calls) == 1:
            raise RuntimeError("first deterministic fixture failure")
        return await self.fixture.analyze(role, request, evidence_refs, prompt)


class _OversizedUsageCommittee(FixtureCommittee):
    async def analyze(
        self,
        role: AgentRole,
        request: ResearchRequest,
        evidence_refs: tuple[EvidenceRef, ...],
        prompt: PromptAsset,
    ) -> CommitteeRoleOutput:
        output = await super().analyze(role, request, evidence_refs, prompt)
        return output.model_copy(update={"input_tokens": 1_000_001})
