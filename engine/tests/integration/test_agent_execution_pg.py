"""Real-PostgreSQL durable fixture committee lifecycle tests."""

import json
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
from nexus.agents.execution import LIFECYCLE, execute_research_run
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import admit_research_run, release_daily_budget
from nexus.domain.evidence import EvidenceRef
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    AgentResearchEventRecord,
    AgentResearchOutputEvidenceRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.persistence.models.runtime import Job
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from nexus.worker import run_agent_worker_once
from sqlalchemy import func, select

_NOW = datetime(2026, 8, 24, 12, tzinfo=UTC)
_INSTRUMENT_ID = UUID("0198d500-1000-7000-8000-000000001601")
_SNAPSHOT_ID = UUID("0198d500-1000-7000-8000-000000001602")
_MODEL_ID = UUID("0198d500-1000-7000-8000-000000001603")
_EVIDENCE_ID = UUID("0198d500-1000-7000-8000-000000001604")


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
            evidence_ref_ids=(_EVIDENCE_ID,),
        )
    assert result.status == "completed"
    assert result.intent is not None
    assert (result.intent.stance, result.intent.exposure_action) == (
        "buy_candidate",
        "eligible_for_increase",
    )
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
async def test_fixture_worker_abstains_and_releases_budget_without_authority(
    migrated_database_url: str,
) -> None:
    """Missing evidence cannot support synthesis and must release the unused reservation."""
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
            evidence_ref_ids=(),
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
            evidence_ref_ids=(_EVIDENCE_ID,),
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
            evidence_ref_ids=(_EVIDENCE_ID,),
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
    committee = _FailOnceCommittee()
    async with factory() as session, session.begin():
        result = await execute_research_run(
            session,
            run_id=admission.run_id,
            committee=committee,
            evidence_ref_ids=(_EVIDENCE_ID,),
        )
    async with factory() as session:
        run = await session.get(AgentResearchRunRecord, admission.run_id)
    assert result.status == "completed"
    assert run is not None and run.retry_count == 1
    assert committee.calls[0][0] is committee.calls[1][0]
    assert committee.calls[0][1] is committee.calls[1][1]
    assert committee.calls[0][2] is committee.calls[1][2]


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
                    evidence_ref_ids=(_EVIDENCE_ID,),
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
    assert {reservation.state for reservation in reservations} == {"released"}


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
            evidence_ref_ids=(_EVIDENCE_ID,),
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
        "complete": 1,
        "partial": 0,
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
            evidence_ref_ids=(_EVIDENCE_ID,),
        )
    assert result.intent is not None
    assert (result.intent.stance, result.intent.exposure_action) == ("watch", "none")
    fundamental = next(vote for vote in result.intent.role_votes if vote.role == "fundamental")
    assert fundamental.vote == "abstain"
    assert result.intent.quality_status == "partial"
    assert "HKEX fundamental authority is unavailable." in result.intent.uncertainties


async def _seed_authority(factory, *, symbol: str = "ABC") -> dict[str, UUID]:
    prompts = load_prompt_assets()
    prompt_ids = {
        role: UUID(f"0198d500-1000-7000-8000-0000000016{10 + index:02d}")
        for index, role in enumerate(ROLE_ORDER)
    }
    async with factory() as session, session.begin():
        session.add_all(
            (
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
                ResearchEvidenceRef(
                    id=_EVIDENCE_ID,
                    source="fixture",
                    provider_object_id="execution-evidence",
                    revision="v1",
                    available_at=_NOW - timedelta(minutes=1),
                    location=f"memory://sha256/{'e' * 64}",
                    content_hash="e" * 64,
                    byte_length=42,
                ),
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
        )
    return prompt_ids


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
