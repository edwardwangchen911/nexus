"""Real-PostgreSQL durable fixture committee lifecycle tests."""

import hashlib
import json
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import UUID

import httpx
import pytest
from helpers.fosun_lineage import seed_fosun_daily_lineage
from helpers.portfolio_api import app, headers
from nexus.agents.codex_staged import StagedCodexCommittee
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
    ProviderCapability,
)
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.research import Filing, FinancialFactRecord, ResearchEvidenceRef
from nexus.persistence.models.runtime import IdempotencyRecord, IntegrationStatus, Job
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
_DRIFT_INSTRUMENT_ID = UUID("0198d500-1000-7000-8000-000000001712")


@pytest.mark.asyncio
@pytest.mark.parametrize("model_fails", [False, True, "invalid_citation"])
@pytest.mark.parametrize("staged", [False, True, "v3", "v3_96k"])
async def test_codex_sdk_worker_persists_one_batch_and_api_usage_without_retries(
    migrated_database_url,
    monkeypatch,
    model_fails,
    staged,
):
    from nexus.agents import codex_sdk, codex_staged
    from nexus.agents.responses_budget import UsageSnapshot
    from nexus.ops.openai_local import prepare_database
    from nexus.portfolio.research import read_position_research

    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    factory = session_factory(settings)
    await _seed_authority(factory, include_base_evidence=False)
    await _seed_database_tool_authority(
        factory, link_price=False, bar_count=1, price_quality_flags=("missing",)
    )
    await prepare_database(
        settings,
        staged=staged is True,
        structured_synthesis=staged in {"v3", "v3_96k"},
        aggregate_input_tokens=96000 if staged == "v3_96k" else 64000,
    )
    async with factory() as session, session.begin():
        model = await session.scalar(
            select(AgentModelConfigRecord).where(
                AgentModelConfigRecord.version
                == (
                    "codex-sdk-chatgpt-v3-input96k"
                    if staged == "v3_96k"
                    else codex_sdk.STRUCTURED_CONFIG_VERSION
                    if staged in {"v3", "v3_96k"}
                    else codex_sdk.STAGED_CONFIG_VERSION
                    if staged
                    else codex_sdk.CONFIG_VERSION
                )
            )
        )
        admission = await admit_research_run(
            session,
            actor_id="sdk-test",
            route="/internal/v1/research-runs",
            idempotency_key="sdk-batch",
            request_hash="a" * 64,
            request=ResearchRequest(
                instrument_id=_INSTRUMENT_ID, data_snapshot_id=_SNAPSHOT_ID, decision_time=_NOW
            ),
            prompt_version_id=None,
            model_config_id=model.id,
            daily_limit_usd=Decimal("3"),
            now=datetime(2026, 9, 2, tzinfo=UTC),
        )
    async with factory() as session:
        index = await read_position_research(session, (_INSTRUMENT_ID,), read_at=datetime.now(UTC))
        assert index[_INSTRUMENT_ID].status == "prepared"
        assert index[_INSTRUMENT_ID].quality_status is None
        assert index[_INSTRUMENT_ID].run_id == admission.run_id
    batch = {
        "roles": [
            {
                "role": role,
                "vote": "abstain",
                "claims": [
                    {"text": "冻结财报包含季度收入；缺少合格行情，保持观察。", "citations": [0]}
                ],
                "conflicting_facts": False,
            }
            for role in ROLE_ORDER
        ]
    }
    invoke = AsyncMock(
        return_value=(
            json.dumps(batch),
            UsageSnapshot(input_tokens=300, output_tokens=80, provider_calls=1),
        )
    )
    if staged:

        async def staged_invoke(payload, *, output_model):
            if model_fails is True and payload["stage"] == "critique":
                raise TimeoutError("synthetic-secret-canary")
            roles = [prompt["role"] for prompt in payload["prompts"]]
            reviewed = (
                []
                if payload["stage"] == "research"
                else ["bull:0"]
                if payload["stage"] == "critique"
                else ["bear:0", "evidence_audit:0"]
            )
            response = {
                "roles": [
                    {**item, "reviewed_claim_ids": reviewed}
                    for item in batch["roles"]
                    if item["role"] in roles
                ]
            }
            if staged in {"v3", "v3_96k"} and payload["stage"] == "synthesis":
                response = {
                    "synthesis_assessment": {
                        "stance": "buy_candidate",
                        "claims": [
                            {"text": "收入证据可作研究参考，行情缺口阻止比较。", "citations": [0]}
                        ],
                        "reviews": [
                            {
                                "claim_id": claim_id,
                                "status": "accepted",
                                "rationale": "认可行情缺口。",
                                "citations": [0],
                                "resolved": False,
                                "missing_evidence": ["合格行情"],
                                "prevented_conclusion": "尚不能比较方案。",
                            }
                            for claim_id in reviewed
                        ],
                        "review_conclusion": "insufficient",
                        "uncertainties": ["估值仍未知。"],
                        "conflicting_facts": False,
                    }
                }
            if model_fails == "invalid_citation" and payload["stage"] == "critique":
                response["roles"][0]["claims"] = [{"text": "invalid", "citations": [999]}]
            return json.dumps(response), UsageSnapshot(
                input_tokens=30000 if staged == "v3_96k" else 300, output_tokens=80
            )

        invoke.side_effect = staged_invoke
    elif model_fails:
        invoke.side_effect = RuntimeError("offline simulated model failure")
    monkeypatch.setattr(codex_staged if staged else codex_sdk, "invoke_codex", invoke)
    assert await run_agent_worker_once(settings) is True
    assert invoke.await_count == (2 if model_fails else 3) if staged else invoke.await_count == 1
    submitted = invoke.call_args.args[0]
    from unit.agents.test_codex_sdk import _restore_view

    frozen_inputs = _restore_view(submitted["frozen_inputs"])
    assert frozen_inputs["financial_facts"]["items"][0]["period_start"] == "2026-04-01"
    async with factory() as session:
        run = await session.get(AgentResearchRunRecord, admission.run_id)
        assert run.retry_count == 0
        assert run.status == ("failed" if model_fails else "completed")
        job = await session.scalar(select(Job).where(Job.payload["run_id"].astext == str(run.id)))
        assert job.max_attempts == 1
        index = await read_position_research(session, (_INSTRUMENT_ID,), read_at=datetime.now(UTC))
        research = index[_INSTRUMENT_ID]
        assert research.status == run.status
        assert research.data_snapshot_id == _SNAPSHOT_ID
        assert research.decision_time == _NOW
        assert research.newer_inputs_available is False
        assert research.quality_status == (None if model_fails else "partial")
        if model_fails:
            assert research.summary is None and not research.uncertainties
        else:
            assert "Price authority is unavailable." in research.uncertainties
            assert research.summary
        if not model_fails:
            assert (run.input_tokens, run.output_tokens) == (
                (90000, 240) if staged == "v3_96k" else (900, 240) if staged else (300, 80)
            )
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(AgentResearchOutputRecord)
                    .where(
                        AgentResearchOutputRecord.run_id == run.id,
                        AgentResearchOutputRecord.role.in_(ROLE_ORDER),
                    )
                )
                == 8
            )
        if staged:
            outputs = tuple(
                await session.scalars(
                    select(AgentResearchOutputRecord)
                    .where(
                        AgentResearchOutputRecord.run_id == run.id,
                        AgentResearchOutputRecord.role.in_(ROLE_ORDER),
                    )
                    .order_by(AgentResearchOutputRecord.sequence)
                )
            )
            assert len(outputs) == (5 if model_fails else 8)
            assert outputs[0].payload["stage"] == "research"
            if not model_fails:
                assert outputs[-1].payload["stage"] == "synthesis"
                assert outputs[-1].payload["reviewed_claim_ids"] == ["bear:0", "evidence_audit:0"]
    assert await run_agent_worker_once(settings) is False
    path = f"/internal/v1/research-runs/{admission.run_id}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)), base_url="http://testserver"
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["billing_mode"] == "chatgpt_subscription"
    if staged:
        trace = data["stage_trace"]
        assert trace["status"] == "verified"
        assert trace["evidence_hash"] == run.evidence_hash
        assert [stage["status"] for stage in trace["stages"]] == (
            ["complete", "unavailable", "unavailable"] if model_fails else ["complete"] * 3
        )
        assert trace["stages"][0]["input_tokens"] == (30000 if staged == "v3_96k" else 300)
        assert trace["stages"][0]["input_hash"] is not None
        assert trace["stages"][0]["frozen_inputs_hash"] is not None
        if not model_fails:
            assert len({stage["frozen_inputs_hash"] for stage in trace["stages"]}) == 1
            assert trace["stages"][2]["roles"][0]["reviewed_claim_ids"] == [
                "bear:0",
                "evidence_audit:0",
            ]
    else:
        assert data["stage_trace"] is None
    if not model_fails:
        assert data["input_tokens"] == (90000 if staged == "v3_96k" else 900 if staged else 300)
        assert data["output_tokens"] == (240 if staged else 80)
        assert data["intent"]["stance"] == "watch"
        assert data["intent"]["exposure_action"] == "none"
        if staged in {"v3", "v3_96k"}:
            assessment = data["intent"]["synthesis_assessment"]
            assert assessment["stance"] == "buy_candidate"
            assert len(assessment["reviews"]) == 2
            assert not any(item["resolved"] for item in assessment["reviews"])
            assert (
                data["stage_trace"]["stages"][2]["roles"][0]["synthesis_assessment"] == assessment
            )
            assert outputs[-1].payload["synthesis_assessment"] == assessment
        else:
            assert "synthesis_assessment" not in data["intent"]

        from nexus.agents.snapshots import prepare_research_snapshot

        async with factory() as session, session.begin():
            newer = await prepare_research_snapshot(
                session, _INSTRUMENT_ID, as_of=_NOW + timedelta(hours=1)
            )
            assert newer is not None
        async with factory() as session:
            index = await read_position_research(
                session, (_INSTRUMENT_ID,), read_at=datetime.now(UTC)
            )
            assert index[_INSTRUMENT_ID].newer_inputs_available is True
            assert index[_INSTRUMENT_ID].run_id == admission.run_id
            assert index[_INSTRUMENT_ID].data_snapshot_id == _SNAPSHOT_ID
            past = await read_position_research(
                session, (_INSTRUMENT_ID,), read_at=_NOW - timedelta(seconds=1)
            )
            assert past[_INSTRUMENT_ID].status == "not_started"
            assert past[_INSTRUMENT_ID].run_id is None
    elif staged:
        assert data["failed_stage_input"]["stage"] == "critique"
        prior_claim_ids = tuple(item["claim_id"] for item in submitted["previous_claims"])
        assert data["failed_stage_input"]["request_hash"] == codex_sdk.codex_request_hash(
            submitted,
            codex_staged._stage_model_for_prior_claims("critique", prior_claim_ids),
        )
        assert (
            data["failed_stage_input"]["frozen_inputs_hash"]
            == (data["stage_trace"]["stages"][0]["frozen_inputs_hash"])
        )
        assert data["abstention_reason"] == (
            "model_stage_citation_invalid"
            if model_fails == "invalid_citation"
            else "model_usage_uncertain"
        )
        multiplier = 2 if model_fails == "invalid_citation" else 1
        assert data["input_tokens"] == (30000 if staged == "v3_96k" else 300) * multiplier
        assert data["output_tokens"] == 80 * multiplier


@pytest.mark.asyncio
async def test_registering_staged_config_does_not_activate_it_for_default_admission(
    migrated_database_url,
):
    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    factory = session_factory(settings)
    await _seed_authority(factory)
    # The shared test fixture freezes created_at defaults; use explicit ordered timestamps.
    async with factory() as session, session.begin():
        for ordinal, version in enumerate(
            (
                "codex-sdk-chatgpt-v1",
                "codex-sdk-chatgpt-v2",
                "codex-sdk-chatgpt-v3",
                "codex-sdk-strategy-rnd-v1",
            ),
            start=1,
        ):
            session.add(
                AgentModelConfigRecord(
                    model_id="gpt-5.4-mini",
                    version=version,
                    config_hash=str(ordinal) * 64,
                    max_input_tokens=64_000,
                    max_tool_calls=0,
                    max_output_tokens=8_192,
                    input_usd_per_million=Decimal("0"),
                    output_usd_per_million=Decimal("0"),
                    pricing_available_at=_NOW,
                    pricing_expires_at=_NOW + timedelta(days=30),
                    created_at=_NOW + timedelta(hours=ordinal),
                )
            )
    async with factory() as session, session.begin():
        for explicit in (None, "codex-sdk-chatgpt-v2", "codex-sdk-chatgpt-v3"):
            staged_id = await session.scalar(
                select(AgentModelConfigRecord.id).where(AgentModelConfigRecord.version == explicit)
            )
            admission = await admit_research_run(
                session,
                actor_id="sdk-test",
                route="/internal/v1/research-runs",
                idempotency_key=f"sdk-opt-in-{explicit}",
                request_hash="b" * 64,
                request=ResearchRequest(
                    instrument_id=_INSTRUMENT_ID,
                    data_snapshot_id=_SNAPSHOT_ID,
                    decision_time=_NOW,
                ),
                prompt_version_id=None,
                model_config_id=staged_id if explicit else None,
                daily_limit_usd=Decimal("3"),
                now=datetime(2026, 9, 2, tzinfo=UTC),
            )
            selected = await session.get(AgentModelConfigRecord, admission.run.model_config_id)
            assert selected.version == (explicit or "codex-sdk-chatgpt-v1")


@pytest.mark.asyncio
async def test_research_rejects_strategy_rnd_config_at_read_and_write_admission(
    migrated_database_url,
    monkeypatch,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory, model_version="codex-sdk-strategy-rnd-v1")
    monkeypatch.setattr("nexus.api.routes.research.utc_now", lambda: _NOW)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url, now=_NOW)),
        base_url="http://testserver",
    ) as client:
        path = (
            f"/internal/v1/instruments/{_INSTRUMENT_ID}/research-admission"
            "?as_of=2026-08-24T12%3A00%3A00Z"
        )
        response = await client.get(path, headers=headers("GET", path))
        assert response.status_code == 200
        assert response.json()["data"]["eligible"] is False
        assert response.json()["data"]["reason"] == "model_pricing_unavailable"
        path = "/internal/v1/research-runs"
        body = json.dumps(
            {
                "instrument_id": str(_INSTRUMENT_ID),
                "data_snapshot_id": str(_SNAPSHOT_ID),
                "decision_time": "2026-08-24T12:00:00Z",
            }
        ).encode()
        response = await client.post(
            path,
            content=body,
            headers={
                **headers("POST", path, body),
                "Idempotency-Key": "rnd-not-research",
            },
        )
        assert response.status_code == 409
    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="point-in-time data authority"):
            await admit_research_run(
                session,
                actor_id="test",
                route="/internal/v1/research-runs",
                idempotency_key="explicit-rnd-not-research",
                request_hash="a" * 64,
                request=ResearchRequest(
                    instrument_id=_INSTRUMENT_ID,
                    data_snapshot_id=_SNAPSHOT_ID,
                    decision_time=_NOW,
                ),
                prompt_version_id=None,
                model_config_id=_MODEL_ID,
                daily_limit_usd=Decimal("3"),
                now=_NOW,
            )
        assert await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(Job)) == 0
        assert (
            await session.scalar(
                select(func.count()).select_from(AgentDailyBudgetReservationRecord)
            )
            == 0
        )


@pytest.mark.asyncio
async def test_fixture_worker_does_not_synthesize_under_strategy_rnd_identity(
    migrated_database_url,
):
    from nexus.worker import _committee

    settings = EngineSettings(database_url=migrated_database_url)
    factory = session_factory(settings)
    await _seed_authority(factory, model_version="codex-sdk-strategy-rnd-v1")
    async with factory() as session:
        committee = await _committee(
            session,
            settings,
            AgentResearchRunRecord(model_config_id=_MODEL_ID),
            responses_client=None,
            tools=None,
        )
        with pytest.raises(RuntimeError, match="committee is unavailable"):
            await committee.analyze(
                "market_state",
                ResearchRequest(
                    instrument_id=_INSTRUMENT_ID,
                    data_snapshot_id=_SNAPSHOT_ID,
                    decision_time=_NOW,
                ),
                (
                    EvidenceRef(
                        source="fixture",
                        object_id="test",
                        available_at=_NOW,
                        location="memory://test",
                        content_hash="e" * 64,
                    ),
                ),
                load_prompt_assets()["market_state"],
            )


@pytest.mark.asyncio
async def test_old_staged_direction_is_rejected_by_readers_without_rewriting_history(
    migrated_database_url,
    monkeypatch,
):
    from nexus.agents import execution
    from nexus.portfolio.research import read_position_research

    settings = EngineSettings(database_url=migrated_database_url)
    factory = session_factory(settings)
    await _seed_authority(factory, model_version="codex-sdk-chatgpt-v2")
    original_synthesis = execution.synthesize_intent

    def historical_synthesis(*args, **kwargs):
        return original_synthesis(*args, **kwargs).model_copy(
            update={
                "stance": "buy_candidate",
                "exposure_action": "eligible_for_increase",
                "quality_status": "complete",
                "evidence_audit": "complete",
            }
        )

    monkeypatch.setattr(execution, "synthesize_intent", historical_synthesis)
    async with factory() as session, session.begin():
        admission = await admit_research_run(
            session,
            actor_id="historical-test",
            route="/internal/v1/research-runs",
            idempotency_key="historical-staged-direction",
            request_hash="c" * 64,
            request=ResearchRequest(
                instrument_id=_INSTRUMENT_ID, data_snapshot_id=_SNAPSHOT_ID, decision_time=_NOW
            ),
            prompt_version_id=None,
            model_config_id=_MODEL_ID,
            daily_limit_usd=Decimal("3"),
            now=_NOW,
        )
        result = await execute_research_run(
            session, run_id=admission.run_id, committee=FixtureCommittee()
        )
        assert result.status == "completed"
    async with factory() as session:
        before = (
            (
                await session.execute(
                    text(
                        "SELECT md5(row_to_json(t)::text) FROM agent_research_outputs t "
                        "WHERE run_id=:run_id ORDER BY sequence"
                    ),
                    {"run_id": admission.run_id},
                )
            )
            .scalars()
            .all()
        )
        index = await read_position_research(session, (_INSTRUMENT_ID,), read_at=datetime.now(UTC))
        assert index[_INSTRUMENT_ID].status == "completed"
        assert index[_INSTRUMENT_ID].quality_status == "partial"
        assert index[_INSTRUMENT_ID].summary is None
    path = f"/internal/v1/research-runs/{admission.run_id}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)), base_url="http://testserver"
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["status"] == "completed"
    assert data["intent"] is None and data["quality_status"] == "partial"
    assert data["abstention_reason"] == "staged_action_semantics_unverified"
    async with factory() as session:
        after = (
            (
                await session.execute(
                    text(
                        "SELECT md5(row_to_json(t)::text) FROM agent_research_outputs t "
                        "WHERE run_id=:run_id ORDER BY sequence"
                    ),
                    {"run_id": admission.run_id},
                )
            )
            .scalars()
            .all()
        )
        assert after == before


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
    assert (
        next(vote.vote for vote in result.intent.role_votes if vote.role == "market_state")
        == "abstain"
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
    assert job_status == "succeeded"


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
    assert {job.status for job in jobs} == {"skipped"}
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
        tools = await database_read_only_tools(
            session,
            admission.run,
            allow_fixture_evidence=True,
            allow_fixture_price_evidence=True,
        )
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
        assert [item["accession_number"] for item in filings["items"]] == ["0000000001-26-000001"]
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
        assert price["items"][0]["evidence_ref_id"] != str(_PARQUET_PRICE_EVIDENCE_ID)
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
async def test_database_price_tool_accepts_exact_fosun_v2_authority_without_fixture_mode(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    await _seed_database_tool_authority(factory, price_source="fosun")
    async with factory() as session, session.begin():
        admission = await _admit_price_run(
            session,
            prompt_ids,
            allow_fixture_price_authority=False,
        )
        authority = await session.scalar(select(PriceBarSetAuthority))
        assert authority is not None
        assert authority.policy_version == "agent_price_authority_v2"
        tools = await database_read_only_tools(
            session,
            admission.run,
            allow_fixture_evidence=True,
            allow_fixture_price_evidence=False,
        )
        price = await tools.execute(
            "get_price_features",
            {
                "instrument_id": str(_INSTRUMENT_ID),
                "data_snapshot_id": str(_SNAPSHOT_ID),
                "as_of": "2026-08-24T12:00:00Z",
                "features": ["close"],
            },
        )

    assert price["status"] == "ok"
    assert price["items"][0]["source"] == "fosun"
    assert price["items"][0]["feed"] == "l1"
    assert price["items"][0]["adjustment"] == "raw"


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


async def _admit_scoped_price_run(
    session: AsyncSession,
    prompt_ids: Mapping[AgentRole, UUID],
    *,
    scope: str,
    request_hash: str,
) -> ResearchAdmission:
    return await admit_research_run(
        session,
        actor_id=scope,
        route="/internal/v1/research-runs",
        idempotency_key=scope,
        request_hash=request_hash,
        request=ResearchRequest(
            instrument_id=_INSTRUMENT_ID,
            data_snapshot_id=_SNAPSHOT_ID,
            decision_time=_NOW,
        ),
        prompt_version_id=prompt_ids["orchestrator"],
        model_config_id=_MODEL_ID,
        daily_limit_usd=Decimal("3.00"),
        now=_NOW,
        allow_fixture_price_authority=False,
    )


async def _assert_empty_admission_scope(
    factory,
    *,
    scope: str,
    request_hash: str,
) -> None:
    run_ids = select(AgentResearchRunRecord.id).where(
        AgentResearchRunRecord.actor_id == scope,
        AgentResearchRunRecord.idempotency_key == scope,
    )
    async with factory() as session:
        counts = (
            await session.scalar(
                select(func.count())
                .select_from(AgentResearchRunRecord)
                .where(
                    AgentResearchRunRecord.actor_id == scope,
                    AgentResearchRunRecord.idempotency_key == scope,
                )
            ),
            await session.scalar(
                select(func.count()).select_from(Job).where(Job.input_hash == request_hash)
            ),
            await session.scalar(
                select(func.count())
                .select_from(AgentDailyBudgetReservationRecord)
                .where(AgentDailyBudgetReservationRecord.run_id.in_(run_ids))
            ),
            await session.scalar(
                select(func.count())
                .select_from(IdempotencyRecord)
                .where(
                    IdempotencyRecord.actor_id == scope,
                    IdempotencyRecord.route == "/internal/v1/research-runs",
                    IdempotencyRecord.idempotency_key == scope,
                )
            ),
        )
    assert counts == (0, 0, 0, 0)


@pytest.mark.asyncio
async def test_retained_legacy_alpaca_v1_authority_replays_run_and_price_tool(
    migrated_database_url: str,
) -> None:
    """A frozen Alpaca v1 run remains replayable without enabling new Alpaca use."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory, include_base_evidence=False)
    await _seed_database_tool_authority(
        factory,
        link_price=True,
        price_source="alpaca",
    )
    scope = "retained-legacy-alpaca-v1-admission"
    request_hash = "1" * 64

    async with factory() as session, session.begin():
        admission = await _admit_scoped_price_run(
            session,
            prompt_ids,
            scope=scope,
            request_hash=request_hash,
        )
        tools = await database_read_only_tools(
            session,
            admission.run,
            allow_fixture_evidence=False,
            allow_fixture_price_evidence=False,
        )
        price = await tools.execute("get_price_features", _price_tool_payload())
        result = await execute_research_run(
            session,
            run_id=admission.run_id,
            committee=FixtureCommittee(),
            allow_fixture_evidence=False,
            allow_fixture_price_authority=False,
        )

    assert result.status == "completed"
    assert result.intent is not None
    assert price["status"] == "ok"
    assert price["items"][0]["source"] == "alpaca"
    assert price["items"][0]["feed"] == "sip"


@pytest.mark.asyncio
async def test_unbound_legacy_alpaca_v1_authority_is_not_newly_frozen(
    migrated_database_url: str,
) -> None:
    """Production admission must not create a new binding from retained Alpaca v1."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    await _seed_database_tool_authority(
        factory,
        link_price=False,
        price_source="alpaca",
    )

    async with factory() as session, session.begin():
        admission = await _admit_scoped_price_run(
            session,
            prompt_ids,
            scope="unbound-legacy-alpaca-v1-admission",
            request_hash="2" * 64,
        )
        price_binding_count = await session.scalar(
            select(func.count())
            .select_from(AgentDataSnapshotEvidenceRecord)
            .where(AgentDataSnapshotEvidenceRecord.authority_kind == "price")
        )

    assert not any(
        item["provider_object_id"].startswith("price-bar-set:")
        for item in admission.run.evidence_manifest
    )
    assert price_binding_count == 0


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


async def _prepared_production_price_admission(database_url: str):
    factory = session_factory(EngineSettings(database_url=database_url))
    prompt_ids = await _seed_authority(factory)
    await _seed_database_tool_authority(
        factory,
        link_price=False,
        price_source="fosun",
    )
    async with factory() as session, session.begin():
        admission = await _admit_price_run(
            session,
            prompt_ids,
            allow_fixture_price_authority=False,
        )
    return factory, admission


async def _persisted_run_intents(session: AsyncSession, run_id: UUID):
    return tuple(
        await session.scalars(
            select(AgentResearchOutputRecord.intent).where(
                AgentResearchOutputRecord.run_id == run_id,
            )
        )
    )


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
                        f"db://price-bar-set/{_PRICE_MANIFEST_ID}@sha256:{corrupted_hash}"
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
        tools = await database_read_only_tools(
            session,
            run,
            allow_fixture_evidence=True,
            allow_fixture_price_evidence=True,
        )
        await _corrupt_price_evidence_content(factory)
        result = await tools.execute("get_price_features", _price_tool_payload())
    assert (result["status"], result["reason"], result["items"]) == (
        "unavailable",
        "invalid_price_bar_set_authority",
        [],
    )


@pytest.mark.asyncio
async def test_evidence_backed_tools_fail_closed_when_current_evidence_drifts(
    migrated_database_url: str,
) -> None:
    factory, run = await _prepared_price_run(migrated_database_url)
    async with factory() as session:
        tools = await database_read_only_tools(
            session,
            run,
            allow_fixture_evidence=True,
            allow_fixture_price_evidence=True,
        )
        await _corrupt_price_evidence_content(factory)
        results = (
            await tools.execute(
                "get_security_snapshot",
                {
                    "instrument_id": str(_INSTRUMENT_ID),
                    "data_snapshot_id": str(_SNAPSHOT_ID),
                    "as_of": _NOW,
                },
            ),
            await tools.execute(
                "get_financial_facts",
                {
                    "instrument_id": str(_INSTRUMENT_ID),
                    "data_snapshot_id": str(_SNAPSHOT_ID),
                    "as_of": _NOW,
                    "fields": ["revenue"],
                },
            ),
            await tools.execute(
                "search_filing_evidence",
                {
                    "instrument_id": str(_INSTRUMENT_ID),
                    "data_snapshot_id": str(_SNAPSHOT_ID),
                    "as_of": _NOW,
                    "forms": ["10-Q"],
                    "filed_from": "2026-01-01",
                    "filed_to": "2026-08-24",
                },
            ),
            await tools.execute(
                "get_macro_snapshot",
                {
                    "data_snapshot_id": str(_SNAPSHOT_ID),
                    "as_of": _NOW,
                    "series": ["DGS10"],
                },
            ),
        )
        price = await tools.execute("get_price_features", _price_tool_payload())
    assert {(result["status"], result["reason"], tuple(result["items"])) for result in results} == {
        ("unavailable", "invalid_frozen_evidence_authority", ())
    }
    assert (price["status"], price["reason"], price["items"]) == (
        "unavailable",
        "invalid_price_bar_set_authority",
        [],
    )


@pytest.mark.asyncio
async def test_empty_evidence_queries_retain_noncorruption_statuses(
    migrated_database_url: str,
) -> None:
    factory, run = await _prepared_price_run(migrated_database_url)
    async with factory() as session:
        tools = await database_read_only_tools(
            session,
            run,
            allow_fixture_evidence=True,
            allow_fixture_price_evidence=True,
        )
        financial = await tools.execute(
            "get_financial_facts",
            {
                "instrument_id": str(_INSTRUMENT_ID),
                "data_snapshot_id": str(_SNAPSHOT_ID),
                "as_of": _NOW,
                "fields": ["net_income"],
            },
        )
        filing = await tools.execute(
            "search_filing_evidence",
            {
                "instrument_id": str(_INSTRUMENT_ID),
                "data_snapshot_id": str(_SNAPSHOT_ID),
                "as_of": _NOW,
                "forms": ["8-K"],
                "filed_from": "2026-01-01",
                "filed_to": "2026-08-24",
            },
        )
        macro = await tools.execute(
            "get_macro_snapshot",
            {
                "data_snapshot_id": str(_SNAPSHOT_ID),
                "as_of": _NOW,
                "series": ["DFF"],
            },
        )
    assert (financial["status"], financial["reason"], financial["items"]) == (
        "ok",
        None,
        [],
    )
    assert (filing["status"], filing["reason"], filing["items"]) == (
        "ok",
        None,
        [],
    )
    assert (macro["status"], macro["reason"], macro["items"]) == (
        "unavailable",
        "no_frozen_macro_authority",
        [],
    )


@pytest.mark.asyncio
async def test_price_tool_degrades_when_evidence_is_corrupt_before_construction(
    migrated_database_url: str,
) -> None:
    factory, run = await _prepared_price_run(migrated_database_url)
    await _corrupt_price_evidence_content(factory)
    async with factory() as session:
        tools = await database_read_only_tools(
            session,
            run,
            allow_fixture_evidence=True,
            allow_fixture_price_evidence=True,
        )
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
    assert (financial["status"], financial["reason"], financial["items"]) == (
        "unavailable",
        "invalid_frozen_evidence_authority",
        [],
    )


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
        await session.execute(text("ALTER TABLE agent_data_snapshot_evidence DISABLE TRIGGER USER"))
        try:
            await session.execute(text(damage_statement))
        finally:
            await session.execute(
                text("ALTER TABLE agent_data_snapshot_evidence ENABLE TRIGGER USER")
            )
    async with factory() as session:
        tools = await database_read_only_tools(
            session,
            run,
            allow_fixture_evidence=True,
            allow_fixture_price_evidence=True,
        )
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
        tools = await database_read_only_tools(
            session,
            run,
            allow_fixture_evidence=True,
            allow_fixture_price_evidence=True,
        )
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
        tools = await database_read_only_tools(
            session,
            run,
            allow_fixture_evidence=True,
            allow_fixture_price_evidence=True,
        )
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
        tools = await database_read_only_tools(
            session,
            run,
            allow_fixture_evidence=True,
            allow_fixture_price_evidence=True,
        )
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
            session,
            admission.run,
            allow_fixture_evidence=True,
        )
        result = await tools.execute("get_price_features", _price_tool_payload())
    assert (result["status"], result["reason"], result["items"]) == (
        "unavailable",
        "no_frozen_price_authority",
        [],
    )


@pytest.mark.asyncio
async def test_generic_fixture_permission_does_not_enable_fixture_price_tool(
    migrated_database_url: str,
) -> None:
    factory, run = await _prepared_price_run(migrated_database_url)
    async with factory() as session:
        tools = await database_read_only_tools(
            session,
            run,
            allow_fixture_evidence=True,
        )
        result = await tools.execute("get_price_features", _price_tool_payload())
    assert (result["status"], result["reason"], result["items"]) == (
        "unavailable",
        "invalid_price_bar_set_authority",
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
            await _admit_price_run(session, prompt_ids, allow_fixture_price_authority=False)
    async with factory() as session, session.begin():
        admission = await _admit_price_run(session, prompt_ids, allow_fixture_price_authority=True)
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
            await _admit_price_run(session, prompt_ids, allow_fixture_price_authority=False)
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 0
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
            session,
            admission.run,
            allow_fixture_evidence=True,
            allow_fixture_price_evidence=True,
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
async def test_existing_price_link_rejects_copied_owner_tamper(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    await _seed_database_tool_authority(factory, link_price=True)
    async with factory() as session, session.begin():
        copied_owner_id = UUID("0198d500-1000-7000-8000-000000001711")
        session.add(
            ResearchEvidenceRef(
                id=copied_owner_id,
                source="fixture",
                provider_object_id="copied-owner-target",
                revision="c" * 64,
                available_at=_NOW - timedelta(minutes=1),
                location=f"memory://sha256/{'c' * 64}",
                content_hash="c" * 64,
                byte_length=10,
            )
        )
        await session.flush()
        price_binding = await session.scalar(
            select(AgentDataSnapshotEvidenceRecord).where(
                AgentDataSnapshotEvidenceRecord.data_snapshot_id == _SNAPSHOT_ID,
                AgentDataSnapshotEvidenceRecord.instrument_id == _INSTRUMENT_ID,
                AgentDataSnapshotEvidenceRecord.authority_kind == "price",
            )
        )
        assert price_binding is not None
        await session.execute(text("ALTER TABLE agent_data_snapshot_evidence DISABLE TRIGGER USER"))
        try:
            price_binding.evidence_ref_id = copied_owner_id
            await session.flush()
        finally:
            await session.execute(
                text("ALTER TABLE agent_data_snapshot_evidence ENABLE TRIGGER USER")
            )
    with pytest.raises(ValueError, match="existing price authority is invalid"):
        async with factory() as session, session.begin():
            await _admit_price_run(
                session,
                prompt_ids,
                allow_fixture_price_authority=True,
            )


@pytest.mark.asyncio
async def test_admission_rejects_retained_price_binding_copied_field_drift(
    migrated_database_url: str,
) -> None:
    """Admission must compare a retained binding with its current copied fields."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    await _seed_database_tool_authority(
        factory,
        link_price=True,
        price_source="fosun",
    )
    scope = "retained-price-binding-admission"
    request_hash = "a" * 64
    with pytest.raises(ValueError, match="existing price authority is invalid"):
        async with factory() as session, session.begin():
            held_binding = await session.scalar(
                select(AgentDataSnapshotEvidenceRecord).where(
                    AgentDataSnapshotEvidenceRecord.data_snapshot_id == _SNAPSHOT_ID,
                    AgentDataSnapshotEvidenceRecord.instrument_id == _INSTRUMENT_ID,
                    AgentDataSnapshotEvidenceRecord.authority_kind == "price",
                )
            )
            assert held_binding is not None
            original_hash = held_binding.content_hash
            drifted_hash = "0" * 64
            assert original_hash != drifted_hash
            await session.execute(
                text("ALTER TABLE agent_data_snapshot_evidence DISABLE TRIGGER USER")
            )
            try:
                await session.execute(
                    text(
                        "UPDATE agent_data_snapshot_evidence "
                        "SET content_hash=:content_hash "
                        "WHERE data_snapshot_id=:snapshot_id "
                        "AND instrument_id=:instrument_id "
                        "AND evidence_ref_id=:evidence_id"
                    ),
                    {
                        "content_hash": drifted_hash,
                        "snapshot_id": held_binding.data_snapshot_id,
                        "instrument_id": held_binding.instrument_id,
                        "evidence_id": held_binding.evidence_ref_id,
                    },
                )
                raw_hash = await session.scalar(
                    text(
                        "SELECT content_hash FROM agent_data_snapshot_evidence "
                        "WHERE data_snapshot_id=:snapshot_id "
                        "AND instrument_id=:instrument_id "
                        "AND evidence_ref_id=:evidence_id"
                    ),
                    {
                        "snapshot_id": held_binding.data_snapshot_id,
                        "instrument_id": held_binding.instrument_id,
                        "evidence_id": held_binding.evidence_ref_id,
                    },
                )
                assert raw_hash == drifted_hash
                assert held_binding.content_hash == original_hash
            finally:
                await session.execute(
                    text("ALTER TABLE agent_data_snapshot_evidence ENABLE TRIGGER USER")
                )
            await _admit_scoped_price_run(
                session,
                prompt_ids,
                scope=scope,
                request_hash=request_hash,
            )
    await _assert_empty_admission_scope(
        factory,
        scope=scope,
        request_hash=request_hash,
    )


@pytest.mark.parametrize("timestamp_column", ("available_at", "received_at"))
@pytest.mark.asyncio
async def test_admission_rejects_retained_snapshot_pit_drift(
    migrated_database_url: str,
    timestamp_column: str,
) -> None:
    """Admission must reject a retained snapshot whose current PIT is later."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    await _seed_database_tool_authority(
        factory,
        link_price=True,
        price_source="fosun",
    )
    scope = f"retained-{timestamp_column}-admission"
    request_hash = ("b" if timestamp_column == "available_at" else "c") * 64
    with pytest.raises(ValueError, match="existing price authority is invalid"):
        async with factory() as session, session.begin():
            held_snapshot = await session.get(DataSnapshot, _SNAPSHOT_ID)
            assert held_snapshot is not None
            original_timestamp = getattr(held_snapshot, timestamp_column)
            drifted_timestamp = _NOW + timedelta(seconds=1)
            assert original_timestamp <= _NOW < drifted_timestamp
            await session.execute(
                text(
                    f"UPDATE data_snapshots SET {timestamp_column}=:timestamp WHERE id=:snapshot_id"
                ),
                {
                    "timestamp": drifted_timestamp,
                    "snapshot_id": held_snapshot.id,
                },
            )
            raw_timestamp = await session.scalar(
                text(f"SELECT {timestamp_column} FROM data_snapshots WHERE id=:snapshot_id"),
                {"snapshot_id": held_snapshot.id},
            )
            assert raw_timestamp == drifted_timestamp
            assert getattr(held_snapshot, timestamp_column) == original_timestamp
            await _admit_scoped_price_run(
                session,
                prompt_ids,
                scope=scope,
                request_hash=request_hash,
            )
    await _assert_empty_admission_scope(
        factory,
        scope=scope,
        request_hash=request_hash,
    )


@pytest.mark.asyncio
async def test_admission_rejects_retained_price_owner_copied_field_drift(
    migrated_database_url: str,
) -> None:
    """Admission must refresh retained evidence from the existing owner chain."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    await _seed_database_tool_authority(
        factory,
        link_price=True,
        price_source="fosun",
    )
    scope = "retained-price-owner-admission"
    request_hash = "d" * 64
    with pytest.raises(ValueError, match="existing price authority is invalid"):
        async with factory() as session, session.begin():
            held_evidence = await session.scalar(
                select(ResearchEvidenceRef)
                .join(
                    PriceBarSetAuthority,
                    PriceBarSetAuthority.evidence_ref_id == ResearchEvidenceRef.id,
                )
                .where(PriceBarSetAuthority.price_series_manifest_id == _PRICE_MANIFEST_ID)
            )
            assert held_evidence is not None
            original_hash = held_evidence.content_hash
            original_location = held_evidence.location
            drifted_hash = "0" * 64
            drifted_location = f"db://price-bar-set/{_PRICE_MANIFEST_ID}@sha256:{drifted_hash}"
            assert (original_hash, original_location) != (
                drifted_hash,
                drifted_location,
            )
            await session.execute(text("ALTER TABLE research_evidence_refs DISABLE TRIGGER USER"))
            try:
                await session.execute(
                    text(
                        "UPDATE research_evidence_refs "
                        "SET content_hash=:content_hash, location=:location "
                        "WHERE id=:evidence_id"
                    ),
                    {
                        "content_hash": drifted_hash,
                        "location": drifted_location,
                        "evidence_id": held_evidence.id,
                    },
                )
                raw_evidence = (
                    (
                        await session.execute(
                            text(
                                "SELECT content_hash, location "
                                "FROM research_evidence_refs WHERE id=:evidence_id"
                            ),
                            {"evidence_id": held_evidence.id},
                        )
                    )
                    .mappings()
                    .one()
                )
                assert (raw_evidence["content_hash"], raw_evidence["location"]) == (
                    drifted_hash,
                    drifted_location,
                )
                assert (held_evidence.content_hash, held_evidence.location) == (
                    original_hash,
                    original_location,
                )
            finally:
                await session.execute(
                    text("ALTER TABLE research_evidence_refs ENABLE TRIGGER USER")
                )
            await _admit_scoped_price_run(
                session,
                prompt_ids,
                scope=scope,
                request_hash=request_hash,
            )
    await _assert_empty_admission_scope(
        factory,
        scope=scope,
        request_hash=request_hash,
    )


@pytest.mark.asyncio
async def test_admission_rejects_research_shaped_existing_price_binding(
    migrated_database_url: str,
) -> None:
    """A corrupted price binding must not be mistaken for no existing link."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    await _seed_database_tool_authority(
        factory,
        link_price=True,
        price_source="fosun",
    )
    scope = "research-shaped-price-binding-admission"
    request_hash = "f" * 64
    with pytest.raises(ValueError, match="existing price authority is invalid"):
        async with factory() as session, session.begin():
            held_binding = await session.scalar(
                select(AgentDataSnapshotEvidenceRecord).where(
                    AgentDataSnapshotEvidenceRecord.data_snapshot_id == _SNAPSHOT_ID,
                    AgentDataSnapshotEvidenceRecord.instrument_id == _INSTRUMENT_ID,
                    AgentDataSnapshotEvidenceRecord.authority_kind == "price",
                )
            )
            assert held_binding is not None
            assert held_binding.selected_at is not None
            await session.execute(
                text("ALTER TABLE agent_data_snapshot_evidence DISABLE TRIGGER USER")
            )
            try:
                await session.execute(
                    text(
                        "UPDATE agent_data_snapshot_evidence "
                        "SET authority_kind='research', selected_at=NULL "
                        "WHERE data_snapshot_id=:snapshot_id "
                        "AND instrument_id=:instrument_id "
                        "AND evidence_ref_id=:evidence_id"
                    ),
                    {
                        "snapshot_id": held_binding.data_snapshot_id,
                        "instrument_id": held_binding.instrument_id,
                        "evidence_id": held_binding.evidence_ref_id,
                    },
                )
                raw_binding = (
                    (
                        await session.execute(
                            text(
                                "SELECT authority_kind, selected_at "
                                "FROM agent_data_snapshot_evidence "
                                "WHERE data_snapshot_id=:snapshot_id "
                                "AND instrument_id=:instrument_id "
                                "AND evidence_ref_id=:evidence_id"
                            ),
                            {
                                "snapshot_id": held_binding.data_snapshot_id,
                                "instrument_id": held_binding.instrument_id,
                                "evidence_id": held_binding.evidence_ref_id,
                            },
                        )
                    )
                    .mappings()
                    .one()
                )
                assert (raw_binding["authority_kind"], raw_binding["selected_at"]) == (
                    "research",
                    None,
                )
                assert held_binding.authority_kind == "price"
                assert held_binding.selected_at is not None
            finally:
                await session.execute(
                    text("ALTER TABLE agent_data_snapshot_evidence ENABLE TRIGGER USER")
                )
            await _admit_scoped_price_run(
                session,
                prompt_ids,
                scope=scope,
                request_hash=request_hash,
            )
    await _assert_empty_admission_scope(
        factory,
        scope=scope,
        request_hash=request_hash,
    )


@pytest.mark.parametrize(
    ("provider_object_id", "location"),
    (
        (
            f"price-bar-set:{_PRICE_MANIFEST_ID}",
            f"memory://sha256/{'d' * 64}",
        ),
        (
            "shape-only-research",
            f"db://price-bar-set/{_PRICE_MANIFEST_ID}@sha256:{'d' * 64}",
        ),
    ),
    ids=("reserved-object-id", "reserved-location"),
)
@pytest.mark.asyncio
async def test_research_owner_trigger_reserves_price_bar_identity_and_location(
    migrated_database_url: str,
    provider_object_id: str,
    location: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    with pytest.raises(DBAPIError, match="price evidence requires price authority"):
        async with factory() as session, session.begin():
            evidence = ResearchEvidenceRef(
                id=UUID("0198d500-1000-7000-8000-000000001709"),
                source="fixture",
                provider_object_id=provider_object_id,
                revision="d" * 64,
                available_at=_NOW - timedelta(minutes=1),
                location=location,
                content_hash="d" * 64,
                byte_length=10,
            )
            session.add(evidence)
            await session.flush()
            session.add(
                AgentDataSnapshotEvidenceRecord(
                    data_snapshot_id=_SNAPSHOT_ID,
                    instrument_id=_INSTRUMENT_ID,
                    evidence_ref_id=evidence.id,
                    authority_kind="research",
                    selected_at=None,
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
async def test_disguised_research_price_link_cannot_create_positive_intent(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    evidence_id = UUID("0198d500-1000-7000-8000-000000001710")
    digest = "d" * 64
    async with factory() as session, session.begin():
        evidence = ResearchEvidenceRef(
            id=evidence_id,
            source="fixture",
            provider_object_id=f"price-bar-set:{_PRICE_MANIFEST_ID}",
            revision=digest,
            available_at=_NOW - timedelta(minutes=1),
            location=f"db://price-bar-set/{_PRICE_MANIFEST_ID}@sha256:{digest}",
            content_hash=digest,
            byte_length=10,
        )
        session.add(evidence)
        await session.flush()
        await session.execute(text("ALTER TABLE agent_data_snapshot_evidence DISABLE TRIGGER USER"))
        try:
            session.add(
                AgentDataSnapshotEvidenceRecord(
                    data_snapshot_id=_SNAPSHOT_ID,
                    instrument_id=_INSTRUMENT_ID,
                    evidence_ref_id=evidence.id,
                    authority_kind="research",
                    selected_at=None,
                    source=evidence.source,
                    provider_object_id=evidence.provider_object_id,
                    revision=evidence.revision,
                    available_at=evidence.available_at,
                    location=evidence.location,
                    content_hash=evidence.content_hash,
                )
            )
            await session.flush()
        finally:
            await session.execute(
                text("ALTER TABLE agent_data_snapshot_evidence ENABLE TRIGGER USER")
            )
    async with factory() as session, session.begin():
        admission = await _admit_price_run(
            session,
            prompt_ids,
            allow_fixture_price_authority=True,
        )
    async with factory() as session, session.begin():
        result = await execute_research_run(
            session,
            run_id=admission.run_id,
            committee=FixtureCommittee(),
        )
    assert result.intent is None or (
        result.intent.stance,
        result.intent.exposure_action,
        next(vote.vote for vote in result.intent.role_votes if vote.role == "market_state"),
        result.intent.quality_status,
    ) == ("watch", "none", "abstain", "partial")


@pytest.mark.asyncio
async def test_corrupted_frozen_price_bar_set_cannot_create_positive_intent(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    await _seed_database_tool_authority(
        factory,
        link_price=False,
        price_source="fosun",
    )
    async with factory() as session, session.begin():
        admission = await _admit_price_run(
            session,
            prompt_ids,
            allow_fixture_price_authority=False,
        )
    async with factory() as session, session.begin():
        await session.execute(text("ALTER TABLE price_bar_records DISABLE TRIGGER USER"))
        try:
            await session.execute(
                text(
                    "UPDATE price_bar_records SET close = close + 0.010000000000 "
                    "WHERE price_series_manifest_id = :manifest_id"
                ),
                {"manifest_id": _PRICE_MANIFEST_ID},
            )
        finally:
            await session.execute(text("ALTER TABLE price_bar_records ENABLE TRIGGER USER"))
    async with factory() as session, session.begin():
        result = await execute_research_run(
            session,
            run_id=admission.run_id,
            committee=FixtureCommittee(),
        )
    assert result.intent is None or (
        result.intent.stance,
        result.intent.exposure_action,
        next(vote.vote for vote in result.intent.role_votes if vote.role == "market_state"),
        result.intent.quality_status,
    ) == ("watch", "none", "abstain", "partial")


@pytest.mark.asyncio
async def test_price_binding_drift_before_final_synthesis_cannot_create_positive_intent(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    await _seed_database_tool_authority(
        factory,
        link_price=False,
        price_source="fosun",
    )
    async with factory() as session, session.begin():
        admission = await _admit_price_run(
            session,
            prompt_ids,
            allow_fixture_price_authority=False,
        )

    class _BindingTamperingCommittee:
        def __init__(self, session: AsyncSession) -> None:
            self._session = session
            self._fixture = FixtureCommittee()
            self._tampered = False

        async def analyze(
            self,
            role: AgentRole,
            request: ResearchRequest,
            evidence_refs: tuple[EvidenceRef, ...],
            prompt: PromptAsset,
        ) -> CommitteeRoleOutput:
            if not self._tampered:
                await self._session.execute(
                    text("ALTER TABLE agent_data_snapshot_evidence DISABLE TRIGGER USER")
                )
                try:
                    await self._session.execute(
                        text(
                            "UPDATE agent_data_snapshot_evidence "
                            "SET authority_kind='research', selected_at=NULL "
                            "WHERE authority_kind='price'"
                        )
                    )
                finally:
                    await self._session.execute(
                        text("ALTER TABLE agent_data_snapshot_evidence ENABLE TRIGGER USER")
                    )
                self._tampered = True
            return await self._fixture.analyze(
                role,
                request,
                evidence_refs,
                prompt,
            )

    async with factory() as session, session.begin():
        result = await execute_research_run(
            session,
            run_id=admission.run_id,
            committee=_BindingTamperingCommittee(session),
        )
    assert result.intent is None or (
        result.intent.stance,
        result.intent.exposure_action,
        next(vote.vote for vote in result.intent.role_votes if vote.role == "market_state"),
        result.intent.quality_status,
    ) == ("watch", "none", "abstain", "partial")


@pytest.mark.asyncio
async def test_retained_price_binding_drift_abstains_without_emitting_positive_intent(
    migrated_database_url: str,
) -> None:
    """A retained stale price binding must not survive current-state revalidation."""
    factory, admission = await _prepared_production_price_admission(migrated_database_url)
    async with factory() as session, session.begin():
        held_binding = await session.scalar(
            select(AgentDataSnapshotEvidenceRecord).where(
                AgentDataSnapshotEvidenceRecord.data_snapshot_id == admission.run.data_snapshot_id,
                AgentDataSnapshotEvidenceRecord.instrument_id == admission.run.instrument_id,
                AgentDataSnapshotEvidenceRecord.authority_kind == "price",
            )
        )
        assert held_binding is not None
        assert held_binding.authority_kind == "price"
        assert held_binding.selected_at is not None

        async def tamper() -> None:
            await session.execute(
                text("ALTER TABLE agent_data_snapshot_evidence DISABLE TRIGGER USER")
            )
            try:
                await session.execute(
                    text(
                        "UPDATE agent_data_snapshot_evidence "
                        "SET authority_kind='research', selected_at=NULL "
                        "WHERE data_snapshot_id=:snapshot_id "
                        "AND instrument_id=:instrument_id "
                        "AND evidence_ref_id=:evidence_id"
                    ),
                    {
                        "snapshot_id": held_binding.data_snapshot_id,
                        "instrument_id": held_binding.instrument_id,
                        "evidence_id": held_binding.evidence_ref_id,
                    },
                )
                raw_binding = (
                    (
                        await session.execute(
                            text(
                                "SELECT authority_kind, selected_at "
                                "FROM agent_data_snapshot_evidence "
                                "WHERE data_snapshot_id=:snapshot_id "
                                "AND instrument_id=:instrument_id "
                                "AND evidence_ref_id=:evidence_id"
                            ),
                            {
                                "snapshot_id": held_binding.data_snapshot_id,
                                "instrument_id": held_binding.instrument_id,
                                "evidence_id": held_binding.evidence_ref_id,
                            },
                        )
                    )
                    .mappings()
                    .one()
                )
                assert (raw_binding["authority_kind"], raw_binding["selected_at"]) == (
                    "research",
                    None,
                )
                assert held_binding.authority_kind == "price"
                assert held_binding.selected_at is not None
            finally:
                await session.execute(
                    text("ALTER TABLE agent_data_snapshot_evidence ENABLE TRIGGER USER")
                )

        committee = _SingleTamperingCommittee(tamper)
        result = await execute_research_run(
            session,
            run_id=admission.run_id,
            committee=committee,
        )
        persisted_intents = await _persisted_run_intents(session, admission.run_id)

    assert committee.tampered is True
    assert result.status == "abstained"
    assert result.intent is None
    assert persisted_intents
    assert all(intent is None for intent in persisted_intents)


@pytest.mark.asyncio
async def test_retained_run_identity_drift_abstains_without_emitting_positive_intent(
    migrated_database_url: str,
) -> None:
    """A retained stale run must not redefine the current persisted execution scope."""
    factory, admission = await _prepared_production_price_admission(migrated_database_url)
    async with factory() as session, session.begin():
        session.add(
            Instrument(
                id=_DRIFT_INSTRUMENT_ID,
                asset_type="equity",
                exchange="NASDAQ",
                currency="USD",
                tick_size=Decimal("0.01"),
                risk_bucket="equity",
            )
        )

    async with factory() as session, session.begin():
        held_run = await session.get(AgentResearchRunRecord, admission.run_id)
        assert held_run is not None
        assert held_run.instrument_id == _INSTRUMENT_ID

        async def tamper() -> None:
            await session.execute(text("ALTER TABLE agent_research_runs DISABLE TRIGGER USER"))
            try:
                await session.execute(
                    text(
                        "UPDATE agent_research_runs SET instrument_id=:instrument_id "
                        "WHERE id=:run_id"
                    ),
                    {
                        "instrument_id": _DRIFT_INSTRUMENT_ID,
                        "run_id": held_run.id,
                    },
                )
                raw_run = (
                    (
                        await session.execute(
                            text("SELECT instrument_id FROM agent_research_runs WHERE id=:run_id"),
                            {"run_id": held_run.id},
                        )
                    )
                    .mappings()
                    .one()
                )
                assert raw_run["instrument_id"] == _DRIFT_INSTRUMENT_ID
                assert held_run.instrument_id == _INSTRUMENT_ID
            finally:
                await session.execute(text("ALTER TABLE agent_research_runs ENABLE TRIGGER USER"))

        committee = _SingleTamperingCommittee(tamper)
        result = await execute_research_run(
            session,
            run_id=admission.run_id,
            committee=committee,
        )
        persisted_intents = await _persisted_run_intents(session, admission.run_id)

    positive_summary = None
    if result.intent is not None:
        positive_summary = (
            result.intent.stance,
            result.intent.exposure_action,
            next(vote.vote for vote in result.intent.role_votes if vote.role == "market_state"),
        )
    assert committee.tampered is True
    assert result.status == "abstained", (
        f"status={result.status}, positive_summary={positive_summary}"
    )
    assert result.intent is None
    assert persisted_intents
    assert all(intent is None for intent in persisted_intents)


@pytest.mark.asyncio
async def test_retained_price_binding_copied_field_drift_abstains_without_intent(
    migrated_database_url: str,
) -> None:
    """Refreshing the retained binding must expose drift in copied evidence fields."""
    factory, admission = await _prepared_production_price_admission(migrated_database_url)
    async with factory() as session, session.begin():
        held_binding = await session.scalar(
            select(AgentDataSnapshotEvidenceRecord).where(
                AgentDataSnapshotEvidenceRecord.data_snapshot_id == admission.run.data_snapshot_id,
                AgentDataSnapshotEvidenceRecord.instrument_id == admission.run.instrument_id,
                AgentDataSnapshotEvidenceRecord.authority_kind == "price",
            )
        )
        assert held_binding is not None
        original_hash = held_binding.content_hash
        drifted_hash = "0" * 64
        assert original_hash != drifted_hash

        async def tamper() -> None:
            await session.execute(
                text("ALTER TABLE agent_data_snapshot_evidence DISABLE TRIGGER USER")
            )
            try:
                await session.execute(
                    text(
                        "UPDATE agent_data_snapshot_evidence "
                        "SET content_hash=:content_hash "
                        "WHERE data_snapshot_id=:snapshot_id "
                        "AND instrument_id=:instrument_id "
                        "AND evidence_ref_id=:evidence_id"
                    ),
                    {
                        "content_hash": drifted_hash,
                        "snapshot_id": held_binding.data_snapshot_id,
                        "instrument_id": held_binding.instrument_id,
                        "evidence_id": held_binding.evidence_ref_id,
                    },
                )
                raw_hash = await session.scalar(
                    text(
                        "SELECT content_hash FROM agent_data_snapshot_evidence "
                        "WHERE data_snapshot_id=:snapshot_id "
                        "AND instrument_id=:instrument_id "
                        "AND evidence_ref_id=:evidence_id"
                    ),
                    {
                        "snapshot_id": held_binding.data_snapshot_id,
                        "instrument_id": held_binding.instrument_id,
                        "evidence_id": held_binding.evidence_ref_id,
                    },
                )
                assert raw_hash == drifted_hash
                assert held_binding.content_hash == original_hash
                assert held_binding.authority_kind == "price"
                assert held_binding.selected_at is not None
            finally:
                await session.execute(
                    text("ALTER TABLE agent_data_snapshot_evidence ENABLE TRIGGER USER")
                )

        committee = _SingleTamperingCommittee(tamper)
        result = await execute_research_run(
            session,
            run_id=admission.run_id,
            committee=committee,
        )
        persisted_intents = await _persisted_run_intents(session, admission.run_id)

    assert committee.tampered is True
    assert result.status == "abstained"
    assert result.intent is None
    assert persisted_intents
    assert all(intent is None for intent in persisted_intents)


@pytest.mark.asyncio
async def test_retained_snapshot_drift_abstains_without_emitting_positive_intent(
    migrated_database_url: str,
) -> None:
    """Refreshing the retained snapshot must expose invalid selection-time drift."""
    factory, admission = await _prepared_production_price_admission(migrated_database_url)
    async with factory() as session, session.begin():
        held_snapshot = await session.get(DataSnapshot, admission.run.data_snapshot_id)
        assert held_snapshot is not None
        original_available_at = held_snapshot.available_at
        drifted_available_at = original_available_at - timedelta(seconds=1)

        async def tamper() -> None:
            await session.execute(text("ALTER TABLE data_snapshots DISABLE TRIGGER USER"))
            try:
                await session.execute(
                    text(
                        "UPDATE data_snapshots SET available_at=:available_at WHERE id=:snapshot_id"
                    ),
                    {
                        "available_at": drifted_available_at,
                        "snapshot_id": held_snapshot.id,
                    },
                )
                raw_available_at = await session.scalar(
                    text("SELECT available_at FROM data_snapshots WHERE id=:snapshot_id"),
                    {"snapshot_id": held_snapshot.id},
                )
                assert raw_available_at == drifted_available_at
                assert held_snapshot.available_at == original_available_at
            finally:
                await session.execute(text("ALTER TABLE data_snapshots ENABLE TRIGGER USER"))

        committee = _SingleTamperingCommittee(tamper)
        result = await execute_research_run(
            session,
            run_id=admission.run_id,
            committee=committee,
        )
        persisted_intents = await _persisted_run_intents(session, admission.run_id)

    assert committee.tampered is True
    assert result.status == "abstained"
    assert result.intent is None
    assert persisted_intents
    assert all(intent is None for intent in persisted_intents)


@pytest.mark.parametrize("timestamp_column", ("available_at", "received_at"))
@pytest.mark.asyncio
async def test_current_snapshot_pit_drift_abstains_without_emitting_positive_intent(
    migrated_database_url: str,
    timestamp_column: str,
) -> None:
    """A current snapshot later than decision time must invalidate final execution."""
    factory, admission = await _prepared_production_price_admission(migrated_database_url)
    async with factory() as session, session.begin():
        held_snapshot = await session.get(DataSnapshot, admission.run.data_snapshot_id)
        assert held_snapshot is not None
        original_timestamp = getattr(held_snapshot, timestamp_column)
        drifted_timestamp = admission.run.decision_time + timedelta(seconds=1)
        assert original_timestamp <= admission.run.decision_time < drifted_timestamp

        async def tamper() -> None:
            await session.execute(
                text(
                    f"UPDATE data_snapshots SET {timestamp_column}=:timestamp WHERE id=:snapshot_id"
                ),
                {
                    "timestamp": drifted_timestamp,
                    "snapshot_id": held_snapshot.id,
                },
            )
            raw_timestamp = await session.scalar(
                text(f"SELECT {timestamp_column} FROM data_snapshots WHERE id=:snapshot_id"),
                {"snapshot_id": held_snapshot.id},
            )
            assert raw_timestamp == drifted_timestamp
            assert getattr(held_snapshot, timestamp_column) == original_timestamp

        committee = _SingleTamperingCommittee(tamper)
        result = await execute_research_run(
            session,
            run_id=admission.run_id,
            committee=committee,
        )
        persisted_intents = await _persisted_run_intents(session, admission.run_id)

    positive_summary = None
    if result.intent is not None:
        positive_summary = (
            result.intent.stance,
            result.intent.exposure_action,
            next(vote.vote for vote in result.intent.role_votes if vote.role == "market_state"),
            result.intent.quality_status,
        )
    assert committee.tampered is True
    assert result.status == "abstained", (
        f"timestamp_column={timestamp_column}, status={result.status}, "
        f"positive_summary={positive_summary}"
    )
    assert result.intent is None
    assert persisted_intents
    assert all(intent is None for intent in persisted_intents)


@pytest.mark.asyncio
async def test_development_responses_rejects_fixture_price_before_client_creation(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    await _seed_database_tool_authority(
        factory,
        link_price=False,
        price_source="fixture",
    )
    async with factory() as session, session.begin():
        admission = await _admit_price_run(
            session,
            prompt_ids,
            allow_fixture_price_authority=True,
        )
    fake = _ToolLoopResponsesClient()
    constructor_calls: list[str] = []

    def fake_openai_client(*, api_key: str) -> _ToolLoopResponsesClient:
        del api_key
        constructor_calls.append("constructed")
        return fake

    monkeypatch.setattr("nexus.worker.AsyncOpenAI", fake_openai_client)
    settings = EngineSettings(
        environment=Environment.DEVELOPMENT,
        database_url=migrated_database_url,
        agent_runtime_mode="responses",
        openai_api_key="fixture-key",
        openai_extraction_model="fixture-extraction",
        openai_reasoning_model="fixture-model",
    )
    assert await run_agent_worker_once(settings) is True
    async with factory() as session:
        run = await session.get(AgentResearchRunRecord, admission.run_id)
    assert run is not None
    assert (constructor_calls, len(fake.responses.calls), run.status) == (
        [],
        0,
        "abstained",
    ), (
        f"constructor_count={len(constructor_calls)}, "
        f"responses_calls={len(fake.responses.calls)}, status={run.status}"
    )


@pytest.mark.asyncio
async def test_price_owner_trigger_rejects_a_future_bar(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    await _seed_database_tool_authority(factory, link_price=False, future_last_bar=True)
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
async def test_nonfixture_raw_fosun_prices_expose_close_but_reject_return_features(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory, include_base_evidence=False)
    await _seed_database_tool_authority(factory, link_price=False, price_source="fosun")
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
        tools = await database_read_only_tools(session, admission.run, allow_fixture_evidence=False)
        close = await tools.execute(
            "get_price_features",
            {
                "instrument_id": str(_INSTRUMENT_ID),
                "data_snapshot_id": str(_SNAPSHOT_ID),
                "as_of": _NOW,
                "features": ["close"],
            },
        )
        derived = await tools.execute(
            "get_price_features",
            {
                "instrument_id": str(_INSTRUMENT_ID),
                "data_snapshot_id": str(_SNAPSHOT_ID),
                "as_of": _NOW,
                "features": ["close", "sma_20", "rsi_14", "volatility_20"],
            },
        )
        committee = StagedCodexCommittee(prompts=load_prompt_assets(), tools=tools)
        inputs = await committee._inputs(
            ResearchRequest(
                instrument_id=_INSTRUMENT_ID,
                data_snapshot_id=_SNAPSHOT_ID,
                decision_time=_NOW,
            )
        )
        assert inputs["price_close"] == close
        assert inputs["price_features"] == derived
    assert close["status"] == "ok"
    assert close["items"][0]["source"] == "fosun"
    assert close["items"][0]["features"] == {"close": "100.000000000000"}
    assert (
        derived["tool"],
        derived["status"],
        derived["reason"],
        derived["items"],
    ) == (
        "get_price_features",
        "unavailable",
        "adjusted_price_authority_unavailable",
        [],
    )


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
                    price_series_manifest_id=UUID("0198d500-1000-7000-8000-000000001707"),
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
        "DELETE FROM price_series_manifests WHERE id = '0198d500-1000-7000-8000-000000001707'",
        "DELETE FROM data_artifacts WHERE id = '0198d500-1000-7000-8000-000000001706'",
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
        "inputs": None,
    }
    assert future.status_code == 200
    assert future.json()["data"] == {
        "eligible": False,
        "reason": "decision_time_in_future",
        "instrument_id": str(_INSTRUMENT_ID),
        "data_snapshot_id": None,
        "decision_time": "2036-08-24T12:00:00Z",
        "inputs": None,
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
    model_version: str = "fixture-pricing-v1",
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
                version=model_version,
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
    fosun_capability_id = UUID("0198d500-1000-7000-8000-000000001709")
    fosun_status_id = UUID("0198d500-1000-7000-8000-000000001710")
    fosun_contract_hash = "a" * 64

    def bar_time(index: int) -> datetime:
        if future_last_bar and index == bar_count - 1:
            return _NOW + timedelta(seconds=1)
        return _NOW - timedelta(days=bar_count - index)

    async with factory() as session, session.begin():
        if price_source == "fosun":
            session.add_all(
                (
                    ProviderCapability(
                        id=fosun_capability_id,
                        provider="fosun",
                        capability="READ_ONLY",
                        contract_hash=fosun_contract_hash,
                        expires_at=datetime(2027, 1, 1, tzinfo=UTC),
                        schema_version="1",
                        payload={
                            "operation": "fosun_get_bars",
                            "market_scope": "US",
                            "delivery_mode": "real_time",
                        },
                        created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    ),
                    IntegrationStatus(
                        id=fosun_status_id,
                        provider="fosun",
                        status="read_only",
                        version=1,
                        mode_changed_at=datetime(2026, 1, 1, tzinfo=UTC),
                        admitted_contract_hash=fosun_contract_hash,
                        created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    ),
                )
            )
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
                provider_object_id=("price-series-manifest:0198d500-1000-7000-8000-000000001707"),
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
        fosun_lineage = None
        if price_source == "fosun":
            fosun_lineage = await seed_fosun_daily_lineage(
                session,
                manifest_id=_PRICE_MANIFEST_ID,
                provider_authorized_at=_NOW - timedelta(seconds=30),
                sessions=tuple(bar_time(index).date() for index in range(bar_count)),
                raw_payload=b'{"fixture":"agent-database-tool"}',
                received_at=_NOW - timedelta(minutes=1),
            )
        manifest = PriceSeriesManifest(
            id=UUID("0198d500-1000-7000-8000-000000001707"),
            evidence_ref_id=price_evidence_id,
            data_artifact_id=UUID("0198d500-1000-7000-8000-000000001706"),
            instrument=str(_INSTRUMENT_ID),
            source=price_source,
            feed=(
                "sip"
                if price_source == "alpaca"
                else "l1"
                if price_source == "fosun"
                else "adjusted-daily"
            ),
            delivery_mode=(
                "delayed"
                if price_source == "alpaca"
                else "real_time"
                if price_source == "fosun"
                else "fixture"
            ),
            adjustment="raw" if price_source == "fosun" else "split_dividend",
            interval="1Day",
            start_at=_NOW - timedelta(days=bar_count),
            end_at=bar_time(bar_count - 1),
            row_count=bar_count,
            provider_capability_id=(fosun_capability_id if price_source == "fosun" else None),
            provider_status_id=fosun_status_id if price_source == "fosun" else None,
            provider_status_version=1 if price_source == "fosun" else None,
            provider_contract_hash=(fosun_contract_hash if price_source == "fosun" else None),
            provider_authorized_at=(
                _NOW - timedelta(seconds=30) if price_source == "fosun" else None
            ),
            gateway_transport_artifact_id=(
                fosun_lineage.raw_artifact.id if fosun_lineage is not None else None
            ),
            daily_calendar_authority_id=(
                fosun_lineage.calendar_authority_id if fosun_lineage is not None else None
            ),
            gateway_request_start_at=(
                fosun_lineage.request_start_at if fosun_lineage is not None else None
            ),
            gateway_request_end_at=(
                fosun_lineage.request_end_at if fosun_lineage is not None else None
            ),
        )
        session.add(manifest)
        await session.flush()
        session.add_all(
            PriceBarRecord(
                price_series_manifest_id=UUID("0198d500-1000-7000-8000-000000001707"),
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
                revision=(fosun_lineage.raw_digest if fosun_lineage is not None else "fixture-r1"),
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


class _SingleTamperingCommittee:
    def __init__(self, tamper: Callable[[], Awaitable[None]]) -> None:
        self._tamper = tamper
        self._fixture = FixtureCommittee()
        self.tampered = False

    async def analyze(
        self,
        role: AgentRole,
        request: ResearchRequest,
        evidence_refs: tuple[EvidenceRef, ...],
        prompt: PromptAsset,
    ) -> CommitteeRoleOutput:
        if not self.tampered:
            await self._tamper()
            self.tampered = True
        return await self._fixture.analyze(role, request, evidence_refs, prompt)


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


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_stage", ["research", "critique"])
@pytest.mark.parametrize("timeout", [False, True])
async def test_native_sdk_invocation_failure_keeps_safe_diagnostic_and_usage(
    migrated_database_url, monkeypatch, failure_stage, timeout
):
    """Only the SDK process is replaced; admission, adapter, worker, DB and API are real."""
    from types import SimpleNamespace

    from nexus.agents import codex_sdk
    from nexus.ops.openai_local import prepare_database
    from openai_codex.generated.v2_all import ChatgptAccount, TurnStatus

    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    factory = session_factory(settings)
    await _seed_authority(factory, include_base_evidence=False)
    await _seed_database_tool_authority(
        factory, link_price=False, bar_count=1, price_quality_flags=("missing",)
    )
    await prepare_database(settings, structured_synthesis=True)
    async with factory() as session, session.begin():
        model = await session.scalar(
            select(AgentModelConfigRecord).where(
                AgentModelConfigRecord.version == "codex-sdk-chatgpt-v3"
            )
        )
        admission = await admit_research_run(
            session,
            actor_id="sdk-test",
            route="/internal/v1/research-runs",
            idempotency_key="sdk-invocation-failure",
            request_hash="b" * 64,
            request=ResearchRequest(
                instrument_id=_INSTRUMENT_ID, data_snapshot_id=_SNAPSHOT_ID, decision_time=_NOW
            ),
            prompt_version_id=None,
            model_config_id=model.id,
            daily_limit_usd=Decimal("3"),
            now=datetime(2026, 9, 2, tzinfo=UTC),
        )
    requests = []

    async def sdk_turn(wire, **kwargs):
        payload = json.loads(wire)
        requests.append(payload)
        if payload["stage"] == failure_stage:
            if timeout:
                raise TimeoutError("synthetic-private-invocation-canary")
            raise RuntimeError("synthetic-private-invocation-canary")
        return SimpleNamespace(
            status=TurnStatus.completed,
            items=[],
            usage=SimpleNamespace(total=SimpleNamespace(input_tokens=300, output_tokens=80)),
            final_response=json.dumps(
                {
                    "roles": [
                        {
                            "role": role,
                            "vote": "abstain",
                            "conflicting_facts": False,
                            "claims": [{"text": "缺少合格行情。", "citations": [0]}],
                            "reviewed_claim_ids": [],
                        }
                        for role in payload["requested_roles"]
                    ]
                }
            ),
        )

    thread = SimpleNamespace(run=AsyncMock(side_effect=sdk_turn))
    client = SimpleNamespace(
        account=AsyncMock(
            return_value=SimpleNamespace(
                account=SimpleNamespace(root=ChatgptAccount.model_construct())
            )
        ),
        _client=SimpleNamespace(
            request=AsyncMock(return_value=SimpleNamespace(config=SimpleNamespace(model_extra={})))
        ),
        thread_start=AsyncMock(return_value=thread),
        close=AsyncMock(),
    )
    monkeypatch.setattr(codex_sdk, "AsyncCodex", lambda config: client)
    assert await run_agent_worker_once(settings) is True
    assert await run_agent_worker_once(settings) is False
    calls = 1 if failure_stage == "research" else 2
    assert len(requests) == calls
    assert client.close.await_count == calls
    async with factory() as session:
        run = await session.get(AgentResearchRunRecord, admission.run_id)
        assert run.status == "failed" and run.retry_count == 0
        expected_usage = (None, None) if calls == 1 else (300, 80)
        assert (run.input_tokens, run.output_tokens) == expected_usage
        job = await session.scalar(select(Job).where(Job.payload["run_id"].astext == str(run.id)))
        assert job.attempt_count == job.max_attempts == 1
        outputs = tuple(
            await session.scalars(
                select(AgentResearchOutputRecord).where(AgentResearchOutputRecord.run_id == run.id)
            )
        )
        assert len(outputs) == (1 if calls == 1 else 6)
        assert all(output.intent is None for output in outputs)
        terminal = next(output for output in outputs if output.role is None)
        assert terminal.payload["reason"] == "model_usage_uncertain"
        assert terminal.payload["diagnostic"] == {
            "stage": failure_stage,
            "boundary": "invocation",
            "role": None,
            "response_hash": None,
            "validation_kind": None,
            "invocation_phase": "turn_run",
            "invocation_kind": "timeout" if timeout else "unexpected_exception",
        }
        stage_input = terminal.payload["stage_input"]
        assert stage_input["stage"] == failure_stage
        assert len(stage_input["request_hash"]) == len(stage_input["frozen_inputs_hash"]) == 64
        assert "synthetic-private-invocation-canary" not in json.dumps([o.payload for o in outputs])
        assert (
            terminal.content_hash
            == hashlib.sha256(
                json.dumps(
                    {"status": "failed", "role": None, "payload": terminal.payload, "intent": None},
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                ).encode()
            ).hexdigest()
        )
    path = f"/internal/v1/research-runs/{admission.run_id}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)), base_url="http://testserver"
    ) as client_http:
        response = await client_http.get(path, headers=headers("GET", path))
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["status"] == "failed" and data["intent"] is None
    assert data["abstention_reason"] == "model_usage_uncertain"
    assert (data["input_tokens"], data["output_tokens"]) == expected_usage
    assert data["failed_stage_input"] == stage_input
    assert "diagnostic" not in data  # This slice does not expand the public DTO.
    assert "synthetic-private-invocation-canary" not in response.text


@pytest.mark.asyncio
@pytest.mark.parametrize("assessment_rejection", [False, True])
async def test_native_v3_domain_rejection_persists_safe_reason_without_text_or_retry(
    migrated_database_url,
    monkeypatch,
    assessment_rejection,
):
    from nexus.agents import codex_staged
    from nexus.agents.responses_budget import UsageSnapshot
    from nexus.ops.openai_local import prepare_database

    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    factory = session_factory(settings)
    await _seed_authority(factory, include_base_evidence=False)
    await _seed_database_tool_authority(
        factory, link_price=False, bar_count=1, price_quality_flags=("missing",)
    )
    await prepare_database(settings, structured_synthesis=True)
    async with factory() as session, session.begin():
        model = await session.scalar(
            select(AgentModelConfigRecord).where(
                AgentModelConfigRecord.version == "codex-sdk-chatgpt-v3"
            )
        )
        admission = await admit_research_run(
            session,
            actor_id="sdk-test",
            route="/internal/v1/research-runs",
            idempotency_key="sdk-domain-rejection",
            request_hash="a" * 64,
            request=ResearchRequest(
                instrument_id=_INSTRUMENT_ID, data_snapshot_id=_SNAPSHOT_ID, decision_time=_NOW
            ),
            prompt_version_id=None,
            model_config_id=model.id,
            daily_limit_usd=Decimal("3"),
            now=datetime(2026, 9, 2, tzinfo=UTC),
        )
    body = {
        "roles": [
            {
                "role": role,
                "vote": "abstain",
                "conflicting_facts": False,
                "claims": [{"text": "synthetic-private-canary; blocked", "citations": [0]}],
                "reviewed_claim_ids": [],
            }
            for role in ROLE_ORDER[:5]
        ]
    }
    raw = json.dumps(body)
    codex_staged._ResearchBatch.model_validate_json(raw)
    invoke = AsyncMock(return_value=(raw, UsageSnapshot(input_tokens=300, output_tokens=80)))
    if assessment_rejection:

        async def staged_invoke(payload, *, output_model):
            nonlocal raw
            if payload["stage"] == "synthesis":
                response = {
                    "synthesis_assessment": {
                        "stance": "watch",
                        "claims": [{"text": "冻结资料不足以形成结论。", "citations": [0]}],
                        "reviews": [
                            {
                                "claim_id": item["claim_id"],
                                "status": "accepted",
                                "rationale": "synthetic-private-canary",
                                "citations": [0],
                                "resolved": True,
                                "missing_evidence": [],
                                # Empty text is schema-valid but not the required null.
                                "prevented_conclusion": "",
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
                            "vote": "abstain",
                            "conflicting_facts": False,
                            "claims": [{"text": "缺少合格行情。", "citations": [0]}],
                            "reviewed_claim_ids": (
                                [] if payload["stage"] == "research" else ["bull:0"]
                            ),
                        }
                        for role in payload["requested_roles"]
                    ]
                }
            raw = json.dumps(response)
            output_model.model_validate_json(raw)
            return raw, UsageSnapshot(input_tokens=300, output_tokens=80)

        invoke.side_effect = staged_invoke
    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    assert await run_agent_worker_once(settings) is True
    assert await run_agent_worker_once(settings) is False
    calls = 3 if assessment_rejection else 1
    reason = "model_stage_review_invalid" if assessment_rejection else "model_stage_output_invalid"
    assert invoke.await_count == calls
    async with factory() as session:
        run = await session.get(AgentResearchRunRecord, admission.run_id)
        assert run.status == "failed" and run.retry_count == 0
        assert (run.input_tokens, run.output_tokens) == (300 * calls, 80 * calls)
        job = await session.scalar(select(Job).where(Job.payload["run_id"].astext == str(run.id)))
        assert job.attempt_count == job.max_attempts == 1
        outputs = tuple(
            await session.scalars(
                select(AgentResearchOutputRecord).where(AgentResearchOutputRecord.run_id == run.id)
            )
        )
        assert len(outputs) == (8 if assessment_rejection else 1)
        assert {output.role for output in outputs if output.role is not None} == (
            set(ROLE_ORDER[:7]) if assessment_rejection else set()
        )
        assert all(output.intent is None for output in outputs)
        terminal = next(output for output in outputs if output.role is None)
        assert terminal.payload["reason"] == reason
        diagnostic = terminal.payload["diagnostic"]
        assert diagnostic["boundary"] == (
            "assessment_binding" if assessment_rejection else "role_binding"
        )
        assert diagnostic["validation_kind"] == (
            "review_resolution_inconsistent" if assessment_rejection else "unsafe_text"
        )
        assert diagnostic["response_hash"] == hashlib.sha256(raw.encode()).hexdigest()
        assert diagnostic["role"] == ("portfolio" if assessment_rejection else "orchestrator")
        assert terminal.payload["stage_input"]["stage"] == (
            "synthesis" if assessment_rejection else "research"
        )
        assert "synthetic-private-canary" not in json.dumps([output.payload for output in outputs])
    path = f"/internal/v1/research-runs/{admission.run_id}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)), base_url="http://testserver"
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
    assert response.status_code == 200
    assert response.json()["data"]["abstention_reason"] == reason
    assert "synthetic-private-canary" not in response.text


@pytest.mark.asyncio
async def test_financial_raw_semantics_reach_sdk_wire_and_respect_old_cutoff(
    migrated_database_url, monkeypatch
):
    from nexus.agents.codex_sdk import CodexCommittee
    from unit.agents.test_codex_sdk import _sdk

    factory, run = await _prepared_price_run(migrated_database_url)
    try:
        async with factory() as session, session.begin():
            original = await session.scalar(select(FinancialFactRecord))
            fields = {
                column.name: getattr(original, column.name)
                for column in FinancialFactRecord.__table__.columns
                if column.name not in {"id", "created_at"}
            }
            session.add(
                FinancialFactRecord(
                    **dict(
                        fields,
                        raw_tag="Revenues",
                        period_start=date(2026, 1, 1),
                        period_end=date(2026, 3, 31),
                        value=Decimal("90.125"),
                    ),
                    created_at=_NOW - timedelta(days=1),
                )
            )
            # Accepted earlier but appended after this run's cutoff: not old knowledge.
            session.add(
                FinancialFactRecord(
                    **dict(fields, period_start=date(2025, 10, 1), period_end=date(2025, 12, 31)),
                    created_at=_NOW + timedelta(seconds=1),
                )
            )
            session.add(
                FinancialFactRecord(
                    **dict(
                        fields,
                        period_start=date(2025, 7, 1),
                        period_end=date(2025, 9, 30),
                        accepted_at=_NOW + timedelta(seconds=1),
                    ),
                    created_at=_NOW - timedelta(days=1),
                )
            )
        _, thread, _, _ = _sdk(monkeypatch)
        async with factory() as session:
            tools = await database_read_only_tools(
                session, run, allow_fixture_evidence=True, allow_fixture_price_evidence=True
            )
            payload = dict(
                instrument_id=str(_INSTRUMENT_ID),
                data_snapshot_id=str(_SNAPSHOT_ID),
                as_of=_NOW,
                fields=["revenue"],
            )
            financial = await tools.execute("get_financial_facts", payload)
            rows = financial["items"]
            assert [(row["raw_taxonomy"], row["raw_tag"]) for row in rows] == [
                ("us-gaap", "RevenueFromContractWithCustomerExcludingAssessedTax"),
                ("us-gaap", "Revenues"),
            ]
            assert [row["period_start"] for row in rows] == ["2026-04-01", "2026-01-01"]
            assert [row["period_end"] for row in rows] == ["2026-06-30", "2026-03-31"]
            assert [row["value"] for row in rows] == ["100.000000000000", "90.125000000000"]
            assert all(
                row["fact"] == "revenue"
                and row["unit"] == "USD"
                and row["period_kind"] == "duration"
                and row["revision"] == "v1"
                and row["evidence_ref_id"] == str(original.evidence_ref_id)
                for row in rows
            )
            before = json.dumps(financial, sort_keys=True)
            prompts = load_prompt_assets()
            evidence = await session.get(ResearchEvidenceRef, original.evidence_ref_id)
            committee = CodexCommittee(prompts=prompts, tools=tools)
            await committee.analyze(
                "orchestrator",
                ResearchRequest(
                    instrument_id=_INSTRUMENT_ID, data_snapshot_id=_SNAPSHOT_ID, decision_time=_NOW
                ),
                (
                    EvidenceRef(
                        source=evidence.source,
                        object_id=evidence.provider_object_id,
                        content_hash=evidence.content_hash,
                        location=evidence.location,
                        available_at=evidence.available_at,
                    ),
                ),
                prompts["orchestrator"],
            )
            thread.run.assert_awaited_once()
            wire = json.loads(thread.run.call_args.args[0])
            assert wire["frozen_inputs"]["financial_facts"] == financial
            assert json.dumps(financial, sort_keys=True) == before
    finally:
        await factory.kw["bind"].dispose()
