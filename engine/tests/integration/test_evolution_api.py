"""Authenticated PostgreSQL contracts for the strategy-evolution API."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from uuid import UUID

import httpx
import pytest
from helpers.evolution_evaluation import NOW as EVALUATION_NOW
from helpers.evolution_evaluation import seed_evaluation_pair
from helpers.hmac import TEST_HMAC_KEY_ID, TEST_HMAC_SECRET, build_test_hmac_headers
from nexus.backtest.engine import exchange_calendar_content_hash
from nexus.domain.ids import new_uuid7
from nexus.evolution.evaluation import (
    persist_champion_comparison,
    persist_historical_promotion_decision,
)
from nexus.evolution.jobs import (
    build_strategy_rnd_job_payload,
    build_strategy_rnd_registry,
    enqueue_strategy_rnd_job,
)
from nexus.evolution.registry import registry_event_content_hash
from nexus.evolution.repository import append_admitted_event
from nexus.jobs.models import QueueName
from nexus.jobs.registry import JobHandlerResult
from nexus.jobs.worker import run_job_worker_once
from nexus.main import create_engine_app
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
    StrategyDefinitionRecord,
)
from nexus.persistence.models.data import DataSnapshot, QualityReport
from nexus.persistence.models.evolution import (
    StrategyEvolutionPolicyRecord,
    StrategyExperimentFamilyRecord,
    StrategyExperimentRecord,
    StrategyExperimentStateProjectionRecord,
    StrategyRegistryEventRecord,
    StrategyRegistryHeadRecord,
    StrategyResearchLineageRecord,
)
from nexus.persistence.models.runtime import Job
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from nexus.strategy.dsl import baseline_v1_document, compile_strategy, evolution_feature_registry_v1
from sqlalchemy import select

_CLOCK = datetime(2026, 8, 27, 16, tzinfo=UTC)


def _settings(database_url: str) -> EngineSettings:
    return EngineSettings(
        NEXUS_ENV="test",
        DATABASE_URL=database_url,
        NEXUS_ENGINE_HMAC_CURRENT_KEY_ID=TEST_HMAC_KEY_ID,
        NEXUS_ENGINE_HMAC_CURRENT_SECRET=TEST_HMAC_SECRET,
    )


def _app(database_url: str):
    app = create_engine_app(_settings(database_url))
    app.state.utc_clock = lambda: _CLOCK
    return app


def _headers(method: str, path: str, body: bytes = b"") -> dict[str, str]:
    headers = build_test_hmac_headers(
        method,
        path,
        body,
        nonce=str(new_uuid7()),
    )
    if body:
        headers["Content-Type"] = "application/json"
    return headers


def _action_headers(
    path: str,
    body: bytes,
    *,
    key: str,
    etag: str,
) -> dict[str, str]:
    headers = _headers("POST", path, body)
    headers["Idempotency-Key"] = key
    headers["If-Match"] = etag
    return headers


@pytest.mark.asyncio
async def test_empty_overview_is_verified_live_closed_and_hmac_protected(
    migrated_database_url: str,
) -> None:
    app = _app(migrated_database_url)
    path = "/internal/v1/strategy-evolution"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        unauthenticated = await client.get(path)
        response = await client.get(path, headers=_headers("GET", path))

    assert unauthenticated.status_code == 401
    assert response.status_code == 200
    assert response.headers["etag"] == '"g1-v1"'
    payload = response.json()
    assert payload["data"]["status"] == "empty"
    assert payload["data"]["write_enabled"] is True
    assert payload["data"]["experiment_start_enabled"] is False
    assert payload["data"]["experiment_start_reason"] == "LINEAGE_AUTHORITY_INCOMPLETE"
    assert payload["data"]["live_execution_enabled"] is False
    assert payload["data"]["champion"] == {
        "strategy_version": "baseline_v1",
        "generation": 1,
        "registry_version": 1,
        "event_sequence": 1,
        "effective_session": None,
        "auto_promotion_paused": False,
        "pending_transition": None,
        "pending_effective_session": None,
        "live_execution_enabled": False,
    }
    assert payload["data"]["experiments"] == []
    assert payload["data"]["registry_events"][0]["event_type"] == "GENESIS"
    wire = response.text.lower()
    assert not any(
        forbidden in wire
        for forbidden in (
            "prompt_version_id",
            "model_config_id",
            "candidate_delta",
            "strategy_dsl",
            "account_id",
            "order_id",
            "api_key",
        )
    )


@pytest.mark.asyncio
async def test_pause_resume_are_cas_guarded_and_completed_replays_ignore_new_head(
    migrated_database_url: str,
) -> None:
    app = _app(migrated_database_url)
    path = "/internal/v1/strategy-registry/control"
    pause = json.dumps({"action": "pause_auto_promotion"}, separators=(",", ":")).encode()
    resume = json.dumps({"action": "resume_auto_promotion"}, separators=(",", ":")).encode()

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        first = await client.post(
            path,
            content=pause,
            headers=_action_headers(path, pause, key="pause-first", etag='"g1-v1"'),
        )
        replay = await client.post(
            path,
            content=pause,
            headers=_action_headers(path, pause, key="pause-first", etag='"g1-v1"'),
        )
        conflicting_replay = await client.post(
            path,
            content=resume,
            headers=_action_headers(path, resume, key="pause-first", etag='"g1-v1"'),
        )
        missing_idempotency = await client.post(
            path,
            content=resume,
            headers={
                **_headers("POST", path, resume),
                "If-Match": '"g1-v2"',
            },
        )
        stale = await client.post(
            path,
            content=resume,
            headers=_action_headers(path, resume, key="resume-stale", etag='"g1-v1"'),
        )
        resumed = await client.post(
            path,
            content=resume,
            headers=_action_headers(path, resume, key="resume-fresh", etag='"g1-v2"'),
        )

    assert first.status_code == replay.status_code == 200
    assert first.json() == replay.json()
    assert first.json()["data"]["status"] == "PAUSED"
    assert first.headers["etag"] == replay.headers["etag"] == '"g1-v2"'
    assert stale.status_code == 409
    assert stale.json()["error"]["code"] == "VERSION_CONFLICT"
    assert resumed.status_code == 200
    assert resumed.json()["data"]["status"] == "EFFECTIVE"
    assert resumed.headers["etag"] == '"g1-v3"'
    assert conflicting_replay.status_code == 409
    assert conflicting_replay.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
    assert missing_idempotency.status_code == 422


@pytest.mark.asyncio
async def test_start_experiment_accepts_no_parameters_and_queues_only_strategy_job(
    migrated_database_url: str,
) -> None:
    factory = session_factory(_settings(migrated_database_url))
    async with factory() as session, session.begin():
        search_policy = await session.scalar(
            select(StrategyEvolutionPolicyRecord).where(
                StrategyEvolutionPolicyRecord.policy_kind == "evolution_search_space"
            )
        )
        assert search_policy is not None
        snapshot = DataSnapshot(
            source="fixture",
            provider_object_id="evolution-api-quality",
            revision="v1",
            observed_at=_CLOCK - timedelta(minutes=2),
            available_at=_CLOCK - timedelta(minutes=1),
            received_at=_CLOCK - timedelta(minutes=1),
            schema_version="evolution-api-v1",
            payload={},
            created_at=_CLOCK - timedelta(minutes=1),
        )
        session.add(snapshot)
        await session.flush()
        session.add_all(
            [
                QualityReport(
                    data_snapshot_id=snapshot.id,
                    status="PASS",
                    schema_version="1",
                    payload={},
                    created_at=_CLOCK - timedelta(seconds=30),
                ),
                StrategyResearchLineageRecord(
                    lineage_key="evolution-api-lineage-v1",
                    evolution_policy_id=search_policy.id,
                    cumulative_oos_trial_count=0,
                    status="active",
                    created_at=_CLOCK - timedelta(minutes=1),
                ),
            ]
        )

    app = _app(migrated_database_url)
    path = "/internal/v1/strategy-experiments"
    empty_body = b"{}"
    forbidden_body = b'{"strategy":{"version":"escape"}}'
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        forbidden = await client.post(
            path,
            content=forbidden_body,
            headers=_action_headers(
                path, forbidden_body, key="generate-forbidden", etag='"g1-v1"'
            ),
        )
        queued = await client.post(
            path,
            content=empty_body,
            headers=_action_headers(path, empty_body, key="generate-first", etag='"g1-v1"'),
        )
        replay = await client.post(
            path,
            content=empty_body,
            headers=_action_headers(path, empty_body, key="generate-first", etag='"g1-v1"'),
        )

    assert forbidden.status_code == 422
    assert queued.status_code == replay.status_code == 202
    assert queued.json() == replay.json()
    assert queued.json()["data"]["status"] == "QUEUED"
    assert queued.json()["data"]["live_execution_enabled"] is False
    assert queued.headers["location"].startswith("/internal/v1/strategy-evolution")
    async with factory() as session:
        jobs = tuple(await session.scalars(select(Job)))
    assert len(jobs) == 1
    assert jobs[0].queue == "strategy_rnd"
    assert jobs[0].payload["operation"] == "generate_family"


@pytest.mark.asyncio
async def test_start_experiment_fails_closed_without_lineage_or_quality(
    migrated_database_url: str,
) -> None:
    app = _app(migrated_database_url)
    path = "/internal/v1/strategy-experiments"
    body = b"{}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        response = await client.post(
            path,
            content=body,
            headers=_action_headers(path, body, key="generate-no-authority", etag='"g1-v1"'),
        )

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "AUTHORITY_UNAVAILABLE"
    assert response.json()["error"]["details"]["reason_code"] == (
        "LINEAGE_AUTHORITY_INCOMPLETE"
    )


@pytest.mark.asyncio
async def test_veto_appends_verified_events_and_never_changes_champion(
    migrated_database_url: str,
) -> None:
    factory = session_factory(_settings(migrated_database_url))
    experiment_id, family_id = await _seed_admitted_experiment(factory)
    async with factory() as session, session.begin():
        queued_job = await enqueue_strategy_rnd_job(
            session,
            payload=build_strategy_rnd_job_payload(
                operation="evaluate_candidate",
                family_id=family_id,
                experiment_id=experiment_id,
                expected_champion_generation=1,
            ),
            effective_at=_CLOCK - timedelta(minutes=1),
        )
    app = _app(migrated_database_url)
    path = f"/internal/v1/strategy-experiments/{experiment_id}/veto"
    body = json.dumps({"reason": "USER_VETO"}, separators=(",", ":")).encode()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        start_path = "/internal/v1/strategy-experiments"
        empty_body = b"{}"
        active_family = await client.post(
            start_path,
            content=empty_body,
            headers=_action_headers(
                start_path,
                empty_body,
                key="generate-active-family",
                etag='"g1-v1"',
            ),
        )
        detail = await client.get(
            f"/internal/v1/strategy-experiments/{experiment_id}",
            headers=_headers("GET", f"/internal/v1/strategy-experiments/{experiment_id}"),
        )
        vetoed = await client.post(
            path,
            content=body,
            headers=_action_headers(path, body, key="veto-first", etag='"g1-v1"'),
        )

    assert active_family.status_code == 422
    assert active_family.json()["error"]["details"]["reason_code"] == (
        "ACTIVE_FAMILY_EXISTS"
    )
    assert detail.status_code == 200
    assert detail.json()["data"]["integrity_status"] == "verified"
    assert vetoed.status_code == 200
    assert vetoed.json()["data"]["status"] == "VETOED"
    assert vetoed.headers["etag"] == '"g1-v2"'

    executed: list[str] = []

    async def execute(context, payload):
        del context
        executed.append(payload.operation)
        return JobHandlerResult(outcome="succeeded", result_code="SHOULD_NOT_EXECUTE")

    await run_job_worker_once(
        factory,
        registry=build_strategy_rnd_registry(executor=execute),
        queue=QueueName.STRATEGY_RND,
        worker_id="veto-preflight-test",
        worker_started_at=_CLOCK,
        process_version="test-v1",
        migration_head="0022_strategy_evolution",
        clock=lambda: _CLOCK + timedelta(seconds=1),
        claim_registered_only=True,
    )
    async with factory() as session:
        head = await session.get(StrategyRegistryHeadRecord, "internal_paper")
        projection = await session.get(StrategyExperimentStateProjectionRecord, family_id)
        persisted_job = await session.get(Job, queued_job.id)
    assert head is not None and head.active_generation == 1
    assert projection is not None and projection.current_state == "VETOED"
    assert persisted_job is not None and persisted_job.status == "skipped"
    assert persisted_job.result_code == "FAMILY_TERMINAL"
    assert executed == []


@pytest.mark.asyncio
async def test_experiment_history_defaults_to_twenty_and_uses_signed_stable_cursor(
    migrated_database_url: str,
) -> None:
    factory = session_factory(_settings(migrated_database_url))
    await _seed_admitted_experiment(factory, candidate_count=21)
    app = _app(migrated_database_url)
    first_path = "/internal/v1/strategy-evolution"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        first = await client.get(first_path, headers=_headers("GET", first_path))
        cursor = first.json()["data"]["next_cursor"]
        second_path = f"{first_path}?cursor={cursor}"
        second = await client.get(second_path, headers=_headers("GET", second_path))
        too_large_path = f"{first_path}?limit=101"
        too_large = await client.get(
            too_large_path, headers=_headers("GET", too_large_path)
        )
        forged_path = f"{first_path}?cursor={cursor[:-1]}x"
        forged = await client.get(forged_path, headers=_headers("GET", forged_path))

    assert first.status_code == second.status_code == 200
    assert len(first.json()["data"]["experiments"]) == 20
    assert len(second.json()["data"]["experiments"]) == 1
    first_ids = {item["experiment_id"] for item in first.json()["data"]["experiments"]}
    second_ids = {item["experiment_id"] for item in second.json()["data"]["experiments"]}
    assert first_ids.isdisjoint(second_ids)
    assert second.json()["data"]["next_cursor"] is None
    assert too_large.status_code == 422
    assert forged.status_code == 422


@pytest.mark.asyncio
async def test_detail_serializes_decimal_evidence_from_a_verified_event_chain(
    migrated_database_url: str,
) -> None:
    factory = session_factory(_settings(migrated_database_url))
    pair = await seed_evaluation_pair(factory)
    async with factory() as session, session.begin():
        comparison = await persist_champion_comparison(
            session,
            experiment_id=pair.experiment_id,
            champion_backtest_run_id=pair.champion_run_id,
            challenger_backtest_run_id=pair.challenger_run_id,
            champion_generation=1,
            cumulative_oos_trial_count=3,
            now=EVALUATION_NOW,
        )
        await persist_historical_promotion_decision(
            session,
            comparison_id=comparison.id,
            promotion_policy_id=pair.promotion_policy_id,
            now=EVALUATION_NOW,
        )

    app = _app(migrated_database_url)
    path = f"/internal/v1/strategy-experiments/{pair.experiment_id}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        response = await client.get(path, headers=_headers("GET", path))

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["integrity_status"] == "verified"
    assert data["write_enabled"] is True
    assert data["comparison"]["status"] == "available"
    for metric in ("annualized_return", "sharpe", "max_drawdown", "double_cost_return"):
        assert all(
            value is None or isinstance(value, str)
            for value in data["comparison"][metric].values()
        )
    assert [item["gate"] for item in data["gates"]] == [
        "minimum_oos_windows",
        "deflated_sharpe_confidence",
        "sharpe_or_return_improvement",
        "maximum_drawdown_degradation",
        "double_cost_excess_positive",
        "evidence_completeness",
    ]
    assert not any(
        forbidden in response.text.lower()
        for forbidden in ("candidate_delta", "prompt", "model_config", "account_id", "order_id")
    )


@pytest.mark.asyncio
async def test_rollback_uses_server_calendar_and_leaves_transition_pending(
    migrated_database_url: str,
) -> None:
    factory = session_factory(_settings(migrated_database_url))
    await _seed_effective_registry_promotion(factory)
    app = _app(migrated_database_url)
    path = "/internal/v1/strategy-registry/rollback"
    body = json.dumps({"reason": "USER_REQUESTED"}, separators=(",", ":")).encode()
    headers = _action_headers(path, body, key="rollback-api-first", etag='"g2-v2"')
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        response = await client.post(path, content=body, headers=headers)

    assert response.status_code == 202
    assert response.headers["etag"] == '"g2-v3"'
    assert response.headers["location"] == "/internal/v1/strategy-evolution"
    assert response.json()["data"]["status"] == "PENDING_BOUNDARY"
    assert response.json()["data"]["pending_effective_session"] == "2026-08-28"
    assert response.json()["data"]["live_execution_enabled"] is False
    async with factory() as session:
        head = await session.get(StrategyRegistryHeadRecord, "internal_paper")
        events = tuple(
            await session.scalars(
                select(StrategyRegistryEventRecord).order_by(
                    StrategyRegistryEventRecord.sequence
                )
            )
        )
    assert head is not None and head.active_generation == 2
    assert head.pending_event_id == events[-1].id
    assert events[-1].event_type == "ROLLBACK"
    assert events[-1].champion_generation == 3


async def _seed_effective_registry_promotion(factory) -> None:
    effective_session = datetime(2026, 8, 27, tzinfo=UTC).date()
    next_session = datetime(2026, 8, 28, tzinfo=UTC).date()
    decision_at = datetime(2026, 8, 28, 21, tzinfo=UTC)
    async with factory() as session, session.begin():
        head = await session.get(StrategyRegistryHeadRecord, "internal_paper")
        assert head is not None
        prior = await session.get(StrategyRegistryEventRecord, head.last_event_id)
        assert prior is not None
        document = baseline_v1_document()
        document["version"] = "api_promoted_champion_v2"
        compiled = compile_strategy(document, evolution_feature_registry_v1())
        challenger = StrategyDefinitionRecord(
            strategy_version=compiled.strategy_version,
            schema_version="strategy_dsl_v1",
            expression_hash=compiled.expression_hash,
            document=document,
            lifecycle="challenger",
            published_at=_CLOCK - timedelta(minutes=2),
            created_at=_CLOCK - timedelta(minutes=2),
        )
        session.add(challenger)
        await session.flush()
        payload = {"schema_version": "strategy_registry_promote_payload_v1"}
        event = StrategyRegistryEventRecord(
            channel="internal_paper",
            schema_version="strategy_registry_event_v1",
            sequence=2,
            event_type="PROMOTE",
            idempotency_key="api-effective-promotion",
            actor_kind="service",
            occurred_at=_CLOCK - timedelta(minutes=1),
            from_definition_id=head.active_definition_id,
            to_definition_id=challenger.id,
            promotion_decision_id=None,
            champion_generation=2,
            effective_session=effective_session,
            payload=payload,
            previous_hash=prior.event_hash,
            event_hash=registry_event_content_hash(
                sequence=2,
                event_type="PROMOTE",
                idempotency_key="api-effective-promotion",
                actor_kind="service",
                occurred_at=_CLOCK - timedelta(minutes=1),
                from_definition_id=head.active_definition_id,
                to_definition_id=challenger.id,
                promotion_decision_id=None,
                champion_generation=2,
                effective_session=effective_session,
                payload=payload,
                previous_hash=prior.event_hash,
            ),
            created_at=_CLOCK - timedelta(minutes=1),
        )
        session.add(event)
        await session.flush()
        head.active_definition_id = challenger.id
        head.active_generation = 2
        head.last_event_id = event.id
        head.event_sequence = 2
        head.version = 2
        head.updated_at = _CLOCK - timedelta(minutes=1)
        calendar = ExchangeCalendarAuthorityRecord(
            exchange="XNYS",
            version="evolution-api-next-session-v1",
            source="fixture",
            range_start=next_session,
            range_end=next_session,
            query_parameters={},
            completeness_status="complete",
            content_hash=exchange_calendar_content_hash(
                (next_session,), {next_session: decision_at}
            ),
            published_at=_CLOCK - timedelta(minutes=1),
            created_at=_CLOCK - timedelta(minutes=1),
        )
        session.add(calendar)
        await session.flush()
        session.add(
            ExchangeCalendarSessionRecord(
                calendar_authority_id=calendar.id,
                session=next_session,
                market_open_at=datetime(2026, 8, 28, 13, 30, tzinfo=UTC),
                market_close_at=datetime(2026, 8, 28, 20, tzinfo=UTC),
                decision_at=decision_at,
                created_at=_CLOCK - timedelta(minutes=1),
            )
        )


async def _seed_admitted_experiment(
    factory, *, candidate_count: int = 1
) -> tuple[UUID, UUID]:
    now = _CLOCK - timedelta(minutes=1)
    experiment_ids = tuple(new_uuid7() for _ in range(candidate_count))
    experiment_id = experiment_ids[0]
    family_id = new_uuid7()
    lineage_id = new_uuid7()
    async with factory() as session, session.begin():
        head = await session.get(StrategyRegistryHeadRecord, "internal_paper")
        search_policy = await session.scalar(
            select(StrategyEvolutionPolicyRecord).where(
                StrategyEvolutionPolicyRecord.policy_kind == "evolution_search_space"
            )
        )
        assert head is not None and search_policy is not None
        lineage = StrategyResearchLineageRecord(
            id=lineage_id,
            lineage_key=f"api-lineage-{lineage_id.hex[-12:]}",
            evolution_policy_id=search_policy.id,
            cumulative_oos_trial_count=0,
            status="active",
            created_at=now,
        )
        session.add(lineage)
        await session.flush()
        family = StrategyExperimentFamilyRecord(
            id=family_id,
            research_lineage_id=lineage.id,
            parent_strategy_definition_id=head.active_definition_id,
            rnd_output_id=None,
            champion_generation=head.active_generation,
            admission_status="ADMITTED",
            candidate_count=candidate_count,
            idempotency_key=f"api-family-{family_id.hex[-20:]}",
            authority_hash="a" * 64,
            protocol_hash="b" * 64,
            code_hash="c" * 64,
            created_at=now,
        )
        session.add(family)
        await session.flush()
        for ordinal, candidate_id in enumerate(experiment_ids, start=1):
            document = baseline_v1_document()
            document["version"] = f"api_challenger_{candidate_id.hex[-12:]}"
            compiled = compile_strategy(document, evolution_feature_registry_v1())
            definition = StrategyDefinitionRecord(
                strategy_version=compiled.strategy_version,
                schema_version="strategy_dsl_v1",
                expression_hash=compiled.expression_hash,
                document=document,
                lifecycle="challenger",
                published_at=now,
                created_at=now,
            )
            session.add(definition)
            await session.flush()
            session.add(
                StrategyExperimentRecord(
                    id=candidate_id,
                    family_id=family.id,
                    candidate_strategy_definition_id=definition.id,
                    candidate_ordinal=ordinal,
                    candidate_delta={"schema_version": "strategy_candidate_delta_v1"},
                    candidate_hash=f"{ordinal:064x}",
                    expression_hash=definition.expression_hash,
                    status="ADMITTED",
                    created_at=now,
                )
            )
        event = await append_admitted_event(
            session,
            family_id=family.id,
            idempotency_key=f"api-admitted-{family_id.hex[-20:]}",
            occurred_at=now,
            payload={"schema_version": "strategy_family_admitted_payload_v1"},
        )
        session.add(
            StrategyExperimentStateProjectionRecord(
                family_id=family.id,
                research_lineage_id=lineage.id,
                current_state="ADMITTED",
                event_sequence=event.sequence,
                cumulative_oos_trial_count=0,
                version=1,
                updated_at=now,
            )
        )
    return experiment_id, family_id
