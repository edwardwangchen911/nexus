"""Fail-closed identity checks for strategy-evolution read models."""

from __future__ import annotations

import hashlib
from uuid import uuid4

import httpx
import pytest
from helpers.evolution_evaluation import NOW as EVALUATION_NOW
from helpers.evolution_evaluation import seed_evaluation_pair
from helpers.evolution_registry import seed_promotable_registry
from nexus.audit.canonical import canonical_json
from nexus.domain.ids import new_uuid7
from nexus.evolution.evaluation import (
    persist_champion_comparison,
    persist_historical_promotion_decision,
)
from nexus.evolution.repository import _experiment_event_identity
from nexus.persistence.models.evolution import (
    StrategyExperimentEventRecord,
    StrategyPromotionDecisionRecord,
    StrategyPromotionPolicyRecord,
    StrategyShadowFinalizationRecord,
)
from nexus.persistence.session import session_factory
from sqlalchemy import select, text
from test_evolution_api import _app, _headers, _settings


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "tamper",
    (
        "comparison_id",
        "experiment_id",
        "comparison_hash",
        "promotion_policy_id",
        "decision_hash",
    ),
)
async def test_detail_rejects_schema_valid_historical_identity_mismatches(
    migrated_database_url: str,
    tamper: str,
) -> None:
    factory = session_factory(_settings(migrated_database_url))
    pair = await seed_evaluation_pair(factory)
    async with factory() as session, session.begin():
        comparison = await persist_champion_comparison(
            session,
            experiment_id=pair.experiment_id,
            champion_backtest_run_id=pair.champion_run_id,
            challenger_backtest_run_id=pair.challenger_run_id,
            now=EVALUATION_NOW,
        )
        outcome = await persist_historical_promotion_decision(
            session,
            comparison_id=comparison.id,
            promotion_policy_id=pair.promotion_policy_id,
            now=EVALUATION_NOW,
        )
        decision = await session.get(StrategyPromotionDecisionRecord, outcome.decision_id)
        assert decision is not None
        table = (
            "strategy_champion_comparisons"
            if tamper in {"comparison_id", "experiment_id", "comparison_hash"}
            else "strategy_promotion_decisions"
        )
        await session.execute(text(f"ALTER TABLE {table} DISABLE TRIGGER USER"))
        if tamper in {"comparison_id", "experiment_id", "comparison_hash"}:
            report = dict(comparison.metric_report)
            report[tamper] = str(uuid4()) if tamper != "comparison_hash" else "f" * 64
            comparison.metric_report = report
        elif tamper == "decision_hash":
            report = dict(decision.gate_report)
            report["decision_hash"] = "f" * 64
            decision.gate_report = report
        else:
            canonical_policy = await session.get(
                StrategyPromotionPolicyRecord, pair.promotion_policy_id
            )
            assert canonical_policy is not None
            alternate = StrategyPromotionPolicyRecord(
                id=new_uuid7(),
                version="promotion_policy_read_tamper_v1",
                document=canonical_policy.document,
                content_hash=canonical_policy.content_hash,
                published_at=canonical_policy.published_at,
                created_at=canonical_policy.created_at,
            )
            session.add(alternate)
            await session.flush()
            decision.promotion_policy_id = alternate.id
        await session.flush()
        await session.execute(text(f"ALTER TABLE {table} ENABLE TRIGGER USER"))

    response = await _detail_response(migrated_database_url, pair.experiment_id)

    assert response.status_code == 200
    detail = response.json()["data"]
    assert detail["integrity_status"] in {"partial", "stale"}
    assert detail["write_enabled"] is False
    assert detail["gates"] == []
    if tamper in {"comparison_id", "experiment_id", "comparison_hash"}:
        assert detail["comparison"]["status"] == "partial"
        for metric in ("annualized_return", "sharpe", "max_drawdown", "double_cost_return"):
            assert set(detail["comparison"][metric].values()) == {None}


@pytest.mark.asyncio
@pytest.mark.parametrize("tamper", ("finalization_hash", "shadow_group_id"))
async def test_detail_rejects_schema_valid_shadow_identity_mismatches(
    migrated_database_url: str,
    tamper: str,
) -> None:
    factory = session_factory(_settings(migrated_database_url))
    seeded = await seed_promotable_registry(migrated_database_url, factory)
    async with factory() as session, session.begin():
        finalization = await session.scalar(select(StrategyShadowFinalizationRecord))
        assert finalization is not None
        table = (
            "strategy_shadow_finalizations"
            if tamper == "finalization_hash"
            else "strategy_experiment_events"
        )
        await session.execute(text(f"ALTER TABLE {table} DISABLE TRIGGER USER"))
        if tamper == "finalization_hash":
            report = dict(finalization.gate_report)
            report["finalization_hash"] = "f" * 64
            finalization.gate_report = report
        else:
            event = await session.scalar(
                select(StrategyExperimentEventRecord)
                .where(
                    StrategyExperimentEventRecord.family_id == seeded.shadow.pair.family_id,
                    StrategyExperimentEventRecord.event_type == "PROMOTION_ELIGIBLE",
                )
                .order_by(StrategyExperimentEventRecord.sequence.desc())
                .limit(1)
            )
            assert event is not None
            payload = dict(event.payload)
            payload["group_id"] = str(uuid4())
            event.payload = payload
            identity = _experiment_event_identity(
                family_id=event.family_id,
                experiment_id=event.experiment_id,
                schema_version=event.schema_version,
                sequence=event.sequence,
                event_type=event.event_type,
                idempotency_key=event.idempotency_key,
                actor_kind=event.actor_kind,
                occurred_at=event.occurred_at,
                payload=payload,
                previous_hash=event.previous_hash,
            )
            event.event_hash = hashlib.sha256(canonical_json(identity).encode()).hexdigest()
        await session.flush()
        await session.execute(text(f"ALTER TABLE {table} ENABLE TRIGGER USER"))

    response = await _detail_response(
        migrated_database_url, seeded.shadow.pair.experiment_id
    )

    assert response.status_code == 200
    detail = response.json()["data"]
    assert detail["integrity_status"] in {"partial", "stale"}
    assert detail["write_enabled"] is False
    assert detail["gates"] == []
    assert detail["shadow"]["after_cost_excess_return"] is None
    assert detail["shadow"]["drawdown_difference"] is None


@pytest.mark.asyncio
async def test_shadow_progress_separates_evaluation_and_grace_sessions(
    migrated_database_url: str,
) -> None:
    factory = session_factory(_settings(migrated_database_url))
    seeded = await seed_promotable_registry(migrated_database_url, factory)

    response = await _detail_response(
        migrated_database_url, seeded.shadow.pair.experiment_id
    )

    assert response.status_code == 200
    shadow = response.json()["data"]["shadow"]
    assert shadow["status"] == "finalized"
    assert shadow["completed_sessions"] == 63
    assert shadow["evaluation_sessions"] == 63
    assert shadow["grace_sessions"] == 5
    assert shadow["grace_elapsed_sessions"] == 0


@pytest.mark.asyncio
async def test_experiment_summary_uses_candidate_state_not_family_aggregate(
    migrated_database_url: str,
) -> None:
    factory = session_factory(_settings(migrated_database_url))
    pair = await seed_evaluation_pair(factory)
    app = _app(migrated_database_url)
    overview_path = "/internal/v1/strategy-evolution"

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        overview = await client.get(
            overview_path, headers=_headers("GET", overview_path)
        )
    detail = await _detail_response(migrated_database_url, pair.experiment_id)

    assert overview.status_code == 200
    overview_data = overview.json()["data"]
    summary = next(
        item
        for item in overview_data["experiments"]
        if item["experiment_id"] == str(pair.experiment_id)
    )
    assert summary["state"] == "OOS_RESERVED"
    assert overview_data["active_family"]["state"] == "BACKTEST_RUNNING"
    assert detail.status_code == 200
    detail_data = detail.json()["data"]
    assert detail_data["experiment"]["state"] == "OOS_RESERVED"
    assert detail_data["family"]["state"] == "BACKTEST_RUNNING"
    assert detail_data["integrity_status"] == "verified"
    assert detail_data["write_enabled"] is True
    assert detail_data["write_block_reason"] is None


async def _detail_response(database_url: str, experiment_id) -> httpx.Response:
    app = _app(database_url)
    path = f"/internal/v1/strategy-experiments/{experiment_id}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        return await client.get(path, headers=_headers("GET", path))
