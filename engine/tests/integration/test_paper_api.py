from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from urllib.parse import urlencode
from uuid import UUID

import httpx
import pytest
from helpers.paper_api import app, headers, seed_ready_paper_api, settings
from helpers.paper_batch import (
    CALENDAR_ID,
    DATA_SNAPSHOT_ID,
    DECISION_SESSION,
    DECISION_TIME,
    EXECUTION_SESSION,
    INSTRUMENTS,
    UNIVERSE_ID,
    seed_paper_batch,
)
from helpers.paper_execution import seed_paper_execution
from nexus.market.intraday_ingestion import canonical_quote_observation
from nexus.paper.agent_cycle_models import (
    CandidateManifestEntry,
    candidate_manifest_hash,
    canonical_cycle_event_hash,
    floor_decision_slot,
)
from nexus.paper.execution_service import advance_internal_paper_order
from nexus.persistence.models.data import DataArtifact
from nexus.persistence.models.market import LatestQuote
from nexus.persistence.models.paper import (
    PAPER_DECISION_SOURCE_POLICY_ID,
    PAPER_EXECUTION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
    AgentPaperCycleEventRecord,
    AgentPaperCycleRecord,
    StrategyDecisionRecord,
)
from nexus.persistence.session import session_factory
from sqlalchemy import text


@pytest.fixture(autouse=True)
def _historical_execution_authority_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exercise the legacy paper API state machine behind its test-only gate."""
    monkeypatch.setattr(
        "nexus.paper.execution_service._paper_execution_authority_available",
        lambda _source_policy_id: True,
    )


@pytest.mark.asyncio
async def test_paper_endpoints_require_hmac_and_bound_queries(
    migrated_database_url: str,
) -> None:
    engine_app = app(migrated_database_url)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=engine_app),
        base_url="http://testserver",
    ) as client:
        unauthenticated = await client.get("/internal/v1/paper/portfolio")
        invalid = []
        for query in ("limit=0", "limit=51", "status=unknown", "cursor=bad"):
            path = f"/internal/v1/paper/orders?{query}"
            invalid.append(await client.get(path, headers=headers("GET", path)))

    assert unauthenticated.status_code == 401
    assert unauthenticated.json()["error"]["code"] == "UNAUTHENTICATED"
    assert all(response.status_code == 422 for response in invalid)
    assert all(response.json()["error"]["code"] == "VALIDATION_ERROR" for response in invalid)


@pytest.mark.asyncio
async def test_latest_agent_paper_cycle_is_404_without_a_cycle(
    migrated_database_url: str,
) -> None:
    path = "/internal/v1/paper/cycles/latest"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(path, headers=headers("GET", path))

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "NOT_FOUND"


@pytest.mark.asyncio
async def test_workbench_endpoint_is_authenticated_read_only_and_rejects_queries(
    migrated_database_url: str,
) -> None:
    await seed_paper_batch(migrated_database_url)
    path = "/internal/v1/workbench"
    invalid_path = f"{path}?account_id=hidden"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
        invalid = await client.get(
            invalid_path,
            headers=headers("GET", invalid_path),
        )

    assert response.status_code == 200
    assert response.json()["data"]["mode"] == "internal_paper"
    assert response.json()["data"]["execution_locked"] is True
    assert response.json()["data"]["loop"]["status"] == "unknown"
    assert "account_id" not in response.text.lower()
    assert invalid.status_code == 422
    assert invalid.json()["error"]["code"] == "VALIDATION_ERROR"


@pytest.mark.asyncio
async def test_agent_cycle_list_returns_same_day_cycles_newest_first(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    older_time = DECISION_TIME - timedelta(minutes=10)
    newer_time = DECISION_TIME - timedelta(minutes=5)
    older_id = UUID("72000000-0000-0000-0000-000000000101")
    newer_id = UUID("72000000-0000-0000-0000-000000000102")
    older_decision_id = UUID("72000000-0000-0000-0000-000000000201")
    newer_decision_id = UUID("72000000-0000-0000-0000-000000000202")
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        for cycle_id, decision_id, decision_time, hash_digit in (
            (older_id, older_decision_id, older_time, "1"),
            (newer_id, newer_decision_id, newer_time, "2"),
        ):
            session.add(
                AgentPaperCycleRecord(
                    id=cycle_id,
                    account_id=seed.account_id,
                    portfolio_id=seed.portfolio_id,
                    decision_session=DECISION_SESSION,
                    decision_time=decision_time,
                    decision_slot_at=floor_decision_slot(decision_time),
                    execution_session=DECISION_SESSION,
                    universe_snapshot_id=UNIVERSE_ID,
                    quality_report_id=None,
                    decision_price_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
                    execution_price_policy_id=PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
                    candidate_manifest=[],
                    candidate_hash=candidate_manifest_hash(()),
                    sizing_policy_version="agent_paper_sizing_v1",
                    sizing_policy_hash="9" * 64,
                    execution_locked=True,
                )
            )
            await session.flush()
            session.add(
                AgentPaperCycleEventRecord(
                    cycle_id=cycle_id,
                    sequence=1,
                    from_status=None,
                    to_status="prepared",
                    result_code="CYCLE_PREPARED",
                    fact_ids={},
                    occurred_at=decision_time,
                    event_hash=canonical_cycle_event_hash(
                        cycle_id=cycle_id,
                        sequence=1,
                        from_status=None,
                        to_status="prepared",
                        result_code="CYCLE_PREPARED",
                        fact_ids={},
                        occurred_at=decision_time,
                    ),
                )
            )
            session.add(
                StrategyDecisionRecord(
                    id=decision_id,
                    account_id=seed.account_id,
                    portfolio_id=seed.portfolio_id,
                    strategy_id="agent_research",
                    strategy_version="agent_paper_decision_v1",
                    strategy_definition_id=None,
                    registry_event_id=None,
                    champion_generation=None,
                    decision_origin="agent_research",
                    agent_paper_cycle_id=cycle_id,
                    decision_session=DECISION_SESSION,
                    decision_time=decision_time,
                    execution_session=DECISION_SESSION,
                    source_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
                    execution_source_policy_id=PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
                    calendar_authority_id=CALENDAR_ID,
                    universe_snapshot_id=UNIVERSE_ID,
                    quality_report_id=None,
                    portfolio_projection_hash=hash_digit * 64,
                    authority_hash=hash_digit * 64,
                    strategy_parameters_hash=hash_digit * 64,
                    decision_hash=("3" if hash_digit == "1" else "4") * 64,
                    cash_weight=Decimal("1"),
                    reason="AGENT_RESEARCH",
                )
            )
            session.add(
                AgentPaperCycleEventRecord(
                    cycle_id=cycle_id,
                    sequence=2,
                    from_status="prepared",
                    to_status="unavailable",
                    result_code="RISK_INPUT_REQUIRED",
                    fact_ids={},
                    occurred_at=decision_time,
                    event_hash=canonical_cycle_event_hash(
                        cycle_id=cycle_id,
                        sequence=2,
                        from_status="prepared",
                        to_status="unavailable",
                        result_code="RISK_INPUT_REQUIRED",
                        fact_ids={},
                        occurred_at=decision_time,
                    ),
                )
            )

    path = f"/internal/v1/paper/cycles?decision_session={DECISION_SESSION}&limit=20"
    limited_path = f"/internal/v1/paper/cycles?decision_session={DECISION_SESSION}&limit=1"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
        limited = await client.get(limited_path, headers=headers("GET", limited_path))
        current_session_path = "/internal/v1/paper/cycles?limit=20"
        current_session = await client.get(
            current_session_path,
            headers=headers("GET", current_session_path),
        )
        invalid_low_path = "/internal/v1/paper/cycles?limit=0"
        invalid_high_path = "/internal/v1/paper/cycles?limit=21"
        invalid_low = await client.get(
            invalid_low_path, headers=headers("GET", invalid_low_path)
        )
        invalid_high = await client.get(
            invalid_high_path, headers=headers("GET", invalid_high_path)
        )
        detail_path = f"/internal/v1/paper/cycles/{newer_id}"
        detail = await client.get(detail_path, headers=headers("GET", detail_path))
        detail_invalid_path = f"{detail_path}?account_id=hidden"
        detail_invalid = await client.get(
            detail_invalid_path,
            headers=headers("GET", detail_invalid_path),
        )

    assert response.status_code == 200
    assert limited.status_code == 200
    assert len(limited.json()["data"]["items"]) == 1
    assert limited.json()["data"]["summary"]["cycle_count"] == 2
    assert limited.json()["data"]["summary"]["completed_count"] == 0
    items = response.json()["data"]["items"]
    assert [item["cycle_id"] for item in items] == [str(newer_id), str(older_id)]
    assert [item["decision"]["decision_id"] for item in items] == [
        str(newer_decision_id),
        str(older_decision_id),
    ]
    assert [item["decision_slot_at"] for item in items] == [
        floor_decision_slot(newer_time).isoformat().replace("+00:00", "Z"),
        floor_decision_slot(older_time).isoformat().replace("+00:00", "Z"),
    ]
    assert all(item["decision_session"] == DECISION_SESSION.isoformat() for item in items)
    assert current_session.status_code == 200
    assert current_session.json()["data"]["items"] == []
    assert current_session.json()["data"]["summary"] == {
        "decision_session": EXECUTION_SESSION.isoformat(),
        "status": "closed",
        "cycle_count": 0,
        "completed_count": 0,
        "latest_result_code": None,
        "last_completed_decision_at": None,
        "next_eligible_at": None,
        "next_eligible_reason": "CALENDAR_UNAVAILABLE",
        "execution_locked": True,
    }
    assert invalid_low.status_code == invalid_high.status_code == 422
    assert detail.status_code == 200
    assert detail.json()["data"]["cycle"]["cycle_id"] == str(newer_id)
    assert detail.json()["data"]["orders"] == []
    assert detail_invalid.status_code == 422
    assert detail_invalid.json()["error"]["code"] == "VALIDATION_ERROR"
    assert "account_id" not in response.text.lower()


@pytest.mark.asyncio
async def test_latest_schwab_agent_cycle_preserves_terminal_empty_candidate_result(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    cycle_id = UUID("72000000-0000-0000-0000-000000000009")
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        session.add(
            AgentPaperCycleRecord(
                id=cycle_id,
                account_id=seed.account_id,
                portfolio_id=seed.portfolio_id,
                decision_session=DECISION_SESSION,
                decision_time=DECISION_TIME,
                decision_slot_at=floor_decision_slot(DECISION_TIME),
                execution_session=DECISION_SESSION,
                universe_snapshot_id=UNIVERSE_ID,
                quality_report_id=None,
                decision_price_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
                execution_price_policy_id=PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
                candidate_manifest=[],
                candidate_hash=candidate_manifest_hash(()),
                sizing_policy_version="agent_paper_sizing_v1",
                sizing_policy_hash="9" * 64,
                execution_locked=True,
            )
        )
        await session.flush()
        events = (
            (1, None, "prepared", "CYCLE_PREPARED"),
            (2, "prepared", "unavailable", "RISK_INPUT_REQUIRED"),
        )
        for sequence, from_status, to_status, result_code in events:
            session.add(
                AgentPaperCycleEventRecord(
                    cycle_id=cycle_id,
                    sequence=sequence,
                    from_status=from_status,
                    to_status=to_status,
                    result_code=result_code,
                    fact_ids={},
                    occurred_at=DECISION_TIME,
                    event_hash=canonical_cycle_event_hash(
                        cycle_id=cycle_id,
                        sequence=sequence,
                        from_status=from_status,
                        to_status=to_status,
                        result_code=result_code,
                        fact_ids={},
                        occurred_at=DECISION_TIME,
                    ),
                )
            )

    path = "/internal/v1/paper/cycles/latest"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(path, headers=headers("GET", path))

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["cycle_id"] == str(cycle_id)
    assert data["status"] == "unavailable"
    assert data["result_code"] == "RISK_INPUT_REQUIRED"
    assert data["candidate_count"] == 0
    assert data["candidates"] == []
    assert data["decision_market"] is None
    assert data["execution_locked"] is True


@pytest.mark.asyncio
async def test_latest_agent_paper_cycle_returns_only_bounded_redacted_facts(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    candidate = {
        "instrument_id": str(INSTRUMENTS[0][0]),
        "symbol": "AAPL",
        "inclusion_reason": "quality_rank",
        "research_snapshot_id": str(DATA_SNAPSHOT_ID),
    }
    candidate_hash = sha256(
        json.dumps([candidate], sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    cycle_id = UUID("72000000-0000-0000-0000-000000000001")
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        session.add(
            AgentPaperCycleRecord(
                id=cycle_id,
                account_id=seed.account_id,
                portfolio_id=seed.portfolio_id,
                decision_session=DECISION_SESSION,
                decision_time=DECISION_TIME,
                decision_slot_at=floor_decision_slot(DECISION_TIME),
                execution_session=EXECUTION_SESSION,
                universe_snapshot_id=UNIVERSE_ID,
                quality_report_id=None,
                decision_price_policy_id=PAPER_DECISION_SOURCE_POLICY_ID,
                execution_price_policy_id=PAPER_EXECUTION_SOURCE_POLICY_ID,
                candidate_manifest=[candidate],
                candidate_hash=candidate_hash,
                sizing_policy_version="agent_paper_sizing_v1",
                sizing_policy_hash="7" * 64,
                execution_locked=True,
            )
        )
        await session.flush()
        session.add(
            AgentPaperCycleEventRecord(
                cycle_id=cycle_id,
                sequence=1,
                from_status=None,
                to_status="prepared",
                result_code="CYCLE_PREPARED",
                fact_ids={},
                occurred_at=DECISION_TIME,
                event_hash=canonical_cycle_event_hash(
                    cycle_id=cycle_id,
                    sequence=1,
                    from_status=None,
                    to_status="prepared",
                    result_code="CYCLE_PREPARED",
                    fact_ids={},
                    occurred_at=DECISION_TIME,
                ),
            )
        )
    path = "/internal/v1/paper/cycles/latest"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["cycle_id"] == str(cycle_id)
    assert data["execution_locked"] is True
    assert data["candidate_count"] == len(data["candidates"]) == 1
    assert data["decision_market"] is None
    assert data["execution_market"] == {
        "source": "alpaca",
        "feed": "iex",
        "delivery_mode": "real_time",
        "window_minutes": 5,
        "status": "pending",
    }
    assert "prompt" not in response.text.lower()
    assert "account_id" not in response.text.lower()


@pytest.mark.asyncio
async def test_latest_schwab_agent_cycle_uses_its_frozen_quote_and_unknown_execution(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    decision_time = DECISION_TIME - timedelta(minutes=90)
    quote_at = decision_time - timedelta(seconds=10)
    artifact_id = UUID("72000000-0000-0000-0000-000000000010")
    quote_id = UUID("72000000-0000-0000-0000-000000000011")
    artifact_hash = "e" * 64
    candidate = CandidateManifestEntry(
        instrument_id=INSTRUMENTS[0][0],
        symbol="AAPL",
        inclusion_reason="quality_rank",
        research_snapshot_id=DATA_SNAPSHOT_ID,
        prior_daily_price_authority_id=UUID(int=41),
        decision_quote_id=quote_id,
        decision_trade_id=None,
        decision_artifact_id=artifact_id,
        decision_evidence_hash=artifact_hash,
        risk_query_authority_ids=(UUID(int=44), UUID(int=45), UUID(int=46)),
    )
    cycle_id = UUID("72000000-0000-0000-0000-000000000012")
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        artifact = DataArtifact(
            id=artifact_id,
            uri="memory://must-not-leak/schwab.json",
            sha256=artifact_hash,
            byte_size=100,
            mime_type="application/json",
            schema_version="schwab_level_one_stream_v1",
            source="schwab",
            observed_at=quote_at,
            available_at=decision_time,
            received_at=decision_time,
            created_at=decision_time,
        )
        quote_payload = canonical_quote_observation(
            instrument_id=INSTRUMENTS[0][0],
            source="schwab",
            feed="l1",
            delivery_mode="real_time",
            event_kind="quote",
            bid=Decimal("99.90"),
            ask=Decimal("100.10"),
            last=None,
            observed_at=quote_at,
            available_at=decision_time,
            received_at=decision_time,
            revision=artifact_hash,
        )
        frozen_quote = LatestQuote(
            id=quote_id,
            instrument_id=INSTRUMENTS[0][0],
            source="schwab",
            feed="l1",
            delivery_mode="real_time",
            bid=Decimal("99.90"),
            ask=Decimal("100.10"),
            last=None,
            observed_at=quote_at,
            available_at=decision_time,
            received_at=decision_time,
            revision=artifact_hash,
            quality_flags=[],
            event_kind="quote",
            content_hash=sha256(quote_payload).hexdigest(),
            evidence_status="verified",
            data_artifact_id=artifact_id,
            created_at=decision_time,
        )
        newer_at = decision_time + timedelta(seconds=1)
        newer_payload = canonical_quote_observation(
            instrument_id=INSTRUMENTS[0][0],
            source="schwab",
            feed="l1",
            delivery_mode="real_time",
            event_kind="quote",
            bid=Decimal("100.90"),
            ask=Decimal("101.10"),
            last=None,
            observed_at=newer_at,
            available_at=newer_at,
            received_at=newer_at,
            revision=artifact_hash,
        )
        newer_global_quote = LatestQuote(
            id=UUID("72000000-0000-0000-0000-000000000013"),
            instrument_id=INSTRUMENTS[0][0],
            source="schwab",
            feed="l1",
            delivery_mode="real_time",
            bid=Decimal("100.90"),
            ask=Decimal("101.10"),
            last=None,
            observed_at=newer_at,
            available_at=newer_at,
            received_at=newer_at,
            revision=artifact_hash,
            quality_flags=[],
            event_kind="quote",
            content_hash=sha256(newer_payload).hexdigest(),
            evidence_status="verified",
            data_artifact_id=artifact_id,
            created_at=newer_at,
        )
        session.add_all((artifact, frozen_quote, newer_global_quote))
        session.add(
            AgentPaperCycleRecord(
                id=cycle_id,
                account_id=seed.account_id,
                portfolio_id=seed.portfolio_id,
                decision_session=DECISION_SESSION,
                decision_time=decision_time,
                decision_slot_at=floor_decision_slot(decision_time),
                execution_session=DECISION_SESSION,
                universe_snapshot_id=UNIVERSE_ID,
                quality_report_id=None,
                decision_price_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
                execution_price_policy_id=PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
                candidate_manifest=[candidate.model_dump(mode="json")],
                candidate_hash=candidate_manifest_hash((candidate,)),
                sizing_policy_version="agent_paper_sizing_v1",
                sizing_policy_hash="8" * 64,
                execution_locked=True,
            )
        )
        await session.flush()
        session.add(
            AgentPaperCycleEventRecord(
                cycle_id=cycle_id,
                sequence=1,
                from_status=None,
                to_status="prepared",
                result_code="CYCLE_PREPARED",
                fact_ids={},
                occurred_at=decision_time,
                event_hash=canonical_cycle_event_hash(
                    cycle_id=cycle_id,
                    sequence=1,
                    from_status=None,
                    to_status="prepared",
                    result_code="CYCLE_PREPARED",
                    fact_ids={},
                    occurred_at=decision_time,
                ),
            )
        )

    path = "/internal/v1/paper/cycles/latest"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(path, headers=headers("GET", path))

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["decision_market"] == {
        "source": "schwab",
        "feed": "l1",
        "delivery_mode": "real_time",
        "delayed": False,
        "quote_at": quote_at.isoformat().replace("+00:00", "Z"),
    }
    assert data["execution_market"] == {
        "source": "schwab",
        "feed": "price_history",
        "delivery_mode": "unknown",
        "window_minutes": 5,
        "status": "pending",
    }
    wire = response.text.lower()
    assert "must-not-leak" not in wire
    assert all(secret not in wire for secret in ("artifact", "manifest", "token", "transcript"))

    async with factory() as session, session.begin():
        await session.execute(text("ALTER TABLE latest_quotes DISABLE TRIGGER USER"))
        try:
            await session.execute(
                text("DELETE FROM latest_quotes WHERE id=:quote_id"), {"quote_id": quote_id}
            )
        finally:
            await session.execute(text("ALTER TABLE latest_quotes ENABLE TRIGGER USER"))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        incomplete = await client.get(path, headers=headers("GET", path))
    assert incomplete.status_code == 409
    assert incomplete.json()["error"]["code"] == "DATA_PARTIAL"


@pytest.mark.asyncio
async def test_paper_portfolio_returns_redacted_authoritative_performance(
    migrated_database_url: str,
) -> None:
    seed = await seed_ready_paper_api(migrated_database_url)
    path = "/internal/v1/paper/portfolio"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(path, headers=headers("GET", path))

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["mode"] == "internal_paper"
    assert data["currency"] == "USD"
    assert data["data_state"] == "READY"
    assert data["performance"]["as_of"] == "2026-08-27T20:00:00Z"
    comparison = data["comparison"]
    assert comparison["as_of"] == data["performance"]["as_of"]
    assert [row["key"] for row in comparison["rows"]] == [
        "agent",
        "hold_current",
        "baseline_v1",
        "market_benchmark",
    ]
    assert comparison["rows"][0]["data_state"] == "UNAVAILABLE"
    assert comparison["rows"][0]["reason_code"] == "MIXED_STRATEGY_FACTS_UNAVAILABLE"
    assert comparison["rows"][1]["nav"] == data["initial_capital"]
    assert comparison["rows"][1]["total_return_ratio"] == "0"
    assert all(
        row["reason_code"] == "SAME_WINDOW_FORWARD_FACTS_UNAVAILABLE"
        for row in comparison["rows"][2:]
    )
    quantity = seed.execution.quantity
    cost = quantity * Decimal("100.25")
    cash = Decimal("10000") - cost
    assert Decimal(data["performance"]["cash"]) == cash
    available_cash = Decimal(data["performance"]["available_cash"])
    reserved_cash = Decimal(data["performance"]["reserved_cash"])
    assert available_cash + reserved_cash == cash
    assert reserved_cash > 0
    assert len(data["positions"]) == 1
    position = data["positions"][0]
    assert position["instrument_id"] == str(seed.execution.instrument_id)
    assert Decimal(position["quantity"]) == quantity
    assert Decimal(position["available_quantity"]) == quantity
    assert Decimal(position["cost_basis"]) == cost
    assert Decimal(position["average_cost"]) == Decimal("100.25")
    assert Decimal(position["reference_close"]) == Decimal("110")
    assert Decimal(position["market_value"]) == quantity * Decimal("110")
    assert position["price_source"] == "fixture"
    assert position["price_feed"] == "fixture"
    assert position["delivery_mode"] == "fixture"
    assert position["price_as_of"] == "2026-08-27T20:00:00Z"
    assert {item["dimension"] for item in data["attributions"]} == {
        "cash",
        "instrument",
        "sector",
    }
    wire = response.text.lower()
    assert "account_id" not in wire
    assert "client_order_id" not in wire
    assert "provider_object" not in wire


@pytest.mark.asyncio
async def test_paper_orders_are_status_filtered_paginated_and_include_server_timeline(
    migrated_database_url: str,
) -> None:
    await seed_ready_paper_api(migrated_database_url)
    engine_app = app(migrated_database_url)
    first_path = "/internal/v1/paper/orders?limit=1"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=engine_app),
        base_url="http://testserver",
    ) as client:
        first = await client.get(first_path, headers=headers("GET", first_path))
        cursor = first.json()["data"]["next_cursor"]
        second_path = "/internal/v1/paper/orders?" + urlencode({"limit": 1, "cursor": cursor})
        second = await client.get(second_path, headers=headers("GET", second_path))
        filled_path = "/internal/v1/paper/orders?status=filled"
        filled = await client.get(filled_path, headers=headers("GET", filled_path))
        drift_path = "/internal/v1/paper/orders?" + urlencode(
            {"status": "submitted", "cursor": cursor}
        )
        drift = await client.get(drift_path, headers=headers("GET", drift_path))

    assert first.status_code == second.status_code == filled.status_code == 200
    assert len(first.json()["data"]["items"]) == 1
    assert len(second.json()["data"]["items"]) == 1
    assert (
        first.json()["data"]["items"][0]["order_id"]
        != second.json()["data"]["items"][0]["order_id"]
    )
    filled_items = filled.json()["data"]["items"]
    assert len(filled_items) == 1
    filled_order = filled_items[0]
    assert filled_order["status"] == "filled"
    assert filled_order["cancellable"] is False
    assert filled_order["strategy_version"] == "baseline_v1"
    assert filled_order["risk_policy_version"] == "risk_policy_v1"
    assert [event["event_seq"] for event in filled_order["events"]] == list(
        range(1, filled_order["current_event_seq"] + 1)
    )
    assert len(filled_order["fills"]) == 1
    assert filled_order["fills"][0]["source"] == "fixture"
    assert filled_order["fills"][0]["slippage_bps"] == 25
    assert drift.status_code == 422
    assert drift.json()["error"]["code"] == "VALIDATION_ERROR"
    assert "account_id" not in first.text + second.text + filled.text


@pytest.mark.asyncio
async def test_paper_cancel_is_versioned_idempotent_and_never_creates_a_fill(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_execution(
        migrated_database_url,
        window_bar_volumes=(Decimal("40"),),
    )
    now = datetime(2026, 8, 27, 14, 0, tzinfo=UTC)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        partial = await advance_internal_paper_order(
            session,
            order_id=seed.order_id,
            evaluated_at=seed.evaluation_times[0],
            idempotency_key="paper-api-partial-fill",
        )
    assert partial.action == "partially_filled"
    engine_app = app(migrated_database_url, now=now)
    list_path = "/internal/v1/paper/orders?status=partially_filled"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=engine_app),
        base_url="http://testserver",
    ) as client:
        page = await client.get(list_path, headers=headers("GET", list_path))
        order = page.json()["data"]["items"][0]
        cancel_path = f"/internal/v1/paper/orders/{seed.order_id}/cancel"
        body = json.dumps(
            {"expected_event_seq": order["current_event_seq"]},
            separators=(",", ":"),
        ).encode()
        wrong_body = json.dumps(
            {"expected_event_seq": order["current_event_seq"] + 1},
            separators=(",", ":"),
        ).encode()
        wrong_headers = headers("POST", cancel_path, wrong_body)
        wrong_headers["Idempotency-Key"] = "paper-api-wrong-version"
        wrong = await client.post(cancel_path, content=wrong_body, headers=wrong_headers)
        extra_body = json.dumps(
            {"expected_event_seq": order["current_event_seq"], "force": True},
            separators=(",", ":"),
        ).encode()
        extra_headers = headers("POST", cancel_path, extra_body)
        extra_headers["Idempotency-Key"] = "paper-api-extra-field"
        extra = await client.post(cancel_path, content=extra_body, headers=extra_headers)
        unknown_path = "/internal/v1/paper/orders/67000000-0000-0000-0000-000000000099/cancel"
        unknown_headers = headers("POST", unknown_path, body)
        unknown_headers["Idempotency-Key"] = "paper-api-unknown"
        unknown = await client.post(unknown_path, content=body, headers=unknown_headers)
        cancel_headers = headers("POST", cancel_path, body)
        cancel_headers["Idempotency-Key"] = "paper-api-cancel"
        cancelled = await client.post(cancel_path, content=body, headers=cancel_headers)
        replay_headers = headers("POST", cancel_path, body)
        replay_headers["Idempotency-Key"] = "paper-api-cancel"
        replay = await client.post(cancel_path, content=body, headers=replay_headers)
        changed_body = json.dumps(
            {"expected_event_seq": order["current_event_seq"] + 1},
            separators=(",", ":"),
        ).encode()
        changed_headers = headers("POST", cancel_path, changed_body)
        changed_headers["Idempotency-Key"] = "paper-api-cancel"
        changed = await client.post(
            cancel_path,
            content=changed_body,
            headers=changed_headers,
        )

    assert cancelled.status_code == replay.status_code == 200
    assert wrong.status_code == 409
    assert wrong.json()["error"]["code"] == "VERSION_CONFLICT"
    assert extra.status_code == 422
    assert extra.json()["error"]["code"] == "VALIDATION_ERROR"
    assert unknown.status_code == 404
    assert unknown.json()["error"]["code"] == "NOT_FOUND"
    assert cancelled.json()["data"]["status"] == "cancelled"
    assert cancelled.json()["data"]["filled_quantity"] == "2.000000000000"
    assert cancelled.json()["data"]["replayed"] is False
    assert replay.json()["data"]["replayed"] is True
    assert changed.status_code == 409
    assert changed.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
