from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from urllib.parse import urlencode
from uuid import UUID

import httpx
import pytest
from helpers.paper_api import app, headers, seed_ready_paper_api, settings
from helpers.paper_batch import (
    DATA_SNAPSHOT_ID,
    DECISION_SESSION,
    DECISION_TIME,
    EXECUTION_SESSION,
    INSTRUMENTS,
    QUALITY_REPORT_ID,
    UNIVERSE_ID,
    seed_paper_batch,
)
from helpers.paper_execution import seed_paper_execution
from nexus.paper.agent_cycle_models import (
    CandidateManifestEntry,
    candidate_manifest_hash,
    canonical_cycle_event_hash,
)
from nexus.paper.execution_service import advance_internal_paper_order
from nexus.persistence.models.paper import (
    PAPER_DECISION_SOURCE_POLICY_ID,
    PAPER_EXECUTION_SOURCE_POLICY_ID,
    AgentPaperCycleEventRecord,
    AgentPaperCycleRecord,
)
from nexus.persistence.session import session_factory


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
async def test_latest_agent_paper_cycle_returns_only_bounded_redacted_facts(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    candidate = CandidateManifestEntry(
        instrument_id=INSTRUMENTS[0][0],
        symbol="T1",
        inclusion_reason="quality_rank",
        research_snapshot_id=DATA_SNAPSHOT_ID,
    )
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
                execution_session=EXECUTION_SESSION,
                universe_snapshot_id=UNIVERSE_ID,
                quality_report_id=QUALITY_REPORT_ID,
                decision_price_policy_id=PAPER_DECISION_SOURCE_POLICY_ID,
                execution_price_policy_id=PAPER_EXECUTION_SOURCE_POLICY_ID,
                candidate_manifest=[candidate.model_dump(mode="json")],
                candidate_hash=candidate_manifest_hash((candidate,)),
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
    assert data["market"] == {
        "source": "alpaca",
        "feed": "iex",
        "delivery_mode": "real_time",
        "coverage": "IEX_EXCHANGE_SUBSET",
        "window_minutes": 5,
        "status": "pending",
    }
    assert "prompt" not in response.text.lower()
    assert "account_id" not in response.text.lower()


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
