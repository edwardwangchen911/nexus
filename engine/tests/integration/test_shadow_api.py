"""Authenticated, fail-closed Proposal Shadow API contracts."""

import json

import httpx
import pytest
from helpers.portfolio_api import app, headers, settings
from helpers.shadow import (
    seed_complete_shadow_session_inputs,
    seed_complete_shadow_window,
    seed_shadow_eligible_group,
    seed_started_shadow,
)
from nexus.persistence.session import session_factory
from nexus.shadow.service import advance_shadow_session
from sqlalchemy import text


def _post_headers(path: str, body: bytes, *, key: str, version: int) -> dict[str, str]:
    request_headers = headers("POST", path, body)
    request_headers["Idempotency-Key"] = key
    request_headers["If-Match"] = f'"{version}"'
    return request_headers


@pytest.mark.asyncio
async def test_shadow_post_returns_distinct_start_contract_and_replays(
    migrated_database_url: str,
) -> None:
    eligible = await seed_shadow_eligible_group(migrated_database_url)
    path = f"/internal/v1/optimization-proposals/{eligible.group_id}/shadow"
    body = json.dumps({"version": 1}, separators=(",", ":")).encode()
    engine_app = app(migrated_database_url)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=engine_app), base_url="http://testserver"
    ) as client:
        first = await client.post(
            path,
            content=body,
            headers=_post_headers(path, body, key="shadow-api-start", version=1),
        )
        replay = await client.post(
            path,
            content=body,
            headers=_post_headers(path, body, key="shadow-api-start", version=1),
        )

    assert first.status_code == replay.status_code == 200
    assert first.json()["data"] == replay.json()["data"]
    assert first.json()["data"] == {
        "proposal_group_id": str(eligible.group_id),
        "shadow_group_id": first.json()["data"]["shadow_group_id"],
        "status": "shadowing",
        "version": 2,
        "start_session": eligible.calendar_sessions[0].isoformat(),
        "evaluation_end_session": eligible.calendar_sessions[62].isoformat(),
        "grace_end_session": eligible.calendar_sessions[67].isoformat(),
    }
    assert "order_id" not in first.text.lower()
    assert "trade_id" not in first.text.lower()


@pytest.mark.asyncio
async def test_shadow_post_enforces_exact_body_cas_and_idempotency_boundaries(
    migrated_database_url: str,
) -> None:
    eligible = await seed_shadow_eligible_group(migrated_database_url)
    path = f"/internal/v1/optimization-proposals/{eligible.group_id}/shadow"
    body = b'{"version":1}'
    engine_app = app(migrated_database_url)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=engine_app), base_url="http://testserver"
    ) as client:
        wrong_if_match = await client.post(
            path,
            content=body,
            headers=_post_headers(path, body, key="shadow-bad-etag", version=0),
        )
        extra_body = b'{"version":1,"advance":true}'
        extra_field = await client.post(
            path,
            content=extra_body,
            headers=_post_headers(path, extra_body, key="shadow-extra", version=1),
        )
        stale_body = b'{"version":0}'
        stale_version = await client.post(
            path,
            content=stale_body,
            headers=_post_headers(path, stale_body, key="shadow-stale", version=0),
        )
        started = await client.post(
            path,
            content=body,
            headers=_post_headers(path, body, key="shadow-cas", version=1),
        )
        changed_body = b'{"version":2}'
        changed_replay = await client.post(
            path,
            content=changed_body,
            headers=_post_headers(path, changed_body, key="shadow-cas", version=2),
        )
        invalid_state = await client.post(
            path,
            content=changed_body,
            headers=_post_headers(path, changed_body, key="shadow-second-start", version=2),
        )

    assert wrong_if_match.status_code == extra_field.status_code == 422
    assert wrong_if_match.json()["error"]["code"] == "VALIDATION_ERROR"
    assert extra_field.json()["error"]["code"] == "VALIDATION_ERROR"
    assert stale_version.status_code == 409
    assert stale_version.json()["error"]["code"] == "VERSION_CONFLICT"
    assert stale_version.json()["error"]["details"] == {"reason": "VERSION_CONFLICT"}
    assert started.status_code == 200
    assert changed_replay.status_code == 409
    assert changed_replay.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
    assert invalid_state.status_code == 409
    assert invalid_state.json()["error"]["code"] == "PROPOSAL_NOT_EXECUTABLE"
    assert invalid_state.json()["error"]["details"] == {
        "reason": "PROPOSAL_STATE_INVALID"
    }


@pytest.mark.asyncio
async def test_shadow_get_distinguishes_not_started_from_empty_verified_shadow(
    migrated_database_url: str,
) -> None:
    eligible = await seed_shadow_eligible_group(migrated_database_url)
    path = f"/internal/v1/optimization-proposals/{eligible.group_id}/shadow"
    engine_app = app(migrated_database_url)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=engine_app), base_url="http://testserver"
    ) as client:
        not_started = await client.get(path, headers=headers("GET", path))
        body = b'{"version":1}'
        started = await client.post(
            path,
            content=body,
            headers=_post_headers(path, body, key="shadow-api-empty", version=1),
        )
        ready = await client.get(path, headers=headers("GET", path))

    assert not_started.status_code == 404
    assert not_started.json()["error"] == {
        "code": "NOT_FOUND",
        "message": "Proposal Shadow was not started.",
        "request_id": not_started.json()["error"]["request_id"],
        "details": {"reason": "SHADOW_NOT_STARTED"},
    }
    assert started.status_code == ready.status_code == 200
    data = ready.json()["data"]
    assert data["coverage"] == {
        "completed": 0,
        "due": 0,
        "required": 63,
        "next_expected_session": eligible.calendar_sessions[0].isoformat(),
        "missing_sessions": [],
    }
    assert data["freshness"]["status"] == "unknown"
    assert data["freshness"]["reason"] == "NO_SESSION_DUE"
    assert [portfolio["variant"] for portfolio in data["portfolios"]] == [
        "hold_current",
        "minimum_change",
        "strategy_target",
    ]
    assert all(portfolio["nav"] == [] for portfolio in data["portfolios"])
    assert data["safety"] == {"creates_orders": False, "trading_approval": False}


@pytest.mark.asyncio
async def test_shadow_get_serializes_exact_decimal_sources_and_initial_outcomes(
    migrated_database_url: str,
) -> None:
    started = await seed_started_shadow(migrated_database_url)
    observed_at = await seed_complete_shadow_session_inputs(
        migrated_database_url, started=started, ordinal=1
    )
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        await advance_shadow_session(
            session,
            group_id=started.eligible.group_id,
            session_date=started.start.start_session,
            observed_at=observed_at,
        )

    path = f"/internal/v1/optimization-proposals/{started.eligible.group_id}/shadow"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(path, headers=headers("GET", path))

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["coverage"]["completed"] == 1
    assert [portfolio["variant"] for portfolio in data["portfolios"]] == [
        "hold_current",
        "minimum_change",
        "strategy_target",
    ]
    assert all(
        isinstance(portfolio["nav"][0]["nav"], str)
        and "." in portfolio["nav"][0]["nav"]
        for portfolio in data["portfolios"]
    )
    assert {(source["source"], source["delivery_mode"]) for source in data["sources"]} == {
        ("alpaca", "delayed"),
        ("fixture", "unknown"),
    }
    assert all(
        isinstance(outcome["fill"]["quantity"], str)
        and isinstance(outcome["fill"]["cost"], str)
        for portfolio in data["portfolios"]
        for outcome in portfolio["initial_outcomes"]
        if outcome["fill"] is not None
    )
    assert "order_id" not in response.text.lower()
    assert "trade_id" not in response.text.lower()


@pytest.mark.asyncio
async def test_shadow_get_serializes_complete_checkpoint_metrics_without_recomputation(
    migrated_database_url: str,
) -> None:
    started = await seed_started_shadow(migrated_database_url)
    observed = await seed_complete_shadow_window(
        migrated_database_url, started=started, through_ordinal=5
    )
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        for index in range(5):
            await advance_shadow_session(
                session,
                group_id=started.eligible.group_id,
                session_date=started.eligible.calendar_sessions[index],
                observed_at=observed[index],
            )

    path = f"/internal/v1/optimization-proposals/{started.eligible.group_id}/shadow"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(path, headers=headers("GET", path))

    assert response.status_code == 200
    checkpoint = response.json()["data"]["checkpoints"][0]
    assert (checkpoint["ordinal"], checkpoint["status"], checkpoint["coverage_count"]) == (
        5,
        "complete",
        5,
    )
    assert [item["variant"] for item in checkpoint["metrics"]] == [
        "hold_current",
        "minimum_change",
        "strategy_target",
    ]
    for variant in checkpoint["metrics"]:
        assert variant["metrics"]["net_cumulative_return"]["status"] == "known"
        assert isinstance(variant["metrics"]["net_cumulative_return"]["value"], str)
        assert all(
            isinstance(item["return_value"], str)
            for item in variant["metrics"]["worst_sessions"]
        )


@pytest.mark.asyncio
async def test_shadow_get_maps_verified_projection_corruption_to_data_partial(
    migrated_database_url: str,
) -> None:
    started = await seed_started_shadow(migrated_database_url)
    observed_at = await seed_complete_shadow_session_inputs(
        migrated_database_url, started=started, ordinal=1
    )
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        await advance_shadow_session(
            session,
            group_id=started.eligible.group_id,
            session_date=started.start.start_session,
            observed_at=observed_at,
        )
    async with factory() as session, session.begin():
        await session.execute(text("ALTER TABLE shadow_portfolio_states DISABLE TRIGGER USER"))
        await session.execute(
            text("UPDATE shadow_portfolio_states SET state_hash = :value"),
            {"value": "f" * 64},
        )
        await session.execute(text("ALTER TABLE shadow_portfolio_states ENABLE TRIGGER USER"))

    path = f"/internal/v1/optimization-proposals/{started.eligible.group_id}/shadow"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(path, headers=headers("GET", path))

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "DATA_PARTIAL"
    assert response.json()["error"]["details"] == {"reason": "DATA_PARTIAL"}
    for forbidden in ("shadow_portfolio_states", "postgres", "credential", "provider"):
        assert forbidden not in response.text.lower()
