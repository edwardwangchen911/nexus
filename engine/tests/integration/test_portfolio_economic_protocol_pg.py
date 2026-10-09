"""Actual PostgreSQL registration and redacted API reads, without economic runs."""

import asyncio
from datetime import date, timedelta
from uuid import UUID

import httpx
import pytest
from helpers.paper_api import app, headers, settings
from nexus.paper.portfolio_economic_preflight import EconomicPlanRequest
from nexus.paper.portfolio_economic_protocol import register_protocol
from nexus.persistence.models.runtime import AuditEvent
from nexus.persistence.session import session_factory
from sqlalchemy import func, select, text


def protocol_request(key="isolated-protocol-case"):
    return EconomicPlanRequest(
        evaluation_key=key,
        instruments=(
            {"instrument_id": UUID(int=1), "symbol": "AVGO"},
            {"instrument_id": UUID(int=2), "symbol": "NVDA"},
        ),
        start_session=date(2027, 1, 4),
        research_cost_usd="1",
        synthesis_cost_usd="2",
        global_daily_limit_usd="20",
    )


@pytest.mark.asyncio
async def test_registration_idempotency_conflict_and_normal_api(migrated_database_url):
    factory = session_factory(settings(migrated_database_url))
    async with factory.begin() as session:
        actual = await session.scalar(text("SELECT current_database()"))
        first = await register_protocol(session, protocol_request(), expected_database=actual)
        before = await session.scalar(select(func.count()).select_from(AuditEvent))
    async with factory.begin() as session:
        second = await register_protocol(session, protocol_request(), expected_database=actual)
        assert first.id == second.id
        assert before == await session.scalar(select(func.count()).select_from(AuditEvent))
    async with factory.begin() as session:
        with pytest.raises(ValueError, match="ECONOMIC_PROTOCOL_IDEMPOTENCY_CONFLICT"):
            await register_protocol(
                session, protocol_request("different"), expected_database=actual
            )
    application = app(migrated_database_url, now=first.occurred_at + timedelta(seconds=1))
    path = "/internal/v1/paper/economic-evidence"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=application), base_url="http://test"
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["protocol_status"] == "registered_not_activated"
        assert data["planned_slot_count"] == 24
        assert data["economic_pair_count"] is None
        assert data["economic_status"] == "not_evaluated"
        assert data["execution_locked"] is True
        protocol = data["protocol"]
        assert len(protocol["slots"]) == 24
        assert protocol["daily_model_jobs"] == 6
        assert protocol["daily_sdk_calls"] == 14
        assert "FIXED_STRATEGY_BASELINE_UNIMPLEMENTED" in protocol["activation_blockers"]
        for forbidden in (
            "instrument_id",
            "symbol",
            "research_cost_usd",
            "cash_usd",
            "account_id",
            "prompt",
        ):
            assert forbidden not in response.text
        assert (await client.post(path)).status_code == 405
    import os

    if os.environ.get("NEXUS_CONTROLLED_BROWSER") == "1":
        from helpers.controlled_browser import verify_controlled_browser

        await verify_controlled_browser(
            migrated_database_url,
            api_now=first.occurred_at + timedelta(seconds=1),
            cycle_id=None,
            order_ids=(),
            economic_protocol_hash=protocol["protocol_hash"],
        )


@pytest.mark.asyncio
async def test_actual_database_guard_prevents_misnamed_target(migrated_database_url):
    factory = session_factory(settings(migrated_database_url))
    async with factory.begin() as session:
        before = await session.scalar(select(func.count()).select_from(AuditEvent))
        with pytest.raises(ValueError, match="ECONOMIC_PROTOCOL_DATABASE_MISMATCH"):
            await register_protocol(
                session, protocol_request(), expected_database="nexus_test_" + "0" * 32
            )
        assert before == await session.scalar(select(func.count()).select_from(AuditEvent))


@pytest.mark.asyncio
async def test_concurrent_registration_is_one_fact(migrated_database_url):
    factory = session_factory(settings(migrated_database_url))

    async def register():
        async with factory.begin() as session:
            actual = await session.scalar(text("SELECT current_database()"))
            return (
                await register_protocol(session, protocol_request(), expected_database=actual)
            ).id

    assert len(set(await asyncio.gather(register(), register()))) == 1


@pytest.mark.asyncio
async def test_corrupted_protocol_is_unknown_not_missing(migrated_database_url):
    from nexus.audit.service import append_audit_event
    from nexus.paper.portfolio_economic_protocol import PROTOCOL_ACTION, PROTOCOL_SCHEMA

    factory = session_factory(settings(migrated_database_url))
    async with factory.begin() as session:
        now = await session.scalar(select(func.clock_timestamp()))
        await append_audit_event(
            session,
            actor_id="local-economic-protocol-cli",
            action=PROTOCOL_ACTION,
            schema_version=PROTOCOL_SCHEMA,
            payload={"broken": True},
            occurred_at=now,
        )
    application = app(migrated_database_url, now=now + timedelta(seconds=1))
    path = "/internal/v1/paper/economic-evidence"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=application), base_url="http://test"
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
        assert response.status_code == 200
        assert response.json()["data"]["protocol_status"] == "unavailable"
        assert response.json()["data"]["planned_slot_count"] is None


@pytest.mark.asyncio
async def test_business_cycle_in_an_isolated_name_still_prevents_registration(
    migrated_database_url,
):
    from integration.test_portfolio_proposal_pg import proposal_source

    engine, factory, _ = await proposal_source(migrated_database_url)
    try:
        async with factory.begin() as session:
            actual = await session.scalar(text("SELECT current_database()"))
            before = await session.scalar(select(func.count()).select_from(AuditEvent))
            with pytest.raises(ValueError, match="ECONOMIC_PROTOCOL_BUSINESS_STATE_FORBIDDEN"):
                await register_protocol(session, protocol_request(), expected_database=actual)
            assert before == await session.scalar(select(func.count()).select_from(AuditEvent))
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_registration_does_not_become_visible_before_its_real_clock(migrated_database_url):
    from nexus.paper.portfolio_economic_protocol import read_protocol

    factory = session_factory(settings(migrated_database_url))
    async with factory.begin() as session:
        actual = await session.scalar(text("SELECT current_database()"))
        event = await register_protocol(session, protocol_request(), expected_database=actual)
    async with factory.begin() as session:
        await session.execute(text("SET TRANSACTION READ ONLY"))
        assert (await read_protocol(session, as_of=event.occurred_at - timedelta(seconds=1)))[
            0
        ] == "not_implemented"
        status, read_event, plan = await read_protocol(
            session, as_of=event.occurred_at + timedelta(seconds=1)
        )
        assert status == "registered_not_activated"
        assert read_event.id == event.id and plan.content_hash == event.payload["content_hash"]
