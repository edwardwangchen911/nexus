import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from urllib.parse import urlencode
from uuid import UUID

import httpx
import pytest
from helpers.hmac import TEST_HMAC_KEY_ID, TEST_HMAC_SECRET, build_test_hmac_headers
from nexus.main import create_engine_app
from nexus.persistence.models.market import Instrument
from nexus.persistence.models.portfolio import (
    Account,
    BrokerSyncRun,
    CashSnapshot,
    Portfolio,
    PortfolioSnapshotRecord,
    PositionSnapshot,
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from uuid6 import uuid7

_CLOCK = datetime(2026, 8, 24, 4, 0, tzinfo=UTC)
_INSTRUMENT_ID = UUID("0198d500-1000-7000-8000-000000000001")
_PORTFOLIOS = (
    UUID("0198d500-1000-7000-8000-000000000011"),
    UUID("0198d500-1000-7000-8000-000000000012"),
    UUID("0198d500-1000-7000-8000-000000000013"),
)
_ACCOUNTS = (
    UUID("0198d500-1000-7000-8000-000000000021"),
    UUID("0198d500-1000-7000-8000-000000000022"),
    UUID("0198d500-1000-7000-8000-000000000023"),
)


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
    return build_test_hmac_headers(method, path, body, nonce=str(uuid7()))


async def _seed_portfolio_list(database_url: str) -> None:
    factory = session_factory(_settings(database_url))
    async with factory() as session, session.begin():
        session.add(
            Instrument(
                id=_INSTRUMENT_ID,
                asset_type="equity",
                exchange="XHKG",
                currency="HKD",
                tick_size=Decimal("0.001"),
                risk_bucket="hk_equity",
            )
        )
        for account_id, portfolio_id, name in zip(
            _ACCOUNTS, _PORTFOLIOS, ("Alpha", "Alpha", "Beta"), strict=True
        ):
            session.add(Account(id=account_id, provider="fosun", status="ACTIVE"))
            session.add(Portfolio(id=portfolio_id, account_id=account_id, name=name))
        await session.flush()
        for index, (portfolio_id, account_id, status) in enumerate(
            ((_PORTFOLIOS[0], _ACCOUNTS[0], "READY"), (_PORTFOLIOS[2], _ACCOUNTS[2], "PARTIAL"))
        ):
            run = BrokerSyncRun(
                id=UUID(f"0198d500-1000-7000-8000-00000000003{index + 1}"),
                account_id=account_id,
                portfolio_id=portfolio_id,
                trigger_kind="AUTOMATIC",
                actor_id=f"synthetic-{index}",
                route="broker-sync",
                idempotency_key=f"synthetic-{index}",
                request_hash=str(index + 1) * 64,
                status=status,
                accepted_at=_CLOCK - timedelta(minutes=2),
                completed_at=_CLOCK - timedelta(minutes=1),
                attempt_count=1,
                failure_reason=None,
                cash_flow_quality="NOT_RUN",
            )
            snapshot = PortfolioSnapshotRecord(
                id=UUID(f"0198d500-1000-7000-8000-00000000004{index + 1}"),
                portfolio_id=portfolio_id,
                sync_run_id=run.id,
                stale_from_snapshot_id=None,
                status=status,
                as_of=_CLOCK - timedelta(minutes=1),
                position_hash_version="broker_position_hash_v1",
                position_hash="a" * 64,
                order_hash_version="broker_order_hash_v1",
                order_hash="b" * 64,
                source_request_ids=["external-request-canary"],
                source_observations=[{"provider": "secret-canary"}],
            )
            session.add_all((run, snapshot))
            await session.flush()
            session.add(
                CashSnapshot(
                    portfolio_snapshot_id=snapshot.id,
                    currency="USD" if index == 0 else "HKD",
                    cash_balance=Decimal("1000"),
                    available_cash=Decimal("900"),
                    provider_as_of=snapshot.as_of,
                )
            )
            if index == 0:
                session.add(
                    PositionSnapshot(
                        portfolio_snapshot_id=snapshot.id,
                        instrument_id=_INSTRUMENT_ID,
                        currency="HKD",
                        quantity=Decimal("90000"),
                        available_quantity=Decimal("90000"),
                        frozen_quantity=Decimal("0"),
                        avg_cost=Decimal("5.25"),
                        diluted_cost=Decimal("4.95"),
                        current_price=Decimal("6.125"),
                        provider_as_of=snapshot.as_of,
                    )
                )


@pytest.mark.asyncio
async def test_portfolio_list_requires_hmac_authentication(migrated_database_url: str) -> None:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.get("/internal/v1/portfolios")

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "UNAUTHENTICATED"


@pytest.mark.asyncio
async def test_portfolio_filters_and_limits_are_bounded(migrated_database_url: str) -> None:
    app = _app(migrated_database_url)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        for query in ("limit=0", "limit=51", "kind=unknown"):
            path = f"/internal/v1/portfolios?{query}"
            response = await client.get(path, headers=_headers("GET", path))
            assert response.status_code == 422
            assert response.json()["error"]["code"] == "VALIDATION_ERROR"
        for kind in ("internal_paper", "shadow"):
            path = f"/internal/v1/portfolios?kind={kind}"
            response = await client.get(path, headers=_headers("GET", path))
            assert response.status_code == 200
            assert response.json()["data"] == {"items": [], "next_cursor": None}


@pytest.mark.asyncio
async def test_portfolio_list_is_stable_paginated_and_redacted(
    migrated_database_url: str,
) -> None:
    await _seed_portfolio_list(migrated_database_url)
    app = _app(migrated_database_url)
    first_path = "/internal/v1/portfolios?kind=real_read_only&limit=2"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        first = await client.get(first_path, headers=_headers("GET", first_path))
        cursor = first.json()["data"]["next_cursor"]
        second_path = "/internal/v1/portfolios?" + urlencode(
            {"kind": "real_read_only", "limit": 2, "cursor": cursor}
        )
        second = await client.get(second_path, headers=_headers("GET", second_path))
        drift_path = "/internal/v1/portfolios?" + urlencode(
            {"kind": "shadow", "cursor": cursor}
        )
        drift = await client.get(drift_path, headers=_headers("GET", drift_path))
        bad_path = "/internal/v1/portfolios?cursor=not-a-cursor"
        bad = await client.get(bad_path, headers=_headers("GET", bad_path))

    assert first.status_code == second.status_code == 200
    assert [item["portfolio_id"] for item in first.json()["data"]["items"]] == [
        str(_PORTFOLIOS[0]),
        str(_PORTFOLIOS[1]),
    ]
    first_item = first.json()["data"]["items"][0]
    assert first_item["original_currencies"] == ["HKD", "USD"]
    assert first_item["latest_snapshot_status"] == "READY"
    assert first_item["last_sync_run_status"] == "READY"
    assert second.json()["data"]["items"][0]["portfolio_id"] == str(_PORTFOLIOS[2])
    assert drift.status_code == bad.status_code == 422
    encoded = json.dumps(first.json()) + json.dumps(second.json())
    assert "external-request-canary" not in encoded
    assert "secret-canary" not in encoded
    assert "account_id" not in encoded
