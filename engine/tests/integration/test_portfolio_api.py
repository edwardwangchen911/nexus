import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from urllib.parse import urlencode
from uuid import UUID

import httpx
import pytest
from helpers.hmac import TEST_HMAC_KEY_ID, TEST_HMAC_SECRET, build_test_hmac_headers
from nexus.main import create_engine_app
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.portfolio import (
    Account,
    BrokerOrderSnapshot,
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
_ORDER_ID = UUID("0198d500-1000-7000-8000-000000000002")
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
        session.add(
            InstrumentAlias(
                instrument_id=_INSTRUMENT_ID,
                symbol="00656.HK",
                alias_type="ticker",
                valid_from=date(2020, 1, 1),
                valid_to=None,
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
                session.add_all(
                    (
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
                    ),
                    BrokerOrderSnapshot(
                        id=_ORDER_ID,
                        portfolio_snapshot_id=snapshot.id,
                        provider="fosun",
                        provider_order_id="provider-order-secret-canary",
                        instrument_id=_INSTRUMENT_ID,
                        side="sell",
                        quantity=Decimal("1000"),
                        filled_quantity=Decimal("0"),
                        price=Decimal("6.5"),
                        status="submitted",
                        updated_at=None,
                    ),
                    )
                )


async def _add_snapshot_state(
    database_url: str,
    status: str,
    *,
    as_of: datetime | None,
) -> UUID:
    factory = session_factory(_settings(database_url))
    snapshot_id = UUID("0198d500-1000-7000-8000-000000000099")
    async with factory() as session, session.begin():
        stale_source_id = None
        if status == "STALE":
            prior_run = BrokerSyncRun(
                id=UUID("0198d500-1000-7000-8000-000000000097"),
                account_id=_ACCOUNTS[1], portfolio_id=_PORTFOLIOS[1], trigger_kind="AUTOMATIC",
                actor_id="prior", route="broker-sync", idempotency_key="prior",
                request_hash="7" * 64, status="READY", accepted_at=_CLOCK,
                completed_at=_CLOCK, attempt_count=1, failure_reason=None,
                cash_flow_quality="NOT_RUN",
            )
            stale_source_id = UUID("0198d500-1000-7000-8000-000000000098")
            session.add(prior_run)
            session.add(_snapshot_record(stale_source_id, prior_run.id, "READY", _CLOCK, None))
            await session.flush()
        run = BrokerSyncRun(
            id=UUID("0198d500-1000-7000-8000-000000000096"),
            account_id=_ACCOUNTS[1], portfolio_id=_PORTFOLIOS[1], trigger_kind="AUTOMATIC",
            actor_id="state", route="broker-sync", idempotency_key="state",
            request_hash="8" * 64, status=status, accepted_at=_CLOCK,
            completed_at=_CLOCK, attempt_count=1, failure_reason=None,
            cash_flow_quality="NOT_RUN",
        )
        session.add(run)
        session.add(_snapshot_record(snapshot_id, run.id, status, as_of, stale_source_id))
    return snapshot_id


def _snapshot_record(
    snapshot_id: UUID,
    run_id: UUID,
    status: str,
    as_of: datetime | None,
    stale_source_id: UUID | None,
) -> PortfolioSnapshotRecord:
    return PortfolioSnapshotRecord(
        id=snapshot_id, portfolio_id=_PORTFOLIOS[1], sync_run_id=run_id,
        stale_from_snapshot_id=stale_source_id, status=status, as_of=as_of,
        position_hash_version="broker_position_hash_v1", position_hash="c" * 64,
        order_hash_version="broker_order_hash_v1", order_hash="d" * 64,
        source_request_ids=[], source_observations=[],
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


@pytest.mark.asyncio
async def test_latest_snapshot_preserves_currency_costs_and_read_only_ids(
    migrated_database_url: str,
) -> None:
    await _seed_portfolio_list(migrated_database_url)
    path = f"/internal/v1/portfolios/{_PORTFOLIOS[0]}/snapshots/latest"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(path, headers=_headers("GET", path))

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["coverage_scope"] == "broker_account"
    assert data["converted_view"] is None
    assert [bucket["currency"] for bucket in data["cash"]] == ["USD"]
    position = data["positions"][0]
    assert position["symbol"] == "00656.HK"
    assert position["currency"] == "HKD"
    assert position["avg_cost"] == "5.250000000000"
    assert position["diluted_cost"] == "4.950000000000"
    assert position["read_only"] is True
    order = data["open_orders"][0]
    assert order["broker_order_snapshot_id"] == str(_ORDER_ID)
    assert order["read_only"] is True
    assert "provider_order_id" not in order
    encoded = json.dumps(response.json())
    for canary in ("provider-order-secret-canary", "external-request-canary", "secret-canary"):
        assert canary not in encoded


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ("READY", "PARTIAL", "STALE", "RECONCILIATION_REQUIRED"))
async def test_latest_snapshot_exposes_every_state_with_nullable_as_of(
    migrated_database_url: str,
    status: str,
) -> None:
    await _seed_portfolio_list(migrated_database_url)
    snapshot_id = await _add_snapshot_state(migrated_database_url, status, as_of=None)
    path = f"/internal/v1/portfolios/{_PORTFOLIOS[1]}/snapshots/latest"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(path, headers=_headers("GET", path))

    assert response.status_code == 200
    assert response.json()["data"]["portfolio_snapshot_id"] == str(snapshot_id)
    assert response.json()["data"]["status"] == status
    assert response.json()["data"]["as_of"] is None


@pytest.mark.asyncio
async def test_latest_snapshot_requires_auth_and_returns_safe_not_found(
    migrated_database_url: str,
) -> None:
    await _seed_portfolio_list(migrated_database_url)
    missing_snapshot_path = f"/internal/v1/portfolios/{_PORTFOLIOS[1]}/snapshots/latest"
    unknown_path = "/internal/v1/portfolios/0198d500-1000-7000-8000-000000000099/snapshots/latest"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        unauthenticated = await client.get(missing_snapshot_path)
        missing = await client.get(
            missing_snapshot_path, headers=_headers("GET", missing_snapshot_path)
        )
        unknown = await client.get(unknown_path, headers=_headers("GET", unknown_path))

    assert unauthenticated.status_code == 401
    assert missing.status_code == unknown.status_code == 404
    assert missing.json()["error"]["code"] == "NOT_FOUND"
