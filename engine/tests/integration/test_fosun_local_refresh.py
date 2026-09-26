from datetime import UTC, date, datetime, timedelta
from uuid import UUID, uuid4

import httpx
import pytest
from helpers.broker_facts import position, read_set
from helpers.broker_read import ScriptedBrokerProvider
from helpers.diagnostic_artifacts import FixtureArtifactReader
from helpers.observed_gateway import NOW, ObservedGateway
from helpers.portfolio_api import app as portfolio_app
from helpers.portfolio_api import headers
from nexus.ops import fosun_local_refresh as refresh_module
from nexus.ops.fosun_local import read_local_status
from nexus.ops.fosun_local_refresh import (
    _diagnose_local_snapshot,
    _ensure_market_bindings,
    _insert_calendar,
    _persist_ready_broker_snapshot,
    _read_broker_facts,
)
from nexus.persistence.models.data import ProviderCapability
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.portfolio import (
    BrokerCashFlow,
    BrokerSyncRun,
    PortfolioSnapshotRecord,
)
from nexus.persistence.session import session_factory
from nexus.providers.broker.base import SourceObservation
from nexus.providers.broker.fosun_contracts import PositionItem
from nexus.reconciliation.broker_snapshot import freeze_broker_snapshot
from nexus.reconciliation.cash_flows import CashFlowSyncResult
from nexus.reconciliation.facts import canonical_position_hash
from nexus.settings import EngineSettings
from sqlalchemy import select


async def _seed_sourced_us_identity(session, instrument_id, symbol):
    session.add(
        Instrument(
            id=instrument_id,
            asset_type="common_stock",
            exchange="XNAS",
            currency="USD",
            tick_size=None,
            risk_bucket="EQUITY",
        )
    )
    await session.flush()
    session.add(
        InstrumentAlias(
            instrument_id=instrument_id,
            alias_type="ticker",
            symbol=symbol,
            valid_from=date(2020, 1, 1),
            valid_to=None,
        )
    )
    await session.flush()


@pytest.mark.asyncio
@pytest.mark.parametrize("order_code", ("usAAPL", "usMSFT"))
async def test_local_observed_refresh_reaches_database_and_snapshot_api(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
    order_code: str,
) -> None:
    account_id, portfolio_id, sync_run_id, instrument_id = (uuid4() for _ in range(4))
    broker = ObservedGateway(order_code=order_code)
    instrument_ids = {"usAAPL": instrument_id}
    frozen, cash_flows = await _read_broker_facts(
        broker,
        account_id=account_id,
        portfolio_id=portfolio_id,
        sync_run_id=sync_run_id,
        external_account_id="synthetic-account",
        instrument_ids=instrument_ids,
        cash_flow_end=NOW.date(),
        previous_cash_flow_watermark=None,
    )
    holding = PositionItem(
        kind="position",
        external_account_id="synthetic-account",
        instrument_code="usAAPL",
        currency="USD",
        quantity="1",
        available_quantity="1",
        frozen_quantity="0",
        average_cost="90",
        diluted_cost=None,
        current_price="100",
        provider_as_of=None,
    )
    factory = session_factory(_settings(migrated_database_url))
    try:
        async with factory() as session, session.begin():
            await _seed_sourced_us_identity(session, instrument_id, "AAPL")
            if order_code == "usMSFT":
                await _seed_sourced_us_identity(
                    session, frozen.portfolio_snapshot.open_orders[0].instrument_id, "MSFT"
                )
        async with factory() as session, session.begin():
            stored = await _persist_ready_broker_snapshot(
                session,
                account_id=account_id,
                portfolio_id=portfolio_id,
                frozen=frozen,
                holdings=(holding,),
                instrument_ids=instrument_ids,
                valid_from=NOW.date(),
                completed_at=NOW + timedelta(seconds=7),
                cash_flow_result=cash_flows,
            )
            assert {item["time_basis"] for item in stored.source_observations} == {
                "gateway_observed"
            }
        async with factory() as session:
            fact = await session.scalar(select(BrokerCashFlow))
            assert fact.occurred_at is None
            assert fact.provider_business_date == NOW.date()
            assert fact.kind == "fosun:100"
        diagnostic = await _diagnose_local_snapshot(
            factory,
            frozen=frozen,
            cash_flow_quality="PASS",
            artifact_reader=FixtureArtifactReader({}),
        )
        assert diagnostic["status"] == "persisted"
        assert diagnostic["optimization_need"] == "RESEARCH_REQUIRED"
        preliminary = await _diagnose_local_snapshot(
            factory,
            frozen=frozen,
            cash_flow_quality="NOT_RUN",
            artifact_reader=FixtureArtifactReader({}),
        )
        assert preliminary["status"] == "preview"
        assert preliminary["optimization_need"] == "RESEARCH_REQUIRED"
        assert preliminary["items"] > 0

        local_status = await read_local_status(migrated_database_url)
        price_status = local_status["price_evidence"]
        assert price_status["held_instruments"] == 1
        assert price_status["missing_series"] == 1
        assert price_status["stored_authority_records"] == 0
        assert price_status["authority_revalidated"] is False
        assert local_status["network_checked"] is False

        async def fail_diagnostic(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError("synthetic-secret-canary")

        monkeypatch.setattr(refresh_module, "diagnose_and_persist", fail_diagnostic)
        failed = await _diagnose_local_snapshot(
            factory,
            frozen=frozen,
            cash_flow_quality="PASS",
            artifact_reader=FixtureArtifactReader({}),
        )
        assert failed == {"status": "failed", "code": "LOCAL_DIAGNOSTIC_FAILED"}
    finally:
        await factory.kw["bind"].dispose()

    path = f"/internal/v1/portfolios/{portfolio_id}/snapshots/latest"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=portfolio_app(migrated_database_url, now=NOW)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
        diagnostic_path = f"/internal/v1/portfolios/{portfolio_id}/diagnostics/latest"
        diagnostic_response = await client.get(
            diagnostic_path,
            headers=headers("GET", diagnostic_path),
        )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["time_basis"] == "gateway_observed"
    assert data["as_of"] == "2026-09-02T00:00:06Z"
    assert data["status"] == "READY"
    assert data["decision_readiness"] == "DECISION_READY"
    assert data["cash_flow_quality"] == "PASS" and data["cash_flow_count"] == 1
    assert data["execution_locked"] is True
    assert data["cash"][0]["provider_as_of"] is None
    assert data["positions"][0]["provider_as_of"] is None
    assert data["open_orders"][0]["updated_at"] is None
    assert data["open_orders"][0]["symbol"] == order_code.removeprefix("us")
    assert len(data["positions"]) == 1
    assert data["positions"][0]["symbol"] == "AAPL"
    assert broker.calls == 7
    assert diagnostic_response.status_code == 200
    report = diagnostic_response.json()["data"]
    assert report["optimization_need"] == "RESEARCH_REQUIRED"
    cash_item = next(item for item in report["items"] if item["diagnostic_code"] == "CASH_RATIO")
    assert cash_item["metric_value"] is not None
    assert cash_item["evidence_refs"]
    assert any(item["diagnostic_code"] == "CONCENTRATION_TOP1" for item in report["items"])
    assert not any(item["diagnostic_code"].startswith("VOLATILITY") for item in report["items"])
    assert any(item["unknown_reason"] is not None for item in report["items"])


def _settings(database_url: str) -> EngineSettings:
    return EngineSettings(
        NEXUS_ENV="test",
        DATABASE_URL=database_url,
        NEXUS_ENGINE_HMAC_CURRENT_KEY_ID="test-key",
        NEXUS_ENGINE_HMAC_CURRENT_SECRET="test-secret",
    )


@pytest.mark.asyncio
async def test_local_market_bindings_are_reusable_across_successful_refreshes(
    migrated_database_url: str,
) -> None:
    factory = session_factory(_settings(migrated_database_url))
    first_at = datetime(2026, 9, 1, 4, 0, tzinfo=UTC)
    try:
        async with factory() as session, session.begin():
            first_quote, first_bars = await _ensure_market_bindings(
                session,
                authorized_at=first_at,
                delivery_mode="unknown",
            )
        async with factory() as session, session.begin():
            second_quote, second_bars = await _ensure_market_bindings(
                session,
                authorized_at=first_at + timedelta(minutes=1),
                delivery_mode="unknown",
            )
    finally:
        await factory.kw["bind"].dispose()

    assert second_quote.provider_capability_id == first_quote.provider_capability_id
    assert second_bars.provider_capability_id == first_bars.provider_capability_id
    assert second_quote.provider_status_id == first_quote.provider_status_id
    assert second_quote.provider_status_version == first_quote.provider_status_version + 1


@pytest.mark.asyncio
async def test_reported_delivery_appends_capability_version_without_relabeling_history(
    migrated_database_url: str,
) -> None:
    factory = session_factory(_settings(migrated_database_url))
    first_at = datetime.now(UTC) - timedelta(minutes=5)
    try:
        async with factory() as session, session.begin():
            first_quote, first_bars = await _ensure_market_bindings(
                session, authorized_at=first_at, delivery_mode="unknown"
            )
        async with factory() as session, session.begin():
            second_quote, second_bars = await _ensure_market_bindings(
                session,
                authorized_at=first_at + timedelta(minutes=1),
                delivery_mode="unknown",
                bars_delivery_mode="delayed",
            )
        async with factory() as session, session.begin():
            _, third_bars = await _ensure_market_bindings(
                session,
                authorized_at=first_at + timedelta(minutes=2),
                delivery_mode="unknown",
                bars_delivery_mode="delayed",
            )
            old = await session.get(ProviderCapability, first_bars.provider_capability_id)
            new = await session.get(ProviderCapability, second_bars.provider_capability_id)
            assert old.payload["delivery_mode"] == "unknown"
            assert new.payload["delivery_mode"] == "delayed"
            assert old.created_at < new.created_at
        assert first_quote.provider_capability_id == second_quote.provider_capability_id
        assert first_bars.provider_capability_id != second_bars.provider_capability_id
        assert second_bars.provider_capability_id == third_bars.provider_capability_id
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_local_calendar_authority_is_reused_for_the_same_completed_window(
    migrated_database_url: str,
) -> None:
    factory = session_factory(_settings(migrated_database_url))
    published_at = datetime(2026, 9, 1, 4, 0, tzinfo=UTC)
    rows = (
        (
            published_at.date(),
            published_at - timedelta(hours=10),
            published_at - timedelta(hours=3),
        ),
    )
    try:
        async with factory() as session, session.begin():
            first = await _insert_calendar(
                session,
                rows=rows,
                range_start=published_at.date(),
                range_end=published_at.date(),
                published_at=published_at,
            )
        async with factory() as session, session.begin():
            second = await _insert_calendar(
                session,
                rows=rows,
                range_start=published_at.date(),
                range_end=published_at.date(),
                published_at=published_at + timedelta(minutes=1),
            )
    finally:
        await factory.kw["bind"].dispose()

    assert second.id == first.id


@pytest.mark.asyncio
async def test_local_ready_broker_snapshot_is_finalized_atomically(
    migrated_database_url: str,
) -> None:
    account_id = UUID("0198d600-1000-7000-8000-000000000001")
    portfolio_id = UUID("0198d600-1000-7000-8000-000000000002")
    sync_run_id = UUID("0198d600-1000-7000-8000-000000000003")
    instrument_id = UUID("0198d600-1000-7000-8000-000000000004")
    instant = datetime(2026, 9, 1, 4, 0, tzinfo=UTC)
    stable = read_set(positions=(position(instrument_id),), orders=())
    frozen = await freeze_broker_snapshot(
        account_id,
        ScriptedBrokerProvider([(stable, stable)]),
        portfolio_id=portfolio_id,
        sync_run_id=sync_run_id,
        max_attempts=1,
    )
    holding = PositionItem(
        kind="position",
        external_account_id="redacted-test-account",
        instrument_code="usAAPL",
        currency="USD",
        quantity="10",
        available_quantity="10",
        frozen_quantity="0",
        average_cost="100",
        diluted_cost=None,
        current_price="120",
        provider_as_of=instant,
    )
    cash_flow_result = CashFlowSyncResult(
        facts=(),
        quality="PASS",
        pagination_complete=True,
        watermark_date=date(2026, 8, 31),
        source_observations=(
            SourceObservation(
                source_request_id="cash-flows-empty",
                response_as_of=instant,
                quality="PASS",
            ),
        ),
    )
    factory = session_factory(_settings(migrated_database_url))
    try:
        async with factory() as session, session.begin():
            await _seed_sourced_us_identity(session, instrument_id, "AAPL")
        async with factory() as session, session.begin():
            stored = await _persist_ready_broker_snapshot(
                session,
                account_id=account_id,
                portfolio_id=portfolio_id,
                frozen=frozen,
                holdings=(holding,),
                instrument_ids={"usAAPL": instrument_id},
                valid_from=date(2026, 8, 31),
                completed_at=instant,
                cash_flow_result=cash_flow_result,
            )
        async with factory() as session:
            run = await session.get(BrokerSyncRun, sync_run_id)
            snapshot = await session.scalar(
                select(PortfolioSnapshotRecord).where(PortfolioSnapshotRecord.id == stored.id)
            )
    finally:
        await factory.kw["bind"].dispose()

    assert run is not None and run.status == "READY"
    assert run.trigger_kind == "MANUAL"
    assert run.attempt_count == 1
    assert run.cash_flow_quality == "PASS"
    assert snapshot is not None and snapshot.status == "READY"
    assert snapshot.position_hash == canonical_position_hash(frozen.portfolio_snapshot.positions)


@pytest.mark.asyncio
async def test_refresh_calendar_supports_service_after_friday_close(
    migrated_database_url: str,
) -> None:
    from nexus.paper.agent_cycle_authority import select_agent_paper_service_calendar_authority

    now = datetime(2026, 9, 26, 3, 40, tzinfo=UTC)
    start, market_end, rows = refresh_module._daily_window(now)
    factory = session_factory(_settings(migrated_database_url))
    try:
        async with factory() as session, session.begin():
            authority = await _insert_calendar(
                session,
                rows=rows,
                range_start=start.date(),
                range_end=rows[-1][0],
                published_at=now - timedelta(seconds=1),
            )
        async with factory() as session:
            selected = await select_agent_paper_service_calendar_authority(session, now=now)
        assert selected.calendar_authority_id == authority.id
        assert selected.session == date(2026, 9, 28)
        assert selected.next_session == date(2026, 9, 29)
        assert market_end.date() == date(2026, 9, 25)
    finally:
        await factory.kw["bind"].dispose()
