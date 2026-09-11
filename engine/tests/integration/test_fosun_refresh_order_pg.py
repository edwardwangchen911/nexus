"""The normal local refresh publishes market facts before freezing broker time."""

import hashlib
import json
from collections import Counter
from datetime import UTC, date, datetime
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from helpers.portfolio_api import settings
from nexus.diagnostics.assembler import load_diagnostic_source_policy
from nexus.diagnostics.series_authority import select_authoritative_series
from nexus.domain.ids import new_uuid7
from nexus.ops import fosun_local_refresh as module
from nexus.persistence.models.data import DataArtifact, PriceBarSetAuthority, PriceSeriesManifest
from nexus.persistence.models.diagnostics import PortfolioDiagnosticItemRecord
from nexus.persistence.models.market import Instrument, InstrumentAlias, LatestQuote
from nexus.persistence.models.portfolio import BrokerSyncRun, PortfolioSnapshotRecord
from nexus.persistence.session import session_factory
from nexus.providers.broker.gateway_contracts import GatewayResponsePage
from nexus.providers.market.fosun_gateway import RawGatewayResponse
from sqlalchemy import func, select


@pytest.mark.asyncio
@pytest.mark.parametrize("broker_failure", (False, True))
async def test_broker_only_refresh_uses_real_reconciliation_and_persistence_without_market(
    migrated_database_url,
    monkeypatch,
    broker_failure,
):
    checked_at = datetime(2026, 9, 10, 20, tzinfo=UTC)
    instrument_id = uuid4()
    factory = session_factory(settings(migrated_database_url))
    calls = Counter()
    position = {
        "kind": "position",
        "external_account_id": "synthetic",
        "instrument_code": "usALPHA",
        "currency": "USD",
        "quantity": "1",
        "available_quantity": "1",
        "frozen_quantity": "0",
        "average_cost": "100",
        "diluted_cost": None,
        "current_price": "100",
        "provider_as_of": None,
    }

    def page(items):
        return GatewayResponsePage(
            items=tuple(items),
            source_request_id=str(uuid4()),
            has_more=False,
            next_cursor=None,
            as_of=datetime.now(UTC),
            quality="PASS",
        )

    class Broker:
        async def read_accounts(self):
            calls["accounts"] += 1
            return page(
                [
                    {
                        "kind": "account",
                        "external_account_id": "synthetic",
                        "status": "ACTIVE",
                        "base_currency": "USD",
                    }
                ]
            )

        async def read_holdings(self, *_args, **_kwargs):
            calls["holdings"] += 1
            return page([position])

        async def read_cash(self, *_args, **_kwargs):
            calls["cash"] += 1
            if broker_failure:
                raise RuntimeError("synthetic broker failure")
            return page(
                [
                    {
                        "kind": "cash",
                        "external_account_id": "synthetic",
                        "currency": "USD",
                        "cash_balance": "1000",
                        "available_cash": "1000",
                        "provider_as_of": None,
                    }
                ]
            )

        async def read_orders(self, *_args, **_kwargs):
            calls["orders"] += 1
            return page([])

        async def read_cash_flows(self, *_args, **kwargs):
            calls["cash_flows"] += 1
            assert kwargs["date_to"] == date(2026, 9, 11)
            return page([])

        async def aclose(self):
            return None

    class Store:
        async def read_normalized(self, _uri):
            raise OSError("synthetic evidence unavailable")

    async with factory() as session, session.begin():
        session.add(
            Instrument(
                id=instrument_id,
                asset_type="equity",
                exchange="XNYS",
                currency="USD",
                tick_size=None,
                risk_bucket="EQUITY",
            )
        )
        session.add(
            InstrumentAlias(
                instrument_id=instrument_id,
                symbol="ALPHA",
                alias_type="ticker",
                valid_from=date(2026, 1, 1),
                valid_to=None,
            )
        )

    def market_must_not_be_constructed(*_args, **_kwargs):
        raise AssertionError("broker-only refresh constructed the market gateway")

    monkeypatch.setattr(module, "_broker_settings", lambda _: None)
    monkeypatch.setattr(module, "FosunGatewayClient", lambda _: Broker())
    monkeypatch.setattr(module, "FosunMarketGatewayClient", market_must_not_be_constructed)
    monkeypatch.setattr(module, "S3ProviderEvidenceStore", lambda _: Store())
    configured = SimpleNamespace(database_url=migrated_database_url)

    try:
        if broker_failure:
            with pytest.raises(RuntimeError, match="synthetic broker failure"):
                await module.refresh_local_fosun_broker(configured, now=checked_at)
        else:
            result = await module.refresh_local_fosun_broker(configured, now=checked_at)
            assert result["snapshot_status"] == "READY"
            assert result["mode"] == "broker_only"
            assert result["provider_requests"] == {
                "accounts": 1,
                "cash": 2,
                "holdings": 3,
                "orders": 2,
                "cash_flows": 1,
                "quotes": 0,
                "bars": 0,
                "total": 9,
            }
        async with factory() as session:
            snapshots = await session.scalar(
                select(func.count()).select_from(PortfolioSnapshotRecord)
            )
            runs = tuple(await session.scalars(select(BrokerSyncRun)))
            quotes = await session.scalar(select(func.count()).select_from(LatestQuote))
            assert snapshots == (0 if broker_failure else 1)
            assert len(runs) == (0 if broker_failure else 1)
            if runs:
                assert runs[0].route == "local-fosun-broker-refresh"
                assert runs[0].status == "READY"
            assert quotes == 0
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("broker_failure", (False, True))
async def test_normal_refresh_commits_current_raw_evidence_before_snapshot_cutoff(
    migrated_database_url,
    monkeypatch,
    broker_failure,
):
    discovery_at = datetime.now(UTC)
    _, last_session, calendar = module._daily_window(discovery_at)
    position = {
        "kind": "position",
        "external_account_id": "synthetic",
        "instrument_code": "usALPHA",
        "currency": "USD",
        "quantity": "1",
        "available_quantity": "1",
        "frozen_quantity": "0",
        "average_cost": "100",
        "diluted_cost": None,
        "current_price": "100",
        "provider_as_of": None,
    }
    factory = session_factory(settings(migrated_database_url))
    calls = Counter()

    def page(items):
        return GatewayResponsePage(
            items=tuple(items),
            source_request_id=str(uuid4()),
            has_more=False,
            next_cursor=None,
            as_of=datetime.now(UTC),
            quality="PASS",
        )

    class Store:
        def __init__(self):
            self.objects = {}

        async def archive_raw(self, key, content):
            uri = f"memory://{key}"
            self.objects[uri] = content
            return uri

        put_normalized = archive_raw

        async def read_normalized(self, uri):
            return self.objects[uri]

    store = Store()

    class Broker:
        async def read_accounts(self):
            calls["accounts"] += 1
            return page(
                [
                    {
                        "kind": "account",
                        "external_account_id": "synthetic",
                        "status": "ACTIVE",
                        "base_currency": "USD",
                    }
                ]
            )

        async def read_holdings(self, *_args, **_kwargs):
            calls["holdings"] += 1
            return page([position])

        async def read_cash(self, *_args, **_kwargs):
            calls["cash"] += 1
            # This separate transaction proves publication, not just pending INSERT order.
            async with factory() as session:
                assert (
                    await session.scalar(select(func.count()).select_from(PriceBarSetAuthority))
                    == 1
                )
                assert (
                    await session.scalar(select(func.count()).select_from(PortfolioSnapshotRecord))
                    == 0
                )
            if broker_failure:
                raise RuntimeError("synthetic broker failure")
            return page(
                [
                    {
                        "kind": "cash",
                        "external_account_id": "synthetic",
                        "currency": "USD",
                        "cash_balance": "1000",
                        "available_cash": "1000",
                        "provider_as_of": None,
                    }
                ]
            )

        async def read_orders(self, *_args, **_kwargs):
            calls["orders"] += 1
            return page([])

        async def read_cash_flows(self, *_args, **_kwargs):
            calls["cash_flows"] += 1
            return page([])

        async def aclose(self):
            pass

    def raw(operation, items):
        now = datetime.now(UTC)
        if operation == "fosun_get_quotes":
            for item in items:
                item["as_of"] = now.isoformat()
        data = {
            "items": items,
            "source_request_id": str(uuid4()),
            "has_more": False,
            "next_cursor": None,
            "as_of": now.isoformat(),
            "quality": "PARTIAL",
        }
        if operation == "fosun_get_bars":
            data.update(sdk_requested_adjustment="noward", provider_reported_delay=True)
        content = json.dumps(data).encode()
        return RawGatewayResponse(
            operation=operation,
            gateway_request_id=new_uuid7(),
            request_body_sha256=hashlib.sha256(b"synthetic").hexdigest(),
            raw_payload=content,
            sent_at=now,
            received_at=now,
        )

    class Market:
        async def read_quotes_raw(self, codes):
            calls["quotes"] += 1
            return raw(
                "fosun_get_quotes",
                [
                    {
                        "kind": "quote",
                        "instrument_code": codes[0],
                        "price": "100",
                        "as_of": datetime.now(UTC).isoformat(),
                    }
                ],
            )

        async def read_bars_raw(self, *, code, **_kwargs):
            calls["bars"] += 1
            return raw(
                "fosun_get_bars",
                [
                    {
                        "kind": "bar",
                        "instrument_code": code,
                        "interval": "day",
                        "timestamp": f"{day.isoformat()}T00:00:00Z",
                        "open": "100",
                        "high": "101",
                        "low": "99",
                        "close": "100",
                        "volume": "1000",
                    }
                    for day, _, _ in calendar
                    if day <= last_session.date()
                ],
            )

        async def aclose(self):
            pass

    monkeypatch.setattr(module, "_broker_settings", lambda _: None)
    monkeypatch.setattr(module, "_market_identity", lambda _: None)
    monkeypatch.setattr(module, "FosunGatewayClient", lambda _: Broker())
    monkeypatch.setattr(module, "FosunMarketGatewayClient", lambda _: Market())
    monkeypatch.setattr(module, "S3ProviderEvidenceStore", lambda _: store)
    configured = SimpleNamespace(database_url=migrated_database_url, market_delivery_mode="unknown")
    try:
        if broker_failure:
            with pytest.raises((ValueError, RuntimeError)):
                await module.refresh_local_fosun(configured)
        else:
            result = await module.refresh_local_fosun(configured)
            assert result["snapshot_status"] == "READY"
            assert result["diagnostic"]["status"] == "persisted"
            assert result["provider_requests"]["total"] == 11
            counts = result["provider_requests"]
            assert counts["total"] == sum(value for key, value in counts.items() if key != "total")
            assert counts == {**calls, "total": sum(calls.values())}
        async with factory() as session:
            authority = await session.scalar(select(PriceBarSetAuthority))
            assert authority is not None
            manifest = await session.get(PriceSeriesManifest, authority.price_series_manifest_id)
            artifact = await session.get(DataArtifact, manifest.data_artifact_id)
            snapshot = await session.scalar(select(PortfolioSnapshotRecord))
            if broker_failure:
                assert snapshot is None
            else:
                assert snapshot.as_of > max(
                    manifest.created_at,
                    manifest.provider_authorized_at,
                    artifact.created_at,
                    authority.available_at,
                )
                policy = await load_diagnostic_source_policy(session, snapshot.as_of)
                selected = await select_authoritative_series(
                    session,
                    UUID(manifest.instrument),
                    snapshot.as_of,
                    store.read_normalized,
                    policy,
                )
                assert selected.selected is not None
                assert selected.selected[0].adjustment == "raw"
                previous = await select_authoritative_series(
                    session, UUID(manifest.instrument), discovery_at, store.read_normalized, policy
                )
                assert previous.selected is None
                liquidity = await session.scalar(
                    select(PortfolioDiagnosticItemRecord).where(
                        PortfolioDiagnosticItemRecord.diagnostic_code.like(
                            "LIQUIDITY_DAYS_TO_EXIT:%"
                        )
                    )
                )
                assert liquidity is not None
                assert liquidity.metric_value is None
                assert liquidity.unknown_reason == "ADV20_VOLUME_UNIT_UNVERIFIED"
    finally:
        await factory.kw["bind"].dispose()
