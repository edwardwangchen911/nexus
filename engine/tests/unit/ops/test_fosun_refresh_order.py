"""New market evidence must be committed before the broker observation cutoff."""

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from nexus.ops import fosun_local_refresh as module
from nexus.providers.broker.gateway_contracts import GatewayResponsePage


@pytest.mark.asyncio
async def test_unknown_holding_commits_known_market_then_returns_identity_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(UTC)
    known_id = uuid4()
    state = {
        "quote_read": False,
        "bars_read": False,
        "known_market_committed": False,
        "partial_sync": False,
        "broker_read": False,
        "closed": set(),
    }

    def page(items):
        return GatewayResponsePage(
            items=tuple(items),
            source_request_id="synthetic",
            has_more=False,
            next_cursor=None,
            as_of=now,
            quality="PASS",
        )

    def holding(code: str) -> dict[str, object]:
        return {
            "kind": "position",
            "external_account_id": "synthetic",
            "instrument_code": code,
            "currency": "USD",
            "quantity": "1",
            "available_quantity": "1",
            "frozen_quantity": "0",
            "average_cost": "1",
            "diluted_cost": None,
            "current_price": "1",
            "provider_as_of": None,
            "provider_name": code,
            "provider_symbol": code,
            "provider_product_type": 6,
        }

    class Broker:
        async def read_accounts(self):
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
            return page([holding("usKNOWN"), holding("usNEW")])

        async def aclose(self):
            state["closed"].add("broker")

    class Market:
        async def aclose(self):
            state["closed"].add("market")

    class Session:
        pending = None

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        @asynccontextmanager
        async def begin(self):
            self.pending = None
            yield
            if self.pending:
                state[self.pending] = True

    class Engine:
        async def dispose(self):
            state["closed"].add("engine")

    async def quote_read(*_args, **_kwargs):
        state["quote_read"] = True
        return SimpleNamespace(received_at=now)

    async def identity(*_args, **kwargs):
        assert kwargs["allow_unresolved"] is True
        assert state["quote_read"]
        return SimpleNamespace(
            account_id=uuid4(),
            portfolio_id=uuid4(),
            sync_run_id=uuid4(),
            instrument_ids={"usKNOWN": known_id},
            unresolved_codes=("usNEW",),
            identity_date=now.date(),
            previous_cash_flow_watermark=None,
        )

    async def market_reads(*_args, **kwargs):
        assert kwargs["codes"] == ("usKNOWN",)
        assert kwargs["quote_codes"] == ("usKNOWN", "usNEW")
        assert kwargs["quote_raw"].received_at == now
        state["bars_read"] = True
        return SimpleNamespace(
            codes=("usKNOWN", "usNEW"),
            resolved_codes=("usKNOWN",),
            quote_raw=kwargs["quote_raw"],
            bars_raw=(),
            request_counts={"quotes": 1, "bars": 1, "total": 2},
        )

    async def persist_market(session, *_args, **_kwargs):
        assert state["bars_read"] and not state["broker_read"]
        session.pending = "known_market_committed"
        return {
            "known_market_symbols": 1,
            "pending_instrument_count": 1,
            "pending_route_counts": {
                "current_held_common_stock": 1,
                "current_held_fund_or_structured": 0,
                "manual_review": 0,
            },
            "quotes": 2,
            "quote_success": 2,
            "quote_missing": 0,
            "bar_series": 1,
            "bars": 20,
            "market_as_of": now.isoformat(),
        }

    async def persist_partial(session, *_args, **_kwargs):
        assert state["known_market_committed"]
        session.pending = "partial_sync"

    async def broker_reads(*_args, **_kwargs):
        state["broker_read"] = True
        raise AssertionError("pending identity reached broker reconciliation")

    monkeypatch.setattr(module, "_broker_settings", lambda _: None)
    monkeypatch.setattr(module, "_market_identity", lambda _: None)
    monkeypatch.setattr(module, "FosunGatewayClient", lambda _: Broker())
    monkeypatch.setattr(module, "FosunMarketGatewayClient", lambda _: Market())
    monkeypatch.setattr(module, "create_async_engine", lambda *_a, **_k: Engine())
    monkeypatch.setattr(module, "async_sessionmaker", lambda *_a, **_k: Session)
    monkeypatch.setattr(module, "S3ProviderEvidenceStore", lambda _: object())
    monkeypatch.setattr(module, "_read_quote_snapshot", quote_read, raising=False)
    monkeypatch.setattr(module, "_load_local_broker_identity", identity)
    monkeypatch.setattr(module, "_read_market_snapshot", market_reads)
    monkeypatch.setattr(module, "_persist_market_snapshot", persist_market)
    monkeypatch.setattr(module, "_persist_identity_pending_sync", persist_partial, raising=False)
    monkeypatch.setattr(module, "_read_broker_facts", broker_reads)

    result = await module.refresh_local_fosun(SimpleNamespace(database_url="synthetic"))

    assert result == {
        "known_market_symbols": 1,
        "pending_instrument_count": 1,
        "pending_route_counts": {
            "current_held_common_stock": 1,
            "current_held_fund_or_structured": 0,
            "manual_review": 0,
        },
        "quotes": 2,
        "quote_success": 2,
        "quote_missing": 0,
        "bar_series": 1,
        "bars": 20,
        "market_as_of": now.isoformat(),
        "status": "identity_pending",
        "source": "fosun",
        "snapshot_status": "PARTIAL",
        "execution_locked": True,
        "portfolio_snapshot_updated": False,
        "provider_requests": {
            "accounts": 1,
            "holdings": 1,
            "quotes": 1,
            "bars": 1,
            "cash": 0,
            "orders": 0,
            "cash_flows": 0,
            "total": 4,
        },
    }
    assert state == {
        "quote_read": True,
        "bars_read": True,
        "known_market_committed": True,
        "partial_sync": True,
        "broker_read": False,
        "closed": {"broker", "market", "engine"},
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", (None, "identity_invalid", "market", "broker", "identity_source")
)
async def test_refresh_commits_market_before_broker_freeze_without_promoting_failed_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    failure: str | None,
) -> None:
    now = datetime.now(UTC)
    state = {
        "market": False,
        "market_read": False,
        "broker": False,
        "broker_read": False,
        "closed": set(),
    }
    instrument_id = uuid4()
    holding = {
        "kind": "position",
        "external_account_id": "synthetic",
        "instrument_code": "usAAPL",
        "currency": "USD",
        "quantity": "1",
        "available_quantity": "1",
        "frozen_quantity": "0",
        "average_cost": "1",
        "diluted_cost": None,
        "current_price": "1",
        "provider_as_of": None,
    }

    def page(items):
        return GatewayResponsePage(
            items=tuple(items),
            source_request_id="synthetic",
            has_more=False,
            next_cursor=None,
            as_of=now,
            quality="PASS",
        )

    class Broker:
        async def read_accounts(self):
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
            return page([holding])

        async def aclose(self):
            state["closed"].add("broker")

    class Market:
        async def aclose(self):
            state["closed"].add("market")

    class Session:
        pending = None

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        @asynccontextmanager
        async def begin(self):
            self.pending = None
            yield
            if self.pending:
                state[self.pending] = True

    class Store:
        read_normalized = None

    class Engine:
        async def dispose(self):
            state["closed"].add("engine")

    async def quote_read(*_args, **_kwargs):
        return SimpleNamespace(received_at=now)

    async def identity(*_args, **kwargs):
        assert kwargs["allow_unresolved"] is True
        if failure == "identity_invalid":
            raise ValueError("LOCAL_BROKER_INSTRUMENT_INVALID")
        return SimpleNamespace(
            account_id=uuid4(),
            portfolio_id=uuid4(),
            sync_run_id=uuid4(),
            instrument_ids={"usAAPL": instrument_id},
            unresolved_codes=(),
            identity_date=now.date(),
            previous_cash_flow_watermark=None,
        )

    async def market_reads(*_args, **kwargs):
        assert kwargs["codes"] == kwargs["quote_codes"] == ("usAAPL",)
        assert kwargs["quote_raw"].received_at == now
        state["market_read"] = True
        return SimpleNamespace(
            codes=("usAAPL",),
            resolved_codes=("usAAPL",),
            request_counts={"quotes": 1, "bars": 1, "total": 2},
        )

    async def persist_market(session, *_args, **_kwargs):
        assert not state["broker_read"]
        if failure == "market":
            raise ValueError("market persistence failed")
        session.pending = "market"
        return {
            "market_symbols": 1,
            "quotes": 1,
            "quote_success": 1,
            "quote_missing": 0,
            "bar_series": 1,
            "bars": 20,
            "market_as_of": now.isoformat(),
        }

    async def broker_reads(*_args, **_kwargs):
        assert state["market"], "broker cutoff preceded market commit"
        state["broker_read"] = True
        if failure == "broker":
            raise ValueError("broker read failed")
        snapshot = SimpleNamespace(cash=(), positions=(), open_orders=(), as_of=datetime.now(UTC))
        return SimpleNamespace(portfolio_snapshot=snapshot), SimpleNamespace(
            quality="PASS", facts=()
        )

    async def persist_broker(session, **_kwargs):
        assert state["market"] and state["broker_read"]
        if failure == "identity_source":
            raise ValueError("LOCAL_BROKER_INSTRUMENT_SOURCE_REQUIRED")
        session.pending = "broker"
        return SimpleNamespace(status="READY", as_of=datetime.now(UTC))

    async def diagnose(*_args, **_kwargs):
        assert state["broker"]
        return {"status": "persisted"}

    monkeypatch.setattr(module, "_broker_settings", lambda _: None)
    monkeypatch.setattr(module, "_market_identity", lambda _: None)
    monkeypatch.setattr(module, "FosunGatewayClient", lambda _: Broker())
    monkeypatch.setattr(module, "FosunMarketGatewayClient", lambda _: Market())
    monkeypatch.setattr(module, "create_async_engine", lambda *_a, **_k: Engine())
    monkeypatch.setattr(module, "async_sessionmaker", lambda *_a, **_k: Session)
    monkeypatch.setattr(module, "S3ProviderEvidenceStore", lambda _: Store())
    monkeypatch.setattr(module, "_read_quote_snapshot", quote_read)
    monkeypatch.setattr(module, "_load_local_broker_identity", identity)
    monkeypatch.setattr(module, "_read_market_snapshot", market_reads)
    monkeypatch.setattr(module, "_persist_market_snapshot", persist_market, raising=False)
    monkeypatch.setattr(module, "_read_broker_facts", broker_reads)
    monkeypatch.setattr(module, "_persist_ready_broker_snapshot", persist_broker)
    monkeypatch.setattr(module, "_diagnose_local_snapshot", diagnose)
    monkeypatch.setattr(
        module, "_broker_request_counts", lambda _: {"cash": 2, "holdings": 2, "orders": 2}
    )
    if failure == "identity_invalid":
        with pytest.raises(ValueError, match="LOCAL_BROKER_INSTRUMENT_INVALID"):
            await module.refresh_local_fosun(SimpleNamespace(database_url="synthetic"))
    elif failure == "identity_source":
        with pytest.raises(ValueError, match="LOCAL_BROKER_INSTRUMENT_SOURCE_REQUIRED"):
            await module.refresh_local_fosun(SimpleNamespace(database_url="synthetic"))
    elif failure:
        with pytest.raises((ValueError, RuntimeError)):
            await module.refresh_local_fosun(SimpleNamespace(database_url="synthetic"))
    else:
        result = await module.refresh_local_fosun(SimpleNamespace(database_url="synthetic"))
        assert result["snapshot_status"] == "READY" and result["provider_requests"]["total"] == 11
        counts = result["provider_requests"]
        assert counts["total"] == sum(value for key, value in counts.items() if key != "total")
    assert state["market_read"] is (failure != "identity_invalid")
    assert state["market"] is (failure not in {"identity_invalid", "market"})
    assert state["broker"] is (failure is None)
    assert state["broker_read"] is (failure not in {"identity_invalid", "market"})
    assert state["closed"] == {"broker", "market", "engine"}


@pytest.mark.asyncio
async def test_broker_only_refresh_never_constructs_or_calls_market_gateway(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 9, 10, 20, tzinfo=UTC)
    state = {"broker_read": False, "broker_persisted": False, "closed": set()}
    instrument_id = uuid4()
    holding = {
        "kind": "position",
        "external_account_id": "synthetic",
        "instrument_code": "usAAPL",
        "currency": "USD",
        "quantity": "1",
        "available_quantity": "1",
        "frozen_quantity": "0",
        "average_cost": "1",
        "diluted_cost": None,
        "current_price": "1",
        "provider_as_of": None,
    }

    def page(items):
        return GatewayResponsePage(
            items=tuple(items),
            source_request_id="synthetic",
            has_more=False,
            next_cursor=None,
            as_of=now,
            quality="PASS",
        )

    class Broker:
        async def read_accounts(self):
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
            return page([holding])

        async def aclose(self):
            state["closed"].add("broker")

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        @asynccontextmanager
        async def begin(self):
            yield

    class Engine:
        async def dispose(self):
            state["closed"].add("engine")

    async def identity(*_args, **_kwargs):
        return SimpleNamespace(
            account_id=uuid4(),
            portfolio_id=uuid4(),
            sync_run_id=uuid4(),
            instrument_ids={"usAAPL": instrument_id},
            identity_date=now.date(),
            previous_cash_flow_watermark=None,
            unresolved_codes=(),
        )

    async def broker_reads(*_args, **_kwargs):
        assert _kwargs["cash_flow_end"].isoformat() == "2026-09-11"
        state["broker_read"] = True
        snapshot = SimpleNamespace(cash=(), positions=(), open_orders=(), as_of=now)
        return SimpleNamespace(status="READY", portfolio_snapshot=snapshot), SimpleNamespace(
            quality="PASS", facts=()
        )

    async def persist_broker(*_args, **_kwargs):
        assert state["broker_read"]
        assert _kwargs["route"] == "local-fosun-broker-refresh"
        state["broker_persisted"] = True
        return SimpleNamespace(status="READY", as_of=now)

    async def diagnose(*_args, **_kwargs):
        raise AssertionError("diagnosis started without an object-store reader")

    def market_must_not_be_constructed(*_args, **_kwargs):
        raise AssertionError("broker-only refresh constructed the market gateway")

    def object_store_unavailable(*_args, **_kwargs):
        assert state["broker_persisted"]
        raise ValueError("synthetic invalid object-store configuration")

    monkeypatch.setattr(module, "_broker_settings", lambda _: None)
    monkeypatch.setattr(module, "FosunGatewayClient", lambda _: Broker())
    monkeypatch.setattr(module, "FosunMarketGatewayClient", market_must_not_be_constructed)
    monkeypatch.setattr(module, "create_async_engine", lambda *_a, **_k: Engine())
    monkeypatch.setattr(module, "async_sessionmaker", lambda *_a, **_k: Session)
    monkeypatch.setattr(module, "S3ProviderEvidenceStore", object_store_unavailable)
    monkeypatch.setattr(module, "_load_local_broker_identity", identity)
    monkeypatch.setattr(module, "_read_broker_facts", broker_reads)
    monkeypatch.setattr(module, "_persist_ready_broker_snapshot", persist_broker)
    monkeypatch.setattr(module, "_diagnose_local_snapshot", diagnose)
    monkeypatch.setattr(
        module, "_broker_request_counts", lambda _: {"cash": 2, "holdings": 2, "orders": 2}
    )

    result = await module.refresh_local_fosun_broker(
        SimpleNamespace(database_url="synthetic"), now=now
    )

    assert result["snapshot_status"] == "READY"
    assert result["provider_requests"]["quotes"] == 0
    assert result["provider_requests"]["bars"] == 0
    assert result["diagnostic"] == {"status": "failed", "code": "LOCAL_DIAGNOSTIC_FAILED"}
    assert state == {
        "broker_read": True,
        "broker_persisted": True,
        "closed": {"broker", "engine"},
    }
