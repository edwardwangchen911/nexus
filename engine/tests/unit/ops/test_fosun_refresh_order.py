"""New market evidence must be committed before the broker observation cutoff."""

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from nexus.ops import fosun_local_refresh as module
from nexus.providers.broker.gateway_contracts import GatewayResponsePage


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", (None, "identity_preflight", "market", "broker", "identity_source")
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

    async def identity(*_args, **_kwargs):
        if failure == "identity_preflight":
            raise ValueError("LOCAL_BROKER_INSTRUMENT_SOURCE_REQUIRED")
        return SimpleNamespace(
            account_id=uuid4(),
            portfolio_id=uuid4(),
            sync_run_id=uuid4(),
            instrument_ids={"usAAPL": instrument_id},
            identity_date=now.date(),
            previous_cash_flow_watermark=None,
        )

    async def market_reads(*_args, **_kwargs):
        state["market_read"] = True
        return SimpleNamespace(
            codes=("usAAPL",), request_counts={"quotes": 1, "bars": 1, "total": 2}
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
    monkeypatch.setattr(module, "_load_local_broker_identity", identity)
    monkeypatch.setattr(module, "_read_market_snapshot", market_reads)
    monkeypatch.setattr(module, "_persist_market_snapshot", persist_market, raising=False)
    monkeypatch.setattr(module, "_read_broker_facts", broker_reads)
    monkeypatch.setattr(module, "_persist_ready_broker_snapshot", persist_broker)
    monkeypatch.setattr(module, "_diagnose_local_snapshot", diagnose)
    monkeypatch.setattr(
        module, "_broker_request_counts", lambda _: {"cash": 2, "holdings": 2, "orders": 2}
    )
    if failure in {"identity_preflight", "identity_source"}:
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
    assert state["market_read"] is (failure != "identity_preflight")
    assert state["market"] is (failure not in {"identity_preflight", "market"})
    assert state["broker"] is (failure is None)
    assert state["broker_read"] is (failure not in {"identity_preflight", "market"})
    assert state["closed"] == {"broker", "market", "engine"}
