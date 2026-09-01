import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import nexus.ops.fosun_local_refresh as refresh_module
import pytest
from nexus.ops.fosun_local_refresh import (
    _daily_window,
    _freeze_broker_facts,
    _read_market_snapshot,
    _safe_failure_code,
    _SinglePageBrokerGateway,
    _symbol_and_exchange,
    _us_holding_codes,
    refresh_local_fosun,
)
from nexus.providers.broker.fosun_contracts import PositionItem
from nexus.providers.broker.gateway_contracts import GatewayClientError, GatewayResponsePage
from nexus.providers.market.fosun_gateway import RawGatewayResponse


def _position(code: str, *, currency: str = "USD") -> PositionItem:
    return PositionItem(
        kind="position",
        external_account_id="redacted-test-account",
        instrument_code=code,
        currency=currency,
        quantity=Decimal("1"),
        available_quantity=Decimal("1"),
        frozen_quantity=Decimal("0"),
        average_cost=Decimal("1"),
        diluted_cost=None,
        current_price=Decimal("1"),
        provider_as_of=datetime(2026, 8, 31, tzinfo=UTC),
    )


def _raw(operation: str, payload: dict[str, object]) -> RawGatewayResponse:
    sent_at = datetime(2026, 9, 1, 4, 0, tzinfo=UTC)
    body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return RawGatewayResponse(
        operation=operation,
        gateway_request_id=uuid4(),
        request_body_sha256=hashlib.sha256(body).hexdigest(),
        raw_payload=body,
        sent_at=sent_at,
        received_at=sent_at,
    )


class _RecordingMarket:
    def __init__(self, codes: tuple[str, ...]) -> None:
        self.codes = codes
        self.calls: list[tuple[str, object]] = []

    async def read_quotes_raw(self, codes: tuple[str, ...]) -> RawGatewayResponse:
        self.calls.append(("quotes", codes))
        as_of = "2026-09-01T04:00:00Z"
        return _raw(
            "fosun_get_quotes",
            {
                "items": [
                    {
                        "kind": "quote",
                        "instrument_code": code,
                        "price": "100",
                        "as_of": as_of,
                    }
                    for code in codes
                ],
                "source_request_id": "quotes-test",
                "has_more": False,
                "next_cursor": None,
                "as_of": as_of,
                "quality": "PARTIAL",
            },
        )

    async def read_bars_raw(self, **kwargs: object) -> RawGatewayResponse:
        self.calls.append(("bars", dict(kwargs)))
        code = str(kwargs["code"])
        as_of = "2026-09-01T04:00:00Z"
        return _raw(
            "fosun_get_bars",
            {
                "items": [
                    {
                        "kind": "bar",
                        "instrument_code": code,
                        "interval": "day",
                        "timestamp": "2026-08-31T00:00:00Z",
                        "open": "99",
                        "high": "101",
                        "low": "98",
                        "close": "100",
                        "volume": "1000",
                    }
                ],
                "source_request_id": f"bars-{code}",
                "has_more": False,
                "next_cursor": None,
                "as_of": as_of,
                "quality": "PARTIAL",
            },
        )


@pytest.mark.asyncio
async def test_local_broker_facts_use_one_complete_six_phase_read() -> None:
    instant = datetime(2026, 8, 31, tzinfo=UTC)
    account_id = uuid4()
    portfolio_id = uuid4()
    sync_run_id = uuid4()
    instrument_id = uuid4()

    class _Broker:
        def __init__(self) -> None:
            self.calls: list[str] = []

        async def read_cash(self, _external_account_id: str) -> GatewayResponsePage:
            self.calls.append("cash")
            return GatewayResponsePage(
                items=(
                    {
                        "kind": "cash",
                        "external_account_id": "redacted-test-account",
                        "currency": "USD",
                        "cash_balance": "1000",
                        "available_cash": "900",
                        "provider_as_of": instant.isoformat(),
                    },
                ),
                source_request_id=f"cash-{len(self.calls)}",
                has_more=False,
                next_cursor=None,
                as_of=instant,
                quality="PASS",
            )

        async def read_holdings(
            self, _external_account_id: str, *, cursor: str | None, limit: int
        ) -> GatewayResponsePage:
            assert cursor is None and limit == 100
            self.calls.append("holdings")
            return GatewayResponsePage(
                items=(_position("usAAPL").model_dump(mode="json"),),
                source_request_id=f"holdings-{len(self.calls)}",
                has_more=False,
                next_cursor=None,
                as_of=instant,
                quality="PASS",
            )

        async def read_orders(
            self, _external_account_id: str, *, cursor: str | None, limit: int
        ) -> GatewayResponsePage:
            assert cursor is None and limit == 100
            self.calls.append("orders")
            return GatewayResponsePage(
                items=(),
                source_request_id=f"orders-{len(self.calls)}",
                has_more=False,
                next_cursor=None,
                as_of=instant,
                quality="PASS",
            )

    broker = _Broker()

    result = await _freeze_broker_facts(
        broker,
        account_id=account_id,
        portfolio_id=portfolio_id,
        sync_run_id=sync_run_id,
        external_account_id="redacted-test-account",
        instrument_ids={"usAAPL": instrument_id},
    )

    assert result.status == "READY"
    assert result.attempt_count == 1
    assert broker.calls == ["cash", "holdings", "orders", "holdings", "orders", "cash"]
    assert result.portfolio_snapshot.portfolio_id == portfolio_id
    assert result.portfolio_snapshot.positions[0].instrument_id == instrument_id


@pytest.mark.asyncio
async def test_local_broker_read_includes_one_cash_flow_window_after_order_reconciliation() -> None:
    instant = datetime(2026, 8, 31, tzinfo=UTC)
    account_id = uuid4()
    instrument_id = uuid4()

    class _Broker:
        def __init__(self) -> None:
            self.calls: list[str] = []

        async def read_cash(self, _external_account_id: str) -> GatewayResponsePage:
            self.calls.append("cash")
            return GatewayResponsePage(
                items=(
                    {
                        "kind": "cash",
                        "external_account_id": "redacted-test-account",
                        "currency": "USD",
                        "cash_balance": "1000",
                        "available_cash": "900",
                        "provider_as_of": instant.isoformat(),
                    },
                ),
                source_request_id=f"cash-{len(self.calls)}",
                has_more=False,
                next_cursor=None,
                as_of=instant,
                quality="PASS",
            )

        async def read_holdings(
            self, _external_account_id: str, *, cursor: str | None, limit: int
        ) -> GatewayResponsePage:
            assert cursor is None and limit == 100
            self.calls.append("holdings")
            return GatewayResponsePage(
                items=(_position("usAAPL").model_dump(mode="json"),),
                source_request_id=f"holdings-{len(self.calls)}",
                has_more=False,
                next_cursor=None,
                as_of=instant,
                quality="PASS",
            )

        async def read_orders(
            self, _external_account_id: str, *, cursor: str | None, limit: int
        ) -> GatewayResponsePage:
            assert cursor is None and limit == 100
            self.calls.append("orders")
            return GatewayResponsePage(
                items=(),
                source_request_id=f"orders-{len(self.calls)}",
                has_more=False,
                next_cursor=None,
                as_of=instant,
                quality="PASS",
            )

        async def read_cash_flows(
            self,
            _external_account_id: str,
            *,
            date_from: date,
            date_to: date,
            cursor: str | None,
            limit: int,
        ) -> GatewayResponsePage:
            assert (date_from, date_to, cursor, limit) == (
                date(2026, 8, 25),
                date(2026, 8, 31),
                None,
                100,
            )
            self.calls.append("cash_flows")
            return GatewayResponsePage(
                items=(
                    {
                        "kind": "cash_flow",
                        "external_cash_flow_id": "synthetic-flow-1",
                        "currency": "USD",
                        "amount": "10",
                        "occurred_at": instant.isoformat(),
                        "flow_type": "1",
                    },
                ),
                source_request_id="cash-flows-1",
                has_more=False,
                next_cursor=None,
                as_of=instant,
                quality="PASS",
            )

    broker = _Broker()
    frozen, cash_flows = await refresh_module._read_broker_facts(
        broker,
        account_id=account_id,
        portfolio_id=uuid4(),
        sync_run_id=uuid4(),
        external_account_id="redacted-test-account",
        instrument_ids={"usAAPL": instrument_id},
        cash_flow_end=date(2026, 8, 31),
        previous_cash_flow_watermark=None,
    )

    assert frozen.status == "READY"
    assert cash_flows.quality == "PASS"
    assert cash_flows.pagination_complete is True
    assert broker.calls == [
        "cash",
        "holdings",
        "orders",
        "holdings",
        "orders",
        "cash",
        "cash_flows",
    ]


@pytest.mark.asyncio
async def test_local_broker_gateway_rejects_pagination_before_follow_up() -> None:
    instant = datetime(2026, 8, 31, tzinfo=UTC)

    class _Broker:
        async def read_orders(
            self, _external_account_id: str, *, cursor: str | None, limit: int
        ) -> GatewayResponsePage:
            assert cursor is None and limit == 100
            return GatewayResponsePage(
                items=(),
                source_request_id="orders-page-1",
                has_more=True,
                next_cursor="2",
                as_of=instant,
                quality="PASS",
            )

    limited = _SinglePageBrokerGateway(_Broker())  # type: ignore[arg-type]

    with pytest.raises(GatewayClientError, match="pagination rejected"):
        await limited.read_orders(
            "redacted-test-account",
            cursor=None,
            limit=100,
        )


def test_daily_window_is_bounded_to_completed_xnys_sessions() -> None:
    start, end, rows = _daily_window(datetime(2026, 9, 1, 4, 0, tzinfo=UTC))

    assert end.date() == date(2026, 8, 31)
    assert start.date() == date(2025, 7, 27)
    assert 1 <= len(tuple(row for row in rows if row[0] <= end.date())) <= 500
    assert all(market_open < market_close for _, market_open, market_close in rows)


def test_us_holding_codes_are_deduplicated_and_stably_sorted() -> None:
    holdings = (
        _position("usNVDA"),
        _position("usAAPL"),
        _position("usNVDA"),
        _position("usMSFT"),
    )

    assert _us_holding_codes(holdings) == ("usAAPL", "usMSFT", "usNVDA")


def test_us_holding_codes_exclude_hk_and_non_us_positions() -> None:
    holdings = (
        _position("hk00656", currency="HKD"),
        _position("00656.HK", currency="HKD"),
        _position("usAAPL"),
        _position("gbVOD", currency="GBP"),
    )

    assert _us_holding_codes(holdings) == ("usAAPL",)


def test_more_than_twelve_us_holding_codes_fails_closed() -> None:
    holdings = tuple(_position(f"usT{index:02d}") for index in range(13))

    with pytest.raises(ValueError, match="LOCAL_US_HOLDING_BUDGET_EXCEEDED"):
        _us_holding_codes(holdings)


@pytest.mark.asyncio
async def test_market_snapshot_uses_one_exact_quote_set_and_one_bar_read_per_code() -> None:
    codes = tuple(f"usT{index:02d}" for index in range(12))
    market = _RecordingMarket(codes)
    start = datetime(2026, 8, 31, tzinfo=UTC)
    end = datetime(2026, 8, 31, tzinfo=UTC)

    snapshot = await _read_market_snapshot(
        market,
        codes=codes,
        request_start_at=start,
        request_end_at=end,
        expected_sessions=(date(2026, 8, 31),),
    )

    assert market.calls[0] == ("quotes", codes)
    bar_calls = [payload for operation, payload in market.calls if operation == "bars"]
    assert [call["code"] for call in bar_calls] == list(codes)
    assert len(bar_calls) == len(set(call["code"] for call in bar_calls)) == 12
    assert snapshot.request_counts == {
        "quotes": 1,
        "bars": 12,
        "total": 13,
    }


@pytest.mark.asyncio
async def test_market_snapshot_accepts_a_listing_limited_daily_series() -> None:
    market = _RecordingMarket(("usNEW",))
    start = datetime(2026, 8, 28, tzinfo=UTC)
    end = datetime(2026, 8, 31, tzinfo=UTC)

    snapshot = await _read_market_snapshot(
        market,
        codes=("usNEW",),
        request_start_at=start,
        request_end_at=end,
        expected_sessions=(date(2026, 8, 28), date(2026, 8, 31)),
    )

    assert snapshot.codes == ("usNEW",)
    assert snapshot.request_counts["total"] == 2


@pytest.mark.asyncio
async def test_market_snapshot_failure_returns_before_any_persistence_input_exists() -> None:
    class _FailingMarket(_RecordingMarket):
        async def read_bars_raw(self, **kwargs: object) -> RawGatewayResponse:
            if kwargs["code"] == "usMSFT":
                raise RuntimeError("LOCAL_TEST_READ_FAILED")
            return await super().read_bars_raw(**kwargs)

    codes = ("usAAPL", "usMSFT")
    market = _FailingMarket(codes)
    instant = datetime(2026, 8, 31, tzinfo=UTC)

    with pytest.raises(RuntimeError, match="LOCAL_TEST_READ_FAILED"):
        await _read_market_snapshot(
            market,
            codes=codes,
            request_start_at=instant,
            request_end_at=instant,
            expected_sessions=(date(2026, 8, 31),),
        )

    assert market.calls == [
        ("quotes", codes),
        (
            "bars",
            {
                "code": "usAAPL",
                "ktype": "day",
                "start_time": instant,
                "end_time": instant,
                "limit": 1,
            },
        ),
    ]


@pytest.mark.asyncio
async def test_provider_failure_never_opens_database_or_overwrites_previous_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instant = datetime(2026, 8, 31, tzinfo=UTC)

    class _Broker:
        async def read_accounts(self) -> GatewayResponsePage:
            return GatewayResponsePage(
                items=(
                    {
                        "kind": "account",
                        "external_account_id": "redacted-test-account",
                        "status": "ACTIVE",
                        "base_currency": "USD",
                    },
                ),
                source_request_id="accounts-test",
                has_more=False,
                next_cursor=None,
                as_of=instant,
                quality="PASS",
            )

        async def read_cash(self, _account_id: str) -> GatewayResponsePage:
            return GatewayResponsePage(
                items=(
                    {
                        "kind": "cash",
                        "external_account_id": "redacted-test-account",
                        "currency": "USD",
                        "cash_balance": "1",
                        "available_cash": "1",
                        "provider_as_of": instant.isoformat(),
                    },
                ),
                source_request_id="cash-test",
                has_more=False,
                next_cursor=None,
                as_of=instant,
                quality="PASS",
            )

        async def read_holdings(
            self, _account_id: str, *, cursor: None, limit: int
        ) -> GatewayResponsePage:
            assert cursor is None and limit == 100
            return GatewayResponsePage(
                items=tuple(
                    _position(code).model_dump(mode="json") for code in ("usAAPL", "usMSFT")
                ),
                source_request_id="holdings-test",
                has_more=False,
                next_cursor=None,
                as_of=instant,
                quality="PASS",
            )

        async def aclose(self) -> None:
            return None

    class _FailingMarket(_RecordingMarket):
        async def read_bars_raw(self, **kwargs: object) -> RawGatewayResponse:
            if kwargs["code"] == "usMSFT":
                raise RuntimeError("LOCAL_TEST_READ_FAILED")
            return await super().read_bars_raw(**kwargs)

        async def aclose(self) -> None:
            return None

    broker = _Broker()
    market = _FailingMarket(("usAAPL", "usMSFT"))
    database_opened = False

    def _database_must_remain_closed(_url: str, **_kwargs: object) -> object:
        nonlocal database_opened
        database_opened = True
        raise AssertionError("database opened before provider reads completed")

    monkeypatch.setattr(
        refresh_module,
        "_daily_window",
        lambda _now: (
            instant,
            instant,
            ((instant.date(), instant - timedelta(hours=8), instant - timedelta(hours=1)),),
        ),
    )
    monkeypatch.setattr(refresh_module, "_broker_settings", lambda _settings: object())
    monkeypatch.setattr(refresh_module, "_market_identity", lambda _settings: object())
    monkeypatch.setattr(refresh_module, "FosunGatewayClient", lambda _settings: broker)
    monkeypatch.setattr(refresh_module, "FosunMarketGatewayClient", lambda _identity: market)
    monkeypatch.setattr(refresh_module, "create_async_engine", _database_must_remain_closed)

    with pytest.raises(RuntimeError, match="LOCAL_TEST_READ_FAILED"):
        await refresh_local_fosun(SimpleNamespace(), now=instant)  # type: ignore[arg-type]

    assert database_opened is False


@pytest.mark.parametrize(
    ("provider_code", "symbol", "exchange"),
    (("usAAPL", "AAPL", "XNYS"), ("hk00656", "00656.HK", "XHKG")),
)
def test_provider_instrument_codes_are_mapped_without_guessing(
    provider_code: str,
    symbol: str,
    exchange: str,
) -> None:
    assert _symbol_and_exchange(provider_code) == (symbol, exchange)


def test_unsupported_provider_instrument_code_fails_closed() -> None:
    with pytest.raises(ValueError, match="LOCAL_INSTRUMENT_CODE_UNSUPPORTED"):
        _symbol_and_exchange("AAPL")


def test_failure_output_only_preserves_fixed_local_error_codes() -> None:
    assert _safe_failure_code(ValueError("LOCAL_KLINE_WINDOW_INCOMPLETE")) == (
        "LOCAL_KLINE_WINDOW_INCOMPLETE"
    )
    assert _safe_failure_code(ValueError("secret value")) == "LOCAL_READ_ONLY_REFRESH_FAILED"
    assert _safe_failure_code(RuntimeError("LOCAL_BAD-CHAR")) == ("LOCAL_READ_ONLY_REFRESH_FAILED")
