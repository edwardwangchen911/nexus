import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import nexus.ops.fosun_local as local_module
import nexus.ops.fosun_local_refresh as refresh_module
import pytest
from nexus.ops.fosun_local import safe_failure
from nexus.ops.fosun_local_refresh import (
    _active_holdings,
    _daily_window,
    _freeze_broker_facts,
    _read_market_snapshot,
    _safe_failure_code,
    _SinglePageBrokerGateway,
    _symbol_and_market,
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
@pytest.mark.parametrize(
    ("delays", "expected"),
    (
        ((True, True), "delayed"),
        ((False, False), "real_time"),
        ((True, False), "unknown"),
        ((True, None), "unknown"),
        ((None, None), "unknown"),
    ),
)
async def test_local_delivery_uses_only_consistent_response_declarations(delays, expected):
    codes = ("usAAPL", "usMSFT")

    class Market(_RecordingMarket):
        async def read_bars_raw(self, **kwargs):
            raw = await super().read_bars_raw(**kwargs)
            doc = json.loads(raw.raw_payload)
            doc["provider_reported_delay"] = delays[codes.index(kwargs["code"])]
            return raw.model_copy(update={"raw_payload": json.dumps(doc).encode()})

    market = Market(codes)
    quote_raw = await market.read_quotes_raw(codes)
    result = await _read_market_snapshot(
        market,
        codes=codes,
        quote_codes=codes,
        quote_raw=quote_raw,
        request_start_at=datetime(2026, 8, 31, tzinfo=UTC),
        request_end_at=datetime(2026, 9, 1, tzinfo=UTC),
        expected_sessions=(date(2026, 8, 31),),
    )
    assert result.delivery_mode == expected
    assert result.request_counts["total"] == 3


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
@pytest.mark.parametrize(
    ("order_error", "category"),
    (
        (None, None),
        (None, "missing_event_time"),
        ("gateway rejected request: PROVIDER_PERMISSION_DENIED", "permission"),
        ("gateway rejected request: OPERATION_NOT_ALLOWED", "scope"),
        ("gateway rejected request: PROVIDER_AUTHENTICATION_FAILED", "auth"),
        ("gateway rejected request: PROVIDER_RATE_LIMITED", "rate_limit"),
        ("gateway rejected request: PROVIDER_ERROR", "provider"),
        ("gateway rejected request: PROVIDER_APPLICATION_ERROR", "provider"),
        ("gateway rejected request: PROVIDER_MALFORMED_RESPONSE", "contract"),
        ("gateway request failed", "transport"),
        ("gateway rejected request: PROVIDER_ERROR secret-canary", "gateway"),
    ),
)
async def test_local_broker_read_preserves_order_failure_and_bounds_follow_up(
    order_error: str | None, category: str | None
) -> None:
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
                        "provider_as_of": (
                            None if category == "missing_event_time" else instant.isoformat()
                        ),
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
            if order_error is not None:
                raise GatewayClientError(order_error)
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
                        "direction": 1,
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
    read = refresh_module._read_broker_facts(
        broker,
        account_id=account_id,
        portfolio_id=uuid4(),
        sync_run_id=uuid4(),
        external_account_id="redacted-test-account",
        instrument_ids={"usAAPL": instrument_id},
        cash_flow_end=date(2026, 8, 31),
        previous_cash_flow_watermark=None,
    )

    if order_error is not None:
        with pytest.raises(RuntimeError) as failure:
            await read
        assert safe_failure(failure.value) == {
            "status": "failed",
            "code": "LOCAL_ORDER_LIST_READ_FAILED",
            "category": category,
            "operation": "OrderList",
        }
        assert "canary" not in str(failure.value)
        assert "redacted-test-account" not in repr(failure.value)
        assert broker.calls == ["cash", "holdings", "orders"]
        return

    frozen, cash_flows = await read

    assert frozen.status == "READY"
    assert {item.time_basis for item in frozen.portfolio_snapshot.source_observations} == {
        "gateway_observed"
    }
    if category == "missing_event_time":
        assert frozen.portfolio_snapshot.cash[0].provider_as_of is None
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

    with pytest.raises(RuntimeError, match="LOCAL_ORDER_LIST_READ_FAILED") as failure:
        await limited.read_orders(
            "redacted-test-account",
            cursor=None,
            limit=100,
        )
    assert safe_failure(failure.value)["category"] == "contract"


def test_daily_window_is_bounded_to_completed_xnys_sessions() -> None:
    start, end, rows = _daily_window(datetime(2026, 9, 1, 4, 0, tzinfo=UTC))

    assert end.date() == date(2026, 8, 31)
    assert start.date() == date(2025, 7, 27)
    assert 1 <= len(tuple(row for row in rows if row[0] <= end.date())) <= 500
    assert all(market_open < market_close for _, market_open, market_close in rows)


@pytest.mark.parametrize(
    "now",
    [
        datetime(2026, 9, 26, 3, 40, tzinfo=UTC),
        datetime(2026, 11, 28, 15, 0, tzinfo=UTC),
    ],
)
def test_refresh_calendar_covers_future_service_sessions_without_future_bars(now: datetime) -> None:
    start, end, rows = _daily_window(now)

    upcoming = tuple(row for row in rows if row[1] > now)
    assert len(upcoming) >= 2
    assert rows[-1][0] <= now.date() + timedelta(days=30)
    expected_market_sessions = tuple(row[0] for row in rows if row[0] <= end.date())
    assert expected_market_sessions[-1] == end.date()
    assert all(row[2] <= now - timedelta(minutes=30) for row in rows if row[0] <= end.date())
    assert start == end - timedelta(days=400)


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


def test_closed_zero_rows_are_not_current_holdings() -> None:
    active = _position("usAAPL")
    closed = _position("usOLD").model_copy(
        update={
            "quantity": Decimal("0"),
            "available_quantity": Decimal("0"),
            "frozen_quantity": Decimal("0"),
        }
    )

    assert _active_holdings((closed, active)) == (active,)


def test_more_than_twenty_us_holding_codes_fails_closed() -> None:
    holdings = tuple(_position(f"usT{index:02d}") for index in range(21))

    with pytest.raises(ValueError, match="LOCAL_US_HOLDING_BUDGET_EXCEEDED"):
        _us_holding_codes(holdings)


@pytest.mark.asyncio
async def test_market_snapshot_uses_one_exact_quote_set_and_one_bar_read_per_code() -> None:
    codes = tuple(f"usT{index:02d}" for index in range(20))
    market = _RecordingMarket(codes)
    start = datetime(2026, 8, 31, tzinfo=UTC)
    end = datetime(2026, 8, 31, tzinfo=UTC)
    quote_raw = await market.read_quotes_raw(codes)

    snapshot = await _read_market_snapshot(
        market,
        codes=codes,
        quote_codes=codes,
        quote_raw=quote_raw,
        request_start_at=start,
        request_end_at=end,
        expected_sessions=(date(2026, 8, 31),),
    )

    assert market.calls[0] == ("quotes", codes)
    bar_calls = [payload for operation, payload in market.calls if operation == "bars"]
    assert [call["code"] for call in bar_calls] == list(codes)
    assert len(bar_calls) == len(set(call["code"] for call in bar_calls)) == 20
    assert snapshot.request_counts == {
        "quotes": 1,
        "bars": 20,
        "total": 21,
    }


@pytest.mark.asyncio
async def test_market_snapshot_accepts_a_listing_limited_daily_series() -> None:
    market = _RecordingMarket(("usNEW",))
    start = datetime(2026, 8, 28, tzinfo=UTC)
    end = datetime(2026, 8, 31, tzinfo=UTC)
    quote_raw = await market.read_quotes_raw(("usNEW",))

    snapshot = await _read_market_snapshot(
        market,
        codes=("usNEW",),
        quote_codes=("usNEW",),
        quote_raw=quote_raw,
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
    quote_raw = await market.read_quotes_raw(codes)

    with pytest.raises(RuntimeError, match="LOCAL_TEST_READ_FAILED"):
        await _read_market_snapshot(
            market,
            codes=codes,
            quote_codes=codes,
            quote_raw=quote_raw,
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
async def test_provider_failure_after_identity_preflight_never_persists_snapshot(
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
    database_closed = False

    class _Engine:
        async def dispose(self) -> None:
            nonlocal database_closed
            database_closed = True

    class _Session:
        async def __aenter__(self) -> object:
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

    def _open_database(_url: str, **_kwargs: object) -> object:
        nonlocal database_opened
        database_opened = True
        return _Engine()

    async def _identity_preflight(*_args: object, **_kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(instrument_ids={"usAAPL": uuid4(), "usMSFT": uuid4()})

    def _persistence_must_not_start(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("persistence started after a failed provider read")

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
    monkeypatch.setattr(refresh_module, "create_async_engine", _open_database)
    monkeypatch.setattr(refresh_module, "async_sessionmaker", lambda *_a, **_k: _Session)
    monkeypatch.setattr(refresh_module, "_load_local_broker_identity", _identity_preflight)
    monkeypatch.setattr(refresh_module, "S3ProviderEvidenceStore", _persistence_must_not_start)

    with pytest.raises(RuntimeError, match="LOCAL_TEST_READ_FAILED"):
        await refresh_local_fosun(SimpleNamespace(database_url="synthetic"), now=instant)  # type: ignore[arg-type]

    assert database_opened is True
    assert database_closed is True


@pytest.mark.parametrize(
    ("provider_code", "symbol", "exchange"),
    (("usAAPL", "AAPL", "US"), ("hk00656", "00656.HK", "HK")),
)
def test_provider_instrument_codes_identify_market_not_listing_exchange(
    provider_code: str,
    symbol: str,
    exchange: str,
) -> None:
    assert _symbol_and_market(provider_code) == (symbol, exchange)


def test_unsupported_provider_instrument_code_fails_closed() -> None:
    with pytest.raises(ValueError, match="LOCAL_INSTRUMENT_CODE_UNSUPPORTED"):
        _symbol_and_market("AAPL")


def test_failure_output_only_preserves_fixed_local_error_codes() -> None:
    assert _safe_failure_code(ValueError("LOCAL_KLINE_WINDOW_INCOMPLETE")) == (
        "LOCAL_KLINE_WINDOW_INCOMPLETE"
    )
    assert _safe_failure_code(ValueError("secret value")) == "LOCAL_READ_ONLY_REFRESH_FAILED"
    assert _safe_failure_code(RuntimeError("LOCAL_BAD-CHAR")) == ("LOCAL_READ_ONLY_REFRESH_FAILED")


@pytest.mark.parametrize("entrypoint", ("local", "refresh"))
def test_order_failure_reaches_both_cli_outputs_without_payloads(
    entrypoint: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    class _Broker:
        async def read_orders(self, *_args: object, **_kwargs: object) -> GatewayResponsePage:
            raise GatewayClientError("gateway rejected request: PROVIDER_PERMISSION_DENIED")

    async def _refresh(_settings: object) -> dict[str, object]:
        await _SinglePageBrokerGateway(_Broker()).read_orders(  # type: ignore[arg-type]
            "secret-account-canary", cursor=None, limit=100
        )
        raise AssertionError("must stop on first error")

    module = local_module if entrypoint == "local" else refresh_module
    settings_factory = (
        SimpleNamespace(model_validate=lambda _environment: object())
        if entrypoint == "local"
        else lambda: object()
    )
    monkeypatch.setattr(module, "LocalFosunRefreshSettings", settings_factory)
    monkeypatch.setattr(module, "refresh_local_fosun", _refresh)
    monkeypatch.setattr(local_module, "load_local_environment", lambda *_args: {})
    monkeypatch.setattr("sys.argv", ["nexus-fosun-local", "refresh"])
    with pytest.raises(SystemExit) as stopped:
        module.main()

    assert stopped.value.code == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert json.loads(output.err) == {
        "status": "failed",
        "code": "LOCAL_ORDER_LIST_READ_FAILED",
        "category": "permission",
        "operation": "OrderList",
    }
    assert "canary" not in output.err


@pytest.mark.parametrize(
    "provider_code, category",
    [
        ("PROVIDER_RATE_LIMITED", "rate_limit"),
        ("PROVIDER_UNAVAILABLE", "provider"),
        ("PROVIDER_PERMISSION_DENIED", "permission"),
        ("PROVIDER_AUTHENTICATION_FAILED", "auth"),
        ("PROVIDER_MALFORMED_RESPONSE", "contract"),
        ("PROVIDER_WINDOW_MISMATCH", "contract"),
        ("GATEWAY_REPLAY_DRAINING", "not_ready"),
    ],
)
def test_refresh_failure_preserves_only_known_gateway_codes(
    provider_code: str, category: str
) -> None:
    error = GatewayClientError(f"gateway rejected request: {provider_code}")
    assert safe_failure(error) == {
        "status": "failed",
        "category": category,
        "code": f"LOCAL_GATEWAY_{provider_code}",
    }


@pytest.mark.parametrize(
    "message",
    [
        "gateway rejected request: UNKNOWN_PROVIDER_CODE",
        "gateway rejected request: PROVIDER_UNAVAILABLE private-account-payload",
    ],
)
def test_unknown_gateway_error_content_is_not_exported(message: str) -> None:
    assert _safe_failure_code(GatewayClientError(message)) == "LOCAL_READ_ONLY_REFRESH_FAILED"


@pytest.mark.parametrize("entrypoint", ("local", "refresh"))
def test_market_gateway_code_reaches_normal_cli_stderr(
    entrypoint: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def failed_refresh(_settings: object) -> dict[str, object]:
        raise GatewayClientError("gateway rejected request: PROVIDER_RATE_LIMITED")

    module = local_module if entrypoint == "local" else refresh_module
    factory = (
        SimpleNamespace(model_validate=lambda _environment: object())
        if entrypoint == "local"
        else lambda: object()
    )
    monkeypatch.setattr(module, "LocalFosunRefreshSettings", factory)
    monkeypatch.setattr(module, "refresh_local_fosun", failed_refresh)
    monkeypatch.setattr(local_module, "load_local_environment", lambda *_args: {})
    monkeypatch.setattr("sys.argv", ["nexus-fosun-local", "refresh"])
    with pytest.raises(SystemExit) as stopped:
        module.main()
    assert stopped.value.code == 1
    output = capsys.readouterr()
    assert output.out == ""
    expected = {"status": "failed", "code": "LOCAL_GATEWAY_PROVIDER_RATE_LIMITED"}
    if entrypoint == "local":
        expected["category"] = "rate_limit"
    assert json.loads(output.err) == expected


def test_broker_identity_gap_receipt_is_private_minimal_and_immutable(tmp_path) -> None:
    parent = tmp_path / "private"
    parent.mkdir(mode=0o700)
    directory = parent / "identity-gaps"
    observed = datetime(2026, 10, 10, tzinfo=UTC)
    page = GatewayResponsePage(
        items=(_position("usNEW").model_dump(mode="json"),),
        source_request_id="synthetic-holdings-request",
        has_more=False,
        next_cursor=None,
        as_of=observed,
        quality="PASS",
    )
    path = refresh_module._archive_broker_identity_gap(
        directory,
        holdings=(_position("usNEW"),),
        unresolved_codes=("usNEW",),
        holdings_page=page,
    )
    document = json.loads(path.read_text())
    assert document["unresolved"] == [{"provider_code": "usNEW", "currency": "USD"}]
    assert document["identity_authority"] is False
    assert (
        document["source_page_sha256"]
        == hashlib.sha256(page.model_dump_json().encode()).hexdigest()
    )
    assert path.stat().st_mode & 0o777 == 0o600
    assert directory.stat().st_mode & 0o777 == 0o700
    assert "redacted-test-account" not in path.read_text()
    assert "quantity" not in path.read_text()
    second = refresh_module._archive_broker_identity_gap(
        directory,
        holdings=(_position("usNEW"),),
        unresolved_codes=("usNEW",),
        holdings_page=page,
    )
    assert second != path and path.exists()


def test_broker_identity_gap_rejects_unprotected_parent(tmp_path) -> None:
    parent = tmp_path / "unsafe"
    parent.mkdir(mode=0o755)
    page = GatewayResponsePage(
        items=(),
        source_request_id="synthetic",
        has_more=False,
        next_cursor=None,
        as_of=datetime(2026, 10, 10, tzinfo=UTC),
        quality="PASS",
    )
    with pytest.raises(ValueError, match="LOCAL_IDENTITY_GAP_ARCHIVE_UNSAFE"):
        refresh_module._archive_broker_identity_gap(
            parent / "identity-gaps",
            holdings=(),
            unresolved_codes=(),
            holdings_page=page,
        )


@pytest.mark.parametrize("archive_failure", [False, True])
@pytest.mark.asyncio
async def test_broker_refresh_archives_gap_before_any_snapshot_or_follow_up(
    tmp_path, monkeypatch, archive_failure
) -> None:
    observed = datetime(2026, 10, 10, tzinfo=UTC)
    calls = []

    class Broker:
        async def read_accounts(self):
            calls.append("accounts")
            return GatewayResponsePage(
                items=(
                    {
                        "kind": "account",
                        "external_account_id": "redacted-test-account",
                        "base_currency": "USD",
                        "status": "active",
                    },
                ),
                source_request_id="accounts",
                has_more=False,
                next_cursor=None,
                as_of=observed,
                quality="PASS",
            )

        async def read_holdings(self, account, **kwargs):
            calls.append("holdings")
            return GatewayResponsePage(
                items=(_position("usNEW").model_dump(mode="json"),),
                source_request_id="holdings",
                has_more=False,
                next_cursor=None,
                as_of=observed,
                quality="PASS",
            )

        async def aclose(self):
            calls.append("closed")

    class Engine:
        async def dispose(self):
            calls.append("disposed")

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    async def identity(session, *, holdings, allow_unresolved=False):
        assert allow_unresolved
        return SimpleNamespace(unresolved_codes=("usNEW",))

    async def forbidden(*args, **kwargs):
        raise AssertionError("follow-up or business persistence after identity gap")

    monkeypatch.setattr(refresh_module, "_broker_settings", lambda s: object())
    monkeypatch.setattr(refresh_module, "FosunGatewayClient", lambda s: Broker())
    monkeypatch.setattr(refresh_module, "create_async_engine", lambda *a, **k: Engine())
    monkeypatch.setattr(refresh_module, "async_sessionmaker", lambda *a, **k: Session)
    monkeypatch.setattr(refresh_module, "_load_local_broker_identity", identity)
    monkeypatch.setattr(refresh_module, "_read_broker_facts", forbidden)
    monkeypatch.setattr(refresh_module, "_persist_ready_broker_snapshot", forbidden)
    parent = tmp_path / "private"
    parent.mkdir(mode=0o700)
    if archive_failure:

        def broken_archive(*args, **kwargs):
            raise OSError("synthetic-disk-full")

        monkeypatch.setattr(refresh_module, "_archive_broker_identity_gap", broken_archive)
    with pytest.raises(ValueError, match="LOCAL_BROKER_INSTRUMENT_SOURCE_REQUIRED"):
        await refresh_module.refresh_local_fosun_broker(
            SimpleNamespace(database_url="synthetic"),
            now=observed,
            identity_gap_dir=parent / "gaps",
        )
    assert calls == ["accounts", "holdings", "closed", "disposed"]
    assert len(list((parent / "gaps").glob("*.json"))) == (0 if archive_failure else 1)


@pytest.mark.parametrize("symlink", [False, True])
def test_broker_identity_gap_rejects_unsafe_secret_ancestor(tmp_path, symlink):
    ancestor = tmp_path / ".secrets"
    if symlink:
        outside = tmp_path / "outside"
        outside.mkdir(mode=0o700)
        ancestor.symlink_to(outside, target_is_directory=True)
    else:
        ancestor.mkdir(mode=0o755)
    parent = ancestor / "nexus-fosun-local"
    parent.mkdir(mode=0o700)
    page = GatewayResponsePage(
        items=(),
        source_request_id="synthetic",
        has_more=False,
        next_cursor=None,
        as_of=datetime(2026, 10, 10, tzinfo=UTC),
        quality="PASS",
    )
    with pytest.raises(ValueError, match="LOCAL_IDENTITY_GAP_ARCHIVE_UNSAFE"):
        refresh_module._archive_broker_identity_gap(
            parent / "identity-gaps", holdings=(), unresolved_codes=(), holdings_page=page
        )
    assert not (parent / "identity-gaps").exists()
