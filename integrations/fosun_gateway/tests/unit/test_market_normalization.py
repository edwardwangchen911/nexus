"""Fosun market responses are normalized as complete bounded pages."""

from datetime import UTC, datetime

import pytest
from nexus_fosun_gateway.adapter.normalization import normalize_provider_response
from nexus_fosun_gateway.domain import GatewayError

RECEIVED_AT = datetime(2026, 8, 29, 4, tzinfo=UTC)


def _raw(data: object) -> dict[str, object]:
    return {"code": 0, "data": data, "requestId": "synthetic-market-request"}


@pytest.mark.parametrize("delay", [True, False, None, "missing"])
def test_bar_response_retains_only_provider_reported_delay(delay):
    data = {"data": [], "power": 0, "total": 0}
    if delay != "missing":
        data["delay"] = delay
    page = normalize_provider_response(
        "fosun_get_bars",
        _raw(data),
        received_at=RECEIVED_AT,
        context={"code": "usAAPL", "ktype": "day", "limit": 10, "delay": True},
    )
    assert page.model_dump()["provider_reported_delay"] is (None if delay == "missing" else delay)
    assert page.quality == "PARTIAL"


@pytest.mark.parametrize("delay", [0, 1, "true", "false", {}, []])
def test_bar_response_rejects_non_boolean_delay(delay):
    with pytest.raises(GatewayError) as error:
        normalize_provider_response(
            "fosun_get_bars",
            _raw({"data": [], "power": 0, "total": 0, "delay": delay}),
            received_at=RECEIVED_AT,
            context={"code": "usAAPL", "ktype": "day", "limit": 10},
        )
    assert error.value.code == "PROVIDER_MALFORMED_RESPONSE"


def test_quotes_preserve_request_order_and_materialize_missing_price() -> None:
    page = normalize_provider_response(
        "fosun_get_quotes",
        _raw(
            [
                {
                    "mkt": "us",
                    "rawSymbol": "MSFT",
                    "price": 42000,
                    "power": 2,
                }
            ]
        ),
        received_at=RECEIVED_AT,
        context={"codes": ("usAAPL", "usMSFT")},
    )

    assert [item.instrument_code for item in page.items] == ["usAAPL", "usMSFT"]
    assert page.items[0].price is None
    assert page.items[0].as_of == RECEIVED_AT
    assert page.quality == "PARTIAL"


def test_quotes_reject_unrequested_provider_code() -> None:
    with pytest.raises(GatewayError) as error:
        normalize_provider_response(
            "fosun_get_quotes",
            _raw(
                [
                    {
                        "mkt": "us",
                        "rawSymbol": "AAPL",
                        "price": 20000,
                        "power": 2,
                    },
                    {
                        "mkt": "us",
                        "rawSymbol": "MSFT",
                        "price": 42000,
                        "power": 2,
                    },
                ]
            ),
            received_at=RECEIVED_AT,
            context={"codes": ("usAAPL",)},
        )

    assert error.value.code == "PROVIDER_MALFORMED_RESPONSE"


def test_quotes_reject_obsolete_keyed_map_shape() -> None:
    with pytest.raises(GatewayError) as error:
        normalize_provider_response(
            "fosun_get_quotes",
            _raw({"usAAPL": {"price": 20000, "power": 2}}),
            received_at=RECEIVED_AT,
            context={"codes": ("usAAPL",)},
        )

    assert error.value.code == "PROVIDER_MALFORMED_RESPONSE"


@pytest.mark.parametrize(
    "rows",
    [
        [
            {
                "time": "2026-08-21T00:00:00Z",
                "open": 100,
                "high": 110,
                "low": 90,
                "close": 105,
                "vol": 10,
            },
            {
                "time": "2026-08-20T00:00:00Z",
                "open": 100,
                "high": 110,
                "low": 90,
                "close": 105,
                "vol": 10,
            },
        ],
        [
            {
                "time": "2026-08-21T00:00:00Z",
                "open": 100,
                "high": 95,
                "low": 90,
                "close": 105,
                "vol": 10,
            }
        ],
        [
            {
                "time": "2026-08-21T00:00:00Z",
                "open": 100,
                "high": 110,
                "low": 90,
                "close": 105,
                "vol": -1,
            }
        ],
    ],
)
def test_bars_reject_order_ohlc_and_volume_contract_violations(
    rows: list[dict[str, object]],
) -> None:
    with pytest.raises(GatewayError) as error:
        normalize_provider_response(
            "fosun_get_bars",
            _raw({"data": rows, "power": 0, "total": len(rows)}),
            received_at=RECEIVED_AT,
            context={
                "code": "usAAPL",
                "ktype": "day",
                "start_time": "2026-08-01T00:00:00Z",
                "end_time": "2026-08-22T00:00:00Z",
                "limit": 10,
            },
        )

    assert error.value.code == "PROVIDER_MALFORMED_RESPONSE"


def test_bars_reject_rows_outside_window_or_after_receipt() -> None:
    for timestamp in ("2026-07-31T00:00:00Z", "2026-08-30T00:00:00Z"):
        with pytest.raises(GatewayError):
            normalize_provider_response(
                "fosun_get_bars",
                _raw(
                    {
                        "data": [
                            {
                                "time": timestamp,
                                "open": 100,
                                "high": 110,
                                "low": 90,
                                "close": 105,
                                "vol": 10,
                            }
                        ],
                        "power": 0,
                        "total": 1,
                    }
                ),
                received_at=RECEIVED_AT,
                context={
                    "code": "usAAPL",
                    "ktype": "day",
                    "start_time": "2026-08-01T00:00:00Z",
                    "end_time": "2026-08-31T00:00:00Z",
                    "limit": 10,
                },
            )


def test_daily_bar_window_uses_provider_session_date_not_intraday_clock() -> None:
    page = normalize_provider_response(
        "fosun_get_bars",
        _raw(
            {
                "data": [
                    {
                        "time": 20260821,
                        "open": 100,
                        "high": 110,
                        "low": 90,
                        "close": 105,
                        "vol": 10,
                    }
                ],
                "power": 0,
                "total": 1,
            }
        ),
        received_at=RECEIVED_AT,
        context={
            "code": "usAAPL",
            "ktype": "day",
            "start_time": "2026-08-21T20:00:00Z",
            "end_time": "2026-08-22T20:00:00Z",
            "limit": 10,
        },
    )

    assert page.items[0].timestamp == datetime(2026, 8, 21, tzinfo=UTC)


@pytest.mark.parametrize("total", (2, -1, True, "1", None))
def test_bars_reject_partial_or_noncanonical_total(total: object) -> None:
    with pytest.raises(GatewayError, match="reviewed contract"):
        normalize_provider_response(
            "fosun_get_bars",
            _raw(
                {
                    "data": [
                        {
                            "time": 20260821,
                            "open": 100,
                            "high": 110,
                            "low": 90,
                            "close": 105,
                            "vol": 10,
                        }
                    ],
                    "power": 0,
                    "total": total,
                }
            ),
            received_at=RECEIVED_AT,
            context={
                "code": "usAAPL",
                "ktype": "day",
                "start_time": "2026-08-01T00:00:00Z",
                "end_time": "2026-08-22T00:00:00Z",
                "limit": 10,
            },
        )
