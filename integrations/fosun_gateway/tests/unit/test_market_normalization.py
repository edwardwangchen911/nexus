"""Fosun market responses are normalized as complete bounded pages."""

from datetime import UTC, datetime

import pytest
from nexus_fosun_gateway.adapter.normalization import normalize_provider_response
from nexus_fosun_gateway.domain import GatewayError

RECEIVED_AT = datetime(2026, 8, 29, 4, tzinfo=UTC)


def _raw(data: object) -> dict[str, object]:
    return {"code": 0, "data": data, "requestId": "synthetic-market-request"}


@pytest.mark.parametrize("turnover", [None, 0, 900719925474099312345])
def test_bars_preserve_optional_raw_turnover_and_source_scale(turnover):
    row = {"time": 20260828, "open": 1234, "high": 1234, "low": 1234, "close": 1234, "vol": 7}
    if turnover is not None:
        row["turnover"] = turnover
    page = normalize_provider_response(
        "fosun_get_bars",
        _raw({"data": [row], "power": 2, "total": 1}),
        received_at=RECEIVED_AT,
        context={"code": "usAAPL", "ktype": "day", "limit": 10},
    )
    document = page.model_dump(mode="json")
    assert document["items"][0]["provider_raw_turnover"] == turnover
    assert document["provider_scale_power"] == 2
    assert document["items"][0]["close"] == "12.34"
    assert document["items"][0]["volume"] == "7"
    assert page.quality == "PARTIAL"


@pytest.mark.parametrize("turnover", [True, -1, 1.5, "1", {}, []])
def test_bars_reject_malformed_present_raw_turnover(turnover):
    with pytest.raises(GatewayError) as error:
        normalize_provider_response(
            "fosun_get_bars",
            _raw(
                {
                    "data": [
                        {
                            "time": 20260828,
                            "open": 1234,
                            "high": 1234,
                            "low": 1234,
                            "close": 1234,
                            "vol": 7,
                            "turnover": turnover,
                        }
                    ],
                    "power": 2,
                    "total": 1,
                }
            ),
            received_at=RECEIVED_AT,
            context={"code": "usAAPL", "ktype": "day", "limit": 10},
        )
    assert error.value.code == "PROVIDER_MALFORMED_RESPONSE"


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


@pytest.mark.parametrize(
    ("classification", "expected_equity", "expected_net_asset"),
    [
        (
            {
                "first": 1,
                "second": 101,
                "third": 10105,
                "mktCap": 1,
                "floatMktCap": 1,
                "totalShares": 1,
                "floatShares": 1,
                "pb": 1,
                "peStatic": 1,
                "peTTM": 1,
            },
            True,
            False,
        ),
        (
            {
                "first": 2,
                "second": 202,
                "third": 20202,
                "nav": 1,
                "assetSize": 1,
                "issueVol": 1,
                "fundAssetTypeChs": "",
                "fundAssetTypeCht": "",
                "fundAssetTypeEng": "",
            },
            False,
            True,
        ),
    ],
)
def test_quotes_preserve_provider_classification_evidence(
    classification: dict[str, object],
    expected_equity: bool,
    expected_net_asset: bool,
) -> None:
    row = {
        "mkt": "us",
        "rawSymbol": "AAPL",
        "name": "Provider security name",
        "price": 12345,
        "power": 2,
        "delay": True,
        **classification,
    }

    page = normalize_provider_response(
        "fosun_get_quotes",
        _raw([row]),
        received_at=RECEIVED_AT,
        context={"codes": ("usAAPL",)},
    )

    quote = page.items[0]
    assert quote.provider_name == "Provider security name"
    assert quote.provider_reported_delay is True
    assert quote.provider_classification is not None
    assert (
        quote.provider_classification.level_1,
        quote.provider_classification.level_2,
        quote.provider_classification.level_3,
    ) == (classification["first"], classification["second"], classification["third"])
    assert quote.provider_classification.equity_profile_fields_complete is expected_equity
    assert quote.provider_classification.net_asset_profile_fields_complete is expected_net_asset


def test_quotes_without_provider_classification_remain_compatible() -> None:
    page = normalize_provider_response(
        "fosun_get_quotes",
        _raw([{"mkt": "us", "rawSymbol": "AAPL", "price": 12345, "power": 2}]),
        received_at=RECEIVED_AT,
        context={"codes": ("usAAPL",)},
    )

    assert page.items[0].provider_classification is None
    assert page.items[0].provider_name is None
    assert page.items[0].provider_reported_delay is None


@pytest.mark.parametrize(
    "updates",
    [
        {"first": 1, "second": 101, "third": "10105"},
        {"first": True, "second": 101, "third": 10105},
        {"first": 1, "second": 101, "third": 10105, "delay": 1},
    ],
)
def test_quotes_reject_partial_or_malformed_provider_classification(
    updates: dict[str, object],
) -> None:
    with pytest.raises(GatewayError) as error:
        normalize_provider_response(
            "fosun_get_quotes",
            _raw([{"mkt": "us", "rawSymbol": "AAPL", "price": 12345, "power": 2, **updates}]),
            received_at=RECEIVED_AT,
            context={"codes": ("usAAPL",)},
        )

    assert error.value.code == "PROVIDER_MALFORMED_RESPONSE"


def test_quotes_preserve_partial_classification_as_unknown_evidence() -> None:
    page = normalize_provider_response(
        "fosun_get_quotes",
        _raw([{"mkt": "us", "rawSymbol": "AAPL", "first": 2}]),
        received_at=RECEIVED_AT,
        context={"codes": ("usAAPL",)},
    )

    classification = page.items[0].provider_classification
    assert classification is not None
    assert (classification.level_1, classification.level_2, classification.level_3) == (
        2,
        None,
        None,
    )


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
