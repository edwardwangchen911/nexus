from datetime import UTC, datetime
from decimal import Decimal

import pytest
from nexus.providers.market.alltick_stream import (
    AllTickStreamState,
    build_stock_stream_url,
    redact_stock_stream_url,
)
from nexus.providers.market.alltick_stream import (
    build_heartbeat as build_alltick_heartbeat,
)
from nexus.providers.market.alltick_stream import (
    build_subscriptions as build_alltick_subscriptions,
)
from nexus.providers.market.schwab_stream import (
    SchwabLevelOneState,
    SchwabStreamCredentials,
    build_level_one_subscription,
    build_login,
    validate_streamer_url,
)

RECEIVED_AT = datetime(2026, 9, 9, 14, 30, 1, tzinfo=UTC)


def test_schwab_level_one_merges_change_frames_without_inventing_delay_state() -> None:
    state = SchwabLevelOneState(("AAPL",))

    first = state.apply(
        {
            "data": [
                {
                    "service": "LEVELONE_EQUITIES",
                    "timestamp": 1_789_912_201_000,
                    "command": "SUBS",
                    "content": [
                        {
                            "key": "AAPL",
                            "1": 227.14,
                            "2": 227.18,
                            "3": 227.15,
                            "4": 200,
                            "5": 100,
                            "8": 33_292_396,
                            "9": 50,
                            "34": 1_789_912_201_100,
                            "35": 1_789_912_201_050,
                            "delayed": False,
                            "assetMainType": "EQUITY",
                        }
                    ],
                }
            ]
        },
        received_at=RECEIVED_AT,
    )
    second = state.apply(
        {
            "data": [
                {
                    "service": "LEVELONE_EQUITIES",
                    "timestamp": 1_789_912_202_000,
                    "command": "SUBS",
                    "content": [{"key": "AAPL", "2": 227.19, "34": 1_789_912_202_000}],
                }
            ]
        },
        received_at=RECEIVED_AT,
    )

    assert len(first) == len(second) == 1
    assert first[0].source == "schwab"
    assert first[0].bid == Decimal("227.14")
    assert first[0].last_size == Decimal("50")
    assert second[0].bid == Decimal("227.14")
    assert second[0].ask == Decimal("227.19")
    assert second[0].last == Decimal("227.15")
    assert second[0].delayed is False
    assert second[0].quote_time == datetime.fromtimestamp(1_789_912_202, UTC)
    assert second[0].trade_time == datetime.fromtimestamp(1_789_912_201.05, UTC)


def test_schwab_builders_allow_only_the_fixed_stream_origin_and_read_only_services() -> None:
    credentials = SchwabStreamCredentials(
        access_token="access-token",
        customer_id="customer",
        correlation_id="correlation",
        channel="channel",
        function_id="function",
    )

    assert "access-token" not in repr(credentials)

    assert validate_streamer_url("wss://streamer-api.schwab.com/ws") == (
        "wss://streamer-api.schwab.com/ws"
    )
    with pytest.raises(ValueError, match="SCHWAB_STREAM_URL_INVALID"):
        validate_streamer_url("wss://attacker.invalid/ws")
    login = build_login(credentials, request_id=1)
    subscription = build_level_one_subscription(
        credentials, ("AAPL", "SPY"), request_id=2
    )
    assert login["requests"][0]["service"] == "ADMIN"
    assert login["requests"][0]["command"] == "LOGIN"
    assert login["requests"][0]["parameters"]["Authorization"] == "access-token"
    assert subscription["requests"][0] == {
        "service": "LEVELONE_EQUITIES",
        "command": "SUBS",
        "requestid": "2",
        "SchwabClientCustomerId": "customer",
        "SchwabClientCorrelId": "correlation",
        "parameters": {
            "keys": "AAPL,SPY",
            "fields": "0,1,2,3,4,5,6,7,8,9,13,16,25,34,35,37,38,39,40,41",
        },
    }


def test_alltick_parses_trade_and_quote_without_claiming_a_delay_entitlement() -> None:
    state = AllTickStreamState(("AAPL.US",))
    trade = state.apply(
        {
            "cmd_id": 22998,
            "data": {
                "code": "AAPL.US",
                "seq": 1_605_509_068_000_001,
                "tick_time": 1_789_912_201,
                "price": "227.15",
                "volume": "2",
                "turnover": "454.30",
                "trade_direction": 1,
            },
        },
        received_at=RECEIVED_AT,
    )
    quote = state.apply(
        {
            "cmd_id": 22999,
            "data": {
                "code": "AAPL.US",
                "seq": 1_605_509_068_000_002,
                "tick_time": 1_789_912_201_250,
                "bids": [{"price": "227.14", "volume": "200"}],
                "asks": [{"price": "227.18", "volume": "100"}],
            },
        },
        received_at=RECEIVED_AT,
    )

    assert trade[0].last == Decimal("227.15")
    assert trade[0].provider_time_unit == "seconds"
    assert quote[0].bid == Decimal("227.14")
    assert quote[0].ask == Decimal("227.18")
    assert quote[0].provider_time_unit == "milliseconds"
    assert quote[0].delayed is None


def test_alltick_builders_keep_token_out_of_messages_and_redacted_urls() -> None:
    url = build_stock_stream_url("secret token")
    subscriptions = build_alltick_subscriptions(("AAPL.US", "SPY.US"), sequence=7)

    assert url == "wss://quote.alltick.co/quote-stock-b-ws-api?token=secret%20token"
    assert redact_stock_stream_url(url) == "wss://quote.alltick.co/quote-stock-b-ws-api?token=REDACTED"
    assert all("secret" not in str(message) for message in subscriptions)
    assert [message["cmd_id"] for message in subscriptions] == [22004, 22002]
    assert build_alltick_heartbeat(sequence=9)["cmd_id"] == 22000
