"""Real corporate-action wire contracts, independent of ledger eligibility."""

from datetime import date
from decimal import Decimal

import pytest
from nexus.providers.corporate_actions import alpaca_http as actions
from pydantic import SecretStr

from tests.support.provider_fakes import RecordingFixedOriginClient


def request() -> actions.CorporateActionsRequest:
    return actions.CorporateActionsRequest("AAPL", date(2026, 1, 1), date(2026, 1, 31))


@pytest.mark.asyncio
async def test_fetch_preserves_unparsed_original_and_current_query() -> None:
    raw = b"not-json-yet\n"
    client = RecordingFixedOriginClient(payload=raw)
    transport = actions.AlpacaCorporateActionsTransport(
        client, key_id=SecretStr("test-key"), secret=SecretStr("test-secret")
    )
    assert await transport.fetch_raw(request(), "next==") == raw
    assert client.calls[0].path_segments == ("v1", "corporate-actions")
    assert client.calls[0].query == (
        ("symbols", "AAPL"),
        ("start", "2026-01-01"),
        ("end", "2026-01-31"),
        ("region", "us"),
        ("data_quality", "all"),
        ("sort", "asc"),
        ("limit", "1000"),
        ("page_token", "next=="),
    )
    assert client.calls[0].header_names == ("APCA-API-KEY-ID", "APCA-API-SECRET-KEY")


def test_current_event_fields_remain_exact_and_unknown_not_eligible() -> None:
    page = actions.parse_corporate_actions_page(
        b'{"corporate_actions":{"cash_dividends":[{"id":"a", "symbol":"AAPL",'
        b'"rate":0.1234567890123456789,"currency":"","ex_date":null,'
        b'"process_date":"2026-01-10","future_field":{"number":1.25}}]},'
        b'"next_page_token":null}'
    )
    event = page.events[0]
    assert event.category == "cash_dividends"
    assert event.fields["rate"] == Decimal("0.1234567890123456789")
    assert event.fields["currency"] == ""
    assert event.fields["ex_date"] is None
    assert event.fields["future_field"] == {"number": Decimal("1.25")}
    assert "status" not in event.fields
    assert "revision" not in event.fields
    assert page.next_cursor is None


def test_unknown_categories_are_retained_and_empty_pages_still_have_cursor() -> None:
    page = actions.parse_corporate_actions_page(
        b'{"corporate_actions":{"future_events":[{"id":"x","foo":"bar"}]},'
        b'"next_page_token":"opaque +/="}'
    )
    assert page.events[0].category == "future_events"
    assert page.events[0].fields == {"id": "x", "foo": "bar"}
    assert page.next_cursor == "opaque +/="
    empty = actions.parse_corporate_actions_page(
        b'{"corporate_actions":{},"next_page_token":"next"}'
    )
    assert empty.events == ()
    assert empty.next_cursor == "next"


@pytest.mark.parametrize(
    "raw",
    [
        b"{}",
        b'{"corporate_actions":{}}',
        b'{"corporate_actions":null,"next_page_token":null}',
        b'{"corporate_actions":{},"next_page_token":""}',
        b'{"corporate_actions":{},"next_page_token":5}',
        b'{"corporate_actions":{"cash_dividends":{}},"next_page_token":null}',
        b'{"corporate_actions":{"cash_dividends":[{}]},"next_page_token":null}',
        b'{"corporate_actions":{},"corporate_actions":{},"next_page_token":null}',
        b'{"corporate_actions":{"cash_dividends":[{"id":"x","rate":NaN}]},"next_page_token":null}',
    ],
)
def test_malformed_response_cannot_be_reported_as_empty_complete_query(raw: bytes) -> None:
    with pytest.raises(ValueError, match="ALPACA_ACTIONS_PAYLOAD_INVALID"):
        actions.parse_corporate_actions_page(raw)


@pytest.mark.parametrize(
    "symbol,start,end",
    [
        ("AAPL/../", date(2026, 1, 1), date(2026, 1, 2)),
        ("", date(2026, 1, 1), date(2026, 1, 2)),
        ("AAPL", date(2026, 1, 2), date(2026, 1, 1)),
        ("AAPL", date(2020, 1, 1), date(2026, 1, 1)),
    ],
)
def test_invalid_request_has_no_credentialed_http_path(symbol: str, start: date, end: date) -> None:
    with pytest.raises(ValueError, match="ALPACA_ACTIONS_REQUEST_INVALID"):
        actions.CorporateActionsRequest(symbol, start, end)


def test_cursor_control_characters_cannot_reach_http() -> None:
    with pytest.raises(ValueError, match="ALPACA_ACTIONS_CURSOR_INVALID"):
        request().query("unsafe\nsecret")


def test_credentials_required_and_representation_does_not_disclose_them() -> None:
    client = RecordingFixedOriginClient(payload=b"{}")
    with pytest.raises(ValueError, match="ALPACA_CREDENTIALS_REQUIRED"):
        actions.AlpacaCorporateActionsTransport(client, key_id=SecretStr(""), secret=SecretStr("x"))
    transport = actions.AlpacaCorporateActionsTransport(
        client, key_id=SecretStr("canary-key"), secret=SecretStr("canary-secret")
    )
    assert "canary" not in repr(transport)
