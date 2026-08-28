"""Canonical intraday observation hashing contracts."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from nexus.market.intraday_ingestion import canonical_quote_observation


def test_canonical_quote_observation_is_exact_and_stable() -> None:
    payload = canonical_quote_observation(
        instrument_id=UUID("019d0000-0000-7000-8000-0000000000a1"),
        source="alpaca",
        feed="iex",
        delivery_mode="real_time",
        event_kind="quote",
        bid=Decimal("227.140000000000"),
        ask=Decimal("227.180000000000"),
        last=None,
        observed_at=datetime(2026, 8, 28, 14, 30, 14, tzinfo=UTC),
        available_at=datetime(2026, 8, 28, 14, 30, 15, tzinfo=UTC),
        received_at=datetime(2026, 8, 28, 14, 30, 15, tzinfo=UTC),
        revision="raw-sha256",
    )

    assert payload == (
        b'{"ask":"227.180000000000","available_at":"2026-08-28T14:30:15.000000Z",'
        b'"bid":"227.140000000000","delivery_mode":"real_time",'
        b'"event_kind":"quote","feed":"iex",'
        b'"instrument_id":"019d0000-0000-7000-8000-0000000000a1","last":null,'
        b'"observed_at":"2026-08-28T14:30:14.000000Z","received_at":'
        b'"2026-08-28T14:30:15.000000Z","revision":"raw-sha256","source":"alpaca"}'
    )


def test_canonical_quote_observation_distinguishes_absent_and_exact_decimal() -> None:
    common = {
        "instrument_id": UUID("019d0000-0000-7000-8000-0000000000a1"),
        "source": "alpaca",
        "feed": "iex",
        "delivery_mode": "real_time",
        "event_kind": "trade",
        "bid": None,
        "ask": None,
        "observed_at": datetime(2026, 8, 28, 14, 30, 14, tzinfo=UTC),
        "available_at": datetime(2026, 8, 28, 14, 30, 15, tzinfo=UTC),
        "received_at": datetime(2026, 8, 28, 14, 30, 15, tzinfo=UTC),
        "revision": "1",
    }
    assert canonical_quote_observation(last=Decimal("1.0"), **common) != (
        canonical_quote_observation(last=Decimal("1.00"), **common)
    )
