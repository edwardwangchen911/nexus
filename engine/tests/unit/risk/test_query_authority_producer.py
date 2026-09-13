from datetime import UTC, date, datetime

import pytest
from nexus.risk.query_authority_producer import (
    parse_nasdaq_halt_feed,
    parse_nasdaq_listing_directory,
)

DIRECTORY_HEADER = (
    b"Symbol|Security Name|Market Category|Test Issue|Financial Status|"
    b"Round Lot Size|ETF|NextShares\n"
)


def _halt_feed(*items: str, published: str = "Mon, 14 Sep 2026 14:29:00 GMT") -> bytes:
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<rss version="2.0" xmlns:ndaq="http://www.nasdaqtrader.com/">'
        "<channel><title>NASDAQTrader.com</title>"
        f"<pubDate>{published}</pubDate><ndaq:numItems>{len(items)}</ndaq:numItems>"
        f"{''.join(items)}</channel></rss>"
    ).encode()


def _halt(symbol: str) -> str:
    return (
        "<item>"
        f"<title>{symbol}</title><ndaq:HaltDate>09/14/2026</ndaq:HaltDate>"
        f"<ndaq:IssueSymbol>{symbol}</ndaq:IssueSymbol>"
        "<ndaq:ReasonCode>T1</ndaq:ReasonCode>"
        "<ndaq:ResumptionTradeTime />"
        "</item>"
    )


def test_halt_feed_blocks_only_the_exact_requested_symbol() -> None:
    snapshot = parse_nasdaq_halt_feed(
        _halt_feed(_halt("AAPL"), _halt("AAPLW")),
        symbol="AAPL",
        fetched_at=datetime(2026, 9, 14, 14, 30, tzinfo=UTC),
    )

    assert snapshot.blocked is True
    assert snapshot.published_at == datetime(2026, 9, 14, 14, 29, tzinfo=UTC)


def test_halt_feed_rejects_stale_or_internally_incomplete_snapshots() -> None:
    with pytest.raises(ValueError, match="NASDAQ_HALT_SNAPSHOT_INVALID"):
        parse_nasdaq_halt_feed(
            _halt_feed(published="Mon, 14 Sep 2026 14:20:00 GMT"),
            symbol="AAPL",
            fetched_at=datetime(2026, 9, 14, 14, 30, tzinfo=UTC),
        )
    malformed_count = _halt_feed(_halt("MSFT")).replace(b"<ndaq:numItems>1", b"<ndaq:numItems>2")
    with pytest.raises(ValueError, match="NASDAQ_HALT_SNAPSHOT_INVALID"):
        parse_nasdaq_halt_feed(
            malformed_count,
            symbol="AAPL",
            fetched_at=datetime(2026, 9, 14, 14, 30, tzinfo=UTC),
        )


def test_listing_directory_admits_one_normal_current_session_symbol() -> None:
    raw = (
        DIRECTORY_HEADER
        + b"AAPL|Apple Inc. - Common Stock|Q|N|N|40|N|N\n"
        + b"File Creation Time: 0914202610:25|||||||\n"
    )

    snapshot = parse_nasdaq_listing_directory(
        raw,
        symbol="AAPL",
        decision_session=date(2026, 9, 14),
        fetched_at=datetime(2026, 9, 14, 14, 30, tzinfo=UTC),
    )

    assert snapshot.blocked is False
    assert snapshot.file_created_at == datetime(2026, 9, 14, 14, 25, tzinfo=UTC)


@pytest.mark.parametrize(
    "record",
    [
        "",
        "AAPL|Apple Inc. - Common Stock|Q|N|D|40|N|N\n",
        "AAPL|Apple Inc. - Common Stock|Q|Y|N|40|N|N\n",
    ],
)
def test_listing_directory_blocks_absent_non_normal_or_test_symbols(record: str) -> None:
    raw = (
        DIRECTORY_HEADER.decode() + f"{record}" + "File Creation Time: 0914202610:25|||||||\n"
    ).encode()

    snapshot = parse_nasdaq_listing_directory(
        raw,
        symbol="AAPL",
        decision_session=date(2026, 9, 14),
        fetched_at=datetime(2026, 9, 14, 14, 30, tzinfo=UTC),
    )

    assert snapshot.blocked is True


def test_listing_directory_rejects_a_prior_session_file() -> None:
    raw = (
        DIRECTORY_HEADER
        + b"AAPL|Apple Inc. - Common Stock|Q|N|N|40|N|N\n"
        + b"File Creation Time: 0911202621:31|||||||\n"
    )

    with pytest.raises(ValueError, match="NASDAQ_LISTING_SNAPSHOT_INVALID"):
        parse_nasdaq_listing_directory(
            raw,
            symbol="AAPL",
            decision_session=date(2026, 9, 14),
            fetched_at=datetime(2026, 9, 14, 14, 30, tzinfo=UTC),
        )


def test_listing_directory_rejects_a_future_file_timestamp() -> None:
    raw = (
        DIRECTORY_HEADER
        + b"AAPL|Apple Inc. - Common Stock|Q|N|N|40|N|N\n"
        + b"File Creation Time: 0914202610:31|||||||\n"
    )

    with pytest.raises(ValueError, match="NASDAQ_LISTING_SNAPSHOT_INVALID"):
        parse_nasdaq_listing_directory(
            raw,
            symbol="AAPL",
            decision_session=date(2026, 9, 14),
            fetched_at=datetime(2026, 9, 14, 14, 30, tzinfo=UTC),
        )
