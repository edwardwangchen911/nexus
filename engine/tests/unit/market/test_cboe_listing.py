import importlib
from datetime import UTC, datetime

import pytest
from helpers.cboe_listing import CSV, HTML, NOW, PRODUCT


def parse(html=HTML, csv=CSV, product=PRODUCT, now=NOW):
    module = importlib.import_module("nexus.market.cboe_listing")
    return module.parse_cboe_listing(
        listed_html=html, listed_csv=csv, product_html=product, symbol="TEST", now=now
    )


def test_same_complete_directory_and_product_bind_exact_symbol():
    result = parse()
    assert result.symbol == "TEST"
    assert result.directory_as_of == datetime(2026, 10, 11, 0, 0, 4, tzinfo=UTC)
    assert result.product_name == "Synthetic Daily 2x ETF"
    assert result.listed_on.isoformat() == "2026-04-23"


def test_book_viewer_title_is_not_a_second_fund_title():
    result = parse(product=PRODUCT.replace(b"</main>", b"<h1>Book Viewer</h1></main>"))
    assert result.product_name == "Synthetic Daily 2x ETF"


@pytest.mark.parametrize(
    "changes",
    [
        {"csv": CSV + b"TEST,0,0,0.00,0,0.00,0.00,0,0\n"},
        {"csv": CSV.replace(b"TEST,", b"OTHER,")},
        {"html": HTML.replace(b"Listed Symbols", b"Symbols Traded")},
        {"html": HTML.replace(b"symbols/TEST/", b"symbols/OTHER/")},
        {"html": HTML.replace(b"20:00:04 EDT", b"20:00:04 EST")},
        {"html": HTML.replace(b"2026-10-10", b"2026-10-12")},
        {"html": HTML.replace(b"2026-10-10", b"2026-10-09")},
        {"product": PRODUCT.replace(b"Cboe: TEST", b"Cboe: OTHER")},
        {"product": PRODUCT.replace(b"April 23, 2026", b"April 23, 2027")},
        {"html": HTML.replace(b"</table>", b"")},
    ],
)
def test_conflicting_stale_future_or_truncated_sources_reject(changes):
    with pytest.raises(ValueError, match="CBOE_LISTING_INVALID"):
        parse(**changes)


@pytest.mark.parametrize("zone,hour", [("EDT", 5), ("EST", 6)])
def test_fall_back_timestamp_binds_explicit_timezone_fold(zone, hour):
    html = HTML.replace(b"2026-10-10 20:00:04 EDT", f"2026-11-01 01:28:00 {zone}".encode())
    result = parse(html=html, now=datetime(2026, 11, 1, hour, 30, tzinfo=UTC))
    assert result.directory_as_of == datetime(2026, 11, 1, hour, 28, tzinfo=UTC)


@pytest.mark.parametrize("zone", ["EDT", "EST"])
def test_spring_forward_nonexistent_local_time_rejected(zone):
    html = HTML.replace(b"2026-10-10 20:00:04 EDT", f"2026-03-08 02:28:00 {zone}".encode())
    with pytest.raises(ValueError, match="CBOE_LISTING_INVALID"):
        parse(
            html=html,
            product=PRODUCT.replace(b"April 23, 2026", b"January 23, 2026"),
            now=datetime(2026, 3, 8, 8, tzinfo=UTC),
        )
