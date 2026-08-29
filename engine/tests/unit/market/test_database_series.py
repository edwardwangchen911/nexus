"""Pure provenance admission rules for database-backed intraday facts."""

import pytest
from nexus.market.database_series import _intraday_provenance_allowed


@pytest.mark.parametrize(
    ("source", "feed", "delivery_mode", "expected"),
    (
        ("fosun", "l1", "unknown", True),
        ("fosun", "l1", "real_time", True),
        ("fosun", "l1", "delayed", True),
        ("fosun", "sip", "real_time", False),
        ("alpaca", "iex", "real_time", False),
        ("alpaca", "delayed_sip", "delayed", False),
        ("fixture", "fixture", "fixture", False),
        ("unknown", "l1", "real_time", False),
    ),
)
def test_intraday_provenance_requires_an_explicit_source_feed_delivery_pair(
    source: str,
    feed: str,
    delivery_mode: str,
    expected: bool,
) -> None:
    assert _intraday_provenance_allowed(source, feed, delivery_mode) is expected
