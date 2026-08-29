"""Pure provenance admission rules for database-backed intraday facts."""

import pytest
from nexus.market.database_series import _intraday_provenance_allowed


@pytest.mark.parametrize(
    ("source", "feed", "delivery_mode", "expected"),
    (
        ("alpaca", "iex", "real_time", True),
        ("alpaca", "sip", "real_time", True),
        ("alpaca", "delayed_sip", "delayed", True),
        ("alpaca", "iex", "delayed", False),
        ("alpaca", "sip", "delayed", False),
        ("alpaca", "delayed_sip", "real_time", False),
        ("fixture", "fixture", "fixture", True),
        ("fixture", "iex", "fixture", False),
        ("unknown", "iex", "real_time", False),
    ),
)
def test_intraday_provenance_requires_an_explicit_source_feed_delivery_pair(
    source: str,
    feed: str,
    delivery_mode: str,
    expected: bool,
) -> None:
    assert _intraday_provenance_allowed(source, feed, delivery_mode) is expected
