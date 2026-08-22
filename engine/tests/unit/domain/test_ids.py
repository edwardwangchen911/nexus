from datetime import UTC, datetime, timedelta, timezone

import pytest
from nexus.domain.ids import new_uuid7
from nexus.domain.time import require_utc


def test_uuid7_is_time_sortable() -> None:
    """A later generated internal ID must sort after an earlier one."""
    first, second = new_uuid7(), new_uuid7()

    assert first.version == second.version == 7
    assert first.int < second.int


def test_require_utc_rejects_a_naive_timestamp() -> None:
    """Persisting a timestamp without a timezone would make its availability ambiguous."""
    with pytest.raises(ValueError, match="timezone-aware"):
        require_utc(datetime(2026, 8, 22, 9, 30))


def test_require_utc_normalizes_an_aware_timestamp_to_utc() -> None:
    """Equivalent instants must have the canonical UTC storage representation."""
    eastern = timezone(-timedelta(hours=4))

    normalized = require_utc(datetime(2026, 8, 22, 9, 30, tzinfo=eastern))

    assert normalized == datetime(2026, 8, 22, 13, 30, tzinfo=UTC)
