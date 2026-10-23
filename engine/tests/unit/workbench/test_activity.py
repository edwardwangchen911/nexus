"""Object selection never confuses today's activity with historical results."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

from nexus.workbench.activity import ActivityObject, choose_activity

NOW = datetime(2026, 10, 23, 14, tzinfo=UTC)


def obj(n=1, *, today=False, unresolved=False, at=NOW):
    return ActivityObject(identity=UUID(int=n), at=at, today=today, unresolved=unresolved)


def test_historical_failure_does_not_become_today():
    selected, reason = choose_activity([obj(at=NOW - timedelta(days=1))])
    assert reason == "historical"
    assert selected.identity == UUID(int=1)


def test_cross_day_unresolved_precedes_newer_today_completed():
    old = obj(1, unresolved=True, at=NOW - timedelta(days=1))
    selected, reason = choose_activity([obj(2, today=True), old])
    assert selected == old and reason == "historical_unresolved"


def test_multiple_active_objects_are_ambiguous_not_latest_wins():
    selected, reason = choose_activity(
        [obj(1, today=True, unresolved=True), obj(2, unresolved=True)]
    )
    assert selected is None and reason == "ambiguous"


def test_released_unknown_is_history_and_cannot_hide_today():
    selected, reason = choose_activity([obj(1), obj(2, today=True)])
    assert selected.identity == UUID(int=2) and reason == "today"


def test_empty_and_unique_active():
    assert choose_activity([]) == (None, "none")
    selected, reason = choose_activity([obj(today=True, unresolved=True)])
    assert selected.identity == UUID(int=1) and reason == "active"
