from datetime import UTC, datetime, timedelta

from nexus.paper.independent_history_scope import execution_boundary


def test_after_close_uses_budget_day_or_price_not_past_market_close():
    now = datetime(2026, 11, 6, 22, tzinfo=UTC)
    tomorrow = now.date() + timedelta(days=1)
    opening = datetime(2026, 11, 9, 14, 30, tzinfo=UTC)
    assert execution_boundary(
        now=now,
        market_open_at=opening,
        session_date=opening.date(),
        pricing_expires_at=now + timedelta(days=1),
    ) == datetime.combine(tomorrow, datetime.min.time(), UTC)
    assert execution_boundary(
        now=now,
        market_open_at=opening,
        session_date=opening.date(),
        pricing_expires_at=now + timedelta(minutes=20),
    ) == now + timedelta(minutes=20)


def test_opening_and_original_natural_slot_still_take_priority():
    opening = datetime(2026, 11, 6, 14, 30, tzinfo=UTC)
    for now, expected in [
        (opening - timedelta(hours=2), opening),
        (opening + timedelta(minutes=5), opening + timedelta(minutes=30)),
    ]:
        assert (
            execution_boundary(
                now=now,
                market_open_at=opening,
                session_date=opening.date(),
                pricing_expires_at=now + timedelta(days=1),
            )
            == expected
        )
