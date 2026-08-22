from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from nexus.providers.base import PriceBar
from nexus.research.features.price import momentum_126_skip5, point_in_time_closes


def test_skip_five_momentum_uses_fixed_endpoints() -> None:
    closes = tuple(Decimal(index) for index in range(1, 128))
    assert momentum_126_skip5(closes) == Decimal(122) / Decimal(1) - Decimal(1)


def test_skip_five_momentum_requires_history_and_nonzero_denominator() -> None:
    assert momentum_126_skip5(tuple(Decimal(index) for index in range(1, 127))) is None
    assert momentum_126_skip5((Decimal("0"),) + (Decimal("1"),) * 126) is None


def test_point_in_time_closes_excludes_rows_not_yet_available() -> None:
    decision_time = datetime(2026, 8, 21, 22, tzinfo=UTC)
    instrument_id = UUID("29e18e7a-78fb-5fc1-a65a-6641fbc1cc64")

    def bar(session_day: int, close: str, available_at: datetime) -> PriceBar:
        value = Decimal(close)
        return PriceBar(
            instrument_id=instrument_id,
            session=date(2026, 8, session_day),
            open=value,
            high=value,
            low=value,
            close=value,
            volume=Decimal("1"),
            available_at=available_at,
        )

    closes = point_in_time_closes(
        (
            bar(21, "21", decision_time + timedelta(microseconds=1)),
            bar(20, "20", decision_time),
            bar(19, "19", decision_time - timedelta(days=1)),
        ),
        decision_time,
    )

    assert closes == (Decimal("19"), Decimal("20"))


def test_point_in_time_closes_uses_latest_available_revision_per_session() -> None:
    decision_time = datetime(2026, 8, 21, 22, tzinfo=UTC)
    instrument_id = UUID("29e18e7a-78fb-5fc1-a65a-6641fbc1cc64")

    def revision(close: str, available_at: datetime, revision_id: str) -> PriceBar:
        value = Decimal(close)
        return PriceBar(
            instrument_id=instrument_id,
            session=date(2026, 8, 20),
            open=value,
            high=value,
            low=value,
            close=value,
            volume=Decimal("1"),
            available_at=available_at,
            revision=revision_id,
        )

    closes = point_in_time_closes(
        (
            revision("19", decision_time - timedelta(hours=2), "r1"),
            revision("20", decision_time - timedelta(hours=1), "r2"),
            revision("21", decision_time + timedelta(hours=1), "r3"),
        ),
        decision_time,
    )

    assert closes == (Decimal("20"),)
