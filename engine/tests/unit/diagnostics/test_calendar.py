from datetime import UTC, date, datetime

from nexus.diagnostics.calendar import completed_xnys_calendar
from nexus.diagnostics.evidence import EvidenceResolver
from nexus.domain.evidence import EvidenceRef


def test_completed_xnys_calendar_uses_actual_close_and_published_holidays() -> None:
    adoption = EvidenceRef(
        source="nexus_db",
        object_id="source-policy:test:v1",
        available_at=datetime(2026, 1, 1, tzinfo=UTC),
        location="db://source-policies/test",
        content_hash="a" * 64,
    )

    before_close = completed_xnys_calendar(
        datetime(2026, 8, 21, 19, 59, tzinfo=UTC), adoption
    )
    after_close = completed_xnys_calendar(
        datetime(2026, 8, 21, 20, 1, tzinfo=UTC), adoption
    )

    assert len(before_close.sessions) == 253
    assert before_close.sessions[-1] == date(2026, 8, 20)
    assert after_close.sessions[-1] == date(2026, 8, 21)
    assert date(2026, 7, 3) not in after_close.sessions
    assert all(session.weekday() < 5 for session in after_close.sessions)
    assert after_close.binding.session == date(2026, 8, 21)
    assert after_close.binding.refs[1] == adoption
    assert (
        EvidenceResolver(datetime(2026, 8, 21, 20, 1, tzinfo=UTC))
        .resolve(
            after_close.binding,
            "TRADING_CALENDAR",
            expected_session=date(2026, 8, 21),
        )
        .reason
        is None
    )
    assert after_close.canonical_input["name"] == "XNYS"
    assert after_close.canonical_input["sessions"] == tuple(
        session.isoformat() for session in after_close.sessions
    )
    assert len(str(after_close.canonical_input["session_set_hash"])) == 64
