from datetime import UTC, date, datetime

import pytest
from nexus.diagnostics.calendar import completed_xnys_calendar
from nexus.diagnostics.calendar_policy import (
    XNYS_CALENDAR_ADOPTION_ID,
    XNYS_CALENDAR_ADOPTION_KEY,
    XNYS_CALENDAR_ADOPTION_PAYLOAD,
    XNYS_CALENDAR_ADOPTION_VERSION,
    XnysCalendarAdoption,
    validate_xnys_calendar_adoption,
)
from nexus.domain.evidence import EvidenceRef
from nexus.persistence.models.diagnostics import DiagnosticPolicy

SNAPSHOT_AT = datetime(2026, 8, 21, 20, 1, tzinfo=UTC)
ADOPTED_AT = datetime(2026, 8, 20, 12, tzinfo=UTC)


def _record(
    *,
    created_at: datetime = ADOPTED_AT,
    payload: dict[str, object] | None = None,
) -> DiagnosticPolicy:
    return DiagnosticPolicy(
        id=XNYS_CALENDAR_ADOPTION_ID,
        key=XNYS_CALENDAR_ADOPTION_KEY,
        version=XNYS_CALENDAR_ADOPTION_VERSION,
        schema_version="1",
        payload=payload or XNYS_CALENDAR_ADOPTION_PAYLOAD,
        created_at=created_at,
    )


def _adoption() -> XnysCalendarAdoption:
    return validate_xnys_calendar_adoption(_record(), SNAPSHOT_AT)


def test_calendar_requires_typed_db_adoption_and_uses_actual_close_availability() -> None:
    adoption = _adoption()

    authority = completed_xnys_calendar(SNAPSHOT_AT, adoption)

    assert authority.sessions[-1] == date(2026, 8, 21)
    assert authority.binding.available_at == datetime(2026, 8, 21, 20, tzinfo=UTC)
    assert authority.binding.as_of == datetime(2026, 8, 21, 20, tzinfo=UTC)
    assert authority.binding.refs[1] == adoption.ref
    assert authority.canonical_input["adoption"]["row_id"] == str(  # type: ignore[index]
        XNYS_CALENDAR_ADOPTION_ID
    )
    assert authority.canonical_input["adoption"]["payload"] == (  # type: ignore[index]
        XNYS_CALENDAR_ADOPTION_PAYLOAD
    )

    forged_source_policy_ref = EvidenceRef(
        source="nexus_db",
        object_id="source-policy:forged",
        available_at=ADOPTED_AT,
        location="db://source-policies/forged",
        content_hash="f" * 64,
    )
    with pytest.raises(TypeError, match="typed XNYS calendar adoption"):
        completed_xnys_calendar(SNAPSHOT_AT, forged_source_policy_ref)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("record", "snapshot_as_of", "message"),
    (
        (None, SNAPSHOT_AT, "missing"),
        (_record(created_at=SNAPSHOT_AT.replace(minute=2)), SNAPSHOT_AT, "future"),
        (
            _record(payload={**XNYS_CALENDAR_ADOPTION_PAYLOAD, "calendar": "XHKG"}),
            SNAPSHOT_AT,
            "invalid",
        ),
    ),
)
def test_calendar_adoption_rejects_missing_future_and_tampered_records(
    record: DiagnosticPolicy | None,
    snapshot_as_of: datetime,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        validate_xnys_calendar_adoption(record, snapshot_as_of)


def test_calendar_rejects_runtime_package_version_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("nexus.diagnostics.calendar.version", lambda _: "4.13.3")

    with pytest.raises(ValueError, match="runtime version"):
        completed_xnys_calendar(SNAPSHOT_AT, _adoption())
