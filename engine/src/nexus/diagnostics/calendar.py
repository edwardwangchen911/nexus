"""Versioned XNYS session authority for point-in-time diagnostics."""

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from importlib.metadata import version

import exchange_calendars as xcals  # type: ignore[import-untyped]

from nexus.diagnostics.calendar_policy import XnysCalendarAdoption
from nexus.diagnostics.models import EvidenceBinding
from nexus.domain.evidence import EvidenceRef

_CALENDAR_NAME = "XNYS"
_MINIMUM_SESSIONS = 253


@dataclass(frozen=True)
class XnysCalendarAuthority:
    sessions: tuple[date, ...]
    binding: EvidenceBinding
    canonical_input: dict[str, object]


def is_xnys_session(session: date) -> bool:
    """Validate a session label against the adopted XNYS calendar package."""
    return bool(xcals.get_calendar(_CALENDAR_NAME).is_session(session.isoformat()))


def completed_xnys_calendar(
    snapshot_as_of: datetime,
    adoption: XnysCalendarAdoption,
    *,
    count: int = _MINIMUM_SESSIONS,
) -> XnysCalendarAuthority:
    """Return the latest XNYS sessions whose actual close is frozen by snapshot time."""
    if snapshot_as_of.tzinfo is None or snapshot_as_of.utcoffset() is None:
        raise ValueError("snapshot_as_of must be timezone-aware")
    if not isinstance(adoption, XnysCalendarAdoption):
        raise TypeError("completed calendar requires typed XNYS calendar adoption")
    if adoption.created_at > snapshot_as_of:
        raise ValueError("calendar adoption is unavailable at snapshot")
    if count < _MINIMUM_SESSIONS:
        raise ValueError("diagnostic calendar requires at least 253 sessions")
    snapshot_utc = snapshot_as_of.astimezone(UTC)
    calendar = xcals.get_calendar(_CALENDAR_NAME)
    labels = calendar.sessions_in_range(
        (snapshot_utc.date() - timedelta(days=count * 2)).isoformat(),
        snapshot_utc.date().isoformat(),
    )
    completed = tuple(
        label.date()
        for label in labels
        if calendar.session_close(label).to_pydatetime() <= snapshot_utc
    )[-count:]
    if len(completed) < count:
        raise ValueError("XNYS calendar history is insufficient")
    package_version = version("exchange-calendars")
    if package_version != adoption.payload.package_version:
        raise ValueError("exchange-calendars runtime version differs from adopted version")
    last_close = calendar.session_close(completed[-1]).to_pydatetime().astimezone(UTC)
    available_at = max(adoption.created_at, last_close)
    session_set_hash = sha256(
        json.dumps(
            tuple(session.isoformat() for session in completed), separators=(",", ":")
        ).encode()
    ).hexdigest()
    calendar_ref = EvidenceRef(
        source="exchange_calendars",
        object_id=f"calendar:{_CALENDAR_NAME}:{package_version}:{session_set_hash}",
        available_at=available_at,
        location=f"package://exchange-calendars/{package_version}/{_CALENDAR_NAME}",
        content_hash=session_set_hash,
    )
    canonical_input: dict[str, object] = {
        "name": _CALENDAR_NAME,
        "package": "exchange-calendars",
        "package_version": package_version,
        "session_set_hash": session_set_hash,
        "sessions": tuple(session.isoformat() for session in completed),
        "adoption": adoption.canonical_input(),
    }
    return XnysCalendarAuthority(
        sessions=completed,
        binding=EvidenceBinding(
            available_at=available_at,
            as_of=last_close,
            session=completed[-1],
            refs=(calendar_ref, adoption.ref),
        ),
        canonical_input=canonical_input,
    )
