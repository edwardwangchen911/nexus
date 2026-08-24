from datetime import UTC, date, datetime
from hashlib import sha256

import pytest
from nexus.domain.evidence import EvidenceRef
from nexus.providers.fred import conservative_next_xnys_availability, normalize_alfred_observation
from nexus.research.evidence import EvidencePayloadError, evidence_ref_from_payload

AVAILABLE_AT = datetime(2026, 8, 24, 20, tzinfo=UTC)
CALENDAR_REF = EvidenceRef(
    source="exchange_calendars",
    object_id="calendar:XNYS:fixture",
    available_at=AVAILABLE_AT,
    location=f"package://exchange-calendars/XNYS@sha256:{'c' * 64}",
    content_hash="c" * 64,
)
RAW_REF = EvidenceRef(
    source="fred",
    object_id="DFF:2026-08-07",
    available_at=AVAILABLE_AT,
    location=f"memory://sha256/{'d' * 64}",
    content_hash="d" * 64,
)


def test_evidence_reference_hashes_exact_bytes_and_retains_non_executable_location() -> None:
    payload = b'{"accession":"0000320193-26-000001"}'

    evidence = evidence_ref_from_payload(
        payload,
        source="sec",
        object_id="0000320193-26-000001",
        available_at=AVAILABLE_AT,
        location=f"memory://sha256/{sha256(payload).hexdigest()}",
        expected_sha256=sha256(payload).hexdigest(),
        expected_byte_length=len(payload),
    )

    assert (
        evidence.reference.content_hash
        == "fd390eaeaab2fde622e50ea839554f74b8ab76c27f33f60878c43d462dc86f4a"
    )
    assert evidence.byte_length == len(payload)
    assert evidence.reference.location.startswith("memory://sha256/")


@pytest.mark.parametrize(
    "location",
    [
        "https://sec.example/filing",
        "file:///tmp/filing",
        "javascript:alert(1)",
        "memory://evidence/mutable.json",
        "s3://evidence/filing.json",
    ],
)
def test_evidence_reference_rejects_executable_or_remote_location(location: str) -> None:
    with pytest.raises(EvidencePayloadError, match="NON_EXECUTABLE_LOCATION"):
        evidence_ref_from_payload(
            b"fixture",
            source="sec",
            object_id="filing",
            available_at=AVAILABLE_AT,
            location=location,
            expected_sha256=sha256(b"fixture").hexdigest(),
            expected_byte_length=7,
        )


def test_evidence_reference_rejects_mismatched_payload_hash_before_ref_creation() -> None:
    with pytest.raises(EvidencePayloadError, match="CONTENT_HASH_MISMATCH"):
        evidence_ref_from_payload(
            b"fixture",
            source="fred",
            object_id="DFF",
            available_at=AVAILABLE_AT,
            location=f"memory://sha256/{'0' * 64}",
            expected_sha256="0" * 64,
            expected_byte_length=7,
        )


def test_fred_release_without_time_is_not_available_until_next_xnys_session_open() -> None:
    available_at = conservative_next_xnys_availability(
        release_date=None,
        real_time_start=date(2026, 8, 28),
        next_session_open=lambda _: datetime(2026, 8, 31, 13, 30, tzinfo=UTC),
    )

    assert available_at == datetime(2026, 8, 31, 13, 30, tzinfo=UTC)


def test_alfred_observation_preserves_vintage_and_calendar_evidence() -> None:
    observation = normalize_alfred_observation(
        {
            "date": "2026-08-01",
            "realtime_start": "2026-08-07",
            "realtime_end": "2026-09-03",
            "value": "4.25",
            "revision": "2026-08-07",
        },
        series="DFF",
        unit="percent",
        evidence_ref=RAW_REF,
        calendar_evidence_ref=CALENDAR_REF,
        next_session_open=lambda _: datetime(2026, 8, 10, 13, 30, tzinfo=UTC),
    )

    assert observation.real_time_start == date(2026, 8, 7)
    assert observation.real_time_end == date(2026, 9, 3)
    assert observation.evidence_ref == RAW_REF
    assert observation.calendar_evidence_ref == CALENDAR_REF
