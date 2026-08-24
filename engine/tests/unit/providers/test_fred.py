import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from nexus.domain.evidence import EvidenceRef
from nexus.providers.fred import (
    FRED_SERIES,
    FredSeriesClient,
    FredValidationError,
    normalize_alfred_observation,
    select_alfred_observation,
)

FIXTURE = Path(__file__).resolve().parents[2] / "fixtures" / "fred" / "dff.json"
AVAILABLE_AT = datetime(2026, 8, 24, 20, tzinfo=UTC)


class FixtureFredTransport:
    async def get(self, path: str) -> bytes:
        assert path == "/fred/series/observations?series_id=DFF"
        return FIXTURE.read_bytes()


class FixtureAlfredTransport:
    async def get(self, path: str) -> bytes:
        assert path == (
            "/alfred/series/observations?series_id=DFF&realtime_start=2026-08-01&realtime_end=2026-08-31"
        )
        return FIXTURE.read_bytes()


def _evidence(object_id: str, *, source: str = "fred") -> EvidenceRef:
    digest = "d" * 64
    return EvidenceRef(
        source=source,
        object_id=object_id,
        available_at=AVAILABLE_AT,
        location=f"memory://sha256/{digest}",
        content_hash=digest,
    )


@pytest.mark.asyncio
async def test_fred_client_accepts_only_the_eight_authorized_series_from_committed_fixture(
) -> None:
    client = FredSeriesClient(
        FixtureFredTransport(), now=lambda: datetime(2026, 8, 24, tzinfo=UTC)
    )

    document = await client.fetch_series("DFF")

    assert FRED_SERIES == frozenset(
        {"DFF", "DGS2", "DGS10", "CPIAUCSL", "UNRATE", "INDPRO", "VIXCLS", "DEXHKUS"}
    )
    assert document.raw_payload == FIXTURE.read_bytes()


@pytest.mark.asyncio
async def test_fred_client_fails_closed_for_unapproved_series() -> None:
    client = FredSeriesClient(FixtureFredTransport(), now=lambda: datetime(2026, 8, 24, tzinfo=UTC))

    with pytest.raises(FredValidationError, match="UNAPPROVED_SERIES"):
        await client.fetch_series("EURUSD")


@pytest.mark.asyncio
async def test_alfred_client_requests_an_explicit_real_time_vintage_window() -> None:
    client = FredSeriesClient(
        FixtureAlfredTransport(), now=lambda: datetime(2026, 8, 24, tzinfo=UTC)
    )

    document = await client.fetch_alfred_series("DFF", "2026-08-01", "2026-08-31")

    assert document.raw_payload == FIXTURE.read_bytes()


def test_alfred_unknown_release_uses_vintage_date_then_next_actual_xnys_open() -> None:
    raw = json.loads(FIXTURE.read_text())["observations"][0]
    seen_base_dates: list[date] = []

    observation = normalize_alfred_observation(
        raw,
        series="DFF",
        unit="percent",
        evidence_ref=_evidence("DFF:2026-08-07"),
        calendar_evidence_ref=_evidence("calendar:XNYS:fixture", source="exchange_calendars"),
        next_session_open=lambda base_date: (
            seen_base_dates.append(base_date)
            or datetime(2026, 8, 10, 13, 30, tzinfo=UTC)
        ),
    )

    assert seen_base_dates == [date(2026, 8, 7)]
    assert observation.available_at == datetime(2026, 8, 10, 13, 30, tzinfo=UTC)


def test_alfred_known_release_date_overrides_the_economic_observation_date() -> None:
    raw = {**json.loads(FIXTURE.read_text())["observations"][0], "release_date": "2026-08-12"}
    seen_base_dates: list[date] = []

    normalize_alfred_observation(
        raw,
        series="DFF",
        unit="percent",
        evidence_ref=_evidence("DFF:2026-08-07"),
        calendar_evidence_ref=_evidence("calendar:XNYS:fixture", source="exchange_calendars"),
        next_session_open=lambda base_date: (
            seen_base_dates.append(base_date)
            or datetime(2026, 8, 13, 13, 30, tzinfo=UTC)
        ),
    )

    assert seen_base_dates == [date(2026, 8, 12)]


def test_later_alfred_vintage_is_invisible_until_its_derived_availability() -> None:
    raw_observations = json.loads(FIXTURE.read_text())["observations"]
    normalized = tuple(
        normalize_alfred_observation(
            raw,
            series="DFF",
            unit="percent",
            evidence_ref=_evidence(f"DFF:{raw['revision']}"),
            calendar_evidence_ref=_evidence("calendar:XNYS:fixture", source="exchange_calendars"),
            next_session_open=lambda base_date: datetime.combine(
                base_date + timedelta(days=1), datetime.min.time(), UTC
            ),
        )
        for raw in raw_observations
    )

    assert select_alfred_observation(
        normalized, "DFF", datetime(2026, 8, 21, 23, 59, tzinfo=UTC)
    ) == normalized[0]
    assert select_alfred_observation(
        normalized, "DFF", datetime(2026, 8, 22, tzinfo=UTC)
    ) == normalized[1]
