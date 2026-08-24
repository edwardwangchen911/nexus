from datetime import UTC, datetime

import pytest
from nexus.providers.fred import FRED_SERIES, FredSeriesClient, FredValidationError


class FixtureFredTransport:
    async def get(self, path: str) -> bytes:
        assert path == "/fred/series/observations?series_id=DFF"
        return b'{"observations":[]}'


class FixtureAlfredTransport:
    async def get(self, path: str) -> bytes:
        assert path == (
            "/alfred/series/observations?series_id=DFF&realtime_start=2026-08-01&realtime_end=2026-08-31"
        )
        return b'{"observations":[]}'


@pytest.mark.asyncio
async def test_fred_client_accepts_only_the_eight_authorized_series_from_fixture_transport() -> (
    None
):
    client = FredSeriesClient(FixtureFredTransport(), now=lambda: datetime(2026, 8, 24, tzinfo=UTC))

    document = await client.fetch_series("DFF")

    assert FRED_SERIES == frozenset(
        {"DFF", "DGS2", "DGS10", "CPIAUCSL", "UNRATE", "INDPRO", "VIXCLS", "DEXHKUS"}
    )
    assert document.raw_payload == b'{"observations":[]}'


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

    assert document.raw_payload == b'{"observations":[]}'
