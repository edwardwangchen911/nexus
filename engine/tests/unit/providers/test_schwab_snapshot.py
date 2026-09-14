import json
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from nexus.providers.base import InstrumentId
from nexus.providers.market.schwab_oauth import SchwabOAuthError
from nexus.providers.market.schwab_snapshot import SchwabSnapshotFetchError, SchwabSnapshotProvider
from nexus.providers.market.snapshot_contracts import IntradaySnapshotRequest, SnapshotInstrument

NOW = datetime(2026, 8, 28, 14, 30, 12, tzinfo=UTC)
AAPL_ID = InstrumentId(UUID("019d0000-0000-7000-8000-0000000000a1"))


def _request() -> IntradaySnapshotRequest:
    return IntradaySnapshotRequest(
        instruments=(SnapshotInstrument(instrument_id=AAPL_ID, symbol="AAPL"),),
        symbol_as_of=date(2026, 8, 28),
        bucket_start=datetime(2026, 8, 28, 14, 30, tzinfo=UTC),
        feed="l1",
        delivery_mode="real_time",
    )


def _write_incomplete_archive(directory: Path) -> None:
    directory.mkdir(mode=0o700, exist_ok=True)
    receipt = {
        "schema": "nexus_provider_market_stream_receipt_v1",
        "source": "schwab",
        "transport": "websocket",
        "origin": "wss://streamer-api.schwab.com/ws",
        "symbols": ["AAPL"],
        "started_at": NOW.isoformat(),
        "ended_at": NOW.isoformat(),
        "authenticated": False,
        "subscription_confirmed": False,
        "status": "failed",
        "stop_reason": "CONNECT_FAILED",
        "frames": [],
        "observation_count": 0,
        "requested_duration_seconds": 15.0,
        "requested_max_frames": 100,
        "elapsed_monotonic_seconds": 0.0,
        "frame_count": 0,
    }
    path = directory / "receipt.json"
    path.write_text(json.dumps(receipt))
    path.chmod(0o600)


@pytest.mark.asyncio
async def test_schwab_provider_collects_once_returns_raw_and_cleans_temp_directory(
    tmp_path,
) -> None:
    calls = []

    async def capture(arguments, client, *, now=None):
        calls.append((arguments, client, now))
        _write_incomplete_archive(arguments.output_dir)
        return {"status": "failed"}

    async with httpx.AsyncClient() as client:
        provider = SchwabSnapshotProvider(
            tmp_path,
            client,
            now=lambda: NOW,
            capture=capture,
        )
        envelope = await provider.fetch_raw_snapshots(_request())

    assert len(calls) == 1
    arguments, _, captured_now = calls[0]
    assert arguments.symbol == ["AAPL"]
    assert arguments.duration == 15.0
    assert arguments.max_frames == 100
    assert arguments.repo_root == tmp_path
    assert arguments.output_dir.name == "capture"
    assert captured_now == NOW
    assert envelope.raw_payload
    assert envelope.received_at == NOW
    assert envelope.provenance.source == "schwab"
    assert not arguments.output_dir.exists()


@pytest.mark.asyncio
async def test_schwab_provider_does_not_retry_collector_failure(tmp_path) -> None:
    calls = 0

    async def capture(*args, **kwargs):
        nonlocal calls
        del args, kwargs
        calls += 1
        raise RuntimeError("private")

    async with httpx.AsyncClient() as client:
        provider = SchwabSnapshotProvider(tmp_path, client, capture=capture)
        with pytest.raises(RuntimeError, match="private"):
            await provider.fetch_raw_snapshots(_request())

    assert calls == 1


@pytest.mark.asyncio
async def test_schwab_provider_maps_expired_token_without_retry(tmp_path) -> None:
    calls = 0

    async def capture(*args, **kwargs):
        nonlocal calls
        del args, kwargs
        calls += 1
        raise SchwabOAuthError("SCHWAB_OAUTH_ACCESS_TOKEN_EXPIRED")

    async with httpx.AsyncClient() as client:
        provider = SchwabSnapshotProvider(tmp_path, client, capture=capture)
        with pytest.raises(
            SchwabSnapshotFetchError,
            match="INTRADAY_PROVIDER_AUTH_REQUIRED",
        ):
            await provider.fetch_raw_snapshots(_request())

    assert calls == 1
