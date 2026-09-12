"""Bounded Schwab Level One transcript adapter for intraday snapshots."""

from __future__ import annotations

import argparse
import tempfile
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from nexus.ops.schwab_market_stream import (
    ProviderMarketStreamError,
    pack_provider_market_stream_archive,
    parse_schwab_snapshot_batch,
    run_schwab_stream,
)
from nexus.providers.base import MarketDataProvenance
from nexus.providers.market.alpaca import MarketProviderPayloadError
from nexus.providers.market.schwab_oauth import SchwabOAuthError
from nexus.providers.market.schwab_stream import SchwabStreamError
from nexus.providers.market.snapshot_contracts import (
    IntradaySnapshotRequest,
    RawSnapshotEnvelope,
    SnapshotBatch,
)

Capture = Callable[..., Awaitable[dict[str, Any]]]


class SchwabSnapshotFetchError(RuntimeError):
    """Stable worker-facing acquisition failure without provider details."""

    def __init__(self, result_code: str) -> None:
        super().__init__(result_code)
        self.result_code = result_code


class SchwabSnapshotProvider:
    """Capture once, freeze raw files, and normalize only after archival."""

    def __init__(
        self,
        repo_root: Path,
        client: httpx.AsyncClient,
        *,
        now: Callable[[], datetime] | None = None,
        capture: Capture = run_schwab_stream,
    ) -> None:
        self._repo_root = repo_root
        self._client = client
        self._now = now or (lambda: datetime.now(UTC))
        self._capture = capture

    async def fetch_raw_snapshots(
        self,
        request: IntradaySnapshotRequest,
    ) -> RawSnapshotEnvelope:
        self._validate_request(request)
        checked_at = self._now()
        with tempfile.TemporaryDirectory(prefix="nexus-schwab-") as raw_directory:
            directory = Path(raw_directory)
            arguments = argparse.Namespace(
                repo_root=self._repo_root,
                symbol=[item.symbol for item in request.instruments],
                output_dir=directory,
                duration=15.0,
                max_frames=100,
            )
            try:
                await self._capture(arguments, self._client, now=checked_at)
                raw_payload = pack_provider_market_stream_archive(directory)
            except SchwabOAuthError as error:
                code = str(error)
                result_code = (
                    "INTRADAY_PROVIDER_UNAVAILABLE"
                    if code.startswith("SCHWAB_USER_PREFERENCE_")
                    else "INTRADAY_PROVIDER_AUTH_REQUIRED"
                )
                raise SchwabSnapshotFetchError(result_code) from None
            except (ProviderMarketStreamError, SchwabStreamError) as error:
                result_code = (
                    "INTRADAY_CONTRACT_MISMATCH"
                    if str(error) == "PROVIDER_MARKET_STREAM_ARCHIVE_INVALID"
                    else "INTRADAY_PROVIDER_UNAVAILABLE"
                )
                raise SchwabSnapshotFetchError(result_code) from None
        received_at = self._now()
        return RawSnapshotEnvelope(
            raw_payload=raw_payload,
            received_at=received_at,
            provenance=MarketDataProvenance(
                source="schwab",
                feed="l1",
                adjustment="raw",
                interval="1Min",
                mode="read_only",
                delivery_mode="real_time",
            ),
        )

    def normalize_raw_snapshots(
        self,
        envelope: RawSnapshotEnvelope,
        request: IntradaySnapshotRequest,
    ) -> SnapshotBatch:
        self._validate_request(request)
        if (
            envelope.provenance.source != "schwab"
            or envelope.provenance.feed != "l1"
            or envelope.provenance.adjustment != "raw"
            or envelope.provenance.interval != "1Min"
            or envelope.provenance.mode != "read_only"
            or envelope.provenance.delivery_mode != "real_time"
        ):
            raise MarketProviderPayloadError("SCHWAB_INTRADAY_AUTHORITY_MISMATCH")
        batch = parse_schwab_snapshot_batch(envelope.raw_payload, request)
        if batch.received_at > envelope.received_at:
            raise MarketProviderPayloadError("SCHWAB_MARKET_STREAM_EVIDENCE_INVALID")
        return batch.model_copy(update={"received_at": envelope.received_at})

    @staticmethod
    def _validate_request(request: IntradaySnapshotRequest) -> None:
        if (
            request.feed != "l1"
            or request.delivery_mode != "real_time"
            or request.include_minute_bars
            or request.bucket_start is None
        ):
            raise MarketProviderPayloadError("SCHWAB_INTRADAY_AUTHORITY_MISMATCH")
