"""Closed intraday collection handler result mapping."""

from dataclasses import dataclass
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import cast
from uuid import UUID

import pytest
from nexus.jobs.intraday_collection import build_intraday_collection_registry
from nexus.jobs.intraday_schedule import bucket_for, build_intraday_payload
from nexus.jobs.models import JobType
from nexus.jobs.registry import JobHandlerContext
from nexus.jobs.repository import ClaimedJob
from nexus.providers.base import InstrumentId, MarketDataProvenance
from nexus.providers.market.snapshot_contracts import MarketSnapshot, SnapshotBatch

NOW = datetime(2026, 8, 28, 14, 30, 31, tzinfo=UTC)
INSTRUMENT_ID = UUID("019d0000-0000-7000-8000-0000000000a1")
CALENDAR_ID = UUID("019d0000-0000-7000-8000-0000000000c1")


@dataclass
class _Provider:
    calls: int = 0

    async def fetch_snapshots(self, request: object) -> SnapshotBatch:
        self.calls += 1
        return SnapshotBatch(
            snapshots=(
                MarketSnapshot(
                    instrument_id=InstrumentId(INSTRUMENT_ID),
                    symbol="AAPL",
                    trade=None,
                    quote=None,
                    minute_bar=None,
                ),
            ),
            raw_payload=b'{"AAPL":{}}',
            received_at=NOW,
            provenance=MarketDataProvenance(
                source="alpaca",
                feed="iex",
                adjustment="raw",
                interval="1Min",
                mode="read_only",
                delivery_mode="real_time",
            ),
        )


def _context(payload: object, *, now: datetime = NOW) -> JobHandlerContext:
    claim = cast(
        ClaimedJob,
        SimpleNamespace(
            effective_at=bucket_for(NOW).start,
        ),
    )
    return JobHandlerContext(
        session=cast(object, SimpleNamespace()),
        claim=claim,
        payload=cast(object, payload),
        _clock=lambda: now,
    )


def _payload() -> object:
    return build_intraday_payload(
        calendar_authority_id=CALENDAR_ID,
        session="2026-08-28",
        bucket=bucket_for(NOW),
        instruments=((INSTRUMENT_ID, "AAPL"),),
        feed="iex",
        delivery_mode="real_time",
    )


@pytest.mark.asyncio
async def test_collection_maps_missing_snapshot_to_partial_without_writes() -> None:
    provider = _Provider()

    async def forbidden_ingest(*_: object, **__: object) -> object:
        pytest.fail("ingestion called for an empty snapshot")

    registry = build_intraday_collection_registry(
        lambda _session: provider,
        cast(object, SimpleNamespace()),
        lambda: NOW,
        ingest=forbidden_ingest,
    )
    spec = registry.resolve(JobType.COLLECT_INTRADAY_SNAPSHOT.value)
    assert spec is not None

    result = await spec.handler(_context(_payload()))

    assert result.outcome == "skipped"
    assert result.result_code == "INTRADAY_SNAPSHOT_PARTIAL"
    assert provider.calls == 1


@pytest.mark.asyncio
async def test_expired_bucket_stops_before_provider_construction() -> None:
    constructions = 0

    def factory(_session: object) -> _Provider:
        nonlocal constructions
        constructions += 1
        return _Provider()

    registry = build_intraday_collection_registry(
        factory,
        cast(object, SimpleNamespace()),
        lambda: NOW,
    )
    spec = registry.resolve(JobType.COLLECT_INTRADAY_SNAPSHOT.value)
    assert spec is not None
    expired = datetime(2026, 8, 28, 14, 31, 31, tzinfo=UTC)

    result = await spec.handler(_context(_payload(), now=expired))

    assert result.outcome == "skipped"
    assert result.result_code == "INTRADAY_BUCKET_EXPIRED"
    assert constructions == 0
