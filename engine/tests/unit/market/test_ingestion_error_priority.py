"""Object-store stop codes outrank rollback cleanup before database mutation."""

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import cast
from uuid import UUID

import pytest
from nexus.market.ingestion import (
    MarketObjectStore,
    archive_fosun_price_page_raw,
    ingest_price_page,
)
from nexus.market.intraday_ingestion import archive_intraday_snapshot_raw
from nexus.market.object_store import ProviderObjectIOUnavailable
from nexus.providers.base import MarketDataProvenance, PriceBar, ProviderPage
from nexus.providers.market.fosun_gateway import RawGatewayResponse
from nexus.providers.market.snapshot_contracts import RawSnapshotEnvelope
from sqlalchemy.ext.asyncio import AsyncSession

NOW = datetime(2026, 8, 28, 14, 30, 31, tzinfo=UTC)
INSTRUMENT_ID = UUID("019d0000-0000-7000-8000-0000000000a1")


class _RollbackBombSession:
    def __init__(self) -> None:
        self.rollback_calls = 0

    async def rollback(self) -> None:
        self.rollback_calls += 1
        raise AssertionError("object I/O failure must not be masked by rollback")


class _UnavailableStore:
    async def archive_raw(self, key: str, payload: bytes) -> str:
        del key, payload
        raise ProviderObjectIOUnavailable("OBJECT_IO_DEADLINE_EXCEEDED")


def _store() -> MarketObjectStore:
    return cast(MarketObjectStore, _UnavailableStore())


@pytest.mark.asyncio
async def test_fosun_raw_object_failure_does_not_attempt_rollback() -> None:
    session = _RollbackBombSession()
    response = RawGatewayResponse(
        operation="fosun_get_bars",
        gateway_request_id=UUID("019d0000-0000-7000-8000-0000000000f1"),
        request_body_sha256="a" * 64,
        raw_payload=b'{"bars":[]}',
        sent_at=NOW - timedelta(milliseconds=100),
        received_at=NOW,
    )

    with pytest.raises(ProviderObjectIOUnavailable, match="OBJECT_IO_DEADLINE_EXCEEDED"):
        await archive_fosun_price_page_raw(
            cast(AsyncSession, session),
            _store(),
            response,
            commit=False,
        )

    assert session.rollback_calls == 0


@pytest.mark.asyncio
async def test_daily_page_object_failure_does_not_attempt_rollback() -> None:
    session = _RollbackBombSession()
    bar = PriceBar(
        instrument_id=INSTRUMENT_ID,
        session=date(2026, 8, 28),
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=Decimal("1000"),
        observed_at=NOW - timedelta(hours=1),
        available_at=NOW - timedelta(minutes=1),
        received_at=NOW,
        revision="fixture-r1",
    )
    page = ProviderPage(
        items=(bar,),
        next_cursor=None,
        raw_payload=b'{"bars":[{"fixture":true}]}',
        source_request_id="fixture-request",
        as_of=NOW,
        provenance=MarketDataProvenance(
            source="fixture",
            feed="synthetic",
            adjustment="all",
            interval="1Day",
            mode="fixture",
            delivery_mode="fixture",
        ),
    )

    with pytest.raises(ProviderObjectIOUnavailable, match="OBJECT_IO_DEADLINE_EXCEEDED"):
        await ingest_price_page(
            cast(AsyncSession, session),
            _store(),
            page,
            commit=False,
        )

    assert session.rollback_calls == 0


@pytest.mark.asyncio
async def test_intraday_raw_object_failure_does_not_attempt_rollback() -> None:
    session = _RollbackBombSession()
    envelope = RawSnapshotEnvelope(
        raw_payload=b'{"AAPL":{}}',
        received_at=NOW,
        provenance=MarketDataProvenance(
            source="fosun",
            feed="l1",
            adjustment="raw",
            interval="1Min",
            mode="read_only",
            delivery_mode="unknown",
        ),
    )

    with pytest.raises(ProviderObjectIOUnavailable, match="OBJECT_IO_DEADLINE_EXCEEDED"):
        await archive_intraday_snapshot_raw(
            cast(AsyncSession, session),
            _store(),
            envelope,
            commit=False,
        )

    assert session.rollback_calls == 0
