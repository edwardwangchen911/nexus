"""Typed provider test doubles that retain no credential values."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from uuid import UUID

from nexus.providers.base import BarsRequest, InstrumentId


@dataclass(frozen=True)
class RecordedGet:
    path_segments: tuple[str, ...]
    query: tuple[tuple[str, str], ...]
    header_names: tuple[str, ...]


class RecordingFixedOriginClient:
    """Return exact bytes and retain only non-secret request structure."""

    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.calls: list[RecordedGet] = []

    async def get_bytes(
        self,
        path_segments: tuple[str, ...],
        query: tuple[tuple[str, str], ...],
        headers: tuple[tuple[str, str], ...],
    ) -> bytes:
        self.calls.append(
            RecordedGet(
                path_segments=path_segments,
                query=query,
                header_names=tuple(name for name, _ in headers),
            )
        )
        return self.payload


class FixedInstrumentSymbolResolver:
    def __init__(self, symbol: str) -> None:
        self._symbol = symbol

    async def resolve(self, instrument_id: InstrumentId, as_of: date) -> str:
        del instrument_id, as_of
        return self._symbol


def canonical_bars_request() -> BarsRequest:
    return BarsRequest(
        instrument_id=InstrumentId(UUID("fd52a392-130e-4ef8-83e2-01ee6f2b175a")),
        start=datetime(2026, 8, 1, tzinfo=UTC),
        end=datetime(2026, 8, 28, tzinfo=UTC),
        interval="1Day",
        adjustment="all",
    )
