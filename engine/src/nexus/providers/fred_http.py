"""Fixed-series FRED/ALFRED byte transport."""

from __future__ import annotations

from datetime import date
from typing import Protocol
from urllib.parse import parse_qsl, urlsplit

from pydantic import SecretStr

from nexus.providers.fred import FRED_SERIES, FredValidationError

_FRED_PATH = "/fred/series/observations"
_ALFRED_PATH = "/alfred/series/observations"


class FixedOriginBytes(Protocol):
    async def get_bytes(
        self,
        path_segments: tuple[str, ...],
        query: tuple[tuple[str, str], ...],
        headers: tuple[tuple[str, str], ...],
    ) -> bytes: ...


class FredHttpTransport:
    """Translate internal FRED seams to one immutable external endpoint."""

    def __init__(self, client: FixedOriginBytes, api_key: SecretStr) -> None:
        if not api_key.get_secret_value().strip():
            raise ValueError("FRED_API_KEY_REQUIRED")
        self._client = client
        self._api_key = api_key

    async def get(self, path: str) -> bytes:
        parsed = urlsplit(path)
        if parsed.scheme or parsed.netloc or parsed.fragment or parsed.path not in {
            _FRED_PATH,
            _ALFRED_PATH,
        }:
            raise FredValidationError("FRED_PATH_NOT_ALLOWED")
        try:
            pairs = parse_qsl(
                parsed.query,
                keep_blank_values=True,
                strict_parsing=True,
            )
        except ValueError as error:
            raise FredValidationError("FRED_QUERY_INVALID") from error
        values = _unique_query(pairs)
        series = values.get("series_id")
        if series not in FRED_SERIES:
            raise FredValidationError("UNAPPROVED_SERIES")
        if parsed.path == _FRED_PATH:
            if set(values) != {"series_id"}:
                raise FredValidationError("FRED_QUERY_INVALID")
        else:
            if set(values) != {"series_id", "realtime_start", "realtime_end"}:
                raise FredValidationError("INVALID_REAL_TIME_PERIOD")
            _validate_vintage(values["realtime_start"], values["realtime_end"])
        query = {
            "api_key": self._api_key.get_secret_value(),
            "file_type": "json",
            **values,
        }
        return await self._client.get_bytes(
            ("fred", "series", "observations"),
            tuple(sorted(query.items())),
            (),
        )


def _unique_query(pairs: list[tuple[str, str]]) -> dict[str, str]:
    result: dict[str, str] = {}
    for key, value in pairs:
        if key in result or not key or not value:
            raise FredValidationError("FRED_QUERY_INVALID")
        result[key] = value
    return result


def _validate_vintage(start_value: str, end_value: str) -> None:
    try:
        start = date.fromisoformat(start_value)
        end = date.fromisoformat(end_value)
    except ValueError as error:
        raise FredValidationError("INVALID_REAL_TIME_PERIOD") from error
    if end < start:
        raise FredValidationError("INVALID_REAL_TIME_PERIOD")
