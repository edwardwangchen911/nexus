"""Current Alpaca action originals; deliberately separate from ledger normalization."""

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date

from pydantic import SecretStr

from nexus.providers.market.alpaca import MarketProviderPayloadError
from nexus.providers.market.alpaca_http import FixedOriginBytes, load_exact_json

_SYMBOL = re.compile(r"[A-Z][A-Z0-9.-]{0,14}")


def _cursor(value: object) -> str:
    # Tokens are opaque query values, never paths, URLs or additional parameters.
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= 2048
        or not value.isascii()
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise ValueError("ALPACA_ACTIONS_CURSOR_INVALID")
    return value


@dataclass(frozen=True)
class CorporateActionsRequest:
    symbol: str
    start: date
    end: date

    def __post_init__(self) -> None:
        if (
            not isinstance(self.symbol, str)
            or _SYMBOL.fullmatch(self.symbol) is None
            or type(self.start) is not date
            or type(self.end) is not date
            or not 0 <= (self.end - self.start).days <= 366
        ):
            raise ValueError("ALPACA_ACTIONS_REQUEST_INVALID")

    def query(self, cursor: str | None = None) -> tuple[tuple[str, str], ...]:
        query = (
            ("symbols", self.symbol),
            ("start", self.start.isoformat()),
            ("end", self.end.isoformat()),
            ("region", "us"),
            ("data_quality", "all"),
            ("sort", "asc"),
            ("limit", "1000"),
        )
        return query if cursor is None else (*query, ("page_token", _cursor(cursor)))


class AlpacaCorporateActionsTransport:
    """Return raw bytes first so callers can archive even malformed responses."""

    def __init__(self, client: FixedOriginBytes, *, key_id: SecretStr, secret: SecretStr) -> None:
        if not key_id.get_secret_value().strip() or not secret.get_secret_value().strip():
            raise ValueError("ALPACA_CREDENTIALS_REQUIRED")
        self._client = client
        self._key_id = key_id
        self._secret = secret

    async def fetch_raw(self, request: CorporateActionsRequest, cursor: str | None = None) -> bytes:
        return await self._client.get_bytes(
            ("v1", "corporate-actions"),
            request.query(cursor),
            (
                ("APCA-API-KEY-ID", self._key_id.get_secret_value()),
                ("APCA-API-SECRET-KEY", self._secret.get_secret_value()),
            ),
        )


@dataclass(frozen=True)
class CorporateActionObservation:
    category: str
    fields: Mapping[str, object]


@dataclass(frozen=True)
class CorporateActionsPage:
    events: tuple[CorporateActionObservation, ...]
    next_cursor: str | None


def parse_corporate_actions_page(raw: bytes) -> CorporateActionsPage:
    """Validate the envelope without inventing processing status or revision.

    Unknown categories and fields survive in the original and observations.
    Missing/empty currency, dates and rates retain their upstream values.
    Only an explicit null token proves this query has exhausted its pages.
    """
    try:
        if not isinstance(raw, bytes) or not 0 < len(raw) <= 8 * 1024 * 1024:
            raise ValueError
        payload = load_exact_json(raw)
        if not isinstance(payload, Mapping) or "next_page_token" not in payload:
            raise ValueError
        buckets = payload.get("corporate_actions")
        if not isinstance(buckets, Mapping):
            raise ValueError
        token = payload["next_page_token"]
        cursor = None if token is None else _cursor(token)
        events: list[CorporateActionObservation] = []
        for category, entries in buckets.items():
            if not isinstance(category, str) or not category or not isinstance(entries, list):
                raise ValueError
            for entry in entries:
                if (
                    not isinstance(entry, Mapping)
                    or not isinstance(entry.get("id"), str)
                    or not 1 <= len(entry["id"]) <= 255
                ):
                    raise ValueError
                events.append(CorporateActionObservation(category, dict(entry)))
                if len(events) > 1000:
                    raise ValueError
        return CorporateActionsPage(tuple(events), cursor)
    except (ValueError, MarketProviderPayloadError, RecursionError) as error:
        raise ValueError("ALPACA_ACTIONS_PAYLOAD_INVALID") from error
