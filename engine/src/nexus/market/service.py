"""Point-in-time market reads and deterministic private watchlist set operations."""

import base64
import binascii
import hashlib
import hmac
import json
from collections import defaultdict
from collections.abc import Sequence
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Literal, Protocol, cast
from uuid import UUID

from sqlalchemy import Row, and_, delete, func, or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.api.errors import ErrorCode, NexusError
from nexus.api.schemas.market import (
    MarketBarResponse,
    MarketItemResponse,
    MarketPageResponse,
    SelectedMarketSeriesResponse,
    WatchlistMembershipResponse,
)
from nexus.domain.decimal import decimal_to_string
from nexus.domain.time import require_utc
from nexus.persistence.models.market import (
    Instrument,
    InstrumentAlias,
    LatestQuote,
    Watchlist,
    WatchlistMember,
)
from nexus.providers.base import PriceSeries
from nexus.quality.market import xnys_session_window
from nexus.quality.source_policy import (
    SOURCE_POLICY_PAYLOAD,
    load_fosun_market_admission,
    load_source_policy_v1,
    select_price_series,
)

_DEFAULT_WATCHLIST_ID = UUID("8c7cc45f-9420-5ea5-a75e-7df8a8a63b58")
_DEFAULT_WATCHLIST_NAME = "default_private"
_CURSOR_SCOPE = "nexus.market.v1"
_CURSOR_MAX_LENGTH = 2048
_CURSOR_MAX_AGE = timedelta(minutes=5)
_STALE_AFTER = timedelta(seconds=45)
_SELECTED_SERIES_BAR_LIMIT = 500


class PriceSeriesLoader(Protocol):
    """Offline composition seam; it carries data candidates, never policy authority."""

    async def __call__(
        self, instrument_id: UUID, decision_time: datetime
    ) -> Sequence[PriceSeries]: ...


class MarketService:
    """Repository owner for market evidence and watchlist set semantics."""

    def __init__(
        self,
        session: AsyncSession,
        decision_time: datetime,
        *,
        cursor_signing_secrets: Sequence[str],
        price_series_loader: PriceSeriesLoader | None = None,
    ) -> None:
        self._session = session
        self._request_time = require_utc(decision_time)
        self._cursor_secrets = tuple(secret for secret in cursor_signing_secrets if secret)
        if not self._cursor_secrets:
            raise ValueError("at least one cursor signing secret is required")
        self._price_series_loader = price_series_loader

    async def market_page(
        self,
        *,
        limit: int,
        cursor: str | None,
        watchlist: bool | None,
        selected_instrument_id: UUID | None,
    ) -> tuple[MarketPageResponse, datetime]:
        """Return one point-in-time page and the exact evidence decision time."""
        cursor_state = (
            self._decode_cursor(cursor, watchlist) if cursor is not None else None
        )
        decision_time = cursor_state[0] if cursor_state is not None else self._request_time
        after_symbol = cursor_state[1] if cursor_state is not None else None
        after_instrument_id = cursor_state[2] if cursor_state is not None else None
        rows = await self._instrument_rows(
            decision_time=decision_time,
            limit=limit + 1,
            after_symbol=after_symbol,
            after_instrument_id=after_instrument_id,
            watchlist=watchlist,
        )
        has_more = len(rows) > limit
        page_rows = rows[:limit]
        items = await self._market_items(page_rows, decision_time)
        next_cursor = None
        if has_more and page_rows:
            last_instrument, last_symbol = page_rows[-1]
            next_cursor = self._encode_cursor(
                decision_time,
                last_symbol,
                last_instrument.id,
                watchlist=watchlist,
            )
        selected_series = await self._selected_series(
            selected_instrument_id, decision_time
        )
        return (
            MarketPageResponse(
                items=tuple(items),
                next_cursor=next_cursor,
                selected_series=selected_series,
            ),
            decision_time,
        )

    async def set_watchlisted(
        self, instrument_id: UUID, *, watchlisted: bool
    ) -> WatchlistMembershipResponse:
        """Apply idempotent set semantics with PostgreSQL uniqueness as authority."""
        await self._require_instrument(instrument_id)
        if watchlisted:
            watchlist_id = await self._ensure_default_watchlist()
            await self._session.execute(
                insert(WatchlistMember)
                .values(
                    watchlist_id=watchlist_id,
                    instrument_id=instrument_id,
                )
                .on_conflict_do_nothing(
                    index_elements=[
                        WatchlistMember.watchlist_id,
                        WatchlistMember.instrument_id,
                    ]
                )
            )
        else:
            existing_watchlist_id = await self._default_watchlist_id()
            if existing_watchlist_id is not None:
                await self._session.execute(
                    delete(WatchlistMember).where(
                        WatchlistMember.watchlist_id == existing_watchlist_id,
                        WatchlistMember.instrument_id == instrument_id,
                    )
                )
        return WatchlistMembershipResponse(
            instrument_id=instrument_id,
            watchlisted=watchlisted,
        )

    async def _instrument_rows(
        self,
        *,
        decision_time: datetime,
        limit: int,
        after_symbol: str | None,
        after_instrument_id: UUID | None,
        watchlist: bool | None,
    ) -> list[Row[tuple[Instrument, str]]]:
        aliases = (
            select(
                InstrumentAlias.instrument_id.label("instrument_id"),
                InstrumentAlias.symbol.label("symbol"),
                func.row_number()
                .over(
                    partition_by=InstrumentAlias.instrument_id,
                    order_by=(
                        InstrumentAlias.valid_from.desc(),
                        InstrumentAlias.id.desc(),
                    ),
                )
                .label("alias_rank"),
            )
            .where(
                InstrumentAlias.alias_type == "ticker",
                InstrumentAlias.valid_from <= decision_time.date(),
                or_(
                    InstrumentAlias.valid_to.is_(None),
                    InstrumentAlias.valid_to >= decision_time.date(),
                ),
            )
            .subquery()
        )
        statement = (
            select(Instrument, aliases.c.symbol)
            .join(aliases, aliases.c.instrument_id == Instrument.id)
            .where(aliases.c.alias_rank == 1)
            .order_by(aliases.c.symbol.asc(), Instrument.id.asc())
            .limit(limit)
        )
        if after_symbol is not None and after_instrument_id is not None:
            statement = statement.where(
                or_(
                    aliases.c.symbol > after_symbol,
                    and_(
                        aliases.c.symbol == after_symbol,
                        Instrument.id > after_instrument_id,
                    ),
                )
            )
        if watchlist is not None:
            membership_exists = (
                select(WatchlistMember.id)
                .join(Watchlist, Watchlist.id == WatchlistMember.watchlist_id)
                .where(
                    Watchlist.name == _DEFAULT_WATCHLIST_NAME,
                    WatchlistMember.instrument_id == Instrument.id,
                )
                .exists()
            )
            statement = statement.where(
                membership_exists if watchlist else ~membership_exists
            )
        return list((await self._session.execute(statement)).all())

    async def _market_items(
        self,
        rows: Sequence[Row[tuple[Instrument, str]]],
        decision_time: datetime,
    ) -> list[MarketItemResponse]:
        if not rows:
            return []
        instruments = [row[0] for row in rows]
        instrument_ids = [instrument.id for instrument in instruments]
        quotes = list(
            await self._session.scalars(
                select(LatestQuote).where(
                    LatestQuote.instrument_id.in_(instrument_ids),
                    LatestQuote.available_at <= decision_time,
                    LatestQuote.observed_at <= decision_time,
                )
            )
        )
        quotes_by_instrument: dict[UUID, list[LatestQuote]] = defaultdict(list)
        for quote in quotes:
            quotes_by_instrument[quote.instrument_id].append(quote)
        watchlisted = await self._watchlisted_ids(instrument_ids)
        try:
            await load_source_policy_v1(self._session, instrument_tick_size=None)
        except ValueError:
            return [
                self._unavailable_item(
                    instrument,
                    symbol,
                    watchlisted=instrument.id in watchlisted,
                    reason="SOURCE_POLICY_UNAVAILABLE",
                )
                for instrument, symbol in rows
            ]
        fosun_admission = None
        if any(quote.source == "fosun" for quote in quotes):
            fosun_admission = await load_fosun_market_admission(
                self._session,
                decision_time=decision_time,
                operation="fosun_get_bars",
                market_scope="US",
            )
        return [
            self._quote_item(
                instrument,
                symbol,
                quotes_by_instrument[instrument.id],
                watchlisted=instrument.id in watchlisted,
                decision_time=decision_time,
                fosun_admitted=fosun_admission is not None,
            )
            for instrument, symbol in rows
        ]

    def _quote_item(
        self,
        instrument: Instrument,
        symbol: str,
        quotes: Sequence[LatestQuote],
        *,
        watchlisted: bool,
        decision_time: datetime,
        fosun_admitted: bool,
    ) -> MarketItemResponse:
        priority, alpaca_feed = _published_quote_policy()
        eligible = [
            quote
            for quote in quotes
            if quote.source in priority
            and (quote.source != "fosun" or fosun_admitted)
            and (quote.source != "alpaca" or quote.feed == alpaca_feed)
        ]
        if not eligible:
            reason = "NO_ELIGIBLE_SOURCE" if quotes else "QUOTE_UNAVAILABLE"
            return self._unavailable_item(
                instrument, symbol, watchlisted=watchlisted, reason=reason
            )
        eligible.sort(key=lambda quote: _quote_sort_key(quote, priority))
        selected = eligible[0]
        reasons: set[str] = set()
        quality: Literal["PASS", "PARTIAL", "FAIL"] = "PASS"
        cross_source = _quote_cross_check(eligible, instrument.tick_size)
        if cross_source == "SOURCE_CONFLICT":
            return self._unavailable_item(
                instrument,
                symbol,
                watchlisted=watchlisted,
                reason="SOURCE_CONFLICT",
            )
        if cross_source is not None:
            reasons.add(cross_source)
            quality = "PARTIAL"
        if selected.quality_flags:
            reasons.add("QUOTE_QUALITY_FLAGS")
            quality = "PARTIAL"
        if selected.last is None:
            reasons.add("QUOTE_PRICE_UNAVAILABLE")
            quality = "FAIL"
        session = xnys_session_window(decision_time.date())
        if (
            session is not None
            and session.open_at <= decision_time <= session.close_at
            and decision_time - selected.observed_at > _STALE_AFTER
        ):
            reasons.add("SELECTED_SOURCE_STALE")
            if quality == "PASS":
                quality = "PARTIAL"
        return MarketItemResponse(
            instrument_id=instrument.id,
            symbol=symbol,
            price=(
                decimal_to_string(selected.last) if selected.last is not None else None
            ),
            source=selected.source,
            feed=selected.feed,
            as_of=selected.observed_at,
            quality=quality,
            quality_reasons=_ordered_reasons(reasons),
            watchlisted=watchlisted,
        )

    async def _selected_series(
        self, instrument_id: UUID | None, decision_time: datetime
    ) -> SelectedMarketSeriesResponse | None:
        if instrument_id is None:
            return None
        instrument = await self._active_instrument(instrument_id, decision_time)
        if instrument is None:
            raise _instrument_not_found(instrument_id)
        if self._price_series_loader is None:
            return _empty_series(instrument_id, "NO_HISTORY_DATA")
        candidates = tuple(
            await self._price_series_loader(instrument_id, decision_time)
        )
        if not candidates:
            return _empty_series(instrument_id, "NO_HISTORY_DATA")
        if any(
            bar.instrument_id != instrument_id
            for candidate in candidates
            for bar in candidate.bars
        ):
            return _empty_series(instrument_id, "HISTORY_INSTRUMENT_MISMATCH")
        selection = await select_price_series(
            self._session,
            candidates,
            instrument.tick_size,
            decision_time,
        )
        selected = selection.selected
        if selected is None:
            return SelectedMarketSeriesResponse(
                instrument_id=instrument_id,
                source=None,
                feed=None,
                adjustment=None,
                interval=None,
                quality=selection.quality,
                quality_reasons=selection.reasons,
                bars=(),
            )
        bars = selected.bars[-_SELECTED_SERIES_BAR_LIMIT:]
        quality: Literal["PASS", "PARTIAL", "FAIL"] = selection.quality
        reasons = list(selection.reasons)
        if len(selected.bars) > _SELECTED_SERIES_BAR_LIMIT:
            reasons.append("HISTORY_TRUNCATED")
            if quality == "PASS":
                quality = "PARTIAL"
        return SelectedMarketSeriesResponse(
            instrument_id=instrument_id,
            source=selected.source,
            feed=selected.feed,
            adjustment=selected.adjustment,
            interval=selected.interval,
            quality=quality,
            quality_reasons=tuple(reasons),
            bars=tuple(
                MarketBarResponse(
                    session=bar.session,
                    open=decimal_to_string(bar.open),
                    high=decimal_to_string(bar.high),
                    low=decimal_to_string(bar.low),
                    close=decimal_to_string(bar.close),
                    volume=decimal_to_string(bar.volume),
                    available_at=bar.available_at,
                    observed_at=bar.observed_at,
                    quality_flags=bar.quality_flags,
                )
                for bar in bars
            ),
        )

    async def _active_instrument(
        self, instrument_id: UUID, decision_time: datetime
    ) -> Instrument | None:
        return cast(
            Instrument | None,
            await self._session.scalar(
                select(Instrument)
                .join(InstrumentAlias, InstrumentAlias.instrument_id == Instrument.id)
                .where(
                    Instrument.id == instrument_id,
                    InstrumentAlias.alias_type == "ticker",
                    InstrumentAlias.valid_from <= decision_time.date(),
                    or_(
                        InstrumentAlias.valid_to.is_(None),
                        InstrumentAlias.valid_to >= decision_time.date(),
                    ),
                )
                .limit(1)
            ),
        )

    async def _require_instrument(self, instrument_id: UUID) -> None:
        exists = await self._session.scalar(
            select(Instrument.id).where(Instrument.id == instrument_id)
        )
        if exists is None:
            raise _instrument_not_found(instrument_id)

    async def _default_watchlist_id(self) -> UUID | None:
        return cast(
            UUID | None,
            await self._session.scalar(
                select(Watchlist.id).where(Watchlist.name == _DEFAULT_WATCHLIST_NAME)
            ),
        )

    async def _ensure_default_watchlist(self) -> UUID:
        await self._session.execute(
            insert(Watchlist)
            .values(id=_DEFAULT_WATCHLIST_ID, name=_DEFAULT_WATCHLIST_NAME)
            .on_conflict_do_nothing(index_elements=[Watchlist.name])
        )
        watchlist_id = await self._default_watchlist_id()
        if watchlist_id is None:
            raise RuntimeError("default watchlist was not created")
        return watchlist_id

    async def _watchlisted_ids(self, instrument_ids: Sequence[UUID]) -> set[UUID]:
        watchlist_id = await self._default_watchlist_id()
        if watchlist_id is None:
            return set()
        return set(
            await self._session.scalars(
                select(WatchlistMember.instrument_id).where(
                    WatchlistMember.watchlist_id == watchlist_id,
                    WatchlistMember.instrument_id.in_(instrument_ids),
                )
            )
        )

    def _unavailable_item(
        self,
        instrument: Instrument,
        symbol: str,
        *,
        watchlisted: bool,
        reason: str,
    ) -> MarketItemResponse:
        return MarketItemResponse(
            instrument_id=instrument.id,
            symbol=symbol,
            price=None,
            source=None,
            feed=None,
            as_of=None,
            quality="FAIL",
            quality_reasons=(reason,),
            watchlisted=watchlisted,
        )

    def _encode_cursor(
        self,
        decision_time: datetime,
        symbol: str,
        instrument_id: UUID,
        *,
        watchlist: bool | None,
    ) -> str:
        payload = json.dumps(
            {
                "decision_time": decision_time.isoformat().replace("+00:00", "Z"),
                "instrument_id": str(instrument_id),
                "scope": _CURSOR_SCOPE,
                "symbol": symbol,
                "version": 1,
                "watchlist": watchlist,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        signature = hmac.new(
            self._cursor_secrets[0].encode("utf-8"),
            b"nexus-market-cursor-v1\0" + payload,
            hashlib.sha256,
        ).digest()
        cursor = f"{_b64encode(payload)}.{_b64encode(signature)}"
        if len(cursor) > _CURSOR_MAX_LENGTH:
            raise RuntimeError("market cursor exceeded its fixed bound")
        return cursor

    def _decode_cursor(
        self, cursor: str, requested_watchlist: bool | None
    ) -> tuple[datetime, str, UUID]:
        try:
            if not 1 <= len(cursor) <= _CURSOR_MAX_LENGTH:
                raise ValueError
            payload_part, signature_part = cursor.split(".", 1)
            payload = _b64decode(payload_part)
            signature = _b64decode(signature_part)
            if len(signature) != hashlib.sha256().digest_size:
                raise ValueError
            if not any(
                hmac.compare_digest(
                    signature,
                    hmac.new(
                        secret.encode("utf-8"),
                        b"nexus-market-cursor-v1\0" + payload,
                        hashlib.sha256,
                    ).digest(),
                )
                for secret in self._cursor_secrets
            ):
                raise ValueError
            decoded = json.loads(payload)
            if not isinstance(decoded, dict) or set(decoded) != {
                "decision_time",
                "instrument_id",
                "scope",
                "symbol",
                "version",
                "watchlist",
            }:
                raise ValueError
            if decoded["scope"] != _CURSOR_SCOPE or decoded["version"] != 1:
                raise ValueError
            cursor_watchlist = decoded["watchlist"]
            if cursor_watchlist is not None and type(cursor_watchlist) is not bool:
                raise ValueError
            if cursor_watchlist is not requested_watchlist:
                raise ValueError
            decision_time = require_utc(
                datetime.fromisoformat(
                    cast(str, decoded["decision_time"]).replace("Z", "+00:00")
                )
            )
            symbol = decoded["symbol"]
            if not isinstance(symbol, str) or not 1 <= len(symbol) <= 32:
                raise ValueError
            instrument_id = UUID(cast(str, decoded["instrument_id"]))
            age = self._request_time - decision_time
            if age < timedelta(0) or age > _CURSOR_MAX_AGE:
                raise ValueError
            return decision_time, symbol, instrument_id
        except (ValueError, TypeError, KeyError, json.JSONDecodeError, binascii.Error):
            raise NexusError(
                ErrorCode.VALIDATION_ERROR,
                "The request could not be completed.",
                status_code=422,
            ) from None


def _published_quote_policy() -> tuple[tuple[str, ...], str]:
    priority = SOURCE_POLICY_PAYLOAD.get("priority")
    alpaca = SOURCE_POLICY_PAYLOAD.get("alpaca")
    if (
        not isinstance(priority, list)
        or not all(isinstance(source, str) for source in priority)
        or not isinstance(alpaca, dict)
        or not isinstance(alpaca.get("feed"), str)
    ):
        raise RuntimeError("published quote source policy is invalid")
    return tuple(priority), cast(str, alpaca["feed"])


def _quote_sort_key(quote: LatestQuote, priority: Sequence[str]) -> tuple[object, ...]:
    return (
        priority.index(quote.source),
        0 if quote.last is not None else 1,
        0 if not quote.quality_flags else 1,
        -quote.available_at.timestamp(),
        -quote.received_at.timestamp(),
        -quote.observed_at.timestamp(),
        quote.feed,
        quote.revision,
        str(quote.id),
    )


def _quote_cross_check(
    quotes: Sequence[LatestQuote], tick_size: Decimal | None
) -> str | None:
    latest_by_source: dict[str, LatestQuote] = {}
    for quote in quotes:
        if quote.last is not None and quote.source not in latest_by_source:
            latest_by_source[quote.source] = quote
    values = [quote.last for quote in latest_by_source.values() if quote.last is not None]
    if len(values) < 2:
        return None
    if tick_size is None:
        return "TICK_SIZE_UNKNOWN"
    for index, first in enumerate(values):
        for second in values[index + 1 :]:
            if abs(first - second) > max(
                tick_size, min(abs(first), abs(second)) * Decimal("0.002")
            ):
                return "SOURCE_CONFLICT"
    return None


def _ordered_reasons(reasons: set[str]) -> tuple[str, ...]:
    order = (
        "SOURCE_CONFLICT",
        "SELECTED_SOURCE_STALE",
        "QUOTE_PRICE_UNAVAILABLE",
        "TICK_SIZE_UNKNOWN",
        "QUOTE_QUALITY_FLAGS",
    )
    return tuple(reason for reason in order if reason in reasons) + tuple(
        sorted(reasons.difference(order))
    )


def _empty_series(instrument_id: UUID, reason: str) -> SelectedMarketSeriesResponse:
    return SelectedMarketSeriesResponse(
        instrument_id=instrument_id,
        source=None,
        feed=None,
        adjustment=None,
        interval=None,
        quality="FAIL",
        quality_reasons=(reason,),
        bars=(),
    )


def _instrument_not_found(instrument_id: UUID) -> NexusError:
    return NexusError(
        ErrorCode.NOT_FOUND,
        "The instrument was not found.",
        status_code=404,
        details={"instrument_id": str(instrument_id)},
    )


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _b64decode(value: str) -> bytes:
    return base64.b64decode(
        value + "=" * (-len(value) % 4), altchars=b"-_", validate=True
    )
