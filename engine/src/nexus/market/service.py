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
    IntradayBarResponse,
    IntradayMonitorResponse,
    MarketBarResponse,
    MarketItemResponse,
    MarketPageResponse,
    SelectedMarketSeriesResponse,
    WatchlistMembershipResponse,
)
from nexus.domain.decimal import DecimalString, decimal_to_string
from nexus.domain.time import require_utc
from nexus.market.database_series import (
    load_daily_series,
    load_fosun_display_daily_series,
    load_intraday_series,
)
from nexus.market.intraday_metrics import (
    IntradayAuthorityView,
    IntradayMetrics,
    compute_intraday_metrics,
)
from nexus.market.scope import US_EQUITY_EXCHANGES
from nexus.persistence.models.data import DataArtifact, ProviderCapability
from nexus.persistence.models.market import (
    Instrument,
    InstrumentAlias,
    LatestQuote,
    UniverseMember,
    UniverseSnapshot,
    Watchlist,
    WatchlistMember,
)
from nexus.providers.base import DeliveryMode, PriceSeries
from nexus.quality.market import xnys_session_window
from nexus.quality.source_policy import (
    load_quote_source_policy_v2,
    load_source_policy_v2,
    select_price_series,
)

_DEFAULT_WATCHLIST_ID = UUID("8c7cc45f-9420-5ea5-a75e-7df8a8a63b58")
_DEFAULT_WATCHLIST_NAME = "default_private"
_CURSOR_SCOPE = "nexus.market.v1"
_CURSOR_MAX_LENGTH = 2048
_CURSOR_MAX_AGE = timedelta(minutes=5)
_STALE_AFTER = timedelta(seconds=45)
_SELECTED_SERIES_BAR_LIMIT = 500
_UNIVERSE_KEY = "us_liquid"
_UNIVERSE_VERSION = "us_liquid_v1"


class PriceSeriesLoader(Protocol):
    """Offline composition seam; it carries data candidates, never policy authority."""

    async def __call__(
        self, instrument_id: UUID, decision_time: datetime
    ) -> Sequence[PriceSeries]: ...


class IntradaySeriesLoader(Protocol):
    """Test seam for an already authority-validated whole-source intraday view."""

    async def __call__(
        self, instrument_id: UUID, decision_time: datetime
    ) -> IntradayAuthorityView: ...


class MarketService:
    """Repository owner for market evidence and watchlist set semantics."""

    def __init__(
        self,
        session: AsyncSession,
        decision_time: datetime,
        *,
        cursor_signing_secrets: Sequence[str],
        price_series_loader: PriceSeriesLoader | None = None,
        intraday_series_loader: IntradaySeriesLoader | None = None,
    ) -> None:
        self._session = session
        self._request_time = require_utc(decision_time)
        self._cursor_secrets = tuple(secret for secret in cursor_signing_secrets if secret)
        if not self._cursor_secrets:
            raise ValueError("at least one cursor signing secret is required")
        self._price_series_loader = price_series_loader
        self._intraday_series_loader = intraday_series_loader

    async def market_page(
        self,
        *,
        limit: int,
        cursor: str | None,
        watchlist: bool | None,
        selected_instrument_id: UUID | None,
    ) -> tuple[MarketPageResponse, datetime]:
        """Return one point-in-time page and the exact evidence decision time."""
        cursor_state = self._decode_cursor(cursor, watchlist) if cursor is not None else None
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
        selected_series = await self._selected_series(selected_instrument_id, decision_time)
        intraday_monitor = await self._intraday_monitor(
            selected_instrument_id,
            decision_time,
        )
        return (
            MarketPageResponse(
                items=tuple(items),
                next_cursor=next_cursor,
                selected_series=selected_series,
                intraday_monitor=intraday_monitor,
            ),
            decision_time,
        )

    async def set_watchlisted(
        self, instrument_id: UUID, *, watchlisted: bool
    ) -> WatchlistMembershipResponse:
        """Apply idempotent set semantics with PostgreSQL uniqueness as authority."""
        await self._require_instrument(instrument_id, self._request_time)
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
            .join(
                UniverseMember,
                UniverseMember.instrument_id == Instrument.id,
            )
            .join(
                UniverseSnapshot,
                UniverseSnapshot.id == UniverseMember.universe_snapshot_id,
            )
            .where(
                aliases.c.alias_rank == 1,
                UniverseSnapshot.key == _UNIVERSE_KEY,
                UniverseSnapshot.version == _UNIVERSE_VERSION,
                UniverseSnapshot.effective_from <= decision_time.date(),
                UniverseSnapshot.published_at.is_not(None),
                UniverseSnapshot.published_at <= decision_time,
                Instrument.exchange.in_(US_EQUITY_EXCHANGES),
                Instrument.currency == "USD",
            )
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
            await self._default_watchlist_id()
            membership_exists = (
                select(WatchlistMember.id)
                .where(
                    WatchlistMember.watchlist_id == _DEFAULT_WATCHLIST_ID,
                    WatchlistMember.instrument_id == Instrument.id,
                )
                .exists()
            )
            statement = statement.where(membership_exists if watchlist else ~membership_exists)
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
        ranked_quotes = (
            select(
                LatestQuote.id.label("id"),
                func.row_number()
                .over(
                    partition_by=(
                        LatestQuote.instrument_id,
                        LatestQuote.source,
                        LatestQuote.feed,
                        LatestQuote.event_kind,
                    ),
                    order_by=(
                        LatestQuote.observed_at.desc(),
                        LatestQuote.available_at.desc(),
                        LatestQuote.received_at.desc(),
                        LatestQuote.id.desc(),
                    ),
                )
                .label("quote_rank"),
            )
            .outerjoin(DataArtifact, DataArtifact.id == LatestQuote.data_artifact_id)
            .outerjoin(
                ProviderCapability,
                ProviderCapability.id == LatestQuote.provider_capability_id,
            )
            .where(
                LatestQuote.instrument_id.in_(instrument_ids),
                LatestQuote.available_at <= decision_time,
                LatestQuote.received_at <= decision_time,
                LatestQuote.observed_at <= decision_time,
                LatestQuote.event_kind == "snapshot",
                LatestQuote.source == "fosun",
                LatestQuote.feed == "l1",
                LatestQuote.delivery_mode.in_(("unknown", "real_time", "delayed")),
                LatestQuote.evidence_status == "verified",
                LatestQuote.data_artifact_id.is_not(None),
                LatestQuote.provider_capability_id.is_not(None),
                LatestQuote.provider_status_id.is_not(None),
                LatestQuote.provider_status_version.is_not(None),
                LatestQuote.provider_contract_hash.is_not(None),
                LatestQuote.provider_authorized_at.is_not(None),
                LatestQuote.provider_authorized_at <= decision_time,
                DataArtifact.source == "fosun",
                DataArtifact.schema_version == "fosun_gateway_market_snapshot_v1",
                DataArtifact.available_at <= decision_time,
                DataArtifact.received_at <= decision_time,
                LatestQuote.revision == DataArtifact.sha256,
                ProviderCapability.provider == "fosun",
                ProviderCapability.capability == "READ_ONLY",
                ProviderCapability.schema_version == "1",
                ProviderCapability.contract_hash == LatestQuote.provider_contract_hash,
                ProviderCapability.created_at <= LatestQuote.provider_authorized_at,
                ProviderCapability.expires_at > LatestQuote.provider_authorized_at,
                ProviderCapability.payload["operation"].as_string() == "fosun_get_quotes",
                ProviderCapability.payload["market_scope"].as_string() == "US",
                ProviderCapability.payload["delivery_mode"].as_string()
                == LatestQuote.delivery_mode,
            )
            .subquery()
        )
        quotes = list(
            await self._session.scalars(
                select(LatestQuote)
                .join(ranked_quotes, ranked_quotes.c.id == LatestQuote.id)
                .where(
                    ranked_quotes.c.quote_rank == 1,
                )
            )
        )
        quotes_by_instrument: dict[UUID, list[LatestQuote]] = defaultdict(list)
        for quote in quotes:
            quotes_by_instrument[quote.instrument_id].append(quote)
        watchlisted = await self._watchlisted_ids(instrument_ids)
        try:
            await load_quote_source_policy_v2(
                self._session,
                decision_time=decision_time,
            )
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
        return [
            self._quote_item(
                instrument,
                symbol,
                quotes_by_instrument[instrument.id],
                watchlisted=instrument.id in watchlisted,
                decision_time=decision_time,
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
    ) -> MarketItemResponse:
        eligible = [
            quote
            for quote in quotes
            if quote.source == "fosun"
            and quote.feed == "l1"
            and quote.delivery_mode in {"unknown", "real_time", "delayed"}
            and quote.event_kind == "snapshot"
        ]
        if not eligible:
            reason = "NO_ELIGIBLE_SOURCE" if quotes else "QUOTE_UNAVAILABLE"
            return self._unavailable_item(
                instrument, symbol, watchlisted=watchlisted, reason=reason
            )
        eligible.sort(key=_quote_sort_key)
        selected = eligible[0]
        reasons: set[str] = set()
        quality: Literal["PASS", "PARTIAL", "FAIL"] = "PASS"
        if selected.quality_flags:
            reasons.update(selected.quality_flags)
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
            price=(decimal_to_string(selected.last) if selected.last is not None else None),
            source=selected.source,
            feed=selected.feed,
            delivery_mode=cast(DeliveryMode, selected.delivery_mode),
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
        loader = self._price_series_loader
        if loader is None:
            candidates = tuple(await load_daily_series(self._session, instrument_id, decision_time))
            if not candidates:
                candidates = tuple(
                    await load_fosun_display_daily_series(
                        self._session,
                        instrument_id,
                        decision_time,
                    )
                )
            database_authority = True
        else:
            candidates = tuple(await loader(instrument_id, decision_time))
            database_authority = False
        if not candidates:
            return _empty_series(instrument_id, "NO_HISTORY_DATA")
        daily_candidates = tuple(
            candidate for candidate in candidates if candidate.interval == "1Day"
        )
        if not daily_candidates:
            return _empty_series(instrument_id, "NO_DAILY_HISTORY_DATA")
        if any(
            bar.instrument_id != instrument_id
            for candidate in daily_candidates
            for bar in candidate.bars
        ):
            return _empty_series(instrument_id, "HISTORY_INSTRUMENT_MISMATCH")
        selected: PriceSeries | None
        if database_authority:
            try:
                await load_source_policy_v2(
                    self._session,
                    instrument_tick_size=instrument.tick_size,
                    decision_time=decision_time,
                )
            except ValueError:
                return _empty_series(instrument_id, "SOURCE_POLICY_UNAVAILABLE")
            selected = daily_candidates[0]
            selection_quality: Literal["PASS", "PARTIAL", "FAIL"] = selected.quality
            selection_reasons: tuple[str, ...] = (
                ("HISTORY_DELIVERY_UNVERIFIED",) if selected.delivery_mode == "unknown" else ()
            )
        else:
            selection = await select_price_series(
                self._session,
                daily_candidates,
                instrument.tick_size,
                decision_time,
            )
            selected = selection.selected
            selection_quality = selection.quality
            selection_reasons = selection.reasons
        if selected is None:
            return SelectedMarketSeriesResponse(
                instrument_id=instrument_id,
                source=None,
                feed=None,
                delivery_mode=None,
                adjustment=None,
                interval=None,
                quality=selection_quality,
                quality_reasons=selection_reasons,
                bars=(),
            )
        if selected.interval != "1Day":
            return _empty_series(instrument_id, "NO_DAILY_HISTORY_DATA")
        bars = selected.bars[-_SELECTED_SERIES_BAR_LIMIT:]
        quality: Literal["PASS", "PARTIAL", "FAIL"] = selection_quality
        reasons = list(selection_reasons)
        if len(selected.bars) > _SELECTED_SERIES_BAR_LIMIT:
            reasons.append("HISTORY_TRUNCATED")
            if quality == "PASS":
                quality = "PARTIAL"
        return SelectedMarketSeriesResponse(
            instrument_id=instrument_id,
            source=selected.source,
            feed=selected.feed,
            delivery_mode=selected.delivery_mode,
            adjustment=selected.adjustment,
            interval="1Day",
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

    async def _intraday_monitor(
        self,
        instrument_id: UUID | None,
        decision_time: datetime,
    ) -> IntradayMonitorResponse | None:
        if instrument_id is None:
            return None
        view = (
            await load_intraday_series(self._session, instrument_id, decision_time)
            if self._intraday_series_loader is None
            else await self._intraday_series_loader(instrument_id, decision_time)
        )
        if view.instrument_id != instrument_id:
            view = IntradayAuthorityView(
                instrument_id=instrument_id,
                source=None,
                feed=None,
                delivery_mode=None,
                bars=(),
                trade=None,
                quote=None,
                quality_reasons=("INTRADAY_INSTRUMENT_MISMATCH",),
            )
        metrics = compute_intraday_metrics(view, decision_time)
        complete_bars = tuple(
            bar
            for bar in view.bars
            if bar.instrument_id == instrument_id
            and bar.observed_at is not None
            and bar.available_at <= decision_time
            and bar.observed_at + timedelta(minutes=1) <= decision_time
        )[-60:]
        return IntradayMonitorResponse(
            instrument_id=instrument_id,
            status=_intraday_status(view, metrics),
            source=view.source,
            feed=view.feed,
            delivery_mode=view.delivery_mode,
            last=None if view.trade is None else decimal_to_string(view.trade.last),
            bid=None if view.quote is None else decimal_to_string(view.quote.bid),
            ask=None if view.quote is None else decimal_to_string(view.quote.ask),
            trade_as_of=None if view.trade is None else view.trade.observed_at,
            quote_as_of=None if view.quote is None else view.quote.observed_at,
            bar_as_of=None if not complete_bars else complete_bars[-1].observed_at,
            return_5m=_optional_decimal(metrics.return_5m),
            return_30m=_optional_decimal(metrics.return_30m),
            range_30m=_optional_decimal(metrics.range_30m),
            spread_bps=_optional_decimal(metrics.spread_bps),
            quote_age_seconds=metrics.quote_age_seconds,
            bar_age_seconds=metrics.bar_age_seconds,
            sample_count=metrics.sample_count,
            quality_reasons=metrics.quality_reasons,
            bars=tuple(
                IntradayBarResponse(
                    observed_at=cast(datetime, bar.observed_at),
                    open=decimal_to_string(bar.open),
                    high=decimal_to_string(bar.high),
                    low=decimal_to_string(bar.low),
                    close=decimal_to_string(bar.close),
                    volume=decimal_to_string(bar.volume),
                    available_at=bar.available_at,
                    quality_flags=bar.quality_flags,
                )
                for bar in complete_bars
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
                .join(
                    UniverseMember,
                    UniverseMember.instrument_id == Instrument.id,
                )
                .join(
                    UniverseSnapshot,
                    UniverseSnapshot.id == UniverseMember.universe_snapshot_id,
                )
                .where(
                    Instrument.id == instrument_id,
                    InstrumentAlias.alias_type == "ticker",
                    InstrumentAlias.valid_from <= decision_time.date(),
                    or_(
                        InstrumentAlias.valid_to.is_(None),
                        InstrumentAlias.valid_to >= decision_time.date(),
                    ),
                    UniverseSnapshot.key == _UNIVERSE_KEY,
                    UniverseSnapshot.version == _UNIVERSE_VERSION,
                    UniverseSnapshot.effective_from <= decision_time.date(),
                    UniverseSnapshot.published_at.is_not(None),
                    UniverseSnapshot.published_at <= decision_time,
                    Instrument.exchange.in_(US_EQUITY_EXCHANGES),
                    Instrument.currency == "USD",
                )
                .limit(1)
            ),
        )

    async def _require_instrument(self, instrument_id: UUID, decision_time: datetime) -> None:
        if await self._active_instrument(instrument_id, decision_time) is None:
            raise _instrument_not_found(instrument_id)

    async def _default_watchlist_id(self) -> UUID | None:
        rows = list(
            (
                await self._session.execute(
                    select(Watchlist.id, Watchlist.name).where(
                        or_(
                            Watchlist.id == _DEFAULT_WATCHLIST_ID,
                            Watchlist.name == _DEFAULT_WATCHLIST_NAME,
                        )
                    )
                )
            ).all()
        )
        if not rows:
            return None
        if rows != [(_DEFAULT_WATCHLIST_ID, _DEFAULT_WATCHLIST_NAME)]:
            raise NexusError(
                ErrorCode.INTERNAL_ERROR,
                "The request could not be completed.",
                status_code=500,
            )
        return _DEFAULT_WATCHLIST_ID

    async def _ensure_default_watchlist(self) -> UUID:
        await self._session.execute(
            insert(Watchlist)
            .values(id=_DEFAULT_WATCHLIST_ID, name=_DEFAULT_WATCHLIST_NAME)
            .on_conflict_do_nothing()
        )
        watchlist_id = await self._default_watchlist_id()
        if watchlist_id is None:
            raise NexusError(
                ErrorCode.INTERNAL_ERROR,
                "The request could not be completed.",
                status_code=500,
            )
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
            delivery_mode="unknown",
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
                datetime.fromisoformat(cast(str, decoded["decision_time"]).replace("Z", "+00:00"))
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


def _quote_sort_key(quote: LatestQuote) -> tuple[object, ...]:
    return (
        0 if quote.last is not None else 1,
        0 if not quote.quality_flags else 1,
        -quote.available_at.timestamp(),
        -quote.received_at.timestamp(),
        -quote.observed_at.timestamp(),
        quote.feed,
        quote.revision,
        str(quote.id),
    )


def _ordered_reasons(reasons: set[str]) -> tuple[str, ...]:
    order = (
        "INTRADAY_SOURCE_DIVERGENCE",
        "SELECTED_SOURCE_STALE",
        "FOSUN_PROVIDER_TIMESTAMP_UNAVAILABLE",
        "INTRADAY_BID_ASK_UNAVAILABLE",
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
        delivery_mode=None,
        adjustment=None,
        interval=None,
        quality="FAIL",
        quality_reasons=(reason,),
        bars=(),
    )


def _optional_decimal(value: Decimal | None) -> DecimalString | None:
    return None if value is None else decimal_to_string(value)


def _intraday_status(
    view: IntradayAuthorityView,
    metrics: IntradayMetrics,
) -> Literal["ready", "partial", "stale", "outside_session", "unavailable"]:
    reasons = set(metrics.quality_reasons)
    has_facts = view.trade is not None or view.quote is not None or bool(view.bars)
    if not has_facts:
        return "unavailable"
    if "INTRADAY_OUTSIDE_SESSION" in reasons:
        return "outside_session"
    if reasons.intersection({"INTRADAY_TRADE_STALE", "INTRADAY_QUOTE_STALE", "INTRADAY_BAR_STALE"}):
        return "stale"
    if (
        view.trade is not None
        and view.quote is not None
        and metrics.return_5m is not None
        and metrics.return_30m is not None
        and metrics.range_30m is not None
        and metrics.spread_bps is not None
        and not reasons
    ):
        return "ready"
    return "partial"


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
    return base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)
