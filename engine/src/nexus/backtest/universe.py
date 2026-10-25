"""Read-only preflight for the fixed internal ``us_liquid_v1`` definition."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Literal, TextIO
from uuid import UUID

import exchange_calendars  # type: ignore[import-untyped]
from sqlalchemy import func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.time import require_utc
from nexus.market.scope import US_EQUITY_EXCHANGES
from nexus.market.universe import stable_instrument_id
from nexus.persistence.models.market import Instrument, InstrumentAlias

_SEED = Path(__file__).resolve().parent / "data/us_liquid_v1.csv"
_FIELDS = (
    "universe_version",
    "instrument_id",
    "symbol",
    "asset_type",
    "sector_code",
    "risk_bucket",
    "valid_from",
    "valid_to",
    "source",
)
_SYMBOL = re.compile(r"^[A-Z][A-Z0-9.]{0,31}$")
_KEY = "us_liquid"
_VERSION = "us_liquid_v1"
_SOURCE = "nexus_internal_us_liquid_v1"
_INCLUSION_REASON = "internal us_liquid_v1 engineering universe definition"

GapCategory = Literal[
    "INSTRUMENT_MISSING",
    "INSTRUMENT_LATE",
    "INSTRUMENT_IDENTITY_INVALID",
    "TICKER_ALIAS_MISSING",
    "TICKER_ALIAS_LATE",
    "TICKER_ALIAS_AMBIGUOUS",
    "TICKER_ALIAS_MISMATCH",
]

_GAP_ORDER: tuple[GapCategory, ...] = (
    "INSTRUMENT_MISSING",
    "INSTRUMENT_LATE",
    "INSTRUMENT_IDENTITY_INVALID",
    "TICKER_ALIAS_MISSING",
    "TICKER_ALIAS_LATE",
    "TICKER_ALIAS_AMBIGUOUS",
    "TICKER_ALIAS_MISMATCH",
)


class UniversePreflightError(ValueError):
    """Fixed, non-sensitive preflight failure codes."""


@dataclass(frozen=True)
class UniverseDefinitionMember:
    instrument_id: UUID
    symbol: str
    asset_type: str
    risk_bucket: str
    inclusion_reason: str = _INCLUSION_REASON


@dataclass(frozen=True)
class UniverseDefinition:
    key: str
    version: str
    source: str
    effective_session: date
    members: tuple[UniverseDefinitionMember, ...]
    content_hash: str


@dataclass(frozen=True)
class UniversePreflightResult:
    status: Literal["BACKTEST_UNIVERSE_PREFLIGHT_READY", "BACKTEST_UNIVERSE_PREFLIGHT_BLOCKED"]
    member_count: int
    gap_categories: tuple[GapCategory, ...]
    content_hash: str


_MAX_DEFINITION_TEXT = 64 * 1024
_MAX_DEFINITION_VALUES = 16


class _DefinitionParseValues(dict[tuple[date, int, object, str], UniverseDefinition]):
    active = True


_DEFINITION_VALUES: ContextVar[_DefinitionParseValues | None] = ContextVar(
    "universe_definition_parse_values", default=None
)


@contextmanager
def universe_definition_parse_scope() -> Iterator[None]:
    """Reuse pure parsing inside one read; never retain file or eligibility authority."""
    existing = _DEFINITION_VALUES.get()
    if existing is not None and existing.active:
        yield
        return
    values = _DefinitionParseValues()
    token = _DEFINITION_VALUES.set(values)
    try:
        yield
    finally:
        # A child task can inherit the context and outlive its owner.
        values.active = False
        values.clear()
        _DEFINITION_VALUES.reset(token)


def load_us_liquid_v1_definition(
    effective_session: date, *, seed_path: Path = _SEED
) -> UniverseDefinition:
    """Read the current definition on every call; optionally reuse identical parsing."""
    try:
        with seed_path.open(newline="", encoding="utf-8") as handle:
            values = _DEFINITION_VALUES.get()
            if values is None or not values.active or not handle.seekable():
                return _parse_definition(handle, effective_session)
            raw = handle.read(_MAX_DEFINITION_TEXT + 1)
            if len(raw) > _MAX_DEFINITION_TEXT:
                handle.seek(0)
                return _parse_definition(handle, effective_session)
            key = (effective_session, csv.field_size_limit(), csv.get_dialect("excel"), raw)
            if key in values:
                return values[key]
            result = _parse_definition(io.StringIO(raw, newline=""), effective_session)
            if len(values) < _MAX_DEFINITION_VALUES:
                values[key] = result
            return result
    except (OSError, UnicodeError, csv.Error, KeyError, TypeError, ValueError):
        raise UniversePreflightError("BACKTEST_UNIVERSE_DEFINITION_INVALID") from None


def _parse_definition(handle: TextIO, effective_session: date) -> UniverseDefinition:
    try:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != _FIELDS:
            raise ValueError
        rows = list(reader)
        if len(rows) != 49:
            raise ValueError
        members: list[UniverseDefinitionMember] = []
        symbols: set[str] = set()
        instrument_ids: set[UUID] = set()
        for row in rows:
            if set(row) != set(_FIELDS) or any(value is None for value in row.values()):
                raise ValueError
            symbol = row["symbol"]
            instrument_id = UUID(row["instrument_id"])
            asset_type = row["asset_type"]
            common_stock = asset_type == "common_stock"
            if (
                row["universe_version"] != _VERSION
                or _SYMBOL.fullmatch(symbol) is None
                or str(instrument_id) != row["instrument_id"]
                or instrument_id != stable_instrument_id(symbol)
                or symbol in symbols
                or instrument_id in instrument_ids
                or asset_type not in {"common_stock", "equity_etf", "non_equity_etf"}
                or (common_stock and not row["sector_code"])
                or (not common_stock and bool(row["sector_code"]))
                or not row["risk_bucket"]
                or date.fromisoformat(row["valid_from"]) != date(2026, 1, 1)
                or row["valid_to"]
                or row["source"] != ("NEXUS_SECTOR_V1" if common_stock else "versioned_fixture")
            ):
                raise ValueError
            symbols.add(symbol)
            instrument_ids.add(instrument_id)
            members.append(
                UniverseDefinitionMember(
                    instrument_id=instrument_id,
                    symbol=symbol,
                    asset_type=asset_type,
                    risk_bucket=row["risk_bucket"],
                )
            )
    except (OSError, UnicodeError, csv.Error, KeyError, TypeError, ValueError):
        raise UniversePreflightError("BACKTEST_UNIVERSE_DEFINITION_INVALID") from None

    ordered = tuple(sorted(members, key=lambda member: str(member.instrument_id)))
    payload = {
        "schema_version": "us_liquid_universe_candidate_v1",
        "key": _KEY,
        "version": _VERSION,
        "source": _SOURCE,
        "effective_from": effective_session.isoformat(),
        "members": [
            {
                "instrument_id": str(member.instrument_id),
                "inclusion_reason": member.inclusion_reason,
            }
            for member in ordered
        ],
    }
    content_hash = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return UniverseDefinition(
        key=_KEY,
        version=_VERSION,
        source=_SOURCE,
        effective_session=effective_session,
        members=ordered,
        content_hash=content_hash,
    )


def require_xnys_session(effective_session: date) -> datetime:
    """Return the official open for one exact session without date normalization."""
    official = exchange_calendars.get_calendar("XNYS")
    if (
        effective_session < official.first_session.date()
        or effective_session > official.last_session.date()
        or not official.is_session(effective_session)
    ):
        raise UniversePreflightError("BACKTEST_UNIVERSE_SESSION_INVALID")
    return require_utc(official.session_open(effective_session).to_pydatetime())


def require_future_xnys_session(effective_session: date, cutoff: datetime) -> datetime:
    """Require the session open to be later than the actual database preflight clock."""
    market_open = require_xnys_session(effective_session)
    if market_open <= require_utc(cutoff):
        raise UniversePreflightError("BACKTEST_UNIVERSE_SESSION_NOT_FUTURE")
    return market_open


async def preflight_us_liquid_v1(
    session: AsyncSession, effective_session: date
) -> UniversePreflightResult:
    """Check the future candidate against existing master data in a read-only transaction."""
    await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
    cutoff = await session.scalar(select(func.clock_timestamp()))
    assert isinstance(cutoff, datetime)
    require_future_xnys_session(effective_session, cutoff)
    definition = load_us_liquid_v1_definition(effective_session)
    expected_ids = tuple(member.instrument_id for member in definition.members)
    expected_symbols = tuple(member.symbol for member in definition.members)
    instruments = {
        item.id: item
        for item in await session.scalars(select(Instrument).where(Instrument.id.in_(expected_ids)))
    }
    aliases = tuple(
        await session.scalars(
            select(InstrumentAlias).where(
                or_(
                    InstrumentAlias.symbol.in_(expected_symbols),
                    InstrumentAlias.instrument_id.in_(expected_ids),
                ),
                InstrumentAlias.alias_type == "ticker",
                InstrumentAlias.valid_from <= effective_session,
                or_(
                    InstrumentAlias.valid_to.is_(None),
                    InstrumentAlias.valid_to >= effective_session,
                ),
            )
        )
    )

    gaps: set[GapCategory] = set()
    for member in definition.members:
        instrument = instruments.get(member.instrument_id)
        if instrument is None:
            gaps.add("INSTRUMENT_MISSING")
        elif instrument.created_at > cutoff:
            gaps.add("INSTRUMENT_LATE")
        elif (
            instrument.asset_type != member.asset_type
            or instrument.currency != "USD"
            or instrument.exchange not in US_EQUITY_EXCHANGES
            or instrument.risk_bucket != member.risk_bucket
        ):
            gaps.add("INSTRUMENT_IDENTITY_INVALID")

        candidates = [
            alias
            for alias in aliases
            if alias.symbol == member.symbol or alias.instrument_id == member.instrument_id
        ]
        visible = [alias for alias in candidates if alias.created_at <= cutoff]
        if not visible:
            gaps.add("TICKER_ALIAS_LATE" if candidates else "TICKER_ALIAS_MISSING")
        elif len(visible) > 1:
            gaps.add("TICKER_ALIAS_AMBIGUOUS")
        elif visible[0].instrument_id != member.instrument_id or visible[0].symbol != member.symbol:
            gaps.add("TICKER_ALIAS_MISMATCH")

    ordered_gaps = tuple(category for category in _GAP_ORDER if category in gaps)
    return UniversePreflightResult(
        status=(
            "BACKTEST_UNIVERSE_PREFLIGHT_BLOCKED"
            if ordered_gaps
            else "BACKTEST_UNIVERSE_PREFLIGHT_READY"
        ),
        member_count=len(definition.members),
        gap_categories=ordered_gaps,
        content_hash=definition.content_hash,
    )
