"""Strict parsers for the bounded AAPL/Nasdaq risk-authority sources."""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Literal
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.persistence.models.data import SourcePolicy
from nexus.persistence.models.market import (
    Instrument,
    InstrumentAlias,
    UniverseMember,
    UniverseSnapshot,
)
from nexus.persistence.models.paper import PAPER_DECISION_SOURCE_POLICY_ID, RiskQueryAuthorityRecord
from nexus.risk.authority import canonical_query_authority_hash

_NASDAQ = "{http://www.nasdaqtrader.com/}"
_SYMBOL = re.compile(r"[A-Z][A-Z0-9.-]{0,14}")
_MAX_SOURCE_BYTES = 8 * 1024 * 1024
_MAX_HALT_AGE = timedelta(minutes=2)
_DIRECTORY_HEADER = (
    "Symbol",
    "Security Name",
    "Market Category",
    "Test Issue",
    "Financial Status",
    "Round Lot Size",
    "ETF",
    "NextShares",
)


@dataclass(frozen=True)
class NasdaqHaltSnapshot:
    blocked: bool
    published_at: datetime


@dataclass(frozen=True)
class NasdaqListingSnapshot:
    blocked: bool
    file_created_at: datetime


@dataclass(frozen=True)
class RiskSourceProof:
    kind: Literal["corporate_action", "suspension", "delisting"]
    blocked: bool
    source_watermark_hash: str
    available_at: datetime

    def __post_init__(self) -> None:
        if (
            self.kind not in {"corporate_action", "suspension", "delisting"}
            or type(self.blocked) is not bool
            or re.fullmatch(r"[0-9a-f]{64}", self.source_watermark_hash) is None
            or self.available_at.tzinfo is None
            or self.available_at.utcoffset() != UTC.utcoffset(self.available_at)
        ):
            raise ValueError("RISK_SOURCE_PROOF_INVALID")


@dataclass(frozen=True)
class RiskQueryScope:
    instrument_id: UUID
    universe_snapshot_id: UUID
    existing: tuple[RiskQueryAuthorityRecord, ...]


def _valid_symbol(symbol: str) -> bool:
    return isinstance(symbol, str) and _SYMBOL.fullmatch(symbol) is not None


def parse_nasdaq_halt_feed(raw: bytes, *, symbol: str, fetched_at: datetime) -> NasdaqHaltSnapshot:
    """Read one complete, fresh current-halts RSS snapshot without HTML fallback."""
    try:
        if (
            not isinstance(raw, bytes)
            or not 0 < len(raw) <= _MAX_SOURCE_BYTES
            or not _valid_symbol(symbol)
            or fetched_at.tzinfo is None
            or fetched_at.utcoffset() != UTC.utcoffset(fetched_at)
        ):
            raise ValueError
        root = ET.fromstring(raw)
        channel = root.find("channel")
        if root.tag != "rss" or channel is None:
            raise ValueError
        published_text = channel.findtext("pubDate")
        count_text = channel.findtext(f"{_NASDAQ}numItems")
        if published_text is None or count_text is None:
            raise ValueError
        published_at = parsedate_to_datetime(published_text).astimezone(UTC)
        items = channel.findall("item")
        if int(count_text) != len(items) or not published_at <= fetched_at:
            raise ValueError
        if fetched_at - published_at > _MAX_HALT_AGE:
            raise ValueError
        blocked = False
        for item in items:
            issue_symbol = item.findtext(f"{_NASDAQ}IssueSymbol")
            reason = item.findtext(f"{_NASDAQ}ReasonCode")
            halt_date = item.findtext(f"{_NASDAQ}HaltDate")
            if not issue_symbol or not reason or not halt_date:
                raise ValueError
            if issue_symbol == symbol:
                blocked = True
        return NasdaqHaltSnapshot(blocked=blocked, published_at=published_at)
    except (ET.ParseError, TypeError, ValueError, OverflowError) as error:
        raise ValueError("NASDAQ_HALT_SNAPSHOT_INVALID") from error


def parse_nasdaq_listing_directory(
    raw: bytes, *, symbol: str, decision_session: date, fetched_at: datetime
) -> NasdaqListingSnapshot:
    """Resolve current Nasdaq listing status from a full session-stamped directory."""
    try:
        if (
            not isinstance(raw, bytes)
            or not 0 < len(raw) <= _MAX_SOURCE_BYTES
            or not _valid_symbol(symbol)
            or type(decision_session) is not date
            or fetched_at.tzinfo is None
            or fetched_at.utcoffset() != UTC.utcoffset(fetched_at)
        ):
            raise ValueError
        lines = raw.decode("utf-8-sig").splitlines()
        if len(lines) < 2 or tuple(lines[0].split("|")) != _DIRECTORY_HEADER:
            raise ValueError
        prefix = "File Creation Time: "
        if not lines[-1].startswith(prefix):
            raise ValueError
        stamp = lines[-1].split("|", 1)[0][len(prefix) :]
        file_created_at = datetime.strptime(stamp, "%m%d%Y%H:%M").replace(
            tzinfo=ZoneInfo("America/New_York")
        )
        if (
            file_created_at.date() != decision_session
            or file_created_at.astimezone(UTC) > fetched_at
        ):
            raise ValueError
        matches: list[tuple[str, ...]] = []
        for line in lines[1:-1]:
            fields = tuple(line.split("|"))
            if len(fields) != len(_DIRECTORY_HEADER):
                raise ValueError
            if fields[0] == symbol:
                matches.append(fields)
        if len(matches) > 1:
            raise ValueError
        blocked = not matches or matches[0][3] != "N" or matches[0][4] != "N"
        return NasdaqListingSnapshot(
            blocked=blocked,
            file_created_at=file_created_at.astimezone(UTC),
        )
    except (UnicodeError, TypeError, ValueError, OverflowError) as error:
        raise ValueError("NASDAQ_LISTING_SNAPSHOT_INVALID") from error


def _record_matches(record: RiskQueryAuthorityRecord, expected: RiskQueryAuthorityRecord) -> bool:
    return all(
        getattr(record, field) == getattr(expected, field)
        for field in (
            "source_policy_id",
            "universe_snapshot_id",
            "decision_session",
            "kind",
            "required_instrument_ids",
            "blocked_instrument_ids",
            "complete",
            "source_watermark_hash",
            "available_at",
            "content_hash",
        )
    )


async def inspect_query_authority_scope(
    session: AsyncSession,
    *,
    decision_session: date,
    symbol: str,
    as_of: datetime,
    universe_snapshot_id: UUID | None = None,
) -> RiskQueryScope:
    """Resolve the one supported frozen AAPL universe and any persisted result."""
    if (
        symbol != "AAPL"
        or type(decision_session) is not date
        or as_of.tzinfo is None
        or as_of.utcoffset() != UTC.utcoffset(as_of)
    ):
        raise ValueError("RISK_QUERY_AUTHORITY_SCOPE_INVALID")
    universe: UniverseSnapshot | None
    if universe_snapshot_id is None:
        universes = tuple(
            await session.scalars(
                select(UniverseSnapshot)
                .where(
                    UniverseSnapshot.key == "us_liquid",
                    UniverseSnapshot.version == "us_liquid_v1",
                    UniverseSnapshot.source == "fosun_local_read_only",
                    UniverseSnapshot.effective_from <= decision_session,
                    UniverseSnapshot.published_at <= as_of,
                    UniverseSnapshot.created_at <= as_of,
                )
                .order_by(UniverseSnapshot.published_at.desc())
            )
        )
        if len(universes) != 1:
            raise ValueError("RISK_QUERY_AUTHORITY_SCOPE_INVALID")
        universe = universes[0]
        universe_snapshot_id = universe.id
    else:
        universe = await session.get(UniverseSnapshot, universe_snapshot_id)
    policy = await session.get(SourcePolicy, PAPER_DECISION_SOURCE_POLICY_ID)
    members = tuple(
        await session.scalars(
            select(UniverseMember)
            .where(UniverseMember.universe_snapshot_id == universe_snapshot_id)
            .order_by(UniverseMember.instrument_id)
        )
    )
    if (
        policy is None
        or policy.created_at > as_of
        or universe is None
        or universe.key != "us_liquid"
        or universe.source != "fosun_local_read_only"
        or universe.effective_from > decision_session
        or universe.published_at is None
        or universe.published_at > as_of
        or universe.created_at > as_of
        or len(members) != 1
        or members[0].created_at > as_of
    ):
        raise ValueError("RISK_QUERY_AUTHORITY_SCOPE_INVALID")
    instrument_id = members[0].instrument_id
    instrument = await session.get(Instrument, instrument_id)
    aliases = tuple(
        await session.scalars(
            select(InstrumentAlias).where(
                InstrumentAlias.instrument_id == instrument_id,
                InstrumentAlias.alias_type == "ticker",
                InstrumentAlias.symbol == symbol,
                InstrumentAlias.valid_from <= decision_session,
                (InstrumentAlias.valid_to.is_(None))
                | (InstrumentAlias.valid_to >= decision_session),
                InstrumentAlias.created_at <= as_of,
            )
        )
    )
    if (
        instrument is None
        or instrument.created_at > as_of
        or instrument.exchange not in {"NASDAQ", "XNAS"}
        or instrument.asset_type != "common_stock"
        or instrument.currency != "USD"
        or len(aliases) != 1
    ):
        raise ValueError("RISK_QUERY_AUTHORITY_SCOPE_INVALID")
    existing = tuple(
        await session.scalars(
            select(RiskQueryAuthorityRecord)
            .where(
                RiskQueryAuthorityRecord.source_policy_id == PAPER_DECISION_SOURCE_POLICY_ID,
                RiskQueryAuthorityRecord.universe_snapshot_id == universe_snapshot_id,
                RiskQueryAuthorityRecord.decision_session == decision_session,
            )
            .order_by(RiskQueryAuthorityRecord.kind)
        )
    )
    if existing and len(existing) != 3:
        raise ValueError("RISK_QUERY_AUTHORITY_CONFLICT")
    return RiskQueryScope(instrument_id, universe_snapshot_id, existing)


async def persist_query_authorities(
    session: AsyncSession,
    *,
    universe_snapshot_id: UUID,
    decision_session: date,
    symbol: str,
    proofs: tuple[RiskSourceProof, ...],
) -> tuple[RiskQueryAuthorityRecord, ...]:
    """Append or verify the exact three source-backed proofs for one AAPL scope."""
    kinds = {proof.kind for proof in proofs}
    if (
        symbol != "AAPL"
        or type(decision_session) is not date
        or len(proofs) != 3
        or kinds != {"corporate_action", "suspension", "delisting"}
    ):
        raise ValueError("RISK_QUERY_AUTHORITY_SCOPE_INVALID")
    cutoff = min(proof.available_at for proof in proofs)
    scope = await inspect_query_authority_scope(
        session,
        decision_session=decision_session,
        symbol=symbol,
        as_of=cutoff,
        universe_snapshot_id=universe_snapshot_id,
    )
    instrument_id = scope.instrument_id

    expected: list[RiskQueryAuthorityRecord] = []
    for proof in sorted(proofs, key=lambda value: value.kind):
        blocked = [instrument_id] if proof.blocked else []
        content_hash = canonical_query_authority_hash(
            source_policy_id=PAPER_DECISION_SOURCE_POLICY_ID,
            universe_snapshot_id=universe_snapshot_id,
            decision_session=decision_session,
            kind=proof.kind,
            required_instrument_ids=[instrument_id],
            blocked_instrument_ids=blocked,
            complete=True,
            source_watermark_hash=proof.source_watermark_hash,
            available_at=proof.available_at,
        )
        expected.append(
            RiskQueryAuthorityRecord(
                source_policy_id=PAPER_DECISION_SOURCE_POLICY_ID,
                universe_snapshot_id=universe_snapshot_id,
                decision_session=decision_session,
                kind=proof.kind,
                required_instrument_ids=[instrument_id],
                blocked_instrument_ids=blocked,
                complete=True,
                source_watermark_hash=proof.source_watermark_hash,
                available_at=proof.available_at,
                content_hash=content_hash,
            )
        )
    existing = scope.existing
    if existing:
        if len(existing) != 3 or any(
            not _record_matches(record, draft)
            for record, draft in zip(existing, expected, strict=True)
        ):
            raise ValueError("RISK_QUERY_AUTHORITY_CONFLICT")
        return existing
    session.add_all(expected)
    await session.flush()
    return tuple(expected)
