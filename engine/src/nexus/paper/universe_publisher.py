"""Publish an immutable, evidence-bound local research Universe for Agent Paper."""

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from hashlib import sha256
from typing import TypedDict
from urllib.parse import urlsplit
from uuid import UUID, uuid5

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.persistence import select_authoritative_snapshot
from nexus.market.scope import US_EQUITY_EXCHANGES
from nexus.market.universe import EQUITY_SECTOR_CODES
from nexus.persistence.models.market import (
    Instrument,
    InstrumentAlias,
    SectorExposure,
    UniverseMember,
    UniverseSnapshot,
)

_KEY = "agent_paper_research"
_SOURCE = "nexus_local_reviewed_research"
_NAMESPACE = UUID("17c7443d-a8b2-4385-9168-833085a68010")


class ReviewedCandidate(TypedDict):
    symbol: str
    missing: list[str]


class ReviewedEvidence(TypedDict):
    instrument_id: str
    symbol: str
    research_snapshot_id: str
    sector_exposure_id: str


def _validate_request(
    *, instrument_ids: tuple[UUID, ...], effective_from: date, as_of: datetime
) -> tuple[UUID, ...]:
    if (
        as_of.tzinfo is None
        or as_of.utcoffset() != UTC.utcoffset(as_of)
        or not 2 <= len(instrument_ids) <= 5
        or len(set(instrument_ids)) != len(instrument_ids)
        or effective_from < as_of.date()
    ):
        raise ValueError("REVIEWED_UNIVERSE_REQUEST_INVALID")
    return tuple(sorted(instrument_ids, key=lambda item: item.int))


async def inspect_reviewed_research_universe(
    session: AsyncSession,
    *,
    instrument_ids: tuple[UUID, ...],
    effective_from: date,
    as_of: datetime,
) -> tuple[tuple[ReviewedEvidence, ...], tuple[ReviewedCandidate, ...]]:
    """Report exact missing local evidence without writing any business fact."""
    ordered = _validate_request(
        instrument_ids=instrument_ids, effective_from=effective_from, as_of=as_of
    )
    evidence: list[ReviewedEvidence] = []
    candidates: list[ReviewedCandidate] = []
    for instrument_id in ordered:
        instrument = await session.get(Instrument, instrument_id)
        aliases = tuple(
            await session.scalars(
                select(InstrumentAlias).where(
                    InstrumentAlias.instrument_id == instrument_id,
                    InstrumentAlias.alias_type == "ticker",
                    InstrumentAlias.valid_from <= effective_from,
                    (InstrumentAlias.valid_to.is_(None))
                    | (InstrumentAlias.valid_to >= effective_from),
                    InstrumentAlias.created_at <= as_of,
                )
            )
        )
        missing: list[str] = []
        symbol = aliases[0].symbol if len(aliases) == 1 else str(instrument_id)
        if (
            instrument is None
            or instrument.created_at > as_of
            or instrument.asset_type != "common_stock"
            or instrument.currency != "USD"
            or instrument.exchange not in US_EQUITY_EXCHANGES
            or len(aliases) != 1
        ):
            missing.append("IDENTITY_INVALID")
            candidates.append({"symbol": symbol, "missing": missing})
            continue
        snapshot = await select_authoritative_snapshot(
            session, instrument_id=instrument_id, as_of=as_of
        )
        if snapshot is None or snapshot.created_at > as_of:
            missing.append("RESEARCH_SNAPSHOT_MISSING")
        latest_sectors = tuple(
            await session.scalars(
                select(SectorExposure)
                .where(
                    SectorExposure.instrument_id == instrument_id,
                    SectorExposure.as_of <= effective_from,
                    SectorExposure.created_at <= as_of,
                    SectorExposure.reviewed.is_(True),
                    SectorExposure.synthetic.is_(False),
                )
                .order_by(
                    SectorExposure.as_of.desc(),
                    SectorExposure.created_at.desc(),
                    SectorExposure.id.desc(),
                )
                .limit(2)
            )
        )
        sector = latest_sectors[0] if latest_sectors else None
        duplicate_latest = (
            sector is not None
            and len(latest_sectors) == 2
            and latest_sectors[1].as_of == sector.as_of
        )
        sector_source = urlsplit(sector.source_url) if sector is not None else None
        if sector is None:
            missing.append("REVIEWED_SECTOR_MISSING")
        elif (
            duplicate_latest
            or sector.sector_code not in EQUITY_SECTOR_CODES
            or sector.weight != Decimal(1)
            or len(sector.content_hash) != 64
            or any(character not in "0123456789abcdef" for character in sector.content_hash)
            or sector_source is None
            or sector_source.scheme != "https"
            or not sector_source.hostname
            or sector_source.username is not None
            or sector_source.password is not None
            or bool(sector_source.fragment)
        ):
            missing.append("REVIEWED_SECTOR_INVALID")
        candidates.append({"symbol": symbol, "missing": missing})
        if not missing:
            assert snapshot is not None and sector is not None
            evidence.append(
                {
                    "instrument_id": str(instrument_id),
                    "symbol": symbol,
                    "research_snapshot_id": str(snapshot.id),
                    "sector_exposure_id": str(sector.id),
                }
            )
    if len({candidate["symbol"] for candidate in candidates}) != len(candidates):
        for candidate in candidates:
            if "IDENTITY_INVALID" not in candidate["missing"]:
                candidate["missing"].append("IDENTITY_INVALID")
        evidence.clear()
    return tuple(evidence), tuple(candidates)


async def publish_reviewed_research_universe(
    session: AsyncSession,
    *,
    instrument_ids: tuple[UUID, ...],
    effective_from: date,
    as_of: datetime,
) -> UniverseSnapshot:
    """Admit 2–5 stocks with point-in-time research and reviewed sector evidence."""
    ordered = _validate_request(
        instrument_ids=instrument_ids, effective_from=effective_from, as_of=as_of
    )
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtext('agent-paper-reviewed-universe'))")
    )
    evidence, candidates = await inspect_reviewed_research_universe(
        session, instrument_ids=ordered, effective_from=effective_from, as_of=as_of
    )
    if any("IDENTITY_INVALID" in candidate["missing"] for candidate in candidates):
        raise ValueError("REVIEWED_UNIVERSE_IDENTITY_INVALID")
    if any(candidate["missing"] for candidate in candidates):
        raise ValueError("REVIEWED_UNIVERSE_EVIDENCE_MISSING")
    content = {
        "schema": "agent_paper_reviewed_universe_v1",
        "key": _KEY,
        "source": _SOURCE,
        "effective_from": effective_from.isoformat(),
        "members": evidence,
    }
    digest = sha256(
        json.dumps(content, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    version = f"reviewed_v1_{digest[:16]}"
    existing = await session.scalar(
        select(UniverseSnapshot).where(
            UniverseSnapshot.key == _KEY,
            UniverseSnapshot.version == version,
        )
    )
    if existing is not None:
        members = tuple(
            await session.scalars(
                select(UniverseMember)
                .where(UniverseMember.universe_snapshot_id == existing.id)
                .order_by(UniverseMember.instrument_id)
            )
        )
        if (
            existing.content_hash != digest
            or existing.source != _SOURCE
            or existing.effective_from != effective_from
            or existing.published_at is None
            or existing.published_at > as_of
            or existing.created_at > as_of
            or tuple(member.instrument_id for member in members) != ordered
            or any(member.created_at > as_of for member in members)
            or tuple(member.inclusion_reason for member in members)
            != tuple(
                f"research={item['research_snapshot_id']};sector={item['sector_exposure_id']}"
                for item in evidence
            )
        ):
            raise ValueError("REVIEWED_UNIVERSE_CONFLICT")
        return existing
    universe = UniverseSnapshot(
        id=uuid5(_NAMESPACE, digest),
        key=_KEY,
        version=version,
        effective_from=effective_from,
        source=_SOURCE,
        content_hash=digest,
        published_at=None,
        created_at=as_of,
    )
    session.add(universe)
    await session.flush()
    session.add_all(
        UniverseMember(
            universe_snapshot_id=universe.id,
            instrument_id=UUID(item["instrument_id"]),
            inclusion_reason=(
                f"research={item['research_snapshot_id']};sector={item['sector_exposure_id']}"
            ),
            created_at=as_of,
        )
        for item in evidence
    )
    await session.flush()
    universe.published_at = as_of
    await session.flush()
    return universe
