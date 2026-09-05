"""Append-only current listing correction; legacy identity readers remain unchanged."""

import json
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from uuid import UUID

from pydantic import AwareDatetime
from sqlalchemy import func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.time import require_utc
from nexus.market.current_identity import (
    SOURCE as IDENTITY_SOURCE,
)
from nexus.market.current_identity import (
    CurrentCommonStockIdentity,
    IdentityInputs,
    cik,
    load_identity_archives,
    member_for,
    validate_identity_originals,
)
from nexus.market.current_identity_storage import (
    CurrentIdentityImportResult,
    CurrentIdentityProof,
    IdentityStore,
    _archive,
    _binding,
    _read_artifact,
)
from nexus.market.scope import US_EQUITY_EXCHANGES
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.market import Instrument, InstrumentAlias

SOURCE = "current_listing_revision"
SCHEMA = "current_listing_revision_v1"


class CurrentListingRevision(CurrentIdentityProof):
    original_exchange: str
    instrument_fingerprint: str
    alias_fingerprint: str
    effective_at: AwareDatetime


def _fingerprint(row: Instrument | InstrumentAlias) -> str:
    values = {c.name: str(getattr(row, c.name)) for c in row.__table__.columns}
    return sha256(json.dumps(values, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _revision(inputs: IdentityInputs, instrument: Instrument, alias: InstrumentAlias) -> str:
    return sha256(
        (
            SCHEMA
            + ":"
            + inputs.revision
            + ":"
            + _fingerprint(instrument)
            + ":"
            + _fingerprint(alias)
        ).encode()
    ).hexdigest()


async def _base_identity(
    session: AsyncSession, identity: CurrentCommonStockIdentity, cutoff: datetime
) -> tuple[Instrument, InstrumentAlias]:
    member = member_for(identity.symbol)
    aliases = tuple(
        await session.scalars(
            select(InstrumentAlias).where(
                InstrumentAlias.alias_type == "ticker",
                or_(
                    InstrumentAlias.symbol == member.symbol,
                    InstrumentAlias.instrument_id == member.instrument_id,
                ),
            )
        )
    )
    current = [
        a
        for a in aliases
        if a.valid_from <= cutoff.date() and (a.valid_to is None or a.valid_to >= cutoff.date())
    ]
    if len(current) != 1:
        raise ValueError("CURRENT_LISTING_MASTER_CONFLICT")
    alias = current[0]
    current_binding = tuple(
        await session.scalars(
            select(InstrumentAlias).where(
                InstrumentAlias.alias_type == "ticker",
                or_(
                    InstrumentAlias.symbol == member.symbol,
                    InstrumentAlias.instrument_id == alias.instrument_id,
                ),
                InstrumentAlias.valid_from <= cutoff.date(),
                or_(InstrumentAlias.valid_to.is_(None), InstrumentAlias.valid_to >= cutoff.date()),
            )
        )
    )
    if len(current_binding) != 1 or current_binding[0].id != alias.id:
        raise ValueError("CURRENT_LISTING_MASTER_CONFLICT")
    expected = await session.get(Instrument, member.instrument_id)
    instrument = await session.get(Instrument, alias.instrument_id)
    if (
        instrument is None
        or instrument.created_at > cutoff
        or alias.created_at > cutoff
        or any(a.symbol != member.symbol or a.instrument_id != alias.instrument_id for a in aliases)
        or (expected is not None and expected.id != alias.instrument_id)
        or instrument.exchange not in US_EQUITY_EXCHANGES
        or instrument.asset_type != "common_stock"
        or instrument.currency != "USD"
        or instrument.risk_bucket != member.risk_bucket
    ):
        raise ValueError("CURRENT_LISTING_MASTER_CONFLICT")
    listing = {"XNYS": {"XNYS", "NYSE"}, "XNAS": {"XNAS", "NASDAQ"}}
    if instrument.exchange in listing[identity.exchange]:
        raise ValueError("CURRENT_LISTING_REVISION_NOT_REQUIRED")
    identifiers = await session.scalars(
        select(InstrumentAlias).where(
            InstrumentAlias.instrument_id == instrument.id,
            InstrumentAlias.alias_type.in_(("cik", "isin", "cusip")),
        )
    )
    for known in identifiers:
        value = getattr(identity, known.alias_type)
        if (known.alias_type == "cik" and cik(known.symbol) != value) or (
            known.alias_type != "cik" and value is not None and known.symbol != value
        ):
            raise ValueError("CURRENT_LISTING_SOURCE_CONFLICT")
    previous = await session.scalars(
        select(DataSnapshot).where(
            DataSnapshot.source.in_((IDENTITY_SOURCE, SOURCE)),
            DataSnapshot.provider_object_id == f"identity:{identity.symbol}",
        )
    )
    for snapshot in previous:
        model = CurrentListingRevision if snapshot.source == SOURCE else CurrentIdentityProof
        proof = model.model_validate(snapshot.payload)
        if (
            proof.instrument_id != instrument.id
            or proof.identity.cik != identity.cik
            or any(
                getattr(proof.identity, key) is not None
                and getattr(identity, key) is not None
                and getattr(proof.identity, key) != getattr(identity, key)
                for key in ("isin", "cusip")
            )
        ):
            raise ValueError("CURRENT_LISTING_SOURCE_CONFLICT")
    return instrument, alias


async def import_listing_revision(
    session: AsyncSession,
    store: IdentityStore,
    *,
    symbol: str,
    fmp_archive: Path,
    sec_archive: Path,
) -> CurrentIdentityImportResult:
    async with session.begin_nested():
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:symbol, 0))"),
            {"symbol": f"current_identity:{symbol}"},
        )
        cutoff = await session.scalar(select(func.clock_timestamp()))
        assert isinstance(cutoff, datetime)
        inputs = load_identity_archives(
            symbol=symbol, fmp_archive=fmp_archive, sec_archive=sec_archive, now=cutoff
        )
        instrument, alias = await _base_identity(session, inputs.identity, cutoff)
        revision = _revision(inputs, instrument, alias)
        previous = await session.scalar(
            select(DataSnapshot).where(
                DataSnapshot.source == SOURCE,
                DataSnapshot.provider_object_id == f"identity:{symbol}",
                DataSnapshot.revision == revision,
            )
        )
        if previous is not None:
            await read_listing_revision(session, store, snapshot_id=previous.id)
            return CurrentIdentityImportResult("existing", instrument.id, previous.id)
        artifacts = await _archive(session, store, inputs)
        checked = await session.scalar(select(func.clock_timestamp()))
        assert isinstance(checked, datetime)
        proof = CurrentListingRevision(
            identity=inputs.identity,
            instrument_id=instrument.id,
            alias_id=alias.id,
            alias_valid_from=alias.valid_from,
            checked_at=checked,
            effective_at=checked,
            definition_instrument_id=member_for(symbol).instrument_id,
            sec_url=inputs.sec_url,
            artifacts=artifacts,
            original_exchange=instrument.exchange,
            instrument_fingerprint=_fingerprint(instrument),
            alias_fingerprint=_fingerprint(alias),
        )
        snapshot = DataSnapshot(
            source=SOURCE,
            schema_version=SCHEMA,
            provider_object_id=f"identity:{symbol}",
            revision=revision,
            observed_at=checked,
            received_at=checked,
            available_at=checked,
            payload=proof.model_dump(mode="json"),
        )
        session.add(snapshot)
        await session.flush()
        await read_listing_revision(session, store, snapshot_id=snapshot.id)
        return CurrentIdentityImportResult("created", instrument.id, snapshot.id)


async def read_listing_revision(
    session: AsyncSession,
    store: IdentityStore,
    *,
    snapshot_id: UUID,
    as_of: datetime | None = None,
) -> CurrentListingRevision:
    try:
        now = await session.scalar(select(func.clock_timestamp()))
        assert isinstance(now, datetime)
        cutoff = require_utc(as_of or now)
        snapshot = await session.get(DataSnapshot, snapshot_id)
        if (
            snapshot is None
            or snapshot.source != SOURCE
            or snapshot.schema_version != SCHEMA
            or cutoff > now
            or snapshot.available_at > cutoff
            or snapshot.created_at > cutoff
            or snapshot.received_at > cutoff
            or snapshot.observed_at > cutoff
        ):
            raise ValueError
        proof = CurrentListingRevision.model_validate(snapshot.payload)
        if (
            proof.effective_at != proof.checked_at
            or (snapshot.observed_at, snapshot.received_at, snapshot.available_at)
            != (proof.checked_at,) * 3
        ):
            raise ValueError
        contents = []
        for binding in proof.artifacts:
            artifact = await session.scalar(
                select(DataArtifact).where(DataArtifact.sha256 == binding.sha256)
            )
            if artifact is None or _binding(artifact) != binding:
                raise ValueError
            contents.append(await _read_artifact(store, binding))
        inputs = validate_identity_originals(
            symbol=proof.identity.symbol,
            contents=(contents[0], contents[1], contents[2], contents[3]),
            now=proof.checked_at,
        )
        instrument, alias = await _base_identity(session, inputs.identity, now)
        if (
            inputs.identity != proof.identity
            or inputs.sec_url != proof.sec_url
            or snapshot.provider_object_id != f"identity:{proof.identity.symbol}"
            or snapshot.revision != _revision(inputs, instrument, alias)
            or proof.instrument_id != instrument.id
            or proof.alias_id != alias.id
            or proof.alias_valid_from != alias.valid_from
            or proof.definition_instrument_id != member_for(proof.identity.symbol).instrument_id
            or proof.original_exchange != instrument.exchange
            or proof.instrument_fingerprint != _fingerprint(instrument)
            or proof.alias_fingerprint != _fingerprint(alias)
        ):
            raise ValueError
        return proof
    except (ValueError, TypeError, KeyError) as error:
        raise ValueError("CURRENT_LISTING_READBACK_INVALID") from error
