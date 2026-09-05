"""Append a first equity ETF identity and immutable originals; existing masters stay intact."""

import os
from datetime import date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict
from sqlalchemy import func, or_, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.market.current_etf_identity import (
    SCHEMA,
    SOURCE,
    CurrentEtfIdentity,
    EtfInputs,
    member_for,
    validate_etf_originals,
)
from nexus.market.current_identity import MAX_SEC_BYTES
from nexus.market.current_identity_storage import (
    ArtifactBinding,
    CurrentIdentityImportResult,
    IdentityStore,
    _binding,
    _read_artifact,
)
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.research.fmp_profile_archive import _read


class EtfProof(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    identity: CurrentEtfIdentity
    instrument_id: UUID
    alias_id: UUID
    alias_valid_from: date
    checked_at: AwareDatetime
    definition_instrument_id: UUID
    sec_url: str
    artifacts: tuple[ArtifactBinding, ...]
    historical_backtest_eligible: Literal[False] = False
    execution_locked: Literal[True] = True


def _private_files(directory: Path, names: tuple[str, ...]) -> tuple[bytes, ...]:
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        if os.fstat(fd).st_mode & 0o077:
            raise ValueError("CURRENT_ETF_ARCHIVE_INVALID")
        return tuple(_read(fd, name, MAX_SEC_BYTES) for name in names)
    finally:
        os.close(fd)


async def load_etf_archives(
    *, symbol: str, fmp_archive: Path, sec_archive: Path, mapping_file: Path, now: datetime
) -> EtfInputs:
    contents = _private_files(fmp_archive, ("profile.json", "receipt.json"))
    mapping = _private_files(mapping_file.parent, (mapping_file.name,))
    sec = _private_files(
        sec_archive,
        (
            "submissions.raw",
            "filing-index.raw",
            "disclosure.raw",
            "receipts.json",
            "mapping-binding.json",
        ),
    )
    return await validate_etf_originals(symbol=symbol, contents=contents + mapping + sec, now=now)


async def _current_aliases(
    session: AsyncSession, symbol: str, instrument_id: UUID, now: datetime
) -> tuple[InstrumentAlias, ...]:
    return tuple(
        await session.scalars(
            select(InstrumentAlias).where(
                InstrumentAlias.alias_type == "ticker",
                or_(
                    InstrumentAlias.symbol == symbol, InstrumentAlias.instrument_id == instrument_id
                ),
                InstrumentAlias.valid_from <= now.date(),
                or_(InstrumentAlias.valid_to.is_(None), InstrumentAlias.valid_to >= now.date()),
            )
        )
    )


async def import_current_etf(
    session: AsyncSession,
    store: IdentityStore,
    *,
    symbol: str,
    fmp_archive: Path,
    sec_archive: Path,
    mapping_file: Path,
) -> CurrentIdentityImportResult:
    member = member_for(symbol)
    async with session.begin_nested():
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:symbol, 0))"),
            {"symbol": f"current_identity:{symbol}"},
        )
        now = await session.scalar(select(func.clock_timestamp()))
        assert isinstance(now, datetime)
        inputs = await load_etf_archives(
            symbol=symbol,
            fmp_archive=fmp_archive,
            sec_archive=sec_archive,
            mapping_file=mapping_file,
            now=now,
        )
        existing = await session.scalar(
            select(DataSnapshot).where(
                DataSnapshot.source == SOURCE,
                DataSnapshot.provider_object_id == f"identity:{symbol}",
                DataSnapshot.revision == inputs.revision,
            )
        )
        if existing is not None:
            proof = await read_current_etf(session, store, snapshot_id=existing.id)
            return CurrentIdentityImportResult("existing", proof.instrument_id, existing.id)
        # This first-identity slice deliberately does not correct or supersede existing masters.
        if await session.get(Instrument, member.instrument_id) is not None or await session.scalar(
            select(InstrumentAlias.id)
            .where(InstrumentAlias.alias_type == "ticker", InstrumentAlias.symbol == symbol)
            .limit(1)
        ):
            raise ValueError("CURRENT_ETF_MASTER_CONFLICT")
        artifacts = []
        for number, content in enumerate(inputs.contents):
            digest = sha256(content).hexdigest()
            source = "fmp" if number < 2 else "sec"
            artifact = await session.scalar(
                select(DataArtifact).where(DataArtifact.sha256 == digest)
            )
            if artifact is None:
                uri = await store.put_if_absent(
                    key=f"raw/{source}/current-etf/{digest}.raw", content=content
                )
                await session.execute(
                    insert(DataArtifact)
                    .values(
                        uri=uri,
                        sha256=digest,
                        byte_size=len(content),
                        source=source,
                        schema_version=f"current_etf_original_{number}_v1",
                        mime_type="application/octet-stream",
                        received_at=inputs.received[number] or now,
                        observed_at=inputs.received[number] or now,
                        available_at=inputs.received[number] or now,
                    )
                    .on_conflict_do_nothing(index_elements=["sha256"])
                )
                artifact = await session.scalar(
                    select(DataArtifact).where(DataArtifact.sha256 == digest)
                )
            if artifact is None or artifact.source != source:
                raise ValueError("CURRENT_ETF_ARTIFACT_CONFLICT")
            binding = _binding(artifact)
            if await _read_artifact(store, binding) != content:
                raise ValueError("CURRENT_ETF_ARTIFACT_CONFLICT")
            artifacts.append(binding)
        checked = await session.scalar(select(func.clock_timestamp()))
        assert isinstance(checked, datetime)
        instrument = Instrument(
            id=member.instrument_id,
            asset_type="equity_etf",
            exchange="ARCX",
            currency="USD",
            tick_size=None,
            risk_bucket=member.risk_bucket,
        )
        session.add(instrument)
        await session.flush()
        alias = InstrumentAlias(
            instrument_id=instrument.id,
            alias_type="ticker",
            symbol=symbol,
            valid_from=checked.date(),
            valid_to=None,
        )
        session.add(alias)
        await session.flush()
        proof = EtfProof(
            identity=inputs.identity,
            instrument_id=instrument.id,
            alias_id=alias.id,
            checked_at=checked,
            alias_valid_from=alias.valid_from,
            definition_instrument_id=member.instrument_id,
            sec_url=inputs.sec_url,
            artifacts=tuple(artifacts),
        )
        snapshot = DataSnapshot(
            source=SOURCE,
            provider_object_id=f"identity:{symbol}",
            revision=inputs.revision,
            schema_version=SCHEMA,
            observed_at=checked,
            received_at=checked,
            available_at=checked,
            payload=proof.model_dump(mode="json"),
        )
        session.add(snapshot)
        await session.flush()
        await read_current_etf(session, store, snapshot_id=snapshot.id)
        return CurrentIdentityImportResult("created", instrument.id, snapshot.id)


async def read_current_etf(
    session: AsyncSession, store: IdentityStore, *, snapshot_id: UUID
) -> EtfProof:
    try:
        snapshot = await session.get(DataSnapshot, snapshot_id)
        now = await session.scalar(select(func.clock_timestamp()))
        assert isinstance(now, datetime)
        if snapshot is None or snapshot.source != SOURCE or snapshot.schema_version != SCHEMA:
            raise ValueError
        proof = EtfProof.model_validate(snapshot.payload)
        if (
            (snapshot.observed_at, snapshot.received_at, snapshot.available_at)
            != (proof.checked_at,) * 3
            or proof.checked_at > now
            or snapshot.created_at > now
            or len(proof.artifacts) != 8
        ):
            raise ValueError
        contents = []
        for number, binding in enumerate(proof.artifacts):
            artifact = await session.scalar(
                select(DataArtifact).where(DataArtifact.sha256 == binding.sha256)
            )
            if (
                artifact is None
                or _binding(artifact) != binding
                or artifact.source != ("fmp" if number < 2 else "sec")
            ):
                raise ValueError
            contents.append(await _read_artifact(store, binding))
        inputs = await validate_etf_originals(
            symbol=proof.identity.symbol, contents=tuple(contents), now=proof.checked_at
        )
        member = member_for(proof.identity.symbol)
        if (
            inputs.identity != proof.identity
            or inputs.revision != snapshot.revision
            or inputs.sec_url != proof.sec_url
            or proof.definition_instrument_id != member.instrument_id
            or proof.instrument_id != member.instrument_id
            or snapshot.provider_object_id != f"identity:{proof.identity.symbol}"
        ):
            raise ValueError
        instrument = await session.get(Instrument, proof.instrument_id)
        aliases = await _current_aliases(session, proof.identity.symbol, proof.instrument_id, now)
        if (
            instrument is None
            or instrument.asset_type != "equity_etf"
            or instrument.exchange != "ARCX"
            or instrument.currency != "USD"
            or instrument.risk_bucket != member.risk_bucket
            or instrument.tick_size is not None
            or len(aliases) != 1
            or aliases[0].id != proof.alias_id
            or aliases[0].symbol != proof.identity.symbol
            or aliases[0].instrument_id != instrument.id
            or aliases[0].valid_from != proof.alias_valid_from
            or proof.alias_valid_from != proof.checked_at.date()
        ):
            raise ValueError
        return proof
    except (ValueError, TypeError, KeyError) as error:
        raise ValueError("CURRENT_ETF_READBACK_INVALID") from error
