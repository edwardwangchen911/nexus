"""Append held-only Cboe identity with immutable originals and strict readback.

Uses existing master/artifact/snapshot tables; never creates an account snapshot,
research universe, quote, order or execution permission.
"""

import json
from datetime import date, datetime
from hashlib import sha256
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.market.cboe_holding_evidence import (
    CboeHoldingEvidence,
    freeze_cboe_holding_evidence,
    read_cboe_holding_evidence,
)
from nexus.market.current_etf_identity_storage import _current_aliases
from nexus.market.current_identity_storage import (
    ArtifactBinding,
    CurrentIdentityImportResult,
    IdentityStore,
    _binding,
    _read_artifact,
)
from nexus.market.universe import stable_instrument_id
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.providers.broker.gateway_contracts import GatewayResponsePage

SOURCE = "current_held_cboe_fund_identity"
SCHEMA: Literal["held_cboe_fund_identity_v1"] = "held_cboe_fund_identity_v1"


class CboeHeldIdentityProof(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    evidence: CboeHoldingEvidence
    instrument_id: UUID
    alias_id: UUID
    alias_valid_from: date
    artifacts: tuple[ArtifactBinding, ...]


def _revision(evidence: CboeHoldingEvidence) -> str:
    # The same original source bytes and same broker request cannot create a new
    # identity fact just because the registration command was invoked later.
    return sha256(
        (SCHEMA + evidence.source_revision + evidence.holding.sha256).encode()
    ).hexdigest()


def _source(number: int, count: int) -> str:
    if number < 2:
        return "fmp"
    if number < 4 or number >= count - 4:
        return "sec"
    return "cboe" if 8 <= number < 14 else "iso"


def _master_matches(instrument: Instrument, proof: CboeHeldIdentityProof) -> bool:
    return (
        instrument.id == proof.instrument_id
        and instrument.asset_type == "equity_etf"
        and instrument.exchange == "BATS"
        and instrument.currency == "USD"
        and instrument.risk_bucket == "EQUITY"
        and instrument.tick_size is None
    )


async def read_cboe_held_identity(
    session: AsyncSession,
    store: IdentityStore,
    *,
    snapshot_id: UUID,
) -> CboeHeldIdentityProof:
    try:
        snapshot = await session.get(DataSnapshot, snapshot_id)
        now = await session.scalar(select(func.clock_timestamp()))
        if (
            not isinstance(now, datetime)
            or snapshot is None
            or (snapshot.source, snapshot.schema_version) != (SOURCE, SCHEMA)
        ):
            raise ValueError
        proof = CboeHeldIdentityProof.model_validate(snapshot.payload)
        checked = proof.evidence.checked_at
        symbol = proof.evidence.identity.symbol
        if (
            len(proof.artifacts) not in {18, 22}
            or len(proof.artifacts) != len(proof.evidence.artifact_sha256)
            or (snapshot.observed_at, snapshot.received_at, snapshot.available_at) != (checked,) * 3
            or snapshot.created_at > now
            or snapshot.provider_object_id != f"identity:{symbol}"
            or snapshot.revision != _revision(proof.evidence)
            or proof.instrument_id != UUID(str(stable_instrument_id(symbol)))
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
                or artifact.source != _source(number, len(proof.artifacts))
                or binding.sha256 != proof.evidence.artifact_sha256[number]
            ):
                raise ValueError
            contents.append(await _read_artifact(store, binding))
        read_cboe_holding_evidence(
            proof.evidence.model_dump(mode="json"),
            contents=tuple(contents[:-4]),
            directory_contents=tuple(contents[-4:]),
            now=now,
        )
        instrument = await session.get(Instrument, proof.instrument_id)
        aliases = await _current_aliases(session, symbol, proof.instrument_id, now)
        if (
            instrument is None
            or not _master_matches(instrument, proof)
            or len(aliases) != 1
            or aliases[0].id != proof.alias_id
            or aliases[0].instrument_id != proof.instrument_id
            or aliases[0].symbol != symbol
            or aliases[0].valid_from != proof.alias_valid_from
            or proof.alias_valid_from > checked.date()
        ):
            raise ValueError
        return proof
    except (ValueError, TypeError, KeyError):
        raise ValueError("CBOE_HELD_IDENTITY_READBACK_INVALID") from None


async def import_cboe_held_identity(
    session: AsyncSession,
    store: IdentityStore,
    *,
    symbol: str,
    contents: tuple[bytes, ...],
    directory_contents: tuple[bytes, ...],
    accounts_page: GatewayResponsePage,
    holdings_page: GatewayResponsePage,
) -> CurrentIdentityImportResult:
    async with session.begin_nested():
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:symbol, 0))"),
            {"symbol": f"current_identity:{symbol}"},
        )
        now = await session.scalar(select(func.clock_timestamp()))
        if not isinstance(now, datetime):
            raise ValueError("CBOE_HELD_IDENTITY_CLOCK_INVALID")
        evidence = freeze_cboe_holding_evidence(
            symbol=symbol,
            contents=contents,
            directory_contents=directory_contents,
            accounts_page=accounts_page,
            holdings_page=holdings_page,
            now=now,
        )
        revision = _revision(evidence)
        existing = await session.scalar(
            select(DataSnapshot).where(
                DataSnapshot.source == SOURCE,
                DataSnapshot.provider_object_id == f"identity:{symbol}",
                DataSnapshot.revision == revision,
            )
        )
        if existing is not None:
            proof = await read_cboe_held_identity(session, store, snapshot_id=existing.id)
            return CurrentIdentityImportResult("existing", proof.instrument_id, existing.id)
        instrument_id = UUID(str(stable_instrument_id(symbol)))
        instrument = await session.get(Instrument, instrument_id)
        aliases = await _current_aliases(session, symbol, instrument_id, now)
        prior_snapshots = tuple(
            await session.scalars(
                select(DataSnapshot).where(
                    DataSnapshot.source == SOURCE,
                    DataSnapshot.provider_object_id == f"identity:{symbol}",
                )
            )
        )
        if instrument is not None or aliases or prior_snapshots:
            if instrument is None or not prior_snapshots:
                raise ValueError("CBOE_HELD_IDENTITY_MASTER_CONFLICT")
            for prior in prior_snapshots:
                proof = await read_cboe_held_identity(session, store, snapshot_id=prior.id)
                if proof.evidence.identity != evidence.identity:
                    raise ValueError("CBOE_HELD_IDENTITY_MASTER_CONFLICT")
            if len(aliases) != 1:
                raise ValueError("CBOE_HELD_IDENTITY_MASTER_CONFLICT")
            alias = aliases[0]
        else:
            historical = await session.scalar(
                select(InstrumentAlias.id)
                .where(InstrumentAlias.alias_type == "ticker", InstrumentAlias.symbol == symbol)
                .limit(1)
            )
            if historical is not None:
                raise ValueError("CBOE_HELD_IDENTITY_MASTER_CONFLICT")
            instrument = Instrument(
                id=instrument_id,
                asset_type="equity_etf",
                exchange="BATS",
                currency="USD",
                risk_bucket="EQUITY",
                tick_size=None,
            )
            session.add(instrument)
            await session.flush()
            alias = InstrumentAlias(
                instrument_id=instrument_id,
                symbol=symbol,
                alias_type="ticker",
                valid_from=now.date(),
                valid_to=None,
            )
            session.add(alias)
            await session.flush()
        artifacts = []
        all_contents = contents + directory_contents
        for number, content in enumerate(all_contents):
            digest = sha256(content).hexdigest()
            source = _source(number, len(all_contents))
            # Every raw/receipt pair was already validated by the source gate.
            # Preserve the capture time instead of making historical bytes new.
            received = datetime.fromisoformat(
                json.loads(all_contents[number - number % 2 + 1])["received_at"]
            )
            artifact = await session.scalar(
                select(DataArtifact).where(DataArtifact.sha256 == digest)
            )
            if artifact is None:
                uri = await store.put_if_absent(
                    key=f"raw/{source}/current-held-cboe-fund/{digest}.raw", content=content
                )
                await session.execute(
                    insert(DataArtifact)
                    .values(
                        uri=uri,
                        sha256=digest,
                        byte_size=len(content),
                        source=source,
                        schema_version=f"held_cboe_original_{number}_v1",
                        mime_type="application/octet-stream",
                        received_at=received,
                        observed_at=received,
                        available_at=received,
                    )
                    .on_conflict_do_nothing(index_elements=["sha256"])
                )
                artifact = await session.scalar(
                    select(DataArtifact).where(DataArtifact.sha256 == digest)
                )
            if artifact is None or artifact.source != source:
                raise ValueError("CBOE_HELD_IDENTITY_ARTIFACT_CONFLICT")
            binding = _binding(artifact)
            if await _read_artifact(store, binding) != content:
                raise ValueError("CBOE_HELD_IDENTITY_ARTIFACT_CONFLICT")
            artifacts.append(binding)
        proof = CboeHeldIdentityProof(
            evidence=evidence,
            instrument_id=instrument_id,
            alias_id=alias.id,
            alias_valid_from=alias.valid_from,
            artifacts=tuple(artifacts),
        )
        snapshot = DataSnapshot(
            source=SOURCE,
            provider_object_id=f"identity:{symbol}",
            revision=revision,
            schema_version=SCHEMA,
            observed_at=now,
            received_at=now,
            available_at=now,
            payload=proof.model_dump(mode="json"),
        )
        session.add(snapshot)
        await session.flush()
        await read_cboe_held_identity(session, store, snapshot_id=snapshot.id)
        return CurrentIdentityImportResult("created", instrument_id, snapshot.id)
