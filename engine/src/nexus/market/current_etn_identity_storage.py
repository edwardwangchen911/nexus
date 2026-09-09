"""Persist one current-held ETN identity without publishing a tradable universe member."""

from datetime import date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict
from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.market.current_etf_identity_storage import (
    CurrentHoldingBinding,
    _current_aliases,
    _private_files,
    _validate_frozen_holding_binding,
    current_holding_binding,
)
from nexus.market.current_etn_identity import (
    SCHEMA,
    SOURCE,
    CurrentHeldEtnIdentity,
    CurrentHeldEtnInputs,
    validate_current_held_etn_originals,
)
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


class CurrentHeldEtnProof(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    identity: CurrentHeldEtnIdentity
    instrument_id: UUID
    alias_id: UUID
    alias_valid_from: date
    checked_at: AwareDatetime
    scope: Literal["fosun_current_holding"] = "fosun_current_holding"
    holding: CurrentHoldingBinding
    sec_url: str
    artifacts: tuple[ArtifactBinding, ...]
    directory_match: Literal[False] = False
    universe_published: Literal[False] = False
    new_position_eligible: Literal[False] = False
    historical_backtest_eligible: Literal[False] = False
    execution_locked: Literal[True] = True


def load_current_held_etn_archives(
    *, symbol: str, sec_archive: Path, now: datetime
) -> CurrentHeldEtnInputs:
    contents = _private_files(
        sec_archive,
        ("submissions.raw", "filing-index.raw", "disclosure.raw", "receipts.json"),
    )
    return validate_current_held_etn_originals(symbol=symbol, contents=contents, now=now)


def _revision(inputs: CurrentHeldEtnInputs, holding: CurrentHoldingBinding) -> str:
    return sha256((SCHEMA + inputs.revision + holding.sha256).encode()).hexdigest()


async def import_current_held_etn(
    session: AsyncSession,
    store: IdentityStore,
    *,
    symbol: str,
    sec_archive: Path,
    accounts_page: GatewayResponsePage,
    holdings_page: GatewayResponsePage,
) -> CurrentIdentityImportResult:
    """Persist only a fresh broker-held ETN backed by its final SEC 424B2."""

    async with session.begin_nested():
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:symbol, 0))"),
            {"symbol": f"current_identity:{symbol}"},
        )
        now = await session.scalar(select(func.clock_timestamp()))
        assert isinstance(now, datetime)
        try:
            holding = current_holding_binding(
                symbol=symbol,
                accounts_page=accounts_page,
                holdings_page=holdings_page,
                now=now,
            )
        except ValueError:
            raise ValueError("CURRENT_HELD_ETN_HOLDING_INVALID") from None
        inputs = load_current_held_etn_archives(symbol=symbol, sec_archive=sec_archive, now=now)
        instrument_id = UUID(str(stable_instrument_id(symbol)))
        revision = _revision(inputs, holding)
        existing = await session.scalar(
            select(DataSnapshot).where(
                DataSnapshot.source == SOURCE,
                DataSnapshot.provider_object_id == f"identity:{symbol}",
                DataSnapshot.revision == revision,
            )
        )
        if existing is not None:
            proof = await read_current_held_etn(session, store, snapshot_id=existing.id)
            return CurrentIdentityImportResult("existing", proof.instrument_id, existing.id)

        instrument = await session.get(Instrument, instrument_id)
        aliases = await _current_aliases(session, symbol, instrument_id, now)
        if instrument is not None or aliases:
            if (
                instrument is None
                or instrument.asset_type != "exchange_traded_note"
                or instrument.exchange != inputs.identity.exchange
                or instrument.currency != "USD"
                or instrument.risk_bucket != "EXCHANGE_TRADED_NOTE"
                or instrument.tick_size is not None
                or len(aliases) != 1
                or aliases[0].instrument_id != instrument_id
                or aliases[0].symbol != symbol
            ):
                raise ValueError("CURRENT_ETN_MASTER_CONFLICT")
            prior_snapshots = tuple(
                await session.scalars(
                    select(DataSnapshot).where(
                        DataSnapshot.source == SOURCE,
                        DataSnapshot.provider_object_id == f"identity:{symbol}",
                    )
                )
            )
            for prior_snapshot in prior_snapshots:
                prior = CurrentHeldEtnProof.model_validate(prior_snapshot.payload)
                if (
                    prior.identity.symbol,
                    prior.identity.cik,
                    prior.identity.note_title_sha256,
                    prior.identity.exchange,
                ) != (
                    inputs.identity.symbol,
                    inputs.identity.cik,
                    inputs.identity.note_title_sha256,
                    inputs.identity.exchange,
                ):
                    raise ValueError("CURRENT_ETN_MASTER_CONFLICT")
            alias = aliases[0]
        else:
            conflicting_alias = await session.scalar(
                select(InstrumentAlias.id)
                .where(InstrumentAlias.alias_type == "ticker", InstrumentAlias.symbol == symbol)
                .limit(1)
            )
            if conflicting_alias is not None:
                raise ValueError("CURRENT_ETN_MASTER_CONFLICT")

        artifacts: list[ArtifactBinding] = []
        for number, content in enumerate(inputs.contents):
            digest = sha256(content).hexdigest()
            artifact = await session.scalar(
                select(DataArtifact).where(DataArtifact.sha256 == digest)
            )
            received = inputs.received[number] if number < 3 else max(inputs.received)
            if artifact is None:
                uri = await store.put_if_absent(
                    key=f"raw/sec/current-held-etn/{digest}.raw", content=content
                )
                await session.execute(
                    insert(DataArtifact)
                    .values(
                        uri=uri,
                        sha256=digest,
                        byte_size=len(content),
                        source="sec",
                        schema_version=f"current_held_etn_original_{number}_v1",
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
            if artifact is None or artifact.source != "sec":
                raise ValueError("CURRENT_ETN_ARTIFACT_CONFLICT")
            binding = _binding(artifact)
            if await _read_artifact(store, binding) != content:
                raise ValueError("CURRENT_ETN_ARTIFACT_CONFLICT")
            artifacts.append(binding)

        checked = await session.scalar(select(func.clock_timestamp()))
        assert isinstance(checked, datetime)
        if instrument is None:
            instrument = Instrument(
                id=instrument_id,
                asset_type="exchange_traded_note",
                exchange=inputs.identity.exchange,
                currency="USD",
                tick_size=None,
                risk_bucket="EXCHANGE_TRADED_NOTE",
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
        proof = CurrentHeldEtnProof(
            identity=inputs.identity,
            instrument_id=instrument.id,
            alias_id=alias.id,
            alias_valid_from=alias.valid_from,
            checked_at=checked,
            holding=holding,
            sec_url=inputs.sec_url,
            artifacts=tuple(artifacts),
        )
        snapshot = DataSnapshot(
            source=SOURCE,
            provider_object_id=f"identity:{symbol}",
            revision=revision,
            schema_version=SCHEMA,
            observed_at=checked,
            received_at=checked,
            available_at=checked,
            payload=proof.model_dump(mode="json"),
        )
        session.add(snapshot)
        await session.flush()
        await read_current_held_etn(session, store, snapshot_id=snapshot.id)
        return CurrentIdentityImportResult("created", instrument.id, snapshot.id)


async def read_current_held_etn(
    session: AsyncSession, store: IdentityStore, *, snapshot_id: UUID
) -> CurrentHeldEtnProof:
    try:
        snapshot = await session.get(DataSnapshot, snapshot_id)
        now = await session.scalar(select(func.clock_timestamp()))
        assert isinstance(now, datetime)
        if snapshot is None or snapshot.source != SOURCE or snapshot.schema_version != SCHEMA:
            raise ValueError
        proof = CurrentHeldEtnProof.model_validate(snapshot.payload)
        _validate_frozen_holding_binding(
            proof.holding, symbol=proof.identity.symbol, now=proof.checked_at
        )
        if (
            (snapshot.observed_at, snapshot.received_at, snapshot.available_at)
            != (proof.checked_at,) * 3
            or proof.checked_at > now
            or snapshot.created_at > now
            or len(proof.artifacts) != 4
            or proof.directory_match is not False
        ):
            raise ValueError
        contents: list[bytes] = []
        for binding in proof.artifacts:
            artifact = await session.scalar(
                select(DataArtifact).where(DataArtifact.sha256 == binding.sha256)
            )
            if artifact is None or artifact.source != "sec" or _binding(artifact) != binding:
                raise ValueError
            contents.append(await _read_artifact(store, binding))
        inputs = validate_current_held_etn_originals(
            symbol=proof.identity.symbol,
            contents=tuple(contents),
            now=proof.checked_at,
        )
        instrument = await session.get(Instrument, proof.instrument_id)
        aliases = await _current_aliases(
            session, proof.identity.symbol, proof.instrument_id, proof.checked_at
        )
        if (
            inputs.identity != proof.identity
            or _revision(inputs, proof.holding) != snapshot.revision
            or inputs.sec_url != proof.sec_url
            or proof.instrument_id != UUID(str(stable_instrument_id(proof.identity.symbol)))
            or snapshot.provider_object_id != f"identity:{proof.identity.symbol}"
            or instrument is None
            or instrument.asset_type != "exchange_traded_note"
            or instrument.exchange != proof.identity.exchange
            or instrument.currency != "USD"
            or instrument.risk_bucket != "EXCHANGE_TRADED_NOTE"
            or instrument.tick_size is not None
            or len(aliases) != 1
            or aliases[0].id != proof.alias_id
            or aliases[0].valid_from != proof.alias_valid_from
            or proof.alias_valid_from > proof.checked_at.date()
        ):
            raise ValueError
        return proof
    except (ValueError, TypeError, KeyError) as error:
        raise ValueError("CURRENT_HELD_ETN_READBACK_INVALID") from error
