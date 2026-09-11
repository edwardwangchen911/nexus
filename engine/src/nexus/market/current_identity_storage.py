"""Append one current identity proof, preserving existing identities and archived bytes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal, Protocol
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict
from sqlalchemy import func, or_, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.market.current_identity import (
    SCHEMA,
    SOURCE,
    CurrentCommonStockIdentity,
    IdentityInputs,
    cik,
    load_identity_archives,
    member_for,
    validate_identity_originals,
)
from nexus.market.custom_identity import (
    SCHEMA as CUSTOM_SCHEMA,
)
from nexus.market.custom_identity import (
    CustomIdentityInputs,
    load_custom_identity_archives,
    validate_custom_identity_originals,
)
from nexus.market.scope import US_EQUITY_EXCHANGES
from nexus.market.universe import stable_instrument_id
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.providers.broker.fosun_contracts import AccountItem, PositionItem
from nexus.providers.broker.gateway_contracts import GatewayResponsePage


class IdentityStore(Protocol):
    async def put_if_absent(self, *, key: str, content: bytes) -> str: ...
    async def read_raw(self, key: str, *, uri: str) -> bytes: ...


class ArtifactBinding(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    uri: str
    sha256: str
    byte_size: int
    source: str
    schema_version: str


class CurrentIdentityProof(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    identity: CurrentCommonStockIdentity
    instrument_id: UUID
    alias_id: UUID
    alias_valid_from: date
    checked_at: AwareDatetime
    definition: Literal["us_liquid_v1"] = "us_liquid_v1"
    definition_instrument_id: UUID
    sec_url: str
    artifacts: tuple[ArtifactBinding, ArtifactBinding, ArtifactBinding, ArtifactBinding]
    historical_backtest_eligible: Literal[False] = False
    execution_locked: Literal[True] = True


@dataclass(frozen=True)
class CurrentIdentityImportResult:
    status: Literal["created", "existing"]
    instrument_id: UUID
    snapshot_id: UUID


class CustomMemberIdentityProof(CurrentIdentityProof):
    taxonomy_artifacts: tuple[
        ArtifactBinding,
        ArtifactBinding,
        ArtifactBinding,
        ArtifactBinding,
        ArtifactBinding,
        ArtifactBinding,
    ]


HELD_SOURCE = "current_held_common_stock_identity"
HELD_SCHEMA = "current_held_common_stock_v1"
HELD_BINDING_MAX_AGE = timedelta(minutes=5)


class CurrentHeldCommonStockBinding(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    account_source_request_id: str
    holdings_source_request_id: str
    accounts_as_of: AwareDatetime
    holdings_as_of: AwareDatetime
    provider_code: str
    account_quality: Literal["PASS"] = "PASS"
    holdings_quality: Literal["PARTIAL"] = "PARTIAL"
    currency: Literal["USD"] = "USD"
    position_nonzero: Literal[True] = True
    sha256: str


class CurrentHeldCommonStockProof(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    identity: CurrentCommonStockIdentity
    instrument_id: UUID
    alias_id: UUID
    alias_valid_from: date
    checked_at: AwareDatetime
    scope: Literal["fosun_current_holding"] = "fosun_current_holding"
    holding: CurrentHeldCommonStockBinding
    sec_url: str
    artifacts: tuple[ArtifactBinding, ArtifactBinding, ArtifactBinding, ArtifactBinding]
    universe_published: Literal[False] = False
    new_position_eligible: Literal[False] = False
    historical_backtest_eligible: Literal[False] = False
    execution_locked: Literal[True] = True


TAXONOMY_SCHEMAS = tuple(
    "sec_member_" + name + suffix + "_v1"
    for name in ("schema", "definition", "label")
    for suffix in ("_raw", "_receipt")
)


async def _archive_taxonomy(
    session: AsyncSession, store: IdentityStore, inputs: CustomIdentityInputs
) -> tuple[ArtifactBinding, ...]:
    result = []
    for number, content in enumerate(inputs.taxonomy_contents):
        digest = sha256(content).hexdigest()
        schema = TAXONOMY_SCHEMAS[number]
        artifact = await session.scalar(select(DataArtifact).where(DataArtifact.sha256 == digest))
        if artifact is None:
            key = f"raw/sec/current-identity/{schema}/{digest}.json"
            uri = await store.put_if_absent(key=key, content=content)
            received = inputs.taxonomy_received[number // 2]
            await session.execute(
                insert(DataArtifact)
                .values(
                    uri=uri,
                    sha256=digest,
                    byte_size=len(content),
                    source="sec",
                    schema_version=schema,
                    mime_type="application/json" if number % 2 else "application/xml",
                    received_at=received,
                    observed_at=received,
                    available_at=received,
                )
                .on_conflict_do_nothing(index_elements=["sha256"])
            )
            artifact = await session.scalar(
                select(DataArtifact).where(DataArtifact.sha256 == digest)
            )
        if artifact is None or artifact.source != "sec" or artifact.schema_version != schema:
            raise ValueError("CURRENT_IDENTITY_ARTIFACT_CONFLICT")
        binding = _binding(artifact)
        if await _read_artifact(store, binding) != content:
            raise ValueError("CURRENT_IDENTITY_ARTIFACT_INVALID")
        result.append(binding)
    return tuple(result)


def _binding(artifact: DataArtifact) -> ArtifactBinding:
    return ArtifactBinding(
        uri=artifact.uri,
        sha256=artifact.sha256,
        byte_size=artifact.byte_size,
        source=artifact.source,
        schema_version=artifact.schema_version,
    )


async def _read_artifact(store: IdentityStore, binding: ArtifactBinding) -> bytes:
    path = urlsplit(binding.uri).path
    if "/raw/" not in path:
        raise ValueError("CURRENT_IDENTITY_ARTIFACT_INVALID")
    key = "raw/" + path.split("/raw/", 1)[1]
    content = await store.read_raw(key, uri=binding.uri)
    if len(content) != binding.byte_size or sha256(content).hexdigest() != binding.sha256:
        raise ValueError("CURRENT_IDENTITY_ARTIFACT_INVALID")
    return content


async def _archive(
    session: AsyncSession, store: IdentityStore, inputs: IdentityInputs
) -> tuple[ArtifactBinding, ArtifactBinding, ArtifactBinding, ArtifactBinding]:
    result = []
    schemas = (
        "fmp_profile_raw_v1",
        "fmp_profile_receipt_v1",
        "sec_identity_raw_v1",
        "sec_identity_receipt_v1",
    )
    for number, content in enumerate(inputs.contents):
        digest = sha256(content).hexdigest()
        source = "fmp" if number < 2 else "sec"
        artifact = await session.scalar(select(DataArtifact).where(DataArtifact.sha256 == digest))
        if artifact is None:
            key = f"raw/{source}/current-identity/{schemas[number]}/{digest}.json"
            uri = await store.put_if_absent(key=key, content=content)
            received = inputs.received[number // 2]
            await session.execute(
                insert(DataArtifact)
                .values(
                    uri=uri,
                    sha256=digest,
                    byte_size=len(content),
                    source=source,
                    schema_version=schemas[number],
                    mime_type="text/html" if number == 2 else "application/json",
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
            raise ValueError("CURRENT_IDENTITY_ARTIFACT_CONFLICT")
        binding = _binding(artifact)
        if await _read_artifact(store, binding) != content:
            raise ValueError("CURRENT_IDENTITY_ARTIFACT_INVALID")
        result.append(binding)
    return result[0], result[1], result[2], result[3]


def _held_binding_payload(
    *,
    account_source_request_id: str,
    holdings_source_request_id: str,
    accounts_as_of: datetime,
    holdings_as_of: datetime,
    provider_code: str,
) -> dict[str, object]:
    return {
        "account_source_request_id": account_source_request_id,
        "holdings_source_request_id": holdings_source_request_id,
        "accounts_as_of": accounts_as_of.isoformat(),
        "holdings_as_of": holdings_as_of.isoformat(),
        "provider_code": provider_code,
        "account_quality": "PASS",
        "holdings_quality": "PARTIAL",
        "currency": "USD",
        "position_nonzero": True,
    }


def _held_binding_digest(payload: dict[str, object]) -> str:
    import json

    return sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def current_held_common_stock_binding(
    *,
    symbol: str,
    accounts_page: GatewayResponsePage,
    holdings_page: GatewayResponsePage,
    now: datetime,
) -> CurrentHeldCommonStockBinding:
    try:
        if (
            symbol != symbol.strip().upper()
            or not symbol
            or accounts_page.quality != "PASS"
            or holdings_page.quality != "PARTIAL"
            or accounts_page.has_more
            or holdings_page.has_more
            or not accounts_page.as_of <= holdings_page.as_of <= now
            or now - accounts_page.as_of > HELD_BINDING_MAX_AGE
            or now - holdings_page.as_of > HELD_BINDING_MAX_AGE
        ):
            raise ValueError
        accounts = tuple(AccountItem.model_validate(item) for item in accounts_page.items)
        holdings = tuple(PositionItem.model_validate(item) for item in holdings_page.items)
        if len(accounts) != 1:
            raise ValueError
        account_id = accounts[0].external_account_id
        if any(item.external_account_id != account_id for item in holdings):
            raise ValueError
        provider_code = "us" + symbol
        matches = [item for item in holdings if item.instrument_code == provider_code]
        if len(matches) != 1 or matches[0].currency != "USD" or matches[0].quantity == 0:
            raise ValueError
        payload = _held_binding_payload(
            account_source_request_id=accounts_page.source_request_id,
            holdings_source_request_id=holdings_page.source_request_id,
            accounts_as_of=accounts_page.as_of,
            holdings_as_of=holdings_page.as_of,
            provider_code=provider_code,
        )
        return CurrentHeldCommonStockBinding.model_validate(
            {**payload, "sha256": _held_binding_digest(payload)}
        )
    except (ValueError, TypeError, KeyError):
        raise ValueError("CURRENT_HELD_COMMON_STOCK_HOLDING_INVALID") from None


def _validate_held_common_stock_binding(
    binding: CurrentHeldCommonStockBinding, *, symbol: str, now: datetime
) -> None:
    payload = _held_binding_payload(
        account_source_request_id=binding.account_source_request_id,
        holdings_source_request_id=binding.holdings_source_request_id,
        accounts_as_of=binding.accounts_as_of,
        holdings_as_of=binding.holdings_as_of,
        provider_code=binding.provider_code,
    )
    if (
        binding.provider_code != "us" + symbol
        or binding.sha256 != _held_binding_digest(payload)
        or not binding.accounts_as_of <= binding.holdings_as_of <= now
        or now - binding.accounts_as_of > HELD_BINDING_MAX_AGE
        or now - binding.holdings_as_of > HELD_BINDING_MAX_AGE
    ):
        raise ValueError("CURRENT_HELD_COMMON_STOCK_HOLDING_INVALID")


async def _current_held_aliases(
    session: AsyncSession, symbol: str, instrument_id: UUID, now: datetime
) -> tuple[InstrumentAlias, ...]:
    return tuple(
        await session.scalars(
            select(InstrumentAlias).where(
                InstrumentAlias.alias_type == "ticker",
                or_(
                    InstrumentAlias.symbol == symbol,
                    InstrumentAlias.instrument_id == instrument_id,
                ),
                InstrumentAlias.valid_from <= now.date(),
                or_(InstrumentAlias.valid_to.is_(None), InstrumentAlias.valid_to >= now.date()),
            )
        )
    )


def _held_revision(inputs: IdentityInputs, holding: CurrentHeldCommonStockBinding) -> str:
    return sha256(
        (
            HELD_SCHEMA
            + inputs.identity.symbol
            + holding.sha256
            + ":".join(sha256(raw).hexdigest() for raw in inputs.contents)
        ).encode()
    ).hexdigest()


async def import_current_held_common_stock(
    session: AsyncSession,
    store: IdentityStore,
    *,
    symbol: str,
    fmp_archive: Path,
    sec_archive: Path,
    accounts_page: GatewayResponsePage,
    holdings_page: GatewayResponsePage,
) -> CurrentIdentityImportResult:
    """Persist a source-proven current holding without publishing a research universe."""

    try:
        async with session.begin_nested():
            await session.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:symbol, 0))"),
                {"symbol": f"current_identity:{symbol}"},
            )
            now = await session.scalar(select(func.clock_timestamp()))
            assert isinstance(now, datetime)
            instrument_id = UUID(str(stable_instrument_id(symbol)))
            holding = current_held_common_stock_binding(
                symbol=symbol,
                accounts_page=accounts_page,
                holdings_page=holdings_page,
                now=now,
            )
            inputs = load_identity_archives(
                symbol=symbol,
                fmp_archive=fmp_archive,
                sec_archive=sec_archive,
                now=now,
                _instrument_id=instrument_id,
                _allow_outside_definition=True,
            )
            revision = _held_revision(inputs, holding)
            existing = await session.scalar(
                select(DataSnapshot).where(
                    DataSnapshot.source == HELD_SOURCE,
                    DataSnapshot.provider_object_id == f"identity:{symbol}",
                    DataSnapshot.revision == revision,
                )
            )
            if existing is not None:
                proof = await read_current_held_common_stock(
                    session, store, snapshot_id=existing.id
                )
                return CurrentIdentityImportResult("existing", proof.instrument_id, existing.id)

            instrument = await session.get(Instrument, instrument_id)
            aliases = await _current_held_aliases(session, symbol, instrument_id, now)
            if instrument is not None or aliases:
                if (
                    instrument is None
                    or instrument.asset_type != "common_stock"
                    or instrument.exchange != inputs.identity.exchange
                    or instrument.currency != "USD"
                    or instrument.risk_bucket != "EQUITY"
                    or instrument.tick_size is not None
                    or len(aliases) != 1
                    or aliases[0].instrument_id != instrument_id
                    or aliases[0].symbol != symbol
                ):
                    raise ValueError("CURRENT_HELD_COMMON_STOCK_MASTER_CONFLICT")
                alias = aliases[0]
            else:
                instrument = Instrument(
                    id=instrument_id,
                    asset_type="common_stock",
                    exchange=inputs.identity.exchange,
                    currency="USD",
                    tick_size=None,
                    risk_bucket="EQUITY",
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

            artifacts = await _archive(session, store, inputs)
            checked = await session.scalar(select(func.clock_timestamp()))
            assert isinstance(checked, datetime)
            proof = CurrentHeldCommonStockProof(
                identity=inputs.identity,
                instrument_id=instrument_id,
                alias_id=alias.id,
                alias_valid_from=alias.valid_from,
                checked_at=checked,
                holding=holding,
                sec_url=inputs.sec_url,
                artifacts=artifacts,
            )
            snapshot = DataSnapshot(
                source=HELD_SOURCE,
                provider_object_id=f"identity:{symbol}",
                revision=revision,
                schema_version=HELD_SCHEMA,
                observed_at=checked,
                received_at=checked,
                available_at=checked,
                payload=proof.model_dump(mode="json"),
            )
            session.add(snapshot)
            await session.flush()
            await read_current_held_common_stock(session, store, snapshot_id=snapshot.id)
            return CurrentIdentityImportResult("created", instrument_id, snapshot.id)
    except IntegrityError as error:
        raise ValueError("CURRENT_HELD_COMMON_STOCK_CONCURRENT_CONFLICT") from error


async def read_current_held_common_stock(
    session: AsyncSession, store: IdentityStore, *, snapshot_id: UUID
) -> CurrentHeldCommonStockProof:
    try:
        snapshot = await session.get(DataSnapshot, snapshot_id)
        now = await session.scalar(select(func.clock_timestamp()))
        assert isinstance(now, datetime)
        if (
            snapshot is None
            or snapshot.source != HELD_SOURCE
            or snapshot.schema_version != HELD_SCHEMA
        ):
            raise ValueError
        proof = CurrentHeldCommonStockProof.model_validate(snapshot.payload)
        _validate_held_common_stock_binding(
            proof.holding, symbol=proof.identity.symbol, now=proof.checked_at
        )
        if (
            (snapshot.observed_at, snapshot.received_at, snapshot.available_at)
            != (proof.checked_at,) * 3
            or proof.checked_at > now
            or len(proof.artifacts) != 4
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
            _allow_outside_definition=True,
        )
        if (
            inputs.identity != proof.identity
            or inputs.sec_url != proof.sec_url
            or _held_revision(inputs, proof.holding) != snapshot.revision
            or proof.instrument_id != stable_instrument_id(proof.identity.symbol)
            or snapshot.provider_object_id != f"identity:{proof.identity.symbol}"
        ):
            raise ValueError
        instrument = await session.get(Instrument, proof.instrument_id)
        aliases = await _current_held_aliases(
            session, proof.identity.symbol, proof.instrument_id, now
        )
        if (
            instrument is None
            or instrument.asset_type != "common_stock"
            or instrument.exchange != proof.identity.exchange
            or instrument.currency != "USD"
            or instrument.risk_bucket != "EQUITY"
            or instrument.tick_size is not None
            or len(aliases) != 1
            or aliases[0].id != proof.alias_id
            or aliases[0].symbol != proof.identity.symbol
            or aliases[0].instrument_id != proof.instrument_id
            or aliases[0].valid_from != proof.alias_valid_from
            or proof.alias_valid_from > proof.checked_at.date()
        ):
            raise ValueError
        return proof
    except (ValueError, TypeError, KeyError) as error:
        raise ValueError("CURRENT_HELD_COMMON_STOCK_READBACK_INVALID") from error


def _compatible(instrument: Instrument, identity: CurrentCommonStockIdentity) -> bool:
    exchange = instrument.exchange
    # Only reviewed equivalent listing labels; the broad market set alone does not prove equality.
    aliases = {"XNYS": {"XNYS", "NYSE"}, "XNAS": {"XNAS", "NASDAQ"}}
    return (
        exchange in US_EQUITY_EXCHANGES
        and exchange in aliases[identity.exchange]
        and instrument.currency == identity.currency
        and instrument.asset_type == "common_stock"
        and instrument.risk_bucket == member_for(identity.symbol).risk_bucket
    )


async def _select_identity(
    session: AsyncSession, identity: CurrentCommonStockIdentity, checked_at: datetime
) -> tuple[Instrument, InstrumentAlias, bool]:
    member = member_for(identity.symbol)
    aliases = tuple(
        await session.scalars(
            select(InstrumentAlias).where(
                InstrumentAlias.alias_type == "ticker",
                or_(
                    InstrumentAlias.symbol == identity.symbol,
                    InstrumentAlias.instrument_id == member.instrument_id,
                ),
            )
        )
    )
    expected = await session.get(Instrument, member.instrument_id)
    current = [
        a
        for a in aliases
        if a.valid_from <= checked_at.date()
        and (a.valid_to is None or a.valid_to >= checked_at.date())
    ]
    if aliases or expected is not None:
        if (
            len(current) != 1
            or current[0].symbol != identity.symbol
            or any(
                a.symbol != identity.symbol or a.instrument_id != current[0].instrument_id
                for a in aliases
            )
            or (expected is not None and expected.id != current[0].instrument_id)
        ):
            raise ValueError("CURRENT_IDENTITY_MASTER_CONFLICT")
        alias = current[0]
        instrument = await session.get(Instrument, alias.instrument_id)
        if instrument is None or not _compatible(instrument, identity):
            raise ValueError("CURRENT_IDENTITY_MASTER_CONFLICT")
        identifiers = await session.scalars(
            select(InstrumentAlias).where(
                InstrumentAlias.instrument_id == instrument.id,
                InstrumentAlias.alias_type.in_(("cik", "isin", "cusip")),
            )
        )
        for known in identifiers:
            reported = getattr(identity, known.alias_type)
            if (known.alias_type == "cik" and cik(known.symbol) != reported) or (
                known.alias_type != "cik" and reported is not None and known.symbol != reported
            ):
                raise ValueError("CURRENT_IDENTITY_SOURCE_CONFLICT")
        previous = await session.scalars(
            select(DataSnapshot).where(
                DataSnapshot.source == SOURCE,
                DataSnapshot.provider_object_id == f"identity:{identity.symbol}",
            )
        )
        for snapshot in previous:
            proof = CurrentIdentityProof.model_validate(snapshot.payload)
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
                raise ValueError("CURRENT_IDENTITY_SOURCE_CONFLICT")
        return instrument, alias, False
    instrument = Instrument(
        id=member.instrument_id,
        asset_type="common_stock",
        exchange=identity.exchange,
        currency="USD",
        tick_size=None,
        risk_bucket=member.risk_bucket,
    )
    session.add(instrument)
    await session.flush()
    alias = InstrumentAlias(
        instrument_id=instrument.id,
        symbol=identity.symbol,
        alias_type="ticker",
        valid_from=checked_at.date(),
        valid_to=None,
    )
    session.add(alias)
    await session.flush()
    return instrument, alias, True


async def import_current_common_stock(
    session: AsyncSession,
    store: IdentityStore,
    *,
    symbol: str,
    fmp_archive: Path,
    sec_archive: Path,
    taxonomy_archive: Path | None = None,
) -> CurrentIdentityImportResult:
    member_for(symbol)
    try:
        async with session.begin_nested():
            await session.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:symbol, 0))"),
                {"symbol": f"current_identity:{symbol}"},
            )
            checked_at = await session.scalar(select(func.clock_timestamp()))
            assert isinstance(checked_at, datetime)
            inputs: IdentityInputs | CustomIdentityInputs
            inputs = (
                load_identity_archives(
                    symbol=symbol, fmp_archive=fmp_archive, sec_archive=sec_archive, now=checked_at
                )
                if taxonomy_archive is None
                else load_custom_identity_archives(
                    symbol=symbol,
                    fmp_archive=fmp_archive,
                    sec_archive=sec_archive,
                    taxonomy_archive=taxonomy_archive,
                    now=checked_at,
                )
            )
            instrument, alias, created = await _select_identity(
                session, inputs.identity, checked_at
            )
            previous = await session.scalar(
                select(DataSnapshot).where(
                    DataSnapshot.source == SOURCE,
                    DataSnapshot.provider_object_id == f"identity:{symbol}",
                    DataSnapshot.revision == inputs.revision,
                )
            )
            if previous is not None:
                proof = await read_current_identity(session, store, snapshot_id=previous.id)
                if proof.instrument_id != instrument.id or proof.alias_id != alias.id:
                    raise ValueError("CURRENT_IDENTITY_MASTER_CONFLICT")
                return CurrentIdentityImportResult("existing", instrument.id, previous.id)
            custom = isinstance(inputs, CustomIdentityInputs)
            artifacts = await _archive(
                session, store, inputs.base if isinstance(inputs, CustomIdentityInputs) else inputs
            )
            proof_class = CustomMemberIdentityProof if custom else CurrentIdentityProof
            extra: dict[str, Any] = (
                {"taxonomy_artifacts": await _archive_taxonomy(session, store, inputs)}
                if isinstance(inputs, CustomIdentityInputs)
                else {}
            )
            proof = proof_class(
                identity=inputs.identity,
                instrument_id=instrument.id,
                alias_id=alias.id,
                alias_valid_from=alias.valid_from,
                checked_at=checked_at,
                definition_instrument_id=member_for(symbol).instrument_id,
                sec_url=inputs.sec_url,
                artifacts=artifacts,
                **extra,
            )
            snapshot = DataSnapshot(
                source=SOURCE,
                provider_object_id=f"identity:{symbol}",
                revision=inputs.revision,
                schema_version=CUSTOM_SCHEMA if custom else SCHEMA,
                observed_at=checked_at,
                received_at=checked_at,
                available_at=checked_at,
                payload=proof.model_dump(mode="json"),
            )
            session.add(snapshot)
            await session.flush()
            await read_current_identity(session, store, snapshot_id=snapshot.id)
            return CurrentIdentityImportResult(
                "created" if created else "existing", instrument.id, snapshot.id
            )
    except IntegrityError as error:
        raise ValueError("CURRENT_IDENTITY_CONCURRENT_CONFLICT") from error


async def read_current_identity(
    session: AsyncSession, store: IdentityStore, *, snapshot_id: UUID
) -> CurrentIdentityProof | CustomMemberIdentityProof:
    try:
        snapshot = await session.get(DataSnapshot, snapshot_id)
        if (
            snapshot is None
            or snapshot.source != SOURCE
            or snapshot.schema_version not in {SCHEMA, CUSTOM_SCHEMA}
        ):
            raise ValueError
        proof = (
            CustomMemberIdentityProof.model_validate(snapshot.payload)
            if snapshot.schema_version == CUSTOM_SCHEMA
            else CurrentIdentityProof.model_validate(snapshot.payload)
        )
        if (snapshot.observed_at, snapshot.received_at, snapshot.available_at) != (
            proof.checked_at,
        ) * 3:
            raise ValueError
        contents = []
        bindings = proof.artifacts + (
            proof.taxonomy_artifacts if isinstance(proof, CustomMemberIdentityProof) else ()
        )
        for number, binding in enumerate(bindings):
            artifact = await session.scalar(
                select(DataArtifact).where(DataArtifact.sha256 == binding.sha256)
            )
            if artifact is None or _binding(artifact) != binding:
                raise ValueError
            if number >= 4 and (
                binding.source != "sec" or binding.schema_version != TAXONOMY_SCHEMAS[number - 4]
            ):
                raise ValueError
            contents.append(await _read_artifact(store, binding))
        base_contents = (contents[0], contents[1], contents[2], contents[3])
        inputs = (
            validate_custom_identity_originals(
                symbol=proof.identity.symbol,
                contents=base_contents,
                taxonomy_contents=tuple(contents[4:]),
                now=proof.checked_at,
            )
            if isinstance(proof, CustomMemberIdentityProof)
            else validate_identity_originals(
                symbol=proof.identity.symbol,
                contents=base_contents,
                now=proof.checked_at,
            )
        )
        if (
            inputs.identity != proof.identity
            or inputs.sec_url != proof.sec_url
            or inputs.revision != snapshot.revision
            or snapshot.provider_object_id != f"identity:{proof.identity.symbol}"
            or member_for(proof.identity.symbol).instrument_id != proof.definition_instrument_id
        ):
            raise ValueError
        instrument = await session.get(Instrument, proof.instrument_id)
        alias = await session.get(InstrumentAlias, proof.alias_id)
        if (
            instrument is None
            or not _compatible(instrument, proof.identity)
            or alias is None
            or alias.instrument_id != instrument.id
            or alias.symbol != proof.identity.symbol
            or alias.alias_type != "ticker"
            or alias.valid_from != proof.alias_valid_from
            or alias.valid_from > proof.checked_at.date()
            or (alias.valid_to is not None and alias.valid_to < proof.checked_at.date())
        ):
            raise ValueError
        current_time = await session.scalar(select(func.clock_timestamp()))
        assert isinstance(current_time, datetime)
        current_aliases = tuple(
            await session.scalars(
                select(InstrumentAlias).where(
                    InstrumentAlias.alias_type == "ticker",
                    or_(
                        InstrumentAlias.symbol == proof.identity.symbol,
                        InstrumentAlias.instrument_id == proof.instrument_id,
                    ),
                    InstrumentAlias.valid_from <= current_time.date(),
                    or_(
                        InstrumentAlias.valid_to.is_(None),
                        InstrumentAlias.valid_to >= current_time.date(),
                    ),
                )
            )
        )
        if (
            proof.checked_at > current_time
            or len(current_aliases) != 1
            or current_aliases[0].id != proof.alias_id
        ):
            raise ValueError
        return proof
    except (ValueError, TypeError, KeyError) as error:
        raise ValueError("CURRENT_IDENTITY_READBACK_INVALID") from error
