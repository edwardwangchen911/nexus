"""Append a first equity ETF identity and immutable originals; existing masters stay intact."""

import json
import os
from datetime import date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Literal, cast
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, model_validator
from sqlalchemy import func, or_, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.time import require_utc
from nexus.market.current_etf_identity import (
    SCHEMA,
    SOURCE,
    CurrentEtfIdentity,
    CurrentEtfShareClassIdentity,
    CurrentGoldTrustIdentity,
    CurrentHeldMultiAssetEtfIdentity,
    CurrentHeldNasdaqEtfIdentity,
    CurrentNasdaqEtfIdentity,
    CurrentTreasuryEtfIdentity,
    CurrentUitIdentity,
    EtfInputs,
    member_for,
    validate_current_held_etf_originals,
    validate_etf_originals,
)
from nexus.market.current_gold_identity import validate_gold_originals
from nexus.market.current_identity import MAX_SEC_BYTES
from nexus.market.current_identity_storage import (
    ArtifactBinding,
    CurrentIdentityImportResult,
    IdentityStore,
    _binding,
    _read_artifact,
)
from nexus.market.current_uit_identity import validate_uit_originals
from nexus.market.universe import stable_instrument_id
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.providers.broker.fosun_contracts import AccountItem, OrderItem, PositionItem
from nexus.providers.broker.gateway_contracts import GatewayResponsePage
from nexus.research.fmp_profile_archive import _read


class EtfProof(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    identity: (
        CurrentGoldTrustIdentity
        | CurrentTreasuryEtfIdentity
        | CurrentUitIdentity
        | CurrentNasdaqEtfIdentity
        | CurrentEtfShareClassIdentity
        | CurrentEtfIdentity
    )
    instrument_id: UUID
    alias_id: UUID
    alias_valid_from: date
    checked_at: AwareDatetime
    definition_instrument_id: UUID
    sec_url: str
    artifacts: tuple[ArtifactBinding, ...]
    historical_backtest_eligible: Literal[False] = False
    execution_locked: Literal[True] = True


HELD_SOURCE = "current_held_etf_identity"
HELD_SCHEMA = "current_held_etf_identity_v1"
HELD_BINDING_MAX_AGE = timedelta(minutes=5)


class CurrentHoldingBinding(BaseModel):
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


class CurrentOpenOrderBinding(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    account_source_request_id: str
    orders_source_request_id: str
    accounts_as_of: AwareDatetime
    orders_as_of: AwareDatetime
    provider_code: str
    provider_status: Literal["10", "20", "21", "22", "40", "60"]
    submitted_at: AwareDatetime
    updated_at: AwareDatetime | None
    account_quality: Literal["PASS"] = "PASS"
    orders_quality: Literal["PARTIAL"] = "PARTIAL"
    order_active: Literal[True] = True
    order_id_sha256: str
    sha256: str


class CurrentHeldEtfProof(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    identity: (
        CurrentHeldMultiAssetEtfIdentity
        | CurrentHeldNasdaqEtfIdentity
        | CurrentNasdaqEtfIdentity
        | CurrentEtfShareClassIdentity
        | CurrentEtfIdentity
    )
    instrument_id: UUID
    alias_id: UUID
    alias_valid_from: date
    checked_at: AwareDatetime
    scope: Literal["fosun_current_holding", "fosun_current_open_order"] = "fosun_current_holding"
    holding: CurrentHoldingBinding | None = None
    order: CurrentOpenOrderBinding | None = None
    sec_url: str
    artifacts: tuple[ArtifactBinding, ...]
    universe_published: Literal[False] = False
    new_position_eligible: Literal[False] = False
    historical_backtest_eligible: Literal[False] = False
    execution_locked: Literal[True] = True

    @model_validator(mode="after")
    def _one_broker_exposure(self) -> "CurrentHeldEtfProof":
        if (self.holding is None) == (self.order is None):
            raise ValueError("CURRENT_ETF_BROKER_EXPOSURE_INVALID")
        if self.scope == "fosun_current_holding" and self.holding is None:
            raise ValueError("CURRENT_ETF_BROKER_EXPOSURE_INVALID")
        if self.scope == "fosun_current_open_order" and self.order is None:
            raise ValueError("CURRENT_ETF_BROKER_EXPOSURE_INVALID")
        return self


def _private_files(directory: Path, names: tuple[str, ...]) -> tuple[bytes, ...]:
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        if os.fstat(fd).st_mode & 0o077:
            raise ValueError("CURRENT_ETF_ARCHIVE_INVALID")
        return tuple(_read(fd, name, MAX_SEC_BYTES) for name in names)
    finally:
        os.close(fd)


async def load_etf_archives(
    *,
    symbol: str,
    fmp_archive: Path,
    sec_archive: Path,
    mapping_file: Path | None,
    now: datetime,
    allow_treasury: bool = False,
    allow_gold: bool = False,
) -> EtfInputs:
    contents = _private_files(fmp_archive, ("profile.json", "receipt.json"))
    if allow_gold and (mapping_file is not None or allow_treasury):
        raise ValueError("CURRENT_ETF_IDENTITY_INVALID")
    if mapping_file is None:
        sec = _private_files(
            sec_archive,
            (
                "submissions.raw",
                "submissions-receipt.json",
                "filing-index.raw",
                "index-receipt.json",
                "disclosure.raw",
                "disclosure-receipt.json",
            ),
        )
        if allow_gold:
            return validate_gold_originals(symbol=symbol, contents=contents + sec, now=now)
        if allow_treasury:
            raise ValueError("CURRENT_ETF_IDENTITY_INVALID")
        return validate_uit_originals(symbol=symbol, contents=contents + sec, now=now)
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
    return await validate_etf_originals(
        symbol=symbol, contents=contents + mapping + sec, now=now, allow_treasury=allow_treasury
    )


async def load_current_held_etf_archives(
    *,
    symbol: str,
    fmp_archive: Path,
    sec_archive: Path,
    mapping_file: Path,
    now: datetime,
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
    return await validate_current_held_etf_originals(
        symbol=symbol, contents=contents + mapping + sec, now=now
    )


def _broker_exposure_revision(
    inputs: EtfInputs, *, binding: CurrentHoldingBinding | CurrentOpenOrderBinding
) -> str:
    return sha256(
        (
            HELD_SCHEMA
            + inputs.identity.symbol
            + binding.sha256
            + ":".join(sha256(raw).hexdigest() for raw in inputs.contents)
        ).encode()
    ).hexdigest()


def _binding_payload(
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
        "accounts_as_of": require_utc(accounts_as_of).isoformat(),
        "holdings_as_of": require_utc(holdings_as_of).isoformat(),
        "provider_code": provider_code,
        "account_quality": "PASS",
        "holdings_quality": "PARTIAL",
        "currency": "USD",
        "position_nonzero": True,
    }


def _binding_digest(payload: dict[str, object]) -> str:
    return sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


_ACTIVE_ORDER_STATUSES = frozenset({"10", "20", "21", "22", "40", "60"})


def _order_binding_payload(
    *,
    account_source_request_id: str,
    orders_source_request_id: str,
    accounts_as_of: datetime,
    orders_as_of: datetime,
    provider_code: str,
    provider_status: Literal["10", "20", "21", "22", "40", "60"],
    submitted_at: datetime,
    updated_at: datetime | None,
    order_id_sha256: str,
) -> dict[str, object]:
    return {
        "account_source_request_id": account_source_request_id,
        "orders_source_request_id": orders_source_request_id,
        "accounts_as_of": require_utc(accounts_as_of).isoformat(),
        "orders_as_of": require_utc(orders_as_of).isoformat(),
        "provider_code": provider_code,
        "provider_status": provider_status,
        "submitted_at": require_utc(submitted_at).isoformat(),
        "updated_at": require_utc(updated_at).isoformat() if updated_at is not None else None,
        "account_quality": "PASS",
        "orders_quality": "PARTIAL",
        "order_active": True,
        "order_id_sha256": order_id_sha256,
    }


def current_open_order_binding(
    *,
    symbol: str,
    accounts_page: GatewayResponsePage,
    orders_page: GatewayResponsePage,
    now: datetime,
) -> CurrentOpenOrderBinding:
    try:
        if (
            symbol != symbol.strip().upper()
            or not symbol
            or accounts_page.quality != "PASS"
            or orders_page.quality != "PARTIAL"
            or accounts_page.has_more
            or orders_page.has_more
            or not accounts_page.as_of <= orders_page.as_of <= now
            or now - accounts_page.as_of > HELD_BINDING_MAX_AGE
            or now - orders_page.as_of > HELD_BINDING_MAX_AGE
        ):
            raise ValueError
        accounts = tuple(AccountItem.model_validate(item) for item in accounts_page.items)
        orders = tuple(OrderItem.model_validate(item) for item in orders_page.items)
        if len(accounts) != 1:
            raise ValueError
        provider_code = "us" + symbol
        matches = [
            item
            for item in orders
            if item.instrument_code == provider_code and item.status in _ACTIVE_ORDER_STATUSES
        ]
        if len(matches) != 1:
            raise ValueError
        order = matches[0]
        if order.submitted_at > orders_page.as_of or (
            order.updated_at is not None and order.updated_at > orders_page.as_of
        ):
            raise ValueError
        order_id_sha256 = sha256(order.external_order_id.encode()).hexdigest()
        provider_status = cast(Literal["10", "20", "21", "22", "40", "60"], order.status)
        payload = _order_binding_payload(
            account_source_request_id=accounts_page.source_request_id,
            orders_source_request_id=orders_page.source_request_id,
            accounts_as_of=accounts_page.as_of,
            orders_as_of=orders_page.as_of,
            provider_code=provider_code,
            provider_status=provider_status,
            submitted_at=order.submitted_at,
            updated_at=order.updated_at,
            order_id_sha256=order_id_sha256,
        )
        return CurrentOpenOrderBinding.model_validate(
            {**payload, "sha256": _binding_digest(payload)}
        )
    except (ValueError, TypeError, KeyError):
        raise ValueError("CURRENT_ETF_OPEN_ORDER_INVALID") from None


def _validate_frozen_order_binding(
    binding: CurrentOpenOrderBinding, *, symbol: str, now: datetime
) -> None:
    payload = _order_binding_payload(
        account_source_request_id=binding.account_source_request_id,
        orders_source_request_id=binding.orders_source_request_id,
        accounts_as_of=binding.accounts_as_of,
        orders_as_of=binding.orders_as_of,
        provider_code=binding.provider_code,
        provider_status=binding.provider_status,
        submitted_at=binding.submitted_at,
        updated_at=binding.updated_at,
        order_id_sha256=binding.order_id_sha256,
    )
    if (
        binding.provider_code != "us" + symbol
        or binding.sha256 != _binding_digest(payload)
        or not binding.accounts_as_of <= binding.orders_as_of <= now
        or now - binding.accounts_as_of > HELD_BINDING_MAX_AGE
        or now - binding.orders_as_of > HELD_BINDING_MAX_AGE
    ):
        raise ValueError("CURRENT_ETF_OPEN_ORDER_INVALID")


def current_holding_binding(
    *,
    symbol: str,
    accounts_page: GatewayResponsePage,
    holdings_page: GatewayResponsePage,
    now: datetime,
) -> CurrentHoldingBinding:
    """Freeze only the minimum anonymous facts from a fresh, complete broker read."""

    try:
        cutoff = require_utc(now)
        accounts_at = require_utc(accounts_page.as_of)
        holdings_at = require_utc(holdings_page.as_of)
        if (
            symbol != symbol.strip().upper()
            or not symbol
            or accounts_page.quality != "PASS"
            or holdings_page.quality != "PARTIAL"
            or accounts_page.has_more
            or holdings_page.has_more
            or not accounts_at <= holdings_at <= cutoff
            or cutoff - accounts_at > HELD_BINDING_MAX_AGE
            or cutoff - holdings_at > HELD_BINDING_MAX_AGE
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
        payload = _binding_payload(
            account_source_request_id=accounts_page.source_request_id,
            holdings_source_request_id=holdings_page.source_request_id,
            accounts_as_of=accounts_at,
            holdings_as_of=holdings_at,
            provider_code=provider_code,
        )
        return CurrentHoldingBinding.model_validate({**payload, "sha256": _binding_digest(payload)})
    except (ValueError, TypeError, KeyError):
        raise ValueError("CURRENT_HELD_ETF_HOLDING_INVALID") from None


def _validate_frozen_holding_binding(
    binding: CurrentHoldingBinding, *, symbol: str, now: datetime
) -> None:
    payload = _binding_payload(
        account_source_request_id=binding.account_source_request_id,
        holdings_source_request_id=binding.holdings_source_request_id,
        accounts_as_of=binding.accounts_as_of,
        holdings_as_of=binding.holdings_as_of,
        provider_code=binding.provider_code,
    )
    cutoff = require_utc(now)
    if (
        binding.provider_code != "us" + symbol
        or binding.sha256 != _binding_digest(payload)
        or not binding.accounts_as_of <= binding.holdings_as_of <= cutoff
        or cutoff - binding.accounts_as_of > HELD_BINDING_MAX_AGE
        or cutoff - binding.holdings_as_of > HELD_BINDING_MAX_AGE
    ):
        raise ValueError("CURRENT_HELD_ETF_HOLDING_INVALID")


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
    mapping_file: Path | None,
    allow_treasury: bool = False,
    allow_gold: bool = False,
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
            allow_treasury=allow_treasury,
            allow_gold=allow_gold,
        )
        if isinstance(inputs.identity, CurrentHeldMultiAssetEtfIdentity):
            raise ValueError("CURRENT_ETF_IDENTITY_INVALID")
        if inputs.identity.asset_type != member.asset_type:
            raise ValueError("CURRENT_ETF_IDENTITY_INVALID")
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
            asset_type=inputs.identity.asset_type,
            exchange=inputs.identity.exchange,
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


async def _import_current_etf_exposure(
    session: AsyncSession,
    store: IdentityStore,
    *,
    symbol: str,
    fmp_archive: Path,
    sec_archive: Path,
    mapping_file: Path,
    accounts_page: GatewayResponsePage,
    holdings_page: GatewayResponsePage | None = None,
    orders_page: GatewayResponsePage | None = None,
) -> CurrentIdentityImportResult:
    """Persist one proven broker exposure without publishing a research universe."""

    async with session.begin_nested():
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:symbol, 0))"),
            {"symbol": f"current_identity:{symbol}"},
        )
        now = await session.scalar(select(func.clock_timestamp()))
        assert isinstance(now, datetime)
        if (holdings_page is None) == (orders_page is None):
            raise ValueError("CURRENT_ETF_BROKER_EXPOSURE_INVALID")
        exposure_binding: CurrentHoldingBinding | CurrentOpenOrderBinding
        scope: Literal["fosun_current_holding", "fosun_current_open_order"]
        if holdings_page is not None:
            exposure_binding = current_holding_binding(
                symbol=symbol,
                accounts_page=accounts_page,
                holdings_page=holdings_page,
                now=now,
            )
            scope = "fosun_current_holding"
        else:
            assert orders_page is not None
            exposure_binding = current_open_order_binding(
                symbol=symbol,
                accounts_page=accounts_page,
                orders_page=orders_page,
                now=now,
            )
            scope = "fosun_current_open_order"
        inputs = await load_current_held_etf_archives(
            symbol=symbol,
            fmp_archive=fmp_archive,
            sec_archive=sec_archive,
            mapping_file=mapping_file,
            now=now,
        )
        if not isinstance(
            inputs.identity,
            CurrentHeldMultiAssetEtfIdentity
            | CurrentHeldNasdaqEtfIdentity
            | CurrentNasdaqEtfIdentity
            | CurrentEtfShareClassIdentity
            | CurrentEtfIdentity,
        ):
            raise ValueError("CURRENT_ETF_IDENTITY_INVALID")
        identity = cast(
            CurrentHeldMultiAssetEtfIdentity
            | CurrentHeldNasdaqEtfIdentity
            | CurrentNasdaqEtfIdentity
            | CurrentEtfShareClassIdentity
            | CurrentEtfIdentity,
            inputs.identity,
        )
        instrument_id = UUID(str(stable_instrument_id(symbol)))
        revision = _broker_exposure_revision(inputs, binding=exposure_binding)
        existing = await session.scalar(
            select(DataSnapshot).where(
                DataSnapshot.source == HELD_SOURCE,
                DataSnapshot.provider_object_id == f"identity:{symbol}",
                DataSnapshot.revision == revision,
            )
        )
        if existing is not None:
            proof = await read_current_held_etf(session, store, snapshot_id=existing.id)
            return CurrentIdentityImportResult("existing", proof.instrument_id, existing.id)
        instrument = await session.get(Instrument, instrument_id)
        aliases = await _current_aliases(session, symbol, instrument_id, now)
        expected_asset_type = identity.asset_type
        expected_risk_bucket = (
            identity.risk_bucket
            if isinstance(identity, CurrentHeldMultiAssetEtfIdentity)
            else "EQUITY"
        )
        if instrument is not None or aliases:
            if (
                instrument is None
                or instrument.asset_type != expected_asset_type
                or instrument.exchange != identity.exchange
                or instrument.currency != "USD"
                or instrument.risk_bucket != expected_risk_bucket
                or instrument.tick_size is not None
                or len(aliases) != 1
                or aliases[0].instrument_id != instrument_id
                or aliases[0].symbol != symbol
            ):
                raise ValueError("CURRENT_ETF_MASTER_CONFLICT")
            prior_snapshots = tuple(
                await session.scalars(
                    select(DataSnapshot).where(
                        DataSnapshot.source == HELD_SOURCE,
                        DataSnapshot.provider_object_id == f"identity:{symbol}",
                    )
                )
            )
            for prior_snapshot in prior_snapshots:
                prior = CurrentHeldEtfProof.model_validate(prior_snapshot.payload)
                if (
                    prior.identity.symbol,
                    prior.identity.cik,
                    prior.identity.series_id,
                    prior.identity.class_id,
                    prior.identity.legal_name,
                    prior.identity.exchange,
                ) != (
                    identity.symbol,
                    identity.cik,
                    identity.series_id,
                    identity.class_id,
                    identity.legal_name,
                    identity.exchange,
                ):
                    raise ValueError("CURRENT_ETF_MASTER_CONFLICT")
            alias = aliases[0]
        else:
            conflicting_alias = await session.scalar(
                select(InstrumentAlias.id)
                .where(InstrumentAlias.alias_type == "ticker", InstrumentAlias.symbol == symbol)
                .limit(1)
            )
            if conflicting_alias is not None:
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
                    key=f"raw/{source}/current-held-etf/{digest}.raw", content=content
                )
                await session.execute(
                    insert(DataArtifact)
                    .values(
                        uri=uri,
                        sha256=digest,
                        byte_size=len(content),
                        source=source,
                        schema_version=f"current_held_etf_original_{number}_v1",
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
            artifact_binding = _binding(artifact)
            if await _read_artifact(store, artifact_binding) != content:
                raise ValueError("CURRENT_ETF_ARTIFACT_CONFLICT")
            artifacts.append(artifact_binding)

        checked = await session.scalar(select(func.clock_timestamp()))
        assert isinstance(checked, datetime)
        if instrument is None:
            instrument = Instrument(
                id=instrument_id,
                asset_type=expected_asset_type,
                exchange=identity.exchange,
                currency="USD",
                tick_size=None,
                risk_bucket=expected_risk_bucket,
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
        proof = CurrentHeldEtfProof(
            identity=identity,
            instrument_id=instrument.id,
            alias_id=alias.id,
            checked_at=checked,
            alias_valid_from=alias.valid_from,
            scope=scope,
            holding=(
                exposure_binding if isinstance(exposure_binding, CurrentHoldingBinding) else None
            ),
            order=(
                exposure_binding if isinstance(exposure_binding, CurrentOpenOrderBinding) else None
            ),
            sec_url=inputs.sec_url,
            artifacts=tuple(artifacts),
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
        await read_current_held_etf(session, store, snapshot_id=snapshot.id)
        return CurrentIdentityImportResult("created", instrument.id, snapshot.id)


async def import_current_held_etf(
    session: AsyncSession,
    store: IdentityStore,
    *,
    symbol: str,
    fmp_archive: Path,
    sec_archive: Path,
    mapping_file: Path,
    accounts_page: GatewayResponsePage,
    holdings_page: GatewayResponsePage,
) -> CurrentIdentityImportResult:
    return await _import_current_etf_exposure(
        session,
        store,
        symbol=symbol,
        fmp_archive=fmp_archive,
        sec_archive=sec_archive,
        mapping_file=mapping_file,
        accounts_page=accounts_page,
        holdings_page=holdings_page,
    )


async def import_current_open_order_etf(
    session: AsyncSession,
    store: IdentityStore,
    *,
    symbol: str,
    fmp_archive: Path,
    sec_archive: Path,
    mapping_file: Path,
    accounts_page: GatewayResponsePage,
    orders_page: GatewayResponsePage,
) -> CurrentIdentityImportResult:
    return await _import_current_etf_exposure(
        session,
        store,
        symbol=symbol,
        fmp_archive=fmp_archive,
        sec_archive=sec_archive,
        mapping_file=mapping_file,
        accounts_page=accounts_page,
        orders_page=orders_page,
    )


async def read_current_held_etf(
    session: AsyncSession, store: IdentityStore, *, snapshot_id: UUID
) -> CurrentHeldEtfProof:
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
        proof = CurrentHeldEtfProof.model_validate(snapshot.payload)
        exposure_binding: CurrentHoldingBinding | CurrentOpenOrderBinding
        if proof.holding is not None:
            _validate_frozen_holding_binding(
                proof.holding,
                symbol=proof.identity.symbol,
                now=proof.checked_at,
            )
            exposure_binding = proof.holding
        elif proof.order is not None:
            _validate_frozen_order_binding(
                proof.order,
                symbol=proof.identity.symbol,
                now=proof.checked_at,
            )
            exposure_binding = proof.order
        else:
            raise ValueError
        if (
            (snapshot.observed_at, snapshot.received_at, snapshot.available_at)
            != (proof.checked_at,) * 3
            or proof.checked_at > now
            or snapshot.created_at > now
            or len(proof.artifacts) != 8
        ):
            raise ValueError
        contents = []
        for number, artifact_binding in enumerate(proof.artifacts):
            artifact = await session.scalar(
                select(DataArtifact).where(DataArtifact.sha256 == artifact_binding.sha256)
            )
            if (
                artifact is None
                or _binding(artifact) != artifact_binding
                or artifact.source != ("fmp" if number < 2 else "sec")
            ):
                raise ValueError
            contents.append(await _read_artifact(store, artifact_binding))
        inputs = await validate_current_held_etf_originals(
            symbol=proof.identity.symbol,
            contents=tuple(contents),
            now=proof.checked_at,
        )
        expected_id = UUID(str(stable_instrument_id(proof.identity.symbol)))
        if (
            inputs.identity != proof.identity
            or _broker_exposure_revision(inputs, binding=exposure_binding) != snapshot.revision
            or inputs.sec_url != proof.sec_url
            or proof.instrument_id != expected_id
            or snapshot.provider_object_id != f"identity:{proof.identity.symbol}"
        ):
            raise ValueError
        instrument = await session.get(Instrument, proof.instrument_id)
        aliases = await _current_aliases(session, proof.identity.symbol, proof.instrument_id, now)
        expected_risk_bucket = (
            proof.identity.risk_bucket
            if isinstance(proof.identity, CurrentHeldMultiAssetEtfIdentity)
            else "EQUITY"
        )
        if (
            instrument is None
            or instrument.asset_type != proof.identity.asset_type
            or proof.identity.asset_type not in {"equity_etf", "non_equity_etf"}
            or instrument.exchange != proof.identity.exchange
            or instrument.currency != "USD"
            or instrument.risk_bucket != expected_risk_bucket
            or instrument.tick_size is not None
            or len(aliases) != 1
            or aliases[0].id != proof.alias_id
            or aliases[0].symbol != proof.identity.symbol
            or aliases[0].instrument_id != instrument.id
            or aliases[0].valid_from != proof.alias_valid_from
            or proof.alias_valid_from > proof.checked_at.date()
        ):
            raise ValueError
        return proof
    except (ValueError, TypeError, KeyError) as error:
        raise ValueError("CURRENT_HELD_ETF_READBACK_INVALID") from error


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
        if isinstance(proof.identity, CurrentGoldTrustIdentity):
            inputs = validate_gold_originals(
                symbol=proof.identity.symbol, contents=tuple(contents), now=proof.checked_at
            )
        elif isinstance(proof.identity, CurrentUitIdentity):
            inputs = validate_uit_originals(
                symbol=proof.identity.symbol, contents=tuple(contents), now=proof.checked_at
            )
        else:
            inputs = await validate_etf_originals(
                symbol=proof.identity.symbol,
                contents=tuple(contents),
                now=proof.checked_at,
                allow_treasury=isinstance(proof.identity, CurrentTreasuryEtfIdentity),
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
            or instrument.asset_type != proof.identity.asset_type
            or proof.identity.asset_type != member.asset_type
            or instrument.exchange != proof.identity.exchange
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
