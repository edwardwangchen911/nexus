"""Database-authoritative diagnostic evidence assembly."""

import json
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from decimal import Decimal
from hashlib import sha256
from typing import Literal, cast
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.diagnostics.artifacts import ArtifactByteReader
from nexus.diagnostics.calendar import completed_xnys_calendar, is_xnys_session
from nexus.diagnostics.calendar_policy import load_xnys_calendar_adoption
from nexus.diagnostics.evidence import combine_refs
from nexus.diagnostics.models import (
    DiagnosticEvidence,
    EvidenceBinding,
    InstrumentIdentity,
    MarketEvidence,
)
from nexus.diagnostics.series_authority import (
    SeriesAuthority,
    select_authoritative_series,
)
from nexus.domain.evidence import EvidenceRef
from nexus.market.universe import stable_instrument_id
from nexus.persistence.models.data import (
    SourcePolicy as SourcePolicyRecord,
)
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.portfolio import (
    CashSnapshot,
    PortfolioSnapshotRecord,
    PositionSnapshot,
)
from nexus.providers.base import PriceSeries
from nexus.providers.broker.base import PortfolioSnapshot
from nexus.quality.source_policy import (
    SOURCE_POLICY_ID,
    SOURCE_POLICY_PAYLOAD,
    SOURCE_POLICY_VERSION,
    load_source_policy_v1,
)

_SPY_ID = cast(UUID, stable_instrument_id("SPY"))


class DiagnosticEvidenceCapabilityUnavailable(RuntimeError):
    """The transport required to read DB-authorized artifacts is unavailable."""


@dataclass(frozen=True)
class DiagnosticAssembly:
    market: MarketEvidence
    canonical_inputs: dict[str, object]


async def assemble_diagnostic_evidence(
    session: AsyncSession,
    *,
    snapshot: PortfolioSnapshot,
    identities: dict[UUID, InstrumentIdentity],
    artifact_reader: ArtifactByteReader | None,
) -> DiagnosticAssembly:
    if artifact_reader is None:
        raise DiagnosticEvidenceCapabilityUnavailable(
            "diagnostic artifact reader capability is unavailable"
        )
    if snapshot.as_of is None:
        raise DiagnosticEvidenceCapabilityUnavailable(
            "diagnostic snapshot timestamp is unavailable"
        )
    snapshot_bindings, snapshot_inputs = await _snapshot_bindings(session, snapshot)
    policy_ref = await _source_policy_ref(session, snapshot.as_of)
    calendar_adoption = await load_xnys_calendar_adoption(session, snapshot.as_of)
    calendar = completed_xnys_calendar(snapshot.as_of, calendar_adoption)
    benchmark_result = await _canonical_spy_series(
        session, snapshot.as_of, artifact_reader, policy_ref
    )
    benchmark_authority = None if benchmark_result is None else benchmark_result[0]
    benchmark_identity_input = None if benchmark_result is None else benchmark_result[1]
    benchmark_identity_refs = () if benchmark_result is None else benchmark_result[2]
    benchmark = None if benchmark_authority is None else benchmark_authority.selected
    benchmark_input = None if benchmark_authority is None else benchmark_authority.canonical_input
    invalid_benchmark_sessions = (
        ()
        if benchmark is None
        else tuple(
            sorted(
                {
                    bar.session
                    for bar in benchmark[0].bars
                    if bar.available_at <= snapshot.as_of and not is_xnys_session(bar.session)
                }
            )
        )
    )
    conflict_refs: list[EvidenceRef] = []
    incomplete_refs: list[EvidenceRef] = []
    benchmark_source_conflict = bool(
        benchmark_authority is not None and benchmark_authority.source_conflict
    )
    if benchmark_source_conflict and benchmark_authority is not None:
        conflict_refs.extend(
            (
                *benchmark_authority.relevant_refs,
                *benchmark_identity_refs,
                *calendar.binding.refs,
            )
        )
    if benchmark_authority is None:
        incomplete_refs.extend((*calendar.binding.refs, policy_ref))
    elif benchmark_authority.selected is None:
        incomplete_refs.extend(
            (
                *benchmark_authority.relevant_refs,
                *benchmark_identity_refs,
                *calendar.binding.refs,
            )
        )
    if benchmark is not None and invalid_benchmark_sessions:
        conflict_refs.extend((*benchmark[1].refs, *calendar.binding.refs))
        benchmark = None
    selected: dict[UUID, tuple[PriceSeries, EvidenceBinding]] = {}
    series_authorities: dict[UUID, SeriesAuthority] = {}
    source_conflict = False
    for position in snapshot.positions:
        authority = await select_authoritative_series(
            session,
            position.instrument_id,
            snapshot.as_of,
            artifact_reader,
            policy_ref,
        )
        series_authorities[position.instrument_id] = authority
        source_conflict = source_conflict or authority.source_conflict
        if authority.source_conflict:
            conflict_refs.extend(
                (
                    *authority.relevant_refs,
                    *identities[position.instrument_id].authority_refs,
                    *calendar.binding.refs,
                )
            )
        if authority.selected is None:
            incomplete_refs.extend(
                (
                    *authority.relevant_refs,
                    *identities[position.instrument_id].authority_refs,
                    *calendar.binding.refs,
                )
            )
        if authority.selected is not None:
            series, binding = authority.selected
            selected[position.instrument_id] = (
                series,
                binding.model_copy(
                    update={
                        "refs": combine_refs(
                            binding.refs,
                            identities[position.instrument_id].authority_refs,
                        )
                    }
                ),
            )
    trading_sessions = calendar.sessions
    adv20: dict[UUID, Decimal] = {}
    adv_bindings: dict[UUID, EvidenceBinding] = {}
    for instrument_id, (series, binding) in selected.items():
        adv = _adv20(series, trading_sessions)
        adv_bindings[instrument_id] = binding
        if adv is not None:
            adv20[instrument_id] = adv
    quality: Literal["PASS", "PARTIAL", "FAIL"] = (
        "PASS" if benchmark is not None and len(selected) == len(snapshot.positions) else "PARTIAL"
    )
    evidence = DiagnosticEvidence(
        snapshot=cast(EvidenceBinding, snapshot_bindings["snapshot"]),
        cash=cast(dict[str, EvidenceBinding], snapshot_bindings["cash"]),
        position_prices=cast(dict[UUID, EvidenceBinding], snapshot_bindings["position_prices"]),
        adv20=adv_bindings,
        risk_buckets={
            instrument_id: identity_binding
            for instrument_id, identity in identities.items()
            if (identity_binding := _identity_binding(identity)) is not None
        },
        price_series={key: value[1] for key, value in selected.items()},
        benchmark=None if benchmark is None else benchmark[1],
        trading_calendar=calendar.binding,
    )
    market = MarketEvidence(
        prices={key: value[0] for key, value in selected.items()},
        adv20=adv20,
        quality=quality,
        identities=identities,
        trading_sessions=trading_sessions,
        benchmark=None if benchmark is None else benchmark[0],
        evidence=evidence,
        source_conflict=(
            source_conflict or benchmark_source_conflict or bool(invalid_benchmark_sessions)
        ),
        conflict_refs=combine_refs(tuple(conflict_refs)),
        incomplete_refs=combine_refs(tuple(incomplete_refs)),
    )
    return DiagnosticAssembly(
        market=market,
        canonical_inputs={
            "snapshot": snapshot_inputs,
            "identities": tuple(
                _identity_input(identity)
                for _, identity in sorted(identities.items(), key=lambda item: str(item[0]))
            ),
            "series": tuple(
                authority.canonical_input
                for _, authority in sorted(
                    series_authorities.items(), key=lambda item: str(item[0])
                )
            ),
            "benchmark": benchmark_input,
            "benchmark_identity": benchmark_identity_input,
            "benchmark_invalid_sessions": tuple(
                session.isoformat() for session in invalid_benchmark_sessions
            ),
            "adv20": {
                str(key): str(value)
                for key, value in sorted(adv20.items(), key=lambda item: str(item[0]))
            },
            "calendar": calendar.canonical_input,
        },
    )


async def _canonical_spy_series(
    session: AsyncSession,
    as_of: datetime,
    reader: ArtifactByteReader,
    policy_ref: EvidenceRef,
) -> tuple[SeriesAuthority, dict[str, object], tuple[EvidenceRef, ...]] | None:
    row = (
        await session.execute(
            select(Instrument, InstrumentAlias)
            .join(InstrumentAlias, InstrumentAlias.instrument_id == Instrument.id)
            .where(
                Instrument.id == _SPY_ID,
                Instrument.asset_type == "equity_etf",
                Instrument.exchange == "XNYS",
                Instrument.currency == "USD",
                Instrument.created_at <= as_of,
                InstrumentAlias.symbol == "SPY",
                InstrumentAlias.alias_type == "ticker",
                InstrumentAlias.valid_from <= as_of.date(),
                (InstrumentAlias.valid_to.is_(None) | (InstrumentAlias.valid_to >= as_of.date())),
                InstrumentAlias.created_at <= as_of,
            )
        )
    ).one_or_none()
    if row is None:
        return None
    instrument, alias = row
    identity_input = _db_identity_input(instrument, alias)
    identity_refs = (
        _db_ref("instrument", instrument.id, instrument.created_at, identity_input["instrument"]),
        _db_ref("instrument-alias", alias.id, alias.created_at, identity_input["alias"]),
    )
    authority = await select_authoritative_series(session, _SPY_ID, as_of, reader, policy_ref)
    if authority.selected is not None:
        series, binding = authority.selected
        authority = replace(
            authority,
            selected=(
                series,
                binding.model_copy(update={"refs": combine_refs(binding.refs, identity_refs)}),
            ),
        )
    return authority, identity_input, identity_refs


async def _source_policy_ref(session: AsyncSession, as_of: datetime) -> EvidenceRef:
    await load_source_policy_v1(session, instrument_tick_size=None)
    record = await session.scalar(
        select(SourcePolicyRecord).where(SourcePolicyRecord.id == SOURCE_POLICY_ID)
    )
    if record is None or record.created_at > as_of:
        raise ValueError("published source_policy_v1 is unavailable at snapshot")
    return EvidenceRef(
        source="nexus_db",
        object_id=f"source-policy:{SOURCE_POLICY_ID}:{SOURCE_POLICY_VERSION}",
        available_at=record.created_at,
        location=f"db://source-policies/{SOURCE_POLICY_ID}",
        content_hash=_canonical_hash(SOURCE_POLICY_PAYLOAD),
    )


async def _snapshot_bindings(
    session: AsyncSession, snapshot: PortfolioSnapshot
) -> tuple[dict[str, object], dict[str, object]]:
    record = await session.scalar(
        select(PortfolioSnapshotRecord).where(PortfolioSnapshotRecord.id == snapshot.id)
    )
    if record is None or snapshot.as_of is None:
        raise ValueError("diagnostic snapshot authority is unavailable")
    cash_rows = list(
        await session.scalars(
            select(CashSnapshot).where(CashSnapshot.portfolio_snapshot_id == snapshot.id)
        )
    )
    position_rows = list(
        await session.scalars(
            select(PositionSnapshot).where(PositionSnapshot.portfolio_snapshot_id == snapshot.id)
        )
    )
    snapshot_payload = {
        "id": str(record.id),
        "portfolio_id": str(record.portfolio_id),
        "sync_run_id": str(record.sync_run_id),
        "stale_from_snapshot_id": _canonical_value(record.stale_from_snapshot_id),
        "as_of": _canonical_value(record.as_of),
        "status": record.status,
        "position_hash_version": record.position_hash_version,
        "position_hash": record.position_hash,
        "order_hash_version": record.order_hash_version,
        "order_hash": record.order_hash,
        "source_request_ids": tuple(record.source_request_ids),
        "source_observations": tuple(record.source_observations),
        "created_at": _canonical_value(record.created_at),
    }
    snapshot_ref = _db_ref("portfolio-snapshot", record.id, snapshot.as_of, snapshot_payload)
    cash = {
        row.currency: EvidenceBinding(
            available_at=snapshot.as_of,
            as_of=row.provider_as_of or snapshot.as_of,
            refs=(_db_ref("cash-snapshot", row.id, snapshot.as_of, _cash_input(row)),),
        )
        for row in cash_rows
    }
    positions = {
        row.instrument_id: EvidenceBinding(
            available_at=snapshot.as_of,
            as_of=row.provider_as_of or snapshot.as_of,
            refs=(_db_ref("position-snapshot", row.id, snapshot.as_of, _position_input(row)),),
        )
        for row in position_rows
    }
    return (
        {
            "snapshot": EvidenceBinding(
                available_at=snapshot.as_of,
                as_of=snapshot.as_of,
                refs=(snapshot_ref,),
            ),
            "cash": cash,
            "position_prices": positions,
        },
        {
            "snapshot": snapshot_payload,
            "cash": tuple(
                _cash_input(row) for row in sorted(cash_rows, key=lambda row: row.currency)
            ),
            "positions": tuple(
                _position_input(row)
                for row in sorted(position_rows, key=lambda row: str(row.instrument_id))
            ),
        },
    )


def _adv20(series: PriceSeries, sessions: tuple[date, ...]) -> Decimal | None:
    bars = {bar.session: bar for bar in series.bars}
    sample = [bars[session] for session in sessions[-20:] if session in bars]
    if len(sample) < 20:
        return None
    return sum((bar.close * bar.volume for bar in sample), Decimal(0)) / Decimal(20)


def _db_ref(kind: str, record_id: UUID, available_at: datetime, payload: object) -> EvidenceRef:
    return EvidenceRef(
        source="nexus_db",
        object_id=f"{kind}:{record_id}",
        available_at=available_at,
        location=f"db://{kind}/{record_id}",
        content_hash=_canonical_hash(payload),
    )


def _cash_input(row: CashSnapshot) -> dict[str, object]:
    return {
        "id": str(row.id),
        "currency": row.currency,
        "cash_balance": str(row.cash_balance),
        "available_cash": str(row.available_cash),
        "provider_as_of": _canonical_value(row.provider_as_of),
        "created_at": _canonical_value(row.created_at),
    }


def _position_input(row: PositionSnapshot) -> dict[str, object]:
    return {
        "id": str(row.id),
        "instrument_id": str(row.instrument_id),
        "currency": row.currency,
        "quantity": str(row.quantity),
        "available_quantity": str(row.available_quantity),
        "frozen_quantity": str(row.frozen_quantity),
        "avg_cost": None if row.avg_cost is None else str(row.avg_cost),
        "diluted_cost": None if row.diluted_cost is None else str(row.diluted_cost),
        "current_price": None if row.current_price is None else str(row.current_price),
        "provider_as_of": _canonical_value(row.provider_as_of),
        "created_at": _canonical_value(row.created_at),
    }


def _identity_input(identity: InstrumentIdentity) -> dict[str, object]:
    return {
        "instrument_id": str(identity.instrument_id),
        "instrument_record_id": _canonical_value(identity.instrument_record_id),
        "alias_record_id": _canonical_value(identity.alias_record_id),
        "instrument_created_at": _canonical_value(identity.instrument_created_at),
        "alias_created_at": _canonical_value(identity.alias_created_at),
        "asset_type": identity.asset_type,
        "exchange": identity.exchange,
        "currency": identity.currency,
        "risk_bucket": identity.risk_bucket,
        "canonical_ticker": identity.canonical_ticker,
        "restriction": identity.restriction,
        "refs": tuple(
            {
                "source": ref.source,
                "object_id": ref.object_id,
                "available_at": _canonical_value(ref.available_at),
                "content_hash": ref.content_hash,
            }
            for ref in identity.authority_refs
        ),
    }


def _identity_binding(identity: InstrumentIdentity) -> EvidenceBinding | None:
    if not identity.authority_refs:
        return None
    available_at = max(ref.available_at for ref in identity.authority_refs)
    return EvidenceBinding(
        available_at=available_at,
        as_of=available_at,
        refs=identity.authority_refs,
    )


def _db_identity_input(instrument: Instrument, alias: InstrumentAlias) -> dict[str, object]:
    return {
        "instrument": {
            "id": str(instrument.id),
            "asset_type": instrument.asset_type,
            "exchange": instrument.exchange,
            "currency": instrument.currency,
            "tick_size": None if instrument.tick_size is None else str(instrument.tick_size),
            "risk_bucket": instrument.risk_bucket,
            "created_at": _canonical_value(instrument.created_at),
        },
        "alias": {
            "id": str(alias.id),
            "instrument_id": str(alias.instrument_id),
            "symbol": alias.symbol,
            "alias_type": alias.alias_type,
            "valid_from": _canonical_value(alias.valid_from),
            "valid_to": _canonical_value(alias.valid_to),
            "created_at": _canonical_value(alias.created_at),
        },
    }


def _canonical_hash(value: object) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=_canonical_value,
    ).encode()
    return sha256(payload).hexdigest()


def _canonical_value(value: object) -> object:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal | UUID):
        return str(value)
    if value is None or isinstance(value, str | int | bool):
        return value
    raise TypeError(f"unsupported canonical diagnostic value: {type(value).__name__}")
