"""Database-authoritative diagnostic evidence assembly."""

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from hashlib import sha256
from typing import Literal, cast
from uuid import UUID

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.diagnostics.artifacts import ArtifactByteReader, parse_price_series_artifact
from nexus.diagnostics.models import (
    DiagnosticEvidence,
    EvidenceBinding,
    InstrumentIdentity,
    MarketEvidence,
)
from nexus.domain.evidence import EvidenceRef
from nexus.market.universe import stable_instrument_id
from nexus.persistence.models.data import (
    DataArtifact,
    PriceSeriesManifest,
)
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
    select_price_series,
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
    benchmark = await _canonical_spy_series(session, snapshot.as_of, artifact_reader)
    selected: dict[UUID, tuple[PriceSeries, EvidenceBinding]] = {}
    source_conflict = False
    for position in snapshot.positions:
        result, conflict = await _selected_series(
            session, position.instrument_id, snapshot.as_of, artifact_reader
        )
        source_conflict = source_conflict or conflict
        if result is not None:
            selected[position.instrument_id] = result
    if benchmark is None:
        trading_sessions: tuple[date, ...] = ()
    else:
        trading_sessions = tuple(
            sorted(
                {
                    bar.session
                    for bar in benchmark[0].bars
                    if bar.available_at <= snapshot.as_of and bar.session <= snapshot.as_of.date()
                }
            )
        )
    adv20: dict[UUID, Decimal] = {}
    adv_bindings: dict[UUID, EvidenceBinding] = {}
    for instrument_id, (series, binding) in selected.items():
        adv = _adv20(series, trading_sessions)
        if adv is not None:
            adv20[instrument_id] = adv
            adv_bindings[instrument_id] = binding
    quality: Literal["PASS", "PARTIAL", "FAIL"] = (
        "PASS" if benchmark is not None and len(selected) == len(snapshot.positions) else "PARTIAL"
    )
    evidence = DiagnosticEvidence(
        snapshot=cast(EvidenceBinding, snapshot_bindings["snapshot"]),
        cash=cast(dict[str, EvidenceBinding], snapshot_bindings["cash"]),
        position_prices=cast(dict[UUID, EvidenceBinding], snapshot_bindings["position_prices"]),
        adv20=adv_bindings,
        price_series={key: value[1] for key, value in selected.items()},
        benchmark=None if benchmark is None else benchmark[1],
        trading_calendar=None if benchmark is None else benchmark[1],
    )
    market = MarketEvidence(
        prices={key: value[0] for key, value in selected.items()},
        adv20=adv20,
        quality=quality,
        identities=identities,
        trading_sessions=trading_sessions,
        benchmark=None if benchmark is None else benchmark[0],
        evidence=evidence,
        source_conflict=source_conflict,
    )
    return DiagnosticAssembly(
        market=market,
        canonical_inputs={
            "snapshot": snapshot_inputs,
            "series": tuple(
                _binding_input(instrument_id, binding)
                for instrument_id, (_, binding) in sorted(
                    selected.items(), key=lambda item: str(item[0])
                )
            ),
            "benchmark": (None if benchmark is None else _binding_input(_SPY_ID, benchmark[1])),
            "adv20": {
                str(key): str(value)
                for key, value in sorted(adv20.items(), key=lambda item: str(item[0]))
            },
            "trading_sessions": tuple(session.isoformat() for session in trading_sessions),
        },
    )


async def _canonical_spy_series(
    session: AsyncSession, as_of: datetime, reader: ArtifactByteReader
) -> tuple[PriceSeries, EvidenceBinding] | None:
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
    return (await _selected_series(session, _SPY_ID, as_of, reader))[0]


async def _selected_series(
    session: AsyncSession,
    instrument_id: UUID,
    as_of: datetime,
    reader: ArtifactByteReader,
) -> tuple[tuple[PriceSeries, EvidenceBinding] | None, bool]:
    instrument = await session.scalar(
        select(Instrument).where(
            Instrument.id == instrument_id,
            Instrument.created_at <= as_of,
        )
    )
    if instrument is None:
        return None, False
    rows = (
        await session.execute(
            select(PriceSeriesManifest, DataArtifact)
            .join(DataArtifact, DataArtifact.id == PriceSeriesManifest.data_artifact_id)
            .where(
                PriceSeriesManifest.instrument == str(instrument_id),
                PriceSeriesManifest.end_at <= as_of,
                DataArtifact.available_at <= as_of,
                DataArtifact.received_at <= as_of,
            )
            .order_by(
                desc(DataArtifact.available_at),
                desc(DataArtifact.received_at),
                desc(PriceSeriesManifest.id),
            )
        )
    ).all()
    candidates: list[PriceSeries] = []
    bindings: dict[tuple[str, str, str, str], EvidenceBinding] = {}
    for manifest, artifact in rows:
        key = (manifest.source, manifest.feed, manifest.adjustment, manifest.delivery_mode)
        if key in bindings:
            continue
        payload = await reader(artifact.uri)
        series, artifact_ref = parse_price_series_artifact(
            payload, artifact, manifest, instrument_id
        )
        policy_ref = await _source_policy_ref(session, as_of)
        binding = EvidenceBinding(
            available_at=max(artifact.available_at, policy_ref.available_at),
            as_of=max(bar.observed_at or bar.available_at for bar in series.bars),
            session=max(bar.session for bar in series.bars),
            refs=(artifact_ref, policy_ref),
        )
        bindings[key] = binding
        candidates.append(series)
    selection = await select_price_series(session, candidates, instrument.tick_size, as_of)
    conflict = "SOURCE_CONFLICT" in selection.reasons
    if selection.selected is None or selection.quality != "PASS":
        return None, conflict
    key = (
        selection.selected.source,
        selection.selected.feed,
        selection.selected.adjustment,
        selection.selected.delivery_mode,
    )
    return (selection.selected, bindings[key]), conflict


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
        "as_of": _canonical_value(record.as_of),
        "status": record.status,
        "position_hash_version": record.position_hash_version,
        "position_hash": record.position_hash,
        "order_hash_version": record.order_hash_version,
        "order_hash": record.order_hash,
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
    }


def _position_input(row: PositionSnapshot) -> dict[str, object]:
    return {
        "id": str(row.id),
        "instrument_id": str(row.instrument_id),
        "currency": row.currency,
        "quantity": str(row.quantity),
        "current_price": None if row.current_price is None else str(row.current_price),
        "provider_as_of": _canonical_value(row.provider_as_of),
    }


def _binding_input(instrument_id: UUID, binding: EvidenceBinding) -> dict[str, object]:
    return {
        "instrument_id": str(instrument_id),
        "source_policy_decision": "PASS",
        "available_at": _canonical_value(binding.available_at),
        "as_of": _canonical_value(binding.as_of),
        "session": _canonical_value(binding.session),
        "refs": tuple(
            {"source": ref.source, "object_id": ref.object_id, "content_hash": ref.content_hash}
            for ref in binding.refs
        ),
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
