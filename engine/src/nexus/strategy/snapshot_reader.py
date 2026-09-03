"""Explicit frozen database references for unranked features; no source promotion or I/O."""

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, model_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.market.price_bar_authority import prepare_price_bar_set
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.data import (
    DataArtifact,
    PriceBarRecord,
    PriceBarSetAuthority,
    PriceSeriesManifest,
)
from nexus.persistence.models.market import (
    Instrument,
    SectorExposure,
    UniverseMember,
    UniverseSnapshot,
)
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.quality.source_policy import (
    SOURCE_POLICY_V2_ID,
    source_policy_v2_supports_adjusted_prices,
)
from nexus.risk.authority import RiskAuthorityInput
from nexus.strategy.features import BacktestBar, SectorClassification
from nexus.strategy.snapshot import UnrankedStrategySnapshot, prepare_unranked_strategy_snapshot


class StrategySnapshotUnavailable(ValueError):
    """A fixed, non-sensitive reason that frozen inputs cannot serve this request."""


class FrozenStrategyReferences(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    market: RiskAuthorityInput
    raw_bar_set_ids: tuple[UUID, ...]
    adjusted_bar_set_ids: tuple[UUID, ...]
    sector_exposure_ids: tuple[UUID, ...]

    @property
    def canonical_payload(self) -> dict[str, object]:
        payload = self.model_dump(mode="json")
        for field in ("raw_bar_set_ids", "adjusted_bar_set_ids", "sector_exposure_ids"):
            payload[field] = sorted(payload[field])
        return payload

    @property
    def content_hash(self) -> str:
        return hashlib.sha256(
            json.dumps(self.canonical_payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    @model_validator(mode="after")
    def _unique(self) -> "FrozenStrategyReferences":
        prices = self.raw_bar_set_ids + self.adjusted_bar_set_ids
        if (
            not self.raw_bar_set_ids
            or not self.adjusted_bar_set_ids
            or len(set(prices)) != len(prices)
            or len(set(self.sector_exposure_ids)) != len(self.sector_exposure_ids)
        ):
            raise ValueError("explicit price references must be nonempty and unique")
        return self


def require_forward_adjusted_authority(references: FrozenStrategyReferences) -> None:
    """Current frozen v2 is raw-only, even if a manifest happens to say all."""
    if references.market.source_policy_id != SOURCE_POLICY_V2_ID:
        raise StrategySnapshotUnavailable("STRATEGY_SNAPSHOT_SOURCE_POLICY_UNAVAILABLE")
    if not source_policy_v2_supports_adjusted_prices():
        raise StrategySnapshotUnavailable("STRATEGY_SNAPSHOT_ADJUSTED_AUTHORITY_UNAVAILABLE")


@dataclass(frozen=True)
class LoadedStrategyFeatures:
    snapshot: UnrankedStrategySnapshot
    raw_authorities: tuple[tuple[UUID, UUID], ...]


def _visible(cutoff: datetime, *times: datetime | None) -> None:
    if any(value is None or value > cutoff for value in times):
        raise StrategySnapshotUnavailable("STRATEGY_SNAPSHOT_FACT_NOT_VISIBLE")


async def _bars(
    session: AsyncSession,
    ids: tuple[UUID, ...],
    *,
    adjustment: str,
    required: set[UUID],
    cutoff: datetime,
) -> tuple[tuple[BacktestBar, ...], tuple[tuple[UUID, UUID], ...]]:
    rows = (
        await session.execute(
            select(PriceBarSetAuthority, PriceSeriesManifest, DataArtifact, ResearchEvidenceRef)
            .join(
                PriceSeriesManifest,
                PriceSeriesManifest.id == PriceBarSetAuthority.price_series_manifest_id,
            )
            .join(DataArtifact, DataArtifact.id == PriceSeriesManifest.data_artifact_id)
            .join(
                ResearchEvidenceRef, ResearchEvidenceRef.id == PriceBarSetAuthority.evidence_ref_id
            )
            .where(PriceBarSetAuthority.id.in_(ids))
        )
    ).all()
    if {row[0].id for row in rows} != set(ids):
        raise StrategySnapshotUnavailable("STRATEGY_SNAPSHOT_PRICE_REFERENCE_MISSING")
    values: list[BacktestBar] = []
    bindings: dict[UUID, UUID] = {}
    for authority, manifest, artifact, evidence in rows:
        _visible(
            cutoff,
            authority.created_at,
            authority.available_at,
            manifest.created_at,
            artifact.created_at,
            artifact.available_at,
            artifact.received_at,
            evidence.created_at,
            evidence.available_at,
        )
        if manifest.adjustment != adjustment or manifest.interval != "1Day":
            raise StrategySnapshotUnavailable("STRATEGY_SNAPSHOT_PRICE_PROTOCOL_MISMATCH")
        # Synthetic fixtures have the engine's explicit USD/share convention. Current
        # live provider records do not establish share-volume units for ADV20.
        if manifest.source != "fixture" or manifest.delivery_mode != "fixture":
            raise StrategySnapshotUnavailable("STRATEGY_SNAPSHOT_PRICE_UNITS_UNAVAILABLE")
        prepared = await prepare_price_bar_set(session, manifest.id, as_of=cutoff)
        if (
            prepared is None
            or any(
                getattr(prepared, name) != getattr(authority, name)
                for name in (
                    "content_hash",
                    "byte_length",
                    "bar_count",
                    "first_session",
                    "last_session",
                    "available_at",
                )
            )
            or evidence.content_hash != authority.content_hash
            or evidence.byte_length != authority.byte_length
        ):
            raise StrategySnapshotUnavailable("STRATEGY_SNAPSHOT_PRICE_HASH_MISMATCH")
        bars = tuple(
            await session.scalars(
                select(PriceBarRecord)
                .where(PriceBarRecord.price_series_manifest_id == manifest.id)
                .order_by(PriceBarRecord.session)
            )
        )
        instrument_ids = {bar.instrument_id for bar in bars}
        if len(instrument_ids) != 1:
            raise StrategySnapshotUnavailable("STRATEGY_SNAPSHOT_PRICE_COVERAGE_MISMATCH")
        instrument_id = next(iter(instrument_ids))
        if (
            instrument_id not in required
            or instrument_id in bindings
            or manifest.instrument != str(instrument_id)
        ):
            raise StrategySnapshotUnavailable("STRATEGY_SNAPSHOT_PRICE_COVERAGE_MISMATCH")
        bindings[instrument_id] = authority.id
        for bar in bars:
            _visible(cutoff, bar.created_at, bar.available_at, bar.received_at)
            values.append(
                BacktestBar(
                    instrument_id=bar.instrument_id,
                    session=bar.session,
                    open=bar.open,
                    high=bar.high,
                    low=bar.low,
                    close=bar.close,
                    volume=bar.volume,
                    observed_at=bar.observed_at,
                    available_at=bar.available_at,
                    received_at=bar.received_at,
                    revision=bar.revision,
                    quality_flags=tuple(bar.quality_flags),
                )
            )
    if set(bindings) != required:
        raise StrategySnapshotUnavailable("STRATEGY_SNAPSHOT_PRICE_COVERAGE_MISMATCH")
    return tuple(values), tuple(sorted(bindings.items(), key=lambda item: item[0].int))


async def load_unranked_strategy_features(
    session: AsyncSession,
    *,
    references: FrozenStrategyReferences,
    current_positions: tuple[UUID, ...],
) -> LoadedStrategyFeatures:
    """Read explicit immutable inputs only, without claiming forward-purpose permission.

    Fixture assembly is supported independently of admission. The forward entry must
    call require_forward_adjusted_authority; live units remain unavailable, never guessed.
    """
    with session.no_autoflush:
        try:
            return await _load_features(session, references, current_positions)
        except StrategySnapshotUnavailable:
            raise
        except (ValueError, TypeError) as error:
            raise StrategySnapshotUnavailable("STRATEGY_SNAPSHOT_FEATURES_UNAVAILABLE") from error


async def _load_features(
    session: AsyncSession, references: FrozenStrategyReferences, current_positions: tuple[UUID, ...]
) -> LoadedStrategyFeatures:
    market = references.market
    cutoff = market.decision_time
    calendar = await session.get(ExchangeCalendarAuthorityRecord, market.calendar_authority_id)
    universe = await session.get(UniverseSnapshot, market.universe_snapshot_id)
    if calendar is None or universe is None:
        raise StrategySnapshotUnavailable("STRATEGY_SNAPSHOT_ROOT_REFERENCE_MISSING")
    _visible(
        cutoff,
        calendar.created_at,
        calendar.published_at,
        universe.created_at,
        universe.published_at,
    )
    if (
        calendar.completeness_status != "complete"
        or universe.effective_from > market.decision_session
    ):
        raise StrategySnapshotUnavailable("STRATEGY_SNAPSHOT_ROOT_UNAVAILABLE")
    sessions = tuple(
        await session.scalars(
            select(ExchangeCalendarSessionRecord)
            .where(ExchangeCalendarSessionRecord.calendar_authority_id == calendar.id)
            .order_by(ExchangeCalendarSessionRecord.session)
        )
    )
    for row in sessions:
        _visible(cutoff, row.created_at)
    members = tuple(
        await session.scalars(
            select(UniverseMember).where(UniverseMember.universe_snapshot_id == universe.id)
        )
    )
    for member in members:
        _visible(cutoff, member.created_at)
    required = {item.instrument_id for item in members} | set(current_positions)
    instruments = tuple(
        await session.scalars(select(Instrument).where(Instrument.id.in_(required)))
    )
    if {row.id for row in instruments} != required:
        raise StrategySnapshotUnavailable("STRATEGY_SNAPSHOT_INSTRUMENT_MISSING")
    kinds: dict[UUID, Literal["stock", "equity_etf", "non_equity_etf"]] = {}
    for instrument in instruments:
        _visible(cutoff, instrument.created_at)
        kind = "stock" if instrument.asset_type == "common_stock" else instrument.asset_type
        if instrument.currency != "USD" or kind not in {"stock", "equity_etf", "non_equity_etf"}:
            raise StrategySnapshotUnavailable("STRATEGY_SNAPSHOT_INSTRUMENT_UNSUPPORTED")
        kinds[instrument.id] = cast(Literal["stock", "equity_etf", "non_equity_etf"], kind)
    raw, bindings = await _bars(
        session, references.raw_bar_set_ids, adjustment="raw", required=required, cutoff=cutoff
    )
    adjusted, _ = await _bars(
        session, references.adjusted_bar_set_ids, adjustment="all", required=required, cutoff=cutoff
    )
    sectors = tuple(
        await session.scalars(
            select(SectorExposure).where(SectorExposure.id.in_(references.sector_exposure_ids))
        )
    )
    if {row.id for row in sectors} != set(references.sector_exposure_ids) or any(
        row.instrument_id not in required for row in sectors
    ):
        raise StrategySnapshotUnavailable("STRATEGY_SNAPSHOT_SECTOR_REFERENCE_MISMATCH")
    for sector in sectors:
        _visible(cutoff, sector.created_at)
    features = prepare_unranked_strategy_snapshot(
        decision_session=market.decision_session,
        next_session=market.execution_session,
        decision_time=cutoff,
        calendar_sessions=tuple(row.session for row in sessions),
        decision_times={row.session: row.decision_at for row in sessions},
        raw_bars=raw,
        adjusted_bars=adjusted,
        universe_members=tuple(row.instrument_id for row in members),
        asset_types=kinds,
        sector_classifications=tuple(
            SectorClassification(
                authority_id=row.id,
                instrument_id=row.instrument_id,
                sector=row.sector_code,
                effective_session=row.as_of,
                published_at=row.created_at,
                source_uri=row.source_url,
                content_hash=row.content_hash,
                weight=row.weight,
                synthetic=row.synthetic,
                reviewed=row.reviewed,
            )
            for row in sectors
        ),
        sector_source=universe.source,
        current_positions=current_positions,
    )
    return LoadedStrategyFeatures(features, bindings)
