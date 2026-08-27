"""Fail-closed reconstruction of frozen backtest authorities from PostgreSQL."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal, cast
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.backtest.cost_policy import BACKTEST_COST_POLICY_VERSION
from nexus.backtest.engine import (
    AuthorityCollection,
    BacktestBar,
    BacktestDataAuthority,
    BacktestProtocol,
    BacktestResult,
    BenchmarkInstrumentAuthority,
    CorporateActionEvent,
    DelistingEvent,
    EqualWeightBenchmarkAuthority,
    ExchangeCalendarAuthority,
    PointInTimeDataset,
    PriceSeriesAuthority,
    PriceSourceSelectionAuthority,
    SectorClassification,
    UniverseSnapshot,
    UnresolvedDelistingEvent,
    _authority_bundle_hash,
    _mapped_session_on_or_after,
    run_backtest,
)
from nexus.backtest.persistence import (
    BacktestPersistenceAuthority,
    _validate_database_authority,
)
from nexus.domain.time import require_utc
from nexus.persistence.models.backtest import (
    BacktestAuthorityBundleRecord,
    BacktestAuthorityCollectionRecord,
    BacktestCorporateActionAuthorityMemberRecord,
    BacktestInstrumentAuthorityMemberRecord,
    BacktestPriceAuthorityMemberRecord,
    BacktestRunRecord,
    BacktestSectorAuthorityMemberRecord,
    BacktestUniverseAuthorityMemberRecord,
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
    StrategyDefinitionRecord,
)
from nexus.persistence.models.data import (
    DataArtifact,
    DataSnapshot,
    PriceBarRecord,
    PriceBarSetAuthority,
    PriceSeriesManifest,
    SourcePolicy,
)
from nexus.persistence.models.market import (
    Instrument,
    InstrumentAlias,
    SectorExposure,
    UniverseMember,
)
from nexus.persistence.models.market import UniverseSnapshot as UniverseSnapshotRecord
from nexus.persistence.models.optimization import CostPolicyRecord
from nexus.persistence.models.research import CorporateActionRecord, ResearchEvidenceRef
from nexus.strategy.dsl import (
    CompiledStrategy,
    baseline_feature_registry,
    compile_strategy,
    evolution_feature_registry_v1,
)

BACKTEST_AUTHORITY_INCOMPLETE = "BACKTEST_AUTHORITY_INCOMPLETE"

_ASSET_TYPE_BY_MASTER: dict[str, Literal["stock", "equity_etf", "non_equity_etf"]] = {
    "common_stock": "stock",
    "equity_etf": "equity_etf",
    "non_equity_etf": "non_equity_etf",
}


@dataclass(frozen=True)
class LoadedPointInTimeBacktest:
    """Complete database authority needed to replay one sealed baseline run."""

    data: PointInTimeDataset
    protocol: BacktestProtocol
    strategy: CompiledStrategy
    stored_result: BacktestResult
    baseline_run_id: UUID


async def load_point_in_time_dataset(
    session: AsyncSession,
    baseline_run_id: UUID,
    as_of: datetime,
) -> LoadedPointInTimeBacktest:
    """Rebuild and verify one sealed baseline without network or caller-supplied data."""
    cutoff = require_utc(as_of)
    try:
        return await _load_point_in_time_dataset(session, baseline_run_id, cutoff)
    except (LookupError, TypeError, ValueError) as error:
        if str(error).startswith(BACKTEST_AUTHORITY_INCOMPLETE):
            raise
        raise ValueError(f"{BACKTEST_AUTHORITY_INCOMPLETE}: {error}") from error


async def _load_point_in_time_dataset(
    session: AsyncSession,
    baseline_run_id: UUID,
    as_of: datetime,
) -> LoadedPointInTimeBacktest:
    run = await session.get(BacktestRunRecord, baseline_run_id, populate_existing=True)
    if (
        run is None
        or not run.graph_sealed
        or not run.strategy_data_complete
        or not run.benchmark_comparison_complete
        or run.promotable
        or run.historical_agent_overlay
        or run.authority_frozen_at > as_of
        or run.completed_at > as_of
    ):
        raise ValueError("baseline run is missing, unsealed, or not visible at the cutoff")
    definition = await session.get(
        StrategyDefinitionRecord, run.strategy_definition_id, populate_existing=True
    )
    if definition is None or definition.published_at > as_of:
        raise ValueError("baseline strategy definition is missing or invalid")
    if definition.strategy_version == "baseline_v1":
        registry = baseline_feature_registry()
        expected_lifecycle = "champion"
    else:
        registry = evolution_feature_registry_v1()
        expected_lifecycle = "challenger"
    if definition.lifecycle != expected_lifecycle:
        raise ValueError("baseline strategy definition is missing or invalid")
    strategy = compile_strategy(definition.document, registry)
    if strategy.expression_hash != definition.expression_hash:
        raise ValueError("baseline strategy expression hash is invalid")
    protocol = BacktestProtocol.model_validate(run.protocol)
    stored_result = BacktestResult.model_validate(
        {**run.result_payload, "result_hash": run.result_hash}
    )

    bundle = await session.get(
        BacktestAuthorityBundleRecord, run.authority_bundle_id, populate_existing=True
    )
    if (
        bundle is None
        or bundle.data_snapshot_id != run.data_snapshot_id
        or bundle.source_policy_id != run.source_policy_id
        or bundle.cost_policy_id != run.cost_policy_id
        or bundle.content_hash != run.authority_hash
        or bundle.completed_at > as_of
    ):
        raise ValueError("backtest authority bundle is missing or inconsistent")
    await _require_root_authorities(session, run, bundle, as_of)

    calendar_record = await session.get(
        ExchangeCalendarAuthorityRecord,
        bundle.calendar_authority_id,
        populate_existing=True,
    )
    if calendar_record is None or calendar_record.published_at > as_of:
        raise ValueError("calendar authority is missing at the cutoff")
    calendar_rows = tuple(
        await session.scalars(
            select(ExchangeCalendarSessionRecord)
            .where(ExchangeCalendarSessionRecord.calendar_authority_id == calendar_record.id)
            .order_by(ExchangeCalendarSessionRecord.session)
        )
    )
    _require_visible(calendar_rows, as_of, label="calendar sessions")
    calendar = ExchangeCalendarAuthority(
        authority_id=calendar_record.id,
        exchange=cast(Literal["XNYS"], calendar_record.exchange),
        version=calendar_record.version,
        source=calendar_record.source,
        range_start=calendar_record.range_start,
        range_end=calendar_record.range_end,
        query_parameters=calendar_record.query_parameters,
        completeness_status=cast(Literal["complete"], calendar_record.completeness_status),
        content_hash=calendar_record.content_hash,
        sessions=tuple(row.session for row in calendar_rows),
        decision_times={row.session: row.decision_at for row in calendar_rows},
    )

    collections = {
        kind: await _load_collection(session, bundle, kind, as_of)
        for kind in ("universe", "sector", "corporate_action")
    }
    instrument_ids = tuple(
        await session.scalars(
            select(BacktestInstrumentAuthorityMemberRecord.instrument_id)
            .where(BacktestInstrumentAuthorityMemberRecord.bundle_id == bundle.id)
            .order_by(BacktestInstrumentAuthorityMemberRecord.instrument_id)
        )
    )
    instruments = tuple(
        await session.scalars(select(Instrument).where(Instrument.id.in_(instrument_ids)))
    )
    _require_visible(instruments, as_of, label="instruments")
    if {item.id for item in instruments} != set(instrument_ids):
        raise ValueError("instrument exact set is incomplete")
    try:
        asset_types = {item.id: _ASSET_TYPE_BY_MASTER[item.asset_type] for item in instruments}
    except KeyError as error:
        raise ValueError("unsupported instrument asset type") from error
    risk_buckets = {item.id: item.risk_bucket for item in instruments}

    universe_snapshots = await _load_universe_snapshots(session, collections["universe"], as_of)
    sectors = await _load_sectors(session, collections["sector"], as_of)
    corporate_actions, delistings, unresolved_delistings = await _load_actions(
        session, collections["corporate_action"], calendar.sessions, as_of
    )
    price_series, source_selections, bars, adjusted_bars = await _load_prices(
        session, bundle, as_of
    )

    spy_alias = await session.get(
        InstrumentAlias, bundle.spy_instrument_alias_id, populate_existing=True
    )
    if spy_alias is None or spy_alias.created_at > as_of:
        raise ValueError("SPY alias authority is missing at the cutoff")
    spy = BenchmarkInstrumentAuthority(
        alias_id=spy_alias.id,
        instrument_id=spy_alias.instrument_id,
        symbol=cast(Literal["SPY"], spy_alias.symbol),
        alias_type=cast(Literal["ticker"], spy_alias.alias_type),
        valid_from=spy_alias.valid_from,
        valid_to=spy_alias.valid_to,
        created_at=spy_alias.created_at,
    )

    matching_data: list[PointInTimeDataset] = []
    for snapshot in universe_snapshots:
        benchmark = EqualWeightBenchmarkAuthority(
            universe_snapshot_id=snapshot.authority_id,
            key=snapshot.key,
            universe_version=snapshot.version,
            content_hash=snapshot.content_hash,
            source=snapshot.source,
            effective_session=snapshot.effective_session,
            published_at=snapshot.published_at,
            instrument_ids=snapshot.members,
        )
        try:
            candidate = PointInTimeDataset(
                bars=bars,
                adjusted_bars=adjusted_bars,
                universe_snapshots=universe_snapshots,
                asset_types=asset_types,
                risk_buckets=risk_buckets,
                sector_classifications=sectors,
                authority=BacktestDataAuthority(
                    authority_bundle_id=bundle.id,
                    data_snapshot_id=bundle.data_snapshot_id,
                    source_policy_id=bundle.source_policy_id,
                    cost_policy_id=bundle.cost_policy_id,
                    cost_policy_version=BACKTEST_COST_POLICY_VERSION,
                    exchange_calendar=calendar,
                    universe_collection=collections["universe"],
                    sector_collection=collections["sector"],
                    corporate_action_collection=collections["corporate_action"],
                    price_series=price_series,
                    source_selections=source_selections,
                    spy_benchmark=spy,
                    equal_weight_benchmark=benchmark,
                ),
                spy_instrument_id=spy.instrument_id,
                corporate_actions=corporate_actions,
                delistings=delistings,
                unresolved_delistings=unresolved_delistings,
            )
        except ValueError:
            continue
        if _authority_bundle_hash(candidate) == run.authority_hash:
            matching_data.append(candidate)
    if len(matching_data) != 1:
        raise ValueError("equal-weight benchmark authority is missing or ambiguous")
    data = matching_data[0]

    authority = BacktestPersistenceAuthority(
        authority_bundle_id=bundle.id,
        data_snapshot_id=bundle.data_snapshot_id,
        source_policy_id=bundle.source_policy_id,
        cost_policy_id=bundle.cost_policy_id,
        completed_at=run.completed_at,
        data_cutoff_at=bundle.completed_at,
    )
    await _validate_database_authority(
        session,
        data,
        protocol,
        stored_result,
        authority,
        run.authority_frozen_at,
    )
    replay = run_backtest(strategy, data, protocol)
    if replay != stored_result:
        raise ValueError("stored baseline result does not replay from its authority")
    return LoadedPointInTimeBacktest(
        data=data,
        protocol=protocol,
        strategy=strategy,
        stored_result=stored_result,
        baseline_run_id=run.id,
    )


async def _require_root_authorities(
    session: AsyncSession,
    run: BacktestRunRecord,
    bundle: BacktestAuthorityBundleRecord,
    as_of: datetime,
) -> None:
    snapshot = await session.get(DataSnapshot, bundle.data_snapshot_id, populate_existing=True)
    source_policy = await session.get(SourcePolicy, bundle.source_policy_id, populate_existing=True)
    cost_policy = await session.get(CostPolicyRecord, bundle.cost_policy_id, populate_existing=True)
    if (
        snapshot is None
        or source_policy is None
        or cost_policy is None
        or snapshot.available_at > as_of
        or snapshot.received_at > as_of
        or snapshot.created_at > as_of
        or source_policy.created_at > as_of
        or cost_policy.available_at > as_of
        or cost_policy.created_at > as_of
        or snapshot.payload.get("authority_hash") != run.authority_hash
        or snapshot.payload.get("data_hash") != run.data_hash
    ):
        raise ValueError("root authority facts are incomplete at the cutoff")


async def _load_collection(
    session: AsyncSession,
    bundle: BacktestAuthorityBundleRecord,
    kind: str,
    as_of: datetime,
) -> AuthorityCollection:
    collection_id = cast(UUID, getattr(bundle, f"{kind}_collection_id"))
    record = await session.get(
        BacktestAuthorityCollectionRecord, collection_id, populate_existing=True
    )
    collection_id_column, member_column = {
        "universe": (
            BacktestUniverseAuthorityMemberRecord.collection_id,
            BacktestUniverseAuthorityMemberRecord.universe_snapshot_id,
        ),
        "sector": (
            BacktestSectorAuthorityMemberRecord.collection_id,
            BacktestSectorAuthorityMemberRecord.sector_exposure_id,
        ),
        "corporate_action": (
            BacktestCorporateActionAuthorityMemberRecord.collection_id,
            BacktestCorporateActionAuthorityMemberRecord.corporate_action_id,
        ),
    }[kind]
    members = tuple(
        await session.scalars(
            select(member_column)
            .where(collection_id_column == collection_id)
            .order_by(member_column)
        )
    )
    if record is None or record.completed_at > as_of:
        raise ValueError(f"{kind} collection is missing at the cutoff")
    return AuthorityCollection(
        authority_id=record.id,
        kind=cast(Literal["universe", "sector", "corporate_action"], record.kind),
        source=record.source,
        range_start=record.range_start,
        range_end=record.range_end,
        query_parameters=record.query_parameters,
        member_ids=members,
        completeness_status=cast(Literal["complete"], record.completeness_status),
        content_hash=record.content_hash,
    )


async def _load_universe_snapshots(
    session: AsyncSession,
    collection: AuthorityCollection,
    as_of: datetime,
) -> tuple[UniverseSnapshot, ...]:
    records = tuple(
        await session.scalars(
            select(UniverseSnapshotRecord)
            .where(UniverseSnapshotRecord.id.in_(collection.member_ids))
            .order_by(UniverseSnapshotRecord.effective_from, UniverseSnapshotRecord.id)
        )
    )
    if {item.id for item in records} != set(collection.member_ids):
        raise ValueError("universe exact set is incomplete")
    result: list[UniverseSnapshot] = []
    for record in records:
        if record.published_at is None or record.published_at > as_of or record.created_at > as_of:
            raise ValueError("universe authority is not visible at the cutoff")
        members = tuple(
            await session.scalars(
                select(UniverseMember.instrument_id)
                .where(UniverseMember.universe_snapshot_id == record.id)
                .order_by(UniverseMember.instrument_id)
            )
        )
        result.append(
            UniverseSnapshot(
                authority_id=record.id,
                key=cast(Literal["us_liquid"], record.key),
                version=record.version,
                content_hash=record.content_hash,
                source=record.source,
                effective_session=record.effective_from,
                published_at=record.published_at,
                members=members,
            )
        )
    return tuple(result)


async def _load_sectors(
    session: AsyncSession,
    collection: AuthorityCollection,
    as_of: datetime,
) -> tuple[SectorClassification, ...]:
    records = tuple(
        await session.scalars(
            select(SectorExposure)
            .where(SectorExposure.id.in_(collection.member_ids))
            .order_by(SectorExposure.instrument_id, SectorExposure.as_of, SectorExposure.id)
        )
    )
    if {item.id for item in records} != set(collection.member_ids):
        raise ValueError("sector exact set is incomplete")
    _require_visible(records, as_of, label="sector authorities")
    return tuple(
        SectorClassification(
            authority_id=item.id,
            instrument_id=item.instrument_id,
            sector=item.sector_code,
            effective_session=item.as_of,
            published_at=item.created_at,
            source_uri=item.source_url,
            content_hash=item.content_hash,
            weight=item.weight,
            synthetic=item.synthetic,
            reviewed=item.reviewed,
        )
        for item in records
    )


async def _load_actions(
    session: AsyncSession,
    collection: AuthorityCollection,
    sessions: tuple[date, ...],
    as_of: datetime,
) -> tuple[
    tuple[CorporateActionEvent, ...],
    tuple[DelistingEvent, ...],
    tuple[UnresolvedDelistingEvent, ...],
]:
    rows = tuple(
        await session.execute(
            select(CorporateActionRecord, ResearchEvidenceRef)
            .join(
                ResearchEvidenceRef,
                ResearchEvidenceRef.id == CorporateActionRecord.evidence_ref_id,
            )
            .where(CorporateActionRecord.id.in_(collection.member_ids))
            .order_by(CorporateActionRecord.id)
        )
    )
    if {row[0].id for row in rows} != set(collection.member_ids):
        raise ValueError("corporate-action exact set is incomplete")
    actions: list[CorporateActionEvent] = []
    delistings: list[DelistingEvent] = []
    unresolved: list[UnresolvedDelistingEvent] = []
    for record, evidence in rows:
        if (
            record.instrument_id is None
            or record.created_at > as_of
            or evidence.available_at > as_of
            or evidence.created_at > as_of
        ):
            raise ValueError("corporate-action authority is not visible at the cutoff")
        if record.action_type in {"cash_dividend", "stock_split", "reverse_split"}:
            if record.ex_date is None or record.rate is None:
                raise ValueError("mapped corporate action is incomplete")
            mapped_session = _mapped_session_on_or_after(sessions, record.ex_date)
            payable_session = (
                None
                if record.payable_date is None
                else _mapped_session_on_or_after(sessions, record.payable_date)
            )
            if mapped_session is None:
                raise ValueError("corporate action is outside the frozen calendar")
            actions.append(
                CorporateActionEvent(
                    authority_id=record.id,
                    instrument_id=record.instrument_id,
                    source=record.source,
                    provider_event_id=record.provider_event_id,
                    provider_action_type=cast(
                        Literal["cash_dividend", "stock_split", "reverse_split"],
                        record.action_type,
                    ),
                    status=cast(Literal["complete"], record.status),
                    provider_ex_date=record.ex_date,
                    provider_record_date=record.record_date,
                    provider_payable_date=record.payable_date,
                    session=mapped_session,
                    payable_session=payable_session,
                    action_type="dividend" if record.action_type == "cash_dividend" else "split",
                    value=record.rate,
                    available_at=evidence.available_at,
                    evidence_hash=evidence.content_hash,
                    revision=record.revision,
                )
            )
            continue
        mapped_session = _mapped_session_on_or_after(sessions, record.process_date)
        if mapped_session is None:
            raise ValueError("delisting is outside the frozen calendar")
        if record.action_type == "delisting" or record.rate is None:
            unresolved.append(
                UnresolvedDelistingEvent(
                    authority_id=record.id,
                    instrument_id=record.instrument_id,
                    source=record.source,
                    provider_event_id=record.provider_event_id,
                    provider_action_type=cast(
                        Literal["delisting", "bankruptcy"], record.action_type
                    ),
                    status=cast(Literal["complete"], record.status),
                    provider_process_date=record.process_date,
                    session=mapped_session,
                    available_at=evidence.available_at,
                    evidence_hash=evidence.content_hash,
                    revision=record.revision,
                )
            )
        else:
            delistings.append(
                DelistingEvent(
                    authority_id=record.id,
                    instrument_id=record.instrument_id,
                    source=record.source,
                    provider_event_id=record.provider_event_id,
                    provider_action_type=cast(
                        Literal["bankruptcy", "merger_cash"], record.action_type
                    ),
                    recovery_method=(
                        "zero_recovery" if record.action_type == "bankruptcy" else "cash_recovery"
                    ),
                    status=cast(Literal["complete"], record.status),
                    provider_process_date=record.process_date,
                    session=mapped_session,
                    recovery_price=record.rate,
                    available_at=evidence.available_at,
                    evidence_hash=evidence.content_hash,
                    revision=record.revision,
                )
            )
    return tuple(actions), tuple(delistings), tuple(unresolved)


async def _load_prices(
    session: AsyncSession,
    bundle: BacktestAuthorityBundleRecord,
    as_of: datetime,
) -> tuple[
    tuple[PriceSeriesAuthority, ...],
    tuple[PriceSourceSelectionAuthority, ...],
    tuple[BacktestBar, ...],
    tuple[BacktestBar, ...],
]:
    source_selections = tuple(
        PriceSourceSelectionAuthority.model_validate(item)
        for item in bundle.source_selection_reports
    )
    selected_ids = {item.selected_price_bar_set_authority_id for item in source_selections}
    member_ids = set(
        await session.scalars(
            select(BacktestPriceAuthorityMemberRecord.price_bar_set_authority_id).where(
                BacktestPriceAuthorityMemberRecord.bundle_id == bundle.id
            )
        )
    )
    expected_member_ids = {
        authority_id
        for item in source_selections
        for authority_id in item.candidate_price_bar_set_authority_ids
    }
    if member_ids != expected_member_ids or not selected_ids <= member_ids:
        raise ValueError("price authority exact set is incomplete")
    rows = tuple(
        await session.execute(
            select(PriceBarSetAuthority, PriceSeriesManifest, DataArtifact)
            .join(
                PriceSeriesManifest,
                PriceSeriesManifest.id == PriceBarSetAuthority.price_series_manifest_id,
            )
            .join(DataArtifact, DataArtifact.id == PriceSeriesManifest.data_artifact_id)
            .where(PriceBarSetAuthority.id.in_(selected_ids))
            .order_by(PriceSeriesManifest.instrument, PriceSeriesManifest.adjustment)
        )
    )
    if {row[0].id for row in rows} != selected_ids:
        raise ValueError("selected price authority is missing")
    authorities: list[PriceSeriesAuthority] = []
    raw_bars: list[BacktestBar] = []
    adjusted_bars: list[BacktestBar] = []
    for authority, manifest, artifact in rows:
        if (
            authority.available_at > as_of
            or authority.created_at > as_of
            or manifest.created_at > as_of
            or artifact.available_at > as_of
            or artifact.received_at > as_of
            or artifact.created_at > as_of
        ):
            raise ValueError("price authority is not visible at the cutoff")
        price_rows = tuple(
            await session.scalars(
                select(PriceBarRecord)
                .where(PriceBarRecord.price_series_manifest_id == manifest.id)
                .order_by(
                    PriceBarRecord.session,
                    PriceBarRecord.observed_at,
                    PriceBarRecord.revision,
                    PriceBarRecord.instrument_id,
                )
            )
        )
        _require_visible(price_rows, as_of, label="price bars")
        built_bars = tuple(
            BacktestBar(
                instrument_id=item.instrument_id,
                session=item.session,
                open=item.open,
                high=item.high,
                low=item.low,
                close=item.close,
                volume=item.volume,
                observed_at=item.observed_at,
                available_at=item.available_at,
                received_at=item.received_at,
                revision=item.revision,
                quality_flags=tuple(item.quality_flags),
            )
            for item in price_rows
        )
        target = raw_bars if manifest.adjustment == "raw" else adjusted_bars
        target.extend(built_bars)
        authorities.append(
            PriceSeriesAuthority(
                price_bar_set_authority_id=authority.id,
                price_series_manifest_id=manifest.id,
                instrument_id=UUID(manifest.instrument),
                source=manifest.source,
                feed=manifest.feed,
                delivery_mode=cast(
                    Literal["real_time", "delayed", "fixture"], manifest.delivery_mode
                ),
                adjustment=cast(Literal["raw", "all"], manifest.adjustment),
                interval=cast(Literal["1Day"], manifest.interval),
                manifest_hash=artifact.sha256,
                content_hash=authority.content_hash,
                start_at=manifest.start_at,
                end_at=manifest.end_at,
                row_count=manifest.row_count,
            )
        )

    def bar_key(item: BacktestBar) -> tuple[date, datetime, str, int]:
        return item.session, item.observed_at, item.revision, item.instrument_id.int

    return (
        tuple(authorities),
        source_selections,
        tuple(sorted(raw_bars, key=bar_key)),
        tuple(sorted(adjusted_bars, key=bar_key)),
    )


def _require_visible(rows: tuple[object, ...], as_of: datetime, *, label: str) -> None:
    if any(
        not isinstance(created_at := getattr(row, "created_at", None), datetime)
        or require_utc(created_at) > as_of
        for row in rows
    ):
        raise ValueError(f"{label} are not visible at the cutoff")
