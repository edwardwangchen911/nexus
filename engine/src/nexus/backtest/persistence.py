"""Atomic writer for immutable deterministic strategy and backtest evidence."""

import hashlib
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from importlib.metadata import version as package_version
from typing import Literal, cast
from uuid import UUID

import exchange_calendars  # type: ignore[import-untyped]
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.backtest.cost_policy import load_backtest_cost_policy_v1
from nexus.backtest.engine import (
    BacktestProtocol,
    BacktestResult,
    CorporateActionEvent,
    DelistingEvent,
    ExchangeCalendarAuthority,
    PointInTimeDataset,
    PriceSourceSelectionAuthority,
    UnresolvedDelistingEvent,
    _audited_data_hash,
    _authority_bundle_hash,
    _engine_code_hash,
    _hash,
    _mapped_session_on_or_after,
    run_backtest,
    run_challenger_backtest_v1,
)
from nexus.backtest.metrics import BacktestMetrics
from nexus.domain.ids import new_uuid7
from nexus.domain.time import require_utc
from nexus.market.scope import US_EQUITY_EXCHANGES
from nexus.persistence.models.backtest import (
    BacktestAuthorityBundleRecord,
    BacktestAuthorityCollectionRecord,
    BacktestAuthorityPublicationMarkRecord,
    BacktestCorporateActionAuthorityMemberRecord,
    BacktestFillRecord,
    BacktestInstrumentAuthorityMemberRecord,
    BacktestMetricRecord,
    BacktestNavPointRecord,
    BacktestOrderOutcomeRecord,
    BacktestPriceAuthorityMemberRecord,
    BacktestRunRecord,
    BacktestSectorAuthorityMemberRecord,
    BacktestUniverseAuthorityMemberRecord,
    BacktestWindowRecord,
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
)
from nexus.persistence.models.market import (
    Instrument,
    InstrumentAlias,
    SectorExposure,
    UniverseMember,
)
from nexus.persistence.models.market import (
    UniverseSnapshot as UniverseSnapshotRecord,
)
from nexus.persistence.models.research import CorporateActionRecord, ResearchEvidenceRef
from nexus.providers.base import InstrumentId, PriceBar, PriceSeries
from nexus.quality.price_revision import canonical_price_bars
from nexus.quality.source_policy import (
    apply_source_policy_v1,
    load_source_policy_v1,
    price_manifest_admissible_under_source_policy_v1,
)
from nexus.strategy.dsl import (
    CompiledStrategy,
    baseline_feature_registry,
    baseline_v1_document,
    compile_strategy,
    evolution_feature_registry_v1,
)

_BACKTEST_ASSET_TYPE_BY_MASTER = {
    "common_stock": "stock",
    "equity_etf": "equity_etf",
    "non_equity_etf": "non_equity_etf",
}


@dataclass(frozen=True)
class BacktestPersistenceAuthority:
    """Frozen database authorities bound to one historical simulation."""

    authority_bundle_id: UUID
    data_snapshot_id: UUID
    source_policy_id: UUID
    cost_policy_id: UUID
    completed_at: datetime
    data_cutoff_at: datetime | None = None

    def __post_init__(self) -> None:
        require_utc(self.completed_at)
        if self.data_cutoff_at is not None:
            require_utc(self.data_cutoff_at)
            if self.data_cutoff_at > self.completed_at:
                raise ValueError("backtest data cutoff cannot follow execution completion")

    @property
    def authority_cutoff_at(self) -> datetime:
        """Frozen market-data clock, independent of later challenger execution."""
        return self.data_cutoff_at or self.completed_at


async def persist_backtest_result(
    session: AsyncSession,
    strategy: CompiledStrategy,
    data: PointInTimeDataset,
    protocol: BacktestProtocol,
    result: BacktestResult,
    authority: BacktestPersistenceAuthority,
) -> BacktestRunRecord:
    """Stage one complete graph after proving its frozen database authorities."""
    _validate_result(strategy, data, protocol, result)
    await _lock_authority_roots(session, data, authority)
    await _advisory_lock(session, f"strategy:{strategy.strategy_version}")
    await _advisory_lock(session, f"backtest:{result.result_hash}")

    existing_run = await session.scalar(
        select(BacktestRunRecord)
        .where(BacktestRunRecord.result_hash == result.result_hash)
        .execution_options(populate_existing=True)
    )
    authority_frozen_at = (
        existing_run.authority_frozen_at
        if existing_run is not None
        else require_utc(await session.scalar(text("SELECT clock_timestamp()")))
    )
    await _validate_database_authority(
        session,
        data,
        protocol,
        result,
        authority,
        authority_frozen_at,
    )
    if existing_run is not None:
        existing_definition = await session.get(
            StrategyDefinitionRecord,
            existing_run.strategy_definition_id,
            populate_existing=True,
        )
        _validate_strategy_definition(existing_definition, strategy, authority.completed_at)
        _validate_existing_run(existing_run, result, protocol, authority)
        return existing_run

    definition = await session.scalar(
        select(StrategyDefinitionRecord)
        .where(StrategyDefinitionRecord.strategy_version == strategy.strategy_version)
        .execution_options(populate_existing=True)
    )
    if definition is None:
        if strategy.strategy_version != "baseline_v1":
            raise ValueError("challenger strategy must be published before backtest persistence")
        document = baseline_v1_document()
        definition = StrategyDefinitionRecord(
            strategy_version=strategy.strategy_version,
            schema_version="strategy_dsl_v1",
            expression_hash=strategy.expression_hash,
            document=document,
            lifecycle="champion",
            published_at=authority.completed_at,
        )
        session.add(definition)
        await session.flush()
    else:
        _validate_strategy_definition(definition, strategy, authority.completed_at)

    result_payload = result.model_dump(mode="json", exclude={"result_hash"})
    run_id = new_uuid7()
    await _run_graph_lock(session, run_id)
    record = BacktestRunRecord(
        id=run_id,
        strategy_definition_id=definition.id,
        authority_bundle_id=authority.authority_bundle_id,
        data_snapshot_id=authority.data_snapshot_id,
        source_policy_id=authority.source_policy_id,
        cost_policy_id=authority.cost_policy_id,
        start_session=protocol.start,
        end_session=protocol.end,
        initial_nav=protocol.initial_nav,
        protocol=protocol.model_dump(mode="json"),
        result_payload=result_payload,
        protocol_hash=result.protocol_hash,
        authority_hash=result.authority_hash,
        data_hash=result.data_hash,
        code_hash=result.code_hash,
        result_hash=result.result_hash,
        random_seed=result.random_seed,
        oos_window_count=result.valid_oos_window_count,
        strategy_data_complete=result.strategy_data_complete,
        benchmark_comparison_complete=result.benchmark_comparison_complete,
        history_requirement_met=result.history_requirement_met,
        promotable=result.promotable,
        historical_agent_overlay=result.historical_agent_overlay,
        graph_sealed=False,
        authority_frozen_at=authority_frozen_at,
        completed_at=authority.completed_at,
    )
    session.add(record)
    await session.flush()
    session.add_all(
        BacktestWindowRecord(
            backtest_run_id=record.id,
            ordinal=window.ordinal,
            train_start=window.train_start,
            train_end=window.train_end,
            validation_start=window.validation_start,
            validation_end=window.validation_end,
            out_of_sample_start=window.out_of_sample_start,
            out_of_sample_end=window.out_of_sample_end,
            session_coverage=window.session_coverage,
            valid=window.valid,
            invalid_reasons=list(window.invalid_reasons),
            metrics=_required_window_metrics(window.metrics),
        )
        for window in result.windows
    )
    session.add_all(
        BacktestNavPointRecord(
            backtest_run_id=record.id, session=point.session, nav=point.nav, cash=point.cash
        )
        for point in result.nav
    )
    session.add_all(
        BacktestFillRecord(
            backtest_run_id=record.id,
            instrument_id=fill.instrument_id,
            signal_session=fill.signal_session,
            fill_session=fill.fill_session,
            side=fill.side,
            quantity=fill.quantity,
            price=fill.price,
            notional=fill.notional,
            cost=fill.cost,
        )
        for fill in result.fills
    )
    session.add_all(
        BacktestOrderOutcomeRecord(
            backtest_run_id=record.id,
            instrument_id=outcome.instrument_id,
            signal_session=outcome.signal_session,
            target_session=outcome.target_session,
            side=outcome.side,
            status=outcome.status,
            reason=outcome.reason,
        )
        for outcome in result.order_outcomes
    )
    session.add_all(_metric_records(record.id, result))
    await session.flush()
    record.graph_sealed = True
    await session.flush()
    return record


def expected_data_snapshot_payload(
    data: PointInTimeDataset, result: BacktestResult
) -> dict[str, object]:
    """Preserve the original result-based payload API and historical hashes."""
    return input_data_snapshot_payload(
        data, authority_hash=result.authority_hash, data_hash=result.data_hash
    )


def input_data_snapshot_payload(
    data: PointInTimeDataset, *, authority_hash: str, data_hash: str
) -> dict[str, object]:
    """Canonical input-only payload; preparation never fabricates a BacktestResult."""
    events: list[CorporateActionEvent | DelistingEvent | UnresolvedDelistingEvent] = [
        *data.corporate_actions,
        *data.delistings,
        *data.unresolved_delistings,
    ]
    action_ids = [
        str(item.authority_id)
        for item in sorted(
            events,
            key=lambda item: (
                item.session,
                item.instrument_id.int,
                item.source,
                item.provider_event_id,
                item.revision,
                item.authority_id.int,
            ),
        )
    ]
    return {
        "kind": "backtest_authority_v1",
        "authority_bundle_id": str(data.authority.authority_bundle_id),
        "authority_hash": authority_hash,
        "data_hash": data_hash,
        "calendar_version": data.authority.exchange_calendar.version,
        "calendar_authority_id": str(data.authority.exchange_calendar.authority_id),
        "calendar_hash": data.authority.exchange_calendar.content_hash,
        "spy_instrument_alias_id": str(data.authority.spy_benchmark.alias_id),
        "price_bar_set_authority_ids": [
            str(item.price_bar_set_authority_id)
            for item in sorted(
                data.authority.price_series,
                key=lambda value: (value.instrument_id.int, value.adjustment),
            )
        ],
        "source_selection_reports": _source_selection_reports(data),
        "universe_snapshot_ids": sorted(
            {str(item.authority_id) for item in data.universe_snapshots}
            | {str(data.authority.equal_weight_benchmark.universe_snapshot_id)}
        ),
        "sector_exposure_ids": [
            str(item.authority_id)
            for item in sorted(
                data.sector_classifications,
                key=lambda value: (
                    value.instrument_id.int,
                    value.effective_session,
                    value.published_at,
                    value.sector,
                    value.weight,
                    value.content_hash,
                    value.authority_id.int,
                ),
            )
        ],
        "corporate_action_ids": action_ids,
        "collection_authority_ids": {
            "universe": str(data.authority.universe_collection.authority_id),
            "sector": str(data.authority.sector_collection.authority_id),
            "corporate_action": str(data.authority.corporate_action_collection.authority_id),
        },
    }


def _source_selection_reports(data: PointInTimeDataset) -> list[dict[str, object]]:
    reports = [item.model_dump(mode="json") for item in data.authority.source_selections]
    for report in reports:
        report["candidate_price_bar_set_authority_ids"] = sorted(
            report["candidate_price_bar_set_authority_ids"]
        )
    return sorted(
        reports,
        key=lambda item: (str(item["instrument_id"]), str(item["adjustment"])),
    )


def _validate_result(
    strategy: CompiledStrategy,
    data: PointInTimeDataset,
    protocol: BacktestProtocol,
    result: BacktestResult,
) -> None:
    if not protocol.walk_forward:
        raise ValueError("persisted backtest evidence requires walk-forward protocol")
    if result.strategy_version != strategy.strategy_version:
        raise ValueError("backtest strategy version does not match the compiled strategy")
    if result.expression_hash != strategy.expression_hash:
        raise ValueError("backtest expression hash does not match the compiled strategy")
    if result.protocol_hash != _hash(protocol.model_dump(mode="json")):
        raise ValueError("backtest protocol hash does not match the protocol")
    if result.authority_hash != _authority_bundle_hash(data):
        raise ValueError("backtest authority hash does not match the dataset authorities")
    if result.authority_bundle_id != data.authority.authority_bundle_id:
        raise ValueError("backtest authority bundle ID does not match the dataset")
    if result.data_hash != _audited_data_hash(data, protocol):
        raise ValueError("backtest data hash does not match the point-in-time dataset")
    if result.code_hash != _engine_code_hash(protocol.code_version):
        raise ValueError("backtest code hash does not match the executed source artifact")
    if result.source_policy_version != protocol.source_policy_version:
        raise ValueError("backtest source policy does not match the protocol")
    if result.random_seed != protocol.random_seed:
        raise ValueError("backtest random seed does not match the protocol")
    if result.result_hash != _hash(result.model_dump(mode="json", exclude={"result_hash"})):
        raise ValueError("backtest result hash is invalid")
    if result.historical_agent_overlay or result.promotable:
        raise ValueError("historical results must remain non-promotable and Agent-free")
    if not result.strategy_data_complete or result.strategy_incomplete_reasons:
        raise ValueError("incomplete strategy data cannot be persisted as evidence")
    if not result.benchmark_comparison_complete:
        raise ValueError("incomplete benchmark comparisons cannot be persisted as evidence")
    if any(outcome.status == "missing_bar" for outcome in result.order_outcomes):
        raise ValueError("incomplete strategy executions cannot be persisted as evidence")
    for outcome in result.order_outcomes:
        if outcome.status != "untradable":
            continue
        if not any(
            event.instrument_id == outcome.instrument_id
            and event.session <= outcome.target_session
            and event.available_at
            <= data.authority.exchange_calendar.decision_times[outcome.target_session]
            for event in data.delistings
        ):
            raise ValueError("untradable execution lacks frozen delisting authority")
    if not result.attribution.reconciled:
        raise ValueError("unreconciled performance attribution cannot be persisted")
    replay = (
        run_backtest(strategy, data, protocol)
        if strategy.strategy_version == "baseline_v1"
        else run_challenger_backtest_v1(
            strategy,
            data,
            protocol,
            experiment_attempt_count=protocol.experiment_attempt_count,
        )
    )
    if result != replay:
        raise ValueError("backtest result does not match deterministic engine replay")


async def _validate_database_authority(
    session: AsyncSession,
    data: PointInTimeDataset,
    protocol: BacktestProtocol,
    result: BacktestResult,
    authority: BacktestPersistenceAuthority,
    authority_frozen_at: datetime,
) -> None:
    expected_ids = (
        data.authority.authority_bundle_id,
        data.authority.data_snapshot_id,
        data.authority.source_policy_id,
        data.authority.cost_policy_id,
    )
    result_ids = (
        result.authority_bundle_id,
        result.data_snapshot_id,
        result.source_policy_id,
        result.cost_policy_id,
    )
    if (
        result_ids != expected_ids
        or result.cost_policy_version != data.authority.cost_policy_version
    ):
        raise ValueError("backtest result authority does not match the frozen dataset")
    await validate_bundle_inputs(session, data, protocol, authority, authority_frozen_at)


async def validate_bundle_inputs(
    session: AsyncSession,
    data: PointInTimeDataset,
    protocol: BacktestProtocol,
    authority: BacktestPersistenceAuthority,
    authority_frozen_at: datetime,
) -> None:
    """Shared full database checks for input preparation and result persistence."""
    completed_at = authority.authority_cutoff_at
    expected_ids = (
        data.authority.authority_bundle_id,
        data.authority.data_snapshot_id,
        data.authority.source_policy_id,
        data.authority.cost_policy_id,
    )
    actual_ids = (
        authority.authority_bundle_id,
        authority.data_snapshot_id,
        authority.source_policy_id,
        authority.cost_policy_id,
    )
    if actual_ids != expected_ids:
        raise ValueError("backtest authority IDs do not match the frozen dataset")

    source_policy = await load_source_policy_v1(session, instrument_tick_size=None)
    cost_policy = await load_backtest_cost_policy_v1(session)
    if (
        source_policy.record_id != authority.source_policy_id
        or source_policy.available_at > completed_at
    ):
        raise ValueError("backtest source policy is not the published authority")
    if (
        cost_policy.id != authority.cost_policy_id
        or cost_policy.version != data.authority.cost_policy_version
        or cost_policy.available_at > completed_at
    ):
        raise ValueError("backtest cost policy is not the published authority")
    expected_costs = {
        "commission_bps": Decimal(str(cost_policy.parameters["commission_bps"])),
        "spread_bps": Decimal(str(cost_policy.parameters["spread_bps"])),
        "slippage_bps": Decimal(str(cost_policy.parameters["slippage_bps"])),
    }
    if protocol.cost_model.model_dump() != expected_costs:
        raise ValueError("backtest cost parameters do not match the published cost policy")

    snapshot = await session.scalar(
        select(DataSnapshot)
        .where(DataSnapshot.id == authority.data_snapshot_id)
        .execution_options(populate_existing=True)
    )
    if (
        snapshot is None
        or snapshot.source != "nexus_backtest_bundle"
        or snapshot.schema_version != "backtest_authority_v1"
        or snapshot.payload
        != input_data_snapshot_payload(
            data,
            authority_hash=_authority_bundle_hash(data),
            data_hash=_audited_data_hash(data, protocol),
        )
        or not snapshot.observed_at <= snapshot.available_at <= snapshot.received_at
        or snapshot.available_at > completed_at
        or snapshot.received_at > completed_at
    ):
        raise ValueError("frozen backtest data snapshot is missing or invalid")

    await _validate_bundle_authority(
        session,
        data,
        protocol,
        _authority_bundle_hash(data),
        completed_at,
        authority_frozen_at,
    )
    await _validate_source_selection_authorities(session, data, completed_at, authority_frozen_at)
    await _validate_price_authorities(session, data, completed_at, authority_frozen_at)
    await _validate_universe_authorities(session, data, completed_at)
    await _validate_sector_authorities(session, data, completed_at)
    await _validate_corporate_action_authorities(session, data, completed_at)


async def _validate_bundle_authority(
    session: AsyncSession,
    data: PointInTimeDataset,
    protocol: BacktestProtocol,
    authority_hash: str,
    completed_at: datetime,
    authority_frozen_at: datetime,
) -> None:
    bundle = await session.get(
        BacktestAuthorityBundleRecord,
        data.authority.authority_bundle_id,
        populate_existing=True,
    )
    calendar = data.authority.exchange_calendar
    calendar_record = await session.get(
        ExchangeCalendarAuthorityRecord, calendar.authority_id, populate_existing=True
    )
    calendar_session_rows = tuple(
        await session.scalars(
            select(ExchangeCalendarSessionRecord)
            .where(ExchangeCalendarSessionRecord.calendar_authority_id == calendar.authority_id)
            .order_by(ExchangeCalendarSessionRecord.session)
        )
    )
    calendar_sessions = tuple(row.session for row in calendar_session_rows)
    calendar_decision_times = {row.session: row.decision_at for row in calendar_session_rows}
    calendar_package_version = package_version("exchange-calendars")
    authoritative_sessions = tuple(
        value.date()
        for value in exchange_calendars.get_calendar(calendar.exchange).sessions_in_range(
            calendar.range_start, calendar.range_end
        )
    )
    official_calendar = exchange_calendars.get_calendar(calendar.exchange)
    authoritative_decision_times = {
        value: official_calendar.session_close(value).to_pydatetime() + timedelta(minutes=30)
        for value in authoritative_sessions
    }
    if (
        calendar_record is None
        or calendar_record.exchange != calendar.exchange
        or calendar_record.version != calendar.version
        or calendar_record.source != calendar.source
        or calendar_record.range_start != calendar.range_start
        or calendar_record.range_end != calendar.range_end
        or calendar_record.query_parameters != dict(calendar.query_parameters)
        or calendar_record.completeness_status != calendar.completeness_status
        or calendar_record.content_hash != calendar.content_hash
        or calendar_record.published_at > completed_at
        or calendar_sessions != calendar.sessions
        or calendar_decision_times != dict(calendar.decision_times)
        or calendar.source != "exchange_calendars"
        or dict(calendar.query_parameters)
        != {"exchange": calendar.exchange, "package_version": calendar_package_version}
        or calendar.version != f"exchange_calendars_{calendar_package_version}_{calendar.exchange}"
        or calendar.sessions != authoritative_sessions
        or dict(calendar.decision_times) != authoritative_decision_times
    ):
        raise ValueError("exchange-calendar collection authority is missing or incomplete")

    collections = {
        "universe": data.authority.universe_collection,
        "sector": data.authority.sector_collection,
        "corporate_action": data.authority.corporate_action_collection,
    }
    sector_query = (
        {"taxonomy": "NEXUS_SECTOR_V1", "synthetic": True, "reviewed": False}
        if data.authority.sector_collection.source == "fixture"
        else {"taxonomy": "NEXUS_SECTOR_V1", "synthetic": False, "reviewed": True}
    )
    collection_queries: dict[str, dict[str, object]] = {
        "universe": {"key": "us_liquid"},
        "sector": sector_query,
        "corporate_action": {
            "provider_mapping_version": "corporate_action_mapping_v1",
            "status": "complete",
        },
    }
    member_columns = {
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
    }
    for kind, expected in collections.items():
        if (
            expected.range_start != protocol.start
            or expected.range_end != protocol.end
            or dict(expected.query_parameters) != collection_queries[kind]
        ):
            raise ValueError(f"{kind} collection query contract is invalid")
        record = await session.get(
            BacktestAuthorityCollectionRecord, expected.authority_id, populate_existing=True
        )
        collection_id_column, member_column = member_columns[kind]
        member_ids = set(
            await session.scalars(
                select(member_column).where(collection_id_column == expected.authority_id)
            )
        )
        if (
            record is None
            or record.kind != expected.kind
            or record.source != expected.source
            or record.range_start != expected.range_start
            or record.range_end != expected.range_end
            or record.query_parameters != dict(expected.query_parameters)
            or record.completeness_status != expected.completeness_status
            or record.content_hash != expected.content_hash
            or record.completed_at > completed_at
            or member_ids != set(expected.member_ids)
        ):
            raise ValueError(f"{kind} collection authority is missing or incomplete")

    if (
        bundle is None
        or bundle.data_snapshot_id != data.authority.data_snapshot_id
        or bundle.source_policy_id != data.authority.source_policy_id
        or bundle.cost_policy_id != data.authority.cost_policy_id
        or bundle.calendar_authority_id != calendar.authority_id
        or bundle.universe_collection_id != collections["universe"].authority_id
        or bundle.sector_collection_id != collections["sector"].authority_id
        or bundle.corporate_action_collection_id != collections["corporate_action"].authority_id
        or bundle.spy_instrument_alias_id != data.authority.spy_benchmark.alias_id
        or bundle.range_start != protocol.start
        or bundle.range_end != protocol.end
        or bundle.completeness_status != "complete"
        or bundle.content_hash != authority_hash
        or bundle.source_selection_reports != _source_selection_reports(data)
        or bundle.completed_at != completed_at
    ):
        raise ValueError("backtest authority bundle is missing or invalid")

    spy = data.authority.spy_benchmark
    end_cutoff = data.authority.exchange_calendar.decision_times[protocol.end]
    overlapping_spy_aliases = tuple(
        await session.scalars(
            select(InstrumentAlias)
            .join(
                BacktestAuthorityPublicationMarkRecord,
                (BacktestAuthorityPublicationMarkRecord.domain == "instrument_alias")
                & (BacktestAuthorityPublicationMarkRecord.record_id == InstrumentAlias.id),
            )
            .where(
                InstrumentAlias.symbol == "SPY",
                InstrumentAlias.alias_type == "ticker",
                InstrumentAlias.valid_from <= protocol.end,
                (InstrumentAlias.valid_to.is_(None)) | (InstrumentAlias.valid_to >= protocol.start),
                InstrumentAlias.created_at <= end_cutoff,
                BacktestAuthorityPublicationMarkRecord.created_at <= authority_frozen_at,
            )
        )
    )
    if (
        len(overlapping_spy_aliases) != 1
        or overlapping_spy_aliases[0].id != spy.alias_id
        or overlapping_spy_aliases[0].instrument_id != data.spy_instrument_id
        or overlapping_spy_aliases[0].instrument_id != spy.instrument_id
        or overlapping_spy_aliases[0].symbol != spy.symbol
        or overlapping_spy_aliases[0].alias_type != spy.alias_type
        or overlapping_spy_aliases[0].valid_from != spy.valid_from
        or overlapping_spy_aliases[0].valid_to != spy.valid_to
        or overlapping_spy_aliases[0].created_at != spy.created_at
        or spy.valid_from > protocol.start
        or (spy.valid_to is not None and spy.valid_to < protocol.end)
    ):
        raise ValueError("SPY benchmark alias authority is missing or ambiguous")

    price_member_ids = set(
        await session.scalars(
            select(BacktestPriceAuthorityMemberRecord.price_bar_set_authority_id).where(
                BacktestPriceAuthorityMemberRecord.bundle_id == bundle.id
            )
        )
    )
    instrument_member_ids = set(
        await session.scalars(
            select(BacktestInstrumentAuthorityMemberRecord.instrument_id).where(
                BacktestInstrumentAuthorityMemberRecord.bundle_id == bundle.id
            )
        )
    )
    expected_price_member_ids = {
        authority_id
        for report in data.authority.source_selections
        for authority_id in report.candidate_price_bar_set_authority_ids
    }
    if price_member_ids != expected_price_member_ids or instrument_member_ids != set(
        data.asset_types
    ):
        raise ValueError("backtest bundle junctions do not exactly cover prices and instruments")

    await _validate_collection_query_completeness(
        session, data, protocol, completed_at, authority_frozen_at
    )


async def _validate_collection_query_completeness(
    session: AsyncSession,
    data: PointInTimeDataset,
    protocol: BacktestProtocol,
    completed_at: datetime,
    authority_frozen_at: datetime,
) -> None:
    universe_collection = data.authority.universe_collection
    universe_rows = tuple(
        await session.scalars(
            select(UniverseSnapshotRecord)
            .join(
                BacktestAuthorityPublicationMarkRecord,
                (BacktestAuthorityPublicationMarkRecord.domain == "universe")
                & (BacktestAuthorityPublicationMarkRecord.record_id == UniverseSnapshotRecord.id),
            )
            .where(
                UniverseSnapshotRecord.key == "us_liquid",
                UniverseSnapshotRecord.source == universe_collection.source,
                UniverseSnapshotRecord.effective_from <= protocol.end,
                UniverseSnapshotRecord.published_at <= completed_at,
                UniverseSnapshotRecord.created_at <= completed_at,
                BacktestAuthorityPublicationMarkRecord.created_at <= authority_frozen_at,
            )
        )
    )
    expected_universe_ids: set[UUID] = set()
    for market_session in data.authority.exchange_calendar.sessions:
        if not protocol.start <= market_session <= protocol.end:
            continue
        cutoff = data.authority.exchange_calendar.decision_times[market_session]
        universe_eligible = [
            row
            for row in universe_rows
            if row.effective_from <= market_session
            and row.published_at is not None
            and row.published_at <= cutoff
        ]
        if not universe_eligible:
            continue
        universe_latest_clock = max(
            (row.effective_from, row.published_at) for row in universe_eligible
        )
        universe_latest = [
            row
            for row in universe_eligible
            if (row.effective_from, row.published_at) == universe_latest_clock
        ]
        if len(universe_latest) != 1:
            raise ValueError("us_liquid point-in-time frontier is ambiguous")
        expected_universe_ids.add(universe_latest[0].id)
    if expected_universe_ids != set(universe_collection.member_ids):
        raise ValueError("us_liquid collection omits a database-authoritative universe version")

    sector_collection = data.authority.sector_collection
    sector_instruments = {
        instrument_id
        for instrument_id, asset_type in data.asset_types.items()
        if asset_type != "non_equity_etf"
    }
    fixture_sector_authority = sector_collection.source == "fixture"
    sector_rows = tuple(
        await session.scalars(
            select(SectorExposure)
            .join(
                BacktestAuthorityPublicationMarkRecord,
                (BacktestAuthorityPublicationMarkRecord.domain == "sector")
                & (BacktestAuthorityPublicationMarkRecord.record_id == SectorExposure.id),
            )
            .where(
                SectorExposure.instrument_id.in_(sector_instruments),
                SectorExposure.synthetic.is_(fixture_sector_authority),
                SectorExposure.reviewed.is_(not fixture_sector_authority),
                SectorExposure.as_of <= protocol.end,
                SectorExposure.created_at <= completed_at,
                BacktestAuthorityPublicationMarkRecord.created_at <= authority_frozen_at,
            )
        )
    )
    expected_sector_ids: set[UUID] = set()
    for market_session in data.authority.exchange_calendar.sessions:
        if not protocol.start <= market_session <= protocol.end:
            continue
        cutoff = data.authority.exchange_calendar.decision_times[market_session]
        for instrument_id in sector_instruments:
            sector_eligible = [
                row
                for row in sector_rows
                if row.instrument_id == instrument_id
                and row.as_of <= market_session
                and row.created_at <= cutoff
            ]
            if not sector_eligible:
                continue
            latest_as_of = max(row.as_of for row in sector_eligible)
            effective = [row for row in sector_eligible if row.as_of == latest_as_of]
            latest_published = max(row.created_at for row in effective)
            expected_sector_ids.update(
                row.id for row in effective if row.created_at == latest_published
            )
    if expected_sector_ids != set(sector_collection.member_ids):
        raise ValueError("sector collection omits a database-authoritative vintage")

    action_collection = data.authority.corporate_action_collection
    action_instruments = set(data.asset_types)
    action_rows = tuple(
        (
            await session.execute(
                select(
                    CorporateActionRecord,
                    ResearchEvidenceRef.available_at,
                    ResearchEvidenceRef.created_at,
                )
                .join(
                    ResearchEvidenceRef,
                    ResearchEvidenceRef.id == CorporateActionRecord.evidence_ref_id,
                )
                .join(
                    BacktestAuthorityPublicationMarkRecord,
                    (BacktestAuthorityPublicationMarkRecord.domain == "corporate_action")
                    & (
                        BacktestAuthorityPublicationMarkRecord.record_id == CorporateActionRecord.id
                    ),
                )
                .where(
                    CorporateActionRecord.instrument_id.in_(action_instruments),
                    CorporateActionRecord.source == action_collection.source,
                    CorporateActionRecord.created_at <= completed_at,
                    ResearchEvidenceRef.available_at <= completed_at,
                    ResearchEvidenceRef.created_at <= completed_at,
                    BacktestAuthorityPublicationMarkRecord.created_at <= authority_frozen_at,
                )
            )
        )
        .tuples()
        .all()
    )
    action_versions: dict[
        tuple[str, str], list[tuple[CorporateActionRecord, datetime, datetime]]
    ] = {}
    for record, evidence_available_at, evidence_created_at in action_rows:
        action_versions.setdefault((record.source, record.provider_event_id), []).append(
            (record, evidence_available_at, evidence_created_at)
        )
    authoritative_actions: list[CorporateActionRecord] = []
    for versions in action_versions.values():
        action_latest_clock = max(
            (evidence_available_at, evidence_created_at, record.created_at)
            for record, evidence_available_at, evidence_created_at in versions
        )
        action_latest = [
            record
            for record, evidence_available_at, evidence_created_at in versions
            if (evidence_available_at, evidence_created_at, record.created_at)
            == action_latest_clock
        ]
        if len(action_latest) != 1:
            raise ValueError("corporate-action revisions are ambiguous at the cutoff")
        authoritative_actions.append(action_latest[0])
    supported_action_types = {
        "cash_dividend",
        "stock_split",
        "reverse_split",
        "delisting",
        "bankruptcy",
        "merger_cash",
    }

    def mapped_into_protocol(provider_date: object) -> bool:
        if not isinstance(provider_date, date):
            return False
        mapped_session = _mapped_session_on_or_after(
            data.authority.exchange_calendar.sessions, provider_date
        )
        return mapped_session is not None and protocol.start <= mapped_session <= protocol.end

    if any(
        row.status == "complete"
        and (
            (
                row.action_type not in supported_action_types
                and (mapped_into_protocol(row.ex_date) or mapped_into_protocol(row.process_date))
            )
            or (
                row.action_type in {"cash_dividend", "stock_split", "reverse_split"}
                and row.ex_date is None
                and mapped_into_protocol(row.process_date)
            )
        )
        for row in authoritative_actions
    ):
        raise ValueError("unsupported complete corporate action prevents exact mapping")
    expected_action_ids = {
        row.id
        for row in authoritative_actions
        if row.status == "complete"
        and (
            (
                row.action_type in {"cash_dividend", "stock_split", "reverse_split"}
                and row.ex_date is not None
                and mapped_into_protocol(row.ex_date)
            )
            or (
                row.action_type in {"delisting", "bankruptcy", "merger_cash"}
                and mapped_into_protocol(row.process_date)
            )
        )
    }
    if expected_action_ids != set(action_collection.member_ids):
        raise ValueError("corporate-action collection omits a database-authoritative event")


async def _validate_source_selection_authorities(
    session: AsyncSession,
    data: PointInTimeDataset,
    completed_at: datetime,
    authority_frozen_at: datetime,
) -> None:
    for report in data.authority.source_selections:
        replay = await select_source_authority(
            session,
            instrument_id=report.instrument_id,
            adjustment=report.adjustment,
            calendar=data.authority.exchange_calendar,
            source_policy_id=data.authority.source_policy_id,
            completed_at=completed_at,
            authority_frozen_at=authority_frozen_at,
            expected_candidate_ids=set(report.candidate_price_bar_set_authority_ids),
        )
        normalized = report.model_copy(
            update={
                "candidate_price_bar_set_authority_ids": tuple(
                    sorted(
                        report.candidate_price_bar_set_authority_ids, key=lambda value: value.int
                    )
                )
            }
        )
        if replay != normalized:
            raise ValueError("source_policy_v1 selection report is not replayable")


async def select_source_authority(
    session: AsyncSession,
    *,
    instrument_id: UUID,
    adjustment: Literal["raw", "all"],
    calendar: ExchangeCalendarAuthority,
    source_policy_id: UUID,
    completed_at: datetime,
    authority_frozen_at: datetime,
    expected_candidate_ids: set[UUID] | None = None,
) -> PriceSourceSelectionAuthority:
    """One exact candidate query and historical-close policy path for builder and writer."""
    request_sessions = set(calendar.sessions)
    instrument = await session.get(Instrument, instrument_id, populate_existing=True)
    if instrument is None or instrument.tick_size is None:
        raise ValueError("source_policy_v1 instrument missing")
    rows = tuple(
        await session.execute(
            select(PriceBarSetAuthority, PriceSeriesManifest)
            .join(
                PriceSeriesManifest,
                PriceSeriesManifest.id == PriceBarSetAuthority.price_series_manifest_id,
            )
            .join(
                BacktestAuthorityPublicationMarkRecord,
                (BacktestAuthorityPublicationMarkRecord.domain == "price")
                & (BacktestAuthorityPublicationMarkRecord.record_id == PriceBarSetAuthority.id),
            )
            .where(
                PriceBarSetAuthority.source_policy_id == source_policy_id,
                PriceBarSetAuthority.available_at <= completed_at,
                PriceBarSetAuthority.created_at <= completed_at,
                PriceSeriesManifest.created_at <= completed_at,
                PriceSeriesManifest.instrument == str(instrument_id),
                PriceSeriesManifest.adjustment == adjustment,
                PriceSeriesManifest.interval == "1Day",
                PriceSeriesManifest.id.in_(
                    select(PriceBarRecord.price_series_manifest_id).where(
                        PriceBarRecord.session >= calendar.range_start,
                        PriceBarRecord.session <= calendar.range_end,
                    )
                ),
                BacktestAuthorityPublicationMarkRecord.created_at <= authority_frozen_at,
            )
        )
    )
    if expected_candidate_ids is not None and {row[0].id for row in rows} != expected_candidate_ids:
        raise ValueError("source_policy_v1 candidate set is incomplete")
    candidates: list[PriceSeries] = []
    series_by_authority_id: dict[UUID, PriceSeries] = {}
    for authority_record, manifest in rows:
        if authority_record.source != manifest.source:
            raise ValueError("source_policy_v1 candidate provenance is inconsistent")
        artifact = await session.get(
            DataArtifact, manifest.data_artifact_id, populate_existing=True
        )
        database_bars = tuple(
            await session.scalars(
                select(PriceBarRecord)
                .where(PriceBarRecord.price_series_manifest_id == manifest.id)
                .order_by(
                    PriceBarRecord.session,
                    PriceBarRecord.observed_at,
                    PriceBarRecord.revision,
                )
            )
        )
        request_bar_records = tuple(bar for bar in database_bars if bar.session in request_sessions)
        if any(
            bar.available_at < calendar.decision_times[bar.session] - timedelta(minutes=30)
            for bar in request_bar_records
        ):
            raise ValueError("daily price candidate is available before official close")
        base_complete = (
            artifact is not None
            and artifact.created_at <= completed_at
            and authority_record.bar_count == manifest.row_count == len(database_bars)
            and not any(bar.quality_flags for bar in database_bars)
            and all(bar.created_at <= completed_at for bar in database_bars)
        )
        mode = cast(
            Literal["fixture", "delayed", "read_only"],
            "fixture"
            if manifest.delivery_mode == "fixture"
            else "delayed"
            if manifest.delivery_mode == "delayed"
            else "read_only",
        )
        provider_bars = tuple(
            PriceBar(
                instrument_id=InstrumentId(bar.instrument_id),
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
            for bar in request_bar_records
        )
        provisional = PriceSeries(
            source=manifest.source,
            feed=manifest.feed,
            adjustment=manifest.adjustment,
            interval=manifest.interval,
            mode=mode,
            delivery_mode=manifest.delivery_mode,
            quality="PARTIAL",
            bars=provider_bars,
        )
        visible_at_historical_cutoffs = tuple(
            bar
            for bar in provisional.bars
            if bar.available_at <= calendar.decision_times[bar.session]
        )
        historical_request_bars = canonical_price_bars(provisional, visible_at_historical_cutoffs)
        canonical_request_bars = canonical_price_bars(
            provisional,
            tuple(bar for bar in provisional.bars if bar.available_at <= completed_at),
        )
        complete = (
            base_complete and {bar.session for bar in historical_request_bars} == request_sessions
        )
        series = provisional.model_copy(
            update={
                "bars": canonical_request_bars,
                "quality": "PASS" if complete else "PARTIAL",
            }
        )
        candidates.append(series)
        series_by_authority_id[authority_record.id] = series
    policy = await load_source_policy_v1(session, instrument_tick_size=instrument.tick_size)
    selection = apply_source_policy_v1(
        candidates,
        policy,
        completed_at,
        None,
    )

    matching = {
        authority_id
        for authority_id, series in series_by_authority_id.items()
        if series == selection.selected
    }
    if len(matching) != 1 or selection.quality != "PASS" or selection.reasons:
        raise ValueError("source_policy_v1 selection is unavailable or ambiguous")
    return PriceSourceSelectionAuthority(
        instrument_id=instrument_id,
        adjustment=adjustment,
        request_start_session=calendar.range_start,
        request_end_session=calendar.range_end,
        decision_time=completed_at,
        tick_size=instrument.tick_size,
        candidate_price_bar_set_authority_ids=tuple(
            sorted(series_by_authority_id, key=lambda v: v.int)
        ),
        selected_price_bar_set_authority_id=next(iter(matching)),
        quality=selection.quality,
        reasons=selection.reasons,
    )


async def _validate_price_authorities(
    session: AsyncSession,
    data: PointInTimeDataset,
    completed_at: datetime,
    authority_frozen_at: datetime,
) -> None:
    for expected in data.authority.price_series:
        if not price_manifest_admissible_under_source_policy_v1(
            source=expected.source,
            feed=expected.feed,
            delivery_mode=expected.delivery_mode,
        ):
            raise ValueError("price-series authority is not admitted by source_policy_v1")
        instrument = await session.get(Instrument, expected.instrument_id, populate_existing=True)
        record = await session.get(
            PriceBarSetAuthority,
            expected.price_bar_set_authority_id,
            populate_existing=True,
        )
        manifest = await session.get(
            PriceSeriesManifest,
            expected.price_series_manifest_id,
            populate_existing=True,
        )
        artifact = (
            None
            if manifest is None
            else await session.get(DataArtifact, manifest.data_artifact_id, populate_existing=True)
        )
        source_bars = data.bars if expected.adjustment == "raw" else data.adjusted_bars
        expected_bars = tuple(
            sorted(
                (bar for bar in source_bars if bar.instrument_id == expected.instrument_id),
                key=lambda bar: (bar.session, bar.observed_at, bar.revision, bar.instrument_id.int),
            )
        )
        if len(expected_bars) != expected.row_count:
            raise ValueError("price-series row count does not match the frozen dataset")
        if any(
            bar.session not in data.authority.exchange_calendar.decision_times
            or bar.available_at
            < data.authority.exchange_calendar.decision_times[bar.session] - timedelta(minutes=30)
            for bar in expected_bars
        ):
            raise ValueError("daily price authority is available before official close")
        if (
            record is None
            or instrument is None
            or _BACKTEST_ASSET_TYPE_BY_MASTER.get(instrument.asset_type)
            != data.asset_types[expected.instrument_id]
            or instrument.exchange not in US_EQUITY_EXCHANGES
            or instrument.currency != "USD"
            or instrument.risk_bucket != data.risk_buckets[expected.instrument_id]
            or manifest is None
            or artifact is None
            or record.price_series_manifest_id != manifest.id
            or record.source_policy_id != data.authority.source_policy_id
            or record.source != expected.source
            or record.content_hash != expected.content_hash
            or record.bar_count != expected.row_count
            or record.first_session != expected_bars[0].session
            or record.last_session != expected_bars[-1].session
            or record.available_at > completed_at
            or manifest.id != expected.price_series_manifest_id
            or manifest.instrument != str(expected.instrument_id)
            or manifest.source != expected.source
            or manifest.feed != expected.feed
            or manifest.delivery_mode != expected.delivery_mode
            or manifest.adjustment != expected.adjustment
            or manifest.interval != expected.interval
            or manifest.start_at != expected.start_at
            or manifest.end_at != expected.end_at
            or manifest.row_count != expected.row_count
            or artifact.sha256 != expected.manifest_hash
            or artifact.source != expected.source
            or not artifact.observed_at <= artifact.available_at <= artifact.received_at
            or artifact.available_at > completed_at
            or artifact.received_at > completed_at
        ):
            raise ValueError("price-series authority does not match the frozen dataset")
        database_bars = tuple(
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
        actual_values = tuple(
            (
                bar.instrument_id,
                bar.session,
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.volume,
                bar.observed_at,
                bar.available_at,
                bar.received_at,
                bar.revision,
                tuple(bar.quality_flags),
            )
            for bar in database_bars
        )
        expected_values = tuple(
            (
                bar.instrument_id,
                bar.session,
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.volume,
                bar.observed_at,
                bar.available_at,
                bar.received_at,
                bar.revision,
                bar.quality_flags,
            )
            for bar in expected_bars
        )
        if actual_values != expected_values:
            raise ValueError("price bars do not match their database-finalized authority")


async def _validate_universe_authorities(
    session: AsyncSession, data: PointInTimeDataset, completed_at: datetime
) -> None:
    expected = {item.authority_id: item for item in data.universe_snapshots}
    benchmark = data.authority.equal_weight_benchmark
    if benchmark.universe_snapshot_id not in expected:
        benchmark_record = await session.get(
            UniverseSnapshotRecord, benchmark.universe_snapshot_id, populate_existing=True
        )
        if (
            benchmark_record is None
            or benchmark_record.key != benchmark.key
            or benchmark_record.version != benchmark.universe_version
            or benchmark_record.content_hash != benchmark.content_hash
            or benchmark_record.source != benchmark.source
            or benchmark_record.effective_from != benchmark.effective_session
            or benchmark_record.published_at != benchmark.published_at
            or benchmark_record.published_at > completed_at
        ):
            raise ValueError("equal-weight universe authority is missing or invalid")
        member_rows = tuple(
            await session.execute(
                select(UniverseMember.instrument_id, UniverseMember.inclusion_reason).where(
                    UniverseMember.universe_snapshot_id == benchmark_record.id
                )
            )
        )
        if {row.instrument_id for row in member_rows} != set(benchmark.instrument_ids):
            raise ValueError("equal-weight universe members do not match the frozen authority")
    for authority_id, item in expected.items():
        record = await session.get(UniverseSnapshotRecord, authority_id, populate_existing=True)
        if (
            record is None
            or record.key != item.key
            or record.version != item.version
            or record.source != item.source
            or record.content_hash != item.content_hash
            or record.effective_from != item.effective_session
            or record.published_at != item.published_at
            or record.published_at > completed_at
        ):
            raise ValueError("strategy universe authority does not match the frozen dataset")
        member_rows = tuple(
            await session.execute(
                select(UniverseMember.instrument_id, UniverseMember.inclusion_reason).where(
                    UniverseMember.universe_snapshot_id == authority_id
                )
            )
        )
        members = {row.instrument_id for row in member_rows}
        if members != set(item.members):
            raise ValueError("strategy universe members do not match the frozen authority")
        if authority_id == benchmark.universe_snapshot_id and (
            item.version != benchmark.universe_version
            or item.content_hash != benchmark.content_hash
            or item.key != benchmark.key
            or item.source != benchmark.source
            or item.effective_session != benchmark.effective_session
            or item.published_at != benchmark.published_at
            or set(item.members) != set(benchmark.instrument_ids)
        ):
            raise ValueError("equal-weight benchmark is not bound to its universe version")


async def _validate_sector_authorities(
    session: AsyncSession, data: PointInTimeDataset, completed_at: datetime
) -> None:
    for item in data.sector_classifications:
        record = await session.get(SectorExposure, item.authority_id, populate_existing=True)
        if (
            record is None
            or record.instrument_id != item.instrument_id
            or record.sector_code != item.sector
            or record.weight != item.weight
            or record.as_of != item.effective_session
            or record.source_url != item.source_uri
            or record.content_hash != item.content_hash
            or record.synthetic != item.synthetic
            or record.reviewed != item.reviewed
            or record.created_at != item.published_at
            or record.created_at > completed_at
        ):
            raise ValueError("sector authority does not match the point-in-time classification")


async def _validate_corporate_action_authorities(
    session: AsyncSession, data: PointInTimeDataset, completed_at: datetime
) -> None:
    events: list[CorporateActionEvent | DelistingEvent | UnresolvedDelistingEvent] = [
        *data.corporate_actions,
        *data.delistings,
        *data.unresolved_delistings,
    ]
    for item in events:
        record = await session.get(CorporateActionRecord, item.authority_id, populate_existing=True)
        evidence = (
            None
            if record is None
            else await session.get(
                ResearchEvidenceRef, record.evidence_ref_id, populate_existing=True
            )
        )
        expected_type: str
        expected_value: Decimal | None
        if isinstance(item, CorporateActionEvent):
            expected_type = item.provider_action_type
            expected_value = item.value
        elif isinstance(item, DelistingEvent):
            expected_type = item.provider_action_type
            expected_value = item.recovery_price
        else:
            expected_type = item.provider_action_type
            expected_value = None
        if (
            record is None
            or evidence is None
            or record.instrument_id != item.instrument_id
            or record.source != item.source
            or record.provider_event_id != item.provider_event_id
            or record.status != item.status
            or record.action_type != expected_type
            or (
                record.ex_date != item.provider_ex_date
                if isinstance(item, CorporateActionEvent)
                else record.process_date != item.provider_process_date
            )
            or (
                isinstance(item, CorporateActionEvent)
                and item.action_type == "dividend"
                and (
                    record.record_date != item.provider_record_date
                    or record.payable_date != item.provider_payable_date
                )
            )
            or record.rate != expected_value
            or record.revision != item.revision
            or evidence.content_hash != item.evidence_hash
            or evidence.available_at != item.available_at
            or item.available_at > completed_at
        ):
            raise ValueError("corporate-action authority does not match the frozen event")


async def _advisory_lock(session: AsyncSession, identity: str) -> None:
    digest = hashlib.sha256(identity.encode()).digest()
    lock_key = int.from_bytes(digest[:8], byteorder="big", signed=True)
    await session.execute(text("SELECT pg_advisory_xact_lock(:lock_key)"), {"lock_key": lock_key})


async def _run_graph_lock(session: AsyncSession, run_id: UUID) -> None:
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(CAST(:run_id AS text), 1602))"),
        {"run_id": str(run_id)},
    )


async def _lock_authority_roots(
    session: AsyncSession,
    data: PointInTimeDataset,
    authority: BacktestPersistenceAuthority,
) -> None:
    """Serialize exact-set validation with every membership append trigger."""
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended('backtest-authority-global', 1602))")
    )
    await session.execute(
        select(ExchangeCalendarAuthorityRecord.id)
        .where(ExchangeCalendarAuthorityRecord.id == data.authority.exchange_calendar.authority_id)
        .with_for_update()
    )
    await session.execute(
        select(BacktestAuthorityCollectionRecord.id)
        .where(
            BacktestAuthorityCollectionRecord.id.in_(
                sorted(
                    (
                        data.authority.universe_collection.authority_id,
                        data.authority.sector_collection.authority_id,
                        data.authority.corporate_action_collection.authority_id,
                    ),
                    key=lambda value: value.int,
                )
            )
        )
        .order_by(BacktestAuthorityCollectionRecord.id)
        .with_for_update()
    )
    await session.execute(
        select(BacktestAuthorityBundleRecord.id)
        .where(BacktestAuthorityBundleRecord.id == authority.authority_bundle_id)
        .with_for_update()
    )


def _validate_existing_run(
    record: BacktestRunRecord,
    result: BacktestResult,
    protocol: BacktestProtocol,
    authority: BacktestPersistenceAuthority,
) -> None:
    expected = (
        authority.authority_bundle_id,
        authority.data_snapshot_id,
        authority.source_policy_id,
        authority.cost_policy_id,
        result.protocol_hash,
        result.authority_hash,
        result.data_hash,
        result.code_hash,
        result.strategy_data_complete,
        protocol.start,
        protocol.end,
        protocol.initial_nav,
        protocol.model_dump(mode="json"),
        result.random_seed,
        result.valid_oos_window_count,
        result.benchmark_comparison_complete,
        result.history_requirement_met,
        result.promotable,
        result.historical_agent_overlay,
        True,
        authority.completed_at,
        result.model_dump(mode="json", exclude={"result_hash"}),
    )
    actual = (
        record.authority_bundle_id,
        record.data_snapshot_id,
        record.source_policy_id,
        record.cost_policy_id,
        record.protocol_hash,
        record.authority_hash,
        record.data_hash,
        record.code_hash,
        record.strategy_data_complete,
        record.start_session,
        record.end_session,
        record.initial_nav,
        record.protocol,
        record.random_seed,
        record.oos_window_count,
        record.benchmark_comparison_complete,
        record.history_requirement_met,
        record.promotable,
        record.historical_agent_overlay,
        record.graph_sealed,
        record.completed_at,
        record.result_payload,
    )
    if actual != expected:
        raise ValueError("result hash is already bound to different backtest authority")


def _validate_strategy_definition(
    definition: StrategyDefinitionRecord | None,
    strategy: CompiledStrategy,
    completed_at: datetime,
) -> None:
    if definition is None:
        raise ValueError("published strategy definition does not match the compiled strategy")
    registry = (
        baseline_feature_registry()
        if strategy.strategy_version == "baseline_v1"
        else evolution_feature_registry_v1()
    )
    try:
        compiled = compile_strategy(definition.document, registry)
    except ValueError as error:
        raise ValueError(
            "published strategy definition does not match the compiled strategy"
        ) from error
    expected_lifecycle = "champion" if strategy.strategy_version == "baseline_v1" else "challenger"
    if (
        definition.strategy_version != strategy.strategy_version
        or definition.schema_version != "strategy_dsl_v1"
        or definition.expression_hash != strategy.expression_hash
        or compiled != strategy
        or definition.lifecycle != expected_lifecycle
        or definition.published_at > completed_at
    ):
        raise ValueError("published strategy definition does not match the compiled strategy")


def _required_window_metrics(metrics: BacktestMetrics | None) -> dict[str, object]:
    if metrics is None:
        raise ValueError("walk-forward windows require metrics before persistence")
    return metrics.model_dump(mode="json")


def _metric_records(run_id: UUID, result: BacktestResult) -> list[BacktestMetricRecord]:
    records = _metrics_for(run_id, "strategy", result.metrics)
    for comparison in result.comparisons:
        records.extend(_metrics_for(run_id, comparison.comparison_key, comparison.metrics))
    terminal_navs = (
        ("cost_half", result.cost_sensitivity.half_cost_terminal_nav),
        ("cost_double", result.cost_sensitivity.double_cost_terminal_nav),
        ("parameter_adv40", result.parameter_sensitivity.adv20_40m_terminal_nav),
        ("parameter_adv60", result.parameter_sensitivity.adv20_60m_terminal_nav),
        ("execution_close", result.execution_sensitivity.next_close_terminal_nav),
    )
    records.extend(
        BacktestMetricRecord(
            backtest_run_id=run_id,
            comparison_key=comparison_key,
            metric_name="terminal_nav",
            value=value,
            unit="USD",
        )
        for comparison_key, value in terminal_navs
    )
    return records


def _metrics_for(
    run_id: UUID, comparison_key: str, metrics: BacktestMetrics
) -> list[BacktestMetricRecord]:
    return [
        BacktestMetricRecord(
            backtest_run_id=run_id,
            comparison_key=comparison_key,
            metric_name=name,
            value=value,
            unit=_metric_unit(name),
        )
        for name, value in metrics.model_dump().items()
        if isinstance(value, Decimal)
    ]


def _metric_unit(name: str) -> str:
    if name == "total_cost":
        return "USD"
    if name == "deflated_sharpe_confidence":
        return "probability"
    return "ratio"
