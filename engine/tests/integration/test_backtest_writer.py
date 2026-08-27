"""Atomic persistence for immutable, reproducible backtest evidence."""

import asyncio
import hashlib
import json
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from importlib.metadata import version as package_version
from uuid import UUID, uuid4

import exchange_calendars
import pytest
from nexus.backtest.cost_policy import BACKTEST_COST_POLICY_ID
from nexus.backtest.costs import CostModel
from nexus.backtest.engine import (
    AuthorityCollection,
    BacktestBar,
    BacktestDataAuthority,
    BacktestProtocol,
    BacktestResult,
    BenchmarkInstrumentAuthority,
    CorporateActionEvent,
    EqualWeightBenchmarkAuthority,
    ExchangeCalendarAuthority,
    PointInTimeDataset,
    PriceSeriesAuthority,
    PriceSourceSelectionAuthority,
    SectorClassification,
    UniverseSnapshot,
    _hash,
    authority_collection_content_hash,
    exchange_calendar_content_hash,
    run_backtest,
    run_challenger_backtest_v1,
)
from nexus.backtest.persistence import (
    BacktestPersistenceAuthority,
    expected_data_snapshot_payload,
    persist_backtest_result,
)
from nexus.evolution.candidate import apply_candidate_delta, generate_candidate_deltas
from nexus.evolution.models import StrategyExperimentProposalV1
from nexus.evolution.policies import (
    candidate_budget_policy_v1,
    evolution_search_space_v1,
)
from nexus.market.price_bar_authority import finalize_price_bar_set
from nexus.persistence.models.backtest import (
    BacktestFillRecord,
    BacktestMetricRecord,
    BacktestNavPointRecord,
    BacktestOrderOutcomeRecord,
    BacktestRunRecord,
    StrategyDefinitionRecord,
)
from nexus.persistence.models.data import PriceSeriesManifest
from nexus.persistence.session import session_factory
from nexus.quality.source_policy import SOURCE_POLICY_ID
from nexus.settings import EngineSettings
from nexus.strategy.dsl import (
    CompiledStrategy,
    baseline_feature_registry,
    baseline_v1_document,
    compile_strategy,
    evolution_feature_registry_v1,
)
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

INSTRUMENT_ID = UUID("0198d500-1000-7000-8000-000000001601")
DATA_SNAPSHOT_ID = UUID("0198d500-1000-7000-8000-000000001602")
UNIVERSE_ID = UUID("0198d500-1000-7000-8000-000000001605")
UNIVERSE_MEMBER_ID = UUID("0198d500-1000-7000-8000-000000001606")
SECTOR_ID = UUID("0198d500-1000-7000-8000-000000001607")
RAW_ARTIFACT_ID = UUID("0198d500-1000-7000-8000-000000001608")
ADJUSTED_ARTIFACT_ID = UUID("0198d500-1000-7000-8000-000000001609")
RAW_MANIFEST_ID = UUID("0198d500-1000-7000-8000-00000000160a")
ADJUSTED_MANIFEST_ID = UUID("0198d500-1000-7000-8000-00000000160b")
ACTION_EVIDENCE_ID = UUID("0198d500-1000-7000-8000-00000000160c")
ACTION_ID = UUID("0198d500-1000-7000-8000-00000000160d")
AUTHORITY_BUNDLE_ID = UUID("0198d500-1000-7000-8000-00000000160e")
CALENDAR_AUTHORITY_ID = UUID("0198d500-1000-7000-8000-00000000160f")
UNIVERSE_COLLECTION_ID = UUID("0198d500-1000-7000-8000-000000001610")
SECTOR_COLLECTION_ID = UUID("0198d500-1000-7000-8000-000000001611")
ACTION_COLLECTION_ID = UUID("0198d500-1000-7000-8000-000000001612")
SPY_ALIAS_ID = UUID("0198d500-1000-7000-8000-000000001613")
COMPLETED_AT = datetime(2026, 8, 27, tzinfo=UTC)
BACKTEST_COST_MODEL = CostModel(
    commission_bps=Decimal("0"),
    spread_bps=Decimal("5"),
    slippage_bps=Decimal("10"),
)


def _digest(value: object) -> str:
    return hashlib.sha256(str(value).encode()).hexdigest()


def _official_decision_times(sessions: tuple[date, ...]) -> dict[date, datetime]:
    calendar = exchange_calendars.get_calendar("XNYS")
    return {
        session: calendar.session_close(session).to_pydatetime() + timedelta(minutes=30)
        for session in sessions
    }


def _history(end: date) -> tuple[date, ...]:
    calendar = exchange_calendars.get_calendar("XNYS")
    sessions = calendar.sessions_in_range(end - timedelta(days=400), end - timedelta(days=1))
    return tuple(value.date() for value in sessions[-200:])


def _bar(session: date, price: str) -> BacktestBar:
    official_close = exchange_calendars.get_calendar("XNYS").session_close(session).to_pydatetime()
    return BacktestBar(
        instrument_id=INSTRUMENT_ID,
        session=session,
        open=Decimal(price),
        high=Decimal(price),
        low=Decimal(price),
        close=Decimal(price),
        volume=Decimal("7000000"),
        observed_at=official_close,
        available_at=official_close,
        received_at=official_close,
    )


def _raw_inputs() -> tuple[
    CompiledStrategy, tuple[BacktestBar, ...], tuple[date, ...], BacktestProtocol
]:
    friday = date(2021, 1, 8)
    monday = date(2021, 1, 11)
    history = _history(friday)
    sessions = (*history, friday, monday)
    bars = tuple(_bar(session, "8") for session in history) + (
        _bar(friday, "10"),
        _bar(monday, "10"),
    )
    strategy = compile_strategy(baseline_v1_document(), baseline_feature_registry())
    protocol = BacktestProtocol(
        start=friday,
        end=monday,
        initial_nav=Decimal("10000.1234567890129"),
        cost_model=BACKTEST_COST_MODEL,
        walk_forward=True,
    )
    return strategy, bars, sessions, protocol


@pytest.mark.asyncio
async def test_writer_rejects_diagnostic_run_without_walk_forward(
    migrated_database_url: str,
) -> None:
    strategy, bars, sessions, protocol = _raw_inputs()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    price_authorities = await _seed_upstream_authorities(factory, bars, sessions)
    data = _dataset(bars, sessions, price_authorities)
    diagnostic_protocol = protocol.model_copy(update={"walk_forward": False})
    result = run_backtest(strategy, data, diagnostic_protocol)
    authority = BacktestPersistenceAuthority(
        authority_bundle_id=AUTHORITY_BUNDLE_ID,
        data_snapshot_id=DATA_SNAPSHOT_ID,
        source_policy_id=SOURCE_POLICY_ID,
        cost_policy_id=BACKTEST_COST_POLICY_ID,
        completed_at=COMPLETED_AT,
    )

    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="requires walk-forward protocol"):
            await persist_backtest_result(
                session, strategy, data, diagnostic_protocol, result, authority
            )


@pytest.mark.asyncio
async def test_writer_persists_published_challenger_with_trial_count(
    migrated_database_url: str,
) -> None:
    _, bars, sessions, protocol = _raw_inputs()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    price_authorities = await _seed_upstream_authorities(factory, bars, sessions)
    data = _dataset(bars, sessions, price_authorities)
    proposal = StrategyExperimentProposalV1.model_validate(
        {
            "schema_version": "strategy_experiment_proposal_v1",
            "parent_strategy_definition_id": "019d0000-0000-7000-8000-000000002200",
            "champion_generation": 1,
            "research_lineage_id": "019d0000-0000-7000-8000-000000005501",
            "hypothesis": "A registered target weight may improve robustness.",
            "expected_mechanism": "Frozen validation measures the bounded change.",
            "invalidation_conditions": ["The paired comparison does not improve."],
            "evidence_refs": ["019d0000-0000-7000-8000-000000005502"],
            "mutation_axes": [{"axis": "target_weight", "values": ["0.10"]}],
        }
    )
    parent = baseline_v1_document()
    delta = generate_candidate_deltas(proposal, parent, candidate_budget_policy_v1())[0]
    document = apply_candidate_delta(parent, delta, evolution_search_space_v1())
    challenger = compile_strategy(document, evolution_feature_registry_v1())
    challenger_protocol = protocol.model_copy(update={"experiment_attempt_count": 3})
    result = run_challenger_backtest_v1(
        challenger,
        data,
        challenger_protocol,
        experiment_attempt_count=3,
    )
    await _seed_data_snapshot(factory, data, challenger_protocol, result)
    async with factory() as session, session.begin():
        session.add(
            StrategyDefinitionRecord(
                strategy_version=challenger.strategy_version,
                schema_version="strategy_dsl_v1",
                expression_hash=challenger.expression_hash,
                document=document,
                lifecycle="challenger",
                published_at=COMPLETED_AT,
            )
        )
    authority = BacktestPersistenceAuthority(
        authority_bundle_id=AUTHORITY_BUNDLE_ID,
        data_snapshot_id=DATA_SNAPSHOT_ID,
        source_policy_id=SOURCE_POLICY_ID,
        cost_policy_id=BACKTEST_COST_POLICY_ID,
        completed_at=COMPLETED_AT,
    )

    async with factory() as session, session.begin():
        record = await persist_backtest_result(
            session,
            challenger,
            data,
            challenger_protocol,
            result,
            authority,
        )

    assert record.promotable is False
    assert record.historical_agent_overlay is False
    assert record.protocol["experiment_attempt_count"] == 3


async def _seed_upstream_authorities(
    factory: async_sessionmaker[AsyncSession],
    bars: tuple[BacktestBar, ...],
    sessions: tuple[date, ...],
    *,
    price_source: str = "fixture",
    price_feed: str = "test",
    price_delivery_mode: str = "fixture",
) -> tuple[PriceSeriesAuthority, ...]:
    universe_hash = _digest("universe")
    sector_hash = _digest("sector")
    first = sessions[0]
    action_session = sessions[-1]
    action_available_at = datetime.combine(sessions[-2], time(12), tzinfo=UTC)
    action_evidence_hash = _digest("dividend-on-fill")
    published_at = datetime.combine(first, time(20), tzinfo=UTC)
    async with factory() as session, session.begin():
        await session.execute(
            text(
                "INSERT INTO instruments "
                "(id, asset_type, exchange, currency, tick_size, risk_bucket) "
                "VALUES (:id, 'equity_etf', 'XNYS', 'USD', 0.01, 'EQUITY')"
            ),
            {"id": INSTRUMENT_ID},
        )
        await session.execute(
            text(
                "INSERT INTO instrument_aliases "
                "(id, instrument_id, symbol, alias_type, valid_from, valid_to, created_at) "
                "VALUES (:id, :instrument, 'SPY', 'ticker', :valid_from, NULL, :created_at)"
            ),
            {
                "id": SPY_ALIAS_ID,
                "instrument": INSTRUMENT_ID,
                "valid_from": first,
                "created_at": published_at,
            },
        )
        await session.execute(
            text(
                "INSERT INTO universe_snapshots "
                "(id, key, version, effective_from, source, content_hash, published_at) "
                "VALUES (:id, 'us_liquid', 'us_liquid_v1', :effective, 'fixture', "
                ":digest, NULL)"
            ),
            {"id": UNIVERSE_ID, "effective": first, "digest": universe_hash},
        )
        await session.execute(
            text(
                "INSERT INTO universe_members "
                "(id, universe_snapshot_id, instrument_id, inclusion_reason) "
                "VALUES (:id, :universe, :instrument, 'versioned fixture')"
            ),
            {"id": UNIVERSE_MEMBER_ID, "universe": UNIVERSE_ID, "instrument": INSTRUMENT_ID},
        )
        await session.execute(
            text("UPDATE universe_snapshots SET published_at = :published WHERE id = :id"),
            {"id": UNIVERSE_ID, "published": published_at},
        )
        await session.execute(
            text(
                "INSERT INTO sector_exposures "
                "(id, instrument_id, sector_code, weight, as_of, source_url, content_hash, "
                "synthetic, reviewed, created_at) VALUES "
                "(:id, :instrument, 'information_technology', 1, :as_of, "
                "'memory://sector', :digest, "
                "true, false, :published)"
            ),
            {
                "id": SECTOR_ID,
                "instrument": INSTRUMENT_ID,
                "as_of": first,
                "digest": sector_hash,
                "published": published_at,
            },
        )
        await session.execute(
            text(
                "INSERT INTO research_evidence_refs "
                "(id, source, provider_object_id, revision, available_at, location, "
                "content_hash, byte_length) VALUES "
                "(:id, 'fixture', 'dividend-on-fill', '1', :available, :location, "
                ":digest, 100)"
            ),
            {
                "id": ACTION_EVIDENCE_ID,
                "available": action_available_at,
                "location": f"memory://sha256/{action_evidence_hash}",
                "digest": action_evidence_hash,
            },
        )
        await session.execute(
            text(
                "INSERT INTO corporate_actions "
                "(id, evidence_ref_id, instrument_id, source, provider_event_id, "
                "action_type, status, process_date, ex_date, record_date, payable_date, "
                "rate, revision) VALUES "
                "(:id, :evidence, :instrument, 'fixture', 'dividend-on-fill', "
                "'cash_dividend', 'complete', :session, :session, NULL, :session, 0.10, '1')"
            ),
            {
                "id": ACTION_ID,
                "evidence": ACTION_EVIDENCE_ID,
                "instrument": INSTRUMENT_ID,
                "session": action_session,
            },
        )
        authorities: list[PriceSeriesAuthority] = []
        series_specs = (
            ("raw", RAW_ARTIFACT_ID, RAW_MANIFEST_ID, bars),
            (
                "all",
                ADJUSTED_ARTIFACT_ID,
                ADJUSTED_MANIFEST_ID,
                tuple(bar.model_copy() for bar in bars),
            ),
        )
        for adjustment, artifact_id, manifest_id, series_bars in series_specs:
            artifact_hash = _digest((adjustment, "artifact"))
            await session.execute(
                text(
                    "INSERT INTO data_artifacts "
                    "(id, uri, sha256, byte_size, mime_type, schema_version, source, "
                    "observed_at, available_at, received_at) VALUES "
                    "(:id, :uri, :digest, 100, 'application/x-parquet', '1', :source, "
                    ":clock, :clock, :clock)"
                ),
                {
                    "id": artifact_id,
                    "uri": f"memory://sha256/{artifact_hash}",
                    "digest": artifact_hash,
                    "source": price_source,
                    "clock": COMPLETED_AT,
                },
            )
            await session.execute(
                text(
                    "INSERT INTO price_series_manifests "
                    "(id, evidence_ref_id, data_artifact_id, instrument, source, feed, "
                    "delivery_mode, adjustment, interval, start_at, end_at, row_count) VALUES "
                    "(:id, NULL, :artifact, :instrument, :source, :feed, :delivery_mode, "
                    ":adjustment, '1Day', :start_at, :end_at, :row_count)"
                ),
                {
                    "id": manifest_id,
                    "artifact": artifact_id,
                    "instrument": str(INSTRUMENT_ID),
                    "source": price_source,
                    "feed": price_feed,
                    "delivery_mode": price_delivery_mode,
                    "adjustment": adjustment,
                    "start_at": series_bars[0].available_at,
                    "end_at": series_bars[-1].available_at,
                    "row_count": len(series_bars),
                },
            )
            await session.execute(
                text(
                    "INSERT INTO price_bar_records "
                    "(id, price_series_manifest_id, instrument_id, session, open, high, low, "
                    "close, volume, observed_at, available_at, received_at, revision, "
                    "quality_flags) VALUES "
                    "(:id, :manifest, :instrument, :session, :open, :high, :low, :close, "
                    ":volume, :observed, :available, :received, :revision, '[]'::jsonb)"
                ),
                [
                    {
                        "id": uuid4(),
                        "manifest": manifest_id,
                        "instrument": bar.instrument_id,
                        "session": bar.session,
                        "open": bar.open,
                        "high": bar.high,
                        "low": bar.low,
                        "close": bar.close,
                        "volume": bar.volume,
                        "observed": bar.observed_at,
                        "available": bar.available_at,
                        "received": bar.received_at,
                        "revision": bar.revision,
                    }
                    for bar in series_bars
                ],
            )
            manifest = await session.get(PriceSeriesManifest, manifest_id)
            assert manifest is not None
            finalized = await finalize_price_bar_set(session, manifest)
            if finalized is None:
                raise ValueError("price source is not admitted by source_policy_v1")
            authority, _ = finalized
            authorities.append(
                PriceSeriesAuthority(
                    price_bar_set_authority_id=authority.id,
                    price_series_manifest_id=manifest_id,
                    instrument_id=INSTRUMENT_ID,
                    source=price_source,
                    feed=price_feed,
                    delivery_mode=price_delivery_mode,
                    adjustment=adjustment,
                    manifest_hash=artifact_hash,
                    content_hash=authority.content_hash,
                    start_at=series_bars[0].available_at,
                    end_at=series_bars[-1].available_at,
                    row_count=len(series_bars),
                )
            )
        return tuple(authorities)


def _dataset(
    bars: tuple[BacktestBar, ...],
    sessions: tuple[date, ...],
    price_authorities: tuple[PriceSeriesAuthority, ...],
    *,
    action_id: UUID = ACTION_ID,
    action_revision: str = "1",
    action_value: Decimal = Decimal("0.10"),
    action_available_at: datetime | None = None,
    action_evidence_hash: str | None = None,
) -> PointInTimeDataset:
    first = sessions[0]
    last = sessions[-1]
    collection_start = sessions[-2]
    collection_end = sessions[-1]
    published_at = datetime.combine(first, time(20), tzinfo=UTC)
    universe_hash = _digest("universe")
    action_session = sessions[-1]
    action_available_at = action_available_at or datetime.combine(
        sessions[-2], time(12), tzinfo=UTC
    )
    action_evidence_hash = action_evidence_hash or _digest("dividend-on-fill")
    return PointInTimeDataset(
        bars=bars,
        adjusted_bars=tuple(bar.model_copy() for bar in bars),
        universe_snapshots=(
            UniverseSnapshot(
                authority_id=UNIVERSE_ID,
                version="us_liquid_v1",
                content_hash=universe_hash,
                source="fixture",
                effective_session=first,
                published_at=published_at,
                members=(INSTRUMENT_ID,),
            ),
        ),
        asset_types={INSTRUMENT_ID: "equity_etf"},
        risk_buckets={INSTRUMENT_ID: "EQUITY"},
        sector_classifications=(
            SectorClassification(
                authority_id=SECTOR_ID,
                instrument_id=INSTRUMENT_ID,
                sector="information_technology",
                effective_session=first,
                published_at=published_at,
                source_uri="memory://sector",
                content_hash=_digest("sector"),
                weight=Decimal(1),
                synthetic=True,
                reviewed=False,
            ),
        ),
        authority=BacktestDataAuthority(
            authority_bundle_id=AUTHORITY_BUNDLE_ID,
            data_snapshot_id=DATA_SNAPSHOT_ID,
            source_policy_id=SOURCE_POLICY_ID,
            cost_policy_id=BACKTEST_COST_POLICY_ID,
            cost_policy_version="backtest_cost_policy_v1",
            exchange_calendar=ExchangeCalendarAuthority(
                authority_id=CALENDAR_AUTHORITY_ID,
                version=f"exchange_calendars_{package_version('exchange-calendars')}_XNYS",
                source="exchange_calendars",
                range_start=first,
                range_end=last,
                query_parameters={
                    "exchange": "XNYS",
                    "package_version": package_version("exchange-calendars"),
                },
                content_hash=exchange_calendar_content_hash(
                    sessions, _official_decision_times(sessions)
                ),
                sessions=sessions,
                decision_times=_official_decision_times(sessions),
            ),
            universe_collection=AuthorityCollection(
                authority_id=UNIVERSE_COLLECTION_ID,
                kind="universe",
                source="fixture",
                range_start=collection_start,
                range_end=collection_end,
                query_parameters={"key": "us_liquid"},
                member_ids=(UNIVERSE_ID,),
                content_hash=authority_collection_content_hash(
                    kind="universe",
                    source="fixture",
                    range_start=collection_start,
                    range_end=collection_end,
                    query_parameters={"key": "us_liquid"},
                    member_ids=(UNIVERSE_ID,),
                ),
            ),
            sector_collection=AuthorityCollection(
                authority_id=SECTOR_COLLECTION_ID,
                kind="sector",
                source="fixture",
                range_start=collection_start,
                range_end=collection_end,
                query_parameters={
                    "taxonomy": "NEXUS_SECTOR_V1",
                    "synthetic": True,
                    "reviewed": False,
                },
                member_ids=(SECTOR_ID,),
                content_hash=authority_collection_content_hash(
                    kind="sector",
                    source="fixture",
                    range_start=collection_start,
                    range_end=collection_end,
                    query_parameters={
                        "taxonomy": "NEXUS_SECTOR_V1",
                        "synthetic": True,
                        "reviewed": False,
                    },
                    member_ids=(SECTOR_ID,),
                ),
            ),
            corporate_action_collection=AuthorityCollection(
                authority_id=ACTION_COLLECTION_ID,
                kind="corporate_action",
                source="fixture",
                range_start=collection_start,
                range_end=collection_end,
                query_parameters={
                    "provider_mapping_version": "corporate_action_mapping_v1",
                    "status": "complete",
                },
                member_ids=(action_id,),
                content_hash=authority_collection_content_hash(
                    kind="corporate_action",
                    source="fixture",
                    range_start=collection_start,
                    range_end=collection_end,
                    query_parameters={
                        "provider_mapping_version": "corporate_action_mapping_v1",
                        "status": "complete",
                    },
                    member_ids=(action_id,),
                ),
            ),
            price_series=price_authorities,
            source_selections=tuple(
                PriceSourceSelectionAuthority(
                    instrument_id=item.instrument_id,
                    adjustment=item.adjustment,
                    request_start_session=first,
                    request_end_session=last,
                    decision_time=COMPLETED_AT,
                    tick_size=Decimal("0.01"),
                    candidate_price_bar_set_authority_ids=(item.price_bar_set_authority_id,),
                    selected_price_bar_set_authority_id=item.price_bar_set_authority_id,
                )
                for item in price_authorities
            ),
            spy_benchmark=BenchmarkInstrumentAuthority(
                alias_id=SPY_ALIAS_ID,
                instrument_id=INSTRUMENT_ID,
                valid_from=first,
                created_at=published_at,
            ),
            equal_weight_benchmark=EqualWeightBenchmarkAuthority(
                universe_snapshot_id=UNIVERSE_ID,
                universe_version="us_liquid_v1",
                content_hash=universe_hash,
                source="fixture",
                effective_session=first,
                published_at=published_at,
                instrument_ids=(INSTRUMENT_ID,),
            ),
        ),
        spy_instrument_id=INSTRUMENT_ID,
        corporate_actions=(
            CorporateActionEvent(
                authority_id=action_id,
                instrument_id=INSTRUMENT_ID,
                source="fixture",
                provider_event_id="dividend-on-fill",
                provider_action_type="cash_dividend",
                status="complete",
                provider_ex_date=action_session,
                provider_payable_date=action_session,
                session=action_session,
                payable_session=action_session,
                action_type="dividend",
                value=action_value,
                revision=action_revision,
                available_at=action_available_at,
                evidence_hash=action_evidence_hash,
            ),
        ),
    )


async def _seed_data_snapshot(
    factory: async_sessionmaker[AsyncSession],
    data: PointInTimeDataset,
    protocol: BacktestProtocol,
    result: BacktestResult,
) -> None:
    async with factory() as session, session.begin():
        await session.execute(
            text(
                "INSERT INTO data_snapshots "
                "(id, source, provider_object_id, revision, observed_at, available_at, "
                "received_at, schema_version, payload) VALUES "
                "(:id, 'nexus_backtest_bundle', 'baseline-v1', '1', :clock, :clock, :clock, "
                "'backtest_authority_v1', CAST(:payload AS jsonb))"
            ),
            {
                "id": DATA_SNAPSHOT_ID,
                "clock": COMPLETED_AT,
                "payload": json.dumps(expected_data_snapshot_payload(data, result)),
            },
        )
        calendar = data.authority.exchange_calendar
        exchange_calendar = exchange_calendars.get_calendar(calendar.exchange)
        await session.execute(
            text(
                "INSERT INTO exchange_calendar_authorities "
                "(id, exchange, version, source, range_start, range_end, query_parameters, "
                "completeness_status, content_hash, published_at) VALUES "
                "(:id, :exchange, :version, :source, :range_start, :range_end, "
                "CAST(:query AS jsonb), 'complete', :digest, :completed)"
            ),
            {
                "id": calendar.authority_id,
                "exchange": calendar.exchange,
                "version": calendar.version,
                "source": calendar.source,
                "range_start": calendar.range_start,
                "range_end": calendar.range_end,
                "query": json.dumps(dict(calendar.query_parameters)),
                "digest": calendar.content_hash,
                "completed": COMPLETED_AT,
            },
        )
        await session.execute(
            text(
                "INSERT INTO exchange_calendar_sessions "
                "(calendar_authority_id, session, decision_at, market_open_at, "
                "market_close_at) VALUES "
                "(:calendar, :session, :decision_at, :market_open_at, :market_close_at)"
            ),
            [
                {
                    "calendar": calendar.authority_id,
                    "session": value,
                    "decision_at": calendar.decision_times[value],
                    "market_open_at": exchange_calendar.session_open(value).to_pydatetime(),
                    "market_close_at": exchange_calendar.session_close(value).to_pydatetime(),
                }
                for value in calendar.sessions
            ],
        )
        collections = (
            data.authority.universe_collection,
            data.authority.sector_collection,
            data.authority.corporate_action_collection,
        )
        for collection in collections:
            await session.execute(
                text(
                    "INSERT INTO backtest_authority_collections "
                    "(id, kind, source, range_start, range_end, query_parameters, "
                    "completeness_status, content_hash, completed_at) VALUES "
                    "(:id, :kind, :source, :range_start, :range_end, CAST(:query AS jsonb), "
                    "'complete', :digest, :completed)"
                ),
                {
                    "id": collection.authority_id,
                    "kind": collection.kind,
                    "source": collection.source,
                    "range_start": collection.range_start,
                    "range_end": collection.range_end,
                    "query": json.dumps(dict(collection.query_parameters)),
                    "digest": collection.content_hash,
                    "completed": COMPLETED_AT,
                },
            )
        await session.execute(
            text(
                "INSERT INTO backtest_universe_authority_members "
                "(collection_id, universe_snapshot_id) VALUES (:collection, :member)"
            ),
            {"collection": UNIVERSE_COLLECTION_ID, "member": UNIVERSE_ID},
        )
        await session.execute(
            text(
                "INSERT INTO backtest_sector_authority_members "
                "(collection_id, sector_exposure_id) VALUES (:collection, :member)"
            ),
            {"collection": SECTOR_COLLECTION_ID, "member": SECTOR_ID},
        )
        await session.execute(
            text(
                "INSERT INTO backtest_corporate_action_authority_members "
                "(collection_id, corporate_action_id) VALUES (:collection, :member)"
            ),
            {
                "collection": ACTION_COLLECTION_ID,
                "member": data.authority.corporate_action_collection.member_ids[0],
            },
        )
        await session.execute(
            text(
                "INSERT INTO backtest_authority_bundles "
                "(id, data_snapshot_id, source_policy_id, cost_policy_id, "
                "calendar_authority_id, universe_collection_id, sector_collection_id, "
                "corporate_action_collection_id, spy_instrument_alias_id, range_start, "
                "range_end, completeness_status, "
                "content_hash, source_selection_reports, completed_at) VALUES "
                "(:id, :snapshot, :source_policy, :cost_policy, :calendar, :universe, "
                ":sector, :actions, :spy_alias, :range_start, :range_end, 'complete', :digest, "
                "CAST(:source_selections AS jsonb), :completed)"
            ),
            {
                "id": AUTHORITY_BUNDLE_ID,
                "snapshot": DATA_SNAPSHOT_ID,
                "source_policy": SOURCE_POLICY_ID,
                "cost_policy": BACKTEST_COST_POLICY_ID,
                "calendar": CALENDAR_AUTHORITY_ID,
                "universe": UNIVERSE_COLLECTION_ID,
                "sector": SECTOR_COLLECTION_ID,
                "actions": ACTION_COLLECTION_ID,
                "spy_alias": data.authority.spy_benchmark.alias_id,
                "range_start": protocol.start,
                "range_end": protocol.end,
                "digest": result.authority_hash,
                "source_selections": json.dumps(
                    expected_data_snapshot_payload(data, result)["source_selection_reports"]
                ),
                "completed": COMPLETED_AT,
            },
        )
        await session.execute(
            text(
                "INSERT INTO backtest_price_authority_members "
                "(bundle_id, price_bar_set_authority_id) VALUES (:bundle, :member)"
            ),
            [
                {
                    "bundle": AUTHORITY_BUNDLE_ID,
                    "member": item,
                }
                for report in data.authority.source_selections
                for item in report.candidate_price_bar_set_authority_ids
            ],
        )
        await session.execute(
            text(
                "INSERT INTO backtest_instrument_authority_members "
                "(bundle_id, instrument_id) VALUES (:bundle, :member)"
            ),
            [
                {"bundle": AUTHORITY_BUNDLE_ID, "member": instrument_id}
                for instrument_id in data.asset_types
            ],
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    ("range", "universe_query", "sector_query", "corporate_action_query"),
)
async def test_writer_rejects_self_consistent_but_false_collection_contracts(
    migrated_database_url: str,
    case: str,
) -> None:
    strategy, bars, sessions, protocol = _raw_inputs()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    price_authorities = await _seed_upstream_authorities(factory, bars, sessions)
    data = _dataset(bars, sessions, price_authorities)
    attribute = {
        "range": "universe_collection",
        "universe_query": "universe_collection",
        "sector_query": "sector_collection",
        "corporate_action_query": "corporate_action_collection",
    }[case]
    collection = getattr(data.authority, attribute)
    query_parameters = dict(collection.query_parameters)
    range_start = collection.range_start
    if case == "range":
        range_start = sessions[0]
    elif case == "universe_query":
        query_parameters = {"key": "not_us_liquid"}
    elif case == "sector_query":
        query_parameters = {"taxonomy": "NEXUS_SECTOR_V1", "reviewed": False}
    else:
        query_parameters = {
            "provider_mapping_version": "unreviewed_mapping",
            "status": "complete",
        }
    forged_collection = collection.model_copy(
        update={
            "range_start": range_start,
            "query_parameters": query_parameters,
            "content_hash": authority_collection_content_hash(
                kind=collection.kind,
                source=collection.source,
                range_start=range_start,
                range_end=collection.range_end,
                query_parameters=query_parameters,
                member_ids=collection.member_ids,
            ),
        }
    )
    forged_authority = data.authority.model_copy(update={attribute: forged_collection})
    forged_data = data.model_copy(update={"authority": forged_authority})
    result = run_backtest(strategy, forged_data, protocol)
    await _seed_data_snapshot(factory, forged_data, protocol, result)
    authority = BacktestPersistenceAuthority(
        authority_bundle_id=AUTHORITY_BUNDLE_ID,
        data_snapshot_id=DATA_SNAPSHOT_ID,
        source_policy_id=SOURCE_POLICY_ID,
        cost_policy_id=BACKTEST_COST_POLICY_ID,
        completed_at=COMPLETED_AT,
    )

    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="collection query contract"):
            await persist_backtest_result(
                session, strategy, forged_data, protocol, result, authority
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("source", "feed", "delivery_mode"),
    (
        ("evil", "test", "delayed"),
        ("alpaca", "iex", "delayed"),
        ("alpaca", "sip", "real_time"),
        ("fosun", "US", "delayed"),
    ),
)
async def test_writer_rejects_price_sources_not_proven_by_source_policy_v1(
    migrated_database_url: str,
    source: str,
    feed: str,
    delivery_mode: str,
) -> None:
    strategy, bars, sessions, protocol = _raw_inputs()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    with pytest.raises(ValueError, match="source_policy_v1"):
        await _seed_upstream_authorities(
            factory,
            bars,
            sessions,
            price_source=source,
            price_feed=feed,
            price_delivery_mode=delivery_mode,
        )


@pytest.mark.asyncio
async def test_writer_candidate_domain_is_bound_to_request_not_selected_manifest_range(
    migrated_database_url: str,
) -> None:
    strategy, bars, sessions, protocol = _raw_inputs()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    price_authorities = await _seed_upstream_authorities(factory, bars, sessions)
    partial_bars = bars[1:]
    artifact_id = uuid4()
    manifest_id = uuid4()
    artifact_hash = _digest("higher-priority-partial-range")
    async with factory() as session, session.begin():
        await session.execute(
            text(
                "INSERT INTO data_artifacts "
                "(id, uri, sha256, byte_size, mime_type, schema_version, source, "
                "observed_at, available_at, received_at) VALUES "
                "(:id, :uri, :digest, 100, 'application/x-parquet', '1', 'alpaca', "
                ":clock, :clock, :clock)"
            ),
            {
                "id": artifact_id,
                "uri": f"memory://sha256/{artifact_hash}",
                "digest": artifact_hash,
                "clock": COMPLETED_AT,
            },
        )
        await session.execute(
            text(
                "INSERT INTO price_series_manifests "
                "(id, evidence_ref_id, data_artifact_id, instrument, source, feed, "
                "delivery_mode, adjustment, interval, start_at, end_at, row_count) VALUES "
                "(:id, NULL, :artifact, :instrument, 'alpaca', 'sip', 'delayed', "
                "'raw', '1Day', :start_at, :end_at, :row_count)"
            ),
            {
                "id": manifest_id,
                "artifact": artifact_id,
                "instrument": str(INSTRUMENT_ID),
                "start_at": partial_bars[0].available_at,
                "end_at": partial_bars[-1].available_at,
                "row_count": len(partial_bars),
            },
        )
        await session.execute(
            text(
                "INSERT INTO price_bar_records "
                "(id, price_series_manifest_id, instrument_id, session, open, high, low, "
                "close, volume, observed_at, available_at, received_at, revision, "
                "quality_flags) VALUES "
                "(:id, :manifest, :instrument, :session, :open, :high, :low, :close, "
                ":volume, :observed, :available, :received, :revision, '[]'::jsonb)"
            ),
            [
                {
                    "id": uuid4(),
                    "manifest": manifest_id,
                    "instrument": bar.instrument_id,
                    "session": bar.session,
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "volume": bar.volume,
                    "observed": bar.observed_at,
                    "available": bar.available_at,
                    "received": bar.received_at,
                    "revision": bar.revision,
                }
                for bar in partial_bars
            ],
        )
        manifest = await session.get(PriceSeriesManifest, manifest_id)
        assert manifest is not None
        assert await finalize_price_bar_set(session, manifest) is not None
    data = _dataset(bars, sessions, price_authorities)
    result = run_backtest(strategy, data, protocol)
    await _seed_data_snapshot(factory, data, protocol, result)
    authority = BacktestPersistenceAuthority(
        authority_bundle_id=AUTHORITY_BUNDLE_ID,
        data_snapshot_id=DATA_SNAPSHOT_ID,
        source_policy_id=SOURCE_POLICY_ID,
        cost_policy_id=BACKTEST_COST_POLICY_ID,
        completed_at=COMPLETED_AT,
    )

    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="candidate set is incomplete"):
            await persist_backtest_result(session, strategy, data, protocol, result, authority)


@pytest.mark.asyncio
async def test_writer_collapses_corporate_action_revisions_at_the_cutoff(
    migrated_database_url: str,
) -> None:
    strategy, bars, sessions, protocol = _raw_inputs()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    price_authorities = await _seed_upstream_authorities(factory, bars, sessions)
    revised_action_id = uuid4()
    revised_evidence_id = uuid4()
    revised_hash = _digest("dividend-on-fill-revision-2")
    revised_available_at = datetime.combine(sessions[-2], time(13), tzinfo=UTC)
    revised_created_at = COMPLETED_AT - timedelta(hours=1)
    async with factory() as session, session.begin():
        await session.execute(
            text(
                "INSERT INTO research_evidence_refs "
                "(id, source, provider_object_id, revision, available_at, location, "
                "content_hash, byte_length, created_at) VALUES "
                "(:id, 'fixture', 'dividend-on-fill', '2', :available, :location, "
                ":digest, 100, :created)"
            ),
            {
                "id": revised_evidence_id,
                "available": revised_available_at,
                "location": f"memory://sha256/{revised_hash}",
                "digest": revised_hash,
                "created": revised_created_at,
            },
        )
        await session.execute(
            text(
                "INSERT INTO corporate_actions "
                "(id, evidence_ref_id, instrument_id, source, provider_event_id, "
                "action_type, status, process_date, ex_date, record_date, payable_date, "
                "rate, revision, created_at) VALUES "
                "(:id, :evidence, :instrument, 'fixture', 'dividend-on-fill', "
                "'cash_dividend', 'complete', :session, :session, NULL, :session, "
                "0.20, '2', :created)"
            ),
            {
                "id": revised_action_id,
                "evidence": revised_evidence_id,
                "instrument": INSTRUMENT_ID,
                "session": sessions[-1],
                "created": revised_created_at,
            },
        )
    data = _dataset(
        bars,
        sessions,
        price_authorities,
        action_id=revised_action_id,
        action_revision="2",
        action_value=Decimal("0.20"),
        action_available_at=revised_available_at,
        action_evidence_hash=revised_hash,
    )
    result = run_backtest(strategy, data, protocol)
    await _seed_data_snapshot(factory, data, protocol, result)
    authority = BacktestPersistenceAuthority(
        authority_bundle_id=AUTHORITY_BUNDLE_ID,
        data_snapshot_id=DATA_SNAPSHOT_ID,
        source_policy_id=SOURCE_POLICY_ID,
        cost_policy_id=BACKTEST_COST_POLICY_ID,
        completed_at=COMPLETED_AT,
    )

    async with factory() as session, session.begin():
        record = await persist_backtest_result(session, strategy, data, protocol, result, authority)

    assert record.result_hash == result.result_hash


@pytest.mark.asyncio
async def test_writer_maps_pre_start_weekend_delisting_into_first_protocol_session(
    migrated_database_url: str,
) -> None:
    first_protocol_session = date(2021, 1, 11)
    last_protocol_session = date(2021, 1, 12)
    history = _history(first_protocol_session)
    sessions = (*history, first_protocol_session, last_protocol_session)
    bars = tuple(_bar(session, "8") for session in history) + (
        _bar(first_protocol_session, "10"),
        _bar(last_protocol_session, "10"),
    )
    strategy = compile_strategy(baseline_v1_document(), baseline_feature_registry())
    protocol = BacktestProtocol(
        start=first_protocol_session,
        end=last_protocol_session,
        initial_nav=Decimal("10000"),
        cost_model=BACKTEST_COST_MODEL,
        walk_forward=True,
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    price_authorities = await _seed_upstream_authorities(factory, bars, sessions)
    omitted_action_id = uuid4()
    omitted_evidence_id = uuid4()
    omitted_hash = _digest("pre-start-weekend-delisting")
    async with factory() as session, session.begin():
        await session.execute(
            text(
                "INSERT INTO research_evidence_refs "
                "(id, source, provider_object_id, revision, available_at, location, "
                "content_hash, byte_length) VALUES "
                "(:id, 'fixture', 'pre-start-weekend-delisting', '1', :available, "
                ":location, :digest, 100)"
            ),
            {
                "id": omitted_evidence_id,
                "available": datetime(2021, 1, 8, 12, tzinfo=UTC),
                "location": f"memory://sha256/{omitted_hash}",
                "digest": omitted_hash,
            },
        )
        await session.execute(
            text(
                "INSERT INTO corporate_actions "
                "(id, evidence_ref_id, instrument_id, source, provider_event_id, "
                "action_type, status, process_date, ex_date, rate, revision) VALUES "
                "(:id, :evidence, :instrument, 'fixture', 'pre-start-weekend-delisting', "
                "'delisting', 'complete', :process_date, NULL, 0, '1')"
            ),
            {
                "id": omitted_action_id,
                "evidence": omitted_evidence_id,
                "instrument": INSTRUMENT_ID,
                "process_date": first_protocol_session - timedelta(days=1),
            },
        )
    data = _dataset(bars, sessions, price_authorities)
    result = run_backtest(strategy, data, protocol)
    await _seed_data_snapshot(factory, data, protocol, result)
    authority = BacktestPersistenceAuthority(
        authority_bundle_id=AUTHORITY_BUNDLE_ID,
        data_snapshot_id=DATA_SNAPSHOT_ID,
        source_policy_id=SOURCE_POLICY_ID,
        cost_policy_id=BACKTEST_COST_POLICY_ID,
        completed_at=COMPLETED_AT,
    )

    async with factory() as session, session.begin():
        with pytest.raises(
            ValueError, match="corporate-action collection omits a database-authoritative event"
        ):
            await persist_backtest_result(session, strategy, data, protocol, result, authority)


@pytest.mark.asyncio
async def test_writer_persists_same_session_price_revisions_without_lookahead(
    migrated_database_url: str,
) -> None:
    strategy, base_bars, sessions, protocol = _raw_inputs()
    original = base_bars[-2]
    correction_available_at = original.available_at + timedelta(days=1)
    correction = original.model_copy(
        update={
            "open": Decimal("1"),
            "high": Decimal("1"),
            "low": Decimal("1"),
            "close": Decimal("1"),
            "available_at": correction_available_at,
            "received_at": correction_available_at,
            "revision": "2",
        }
    )
    bars = (*base_bars[:-1], correction, base_bars[-1])
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    price_authorities = await _seed_upstream_authorities(factory, bars, sessions)
    data = _dataset(bars, sessions, price_authorities)

    result = run_backtest(strategy, data, protocol)

    assert len(result.fills) == 1
    assert result.fills[0].signal_session == original.session
    await _seed_data_snapshot(factory, data, protocol, result)
    authority = BacktestPersistenceAuthority(
        authority_bundle_id=AUTHORITY_BUNDLE_ID,
        data_snapshot_id=DATA_SNAPSHOT_ID,
        source_policy_id=SOURCE_POLICY_ID,
        cost_policy_id=BACKTEST_COST_POLICY_ID,
        completed_at=COMPLETED_AT,
    )
    async with factory() as session, session.begin():
        record = await persist_backtest_result(session, strategy, data, protocol, result, authority)

    assert record.result_hash == result.result_hash


@pytest.mark.asyncio
async def test_writer_rejects_ambiguous_spy_alias_authority(
    migrated_database_url: str,
) -> None:
    strategy, bars, sessions, protocol = _raw_inputs()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    price_authorities = await _seed_upstream_authorities(factory, bars, sessions)
    forged_alias_id = uuid4()
    forged_valid_from = sessions[0] + timedelta(days=1)
    forged_created_at = datetime.combine(sessions[0], time(20), tzinfo=UTC)
    async with factory() as session, session.begin():
        await session.execute(
            text(
                "INSERT INTO instrument_aliases "
                "(id, instrument_id, symbol, alias_type, valid_from, valid_to, created_at) "
                "VALUES (:id, :instrument, 'SPY', 'ticker', :valid_from, NULL, :created_at)"
            ),
            {
                "id": forged_alias_id,
                "instrument": INSTRUMENT_ID,
                "valid_from": forged_valid_from,
                "created_at": forged_created_at,
            },
        )
    data = _dataset(bars, sessions, price_authorities)
    forged_data = data.model_copy(
        update={
            "authority": data.authority.model_copy(
                update={
                    "spy_benchmark": data.authority.spy_benchmark.model_copy(
                        update={
                            "alias_id": forged_alias_id,
                            "valid_from": forged_valid_from,
                            "created_at": forged_created_at,
                        }
                    )
                }
            )
        }
    )
    result = run_backtest(strategy, forged_data, protocol)
    await _seed_data_snapshot(factory, forged_data, protocol, result)
    authority = BacktestPersistenceAuthority(
        authority_bundle_id=AUTHORITY_BUNDLE_ID,
        data_snapshot_id=DATA_SNAPSHOT_ID,
        source_policy_id=SOURCE_POLICY_ID,
        cost_policy_id=BACKTEST_COST_POLICY_ID,
        completed_at=COMPLETED_AT,
    )

    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="SPY benchmark alias authority"):
            await persist_backtest_result(
                session, strategy, forged_data, protocol, result, authority
            )


@pytest.mark.asyncio
async def test_writer_rejects_daily_bar_available_before_official_close(
    migrated_database_url: str,
) -> None:
    strategy, base_bars, sessions, protocol = _raw_inputs()
    early = base_bars[0].model_copy(
        update={
            "observed_at": datetime.combine(base_bars[0].session, time(12), tzinfo=UTC),
            "available_at": datetime.combine(base_bars[0].session, time(12), tzinfo=UTC),
            "received_at": datetime.combine(base_bars[0].session, time(12), tzinfo=UTC),
        }
    )
    bars = (early, *base_bars[1:])
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    price_authorities = await _seed_upstream_authorities(factory, bars, sessions)
    data = _dataset(bars, sessions, price_authorities)
    result = run_backtest(strategy, data, protocol)
    await _seed_data_snapshot(factory, data, protocol, result)
    authority = BacktestPersistenceAuthority(
        authority_bundle_id=AUTHORITY_BUNDLE_ID,
        data_snapshot_id=DATA_SNAPSHOT_ID,
        source_policy_id=SOURCE_POLICY_ID,
        cost_policy_id=BACKTEST_COST_POLICY_ID,
        completed_at=COMPLETED_AT,
    )

    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="available before official close"):
            await persist_backtest_result(session, strategy, data, protocol, result, authority)


@pytest.mark.asyncio
async def test_writer_serializes_publish_with_concurrent_authority_and_child_appends(
    migrated_database_url: str,
) -> None:
    strategy, bars, sessions, protocol = _raw_inputs()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    price_authorities = await _seed_upstream_authorities(factory, bars, sessions)
    data = _dataset(bars, sessions, price_authorities)
    result = run_backtest(strategy, data, protocol)
    await _seed_data_snapshot(factory, data, protocol, result)
    authority = BacktestPersistenceAuthority(
        authority_bundle_id=AUTHORITY_BUNDLE_ID,
        data_snapshot_id=DATA_SNAPSHOT_ID,
        source_policy_id=SOURCE_POLICY_ID,
        cost_policy_id=BACKTEST_COST_POLICY_ID,
        completed_at=COMPLETED_AT,
    )
    writer_session = factory()
    writer_transaction = await writer_session.begin()
    try:
        record = await persist_backtest_result(
            writer_session, strategy, data, protocol, result, authority
        )

        with pytest.raises(DBAPIError, match="backtest authority set is sealed"):
            async with writer_session.begin_nested():
                await writer_session.execute(
                    text("INSERT INTO backtest_metrics (backtest_run_id) VALUES (:owner)"),
                    {"owner": record.id},
                )

        async def append(statement: str, parameters: dict[str, object]) -> None:
            async with factory() as session, session.begin():
                await session.execute(text(statement), parameters)

        tasks = (
            asyncio.create_task(
                append(
                    "INSERT INTO exchange_calendar_sessions "
                    "(calendar_authority_id, session, decision_at, market_open_at, "
                    "market_close_at) VALUES "
                    "(:owner, :member, :decision_at, :market_open_at, :market_close_at)",
                    {
                        "owner": CALENDAR_AUTHORITY_ID,
                        "member": protocol.end + timedelta(days=1),
                        "decision_at": COMPLETED_AT,
                        "market_open_at": COMPLETED_AT - timedelta(hours=7),
                        "market_close_at": COMPLETED_AT - timedelta(hours=1),
                    },
                )
            ),
            asyncio.create_task(
                append(
                    "INSERT INTO backtest_metrics (backtest_run_id) VALUES (:owner)",
                    {"owner": record.id},
                )
            ),
        )
        for task in tasks:
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(task), timeout=0.1)

        await writer_transaction.commit()
        for task in tasks:
            with pytest.raises(DBAPIError, match="backtest authority set is sealed"):
                await task
    finally:
        if writer_transaction.is_active:
            await writer_transaction.rollback()
        await writer_session.close()


@pytest.mark.asyncio
async def test_writer_serializes_concurrent_upstream_mutation_with_authority_freeze(
    migrated_database_url: str,
) -> None:
    strategy, bars, sessions, protocol = _raw_inputs()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    price_authorities = await _seed_upstream_authorities(factory, bars, sessions)
    data = _dataset(bars, sessions, price_authorities)
    result = run_backtest(strategy, data, protocol)
    await _seed_data_snapshot(factory, data, protocol, result)
    authority = BacktestPersistenceAuthority(
        authority_bundle_id=AUTHORITY_BUNDLE_ID,
        data_snapshot_id=DATA_SNAPSHOT_ID,
        source_policy_id=SOURCE_POLICY_ID,
        cost_policy_id=BACKTEST_COST_POLICY_ID,
        completed_at=COMPLETED_AT,
    )
    writer_session = factory()
    writer_transaction = await writer_session.begin()
    try:
        await persist_backtest_result(writer_session, strategy, data, protocol, result, authority)

        async def mutate_referenced_snapshot() -> None:
            async with factory() as session, session.begin():
                await session.execute(
                    text("UPDATE data_snapshots SET payload = '{}'::jsonb WHERE id = :id"),
                    {"id": DATA_SNAPSHOT_ID},
                )

        mutation = asyncio.create_task(mutate_referenced_snapshot())
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.shield(mutation), timeout=0.1)

        await writer_transaction.commit()
        with pytest.raises(DBAPIError, match="backtest upstream authority is frozen") as exc_info:
            await mutation
        assert getattr(exc_info.value.orig, "sqlstate", None) == "23514"
    finally:
        if writer_transaction.is_active:
            await writer_transaction.rollback()
        await writer_session.close()

    unreferenced_snapshot_id = uuid4()
    async with factory() as session, session.begin():
        await session.execute(
            text(
                "INSERT INTO data_snapshots "
                "(id, source, provider_object_id, revision, observed_at, available_at, "
                "received_at, schema_version, payload) VALUES "
                "(:id, 'fixture', 'unreferenced', '1', :clock, :clock, :clock, '1', "
                "CAST(:payload AS jsonb))"
            ),
            {
                "id": unreferenced_snapshot_id,
                "clock": COMPLETED_AT,
                "payload": json.dumps({"initial": True}),
            },
        )
        await session.execute(
            text("UPDATE data_snapshots SET payload = CAST(:payload AS jsonb) WHERE id = :id"),
            {
                "id": unreferenced_snapshot_id,
                "payload": json.dumps({"updated": True}),
            },
        )
    async with factory() as session:
        updated_payload = await session.scalar(
            text("SELECT payload FROM data_snapshots WHERE id = :id"),
            {"id": unreferenced_snapshot_id},
        )
    assert updated_payload == {"updated": True}


@pytest.mark.asyncio
async def test_writer_serializes_concurrent_price_revision_append_with_authority_freeze(
    migrated_database_url: str,
) -> None:
    strategy, bars, sessions, protocol = _raw_inputs()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    price_authorities = await _seed_upstream_authorities(factory, bars, sessions)
    data = _dataset(bars, sessions, price_authorities)
    result = run_backtest(strategy, data, protocol)
    await _seed_data_snapshot(factory, data, protocol, result)
    authority = BacktestPersistenceAuthority(
        authority_bundle_id=AUTHORITY_BUNDLE_ID,
        data_snapshot_id=DATA_SNAPSHOT_ID,
        source_policy_id=SOURCE_POLICY_ID,
        cost_policy_id=BACKTEST_COST_POLICY_ID,
        completed_at=COMPLETED_AT,
    )
    last_bar = bars[-1]

    async def append_revision(record_id: UUID) -> None:
        async with factory() as session, session.begin():
            await session.execute(
                text(
                    "INSERT INTO price_bar_records "
                    "(id, price_series_manifest_id, instrument_id, session, open, high, low, "
                    "close, volume, observed_at, available_at, received_at, revision, "
                    "quality_flags) VALUES "
                    "(:id, :manifest, :instrument, :session, :open, :high, :low, :close, "
                    ":volume, :observed, :available, :available, 'late-2', '[]'::jsonb)"
                ),
                {
                    "id": record_id,
                    "manifest": RAW_MANIFEST_ID,
                    "instrument": INSTRUMENT_ID,
                    "session": last_bar.session,
                    "open": last_bar.open,
                    "high": last_bar.high,
                    "low": last_bar.low,
                    "close": last_bar.close,
                    "volume": last_bar.volume,
                    "observed": last_bar.observed_at,
                    "available": COMPLETED_AT,
                },
            )

    writer_session = factory()
    writer_transaction = await writer_session.begin()
    try:
        await persist_backtest_result(writer_session, strategy, data, protocol, result, authority)
        concurrent_append = asyncio.create_task(append_revision(uuid4()))
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.shield(concurrent_append), timeout=0.1)

        await writer_transaction.commit()
        with pytest.raises(DBAPIError, match="backtest upstream authority is frozen") as exc_info:
            await concurrent_append
        assert getattr(exc_info.value.orig, "sqlstate", None) == "23514"
    finally:
        if writer_transaction.is_active:
            await writer_transaction.rollback()
        await writer_session.close()

    with pytest.raises(DBAPIError, match="backtest upstream authority is frozen") as exc_info:
        await append_revision(uuid4())
    assert getattr(exc_info.value.orig, "sqlstate", None) == "23514"


@pytest.mark.asyncio
async def test_writer_waits_for_concurrent_upstream_publication_then_fails_closed(
    migrated_database_url: str,
) -> None:
    strategy, bars, sessions, protocol = _raw_inputs()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    price_authorities = await _seed_upstream_authorities(factory, bars, sessions)
    data = _dataset(bars, sessions, price_authorities)
    result = run_backtest(strategy, data, protocol)
    await _seed_data_snapshot(factory, data, protocol, result)
    authority = BacktestPersistenceAuthority(
        authority_bundle_id=AUTHORITY_BUNDLE_ID,
        data_snapshot_id=DATA_SNAPSHOT_ID,
        source_policy_id=SOURCE_POLICY_ID,
        cost_policy_id=BACKTEST_COST_POLICY_ID,
        completed_at=COMPLETED_AT,
    )
    publisher = factory()
    publisher_transaction = await publisher.begin()
    try:
        evidence_id = uuid4()
        action_id = uuid4()
        evidence_hash = _digest("concurrent-upstream-action")
        await publisher.execute(
            text(
                "INSERT INTO research_evidence_refs "
                "(id, source, provider_object_id, revision, available_at, location, "
                "content_hash, byte_length, created_at) VALUES "
                "(:id, 'fixture', 'concurrent-upstream-action', '1', :available, "
                ":location, :digest, 100, :created_at)"
            ),
            {
                "id": evidence_id,
                "available": datetime.combine(protocol.start, time(12), tzinfo=UTC),
                "location": f"memory://sha256/{evidence_hash}",
                "digest": evidence_hash,
                "created_at": COMPLETED_AT - timedelta(hours=1),
            },
        )
        await publisher.execute(
            text(
                "INSERT INTO corporate_actions "
                "(id, evidence_ref_id, instrument_id, source, provider_event_id, "
                "action_type, status, process_date, ex_date, rate, revision, created_at) "
                "VALUES (:id, :evidence, :instrument, 'fixture', "
                "'concurrent-upstream-action', 'stock_split', 'complete', :event_date, "
                ":event_date, 2, '1', :created_at)"
            ),
            {
                "id": action_id,
                "evidence": evidence_id,
                "instrument": INSTRUMENT_ID,
                "event_date": protocol.end,
                "created_at": COMPLETED_AT - timedelta(hours=1),
            },
        )

        async def write() -> None:
            async with factory() as session, session.begin():
                await persist_backtest_result(session, strategy, data, protocol, result, authority)

        writer = asyncio.create_task(write())
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.shield(writer), timeout=0.1)
        await publisher_transaction.commit()
        with pytest.raises(
            ValueError, match="corporate-action collection omits a database-authoritative event"
        ):
            await writer
    finally:
        if publisher_transaction.is_active:
            await publisher_transaction.rollback()
        await publisher.close()


@pytest.mark.asyncio
async def test_writer_requires_every_universe_vintage_selected_by_the_two_clock_frontier(
    migrated_database_url: str,
) -> None:
    strategy, bars, sessions, protocol = _raw_inputs()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    price_authorities = await _seed_upstream_authorities(factory, bars, sessions)
    newer_universe_id = uuid4()
    async with factory() as session, session.begin():
        await session.execute(
            text(
                "INSERT INTO universe_snapshots "
                "(id, key, version, effective_from, source, content_hash, published_at) "
                "VALUES (:id, 'us_liquid', 'us_liquid_v2', :effective, 'fixture', "
                ":digest, NULL)"
            ),
            {
                "id": newer_universe_id,
                "effective": protocol.end,
                "digest": _digest("us-liquid-v2-two-clock"),
            },
        )
        await session.execute(
            text(
                "INSERT INTO universe_members "
                "(id, universe_snapshot_id, instrument_id, inclusion_reason) "
                "VALUES (:id, :universe, :instrument, 'second selected vintage')"
            ),
            {"id": uuid4(), "universe": newer_universe_id, "instrument": INSTRUMENT_ID},
        )
        await session.execute(
            text("UPDATE universe_snapshots SET published_at = :published WHERE id = :id"),
            {
                "id": newer_universe_id,
                "published": datetime.combine(protocol.end, time(20), tzinfo=UTC),
            },
        )
    data = _dataset(bars, sessions, price_authorities)
    result = run_backtest(strategy, data, protocol)
    await _seed_data_snapshot(factory, data, protocol, result)
    authority = BacktestPersistenceAuthority(
        authority_bundle_id=AUTHORITY_BUNDLE_ID,
        data_snapshot_id=DATA_SNAPSHOT_ID,
        source_policy_id=SOURCE_POLICY_ID,
        cost_policy_id=BACKTEST_COST_POLICY_ID,
        completed_at=COMPLETED_AT,
    )

    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="database-authoritative universe version"):
            await persist_backtest_result(session, strategy, data, protocol, result, authority)


@pytest.mark.asyncio
async def test_writer_fails_closed_on_unsupported_complete_corporate_action(
    migrated_database_url: str,
) -> None:
    strategy, bars, sessions, protocol = _raw_inputs()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    price_authorities = await _seed_upstream_authorities(factory, bars, sessions)
    evidence_id = uuid4()
    evidence_hash = _digest("unsupported-name-change")
    async with factory() as session, session.begin():
        await session.execute(
            text(
                "INSERT INTO research_evidence_refs "
                "(id, source, provider_object_id, revision, available_at, location, "
                "content_hash, byte_length, created_at) VALUES "
                "(:id, 'fixture', 'unsupported-name-change', '1', :available, :location, "
                ":digest, 100, :created_at)"
            ),
            {
                "id": evidence_id,
                "available": datetime.combine(protocol.start, time(12), tzinfo=UTC),
                "location": f"memory://sha256/{evidence_hash}",
                "digest": evidence_hash,
                "created_at": COMPLETED_AT - timedelta(hours=1),
            },
        )
        await session.execute(
            text(
                "INSERT INTO corporate_actions "
                "(id, evidence_ref_id, instrument_id, source, provider_event_id, "
                "action_type, status, process_date, ex_date, rate, revision, created_at) "
                "VALUES (:id, :evidence, :instrument, 'fixture', "
                "'unsupported-name-change', 'name_change', 'complete', :event_date, "
                ":event_date, NULL, '1', :created_at)"
            ),
            {
                "id": uuid4(),
                "evidence": evidence_id,
                "instrument": INSTRUMENT_ID,
                "event_date": protocol.end,
                "created_at": COMPLETED_AT - timedelta(hours=1),
            },
        )
    data = _dataset(bars, sessions, price_authorities)
    result = run_backtest(strategy, data, protocol)
    await _seed_data_snapshot(factory, data, protocol, result)
    authority = BacktestPersistenceAuthority(
        authority_bundle_id=AUTHORITY_BUNDLE_ID,
        data_snapshot_id=DATA_SNAPSHOT_ID,
        source_policy_id=SOURCE_POLICY_ID,
        cost_policy_id=BACKTEST_COST_POLICY_ID,
        completed_at=COMPLETED_AT,
    )

    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="unsupported complete corporate action"):
            await persist_backtest_result(session, strategy, data, protocol, result, authority)


@pytest.mark.asyncio
@pytest.mark.parametrize("action_type", ("cash_dividend", "stock_split", "reverse_split"))
async def test_writer_rejects_complete_price_action_without_ex_date(
    migrated_database_url: str,
    action_type: str,
) -> None:
    strategy, bars, sessions, protocol = _raw_inputs()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    price_authorities = await _seed_upstream_authorities(factory, bars, sessions)
    evidence_id = uuid4()
    evidence_hash = _digest((action_type, "missing-ex-date"))
    async with factory() as session, session.begin():
        await session.execute(
            text(
                "INSERT INTO research_evidence_refs "
                "(id, source, provider_object_id, revision, available_at, location, "
                "content_hash, byte_length, created_at) VALUES "
                "(:id, 'fixture', :provider_object_id, '1', :available, :location, "
                ":digest, 100, :created_at)"
            ),
            {
                "id": evidence_id,
                "provider_object_id": f"{action_type}-missing-ex-date",
                "available": datetime.combine(protocol.start, time(12), tzinfo=UTC),
                "location": f"memory://sha256/{evidence_hash}",
                "digest": evidence_hash,
                "created_at": COMPLETED_AT - timedelta(hours=1),
            },
        )
        await session.execute(
            text(
                "INSERT INTO corporate_actions "
                "(id, evidence_ref_id, instrument_id, source, provider_event_id, "
                "action_type, status, process_date, ex_date, rate, revision, created_at) "
                "VALUES (:id, :evidence, :instrument, 'fixture', :provider_event_id, "
                ":action_type, 'complete', :process_date, NULL, 1, '1', :created_at)"
            ),
            {
                "id": uuid4(),
                "evidence": evidence_id,
                "instrument": INSTRUMENT_ID,
                "provider_event_id": f"{action_type}-missing-ex-date",
                "action_type": action_type,
                "process_date": protocol.end,
                "created_at": COMPLETED_AT - timedelta(hours=1),
            },
        )
    data = _dataset(bars, sessions, price_authorities)
    result = run_backtest(strategy, data, protocol)
    await _seed_data_snapshot(factory, data, protocol, result)
    authority = BacktestPersistenceAuthority(
        authority_bundle_id=AUTHORITY_BUNDLE_ID,
        data_snapshot_id=DATA_SNAPSHOT_ID,
        source_policy_id=SOURCE_POLICY_ID,
        cost_policy_id=BACKTEST_COST_POLICY_ID,
        completed_at=COMPLETED_AT,
    )

    async with factory() as session, session.begin():
        with pytest.raises(
            ValueError, match="unsupported complete corporate action prevents exact mapping"
        ):
            await persist_backtest_result(session, strategy, data, protocol, result, authority)


@pytest.mark.asyncio
async def test_concurrent_writer_is_idempotent_and_round_trip_hash_is_reconstructable(
    migrated_database_url: str,
) -> None:
    strategy, bars, sessions, protocol = _raw_inputs()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    price_authorities = await _seed_upstream_authorities(factory, bars, sessions)
    data = _dataset(bars, sessions, price_authorities)
    result = run_backtest(strategy, data, protocol)
    await _seed_data_snapshot(factory, data, protocol, result)
    authority = BacktestPersistenceAuthority(
        authority_bundle_id=AUTHORITY_BUNDLE_ID,
        data_snapshot_id=DATA_SNAPSHOT_ID,
        source_policy_id=SOURCE_POLICY_ID,
        cost_policy_id=BACKTEST_COST_POLICY_ID,
        completed_at=COMPLETED_AT,
    )

    async def write() -> UUID:
        async with factory() as session, session.begin():
            record = await persist_backtest_result(
                session, strategy, data, protocol, result, authority
            )
            return record.id

    first_id, second_id = await asyncio.gather(write(), write())
    third_id = await write()

    forged = result.model_copy(update={"order_outcomes": (), "result_hash": "0" * 64})
    forged = forged.model_copy(
        update={"result_hash": _hash(forged.model_dump(mode="json", exclude={"result_hash"}))}
    )
    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="deterministic engine replay"):
            await persist_backtest_result(session, strategy, data, protocol, forged, authority)

    assert first_id == second_id == third_id
    async with factory() as session:
        run = await session.scalar(select(BacktestRunRecord))
        nav_rows = tuple(
            await session.scalars(
                select(BacktestNavPointRecord).order_by(BacktestNavPointRecord.session)
            )
        )
        counts = (
            await session.scalar(select(func.count()).select_from(StrategyDefinitionRecord)),
            await session.scalar(select(func.count()).select_from(BacktestRunRecord)),
            await session.scalar(select(func.count()).select_from(BacktestNavPointRecord)),
            await session.scalar(select(func.count()).select_from(BacktestFillRecord)),
            await session.scalar(select(func.count()).select_from(BacktestOrderOutcomeRecord)),
            await session.scalar(select(func.count()).select_from(BacktestMetricRecord)),
        )
    assert run is not None
    assert run.data_hash == result.data_hash
    assert run.code_hash == result.code_hash
    assert run.initial_nav == Decimal("10000.123456789013")
    assert _hash(run.result_payload) == run.result_hash == result.result_hash
    assert tuple(row.nav for row in nav_rows) == tuple(point.nav for point in result.nav)
    assert counts == (1, 1, 2, 1, 1, 53)
    assert result.cost_sensitivity.half_cost_terminal_nav > result.nav[-1].nav
    assert result.cost_sensitivity.double_cost_terminal_nav < result.nav[-1].nav

    sealed_append_attempts = (
        (
            "INSERT INTO exchange_calendar_sessions "
            "(calendar_authority_id, session, decision_at, market_open_at, "
            "market_close_at) VALUES "
            "(:owner, :member, :decision_at, :market_open_at, :market_close_at)",
            {
                "owner": CALENDAR_AUTHORITY_ID,
                "member": protocol.end + timedelta(days=1),
                "decision_at": COMPLETED_AT,
                "market_open_at": COMPLETED_AT - timedelta(hours=7),
                "market_close_at": COMPLETED_AT - timedelta(hours=1),
            },
        ),
        (
            "INSERT INTO backtest_universe_authority_members "
            "(collection_id, universe_snapshot_id) VALUES (:owner, :member)",
            {"owner": UNIVERSE_COLLECTION_ID, "member": uuid4()},
        ),
        (
            "INSERT INTO backtest_sector_authority_members "
            "(collection_id, sector_exposure_id) VALUES (:owner, :member)",
            {"owner": SECTOR_COLLECTION_ID, "member": uuid4()},
        ),
        (
            "INSERT INTO backtest_corporate_action_authority_members "
            "(collection_id, corporate_action_id) VALUES (:owner, :member)",
            {"owner": ACTION_COLLECTION_ID, "member": uuid4()},
        ),
        (
            "INSERT INTO backtest_price_authority_members "
            "(bundle_id, price_bar_set_authority_id) VALUES (:owner, :member)",
            {"owner": AUTHORITY_BUNDLE_ID, "member": uuid4()},
        ),
        (
            "INSERT INTO backtest_instrument_authority_members "
            "(bundle_id, instrument_id) VALUES (:owner, :member)",
            {"owner": AUTHORITY_BUNDLE_ID, "member": uuid4()},
        ),
        (
            "INSERT INTO backtest_windows (backtest_run_id) VALUES (:owner)",
            {"owner": first_id},
        ),
        (
            "INSERT INTO backtest_nav_points (backtest_run_id) VALUES (:owner)",
            {"owner": first_id},
        ),
        (
            "INSERT INTO backtest_fills (backtest_run_id) VALUES (:owner)",
            {"owner": first_id},
        ),
        (
            "INSERT INTO backtest_order_outcomes (backtest_run_id) VALUES (:owner)",
            {"owner": first_id},
        ),
        (
            "INSERT INTO backtest_metrics (backtest_run_id) VALUES (:owner)",
            {"owner": first_id},
        ),
    )
    for statement, parameters in sealed_append_attempts:
        async with factory() as session, session.begin():
            with pytest.raises(DBAPIError, match="backtest authority set is sealed"):
                await session.execute(text(statement), parameters)

    frozen_mutations = (
        ("UPDATE data_snapshots SET payload = '{}'::jsonb WHERE id = :id", DATA_SNAPSHOT_ID),
        ("UPDATE sector_exposures SET weight = 0.5 WHERE id = :id", SECTOR_ID),
        ("UPDATE universe_snapshots SET source = 'changed' WHERE id = :id", UNIVERSE_ID),
        (
            "UPDATE universe_members SET inclusion_reason = 'changed' WHERE id = :id",
            UNIVERSE_MEMBER_ID,
        ),
        ("UPDATE corporate_actions SET rate = 0.2 WHERE id = :id", ACTION_ID),
        (
            "UPDATE research_evidence_refs SET location = 'memory://changed' WHERE id = :id",
            ACTION_EVIDENCE_ID,
        ),
        ("UPDATE instruments SET risk_bucket = 'changed' WHERE id = :id", INSTRUMENT_ID),
        (
            "UPDATE price_bar_set_authorities SET content_hash = repeat('f', 64) "
            "WHERE price_series_manifest_id = :id",
            RAW_MANIFEST_ID,
        ),
        ("UPDATE price_series_manifests SET feed = 'changed' WHERE id = :id", RAW_MANIFEST_ID),
        ("UPDATE data_artifacts SET mime_type = 'text/plain' WHERE id = :id", RAW_ARTIFACT_ID),
        (
            "UPDATE price_bar_records SET close = close + 1 WHERE price_series_manifest_id = :id",
            RAW_MANIFEST_ID,
        ),
        ("UPDATE source_policies SET schema_version = 'changed' WHERE id = :id", SOURCE_POLICY_ID),
        (
            "UPDATE cost_policies SET schema_version = 'changed' WHERE id = :id",
            BACKTEST_COST_POLICY_ID,
        ),
    )
    for statement, record_id in frozen_mutations:
        async with factory() as session, session.begin():
            with pytest.raises(DBAPIError, match="backtest upstream authority is frozen"):
                await session.execute(text(statement), {"id": record_id})

    async with factory() as session, session.begin():
        with pytest.raises(DBAPIError, match="published universe membership is immutable"):
            async with session.begin_nested():
                await session.execute(
                    text(
                        "INSERT INTO universe_members "
                        "(id, universe_snapshot_id, instrument_id, inclusion_reason) "
                        "VALUES (:id, :universe, :instrument, 'late append')"
                    ),
                    {"id": uuid4(), "universe": UNIVERSE_ID, "instrument": INSTRUMENT_ID},
                )

    async with factory() as session:
        transaction = await session.begin()
        omitted_evidence_id = uuid4()
        omitted_action_id = uuid4()
        omitted_hash = _digest("omitted-corporate-action")
        await session.execute(
            text(
                "INSERT INTO research_evidence_refs "
                "(id, source, provider_object_id, revision, available_at, location, "
                "content_hash, byte_length) VALUES "
                "(:id, 'fixture', 'omitted-corporate-action', '1', :available, :location, "
                ":digest, 100)"
            ),
            {
                "id": omitted_evidence_id,
                "available": datetime.combine(protocol.start, time(12), tzinfo=UTC),
                "location": f"memory://sha256/{omitted_hash}",
                "digest": omitted_hash,
            },
        )
        await session.execute(
            text(
                "INSERT INTO corporate_actions "
                "(id, evidence_ref_id, instrument_id, source, provider_event_id, action_type, "
                "status, process_date, ex_date, rate, revision) VALUES "
                "(:id, :evidence, :instrument, 'fixture', 'omitted-corporate-action', "
                "'stock_split', 'complete', :event_date, :event_date, 2, '1')"
            ),
            {
                "id": omitted_action_id,
                "evidence": omitted_evidence_id,
                "instrument": INSTRUMENT_ID,
                "event_date": protocol.end,
            },
        )
        replay = await persist_backtest_result(session, strategy, data, protocol, result, authority)
        assert replay.id == first_id
        await transaction.rollback()

    omitted_universe_id = uuid4()
    async with factory() as session, session.begin():
        await session.execute(
            text(
                "INSERT INTO universe_snapshots "
                "(id, key, version, effective_from, source, content_hash, published_at) "
                "VALUES (:id, 'us_liquid', 'us_liquid_v2', :effective, 'fixture', "
                ":digest, NULL)"
            ),
            {
                "id": omitted_universe_id,
                "effective": protocol.end,
                "digest": _digest("omitted-universe"),
            },
        )
        await session.execute(
            text(
                "INSERT INTO universe_members "
                "(id, universe_snapshot_id, instrument_id, inclusion_reason) "
                "VALUES (:id, :universe, :instrument, 'versioned fixture')"
            ),
            {"id": uuid4(), "universe": omitted_universe_id, "instrument": INSTRUMENT_ID},
        )
        await session.execute(
            text("UPDATE universe_snapshots SET published_at = :published WHERE id = :id"),
            {
                "id": omitted_universe_id,
                "published": datetime.combine(protocol.end, time(20), tzinfo=UTC),
            },
        )

    assert await write() == first_id
