"""Synthetic frozen feature reads, deliberately separate from forward permission."""

from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.market.price_bar_authority import finalize_price_bar_set
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.data import DataArtifact, PriceBarRecord, PriceSeriesManifest
from nexus.persistence.models.market import (
    Instrument,
    SectorExposure,
    UniverseMember,
    UniverseSnapshot,
)
from nexus.quality.source_policy import SOURCE_POLICY_V2_ID
from nexus.risk.authority import RiskAuthorityInput
from nexus.strategy.snapshot_reader import (
    FrozenStrategyReferences,
    StrategySnapshotUnavailable,
    load_unranked_strategy_features,
    require_forward_adjusted_authority,
)
from sqlalchemy import event
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

ID, CALENDAR, UNIVERSE, SECTOR = (UUID(int=n) for n in range(5001, 5005))
DAY = date(2026, 8, 28)
CUTOFF = datetime.combine(DAY, time(21), UTC)


async def seed(factory):
    days = []
    day = DAY
    while len(days) < 200:
        if day.weekday() < 5:
            days.append(day)
        day -= timedelta(days=1)
    days.reverse()
    first = datetime.combine(days[0], time(20), UTC)
    ids = []
    async with factory.begin() as session:
        session.add(
            Instrument(
                id=ID,
                asset_type="common_stock",
                exchange="NASDAQ",
                currency="USD",
                tick_size=Decimal("0.01"),
                risk_bucket="technology",
            )
        )
        session.add(
            ExchangeCalendarAuthorityRecord(
                id=CALENDAR,
                exchange="XNYS",
                version="snapshot-fixture",
                source="fixture",
                range_start=days[0],
                range_end=date(2026, 8, 31),
                query_parameters={},
                completeness_status="complete",
                content_hash="c" * 64,
                published_at=first,
            )
        )
        session.add(
            UniverseSnapshot(
                id=UNIVERSE,
                key="snapshot-fixture",
                version="v1",
                effective_from=days[0],
                source="fixture",
                content_hash="d" * 64,
                published_at=None,
            )
        )
        await session.flush()
        session.add(
            UniverseMember(
                id=UUID(int=5010),
                universe_snapshot_id=UNIVERSE,
                instrument_id=ID,
                inclusion_reason="SYNTHETIC",
            )
        )
        session.add(
            SectorExposure(
                id=SECTOR,
                instrument_id=ID,
                sector_code="information_technology",
                weight=Decimal(1),
                as_of=days[0],
                source_url="memory://sector",
                content_hash="e" * 64,
                synthetic=False,
                reviewed=True,
            )
        )
        session.add_all(
            ExchangeCalendarSessionRecord(
                calendar_authority_id=CALENDAR,
                session=day,
                decision_at=datetime.combine(day, time(21), UTC),
            )
            for day in days + [date(2026, 8, 31)]
        )
        await session.flush()
        from sqlalchemy import text

        await session.execute(
            text("UPDATE universe_snapshots SET published_at=:at WHERE id=:id"),
            {"at": first, "id": UNIVERSE},
        )
        for n, adjustment in enumerate(("raw", "all"), 1):
            manifest_id, artifact_id = UUID(int=5100 + n), UUID(int=5200 + n)
            session.add(
                DataArtifact(
                    id=artifact_id,
                    uri=f"memory://fixture/{n}",
                    sha256=str(n) * 64,
                    byte_size=100,
                    mime_type="application/vnd.apache.parquet",
                    schema_version="price_bars_v1",
                    source="fixture",
                    observed_at=first,
                    available_at=CUTOFF,
                    received_at=CUTOFF,
                )
            )
            await session.flush()
            manifest = PriceSeriesManifest(
                id=manifest_id,
                data_artifact_id=artifact_id,
                instrument=str(ID),
                source="fixture",
                feed="fixture",
                delivery_mode="fixture",
                adjustment=adjustment,
                interval="1Day",
                start_at=first,
                end_at=datetime.combine(DAY, time(20), UTC),
                row_count=len(days),
            )
            session.add(manifest)
            await session.flush()
            session.add_all(
                PriceBarRecord(
                    id=UUID(int=10000 + n * 1000 + i),
                    price_series_manifest_id=manifest_id,
                    instrument_id=ID,
                    session=day,
                    open=Decimal(100 + i),
                    high=Decimal(100 + i),
                    low=Decimal(100 + i),
                    close=Decimal(100 + i),
                    volume=Decimal(1000000),
                    observed_at=datetime.combine(day, time(20), UTC),
                    available_at=datetime.combine(day, time(21), UTC),
                    received_at=datetime.combine(day, time(21), UTC),
                    revision="1",
                    quality_flags=[],
                )
                for i, day in enumerate(days)
            )
            await session.flush()
            finalized = await finalize_price_bar_set(session, manifest)
            assert finalized is not None
            ids.append(finalized[0].id)
    return FrozenStrategyReferences(
        market=RiskAuthorityInput(
            decision_session=DAY,
            execution_session=date(2026, 8, 31),
            decision_time=CUTOFF,
            source_policy_id=SOURCE_POLICY_V2_ID,
            calendar_authority_id=CALENDAR,
            universe_snapshot_id=UNIVERSE,
            quality_report_id=UUID(int=5999),
        ),
        raw_bar_set_ids=(ids[0],),
        adjusted_bar_set_ids=(ids[1],),
        sector_exposure_ids=(SECTOR,),
    )


@pytest.mark.asyncio
async def test_frozen_feature_reader_and_admission_remain_separate(migrated_database_url):
    engine = create_async_engine(migrated_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        references = await seed(factory)
        statements = []

        def record(conn, cursor, statement, parameters, context, executemany):
            statements.append(statement.lstrip().split(None, 1)[0].upper())

        event.listen(engine.sync_engine, "before_cursor_execute", record)
        async with factory.begin() as session:
            result = await load_unranked_strategy_features(
                session, references=references, current_positions=(ID,)
            )
            assert len(result.snapshot.instruments[0].closes) == 200
            assert result.snapshot.instruments[0].dollar_volumes[-1] == Decimal(299000000)
            with pytest.raises(ValueError, match="STRATEGY_RANK_INPUT_REQUIRED"):
                _ = result.snapshot.position_rank_states
            with pytest.raises(StrategySnapshotUnavailable, match="ADJUSTED_AUTHORITY_UNAVAILABLE"):
                require_forward_adjusted_authority(references)
            missing = references.model_copy(update={"raw_bar_set_ids": (UUID(int=9999),)})
            with pytest.raises(StrategySnapshotUnavailable, match="PRICE_REFERENCE_MISSING"):
                await load_unranked_strategy_features(
                    session, references=missing, current_positions=()
                )
            mismatched = references.model_copy(
                update={"raw_bar_set_ids": references.adjusted_bar_set_ids}
            )
            with pytest.raises(StrategySnapshotUnavailable, match="PRICE_PROTOCOL_MISMATCH"):
                await load_unranked_strategy_features(
                    session, references=mismatched, current_positions=()
                )
            late = references.model_copy(
                update={
                    "market": references.market.model_copy(
                        update={"decision_time": CUTOFF - timedelta(seconds=1)}
                    )
                }
            )
            with pytest.raises(StrategySnapshotUnavailable, match="FACT_NOT_VISIBLE"):
                await load_unranked_strategy_features(
                    session, references=late, current_positions=()
                )
        assert set(statements) == {"SELECT"}
    finally:
        await engine.dispose()
