from datetime import UTC, date, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from nexus.persistence.models.data import ProviderCapability
from nexus.persistence.models.market import (
    Instrument,
    InstrumentAlias,
    UniverseMember,
    UniverseSnapshot,
)
from nexus.persistence.models.runtime import IntegrationStatus
from nexus.persistence.session import session_factory
from nexus.quality.source_policy import (
    SOURCE_POLICY_ID,
    load_fosun_market_admission,
    load_source_policy_v1,
)
from nexus.settings import EngineSettings
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError

INSTRUMENT_ID = UUID("38df053d-1e87-5938-b279-a1ef5ab4643e")


@pytest.mark.asyncio
async def test_ticker_alias_has_a_valid_dated_interval(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        session.add(
            Instrument(
                id=INSTRUMENT_ID,
                asset_type="common_stock",
                exchange="XNYS",
                currency="USD",
                tick_size=None,
                risk_bucket="EQUITY",
            )
        )
        await session.commit()
        session.add(
            InstrumentAlias(
                instrument_id=INSTRUMENT_ID,
                symbol="AAPL",
                alias_type="ticker",
                valid_from=date(2026, 8, 2),
                valid_to=date(2026, 8, 1),
            )
        )
        with pytest.raises(IntegrityError, match="instrument_alias_valid_range"):
            await session.commit()


@pytest.mark.asyncio
async def test_internal_instrument_id_cannot_be_rewritten(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        session.add(
            Instrument(
                id=INSTRUMENT_ID,
                asset_type="common_stock",
                exchange="XNYS",
                currency="USD",
                tick_size=None,
                risk_bucket="EQUITY",
            )
        )
        await session.commit()
        with pytest.raises(DBAPIError, match="instrument id is immutable"):
            await session.execute(
                text("UPDATE instruments SET id = :replacement WHERE id = :instrument_id"),
                {"replacement": uuid4(), "instrument_id": INSTRUMENT_ID},
            )


@pytest.mark.asyncio
async def test_published_universe_snapshot_cannot_be_rewritten(
    migrated_database_url: str,
) -> None:
    snapshot_id = UUID("c32df4bf-f6a3-5a18-8e20-1c4fb9c39d38")
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        session.add(
            UniverseSnapshot(
                id=snapshot_id,
                key="us_liquid",
                version="us_liquid_v1",
                effective_from=date(2026, 1, 1),
                source="versioned_fixture",
                content_hash="a" * 64,
            )
        )
        await session.commit()
        with pytest.raises(DBAPIError, match="universe snapshot is immutable"):
            await session.execute(
                text("UPDATE universe_snapshots SET version = 'us_liquid_v2' WHERE id = :id"),
                {"id": snapshot_id},
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["insert", "update", "delete"])
async def test_published_universe_rejects_every_membership_mutation(
    migrated_database_url: str, mutation: str
) -> None:
    snapshot_id = uuid4()
    first_id, second_id = uuid4(), uuid4()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        session.add_all(
            [
                Instrument(
                    id=instrument_id,
                    asset_type="common_stock",
                    exchange="XNYS",
                    currency="USD",
                    tick_size=None,
                    risk_bucket="EQUITY",
                )
                for instrument_id in (first_id, second_id)
            ]
        )
        session.add(
            UniverseSnapshot(
                id=snapshot_id,
                key="test",
                version=str(snapshot_id),
                effective_from=date(2026, 1, 1),
                source="fixture",
                content_hash=snapshot_id.hex.ljust(64, "0"),
                published_at=None,
            )
        )
        await session.flush()
        session.add(
            UniverseMember(
                universe_snapshot_id=snapshot_id, instrument_id=first_id, inclusion_reason="seed"
            )
        )
        await session.commit()
        await session.execute(
            text("UPDATE universe_snapshots SET published_at = :published_at WHERE id = :id"),
            {"published_at": datetime(2026, 8, 23, tzinfo=UTC), "id": snapshot_id},
        )
        await session.commit()
        statements = {
            "insert": (
                "INSERT INTO universe_members "
                "(id, universe_snapshot_id, instrument_id, inclusion_reason) "
                "VALUES (:member_id, :snapshot_id, :second_id, 'late')",
                {"member_id": uuid4(), "snapshot_id": snapshot_id, "second_id": second_id},
            ),
            "update": (
                "UPDATE universe_members SET instrument_id = :second_id "
                "WHERE universe_snapshot_id = :snapshot_id",
                {"snapshot_id": snapshot_id, "second_id": second_id},
            ),
            "delete": (
                "DELETE FROM universe_members WHERE universe_snapshot_id = :snapshot_id",
                {"snapshot_id": snapshot_id},
            ),
        }
        statement, parameters = statements[mutation]
        with pytest.raises(DBAPIError, match="published universe membership is immutable"):
            await session.execute(text(statement), parameters)


@pytest.mark.asyncio
async def test_canonical_source_policy_is_single_loadable_and_database_immutable(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        policy = await load_source_policy_v1(session, instrument_tick_size=None)
        records = list(
            await session.scalars(text("SELECT id FROM source_policies WHERE key = 'market_price'"))
        )
        assert policy.record_id == SOURCE_POLICY_ID
        assert records == [SOURCE_POLICY_ID]
        for statement in (
            "UPDATE source_policies SET payload = '{}'::jsonb WHERE id = :id",
            "DELETE FROM source_policies WHERE id = :id",
        ):
            with pytest.raises(DBAPIError, match="published source policy is immutable"):
                await session.execute(text(statement), {"id": SOURCE_POLICY_ID})
            await session.rollback()


@pytest.mark.asyncio
async def test_fosun_admission_is_loaded_from_matching_task5_authority(
    migrated_database_url: str,
) -> None:
    now = datetime(2026, 8, 23, 12, tzinfo=UTC)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        capability = ProviderCapability(
            provider="fosun",
            capability="READ_ONLY",
            contract_hash="a" * 64,
            expires_at=now + timedelta(hours=1),
            schema_version="1",
            payload={},
            created_at=now - timedelta(hours=2),
        )
        session.add(capability)
        session.add(
            IntegrationStatus(
                provider="fosun",
                status="read_only",
                admitted_contract_hash="a" * 64,
                mode_changed_at=now - timedelta(hours=1),
            )
        )
        await session.commit()
        admitted = await load_fosun_market_admission(
            session, decision_time=now, operation="fosun_get_bars", market_scope="US"
        )
        denied_scope = await load_fosun_market_admission(
            session, decision_time=now, operation="fosun_get_bars", market_scope="HK"
        )
        assert admitted is not None
        assert admitted.contract_hash == "a" * 64
        assert denied_scope is None
        denied_operation = await load_fosun_market_admission(
            session,
            decision_time=now,
            operation="fosun_sync_portfolio",
            market_scope="US",
        )
        assert denied_operation is None
        await session.execute(
            text(
                "UPDATE integration_status SET capability_invalidated_at = :invalidated_at "
                "WHERE provider = 'fosun'"
            ),
            {"invalidated_at": now - timedelta(hours=1)},
        )
        await session.commit()
        assert (
            await load_fosun_market_admission(
                session,
                decision_time=now,
                operation="fosun_get_bars",
                market_scope="US",
            )
            is None
        )
