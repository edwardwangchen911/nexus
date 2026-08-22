import asyncio
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
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
from nexus.providers.base import PriceBar, PriceSeries
from nexus.quality.source_policy import (
    SOURCE_POLICY_ID,
    FosunMarketAdmission,
    SourcePolicy,
    load_fosun_market_admission,
    load_source_policy_v1,
    select_price_series,
)
from nexus.settings import EngineSettings
from pydantic import ValidationError
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError

INSTRUMENT_ID = UUID("38df053d-1e87-5938-b279-a1ef5ab4643e")


def _selection_series(source: str, *, contract_hash: str | None = None) -> PriceSeries:
    decision_time = datetime(2026, 8, 23, 12, tzinfo=UTC)
    price = Decimal("100")
    return PriceSeries(
        source=source,
        feed="authorized" if source == "fosun" else "sip",
        adjustment="all",
        interval="1Day",
        mode="read_only" if source == "fosun" else "delayed",
        quality="PASS",
        contract_hash=contract_hash,
        bars=(
            PriceBar(
                instrument_id=INSTRUMENT_ID,
                session=date(2026, 8, 23),
                open=price,
                high=price,
                low=price,
                close=price,
                volume=Decimal("100"),
                available_at=decision_time,
            ),
        ),
    )


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
async def test_member_first_blocks_seal_then_commits_exactly_one_preseal_member(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    snapshot_id, instrument_id = await _create_draft_universe(factory)
    member_locked = asyncio.Event()
    release_member = asyncio.Event()
    seal_pid_ready = asyncio.Event()
    seal_pid: int | None = None

    async def member_first() -> None:
        async with factory() as session:
            await session.execute(
                text(
                    "INSERT INTO universe_members "
                    "(id, universe_snapshot_id, instrument_id, inclusion_reason) "
                    "VALUES (:id, :snapshot_id, :instrument_id, 'pre-seal')"
                ),
                {"id": uuid4(), "snapshot_id": snapshot_id, "instrument_id": instrument_id},
            )
            member_locked.set()
            await release_member.wait()
            await session.commit()

    async def seal_second() -> None:
        nonlocal seal_pid
        await member_locked.wait()
        async with factory() as session:
            seal_pid = await session.scalar(text("SELECT pg_backend_pid()"))
            seal_pid_ready.set()
            await session.execute(
                text("UPDATE universe_snapshots SET published_at = :now WHERE id = :id"),
                {"now": datetime(2026, 8, 23, tzinfo=UTC), "id": snapshot_id},
            )
            await session.commit()

    member_task = asyncio.create_task(member_first())
    seal_task = asyncio.create_task(seal_second())
    await seal_pid_ready.wait()
    assert seal_pid is not None
    await _assert_backend_waits_on_lock(factory, seal_pid)
    release_member.set()
    await asyncio.gather(member_task, seal_task)

    async with factory() as session:
        assert (
            await session.scalar(
                select(func.count())
                .select_from(UniverseMember)
                .where(UniverseMember.universe_snapshot_id == snapshot_id)
            )
            == 1
        )
        assert (
            await session.scalar(
                select(UniverseSnapshot.published_at).where(UniverseSnapshot.id == snapshot_id)
            )
            is not None
        )


@pytest.mark.asyncio
async def test_seal_first_blocks_then_rejects_postpublication_member(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    snapshot_id, instrument_id = await _create_draft_universe(factory)
    member_pid_ready = asyncio.Event()
    member_pid: int | None = None

    async with factory() as seal_session:
        await seal_session.execute(
            text("UPDATE universe_snapshots SET published_at = :now WHERE id = :id"),
            {"now": datetime(2026, 8, 23, tzinfo=UTC), "id": snapshot_id},
        )

        async def member_second() -> None:
            nonlocal member_pid
            async with factory() as session:
                member_pid = await session.scalar(text("SELECT pg_backend_pid()"))
                member_pid_ready.set()
                await session.execute(
                    text(
                        "INSERT INTO universe_members "
                        "(id, universe_snapshot_id, instrument_id, inclusion_reason) "
                        "VALUES (:id, :snapshot_id, :instrument_id, 'post-seal')"
                    ),
                    {
                        "id": uuid4(),
                        "snapshot_id": snapshot_id,
                        "instrument_id": instrument_id,
                    },
                )
                await session.commit()

        member_task = asyncio.create_task(member_second())
        await member_pid_ready.wait()
        assert member_pid is not None
        await _assert_backend_waits_on_lock(factory, member_pid)
        await seal_session.commit()
        with pytest.raises(DBAPIError, match="published universe membership is immutable"):
            await member_task

    async with factory() as session:
        assert (
            await session.scalar(
                select(func.count())
                .select_from(UniverseMember)
                .where(UniverseMember.universe_snapshot_id == snapshot_id)
            )
            == 0
        )


async def _create_draft_universe(factory) -> tuple[UUID, UUID]:
    snapshot_id, instrument_id = uuid4(), uuid4()
    async with factory() as session:
        session.add(
            Instrument(
                id=instrument_id,
                asset_type="common_stock",
                exchange="XNYS",
                currency="USD",
                tick_size=None,
                risk_bucket="EQUITY",
            )
        )
        session.add(
            UniverseSnapshot(
                id=snapshot_id,
                key="concurrency",
                version=str(snapshot_id),
                effective_from=date(2026, 1, 1),
                source="fixture",
                content_hash=snapshot_id.hex.ljust(64, "0"),
                published_at=None,
            )
        )
        await session.commit()
    return snapshot_id, instrument_id


async def _assert_backend_waits_on_lock(factory, backend_pid: int) -> None:
    for _ in range(100):
        async with factory() as observer:
            wait_type = await observer.scalar(
                text("SELECT wait_event_type FROM pg_stat_activity WHERE pid = :pid"),
                {"pid": backend_pid},
            )
        if wait_type == "Lock":
            return
        await asyncio.sleep(0.01)
    pytest.fail(f"backend {backend_pid} did not observably wait on a PostgreSQL lock")


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
async def test_source_policy_requires_loader_attestation_bound_to_every_field(
    migrated_database_url: str,
) -> None:
    decision_time = datetime(2026, 8, 23, 12, tzinfo=UTC)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        loaded = await load_source_policy_v1(session, instrument_tick_size=Decimal("0.01"))

    candidate = _selection_series("alpaca")
    assert select_price_series((candidate,), loaded, decision_time).selected == candidate
    with pytest.raises(ValidationError):
        SourcePolicy(**loaded.model_dump(), priority=("fixture",))  # type: ignore[call-arg]
    direct = SourcePolicy(**loaded.model_dump())
    constructed = SourcePolicy.model_construct(**loaded.model_dump())
    modified = tuple(
        loaded.model_copy(update=update)
        for update in (
            {"record_id": uuid4()},
            {"key": "other"},
            {"version": "source_policy_v2"},
            {"schema_version": "2"},
            {"payload_sha256": "0" * 64},
            {"instrument_tick_size": Decimal("1")},
        )
    )

    for forged in (direct, constructed, *modified):
        with pytest.raises(ValueError, match="loader attestation"):
            select_price_series((candidate,), forged, decision_time)


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
            payload={"operation": "fosun_get_bars", "market_scope": "US"},
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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"operation": "fosun_sync_portfolio", "market_scope": "US"},
        {"operation": "fosun_get_bars", "market_scope": "HK"},
    ],
)
async def test_fosun_admission_rejects_missing_or_wrong_capability_scope(
    migrated_database_url: str,
    payload: dict[str, str],
) -> None:
    now = datetime(2026, 8, 23, 12, tzinfo=UTC)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        session.add(
            ProviderCapability(
                provider="fosun",
                capability="READ_ONLY",
                contract_hash="a" * 64,
                expires_at=now + timedelta(hours=1),
                schema_version="1",
                payload=payload,
                created_at=now - timedelta(hours=2),
            )
        )
        session.add(
            IntegrationStatus(
                provider="fosun",
                status="read_only",
                admitted_contract_hash="a" * 64,
                mode_changed_at=now - timedelta(hours=1),
            )
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


@pytest.mark.asyncio
async def test_fosun_admission_expiring_at_decision_time_is_invalid(
    migrated_database_url: str,
) -> None:
    now = datetime(2026, 8, 23, 12, tzinfo=UTC)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        session.add(
            ProviderCapability(
                provider="fosun",
                capability="READ_ONLY",
                contract_hash="a" * 64,
                expires_at=now,
                schema_version="1",
                payload={"operation": "fosun_get_bars", "market_scope": "US"},
                created_at=now - timedelta(hours=2),
            )
        )
        session.add(
            IntegrationStatus(
                provider="fosun",
                status="read_only",
                admitted_contract_hash="a" * 64,
                mode_changed_at=now - timedelta(hours=1),
            )
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


@pytest.mark.asyncio
async def test_fosun_admission_requires_loader_attestation_bound_to_every_field(
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
            payload={"operation": "fosun_get_bars", "market_scope": "US"},
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
        loaded = await load_fosun_market_admission(
            session,
            decision_time=now,
            operation="fosun_get_bars",
            market_scope="US",
        )

    assert loaded is not None
    direct = FosunMarketAdmission(**loaded.model_dump())
    constructed = FosunMarketAdmission.model_construct(**loaded.model_dump())
    modified = tuple(
        loaded.model_copy(update=update)
        for update in (
            {"capability_id": uuid4()},
            {"capability_schema_version": "2"},
            {"capability_created_at": now},
            {"status_id": uuid4()},
            {"status_version": loaded.status_version + 1},
            {"capability_invalidated_at": now},
            {"admitted_contract_hash": "b" * 64},
            {"provider": "alpaca"},
            {"mode": "fixture"},
            {"operation": "fosun_sync_portfolio"},
            {"market_scope": "HK"},
            {"contract_hash": "b" * 64},
            {"expires_at": now + timedelta(days=365)},
        )
    )
    candidates = (
        _selection_series("fosun", contract_hash="a" * 64),
        _selection_series("alpaca"),
    )

    assert (
        select_price_series(
            candidates,
            await _load_policy(migrated_database_url),
            now,
            fosun_admission=loaded,
        ).selected
        == candidates[0]
    )
    for forged in (direct, constructed, *modified):
        result = select_price_series(
            candidates,
            await _load_policy(migrated_database_url),
            now,
            fosun_admission=forged,
        )
        assert result.selected == candidates[1]


async def _load_policy(database_url: str) -> SourcePolicy:
    factory = session_factory(EngineSettings(database_url=database_url))
    async with factory() as session:
        return await load_source_policy_v1(session, instrument_tick_size=Decimal("0.01"))
