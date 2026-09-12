import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, date, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from nexus.integrations.service import IntegrationStatusService
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
    INTRADAY_SOURCE_POLICY_ID,
    INTRADAY_SOURCE_POLICY_V2_ID,
    INTRADAY_SOURCE_POLICY_V3_ID,
    INTRADAY_SOURCE_POLICY_V4_ID,
    QUOTE_SOURCE_POLICY_ID,
    QUOTE_SOURCE_POLICY_V2_ID,
    QUOTE_SOURCE_POLICY_V3_ID,
    SOURCE_POLICY_ID,
    SOURCE_POLICY_V2_ID,
    SOURCE_POLICY_V3_ID,
    load_fosun_market_admission,
    load_intraday_source_policy_v1,
    load_intraday_source_policy_v2,
    load_quote_source_policy_v1,
    load_quote_source_policy_v2,
    load_quote_source_policy_v3,
    load_source_policy_v1,
    load_source_policy_v2,
)
from nexus.settings import EngineSettings
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError

INSTRUMENT_ID = UUID("38df053d-1e87-5938-b279-a1ef5ab4643e")
_EVENT_TIMEOUT_SECONDS = 2.0


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
    try:
        snapshot_id, instrument_id = await _create_draft_universe(factory)
        await _exercise_member_first_lock_order(factory, snapshot_id, instrument_id)

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
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_seal_first_blocks_then_rejects_postpublication_member(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        snapshot_id, instrument_id = await _create_draft_universe(factory)
        await _exercise_seal_first_lock_order(factory, snapshot_id, instrument_id)

        async with factory() as session:
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(UniverseMember)
                    .where(UniverseMember.universe_snapshot_id == snapshot_id)
                )
                == 0
            )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_lock_observer_failure_releases_tasks_sessions_and_database_lock(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        snapshot_id, instrument_id = await _create_draft_universe(factory)

        async def fail_observer(_factory, _backend_pid: int) -> None:
            raise RuntimeError("controlled observer failure")

        with pytest.raises(RuntimeError, match="controlled observer failure"):
            await _exercise_member_first_lock_order(
                factory,
                snapshot_id,
                instrument_id,
                observe_lock=fail_observer,
            )

        assert not {
            task.get_name()
            for task in asyncio.all_tasks()
            if task is not asyncio.current_task()
            and task.get_name().startswith("market-lock-")
            and not task.done()
        }
        async with factory() as session:
            await session.execute(text("SET LOCAL lock_timeout = '250ms'"))
            await session.execute(
                text("UPDATE universe_snapshots SET published_at = :now WHERE id = :id"),
                {"now": datetime(2026, 8, 23, tzinfo=UTC), "id": snapshot_id},
            )
            await session.commit()
    finally:
        await factory.kw["bind"].dispose()


async def _exercise_member_first_lock_order(
    factory,
    snapshot_id: UUID,
    instrument_id: UUID,
    *,
    observe_lock: Callable[[object, int], Awaitable[None]] | None = None,
) -> None:
    member_locked = asyncio.Event()
    release_member = asyncio.Event()
    seal_pid_ready = asyncio.Event()
    seal_pid: int | None = None
    tasks: list[asyncio.Task[None]] = []
    observer = observe_lock or _assert_backend_waits_on_lock

    async def member_first() -> None:
        async with factory() as session:
            try:
                await session.execute(
                    text(
                        "INSERT INTO universe_members "
                        "(id, universe_snapshot_id, instrument_id, inclusion_reason) "
                        "VALUES (:id, :snapshot_id, :instrument_id, 'pre-seal')"
                    ),
                    {
                        "id": uuid4(),
                        "snapshot_id": snapshot_id,
                        "instrument_id": instrument_id,
                    },
                )
                member_locked.set()
                await asyncio.wait_for(release_member.wait(), timeout=_EVENT_TIMEOUT_SECONDS)
                await session.commit()
            except BaseException:
                await session.rollback()
                raise

    async def seal_second() -> None:
        nonlocal seal_pid
        await asyncio.wait_for(member_locked.wait(), timeout=_EVENT_TIMEOUT_SECONDS)
        async with factory() as session:
            try:
                seal_pid = await session.scalar(text("SELECT pg_backend_pid()"))
                seal_pid_ready.set()
                await session.execute(
                    text("UPDATE universe_snapshots SET published_at = :now WHERE id = :id"),
                    {"now": datetime(2026, 8, 23, tzinfo=UTC), "id": snapshot_id},
                )
                await session.commit()
            except BaseException:
                await session.rollback()
                raise

    tasks.extend(
        (
            asyncio.create_task(member_first(), name="market-lock-member-first"),
            asyncio.create_task(seal_second(), name="market-lock-seal-second"),
        )
    )
    try:
        await asyncio.wait_for(seal_pid_ready.wait(), timeout=_EVENT_TIMEOUT_SECONDS)
        if seal_pid is None:
            raise AssertionError("seal backend PID was not captured")
        await asyncio.wait_for(observer(factory, seal_pid), timeout=_EVENT_TIMEOUT_SECONDS)
        release_member.set()
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=_EVENT_TIMEOUT_SECONDS)
    finally:
        release_member.set()
        await _cancel_and_gather(tasks)


async def _exercise_seal_first_lock_order(
    factory,
    snapshot_id: UUID,
    instrument_id: UUID,
    *,
    observe_lock: Callable[[object, int], Awaitable[None]] | None = None,
) -> None:
    member_pid_ready = asyncio.Event()
    member_pid: int | None = None
    tasks: list[asyncio.Task[None]] = []
    observer = observe_lock or _assert_backend_waits_on_lock

    async with factory() as seal_session:
        try:
            await seal_session.execute(
                text("UPDATE universe_snapshots SET published_at = :now WHERE id = :id"),
                {"now": datetime(2026, 8, 23, tzinfo=UTC), "id": snapshot_id},
            )

            async def member_second() -> None:
                nonlocal member_pid
                async with factory() as session:
                    try:
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
                    except BaseException:
                        await session.rollback()
                        raise

            tasks.append(asyncio.create_task(member_second(), name="market-lock-member-second"))
            await asyncio.wait_for(member_pid_ready.wait(), timeout=_EVENT_TIMEOUT_SECONDS)
            if member_pid is None:
                raise AssertionError("member backend PID was not captured")
            await asyncio.wait_for(observer(factory, member_pid), timeout=_EVENT_TIMEOUT_SECONDS)
            await seal_session.commit()
            results = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=_EVENT_TIMEOUT_SECONDS,
            )
            if not results or not isinstance(results[0], DBAPIError):
                raise AssertionError("post-publication member was not rejected")
            if "published universe membership is immutable" not in str(results[0]):
                raise AssertionError("member rejection did not come from publication guard")
        finally:
            await seal_session.rollback()
            await _cancel_and_gather(tasks)


async def _cancel_and_gather(tasks: list[asyncio.Task[None]]) -> None:
    for task in tasks:
        if not task.done():
            task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


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
async def test_canonical_source_policies_are_loadable_and_database_immutable(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        policy = await load_source_policy_v1(session, instrument_tick_size=None)
        quote_policy = await load_quote_source_policy_v1(session)
        intraday_policy = await load_intraday_source_policy_v1(session)
        decision_time = datetime(2026, 8, 23, tzinfo=UTC)
        v2_policy = await load_source_policy_v2(
            session,
            instrument_tick_size=None,
            decision_time=decision_time,
        )
        v2_quote_policy = await load_quote_source_policy_v2(
            session,
            decision_time=decision_time,
        )
        v3_quote_policy = await load_quote_source_policy_v3(
            session,
            decision_time=decision_time,
        )
        v2_intraday_policy = await load_intraday_source_policy_v2(
            session,
            decision_time=decision_time,
        )
        records = set(
            (
                await session.execute(
                    text("SELECT key,id FROM source_policies ORDER BY key,version")
                )
            ).all()
        )
        assert policy.record_id == SOURCE_POLICY_ID
        assert quote_policy.record_id == QUOTE_SOURCE_POLICY_ID
        assert intraday_policy.record_id == INTRADAY_SOURCE_POLICY_ID
        assert v2_policy.record_id == SOURCE_POLICY_V2_ID
        assert v2_quote_policy.record_id == QUOTE_SOURCE_POLICY_V2_ID
        assert v3_quote_policy.record_id == QUOTE_SOURCE_POLICY_V3_ID
        assert v2_intraday_policy.record_id == INTRADAY_SOURCE_POLICY_V2_ID
        assert records == {
            ("intraday_monitor", INTRADAY_SOURCE_POLICY_ID),
            ("intraday_monitor", INTRADAY_SOURCE_POLICY_V2_ID),
            ("intraday_monitor", INTRADAY_SOURCE_POLICY_V3_ID),
            ("intraday_monitor", INTRADAY_SOURCE_POLICY_V4_ID),
            ("market_price", SOURCE_POLICY_ID),
            ("market_price", SOURCE_POLICY_V2_ID),
            ("market_price", SOURCE_POLICY_V3_ID),
            ("market_quote", QUOTE_SOURCE_POLICY_ID),
            ("market_quote", QUOTE_SOURCE_POLICY_V2_ID),
            ("market_quote", QUOTE_SOURCE_POLICY_V3_ID),
            (
                "portfolio_scenario_cost_analysis",
                UUID("7733d99e-1594-5f1e-87ea-e5cc23575159"),
            ),
        }
        for _, policy_id in records:
            for statement in (
                "UPDATE source_policies SET payload = '{}'::jsonb WHERE id = :id",
                "DELETE FROM source_policies WHERE id = :id",
            ):
                with pytest.raises(DBAPIError, match="published source policy is immutable"):
                    await session.execute(text(statement), {"id": policy_id})
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
            payload={
                "operation": "fosun_get_bars",
                "market_scope": "US",
                "delivery_mode": "unknown",
            },
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
async def test_fosun_quote_and_bars_capabilities_are_operation_scoped(
    migrated_database_url: str,
) -> None:
    """The newest capability of another operation must not grant or hide authority."""
    now = datetime(2026, 8, 23, 12, tzinfo=UTC)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        session.add_all(
            [
                ProviderCapability(
                    provider="fosun",
                    capability="READ_ONLY",
                    contract_hash="a" * 64,
                    expires_at=now + timedelta(hours=1),
                    schema_version="1",
                    payload={
                        "operation": "fosun_get_quotes",
                        "market_scope": "US",
                        "delivery_mode": "unknown",
                    },
                    created_at=now - timedelta(hours=2),
                ),
                ProviderCapability(
                    provider="fosun",
                    capability="READ_ONLY",
                    contract_hash="a" * 64,
                    expires_at=now + timedelta(hours=1),
                    schema_version="1",
                    payload={
                        "operation": "fosun_get_bars",
                        "market_scope": "US",
                        "delivery_mode": "unknown",
                    },
                    created_at=now - timedelta(hours=1),
                ),
                IntegrationStatus(
                    provider="fosun",
                    status="read_only",
                    admitted_contract_hash="a" * 64,
                    mode_changed_at=now - timedelta(hours=1),
                ),
            ]
        )
        await session.commit()

        service = IntegrationStatusService(session, now)
        generic = await service.capability("fosun")
        quote_scoped = await service.capability(
            "fosun", operation="fosun_get_quotes", market_scope="US"
        )
        bars_scoped = await service.capability(
            "fosun", operation="fosun_get_bars", market_scope="US"
        )
        quote_admission = await load_fosun_market_admission(
            session,
            decision_time=now,
            operation="fosun_get_quotes",
            market_scope="US",
        )
        bars_admission = await load_fosun_market_admission(
            session,
            decision_time=now,
            operation="fosun_get_bars",
            market_scope="US",
        )

    assert generic.contract_hash == "a" * 64
    assert quote_scoped.contract_hash == "a" * 64
    assert bars_scoped.contract_hash == "a" * 64
    assert quote_admission is not None
    assert quote_admission.operation == "fosun_get_quotes"
    assert bars_admission is not None
    assert bars_admission.operation == "fosun_get_bars"


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
                payload={
                    "operation": "fosun_get_bars",
                    "market_scope": "US",
                    "delivery_mode": "unknown",
                },
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
