"""Real-PostgreSQL tests for canonical price-Bar set authority."""

import asyncio
from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from helpers.price_bar_authority import (
    AVAILABLE_AT,
    INSTRUMENT_ID,
    MANIFEST_ID,
    OBSERVED_AT,
    RECEIVED_AT,
    clean_bar,
    insert_valid_authority_from_prepare,
    seed_price_candidate,
    seed_price_candidate_in_reverse,
)
from nexus.agents.price_authority import freeze_price_authority
from nexus.domain.ids import new_uuid7
from nexus.market.price_bar_authority import finalize_price_bar_set
from nexus.persistence.models.agents import AgentDataSnapshotEvidenceRecord
from nexus.persistence.models.data import (
    DataSnapshot,
    PriceBarSetAuthority,
    PriceSeriesManifest,
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

EXPECTED_CANONICAL = (
    '{"bars": [["018f75b4-4d86-7c36-bc4c-cd71f2f64520", "2026-08-24", '
    "101.000000000000, 103.000000000000, 100.000000000000, 102.000000000000, "
    '1200000.000000000000, "2026-08-24T20:00:00.000000Z", '
    '"2026-08-24T20:15:00.000000Z", "2026-08-24T20:15:01.000000Z", "1", []]], '
    '"schema": "price_bar_set_sha256_v1", "manifest": {"feed": "sip", '
    '"source": "alpaca", "interval": "1Day", "adjustment": "all", '
    '"instrument": "018f75b4-4d86-7c36-bc4c-cd71f2f64520", '
    '"delivery_mode": "delayed"}}'
)
EXPECTED_HASH = "a7ff0e567da70d9323da75fffa2e1258f258360e12050f0147e40139585c2cc6"
MUTATED_MANIFEST_ID = UUID("018f75b4-4d86-7c36-bc4c-cd71f2f64522")
SECOND_MUTATED_MANIFEST_ID = UUID("018f75b4-4d86-7c36-bc4c-cd71f2f64523")
OTHER_INSTRUMENT_ID = UUID("018f75b4-4d86-7c36-bc4c-cd71f2f64524")
FORGED_POLICY_ID = UUID("018f75b4-4d86-7c36-bc4c-cd71f2f64525")
SOURCE_POLICY_ID = UUID("3c60888d-e92d-5e04-913c-7d427004cfef")
SELECTION_SNAPSHOT_ID = UUID("018f75b4-4d86-7c36-bc4c-cd71f2f64526")

CANONICAL_MUTATIONS = (
    ({"source": "fosun", "feed": "sip", "delivery_mode": "delayed"}, {}),
    ({"feed": "iex"}, {}),
    ({"delivery_mode": "real_time"}, {}),
    ({"adjustment": "raw"}, {}),
    ({"interval": "1Min"}, {}),
    ({}, {"session": date(2026, 8, 23)}),
    ({}, {"open": Decimal("101.100000000000")}),
    ({}, {"high": Decimal("103.100000000000")}),
    ({}, {"low": Decimal("99.900000000000")}),
    ({}, {"close": Decimal("102.100000000000")}),
    ({}, {"volume": Decimal("1200001.000000000000")}),
    ({}, {"observed_at": OBSERVED_AT + timedelta(microseconds=1)}),
    ({}, {"available_at": AVAILABLE_AT + timedelta(microseconds=1)}),
    ({}, {"received_at": RECEIVED_AT + timedelta(microseconds=1)}),
    ({}, {"revision": "2"}),
    ({}, {"quality_flags": ["suspect"]}),
)


async def _wait_for_advisory_wait(
    factory: async_sessionmaker[AsyncSession], backend_pid: int
) -> None:
    async with asyncio.timeout(2):
        while True:
            async with factory() as observer:
                wait_event = await observer.scalar(
                    text(
                        "SELECT wait_event FROM pg_stat_activity "
                        "WHERE pid=:backend_pid"
                    ),
                    {"backend_pid": backend_pid},
                )
            if wait_event == "advisory":
                return
            await asyncio.sleep(0)


async def _selected_price_manifest(
    factory: async_sessionmaker[AsyncSession],
    *,
    allow_fixture: bool,
) -> UUID:
    selected_at = RECEIVED_AT + timedelta(days=2)
    async with factory() as session, session.begin():
        snapshot = DataSnapshot(
            id=SELECTION_SNAPSHOT_ID,
            source="fixture",
            provider_object_id="price-selection-snapshot",
            revision="v1",
            observed_at=selected_at,
            available_at=selected_at,
            received_at=selected_at,
            schema_version="1",
            payload={},
        )
        session.add(snapshot)
        await session.flush()
        await freeze_price_authority(
            session,
            snapshot=snapshot,
            instrument_id=INSTRUMENT_ID,
            decision_time=selected_at,
            allow_fixture=allow_fixture,
        )
        binding = await session.scalar(
            select(AgentDataSnapshotEvidenceRecord).where(
                AgentDataSnapshotEvidenceRecord.data_snapshot_id == snapshot.id,
                AgentDataSnapshotEvidenceRecord.instrument_id == INSTRUMENT_ID,
                AgentDataSnapshotEvidenceRecord.authority_kind == "price",
            )
        )
        assert binding is not None
        manifest_id = await session.scalar(
            select(PriceBarSetAuthority.price_series_manifest_id).where(
                PriceBarSetAuthority.evidence_ref_id == binding.evidence_ref_id
            )
        )
        assert manifest_id is not None
        return manifest_id


@pytest.mark.asyncio
async def test_price_bar_set_v1_has_a_fixed_postgresql_17_golden_hash(
    migrated_database_url: str,
) -> None:
    """Changing PostgreSQL v1 encoding or hashing must break the fixed protocol fixture."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await seed_price_candidate(factory)
    async with factory() as session:
        canonical = await session.scalar(
            text("SELECT nexus_price_bar_set_canonical_v1(:manifest_id)"),
            {"manifest_id": MANIFEST_ID},
        )
        prepared = (
            (
                await session.execute(
                    text("SELECT * FROM nexus_prepare_price_bar_set(:manifest_id)"),
                    {"manifest_id": MANIFEST_ID},
                )
            )
            .mappings()
            .one()
        )
    assert canonical == EXPECTED_CANONICAL
    assert prepared["content_hash"] == EXPECTED_HASH
    assert prepared["byte_length"] == 471


@pytest.mark.asyncio
@pytest.mark.parametrize(("manifest_mutation", "bar_mutation"), CANONICAL_MUTATIONS)
async def test_each_canonical_price_bar_field_changes_the_v1_payload(
    migrated_database_url: str,
    manifest_mutation: dict[str, object],
    bar_mutation: dict[str, object],
) -> None:
    """Omitting any governed manifest or Bar field would make one mutation hash identically."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await seed_price_candidate(factory)
    candidate_args = {
        key: manifest_mutation[key]
        for key in ("source", "feed", "delivery_mode", "interval", "adjustment")
        if key in manifest_mutation
    }
    manifest_overrides = {
        key: value for key, value in manifest_mutation.items() if key not in candidate_args
    }
    await seed_price_candidate(
        factory,
        manifest_id=MUTATED_MANIFEST_ID,
        manifest_overrides=manifest_overrides,
        bar_overrides=bar_mutation,
        **candidate_args,
    )
    async with factory() as session:
        baseline = await session.scalar(
            text("SELECT nexus_price_bar_set_canonical_v1(:manifest_id)"),
            {"manifest_id": MANIFEST_ID},
        )
        mutated = await session.scalar(
            text("SELECT nexus_price_bar_set_canonical_v1(:manifest_id)"),
            {"manifest_id": MUTATED_MANIFEST_ID},
        )
    assert mutated != baseline


@pytest.mark.asyncio
async def test_manifest_and_bar_instrument_are_independent_canonical_inputs(
    migrated_database_url: str,
) -> None:
    """Dropping either instrument field would hide a manifest/Bar ownership mismatch."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await seed_price_candidate(factory)
    await seed_price_candidate(
        factory,
        manifest_id=MUTATED_MANIFEST_ID,
        manifest_overrides={"instrument": str(OTHER_INSTRUMENT_ID)},
    )
    await seed_price_candidate(
        factory,
        manifest_id=SECOND_MUTATED_MANIFEST_ID,
        bar_overrides={"instrument_id": OTHER_INSTRUMENT_ID},
    )
    async with factory() as session:
        canonicals = []
        for manifest_id in (MANIFEST_ID, MUTATED_MANIFEST_ID, SECOND_MUTATED_MANIFEST_ID):
            canonicals.append(
                await session.scalar(
                    text("SELECT nexus_price_bar_set_canonical_v1(:manifest_id)"),
                    {"manifest_id": manifest_id},
                )
            )
    assert canonicals[1] != canonicals[0]
    assert canonicals[2] != canonicals[0]


@pytest.mark.asyncio
async def test_canonical_price_bars_ignore_row_ids_and_physical_insertion_order(
    migrated_database_url: str,
) -> None:
    """Depending on row IDs or heap order would make equivalent Bar sets hash differently."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    first = await seed_price_candidate(factory, bar_count=3)
    second = await seed_price_candidate_in_reverse(
        factory,
        manifest_id=MUTATED_MANIFEST_ID,
        bar_count=3,
    )
    assert first.bar_ids != second.bar_ids
    async with factory() as session:
        first_canonical = await session.scalar(
            text("SELECT nexus_price_bar_set_canonical_v1(:manifest_id)"),
            {"manifest_id": first.manifest_id},
        )
        second_canonical = await session.scalar(
            text("SELECT nexus_price_bar_set_canonical_v1(:manifest_id)"),
            {"manifest_id": second.manifest_id},
        )
    assert second_canonical == first_canonical


PREPARE_REJECTIONS = (
    pytest.param({"feed": "iex"}, {}, 1, id="alpaca-non-sip"),
    pytest.param({"delivery_mode": "real_time"}, {}, 1, id="alpaca-not-delayed"),
    pytest.param({"interval": "1Min"}, {}, 1, id="intraday"),
    pytest.param(
        {"source": "fosun", "feed": "sip", "delivery_mode": "delayed"},
        {},
        1,
        id="unapproved-source",
    ),
    pytest.param({}, {"quality_flags": ["suspect"]}, 1, id="flagged"),
    pytest.param({}, {"session": date(2026, 8, 23)}, 2, id="duplicate-session"),
    pytest.param({}, {"instrument_id": OTHER_INSTRUMENT_ID}, 1, id="mixed-instrument"),
    pytest.param({"manifest_overrides": {"row_count": 2}}, {}, 1, id="row-count"),
    pytest.param(
        {"manifest_overrides": {"start_at": OBSERVED_AT - timedelta(days=1)}},
        {},
        1,
        id="start-boundary",
    ),
    pytest.param(
        {"manifest_overrides": {"end_at": OBSERVED_AT + timedelta(days=1)}},
        {},
        1,
        id="end-boundary",
    ),
    pytest.param({}, {"quality_flags": {"suspect": True}}, 1, id="flags-not-array"),
)


@pytest.mark.asyncio
@pytest.mark.parametrize(("candidate_args", "bar_overrides", "bar_count"), PREPARE_REJECTIONS)
async def test_prepare_price_bar_set_fails_closed_for_ineligible_candidates(
    migrated_database_url: str,
    candidate_args: dict[str, object],
    bar_overrides: dict[str, object],
    bar_count: int,
) -> None:
    """Weakening any source, quality, ownership, count, or boundary predicate leaks a row."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await seed_price_candidate(
        factory,
        bar_count=bar_count,
        bar_overrides=bar_overrides,
        **candidate_args,
    )
    async with factory() as session:
        prepared = (
            (
                await session.execute(
                    text("SELECT * FROM nexus_prepare_price_bar_set(:manifest_id)"),
                    {"manifest_id": MANIFEST_ID},
                )
            )
            .mappings()
            .all()
        )
    assert prepared == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bar_overrides",
    (
        {"available_at": OBSERVED_AT - timedelta(microseconds=1)},
        {"received_at": AVAILABLE_AT - timedelta(microseconds=1)},
    ),
)
async def test_price_bar_pit_order_is_rejected_at_the_database_boundary(
    migrated_database_url: str,
    bar_overrides: dict[str, object],
) -> None:
    """Removing either timestamp constraint would admit future or out-of-order evidence."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    with pytest.raises(DBAPIError):
        await seed_price_candidate(factory, bar_overrides=bar_overrides)


@pytest.mark.asyncio
async def test_fixture_price_bars_are_database_eligible_for_explicit_test_runtime(
    migrated_database_url: str,
) -> None:
    """Accidentally removing the narrow fixture branch would block controlled test authority."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await seed_price_candidate(
        factory,
        source="fixture",
        feed="fixture-feed",
        delivery_mode="fixture",
    )
    authority = await insert_valid_authority_from_prepare(factory, MANIFEST_ID)
    assert authority.source == "fixture"


@pytest.mark.asyncio
async def test_price_authority_selection_prefers_production_over_newer_fixture(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await seed_price_candidate(factory, manifest_id=MANIFEST_ID)
    await insert_valid_authority_from_prepare(factory, MANIFEST_ID)
    await seed_price_candidate(
        factory,
        manifest_id=MUTATED_MANIFEST_ID,
        source="fixture",
        feed="fixture-feed",
        delivery_mode="fixture",
        bar_overrides={
            "available_at": AVAILABLE_AT + timedelta(hours=1),
            "received_at": RECEIVED_AT + timedelta(hours=1),
        },
    )
    await insert_valid_authority_from_prepare(factory, MUTATED_MANIFEST_ID)
    assert await _selected_price_manifest(factory, allow_fixture=True) == MANIFEST_ID


@pytest.mark.asyncio
async def test_price_authority_selection_uses_available_at_desc_before_manifest_id(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await seed_price_candidate(factory, manifest_id=SECOND_MUTATED_MANIFEST_ID)
    await insert_valid_authority_from_prepare(factory, SECOND_MUTATED_MANIFEST_ID)
    await seed_price_candidate(
        factory,
        manifest_id=MUTATED_MANIFEST_ID,
        bar_overrides={
            "available_at": AVAILABLE_AT + timedelta(hours=1),
            "received_at": RECEIVED_AT + timedelta(hours=1),
        },
    )
    await insert_valid_authority_from_prepare(factory, MUTATED_MANIFEST_ID)
    assert (
        await _selected_price_manifest(factory, allow_fixture=False)
        == MUTATED_MANIFEST_ID
    )


@pytest.mark.asyncio
async def test_price_authority_selection_uses_manifest_id_desc_as_final_tie_break(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await seed_price_candidate(factory, manifest_id=MANIFEST_ID)
    await insert_valid_authority_from_prepare(factory, MANIFEST_ID)
    await seed_price_candidate(factory, manifest_id=MUTATED_MANIFEST_ID)
    await insert_valid_authority_from_prepare(factory, MUTATED_MANIFEST_ID)
    assert (
        await _selected_price_manifest(factory, allow_fixture=False)
        == MUTATED_MANIFEST_ID
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "forgery",
    (
        "non_sip",
        "flagged",
        "duplicate_session",
        "mixed_instrument",
        "policy",
        "source",
        "owner",
        "revision",
        "hash",
        "byte_length",
        "bar_count",
        "first_session",
        "available_at",
        "location",
    ),
)
async def test_direct_sql_cannot_forge_price_bar_set_authority(
    migrated_database_url: str,
    forgery: str,
) -> None:
    """Dropping trigger recomputation lets one direct-SQL forged field establish authority."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    if forgery == "non_sip":
        await seed_price_candidate(factory, feed="iex")
    elif forgery == "flagged":
        await seed_price_candidate(factory, quality_flags=("suspect",))
    elif forgery == "duplicate_session":
        await seed_price_candidate(
            factory,
            bar_count=2,
            bar_overrides={"session": date(2026, 8, 23)},
        )
    elif forgery == "mixed_instrument":
        await seed_price_candidate(
            factory,
            bar_overrides={"instrument_id": OTHER_INSTRUMENT_ID},
        )
    else:
        await seed_price_candidate(factory)
    with pytest.raises(DBAPIError, match="price bar set authority is invalid") as raised:
        await _insert_direct_authority(factory, MANIFEST_ID, forgery=forgery)
    assert getattr(raised.value.orig, "sqlstate", None) == "23514"


@pytest.mark.asyncio
async def test_finalized_price_bar_set_cannot_grow_or_change(
    migrated_database_url: str,
) -> None:
    """Missing Bar/authority guards would let a digest diverge from its frozen row set."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    seeded = await seed_price_candidate(factory)
    await insert_valid_authority_from_prepare(factory, seeded.manifest_id)
    async with factory() as session:
        with pytest.raises(DBAPIError):
            async with session.begin():
                session.add(clean_bar(manifest_id=seeded.manifest_id, session=date(2026, 8, 25)))
                await session.flush()
    with pytest.raises(DBAPIError):
        async with factory() as session, session.begin():
            authority = await session.scalar(select(PriceBarSetAuthority))
            assert authority is not None
            await session.delete(authority)
            await session.flush()


@pytest.mark.asyncio
async def test_concurrent_writer_first_serializes_finalizer_after_the_new_bar(
    migrated_database_url: str,
) -> None:
    """Removing the shared lock lets finalization miss a writer's uncommitted Bar."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await seed_price_candidate(
        factory,
        manifest_overrides={
            "row_count": 2,
            "end_at": OBSERVED_AT + timedelta(days=1),
        },
    )
    writer_locked = asyncio.Event()
    release_writer = asyncio.Event()
    finalizer_pid = asyncio.get_running_loop().create_future()

    async def writer_first() -> int:
        async with factory() as session:
            async with session.begin():
                session.add(
                    clean_bar(manifest_id=MANIFEST_ID, session=date(2026, 8, 25))
                )
                await session.flush()
                writer_locked.set()
                await release_writer.wait()
            return int(await session.scalar(text("SELECT 1")))

    async def finalizer_second() -> int:
        await writer_locked.wait()
        async with factory() as session:
            async with session.begin():
                finalizer_pid.set_result(
                    int(await session.scalar(text("SELECT pg_backend_pid()")))
                )
                manifest = await session.get(PriceSeriesManifest, MANIFEST_ID)
                assert manifest is not None
                finalized = await finalize_price_bar_set(session, manifest)
                assert finalized is not None
            return int(await session.scalar(text("SELECT 1")))

    writer_task = asyncio.create_task(writer_first())
    finalizer_task = asyncio.create_task(finalizer_second())
    tasks = (writer_task, finalizer_task)
    async with asyncio.timeout(5):
        try:
            await _wait_for_advisory_wait(factory, await finalizer_pid)
            release_writer.set()
            writer_probe, finalizer_probe = await asyncio.gather(*tasks)

            async with factory() as session:
                fresh_authority = await session.scalar(
                    select(PriceBarSetAuthority).where(
                        PriceBarSetAuthority.price_series_manifest_id == MANIFEST_ID
                    )
                )
                assert fresh_authority is not None
                fresh_prepared = (
                    (
                        await session.execute(
                            text(
                                "SELECT * FROM "
                                "nexus_prepare_price_bar_set(:manifest_id)"
                            ),
                            {"manifest_id": MANIFEST_ID},
                        )
                    )
                    .mappings()
                    .one()
                )
                assert fresh_authority.bar_count == fresh_prepared["bar_count"] == 2
                assert fresh_authority.content_hash == fresh_prepared["content_hash"]
                assert fresh_authority.byte_length == fresh_prepared["byte_length"]
                assert fresh_authority.first_session == fresh_prepared["first_session"]
                assert fresh_authority.last_session == fresh_prepared["last_session"]
                assert fresh_authority.available_at == fresh_prepared["available_at"]
            assert writer_probe == finalizer_probe == 1
        finally:
            release_writer.set()
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_concurrent_authority_first_serializes_and_rejects_the_late_writer(
    migrated_database_url: str,
) -> None:
    """Removing the Bar guard lock lets a late INSERT grow an already-frozen set."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await seed_price_candidate(factory)
    authority_locked = asyncio.Event()
    writer_pid = asyncio.get_running_loop().create_future()

    async def late_writer() -> None:
        await authority_locked.wait()
        async with factory() as session:
            try:
                async with session.begin():
                    writer_pid.set_result(
                        int(await session.scalar(text("SELECT pg_backend_pid()")))
                    )
                    session.add(
                        clean_bar(manifest_id=MANIFEST_ID, session=date(2026, 8, 25))
                    )
                    await session.flush()
            except DBAPIError:
                assert await session.scalar(text("SELECT 1")) == 1
                raise

    writer_task: asyncio.Task[None] | None = None
    async with asyncio.timeout(5):
        try:
            async with factory() as finalizer:
                async with finalizer.begin():
                    manifest = await finalizer.get(PriceSeriesManifest, MANIFEST_ID)
                    assert manifest is not None
                    finalized = await finalize_price_bar_set(finalizer, manifest)
                    assert finalized is not None
                    authority_locked.set()
                    writer_task = asyncio.create_task(late_writer())
                    await _wait_for_advisory_wait(factory, await writer_pid)
                assert await finalizer.scalar(text("SELECT 1")) == 1

            with pytest.raises(DBAPIError, match="frozen price series cannot grow"):
                await writer_task
        finally:
            if writer_task is not None:
                if not writer_task.done():
                    writer_task.cancel()
                await asyncio.gather(writer_task, return_exceptions=True)


async def _insert_direct_authority(
    factory: async_sessionmaker[AsyncSession],
    manifest_id: UUID,
    *,
    forgery: str,
) -> None:
    async with factory() as session, session.begin():
        manifest = await session.get(PriceSeriesManifest, manifest_id)
        assert manifest is not None
        prepared = (
            (
                await session.execute(
                    text("SELECT * FROM nexus_prepare_price_bar_set(:manifest_id)"),
                    {"manifest_id": manifest_id},
                )
            )
            .mappings()
            .one_or_none()
        )
        digest = str(prepared["content_hash"]) if prepared else "a" * 64
        byte_length = int(prepared["byte_length"]) if prepared else 471
        bar_count = int(prepared["bar_count"]) if prepared else 1
        first_session = prepared["first_session"] if prepared else date(2026, 8, 24)
        last_session = prepared["last_session"] if prepared else date(2026, 8, 24)
        available_at = prepared["available_at"] if prepared else RECEIVED_AT
        evidence_values: dict[str, object] = {
            "id": new_uuid7(),
            "source": manifest.source,
            "provider_object_id": f"price-bar-set:{manifest_id}",
            "revision": digest,
            "available_at": available_at,
            "location": f"db://price-bar-set/{manifest_id}@sha256:{digest}",
            "content_hash": digest,
            "byte_length": byte_length,
        }
        authority_values: dict[str, object] = {
            "id": new_uuid7(),
            "price_series_manifest_id": manifest_id,
            "evidence_ref_id": evidence_values["id"],
            "source_policy_id": SOURCE_POLICY_ID,
            "source": manifest.source,
            "provider_object_id": evidence_values["provider_object_id"],
            "revision": digest,
            "hash_version": "price_bar_set_sha256_v1",
            "policy_version": "agent_price_authority_v1",
            "content_hash": digest,
            "byte_length": byte_length,
            "bar_count": bar_count,
            "first_session": first_session,
            "last_session": last_session,
            "available_at": available_at,
        }
        await _apply_authority_forgery(
            session,
            forgery=forgery,
            digest=digest,
            evidence_values=evidence_values,
            authority_values=authority_values,
        )
        await session.execute(
            text(
                "INSERT INTO research_evidence_refs "
                "(id, source, provider_object_id, revision, available_at, location, "
                "content_hash, byte_length) VALUES "
                "(:id, :source, :provider_object_id, :revision, :available_at, :location, "
                ":content_hash, :byte_length)"
            ),
            evidence_values,
        )
        await session.execute(
            text(
                "INSERT INTO price_bar_set_authorities "
                "(id, price_series_manifest_id, evidence_ref_id, source_policy_id, source, "
                "provider_object_id, revision, hash_version, policy_version, content_hash, "
                "byte_length, bar_count, first_session, last_session, available_at) VALUES "
                "(:id, :price_series_manifest_id, :evidence_ref_id, :source_policy_id, "
                ":source, :provider_object_id, :revision, :hash_version, :policy_version, "
                ":content_hash, :byte_length, :bar_count, :first_session, :last_session, "
                ":available_at)"
            ),
            authority_values,
        )


async def _apply_authority_forgery(
    session: AsyncSession,
    *,
    forgery: str,
    digest: str,
    evidence_values: dict[str, object],
    authority_values: dict[str, object],
) -> None:
    if forgery == "policy":
        await session.execute(
            text(
                "INSERT INTO source_policies "
                "(id, key, version, schema_version, payload) VALUES "
                "(:id, 'forged_market_price', 'forged_source_policy_v1', '1', "
                "CAST(:payload AS jsonb))"
            ),
            {"id": FORGED_POLICY_ID, "payload": '{"priority":["alpaca"]}'},
        )
        authority_values["source_policy_id"] = FORGED_POLICY_ID
    elif forgery == "source":
        evidence_values["source"] = "fixture"
        authority_values["source"] = "fixture"
    elif forgery == "owner":
        forged_owner = f"price-bar-set:{OTHER_INSTRUMENT_ID}"
        evidence_values["provider_object_id"] = forged_owner
        authority_values["provider_object_id"] = forged_owner
    elif forgery == "revision":
        evidence_values["revision"] = "b" * 64
        authority_values["revision"] = "b" * 64
    elif forgery == "hash":
        authority_values["content_hash"] = "b" * 64
    elif forgery == "byte_length":
        evidence_values["byte_length"] = int(evidence_values["byte_length"]) + 1
        authority_values["byte_length"] = int(authority_values["byte_length"]) + 1
    elif forgery == "bar_count":
        authority_values["bar_count"] = int(authority_values["bar_count"]) + 1
    elif forgery == "first_session":
        authority_values["first_session"] = date(2026, 8, 23)
    elif forgery == "available_at":
        shifted = authority_values["available_at"] + timedelta(microseconds=1)
        evidence_values["available_at"] = shifted
        authority_values["available_at"] = shifted
    elif forgery == "location":
        evidence_values["location"] = f"db://price-bar-set/{OTHER_INSTRUMENT_ID}@sha256:{digest}"
    elif forgery not in {"non_sip", "flagged", "duplicate_session", "mixed_instrument"}:
        raise AssertionError(f"unknown authority forgery: {forgery}")
