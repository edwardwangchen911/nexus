"""Bundle preparation starts with source authorities, never an inserted final bundle."""

import asyncio
import json
import os
import sys
from datetime import UTC, datetime
from uuid import uuid4

import exchange_calendars
import httpx
import pytest
from helpers.portfolio_api import app, headers
from nexus.backtest.bundle import BundleRequest, prepare_bundle
from nexus.backtest.cost_policy import BACKTEST_COST_POLICY_ID
from nexus.persistence.idempotency import IdempotencyClaimError
from nexus.persistence.models.backtest import BacktestAuthorityBundleRecord, BacktestRunRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.runtime import IdempotencyRecord
from nexus.persistence.session import session_factory
from nexus.quality.source_policy import SOURCE_POLICY_ID
from nexus.settings import EngineSettings
from sqlalchemy import func, select, text

from engine.tests.integration.test_backtest_writer import (
    ACTION_COLLECTION_ID,
    COMPLETED_AT,
    SECTOR_COLLECTION_ID,
    SECTOR_ID,
    UNIVERSE_COLLECTION_ID,
    UNIVERSE_ID,
    _dataset,
    _raw_inputs,
    _seed_upstream_authorities,
)


async def source_request(url):
    factory = session_factory(EngineSettings(database_url=url))
    _, bars, sessions, protocol = _raw_inputs()
    prices = await _seed_upstream_authorities(factory, bars, sessions)
    data = _dataset(bars, sessions, prices)
    async with factory.begin() as session:
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

    return {
        "strategy_version": "baseline_v1",
        "calendar_authority_id": str(data.authority.exchange_calendar.authority_id),
        "universe_collection_id": str(UNIVERSE_COLLECTION_ID),
        "sector_collection_id": str(SECTOR_COLLECTION_ID),
        "corporate_action_collection_id": str(ACTION_COLLECTION_ID),
        "spy_instrument_alias_id": str(data.authority.spy_benchmark.alias_id),
        "equal_weight_universe_snapshot_id": str(UNIVERSE_ID),
        "source_policy_id": str(SOURCE_POLICY_ID),
        "cost_policy_id": str(BACKTEST_COST_POLICY_ID),
        "start": protocol.start.isoformat(),
        "end": protocol.end.isoformat(),
        "initial_nav": str(protocol.initial_nav),
        "idempotency_key": "synthetic-bundle",
    }


async def cli(url, path):
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "nexus.ops.backtest_once",
        "--prepare-request-file",
        str(path),
        env={**os.environ, "DATABASE_URL": url},
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        returncode_output = await asyncio.wait_for(process.communicate(), timeout=90)
    except TimeoutError:
        process.kill()
        await process.wait()
        raise
    return process.returncode, *returncode_output


@pytest.mark.asyncio
async def test_prepare_cli_runs_and_reads_without_final_bundle_fixture(
    migrated_database_url, tmp_path
):
    request = await source_request(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        assert (
            await session.scalar(select(func.count()).select_from(BacktestAuthorityBundleRecord))
            == 0
        )
        assert (
            await session.scalar(
                select(func.count())
                .select_from(DataSnapshot)
                .where(DataSnapshot.source == "nexus_backtest_bundle")
            )
            == 0
        )
    path = tmp_path / "request.json"
    path.write_text(json.dumps(request))
    before = datetime.now(UTC)
    code, stdout, stderr = await cli(migrated_database_url, path)
    assert code == 0, stderr.decode()
    result = json.loads(stdout)
    assert result["status"] == "BACKTEST_COMPLETED"
    assert result["execution_locked"] and not result["promotable"]
    code, stdout, stderr = await cli(migrated_database_url, path)
    assert code == 0, stderr.decode()
    replay = json.loads(stdout)
    assert replay["status"] == "BACKTEST_REPLAYED" and replay["run_id"] == result["run_id"]
    async with factory() as session:
        bundle = await session.scalar(select(BacktestAuthorityBundleRecord))
        assert bundle.completed_at >= before
        assert await session.scalar(select(func.count()).select_from(BacktestRunRecord)) == 1
    route = "/internal/v1/strategies"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)), base_url="http://testserver"
    ) as client:
        response = await client.get(route, headers=headers("GET", route))
    assert response.status_code == 200
    summary = response.json()["data"][0]["latest_backtest"]
    assert summary["result_hash"] == result["result_hash"] and summary["data_mode"] == "fixture"


async def assert_no_package(session):
    for model in (BacktestAuthorityBundleRecord, BacktestRunRecord, IdempotencyRecord):
        assert await session.scalar(select(func.count()).select_from(model)) == 0
    assert (
        await session.scalar(
            select(func.count())
            .select_from(DataSnapshot)
            .where(DataSnapshot.source == "nexus_backtest_bundle")
        )
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad",
    [
        "missing_collection",
        "wrong_kind",
        "future_collection",
        "future_member",
        "wrong_benchmark",
        "missing_member",
    ],
)
async def test_bad_source_collection_never_publishes_package(migrated_database_url, bad):
    values = await source_request(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    if bad == "missing_collection":
        values["universe_collection_id"] = str(uuid4())
    elif bad == "wrong_kind":
        values["universe_collection_id"] = values["sector_collection_id"]
    elif bad == "wrong_benchmark":
        values["equal_weight_universe_snapshot_id"] = str(uuid4())
    else:
        async with factory.begin() as session:
            # Deliberate isolated corrupt-source injection, not a producer shortcut.
            table = (
                "backtest_authority_collections"
                if bad == "future_collection"
                else "backtest_universe_authority_members"
            )
            await session.execute(text(f"ALTER TABLE {table} DISABLE TRIGGER USER"))
            if bad == "future_collection":
                await session.execute(
                    text(
                        "UPDATE backtest_authority_collections "
                        "SET completed_at='9999-01-01T00:00:00Z'"
                    )
                )
            elif bad == "future_member":
                await session.execute(
                    text(
                        "UPDATE backtest_universe_authority_members "
                        "SET created_at='9999-01-01T00:00:00Z'"
                    )
                )
            else:
                await session.execute(text("DELETE FROM backtest_universe_authority_members"))
            await session.execute(text(f"ALTER TABLE {table} ENABLE TRIGGER USER"))
    async with factory.begin() as session:
        with pytest.raises(ValueError):
            await prepare_bundle(session, BundleRequest.model_validate(values))
        await assert_no_package(session)


@pytest.mark.asyncio
async def test_prepare_concurrent_replay_is_exact_and_conflicting_key_does_not_mutate(
    migrated_database_url,
):
    values = await source_request(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    request = BundleRequest.model_validate(values)

    async def prepare():
        async with factory.begin() as session:
            return await prepare_bundle(session, request)

    first, second = await asyncio.gather(prepare(), prepare())
    assert first.authority_bundle_id == second.authority_bundle_id
    assert first.data_snapshot_id == second.data_snapshot_id
    assert first.replayed != second.replayed
    async with factory.begin() as session:
        snapshot = await session.get(DataSnapshot, first.data_snapshot_id)
        original = dict(snapshot.payload)
        with pytest.raises(IdempotencyClaimError, match="IDEMPOTENCY_CONFLICT"):
            await prepare_bundle(
                session,
                BundleRequest.model_validate(
                    {
                        **values,
                        "initial_nav": "20000",
                    }
                ),
            )
        assert (
            await session.scalar(select(func.count()).select_from(BacktestAuthorityBundleRecord))
            == 1
        )
        assert (await session.get(DataSnapshot, first.data_snapshot_id)).payload == original
        assert await session.scalar(select(func.count()).select_from(BacktestRunRecord)) == 0


@pytest.mark.asyncio
async def test_last_member_insert_failure_rolls_back_snapshot_bundle_and_claim(
    migrated_database_url,
):
    values = await source_request(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory.begin() as session:
        await session.execute(
            text("""
            CREATE FUNCTION synthetic_member_failure() RETURNS trigger LANGUAGE plpgsql AS
            $$ BEGIN RAISE EXCEPTION 'synthetic storage failure'; END; $$;
        """)
        )
        await session.execute(
            text("""
            CREATE TRIGGER synthetic_fail BEFORE INSERT ON backtest_instrument_authority_members
            FOR EACH ROW EXECUTE FUNCTION synthetic_member_failure()
        """)
        )
    from sqlalchemy.exc import DBAPIError

    async with factory.begin() as session:
        with pytest.raises(DBAPIError):
            await prepare_bundle(session, BundleRequest.model_validate(values))
        await assert_no_package(session)


async def later_partial_source(factory):
    from nexus.market.price_bar_authority import finalize_price_bar_set
    from nexus.persistence.models.data import PriceSeriesManifest

    from engine.tests.integration.test_backtest_writer import INSTRUMENT_ID, _digest

    _, bars, _, _ = _raw_inputs()
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


@pytest.mark.asyncio
async def test_replay_keeps_original_candidate_set_but_new_key_sees_later_partial_source(
    migrated_database_url,
):
    values = await source_request(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    request = BundleRequest.model_validate(values)
    async with factory.begin() as session:
        first = await prepare_bundle(session, request)
        original = dict((await session.get(DataSnapshot, first.data_snapshot_id)).payload)
    await later_partial_source(factory)
    async with factory.begin() as session:
        replay = await prepare_bundle(session, request)
        assert replay.replayed and replay.authority_bundle_id == first.authority_bundle_id
        assert (await session.get(DataSnapshot, first.data_snapshot_id)).payload == original
        with pytest.raises(ValueError, match="source_policy_v1"):
            await prepare_bundle(
                session,
                BundleRequest.model_validate(
                    {
                        **values,
                        "idempotency_key": "later-source-query",
                    }
                ),
            )
        assert (
            await session.scalar(select(func.count()).select_from(BacktestAuthorityBundleRecord))
            == 1
        )


@pytest.mark.asyncio
async def test_prepare_waits_for_global_publication_lock_before_freeze_clock(migrated_database_url):
    request = BundleRequest.model_validate(await source_request(migrated_database_url))
    factory = session_factory(EngineSettings(database_url=migrated_database_url))

    async def prepare():
        async with factory.begin() as session:
            return await prepare_bundle(session, request)

    async with factory.begin() as publisher:
        await publisher.execute(
            text(
                "SELECT pg_advisory_xact_lock(hashtextextended('backtest-authority-global', 1602))"
            )
        )
        task = asyncio.create_task(prepare())
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.shield(task), timeout=0.15)
        released_after = await publisher.scalar(select(func.clock_timestamp()))
    result = await asyncio.wait_for(task, timeout=15)
    assert result.completed_at >= released_after


@pytest.mark.asyncio
async def test_cli_execution_failure_retains_one_valid_prepared_package(
    migrated_database_url, tmp_path
):
    values = await source_request(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory.begin() as session:
        # A not-yet-available strategy is a real run_once rejection, not a mocked writer.
        await session.execute(text("ALTER TABLE strategy_definitions DISABLE TRIGGER USER"))
        await session.execute(
            text("UPDATE strategy_definitions SET created_at='9999-01-01T00:00:00Z'")
        )
        await session.execute(text("ALTER TABLE strategy_definitions ENABLE TRIGGER USER"))
    path = tmp_path / "request.json"
    path.write_text(json.dumps(values))
    for _ in range(2):
        code, stdout, stderr = await cli(migrated_database_url, path)
        assert code == 1 and stdout == b""
        assert json.loads(stderr)["code"] == "BACKTEST_STRATEGY_UNAVAILABLE"
    async with factory() as session:
        assert (
            await session.scalar(select(func.count()).select_from(BacktestAuthorityBundleRecord))
            == 1
        )
        assert await session.scalar(select(func.count()).select_from(BacktestRunRecord)) == 0
