"""Migration boundary tests for the display-only intraday market authority."""

import asyncio
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from nexus.persistence.base import Base
from nexus.persistence.models import data as data_models
from nexus.persistence.models import market as market_models
from sqlalchemy import inspect, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

assert data_models is not None
assert market_models is not None


INTRADAY_SOURCE_POLICY_ID = UUID("8fab80c9-5b6a-5b2b-8f36-3b22319b54ef")
INTRADAY_SOURCE_POLICY_PAYLOAD = {
    "priority": ["fosun", "alpaca", "fixture"],
    "fosun": {
        "mode": "read_only",
        "operation": "fosun_get_bars",
        "market_scope": "US",
    },
    "alpaca": {
        "feeds": ["iex", "sip", "delayed_sip"],
        "delivery_modes": ["real_time", "delayed"],
    },
    "fixture": {"delivery_mode": "fixture"},
    "purpose": "display_only_intraday_monitor",
}


def test_0026_is_single_head_over_0025_and_metadata_matches() -> None:
    scripts = ScriptDirectory.from_config(Config("engine/alembic.ini"))
    assert scripts.get_heads() == ["0026_intraday_review_hardening"]
    revision = scripts.get_revision("0026_intraday_review_hardening")
    assert revision is not None
    assert revision.down_revision == "0025_intraday_market_monitor"

    quote = Base.metadata.tables["latest_quotes"]
    assert {"event_kind", "content_hash", "evidence_status", "data_artifact_id"} <= set(
        quote.columns.keys()
    )
    assert "intraday_bar_set_authorities" in Base.metadata.tables


def test_0026_rejects_illegal_alpaca_feed_delivery_pair(
    migrated_database_url: str,
) -> None:
    with pytest.raises(DBAPIError, match="intraday_delivery_pair") as error:
        asyncio.run(
            _insert_verified_alpaca_trade(
                migrated_database_url,
                feed="iex",
                delivery_mode="delayed",
            )
        )
    assert error.value.orig.sqlstate == "23514"


def test_0026_upgrade_accepts_legacy_delayed_sip_snapshot(
    temporary_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config("engine/alembic.ini")
    command.upgrade(config, "0024_broker_simulation")
    legacy = asyncio.run(_seed_legacy_quotes(temporary_database_url))

    command.upgrade(config, "head")

    snapshot = asyncio.run(_schema_snapshot(temporary_database_url))
    assert [row["id"] for row in snapshot["quotes"]] == legacy
    assert snapshot["quotes"][1]["event_kind"] == "snapshot"
    assert snapshot["quotes"][1]["evidence_status"] == "legacy_unverified"


def test_0026_moves_legacy_provider_jobs_without_stranding_retries(
    temporary_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config("engine/alembic.ini")
    command.upgrade(config, "0025_intraday_market_monitor")
    asyncio.run(_seed_legacy_provider_job(temporary_database_url))

    command.upgrade(config, "head")
    assert asyncio.run(_job_queue(temporary_database_url)) == "provider"

    command.downgrade(config, "0025_intraday_market_monitor")
    assert asyncio.run(_job_queue(temporary_database_url)) == "general"


def test_0026_preserves_terminal_legacy_job_execution_queue(
    temporary_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config("engine/alembic.ini")
    command.upgrade(config, "0025_intraday_market_monitor")
    asyncio.run(
        _seed_legacy_provider_job(
            temporary_database_url,
            status="succeeded",
        )
    )

    command.upgrade(config, "head")
    assert asyncio.run(_job_queue(temporary_database_url)) == "general"

    command.downgrade(config, "0025_intraday_market_monitor")
    assert asyncio.run(_job_queue(temporary_database_url)) == "general"


def test_0026_downgrade_refuses_terminal_provider_job_history(
    temporary_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config("engine/alembic.ini")
    command.upgrade(config, "head")
    asyncio.run(
        _seed_legacy_provider_job(
            temporary_database_url,
            queue="provider",
            status="succeeded",
        )
    )

    with pytest.raises(DBAPIError, match="provider runtime history prevents downgrade") as error:
        command.downgrade(config, "0025_intraday_market_monitor")

    assert error.value.orig.sqlstate == "P3D01"


def test_0026_downgrade_refuses_provider_worker_heartbeat(
    temporary_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config("engine/alembic.ini")
    command.upgrade(config, "head")
    asyncio.run(_seed_provider_heartbeat(temporary_database_url))

    with pytest.raises(DBAPIError, match="provider runtime history prevents downgrade") as error:
        command.downgrade(config, "0025_intraday_market_monitor")

    assert error.value.orig.sqlstate == "P3D01"


def test_0026_rejects_verified_quote_bound_to_unrelated_raw_artifact(
    migrated_database_url: str,
) -> None:
    with pytest.raises(DBAPIError, match="raw artifact") as error:
        asyncio.run(
            _insert_verified_alpaca_trade(
                migrated_database_url,
                feed="iex",
                delivery_mode="real_time",
                revision="d" * 64,
                artifact_sha256="c" * 64,
            )
        )
    assert error.value.orig.sqlstate == "23514"


def test_0026_freezes_artifact_referenced_by_verified_intraday_quote(
    migrated_database_url: str,
) -> None:
    asyncio.run(
        _insert_verified_alpaca_trade(
            migrated_database_url,
            feed="iex",
            delivery_mode="real_time",
        )
    )

    with pytest.raises(DBAPIError, match="authority record is immutable") as error:
        asyncio.run(_mutate_alpaca_artifact(migrated_database_url))

    assert error.value.orig.sqlstate == "23514"


def test_0025_round_trip_preserves_legacy_quote_identity_and_policy(
    temporary_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config("engine/alembic.ini")
    command.upgrade(config, "0024_broker_simulation")
    legacy = asyncio.run(_seed_legacy_quotes(temporary_database_url))

    command.upgrade(config, "0025_intraday_market_monitor")
    snapshot = asyncio.run(_schema_snapshot(temporary_database_url))
    assert snapshot["quote_columns"] >= {
        "event_kind",
        "content_hash",
        "evidence_status",
        "data_artifact_id",
    }
    assert "uq_latest_quotes_instrument_source_feed" not in snapshot["quote_uniques"]
    assert "uq_latest_quotes_observation" in snapshot["quote_uniques"]
    assert snapshot["tables"] >= {"intraday_bar_set_authorities"}
    assert snapshot["triggers"] >= {
        "trg_latest_quotes_observation_immutable",
        "trg_intraday_bar_set_authorities_validate",
        "trg_intraday_bar_set_authorities_immutable",
    }
    assert snapshot["policy"] == {
        "id": INTRADAY_SOURCE_POLICY_ID,
        "key": "intraday_monitor",
        "version": "intraday_monitor_policy_v1",
        "schema_version": "1",
        "payload": INTRADAY_SOURCE_POLICY_PAYLOAD,
    }
    rows = snapshot["quotes"]
    assert [row["id"] for row in rows] == legacy
    assert rows[0]["event_kind"] == "fixture"
    assert rows[0]["evidence_status"] == "fixture"
    assert rows[1]["event_kind"] == "snapshot"
    assert rows[1]["evidence_status"] == "legacy_unverified"
    assert all(len(row["content_hash"]) == 64 for row in rows)

    command.downgrade(config, "0024_broker_simulation")
    downgraded = asyncio.run(_schema_snapshot(temporary_database_url))
    assert "intraday_bar_set_authorities" not in downgraded["tables"]
    assert {
        "event_kind",
        "content_hash",
        "evidence_status",
        "data_artifact_id",
    }.isdisjoint(downgraded["quote_columns"])
    assert "uq_latest_quotes_instrument_source_feed" in downgraded["quote_uniques"]
    assert [row["id"] for row in downgraded["quotes"]] == legacy

    command.upgrade(config, "0025_intraday_market_monitor")
    assert asyncio.run(_schema_snapshot(temporary_database_url))["policy"]["payload"] == (
        INTRADAY_SOURCE_POLICY_PAYLOAD
    )


def test_0025_quote_observations_are_immutable_and_downgrade_with_new_facts_fails_closed(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    quote_id = asyncio.run(_insert_new_fixture_trade(migrated_database_url))

    with pytest.raises(DBAPIError, match="intraday quote observations are immutable") as error:
        asyncio.run(_mutate_quote(migrated_database_url, quote_id))
    assert error.value.orig.sqlstate == "23514"

    monkeypatch.setenv("DATABASE_URL", migrated_database_url)
    with pytest.raises(DBAPIError, match="intraday review facts prevent downgrade") as error:
        command.downgrade(Config("engine/alembic.ini"), "0024_broker_simulation")
    assert error.value.orig.sqlstate == "P3D01"


def test_0026_direct_downgrade_refuses_intraday_facts(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    asyncio.run(_insert_new_fixture_trade(migrated_database_url))
    monkeypatch.setenv("DATABASE_URL", migrated_database_url)

    with pytest.raises(DBAPIError, match="intraday review facts prevent downgrade") as error:
        command.downgrade(Config("engine/alembic.ini"), "0025_intraday_market_monitor")

    assert error.value.orig.sqlstate == "P3D01"


async def _seed_legacy_quotes(database_url: str) -> list[UUID]:
    engine = create_async_engine(database_url)
    instrument_id = uuid4()
    quote_ids = [uuid4(), uuid4()]
    observed_at = datetime(2026, 8, 28, 14, 30, tzinfo=UTC)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO instruments "
                    "(id,asset_type,exchange,currency,tick_size,risk_bucket) "
                    "VALUES (:id,'common_stock','XNYS','USD',0.01,'EQUITY')"
                ),
                {"id": instrument_id},
            )
            for quote_id, source, feed, mode in (
                (quote_ids[0], "fixture", "fixture", "fixture"),
                (quote_ids[1], "alpaca", "sip", "delayed"),
            ):
                await connection.execute(
                    text(
                        "INSERT INTO latest_quotes "
                        "(id,instrument_id,source,feed,delivery_mode,bid,ask,last,observed_at,"
                        "available_at,received_at,revision,quality_flags) VALUES "
                        "(:id,:instrument_id,:source,:feed,:mode,99,101,100,:observed_at,"
                        ":observed_at,:observed_at,'legacy-1','[]'::jsonb)"
                    ),
                    {
                        "id": quote_id,
                        "instrument_id": instrument_id,
                        "source": source,
                        "feed": feed,
                        "mode": mode,
                        "observed_at": observed_at,
                    },
                )
        return quote_ids
    finally:
        await engine.dispose()


async def _schema_snapshot(database_url: str) -> dict[str, object]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            schema = await connection.run_sync(
                lambda sync: {
                    "tables": set(inspect(sync).get_table_names()),
                    "quote_columns": {
                        column["name"] for column in inspect(sync).get_columns("latest_quotes")
                    },
                    "quote_uniques": {
                        constraint["name"]
                        for constraint in inspect(sync).get_unique_constraints("latest_quotes")
                    },
                }
            )
            triggers = set(
                (
                    await connection.execute(
                        text(
                            "SELECT trigger.tgname FROM pg_trigger AS trigger "
                            "JOIN pg_class AS relation ON relation.oid=trigger.tgrelid "
                            "WHERE NOT trigger.tgisinternal "
                            "AND relation.relnamespace=current_schema()::regnamespace"
                        )
                    )
                ).scalars()
            )
            policy_row = (
                await connection.execute(
                    text(
                        "SELECT id,key,version,schema_version,payload FROM source_policies "
                        "WHERE id=:id"
                    ),
                    {"id": INTRADAY_SOURCE_POLICY_ID},
                )
            ).mappings().one_or_none()
            quote_columns = schema["quote_columns"]
            optional = (
                ",event_kind,content_hash,evidence_status,data_artifact_id"
                if "event_kind" in quote_columns
                else ""
            )
            quotes = (
                await connection.execute(
                    text(f"SELECT id{optional} FROM latest_quotes ORDER BY source DESC")
                )
            ).mappings().all()
            return {
                **schema,
                "triggers": triggers,
                "policy": None if policy_row is None else dict(policy_row),
                "quotes": [dict(row) for row in quotes],
            }
    finally:
        await engine.dispose()


async def _insert_new_fixture_trade(database_url: str) -> UUID:
    engine = create_async_engine(database_url)
    instrument_id = uuid4()
    quote_id = uuid4()
    observed_at = datetime(2026, 8, 28, 14, 31, tzinfo=UTC)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO instruments "
                    "(id,asset_type,exchange,currency,tick_size,risk_bucket) "
                    "VALUES (:id,'common_stock','XNYS','USD',0.01,'EQUITY')"
                ),
                {"id": instrument_id},
            )
            await connection.execute(
                text(
                    "INSERT INTO latest_quotes "
                    "(id,instrument_id,source,feed,delivery_mode,bid,ask,last,observed_at,"
                    "available_at,received_at,revision,quality_flags,event_kind,content_hash,"
                    "evidence_status,data_artifact_id) VALUES "
                    "(:id,:instrument_id,'fixture','fixture','fixture',NULL,NULL,100,"
                    ":observed_at,:observed_at,:observed_at,'fixture-new','[]'::jsonb,"
                    "'trade',repeat('a',64),'fixture',NULL)"
                ),
                {"id": quote_id, "instrument_id": instrument_id, "observed_at": observed_at},
            )
        return quote_id
    finally:
        await engine.dispose()


async def _mutate_quote(database_url: str, quote_id: UUID) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text("UPDATE latest_quotes SET last=101 WHERE id=:id"), {"id": quote_id}
            )
    finally:
        await engine.dispose()


async def _insert_verified_alpaca_trade(
    database_url: str,
    *,
    feed: str,
    delivery_mode: str,
    revision: str = "c" * 64,
    artifact_sha256: str = "c" * 64,
) -> None:
    engine = create_async_engine(database_url)
    instrument_id = uuid4()
    artifact_id = uuid4()
    observed_at = datetime(2026, 8, 28, 14, 32, tzinfo=UTC)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO instruments "
                    "(id,asset_type,exchange,currency,tick_size,risk_bucket) "
                    "VALUES (:id,'common_stock','XNYS','USD',0.01,'EQUITY')"
                ),
                {"id": instrument_id},
            )
            await connection.execute(
                text(
                    "INSERT INTO data_artifacts "
                    "(id,uri,sha256,byte_size,mime_type,schema_version,source,observed_at,"
                    "available_at,received_at) VALUES "
                    "(:id,:uri,:sha256,2,'application/json','test_v1','alpaca',"
                    ":observed_at,:observed_at,:observed_at)"
                ),
                {
                    "id": artifact_id,
                    "uri": f"memory://sha256/{artifact_sha256}",
                    "sha256": artifact_sha256,
                    "observed_at": observed_at,
                },
            )
            await connection.execute(
                text(
                    "INSERT INTO latest_quotes "
                    "(id,instrument_id,source,feed,delivery_mode,bid,ask,last,observed_at,"
                    "available_at,received_at,revision,quality_flags,event_kind,content_hash,"
                    "evidence_status,data_artifact_id) VALUES "
                    "(:id,:instrument_id,'alpaca',:feed,:delivery_mode,NULL,NULL,100,"
                    ":observed_at,:observed_at,:observed_at,:revision,'[]'::jsonb,"
                    "'trade',repeat('b',64),'verified',:artifact_id)"
                ),
                {
                    "id": uuid4(),
                    "instrument_id": instrument_id,
                    "artifact_id": artifact_id,
                    "feed": feed,
                    "delivery_mode": delivery_mode,
                    "revision": revision,
                    "observed_at": observed_at,
                },
            )
    finally:
        await engine.dispose()


async def _seed_legacy_provider_job(
    database_url: str,
    *,
    queue: str = "general",
    status: str = "queued",
) -> None:
    engine = create_async_engine(database_url)
    now = datetime(2026, 8, 28, 21, tzinfo=UTC)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO jobs "
                    "(id,job_key,input_hash,status,schema_version,payload,job_type,queue,"
                    "scope_kind,scope_id,effective_at,priority,retry_policy,max_attempts,"
                    "attempt_count,manual_retry_count,available_at,started_at,finished_at,"
                    "last_error_code,result_code,version) VALUES "
                    "(:id,'legacy-provider-job',repeat('a',64),:status,'daily_stage_v1',"
                    "'{}'::jsonb,'collect.market',:queue,'global','legacy-provider',"
                    ":now,280,'automatic_safe',3,0,0,:now,:started_at,:finished_at,NULL,"
                    ":result_code,1)"
                ),
                {
                    "id": uuid4(),
                    "now": now,
                    "queue": queue,
                    "status": status,
                    "started_at": None if status == "queued" else now,
                    "finished_at": (
                        now
                        if status in {"succeeded", "failed", "skipped", "dead_letter"}
                        else None
                    ),
                    "result_code": "LEGACY_SUCCEEDED" if status == "succeeded" else None,
                },
            )
    finally:
        await engine.dispose()


async def _job_queue(database_url: str) -> str:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            queue = await connection.scalar(
                text("SELECT queue FROM jobs WHERE job_key='legacy-provider-job'")
            )
            assert isinstance(queue, str)
            return queue
    finally:
        await engine.dispose()


async def _seed_provider_heartbeat(database_url: str) -> None:
    engine = create_async_engine(database_url)
    now = datetime(2026, 8, 28, 21, tzinfo=UTC)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO worker_heartbeats "
                    "(id,queue,worker_id,process_version,migration_head,started_at,seen_at) "
                    "VALUES (:id,'provider','provider-test','test-v1',"
                    "'0026_intraday_review_hardening',:now,:now)"
                ),
                {"id": uuid4(), "now": now},
            )
    finally:
        await engine.dispose()


async def _mutate_alpaca_artifact(database_url: str) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "UPDATE data_artifacts SET uri='memory://tampered' "
                    "WHERE source='alpaca'"
                )
            )
    finally:
        await engine.dispose()
