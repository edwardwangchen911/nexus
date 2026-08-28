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


def test_0025_is_single_head_over_0024_and_metadata_matches() -> None:
    scripts = ScriptDirectory.from_config(Config("engine/alembic.ini"))
    assert scripts.get_heads() == ["0025_intraday_market_monitor"]
    revision = scripts.get_revision("0025_intraday_market_monitor")
    assert revision is not None
    assert revision.down_revision == "0024_broker_simulation"

    quote = Base.metadata.tables["latest_quotes"]
    assert {"event_kind", "content_hash", "evidence_status", "data_artifact_id"} <= set(
        quote.columns.keys()
    )
    assert "intraday_bar_set_authorities" in Base.metadata.tables


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
    with pytest.raises(DBAPIError, match="intraday market facts prevent downgrade") as error:
        command.downgrade(Config("engine/alembic.ini"), "0024_broker_simulation")
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
                (quote_ids[1], "alpaca", "iex", "real_time"),
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
