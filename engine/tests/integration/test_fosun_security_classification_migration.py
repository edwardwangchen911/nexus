import asyncio
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

REVISION = "0052_fosun_classification"
PARENT = "0051_scenario_selections"


def _config() -> Config:
    return Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))


def test_classification_migration_roundtrip_and_refuses_frozen_owner(
    temporary_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = _config()
    scripts = ScriptDirectory.from_config(config)
    revision = scripts.get_revision(REVISION)
    assert revision is not None and revision.down_revision == PARENT
    assert scripts.get_heads() == [CURRENT_MIGRATION_HEAD]

    command.upgrade(config, PARENT)
    command.upgrade(config, REVISION)
    function_definition, table_exists = asyncio.run(_migration_state(temporary_database_url))
    assert "fosun_security_classification" in function_definition
    assert table_exists is False

    command.downgrade(config, PARENT)
    function_definition, _ = asyncio.run(_migration_state(temporary_database_url))
    assert "fosun_security_classification" not in function_definition

    command.upgrade(config, REVISION)
    asyncio.run(_seed_frozen_owner_without_trigger(temporary_database_url))
    with pytest.raises(RuntimeError, match="cannot remove ownership support"):
        command.downgrade(config, PARENT)
    assert asyncio.run(_current_revision(temporary_database_url)) == REVISION


async def _migration_state(database_url: str) -> tuple[str, bool]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            definition = await connection.scalar(
                text(
                    "SELECT pg_get_functiondef("
                    "'public.nexus_validate_agent_snapshot_evidence()'::regprocedure)"
                )
            )
            table_exists = await connection.scalar(
                text("SELECT to_regclass('public.fosun_security_classification_observations')")
            )
            assert isinstance(definition, str)
            return definition, table_exists is not None
    finally:
        await engine.dispose()


async def _current_revision(database_url: str) -> str:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            revision = await connection.scalar(text("SELECT version_num FROM alembic_version"))
            assert isinstance(revision, str)
            return revision
    finally:
        await engine.dispose()


async def _seed_frozen_owner_without_trigger(database_url: str) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            script = (
                    "INSERT INTO instruments "
                    "(id,asset_type,exchange,currency,tick_size,risk_bucket) VALUES "
                    "('00000000-0000-7000-8000-000000000031','equity','XNAS','USD',NULL,'EQUITY'); "
                    "INSERT INTO data_snapshots "
                    "(id,source,provider_object_id,revision,observed_at,available_at,received_at,"
                    "schema_version,payload) VALUES "
                    "('00000000-0000-7000-8000-000000000032','fixture','snapshot','v1',now(),"
                    "now(),now(),'1','{}'); "
                    "INSERT INTO research_evidence_refs "
                    "(id,source,provider_object_id,revision,available_at,location,content_hash,"
                    "byte_length) "
                    "VALUES ('00000000-0000-7000-8000-000000000033',"
                    "'fosun_security_classification','classification:test','v1',now(),"
                    "'memory://sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',"
                    "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',1); "
                    "ALTER TABLE agent_data_snapshot_evidence DISABLE TRIGGER USER; "
                    "INSERT INTO agent_data_snapshot_evidence "
                    "(data_snapshot_id,instrument_id,evidence_ref_id,authority_kind,selected_at,"
                    "source,provider_object_id,revision,available_at,location,content_hash) VALUES "
                    "('00000000-0000-7000-8000-000000000032',"
                    "'00000000-0000-7000-8000-000000000031',"
                    "'00000000-0000-7000-8000-000000000033','research',NULL,"
                    "'fosun_security_classification','classification:test','v1',now(),"
                    "'memory://sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',"
                    "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'); "
                    "ALTER TABLE agent_data_snapshot_evidence ENABLE TRIGGER USER"
            )
            for statement in script.split("; "):
                await connection.execute(text(statement))
    finally:
        await engine.dispose()
