"""One new SQL purpose, immutable history, exact restore and guarded rollback."""

import asyncio
import hashlib
import json
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from integration.test_platform_producer_pg import setup
from nexus.agents.comparison_manifest import parse_comparison_manifest
from nexus.agents.comparison_preparation import prepare_archive
from nexus.agents.comparison_registration import register_comparison
from nexus.agents.platform_schema import LEGACY_PLATFORM_UNITS_VERSION
from nexus.agents.platform_usage import CURRENT_PLATFORM_PRICE_VERSION
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text


@pytest.mark.asyncio
async def test_new_scope_old_rows_empty_search_path_and_irreversible_registration(
    migrated_database_url, monkeypatch
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        source, _, old, _, _, _ = await setup(
            factory, monkeypatch, price_version=CURRENT_PLATFORM_PRICE_VERSION
        )
        async with factory.begin() as session:
            await register_comparison(session, old)
        async with factory() as session:
            before = (
                await session.scalars(
                    text("SELECT to_jsonb(m) FROM research_comparison_manifest m ORDER BY id")
                )
            ).all()
            v2 = await session.scalar(
                text(
                    "SELECT pg_get_functiondef("
                    "'public.research_platform_manifest_v2_valid(jsonb)'::regprocedure)"
                )
            )
        await asyncio.to_thread(
            command.downgrade, Config("engine/alembic.ini"), "0108_platform_restore_lookup"
        )
        await asyncio.to_thread(command.upgrade, Config("engine/alembic.ini"), "head")
        async with factory() as session:
            assert (
                await session.scalars(
                    text("SELECT to_jsonb(m) FROM research_comparison_manifest m ORDER BY id")
                )
            ).all() == before
            assert (
                await session.scalar(
                    text(
                        "SELECT pg_get_functiondef("
                        "'public.research_platform_manifest_v2_valid(jsonb)'::regprocedure)"
                    )
                )
                == v2
            )
        prepared = prepare_archive(
            source,
            expected_source_hash=hashlib.sha256(source).hexdigest(),
            experiment_id=uuid4(),
            purpose="platform-terra-v3-20261101",
            platform_price_version=CURRENT_PLATFORM_PRICE_VERSION,
            platform_units_version=LEGACY_PLATFORM_UNITS_VERSION,
        )
        data = old.model_dump(mode="json")
        for key in (
            "experiment_id",
            "source_manifest_hash",
            "input_hashes",
            "instrument_ids",
            "identity_equivalence_hash",
            "outbound_safety_hash",
            "allocations",
        ):
            data[key] = prepared[key]
        data["platform_contract"] = "platform-terra-v3-20261101"
        new = parse_comparison_manifest(data)
        async with factory.begin() as session:
            await register_comparison(session, new)
        async with factory() as session, session.begin():
            await session.execute(text("SET LOCAL search_path=''"))
            assert (
                await session.scalar(
                    text("SELECT public.research_platform_manifest_valid(CAST(:p AS jsonb))"),
                    {"p": new.model_dump_json()},
                )
                is True
            )
            data["api_config_hash"] = "0" * 64
            assert (
                await session.scalar(
                    text("SELECT public.research_platform_manifest_valid(CAST(:p AS jsonb))"),
                    {"p": json.dumps(data)},
                )
                is False
            )
        for manifest in (old, new):
            async with factory.begin() as session:
                with pytest.raises(ValueError, match="COMPARISON_REGISTRATION_CONFLICT"):
                    await register_comparison(
                        session, manifest.model_copy(update={"experiment_id": uuid4()})
                    )
        with pytest.raises(
            RuntimeError, match="PLATFORM_THIRD_COMPATIBLE_READER_ROLLBACK_REQUIRED"
        ):
            await asyncio.to_thread(
                command.downgrade, Config("engine/alembic.ini"), "0108_platform_restore_lookup"
            )
        async with factory() as session:
            assert (
                await session.scalar(text("SELECT version_num FROM alembic_version"))
                == CURRENT_MIGRATION_HEAD
            )
            rows = (
                await session.scalars(
                    text("SELECT to_jsonb(m) FROM research_comparison_manifest m ORDER BY id")
                )
            ).all()
            assert len(rows) == 2 and before[0] in rows
    finally:
        await factory.kw["bind"].dispose()
