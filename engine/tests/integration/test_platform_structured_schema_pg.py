"""Versioned wire admission, historical rows and compatible rollback in real PG."""

import asyncio
import copy
import hashlib
import json
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from integration.test_platform_producer_pg import setup
from nexus.agents.comparison_manifest import PLATFORM_THIRD_PURPOSE, parse_comparison_manifest
from nexus.agents.comparison_preparation import prepare_archive
from nexus.agents.comparison_registration import register_comparison
from nexus.agents.platform_usage import CURRENT_PLATFORM_PRICE_VERSION
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text


@pytest.mark.asyncio
async def test_old_rows_survive_v2_migration_and_new_rows_require_compatible_rollback(
    migrated_database_url, monkeypatch
):
    # This historical contract belongs to 0110, independently of later heads.
    await asyncio.to_thread(
        command.downgrade, Config("engine/alembic.ini"), "0110_platform_structured_schema"
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        source, _, old, _, held_id, protected = await setup(
            factory, monkeypatch, price_version=CURRENT_PLATFORM_PRICE_VERSION
        )
        async with factory.begin() as session:
            await register_comparison(session, old)
        prepared = prepare_archive(
            source,
            expected_source_hash=hashlib.sha256(source).hexdigest(),
            experiment_id=uuid4(),
            purpose=PLATFORM_THIRD_PURPOSE,
            platform_price_version=CURRENT_PLATFORM_PRICE_VERSION,
        )
        data = old.model_dump(mode="json")
        data.update(
            {
                k: prepared[k]
                for k in (
                    "experiment_id",
                    "source_manifest_hash",
                    "input_hashes",
                    "instrument_ids",
                    "identity_equivalence_hash",
                    "outbound_safety_hash",
                    "allocations",
                )
            }
        )
        data["platform_contract"] = PLATFORM_THIRD_PURPOSE
        new = parse_comparison_manifest(data)

        async def valid(payload):
            async with factory() as session, session.begin():
                await session.execute(text("SELECT pg_catalog.set_config('search_path','',true)"))
                return await session.scalar(
                    text("SELECT public.research_platform_manifest_valid(CAST(:p AS jsonb))"),
                    {"p": json.dumps(payload)},
                )

        assert await valid(old.model_dump(mode="json")) is True
        assert await valid(data) is True
        mixed = copy.deepcopy(data)
        mixed["allocations"][1]["units_version"] = old.allocations[0].units_version
        assert await valid(mixed) is False
        invalid = copy.deepcopy(data)
        invalid["allocations"][0]["units_version"] = "unregistered-version"
        assert await valid(invalid) is False
        async with factory() as session:
            before = (
                await session.scalars(
                    text("SELECT to_jsonb(m) FROM research_comparison_manifest m ORDER BY id")
                )
            ).all()
        await asyncio.to_thread(
            command.downgrade, Config("engine/alembic.ini"), "0109_platform_third_round"
        )
        assert await valid(old.model_dump(mode="json")) is True
        assert await valid(data) is False
        await asyncio.to_thread(
            command.upgrade, Config("engine/alembic.ini"), "0110_platform_structured_schema"
        )
        assert await valid(data) is True
        async with factory() as session:
            assert (
                await session.scalars(
                    text("SELECT to_jsonb(m) FROM research_comparison_manifest m ORDER BY id")
                )
            ).all() == before
        async with factory.begin() as session:
            await register_comparison(session, new)
        with pytest.raises(
            RuntimeError, match="PLATFORM_STRUCTURED_COMPATIBLE_READER_ROLLBACK_REQUIRED"
        ):
            await asyncio.to_thread(
                command.downgrade, Config("engine/alembic.ini"), "0109_platform_third_round"
            )
        async with factory() as session:
            assert (
                await session.scalar(text("SELECT version_num FROM alembic_version"))
                == "0110_platform_structured_schema"
            )
            assert (
                await session.scalar(
                    text("SELECT to_jsonb(b) FROM agent_daily_budget_reservations b WHERE id=:id"),
                    {"id": held_id},
                )
                == protected
            )
    finally:
        await factory.kw["bind"].dispose()
