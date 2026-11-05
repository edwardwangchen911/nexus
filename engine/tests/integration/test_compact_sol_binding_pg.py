"""Format selection cannot forge a different root or erase existing new facts."""

import asyncio
import hashlib
import json

import pytest
from alembic import command
from alembic.config import Config
from integration.test_sol_native_preparation_pg import admit, scope
from integration.test_sol_profile_pg import SOURCE, current_record
from nexus.agents import platform_native, platform_profile
from nexus.agents.platform_schema import (
    COMPACT_PLATFORM_UNITS_VERSION,
    CURRENT_PLATFORM_UNITS_VERSION,
)
from nexus.paper.research_request import prepare_research_request
from nexus.persistence.models.agents import AgentModelConfigRecord
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError


async def compact_model(factory, settings):
    async with factory.begin() as db:
        old = await db.get(AgentModelConfigRecord, settings.agent_paper_formal_model_config_id)
        new = await platform_profile.register_sol_business_profile(
            db,
            price_record=old.platform_profile["price_record"],
            source_evidence=SOURCE,
            units_version=COMPACT_PLATFORM_UNITS_VERSION,
        )
        return new.id


@pytest.mark.asyncio
@pytest.mark.parametrize("compact", [False, True])
async def test_rehashed_route_cannot_change_the_root_format(
    migrated_database_url, tmp_path, monkeypatch, compact
):
    factory, settings, store, _ = await scope(migrated_database_url, tmp_path)
    try:
        if compact:
            settings = settings.model_copy(
                update={
                    "agent_paper_formal_model_config_id": await compact_model(factory, settings)
                }
            )
        request_id = await admit(factory, settings, store)
        original = platform_native.freeze_native_route_binding

        async def forged(*args, **kwargs):
            binding = await original(*args, **kwargs)
            binding["allocation"]["units_version"] = (
                CURRENT_PLATFORM_UNITS_VERSION if compact else COMPACT_PLATFORM_UNITS_VERSION
            )
            binding["allocation_hash"] = hashlib.sha256(
                json.dumps(
                    binding["allocation"], sort_keys=True, ensure_ascii=False, separators=(",", ":")
                ).encode()
            ).hexdigest()
            return binding

        monkeypatch.setattr(platform_native, "freeze_native_route_binding", forged)
        async with factory.begin() as db:
            db.info["sec_exhibit_store"] = store
            with pytest.raises(DBAPIError, match="PLATFORM_NATIVE_ROUTE_INVALID"):
                await prepare_research_request(db, settings, request_id=request_id)
        async with factory() as db:
            for table in (
                "agent_research_runs",
                "jobs",
                "research_stage_route",
                "agent_daily_budget_reservations",
            ):
                assert await db.scalar(text(f"SELECT count(*) FROM {table}")) == 0
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", [None, [], "unknown-format", CURRENT_PLATFORM_UNITS_VERSION])
async def test_sql_profile_rejects_unsupported_format_even_with_a_matching_hash(
    migrated_database_url, bad
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        fields = platform_profile.sol_business_model_fields(await current_record(factory))
        fields["platform_profile"]["output_units_version"] = bad
        digest = hashlib.sha256(
            json.dumps(fields["platform_profile"], sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        fields.update(config_hash=digest, version="platform-sol-business-v1-" + digest)
        with pytest.raises(DBAPIError, match="PLATFORM_SOL_PROFILE_INVALID"):
            async with factory.begin() as db:
                db.add(AgentModelConfigRecord(**fields))
                await db.flush()
    finally:
        await factory.kw["bind"].dispose()


async def frozen_facts(factory):
    async with factory() as db:
        return {
            table: list(
                await db.scalars(text(f"SELECT to_jsonb(r)::text FROM {table} r ORDER BY id"))
            )
            for table in (
                "agent_model_configs",
                "manual_paper_requests",
                "agent_research_runs",
                "agent_daily_budget_reservations",
            )
        }


@pytest.mark.asyncio
async def test_nonempty_old_roots_survive_migration_and_new_profile_fences_destructive_downgrade(
    migrated_database_url, tmp_path, monkeypatch
):
    factory, settings, store, _ = await scope(migrated_database_url, tmp_path)
    try:
        request_id = await admit(factory, settings, store)
        async with factory.begin() as db:
            db.info["sec_exhibit_store"] = store
            assert (
                await prepare_research_request(db, settings, request_id=request_id)
            ).status == "active"
        before = await frozen_facts(factory)
        monkeypatch.setenv("DATABASE_URL", migrated_database_url)
        config = Config("engine/alembic.ini")
        await asyncio.to_thread(command.downgrade, config, "0117_budgeted_single_sol")
        assert await frozen_facts(factory) == before
        await asyncio.to_thread(command.upgrade, config, "head")
        assert await frozen_facts(factory) == before
        await compact_model(factory, settings)
        protected = await frozen_facts(factory)
        with pytest.raises(DBAPIError, match="COMPACT_SOL_DOWNGRADE_FORBIDDEN"):
            await asyncio.to_thread(command.downgrade, config, "0117_budgeted_single_sol")
        assert await frozen_facts(factory) == protected
        async with factory() as db:
            assert (
                await db.scalar(text("SELECT version_num FROM alembic_version"))
                == "0118_compact_sol_profile"
            )
    finally:
        await factory.kw["bind"].dispose()
