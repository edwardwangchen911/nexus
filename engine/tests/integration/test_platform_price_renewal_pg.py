"""Exact SQL identities, irreversible facts and rollback for the renewed window."""

import asyncio
import json
from copy import deepcopy
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from integration.test_platform_producer_pg import setup
from nexus.agents.comparison_manifest import PLATFORM_PURPOSE, parse_comparison_manifest
from nexus.agents.comparison_preparation import prepare_archive
from nexus.agents.comparison_registration import register_comparison
from nexus.agents.platform_profile import platform_profile_hash
from nexus.agents.platform_usage import CURRENT_PLATFORM_PRICE_VERSION, PLATFORM_PRICE_VERSION
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text


@pytest.mark.asyncio
async def test_sql_renewal_matches_frozen_profiles_and_forbids_downgrade(
    migrated_database_url, monkeypatch
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        _, _, manifest, _, _, _ = await setup(
            factory, monkeypatch, price_version=CURRENT_PLATFORM_PRICE_VERSION
        )
        payload = manifest.model_dump(mode="json")
        old = deepcopy(payload)
        old["api_config_hash"] = platform_profile_hash()
        old["platform_contract"] = "platform-gifted-terra-v1-20261031"
        for allocation in old["allocations"]:
            allocation["profile"]["price_version"] = PLATFORM_PRICE_VERSION
        mixed = deepcopy(payload)
        mixed["allocations"][1]["profile"]["price_version"] = PLATFORM_PRICE_VERSION
        altered = deepcopy(payload)
        altered["allocations"][0]["profile"]["cache_ttl"] = "12h"
        async with factory() as session:
            for value, valid in ((payload, True), (old, True), (mixed, False), (altered, False)):
                assert (
                    await session.scalar(
                        text("SELECT research_platform_manifest_valid(CAST(:p AS jsonb))"),
                        {"p": json.dumps(value)},
                    )
                    is valid
                )
            before = (
                await session.scalars(
                    text("SELECT to_jsonb(m) FROM agent_model_configs m ORDER BY id")
                )
            ).all()
        with pytest.raises(
            RuntimeError, match="PLATFORM_RENEWAL_COMPATIBLE_READER_ROLLBACK_REQUIRED"
        ):
            await asyncio.to_thread(
                command.downgrade, Config("engine/alembic.ini"), "0106_platform_qualification"
            )
        async with factory() as session:
            assert (
                await session.scalar(text("SELECT version_num FROM alembic_version"))
                == CURRENT_MIGRATION_HEAD
            )
            assert (
                await session.scalars(
                    text("SELECT to_jsonb(m) FROM agent_model_configs m ORDER BY id")
                )
            ).all() == before
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_empty_renewal_downgrade_and_reupgrade_keep_exact_old_functions(
    migrated_database_url,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        # The renewal assertion compares the original v1 function. 0110 has
        # its own versioned-schema migration/rollback regression; capture the
        # historical function before that intentional predicate extension.
        await asyncio.to_thread(
            command.downgrade, Config("engine/alembic.ini"), "0109_platform_third_round"
        )
        async with factory() as session:
            expected = await session.scalar(
                text(
                    "SELECT pg_get_functiondef("
                    "'research_platform_manifest_v1_valid(jsonb)'::regprocedure)"
                )
            )
        await asyncio.to_thread(
            command.downgrade, Config("engine/alembic.ini"), "0106_platform_qualification"
        )
        async with factory() as session:
            actual = await session.scalar(
                text(
                    "SELECT pg_get_functiondef("
                    "'research_platform_manifest_valid(jsonb)'::regprocedure)"
                )
            )
            assert actual == expected.replace(
                "research_platform_manifest_v1_valid", "research_platform_manifest_valid"
            )
        await asyncio.to_thread(command.upgrade, Config("engine/alembic.ini"), "head")
        async with factory() as session:
            assert (
                await session.scalar(text("SELECT version_num FROM alembic_version"))
                == CURRENT_MIGRATION_HEAD
            )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_consumed_old_purpose_coexists_with_exactly_one_new_registration(
    migrated_database_url, monkeypatch
):
    import hashlib

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        source, _, new, _, _, _ = await setup(
            factory, monkeypatch, price_version=CURRENT_PLATFORM_PRICE_VERSION
        )
        prepared = prepare_archive(
            source,
            expected_source_hash=hashlib.sha256(source).hexdigest(),
            experiment_id=uuid4(),
            purpose=PLATFORM_PURPOSE,
        )
        data = new.model_dump(mode="json")
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
        data.update(platform_contract=PLATFORM_PURPOSE, api_config_hash=platform_profile_hash())
        old = parse_comparison_manifest(data)
        async with factory.begin() as session:
            await register_comparison(session, old)
        async with factory() as session:
            before = await session.scalar(
                text("SELECT to_jsonb(m) FROM research_comparison_manifest m WHERE id=:id"),
                {"id": old.experiment_id},
            )
        async with factory.begin() as session:
            await register_comparison(session, new)
        async with factory.begin() as session:
            with pytest.raises(ValueError, match="COMPARISON_REGISTRATION_CONFLICT"):
                await register_comparison(
                    session, new.model_copy(update={"experiment_id": uuid4()})
                )
            with pytest.raises(ValueError, match="COMPARISON_REGISTRATION_CONFLICT"):
                await register_comparison(
                    session, old.model_copy(update={"experiment_id": uuid4()})
                )
        async with factory() as session:
            assert (
                await session.scalar(text("SELECT count(*) FROM research_comparison_manifest")) == 2
            )
            assert (
                await session.scalar(
                    text("SELECT to_jsonb(m) FROM research_comparison_manifest m WHERE id=:id"),
                    {"id": old.experiment_id},
                )
                == before
            )
            assert await session.scalar(text("SELECT count(*) FROM research_comparison_chain")) == 0
            assert (
                await session.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 0
            )
    finally:
        await factory.kw["bind"].dispose()
