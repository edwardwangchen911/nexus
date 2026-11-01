"""Exact medium identity, immutable low history and compatible rollback."""

import asyncio
import copy
import json

import pytest
from alembic import command
from alembic.config import Config
from integration.test_platform_producer_pg import setup
from nexus.agents.comparison_manifest import PLATFORM_QUALIFICATION_PURPOSE
from nexus.agents.comparison_registration import register_comparison
from nexus.agents.platform_schema import CURRENT_PLATFORM_UNITS_VERSION
from nexus.agents.platform_usage import MEDIUM_PLATFORM_PRICE_VERSION
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text


@pytest.mark.asyncio
async def test_medium_exact_sql_binding_and_downgrade_retains_facts(
    migrated_database_url, monkeypatch
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        _, _, manifest, _, held, protected = await setup(
            factory,
            monkeypatch,
            price_version=MEDIUM_PLATFORM_PRICE_VERSION,
            purpose=PLATFORM_QUALIFICATION_PURPOSE,
            units_version=CURRENT_PLATFORM_UNITS_VERSION,
        )
        async with factory.begin() as s:
            await register_comparison(s, manifest, allow_new_round=True)
            await s.execute(text("SET LOCAL search_path=''"))
            for fault in (
                None,
                "effort",
                "mixed_version",
                "purpose",
                "hash",
                "limit",
                "instruction",
                "old_schema",
            ):
                payload = copy.deepcopy(manifest.model_dump(mode="json"))
                if fault == "effort":
                    payload["allocations"][0]["profile"]["effort"] = "low"
                elif fault == "mixed_version":
                    payload["allocations"][1]["profile"]["price_version"] = (
                        "platform-terra-default-implicit30m-20261101-v2"
                    )
                elif fault == "purpose":
                    payload["platform_contract"] = "platform-terra-v3-20261101"
                elif fault == "hash":
                    payload["api_config_hash"] = "0" * 64
                elif fault == "limit":
                    payload["allocations"][0]["output_limit"] = 8193
                elif fault == "instruction":
                    payload["allocations"][0]["profile"]["developer_instructions_hash"] = "0" * 64
                elif fault == "old_schema":
                    payload["allocations"][0]["units_version"] = (
                        "platform-full-http-utf8-plus8192-v1"
                    )
                assert await s.scalar(
                    text("SELECT public.research_platform_manifest_valid(CAST(:p AS jsonb))"),
                    {"p": json.dumps(payload)},
                ) is (fault is None)
        with pytest.raises(
            RuntimeError, match="PLATFORM_MEDIUM_COMPATIBLE_READER_ROLLBACK_REQUIRED"
        ):
            await asyncio.to_thread(
                command.downgrade, Config("engine/alembic.ini"), "0111_platform_round_registry"
            )
        async with factory() as s:
            assert (
                await s.scalar(text("SELECT version_num FROM alembic_version"))
                == CURRENT_MIGRATION_HEAD
            )
            assert await s.scalar(
                text("SELECT payload FROM research_comparison_manifest WHERE id=:id"),
                {"id": manifest.experiment_id},
            ) == manifest.model_dump(mode="json")
            assert (
                await s.scalar(
                    text("SELECT to_jsonb(b) FROM agent_daily_budget_reservations b WHERE id=:id"),
                    {"id": held},
                )
                == protected
            )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_empty_medium_migration_roundtrip_preserves_old_function_definitions(
    migrated_database_url,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    config = Config("engine/alembic.ini")
    signatures = (
        "research_platform_manifest_valid(jsonb)",
        "research_comparison_binding(uuid,uuid,uuid)",
        "research_platform_stage_fact_guard()",
    )

    async def definitions():
        async with factory() as s:
            return [
                await s.scalar(
                    text("SELECT pg_get_functiondef(CAST(:s AS regprocedure))"), {"s": name}
                )
                for name in signatures
            ]

    try:
        await asyncio.to_thread(command.downgrade, config, "0111_platform_round_registry")
        before = await definitions()
        await asyncio.to_thread(command.upgrade, config, "head")
        await asyncio.to_thread(command.downgrade, config, "0111_platform_round_registry")
        assert await definitions() == before
        await asyncio.to_thread(command.upgrade, config, "head")
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "version", ["platform-terra-default-implicit30m-20261101-v2", MEDIUM_PLATFORM_PRICE_VERSION]
)
async def test_send_authorization_rejects_rehashed_effort_change(
    migrated_database_url, monkeypatch, tmp_path, version
):
    import hashlib
    from dataclasses import replace

    from integration.test_platform_stage_accounting_pg import native_platform
    from nexus.agents.platform_wire import _hash

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        result, manifest, claim, protocol, now, compiled, _, _ = await native_platform(
            factory,
            monkeypatch,
            tmp_path,
            price_version=version,
        )
        body = json.loads(compiled.wire)
        body["reasoning"]["effort"] = (
            "low" if version == MEDIUM_PLATFORM_PRICE_VERSION else "medium"
        )
        wire = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        digest = hashlib.sha256(wire).hexdigest()
        forged = replace(
            compiled,
            wire=wire,
            body_sha256=digest,
            input_units=len(wire) + manifest.allocations[0].framing_units,
            binding_hash=_hash(
                dict(
                    allocation_hash=compiled.allocation_hash,
                    stage=compiled.stage,
                    body_sha256=digest,
                )
            ),
        )
        # The low first-stage envelope is exact: the longer word "medium" is
        # rejected by its original byte bound before checking frozen effort.
        # The shorter low body fits medium's envelope and reaches effort binding.
        rejection = (
            "PLATFORM_AUTHORIZATION_BINDING_INVALID"
            if version == MEDIUM_PLATFORM_PRICE_VERSION
            else "PLATFORM_WIRE_CHANGED"
        )
        with pytest.raises(ValueError, match=rejection):
            await protocol.authorize(claim, stage=1, now=now, wire=wire, platform_wire=forged)
        async with factory() as s:
            assert (
                await s.scalar(
                    text("SELECT count(*) FROM research_stage_authorization WHERE run_id=:id"),
                    {"id": result.run_id},
                )
                == 0
            )
        await protocol.authorize(
            claim, stage=1, now=now, wire=compiled.wire, platform_wire=compiled
        )
    finally:
        await factory.kw["bind"].dispose()
