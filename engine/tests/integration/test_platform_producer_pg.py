"""Synthetic source, isolated PG: the normal producer retains native budget and identity guards."""

import asyncio
import hashlib
import json
from datetime import timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from integration.test_comparison_producer_pg import seed
from nexus.agents import platform_profile
from nexus.agents.comparison_manifest import (
    PLATFORM_ACTOR,
    PLATFORM_PURPOSE,
    PLATFORM_RENEWED_PURPOSE,
    parse_comparison_manifest,
)
from nexus.agents.comparison_preparation import prepare_archive
from nexus.agents.comparison_producer import admit_comparison_position
from nexus.agents.comparison_registration import register_comparison
from nexus.agents.persistence import BudgetExhausted
from nexus.agents.platform_manifest import PlatformManifest
from nexus.agents.platform_usage import CURRENT_PLATFORM_PRICE_VERSION, PLATFORM_PRICE_VERSION
from nexus.agents.research_only import require_formal_research
from nexus.agents.research_stages.routes import selected_route
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError


async def setup(
    factory, monkeypatch, *, source_kind="fixture", price_version=PLATFORM_PRICE_VERSION
):
    purpose = (
        PLATFORM_RENEWED_PURPOSE
        if price_version == CURRENT_PLATFORM_PRICE_VERSION
        else PLATFORM_PURPOSE
    )
    source, _, old, _, held_id = await seed(factory, source_kind=source_kind)
    prepared = prepare_archive(
        source,
        expected_source_hash=hashlib.sha256(source).hexdigest(),
        experiment_id=uuid4(),
        purpose=purpose,
        platform_price_version=price_version,
    )
    manifest = parse_comparison_manifest(
        {
            **old.model_dump(),
            **{
                k: prepared[k]
                for k in (
                    "experiment_id",
                    "source_manifest_hash",
                    "input_hashes",
                    "instrument_ids",
                    "identity_equivalence_hash",
                    "outbound_safety_hash",
                )
            },
            "api_config_hash": platform_profile.platform_profile_hash(price_version),
            "platform_contract": purpose,
            "allocations": prepared["allocations"],
            "funding_proof_hash": "1" * 64,
            "credential_binding_hash": "2" * 64,
        }
    )
    async with factory() as s, s.begin():
        now = await s.scalar(text("SELECT clock_timestamp()"))
        # This test exercises registration/budget, not the separate fixed price expiry gate.
        fields = platform_profile.platform_model_fields(price_version)
        fields.update(
            pricing_available_at=now - timedelta(days=1), pricing_expires_at=now + timedelta(days=1)
        )
        original_fields = platform_profile.platform_model_fields
        monkeypatch.setattr(
            platform_profile,
            "platform_model_fields",
            lambda version=price_version: fields
            if version == price_version
            else original_fields(version),
        )
        model = await platform_profile.register_platform_profile(s, version=price_version)
        model_id = model.id
        prepared["rebuilt_at"] = now.isoformat()
        protected = await s.scalar(
            text("SELECT to_jsonb(b) FROM agent_daily_budget_reservations b WHERE id=:id"),
            {"id": held_id},
        )
    return source, prepared, manifest, model_id, held_id, protected


@pytest.mark.asyncio
@pytest.mark.parametrize("platform", [False, True], ids=["legacy", "platform"])
async def test_maintenance_refuses_comparison_admission_without_any_new_identity(
    migrated_database_url, monkeypatch, tmp_path, platform
):
    from nexus.agents import research_maintenance

    f = session_factory(EngineSettings(database_url=migrated_database_url))
    funding = None
    if platform:
        from integration.test_platform_funding_pg import funding_fixture

        source, prepared, m, model, held, _ = await setup(f, monkeypatch)
        m, funding = await funding_fixture(f, m, tmp_path, monkeypatch)
    else:
        source, prepared, m, model, held = await seed(f)

    async def snapshot():
        async with f() as s:
            return {
                table: (
                    await s.execute(
                        text(f"SELECT to_jsonb(t) FROM {table} t ORDER BY to_jsonb(t)::text")
                    )
                )
                .scalars()
                .all()
                for table in (
                    "research_comparison_manifest",
                    "research_comparison_chain",
                    "research_stage_scope",
                    "research_stage_member",
                    "jobs",
                    "job_attempts",
                    "job_leases",
                    "agent_research_runs",
                    "agent_daily_budget_reservations",
                    "data_snapshots",
                    "agent_data_snapshot_evidence",
                    "agent_research_outputs",
                    "agent_research_events",
                )
            }

    try:
        before = await snapshot()
        monkeypatch.setattr(research_maintenance, "IMAGE_MAINTENANCE", True)
        # Catch then commit: maintenance must refuse before even temporary writes.
        async with f() as s, s.begin():
            with pytest.raises(ValueError, match="^RESEARCH_MAINTENANCE$"):
                await admit_comparison_position(
                    s,
                    source=source,
                    prepared=prepared,
                    manifest=m,
                    ordinal=1,
                    model_config_id=model,
                    daily_limit_usd=Decimal(10),
                    platform_funding=funding,
                )
        after = await snapshot()
        assert after == before
        assert after["research_comparison_manifest"] == []
        assert after["research_comparison_chain"] == []
        old = next(x for x in after["agent_daily_budget_reservations"] if x["id"] == str(held))
        assert old["state"] == "reserved" and old["settled_usd"] is None
    finally:
        await f.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_normal_platform_producer_binds_three_allocations_and_preserves_unknown(
    migrated_database_url, monkeypatch, tmp_path
):
    f = session_factory(EngineSettings(database_url=migrated_database_url))
    source, prepared, m, model, held, protected = await setup(f, monkeypatch)
    from integration.test_platform_funding_pg import funding_fixture

    m, funding = await funding_fixture(f, m, tmp_path, monkeypatch)

    async def admit(i, limit=Decimal(10)):
        async with f() as s, s.begin():
            return await admit_comparison_position(
                s,
                source=source,
                prepared=prepared,
                manifest=m,
                ordinal=i,
                model_config_id=model,
                daily_limit_usd=limit,
                platform_funding=funding,
            )

    try:
        with pytest.raises(BudgetExhausted):
            await admit(1, Decimal(2))
        async with f() as s:
            assert await s.scalar(text("SELECT count(*) FROM research_comparison_manifest")) == 0
        results = [await admit(i) for i in (1, 2, 3)]
        async with f() as s:
            for i, result in enumerate(results, 1):
                run = await s.get(AgentResearchRunRecord, result.run_id)
                assert (
                    run.actor_id == PLATFORM_ACTOR and run.reserved_cost_usd == m.reservation_for(i)
                )
                assert run.settled_cost_usd is None and run.input_tokens is None
                route = await selected_route(s, result.job_id)
                assert route["kind"] == "comparison" and route["scope_id"] == m.experiment_id
                assert (
                    await s.scalar(
                        text("SELECT research_comparison_binding(:r,:j,:e)"),
                        {"r": result.run_id, "j": result.job_id, "e": m.experiment_id},
                    )
                    is True
                )
                with pytest.raises(ValueError, match="BUSINESS_CONSUMPTION_FORBIDDEN"):
                    await require_formal_research(s, run)
            assert (
                await s.scalar(
                    text("SELECT to_jsonb(b) FROM agent_daily_budget_reservations b WHERE id=:id"),
                    {"id": held},
                )
                == protected
            )
            reserved = list(await s.scalars(select(AgentDailyBudgetReservationRecord)))
            assert sum(b.reserved_usd for b in reserved) == Decimal(2) + m.total_reservation_usd
            payload = await s.scalar(
                text("SELECT payload FROM research_comparison_manifest WHERE id=:id"),
                {"id": m.experiment_id},
            )
            assert parse_comparison_manifest(payload).digest == m.digest
        with pytest.raises(ValueError, match="POSITION_CONSUMED"):
            await admit(1)
        with pytest.raises(ValueError, match="POSITION_INVALID"):
            await admit(4)
        changed = PlatformManifest.model_validate({**m.model_dump(), "experiment_id": uuid4()})
        with pytest.raises(ValueError, match="REGISTRATION_CONFLICT"):
            async with f() as s, s.begin():
                await register_comparison(s, changed)
    finally:
        await f.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_concurrent_platform_producers_only_reserve_once(
    migrated_database_url, monkeypatch, tmp_path
):
    f = session_factory(EngineSettings(database_url=migrated_database_url))
    source, prepared, m, model, _, _ = await setup(f, monkeypatch)
    from integration.test_platform_funding_pg import funding_fixture

    m, funding = await funding_fixture(f, m, tmp_path, monkeypatch)
    barrier = asyncio.Barrier(2)

    async def admit():
        await barrier.wait()
        async with f() as s, s.begin():
            return await admit_comparison_position(
                s,
                source=source,
                prepared=prepared,
                manifest=m,
                ordinal=1,
                model_config_id=model,
                daily_limit_usd=Decimal(10),
                platform_funding=funding,
            )

    try:
        results = await asyncio.gather(admit(), admit(), return_exceptions=True)
        assert sum(not isinstance(r, BaseException) for r in results) == 1
        assert [str(r) for r in results if isinstance(r, ValueError)] == [
            "COMPARISON_POSITION_CONSUMED"
        ]
        async with f() as s:
            assert await s.scalar(text("SELECT count(*) FROM research_comparison_chain")) == 1
    finally:
        await f.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_new_purpose_sql_and_downgrade_guards_preserve_frozen_registry(
    migrated_database_url, monkeypatch
):
    f = session_factory(EngineSettings(database_url=migrated_database_url))
    _, _, m, _, held, protected = await setup(f, monkeypatch)
    try:
        for fault in ("purpose", "funding", "input", "output", "profile"):
            payload = json.loads(m.model_dump_json())
            purpose = PLATFORM_PURPOSE
            if fault == "purpose":
                purpose += "-another"
            elif fault == "funding":
                del payload["funding_proof_hash"]
            elif fault == "input":
                payload["allocations"][0]["stage_input_upper_bounds"][0] = 272001
            elif fault == "output":
                payload["allocations"][0]["output_limit"] = 8193
            elif fault == "profile":
                payload["allocations"][0]["profile"]["cache_ttl"] = "12h"
            with pytest.raises(DBAPIError):
                async with f() as s, s.begin():
                    await s.execute(
                        text(
                            "INSERT INTO research_comparison_manifest "
                            "(id,purpose,manifest_hash,payload) "
                            "VALUES (:id,:p,:h,CAST(:body AS jsonb))"
                        ),
                        {
                            "id": m.experiment_id,
                            "p": purpose,
                            "h": m.digest,
                            "body": json.dumps(payload),
                        },
                    )
        async with f() as s, s.begin():
            await register_comparison(s, m)
        with pytest.raises(RuntimeError, match="PLATFORM_COMPATIBLE_READER_ROLLBACK_REQUIRED"):
            await asyncio.to_thread(
                command.downgrade, Config("engine/alembic.ini"), "0105_comparison_preclaim"
            )
        async with f() as s:
            assert (
                await s.scalar(text("SELECT version_num FROM alembic_version"))
                == CURRENT_MIGRATION_HEAD
            )
            assert (
                await s.scalar(
                    text("SELECT manifest_hash FROM research_comparison_manifest WHERE id=:id"),
                    {"id": m.experiment_id},
                )
                == m.digest
            )
            assert (
                await s.scalar(
                    text("SELECT to_jsonb(b) FROM agent_daily_budget_reservations b WHERE id=:id"),
                    {"id": held},
                )
                == protected
            )
    finally:
        await f.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_platform_empty_migration_restores_exact_legacy_binding(migrated_database_url):
    f = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        await asyncio.to_thread(
            command.downgrade, Config("engine/alembic.ini"), "0105_comparison_preclaim"
        )
        async with f() as s:
            old = await s.scalar(
                text(
                    "SELECT pg_get_functiondef("
                    "'research_comparison_binding(uuid,uuid,uuid)'::regprocedure)"
                )
            )
        await asyncio.to_thread(command.upgrade, Config("engine/alembic.ini"), "head")
        await asyncio.to_thread(
            command.downgrade, Config("engine/alembic.ini"), "0105_comparison_preclaim"
        )
        async with f() as s:
            assert (
                await s.scalar(
                    text(
                        "SELECT pg_get_functiondef("
                        "'research_comparison_binding(uuid,uuid,uuid)'::regprocedure)"
                    )
                )
                == old
            )
        await asyncio.to_thread(command.upgrade, Config("engine/alembic.ini"), "head")
    finally:
        await f.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_normal_profile_commands_select_explicit_platform_purpose(
    migrated_database_url, monkeypatch, tmp_path
):
    from argparse import Namespace

    from nexus.ops import openai_local, research_comparison

    f = session_factory(EngineSettings(database_url=migrated_database_url))
    _, _, _, model_id, _, _ = await setup(f, monkeypatch)
    monkeypatch.setattr(
        openai_local,
        "local_settings",
        lambda *a, **k: EngineSettings(database_url=migrated_database_url),
    )
    for command_name in ("register-profile", "inspect-profile"):
        result = await research_comparison.native_command(
            Namespace(
                command=command_name,
                repo_root=tmp_path,
                purpose=PLATFORM_PURPOSE,
                sdk_model_config_id=None,
            )
        )
        assert result["model_config_id"] == str(model_id)
        assert result["dispatch_authority"] is False
        assert result["version"] == platform_profile.platform_profile()["version"]
    await f.kw["bind"].dispose()
