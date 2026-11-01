"""Stable registration retains legacy identities and requires a compatible reader."""

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
from nexus.agents.platform_schema import (
    CURRENT_PLATFORM_UNITS_VERSION,
    LEGACY_PLATFORM_UNITS_VERSION,
)
from nexus.agents.platform_usage import CURRENT_PLATFORM_PRICE_VERSION
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text


@pytest.mark.asyncio
async def test_stable_registration_preserves_legacy_uniqueness_and_requires_compatible_rollback(
    migrated_database_url, monkeypatch
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        source, _, old, _, _, _ = await setup(
            factory, monkeypatch, price_version=CURRENT_PLATFORM_PRICE_VERSION
        )

        def build(purpose, units):
            prepared = prepare_archive(
                source,
                expected_source_hash=hashlib.sha256(source).hexdigest(),
                experiment_id=uuid4(),
                purpose=purpose,
                platform_price_version=CURRENT_PLATFORM_PRICE_VERSION,
                platform_units_version=units,
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
            data["platform_contract"] = purpose
            return parse_comparison_manifest(data)

        third = build("platform-terra-v3-20261101", LEGACY_PLATFORM_UNITS_VERSION)
        for manifest in (old, third):
            async with factory.begin() as session:
                await register_comparison(session, manifest)
        async with factory() as session:
            before = (
                await session.scalars(
                    text("SELECT to_jsonb(m) FROM research_comparison_manifest m ORDER BY id")
                )
            ).all()
        await asyncio.to_thread(
            command.downgrade, Config("engine/alembic.ini"), "0110_platform_structured_schema"
        )
        await asyncio.to_thread(command.upgrade, Config("engine/alembic.ini"), "head")
        async with factory() as session:
            assert (
                await session.scalars(
                    text("SELECT to_jsonb(m) FROM research_comparison_manifest m ORDER BY id")
                )
            ).all() == before
        new = build("platform-qualification-v1", CURRENT_PLATFORM_UNITS_VERSION)
        async with factory.begin() as session:
            await register_comparison(session, new, allow_new_round=True)
        async with factory() as session, session.begin():
            await session.execute(text("SET LOCAL search_path=''"))
            assert (
                await session.scalar(
                    text("SELECT public.research_platform_manifest_valid(CAST(:p AS jsonb))"),
                    {"p": new.model_dump_json()},
                )
                is True
            )
            invalid = new.model_dump(mode="json")
            invalid["api_config_hash"] = "0" * 64
            assert (
                await session.scalar(
                    text("SELECT public.research_platform_manifest_valid(CAST(:p AS jsonb))"),
                    {"p": json.dumps(invalid)},
                )
                is False
            )
        second = build("platform-qualification-v1", CURRENT_PLATFORM_UNITS_VERSION)
        async with factory.begin() as session:
            await register_comparison(session, second, allow_new_round=True)
            await register_comparison(session, new, allow_new_round=True)
        for manifest in (old, third):
            async with factory.begin() as session:
                with pytest.raises(ValueError, match="COMPARISON_REGISTRATION_CONFLICT"):
                    await register_comparison(
                        session, manifest.model_copy(update={"experiment_id": uuid4()})
                    )
        async with factory.begin() as session:
            with pytest.raises(ValueError, match="COMPARISON_REGISTRATION_CONFLICT"):
                await register_comparison(
                    session, new.model_copy(update={"scoring_hash": "9" * 64})
                )
        with pytest.raises(
            RuntimeError, match="PLATFORM_QUALIFICATION_COMPATIBLE_READER_ROLLBACK_REQUIRED"
        ):
            await asyncio.to_thread(
                command.downgrade, Config("engine/alembic.ini"), "0110_platform_structured_schema"
            )
        async with factory() as session:
            assert (
                await session.scalar(text("SELECT version_num FROM alembic_version"))
                == CURRENT_MIGRATION_HEAD
            )
            after = (
                await session.scalars(
                    text("SELECT to_jsonb(m) FROM research_comparison_manifest m ORDER BY id")
                )
            ).all()
            assert len(after) == 4 and all(row in after for row in before)
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("commit_mode", ["normal", "lost_ack", "not_applied"])
async def test_normal_round_registration_is_repeatable_without_new_deployment(
    migrated_database_url, monkeypatch, tmp_path, commit_mode
):
    import argparse
    from datetime import timedelta

    from integration.test_platform_funding_pg import funding_fixture
    from nexus.agents.platform_profile import platform_model_fields
    from nexus.ops import platform_round_registration as entry

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        source, _, old, model, held_id, protected = await setup(
            factory, monkeypatch, price_version=CURRENT_PLATFORM_PRICE_VERSION
        )
        old, _ = await funding_fixture(factory, old, tmp_path, monkeypatch, explicit_spend=True)

        def private(name, raw):
            path = tmp_path / name
            path.write_bytes(raw)
            path.chmod(0o600)
            return path

        source_path = private("source.json", source)
        template = private("template.json", old.model_dump_json().encode())
        policy = private("task-policy.json", b"Synthetic bounded multi-round task policy.")
        diagnosis = private("diagnosis.txt", b"Synthetic verified fix; never blindly retry.")
        async with factory() as session:
            now = await session.scalar(text("SELECT clock_timestamp()"))
        fields = platform_model_fields(CURRENT_PLATFORM_PRICE_VERSION)
        fields.update(
            pricing_available_at=now - timedelta(hours=1),
            pricing_expires_at=now + timedelta(hours=1),
        )
        monkeypatch.setattr(entry, "platform_model_fields", lambda version: fields)

        def args():
            identity = uuid4()
            return argparse.Namespace(
                repo_root=tmp_path,
                source=source_path,
                source_sha256=hashlib.sha256(source).hexdigest(),
                template=template,
                task_policy=policy,
                task_policy_sha256=hashlib.sha256(policy.read_bytes()).hexdigest(),
                continuation_evidence=diagnosis,
                implementation_sha="f" * 40,
                experiment_id=identity,
                output=tmp_path / str(identity),
            )

        first, second = args(), args()
        if commit_mode != "normal":
            from sqlalchemy.ext.asyncio import AsyncSessionTransaction

            original_commit = AsyncSessionTransaction.commit

            async def interrupted_commit(transaction):
                if commit_mode == "lost_ack":
                    await original_commit(transaction)
                raise ConnectionError("synthetic registration acknowledgement lost")

            monkeypatch.setattr(AsyncSessionTransaction, "commit", interrupted_commit)
        expected_status = (
            "registration_unconfirmed" if commit_mode == "not_applied" else "registered"
        )
        for item in (first, second):
            result = await entry.register_round(factory, item)
            assert result["status"] == expected_status and result["dispatch_authority"] is False
            assert result["budget_status"] == "bound_frozen_positions_unreserved"

        def forbidden(*a, **kw):
            raise AssertionError("Readback must not renew the frozen key, window or budget")

        monkeypatch.setattr(entry, "_freeze", forbidden)
        assert (await entry.register_round(factory, first))["status"] == expected_status
        alternate = argparse.Namespace(**vars(first))
        alternate.output = tmp_path / "another-export"
        with monkeypatch.context() as readback:
            readback.setattr(entry, "register_comparison", forbidden)
            assert (await entry.register_round(factory, alternate))["status"] == expected_status
        for name in ("round.json", "manifest.json", "prepared.json", "funding.json"):
            assert (alternate.output / name).read_bytes() == (first.output / name).read_bytes()
        first.source_sha256 = "0" * 64
        with pytest.raises(ValueError, match="COMPARISON_REGISTRATION_CONFLICT"):
            await entry.register_round(factory, first)
        async with factory() as session:
            assert await session.scalar(
                text(
                    "SELECT count(*) FROM research_comparison_manifest "
                    "WHERE purpose='platform-qualification-v1'"
                )
            ) == (0 if commit_mode == "not_applied" else 2)
            assert await session.scalar(text("SELECT count(*) FROM research_comparison_chain")) == 0
            assert (
                await session.scalar(text("SELECT version_num FROM alembic_version"))
                == CURRENT_MIGRATION_HEAD
            )
            assert (
                await session.scalar(
                    text("SELECT to_jsonb(b) FROM agent_daily_budget_reservations b WHERE id=:id"),
                    {"id": held_id},
                )
                == protected
            )
        if commit_mode == "normal":
            from decimal import Decimal

            from nexus.agents.budget import BudgetExhausted
            from nexus.agents.comparison_producer import admit_comparison_position
            from nexus.agents.platform_funding import load_bound_platform_funding

            async def admit(item, limit):
                manifest = parse_comparison_manifest(
                    json.loads((item.output / "manifest.json").read_bytes())
                )
                funding = load_bound_platform_funding(
                    tmp_path, item.output / "funding.json", manifest
                )
                async with factory.begin() as session:
                    return await admit_comparison_position(
                        session,
                        source=source,
                        prepared=json.loads((item.output / "prepared.json").read_bytes()),
                        manifest=manifest,
                        ordinal=1,
                        model_config_id=model,
                        daily_limit_usd=limit,
                        platform_funding=funding,
                    )

            with pytest.raises(BudgetExhausted):
                await admit(first, Decimal(2))
            await admit(first, Decimal(10))
            with pytest.raises(ValueError, match="COMPARISON_OTHER_WORK_UNCONFIRMED"):
                await admit(second, Decimal(10))
            first.source_sha256 = hashlib.sha256(source).hexdigest()
            receipt = await entry.register_round(factory, first)
            assert receipt["budget_status"] == "native_positions_present"
            assert receipt["position_count"] == receipt["reserved_positions"] == 1
    finally:
        await factory.kw["bind"].dispose()
