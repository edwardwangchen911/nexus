"""Isolated real PostgreSQL; normal profile registration has zero model sends."""

import asyncio
import hashlib
import json
from datetime import datetime, timedelta
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from nexus.agents import platform_profile as profiles
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.agents import AgentModelConfigRecord, platform_profile_projection
from nexus.persistence.models.runtime import AuditEvent
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import JSON, func, select, text
from sqlalchemy.exc import DBAPIError
from unit.agents.test_sol_pricing import sol_price_record as record_fixture

SOURCE = b"synthetic official-price evidence; never a real provider call"


async def current_record(factory):
    async with factory() as session:
        now = await session.scalar(text("SELECT clock_timestamp()"))
    return record_fixture() | {
        "verified_at": (now - timedelta(minutes=1)).isoformat(),
        "expires_at": (now + timedelta(days=1)).isoformat(),
        "source_evidence_sha256": hashlib.sha256(SOURCE).hexdigest(),
    }


async def register(factory, record):
    async with factory() as session, session.begin():
        result = await profiles.register_sol_business_profile(
            session, price_record=record, source_evidence=SOURCE
        )
        return result.id


@pytest.mark.asyncio
async def test_sol_registration_is_immutable_idempotent_and_audited_under_concurrency(
    migrated_database_url,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    record = await current_record(factory)
    ids = await asyncio.gather(register(factory, record), register(factory, record))
    assert ids[0] == ids[1]
    async with factory() as session:
        model = await profiles.read_sol_business_profile(session, model_config_id=ids[0])
        assert model is not None and profiles.sol_business_model_matches(model)
        assert not profiles.platform_model_matches(model)
        assert (
            await session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(AuditEvent.action == "platform.sol_profile_registered")
            )
            == 1
        )
    async with factory() as session, session.begin():
        with pytest.raises(DBAPIError):
            await session.execute(
                text("UPDATE agent_model_configs SET platform_profile=NULL WHERE id=:id"),
                {"id": ids[0]},
            )


@pytest.mark.asyncio
async def test_sol_registration_checks_db_time_and_source_without_renewing_old_records(
    migrated_database_url,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    record = await current_record(factory)
    original = await register(factory, record)
    changed = record | {"output_usd_per_million": "11", "source_evidence_sha256": "b" * 64}
    with pytest.raises(ValueError, match="PLATFORM_PRICE_SOURCE_MISMATCH"):
        await register(factory, changed)
    expired = record_fixture() | {
        "verified_at": "2020-01-01T00:00:00Z",
        "expires_at": "2020-01-08T00:00:00Z",
        "source_evidence_sha256": hashlib.sha256(SOURCE).hexdigest(),
    }
    with pytest.raises(ValueError, match="PLATFORM_PRICE_WINDOW_INVALID"):
        await register(factory, expired)
    async with factory() as session, session.begin():
        historical = AgentModelConfigRecord(
            id=uuid4(), **profiles.sol_business_model_fields(expired)
        )
        session.add(historical)
        await session.flush()
        historical_id = historical.id
    async with factory() as session:
        old = await profiles.read_sol_business_profile(session, model_config_id=original)
        assert old is not None and str(old.output_usd_per_million) == "10.000000000000"
        assert (
            await profiles.read_sol_business_profile(session, model_config_id=historical_id)
            is not None
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "fee",
        "wire",
        "extra_price",
        "hash",
        "old_reader",
        "numeric_source_hash",
        "long_window_dst",
        "json_null",
    ],
)
async def test_database_rejects_unbound_sol_profile_even_without_python_registration(
    migrated_database_url,
    mutation,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    record = await current_record(factory)
    fields = profiles.sol_business_model_fields(record)
    if mutation == "json_null":
        fields["platform_profile"] = JSON.NULL
    elif mutation == "fee":
        fields["input_usd_per_million"] = 0
    elif mutation == "wire":
        fields["platform_profile"]["wire_profile"]["effort"] = "low"
    elif mutation == "extra_price":
        fields["platform_profile"]["price_record"]["unrecognized"] = True
    elif mutation == "hash":
        fields["config_hash"] = "b" * 64
    elif mutation == "old_reader":
        fields["model_id"] = "gpt-5.6-terra"
    elif mutation == "numeric_source_hash":
        fields["platform_profile"]["price_record"]["source_evidence_sha256"] = int("1" * 64)
    else:
        data = fields["platform_profile"]["price_record"]
        data.update(verified_at="2026-10-28T00:00:00Z", expires_at="2026-11-04T00:30:00Z")
        fields["pricing_available_at"] = datetime.fromisoformat(data["verified_at"])
        fields["pricing_expires_at"] = datetime.fromisoformat(data["expires_at"])
    if mutation in {"wire", "extra_price", "numeric_source_hash", "long_window_dst"}:
        # A self-consistent attacker hash is not authority for an unsupported
        # capability, price shape or window. Exercise the semantic SQL checks.
        payload = fields["platform_profile"]
        price_hash = hashlib.sha256(
            json.dumps(payload["price_record"], sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        payload["wire_profile"]["price_version"] = "platform-sol-price-v1-" + price_hash
        digest = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        fields.update(config_hash=digest, version="platform-sol-business-v1-" + digest)
    async with factory() as session, session.begin():
        if mutation == "long_window_dst":
            await session.execute(text("SET LOCAL TIME ZONE 'America/New_York'"))
        session.add(AgentModelConfigRecord(id=uuid4(), **fields))
        with pytest.raises(DBAPIError):
            await session.flush()


@pytest.mark.asyncio
async def test_sol_migration_preserves_legacy_and_requires_compatible_reader_after_registration(
    migrated_database_url,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    config = Config("engine/alembic.ini")
    async with factory() as session, session.begin():
        old = AgentModelConfigRecord(
            id=uuid4(), platform_profile=None, **profiles.platform_model_fields()
        )
        session.add(old)
        await session.flush()
        old_id = old.id
        assert await session.scalar(
            text("SELECT platform_profile IS NULL FROM agent_model_configs WHERE id=:id"),
            {"id": old_id},
        )
        no_profile = select(AgentModelConfigRecord.id).where(
            AgentModelConfigRecord.id == old_id,
            platform_profile_projection(AgentModelConfigRecord.platform_profile.expression).is_(
                None
            ),
        )
        assert await session.scalar(no_profile) == old_id
        before = await session.scalar(
            text("SELECT to_jsonb(m)-'platform_profile' FROM agent_model_configs m WHERE id=:id"),
            {"id": old_id},
        )
    await asyncio.to_thread(command.downgrade, config, "0112_platform_medium_profile")
    async with factory() as session:
        assert (
            await session.scalar(
                text("SELECT to_jsonb(m) FROM agent_model_configs m WHERE id=:id"), {"id": old_id}
            )
            == before
        )
        legacy = await session.get(AgentModelConfigRecord, old_id)
        assert legacy.platform_profile is None and profiles.platform_model_matches(legacy)
        assert await session.scalar(no_profile) == old_id
    await asyncio.to_thread(command.upgrade, config, "head")
    async with factory() as session:
        legacy = await session.get(AgentModelConfigRecord, old_id)
        assert legacy.platform_profile is None and profiles.platform_model_matches(legacy)
        assert await session.scalar(no_profile) == old_id
    await register(factory, await current_record(factory))
    with pytest.raises(RuntimeError, match="PLATFORM_SOL_COMPATIBLE_READER_ROLLBACK_REQUIRED"):
        await asyncio.to_thread(command.downgrade, config, "0112_platform_medium_profile")
    async with factory() as session:
        assert (
            await session.scalar(text("SELECT version_num FROM alembic_version"))
            == CURRENT_MIGRATION_HEAD
        )
