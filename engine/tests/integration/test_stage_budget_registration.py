"""Offline registration on a synthetic non-diagnostic database, never the business DB."""

import asyncio
from uuid import uuid4

import pytest
from nexus.agents.codex_versions import ACTIONABLE_CONFIG_VERSION, DEADLINE_BUDGET_CONFIG_VERSION
from nexus.ops.openai_local import prepare_database
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.paper import AgentPaperCycleRecord, PaperOrderRecord
from nexus.persistence.models.runtime import Job
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import create_async_engine


@pytest.fixture
def registration_database_url(temporary_database_factory, migrated_database_template):
    name = f"nexus_budget_registration_{uuid4().hex}"
    admin = temporary_database_factory.admin_url

    async def change(create):
        engine = create_async_engine(admin, isolation_level="AUTOCOMMIT")
        try:
            async with engine.connect() as connection:
                statement = (
                    f'CREATE DATABASE "{name}" TEMPLATE "{migrated_database_template.name}"'
                    if create
                    else f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'
                )
                await connection.execute(text(statement))
        finally:
            await engine.dispose()

    try:
        asyncio.run(change(True))
        yield admin.set(database=name).render_as_string(hide_password=False)
    finally:
        asyncio.run(change(False))


@pytest.mark.asyncio
async def test_explicit_registration_is_idempotent_and_never_executes_research_or_orders(
    registration_database_url,
):
    settings = EngineSettings(
        database_url=registration_database_url, agent_runtime_mode="codex_sdk"
    )
    options = dict(
        actionable_paper=True,
        structured_synthesis=True,
        aggregate_input_tokens=96000,
        max_wire_bytes=64000,
        inline_citations=True,
    )
    factory = session_factory(settings)
    try:
        await prepare_database(settings, **options)
        async with factory() as session:
            old = await session.scalar(
                select(AgentModelConfigRecord).where(
                    AgentModelConfigRecord.version == ACTIONABLE_CONFIG_VERSION
                )
            )
            old_identity = (old.id, old.config_hash)
        with pytest.raises(ValueError, match="CODEX_STAGE_TIME_BUDGET_ISOLATION_REQUIRED"):
            await prepare_database(settings, **options, stage_time_budget=True)
        async with factory() as session:
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(AgentModelConfigRecord)
                    .where(AgentModelConfigRecord.version == DEADLINE_BUDGET_CONFIG_VERSION)
                )
                == 0
            )
        for _ in range(2):
            await prepare_database(
                settings,
                **options,
                stage_time_budget=True,
                allow_stage_time_budget_registration=True,
            )
        async with factory() as session:
            old = await session.scalar(
                select(AgentModelConfigRecord).where(
                    AgentModelConfigRecord.version == ACTIONABLE_CONFIG_VERSION
                )
            )
            assert (old.id, old.config_hash) == old_identity
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(AgentModelConfigRecord)
                    .where(AgentModelConfigRecord.version == DEADLINE_BUDGET_CONFIG_VERSION)
                )
                == 1
            )
            for model in (
                AgentResearchRunRecord,
                Job,
                DataSnapshot,
                AgentPaperCycleRecord,
                PaperOrderRecord,
            ):
                assert await session.scalar(select(func.count()).select_from(model)) == 0
    finally:
        await factory.kw["bind"].dispose()
