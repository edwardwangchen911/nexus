"""Output binding gate migrates online/offline without changing historical data."""

import asyncio
import re
from io import StringIO
from pathlib import Path

from alembic import command
from alembic.config import Config
from integration.test_research_identity_migration import apply_script
from nexus.agents.codex_candidate import candidate_config_hash
from nexus.agents.codex_formal_stage import formal_stage_config_hash
from nexus.agents.codex_versions import ACTIONABLE_CONFIG_VERSIONS, CANDIDATE_BUDGET_CONFIG_VERSION
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


async def binding_definition(url):
    engine = create_async_engine(url)
    try:
        async with engine.connect() as conn:
            return await conn.scalar(
                text(
                    "SELECT pg_get_functiondef("
                    "'public.nexus_agent_output_binding_valid()'::regprocedure)"
                )
            )
    finally:
        await engine.dispose()


def test_output_binding_gate_online_and_offline_roundtrip(temporary_database_url, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    command.upgrade(config, "0066_research_identity_binding")
    before = asyncio.run(binding_definition(temporary_database_url))
    command.upgrade(config, "0067_agent_output_versions")
    intermediate = asyncio.run(binding_definition(temporary_database_url))
    command.upgrade(config, "head")
    after = asyncio.run(binding_definition(temporary_database_url))
    formal = re.search(r"run_version_ref IN \((.*?)\)", after, re.S).group(1)
    assert set(re.findall(r"'([^']+)'", formal)) == ACTIONABLE_CONFIG_VERSIONS
    assert CANDIDATE_BUDGET_CONFIG_VERSION not in formal
    for guard in (
        "binding_count >= 5",
        "output_status_ref",
        "existing_run.instrument_id=instrument_ref",
    ):
        assert guard in before and guard in after
    assert "run.model_hash = model.config_hash" in after
    assert candidate_config_hash() in after
    assert "current_database() ~ '^nexus_(test|diag)_'" in after
    assert "prepared.fact_ids->'candidate_stage_capacity' = 'true'::jsonb" in after
    assert "codex-sdk-chatgpt-v8-stage-capacity" in after
    assert formal_stage_config_hash() in after
    assert "prepared.fact_ids->'formal_stage_capacity' = 'true'::jsonb" in after
    assert "prepared.fact_ids->>'formal_model_config_id' = model.id::text" in after
    assert "prepared.fact_ids->>'formal_model_hash' = model.config_hash" in after
    command.downgrade(config, "0067_agent_output_versions")
    assert asyncio.run(binding_definition(temporary_database_url)) == intermediate
    command.upgrade(config, "head")
    assert asyncio.run(binding_definition(temporary_database_url)) == after
    command.downgrade(config, "0066_research_identity_binding")
    assert asyncio.run(binding_definition(temporary_database_url)) == before
    buffer = StringIO()
    offline = Config(config.config_file_name, output_buffer=buffer)
    command.upgrade(offline, "0066_research_identity_binding:head", sql=True)
    asyncio.run(apply_script(temporary_database_url, buffer.getvalue()))
    assert asyncio.run(binding_definition(temporary_database_url)) == after
    buffer.truncate(0)
    buffer.seek(0)
    command.downgrade(offline, "head:0066_research_identity_binding", sql=True)
    asyncio.run(apply_script(temporary_database_url, buffer.getvalue()))
    assert asyncio.run(binding_definition(temporary_database_url)) == before
