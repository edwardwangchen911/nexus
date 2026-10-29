"""Formal recovery and seal preserve unknown usage through normal public entry."""

import pytest
from integration.test_research_orphan_seal_pg import (
    test_orphan_normal_seal_closes_manual_and_admits_fresh_request as seal_case,
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case", ["same_day", "cross_day", "cross_day_no_ack", "late_fact", "close_rollback"]
)
@pytest.mark.parametrize("known_prefix", [None, (10, 5), (0, 0), (10, 0), (0, 5)])
async def test_formal_public_recovery_seal_and_fresh_admission(
    migrated_database_url, monkeypatch, tmp_path, case, known_prefix
):
    await seal_case(migrated_database_url, monkeypatch, tmp_path, case, known_prefix=known_prefix)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        async with factory() as session:
            run_id = await session.scalar(
                text("SELECT id FROM agent_research_runs WHERE status='failed'")
            )
            assert await session.scalar(text("SELECT count(*) FROM research_stage_route")) == 3
            assert (
                await session.scalar(
                    text("SELECT known FROM research_stage_terminal WHERE run_id=:run"),
                    {"run": run_id},
                )
                is False
            )
            assert await session.scalar(
                text("SELECT count(*) FROM research_stage_authorization WHERE run_id=:run"),
                {"run": run_id},
            ) == (1 if known_prefix is None else 2)
            assert (
                await session.scalar(
                    text("SELECT state FROM agent_daily_budget_reservations WHERE run_id=:run"),
                    {"run": run_id},
                )
                == "reserved"
            )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["same_day", "cross_day"])
async def test_historical_unselected_orphan_keeps_original_public_seal(
    migrated_database_url, monkeypatch, tmp_path, case
):
    await seal_case(migrated_database_url, monkeypatch, tmp_path, case, legacy=True)
