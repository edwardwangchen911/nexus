"""Preserve both immutable histories when integrating formal budget and output gates."""

import asyncio
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

MERGE = "0069_formal_budget_output_merge"
PARENTS = {"0067_formal_budget_binding", "0068_formal_output_binding"}


def _config() -> Config:
    return Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))


def test_single_head_preserves_both_histories() -> None:
    scripts = ScriptDirectory.from_config(_config())
    assert scripts.get_heads() == [MERGE]
    assert CURRENT_MIGRATION_HEAD == MERGE
    revision = scripts.get_revision(MERGE)
    assert revision is not None
    assert set(revision.down_revision) == PARENTS


@pytest.mark.parametrize("start", sorted(PARENTS))
def test_upgrade_from_either_branch(
    start: str, temporary_database_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    command.upgrade(_config(), start)
    command.upgrade(_config(), "head")
    state = asyncio.run(_state(temporary_database_url))
    assert state["versions"] == [MERGE]
    _assert_guards(state)


@pytest.mark.parametrize("rollback_target", sorted(PARENTS))
def test_merge_and_unmerge_preserve_schema(
    rollback_target: str, temporary_database_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    # Upgrade each explicit branch, without applying the merge.
    for parent in sorted(PARENTS):
        command.upgrade(_config(), parent)
    before = asyncio.run(_state(temporary_database_url))
    assert set(before["versions"]) == PARENTS
    _assert_guards(before)
    command.upgrade(_config(), "head")
    merged = asyncio.run(_state(temporary_database_url))
    assert merged["versions"] == [MERGE]
    assert merged["schema"] == before["schema"]
    assert merged["output_guard"] == before["output_guard"]
    command.downgrade(_config(), rollback_target)
    after = asyncio.run(_state(temporary_database_url))
    assert after == before


def _assert_guards(state: dict) -> None:
    assert "codex-sdk-chatgpt-v8-stage-capacity" in state["output_guard"]
    assert "FORMAL_BUDGET_BINDING_IMMUTABLE" in state["schema"]
    assert "FORMAL_BUDGET_PIN_IMMUTABLE" in state["schema"]
    assert "formal_budget_binding_immutable" in state["schema"]
    assert "formal_budget_pin_immutable" in state["schema"]
    assert state["pins"] == ["agent_paper_cycles", "idempotency_records"]


async def _state(url: str) -> dict:
    engine = create_async_engine(url)
    try:
        async with engine.connect() as connection:
            database = await connection.scalar(text("SELECT current_database()"))
            assert database.startswith("nexus_test_") and len(database) == 43
            versions = list(
                (
                    await connection.execute(
                        text("SELECT version_num FROM alembic_version ORDER BY version_num")
                    )
                ).scalars()
            )
            pins = list(
                (
                    await connection.execute(
                        text(
                            "SELECT table_name FROM information_schema.columns "
                            "WHERE table_schema='public' "
                            "AND column_name='formal_budget_binding_id' "
                            "ORDER BY table_name"
                        )
                    )
                ).scalars()
            )
            guards = list(
                (
                    await connection.execute(
                        text(
                            "SELECT pg_get_functiondef(oid) FROM pg_proc WHERE pronamespace="
                            "'public'::regnamespace AND proname IN "
                            "('reject_formal_budget_binding_mutation',"
                            "'reject_formal_budget_pin_mutation') "
                            "ORDER BY proname"
                        )
                    )
                ).scalars()
            )
            schema = list(
                (
                    await connection.execute(
                        text(
                            "SELECT table_name,column_name,data_type,is_nullable FROM "
                            "information_schema.columns WHERE table_schema='public' "
                            "ORDER BY table_name,ordinal_position"
                        )
                    )
                ).all()
            )
            triggers = list(
                (
                    await connection.execute(
                        text(
                            "SELECT c.relname,t.tgname,t.tgenabled,pg_get_triggerdef(t.oid) "
                            "FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid "
                            "JOIN pg_namespace n ON n.oid=c.relnamespace "
                            "WHERE n.nspname='public' AND NOT t.tgisinternal "
                            "ORDER BY c.relname,t.tgname"
                        )
                    )
                ).all()
            )
            output_guard = await connection.scalar(
                text(
                    "SELECT pg_get_functiondef('nexus_agent_output_binding_valid()'::regprocedure)"
                )
            )
            return {
                "versions": versions,
                "pins": pins,
                "schema": repr(schema) + repr(guards) + repr(triggers),
                "output_guard": output_guard,
            }
    finally:
        await engine.dispose()
