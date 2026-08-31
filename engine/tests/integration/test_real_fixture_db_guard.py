"""PostgreSQL integration coverage for the real fixture database guard."""

import asyncio
import importlib.util
import re
import sys
from collections.abc import Generator
from pathlib import Path
from types import ModuleType
from uuid import uuid4

import asyncpg
import pytest

_E2E_DATABASE_NAME = re.compile(r"^nexus_e2e_guard_[0-9a-f]{32}$")


def _guard_module() -> ModuleType:
    path = Path(__file__).resolve().parents[3] / "scripts/e2e/real_fixture_db_guard.py"
    spec = importlib.util.spec_from_file_location("nexus_real_fixture_db_guard", path)
    assert spec is not None, "real fixture database guard module is required"
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def guard_database_url(temporary_database_factory: object) -> Generator[str]:
    admin_url = temporary_database_factory.admin_url
    database_name = f"nexus_e2e_guard_{uuid4().hex}"
    assert _E2E_DATABASE_NAME.fullmatch(database_name)
    database_url = admin_url.set(database=database_name).render_as_string(hide_password=False)
    admin_dsn = admin_url.render_as_string(hide_password=False).replace(
        "postgresql+asyncpg://", "postgresql://", 1
    )

    async def execute(statement: str) -> None:
        connection = await asyncpg.connect(admin_dsn)
        try:
            await connection.execute(statement)
        finally:
            await connection.close()

    asyncio.run(execute(f'CREATE DATABASE "{database_name}"'))
    try:
        yield database_url
    finally:
        asyncio.run(execute(f'DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)'))


@pytest.mark.asyncio
async def test_guard_accepts_an_empty_database_and_releases_its_advisory_lock(
    guard_database_url: str,
) -> None:
    guard = _guard_module()
    dsn = guard.asyncpg_dsn(guard_database_url)

    async with guard.hold_database_guard(guard_database_url):
        contender = await asyncpg.connect(dsn)
        try:
            assert await contender.fetchval(
                "SELECT pg_try_advisory_lock(hashtextextended($1, 0))",
                guard.ADVISORY_LOCK_KEY,
            ) is False
        finally:
            await contender.close()

    contender = await asyncpg.connect(dsn)
    try:
        assert await contender.fetchval(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))",
            guard.ADVISORY_LOCK_KEY,
        ) is True
        assert await contender.fetchval(
            "SELECT pg_advisory_unlock(hashtextextended($1, 0))",
            guard.ADVISORY_LOCK_KEY,
        ) is True
    finally:
        await contender.close()


@pytest.mark.asyncio
async def test_guard_rejects_a_concurrent_holder(
    guard_database_url: str,
) -> None:
    guard = _guard_module()

    async with guard.hold_database_guard(guard_database_url):
        with pytest.raises(guard.DatabaseGuardError) as captured:
            async with guard.hold_database_guard(guard_database_url):
                raise AssertionError("concurrent guard unexpectedly acquired the lock")

    assert captured.value.code == "LOCK_UNAVAILABLE"
    assert str(captured.value) == "real fixture E2E database is already in use"


@pytest.mark.asyncio
async def test_guard_rejects_a_database_with_user_objects(
    guard_database_url: str,
) -> None:
    guard = _guard_module()
    connection = await asyncpg.connect(guard.asyncpg_dsn(guard_database_url))
    try:
        await connection.execute("CREATE TABLE public.guard_canary (id integer PRIMARY KEY)")
    finally:
        await connection.close()

    with pytest.raises(guard.DatabaseGuardError) as captured:
        async with guard.hold_database_guard(guard_database_url):
            raise AssertionError("non-empty database unexpectedly passed the guard")

    assert captured.value.code == "DATABASE_NOT_EMPTY"
    assert str(captured.value) == "real fixture E2E database must be empty"
