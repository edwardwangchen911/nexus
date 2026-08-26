import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.engine import URL
from sqlalchemy.exc import ProgrammingError

try:
    from engine.tests.integration import conftest as integration_conftest
except ModuleNotFoundError as error:
    if error.name != "engine":
        raise
    import conftest as integration_conftest

_FORCE_DROP_ATTEMPTS = integration_conftest._FORCE_DROP_ATTEMPTS
TemporaryDatabase = integration_conftest.TemporaryDatabase
TemporaryDatabaseFactory = integration_conftest.TemporaryDatabaseFactory
_create_database = integration_conftest._create_database
_drop_database = integration_conftest._drop_database


class _InjectedSetupFailure(RuntimeError):
    """Deliberately interrupt setup after PostgreSQL has created a temporary database."""


class _ProtectedBackendError(Exception):
    sqlstate = "42501"


class _RecordingEngine:
    def __init__(self, *, transient_failures: int = 0, error_message: str = "") -> None:
        self.statements: list[str] = []
        self.transient_failures = transient_failures
        self.error_message = error_message
        self.dispose_count = 0

    @asynccontextmanager
    async def connect(self) -> AsyncIterator["_RecordingEngine"]:
        yield self

    async def execute(self, statement) -> None:
        self.statements.append(str(statement))
        if self.transient_failures > 0:
            self.transient_failures -= 1
            raise ProgrammingError(
                str(statement),
                {},
                _ProtectedBackendError(self.error_message),
            )

    async def dispose(self) -> None:
        self.dispose_count += 1


def test_drop_database_delegates_backend_cleanup_to_force_drop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cleanup must not issue a broad manual backend-termination query."""
    database_name = "nexus_test_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    engine = _RecordingEngine()
    monkeypatch.setattr(
        integration_conftest,
        "create_async_engine",
        lambda *_args, **_kwargs: engine,
    )

    asyncio.run(
        _drop_database(
            URL.create("postgresql+asyncpg", database="postgres"),
            database_name,
        )
    )

    assert engine.statements == [
        f'DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)'
    ]


def test_drop_database_retries_a_transient_protected_backend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A short-lived PostgreSQL background worker must not leak the temporary database."""
    database_name = "nexus_test_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    failed_engine = _RecordingEngine(
        transient_failures=1,
        error_message="<class 'asyncpg.exceptions.InsufficientPrivilegeError'>: "
        "permission denied to terminate process",
    )
    successful_engine = _RecordingEngine()
    engines = iter((failed_engine, successful_engine))
    sleep = AsyncMock()
    monkeypatch.setattr(integration_conftest, "asyncio", type("Asyncio", (), {"sleep": sleep}))
    monkeypatch.setattr(
        integration_conftest,
        "create_async_engine",
        lambda *_args, **_kwargs: next(engines),
    )

    asyncio.run(
        _drop_database(
            URL.create("postgresql+asyncpg", database="postgres"),
            database_name,
        )
    )

    statement = f'DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)'
    assert failed_engine.statements == [statement]
    assert successful_engine.statements == [statement]
    assert failed_engine.dispose_count == 1
    assert successful_engine.dispose_count == 1
    sleep.assert_awaited_once()


def test_drop_database_does_not_retry_a_near_match_privilege_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Text that merely contains the protected-worker reason must fail closed."""
    database_name = "nexus_test_eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
    engine = _RecordingEngine(
        transient_failures=1,
        error_message="permission denied to terminate process owned by another role",
    )
    sleep = AsyncMock()
    monkeypatch.setattr(integration_conftest, "asyncio", type("Asyncio", (), {"sleep": sleep}))
    monkeypatch.setattr(
        integration_conftest,
        "create_async_engine",
        lambda *_args, **_kwargs: engine,
    )

    with pytest.raises(ProgrammingError, match="owned by another role"):
        asyncio.run(
            _drop_database(
                URL.create("postgresql+asyncpg", database="postgres"),
                database_name,
            )
        )

    assert len(engine.statements) == 1
    assert engine.dispose_count == 1
    sleep.assert_not_awaited()


def test_drop_database_does_not_retry_an_unrelated_privilege_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A real ownership/permission defect must remain fail closed."""
    database_name = "nexus_test_cccccccccccccccccccccccccccccccc"
    engine = _RecordingEngine(
        transient_failures=1,
        error_message="permission denied for database postgres",
    )
    sleep = AsyncMock()
    monkeypatch.setattr(integration_conftest, "asyncio", type("Asyncio", (), {"sleep": sleep}))
    monkeypatch.setattr(
        integration_conftest,
        "create_async_engine",
        lambda *_args, **_kwargs: engine,
    )

    with pytest.raises(ProgrammingError, match="permission denied for database"):
        asyncio.run(
            _drop_database(
                URL.create("postgresql+asyncpg", database="postgres"),
                database_name,
            )
        )

    assert len(engine.statements) == 1
    sleep.assert_not_awaited()


def test_drop_database_retry_is_bounded_and_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A persistent protected backend must surface after the bounded retry window."""
    database_name = "nexus_test_dddddddddddddddddddddddddddddddd"
    engines = [
        _RecordingEngine(
            transient_failures=1,
            error_message="permission denied to terminate process",
        )
        for _ in range(_FORCE_DROP_ATTEMPTS)
    ]
    engine_iterator = iter(engines)
    sleep = AsyncMock()
    monkeypatch.setattr(integration_conftest, "asyncio", type("Asyncio", (), {"sleep": sleep}))
    monkeypatch.setattr(
        integration_conftest,
        "create_async_engine",
        lambda *_args, **_kwargs: next(engine_iterator),
    )

    with pytest.raises(ProgrammingError, match="permission denied to terminate process"):
        asyncio.run(
            _drop_database(
                URL.create("postgresql+asyncpg", database="postgres"),
                database_name,
            )
        )

    assert all(len(engine.statements) == 1 for engine in engines)
    assert all(engine.dispose_count == 1 for engine in engines)
    assert sleep.await_count == _FORCE_DROP_ATTEMPTS - 1


def test_temporary_database_is_dropped_when_post_create_setup_fails(
    temporary_database_factory: TemporaryDatabaseFactory,
) -> None:
    """A post-create failure must not leave the UUID-derived database in PostgreSQL."""
    created: TemporaryDatabase | None = None

    def fail_after_create(temporary_database: TemporaryDatabase) -> None:
        nonlocal created
        created = temporary_database
        assert temporary_database_factory.exists(temporary_database.name) is True
        raise _InjectedSetupFailure("stop setup after CREATE DATABASE")

    try:
        with pytest.raises(_InjectedSetupFailure):
            with temporary_database_factory.provision(after_create=fail_after_create):
                pytest.fail("setup failure should prevent the test body")

        assert created is not None
        assert temporary_database_factory.exists(created.name) is False
    finally:
        if created is not None:
            temporary_database_factory.drop(created)


@pytest.mark.parametrize(
    "unsafe_name",
    ["postgres", "nexus", "nexus_test_not-a-uuid", 'nexus_test_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"'],
)
def test_database_helpers_reject_unsafe_names_before_connecting(
    temporary_database_factory: TemporaryDatabaseFactory,
    unsafe_name: str,
) -> None:
    """Shared and malformed targets are rejected before any destructive helper can connect."""
    unreachable_factory = TemporaryDatabaseFactory(
        temporary_database_factory.admin_url.set(port=1)
    )
    assert temporary_database_factory.exists("postgres") is True

    with pytest.raises(ValueError, match="temporary database name"):
        asyncio.run(_create_database(unreachable_factory.admin_url, unsafe_name))
    with pytest.raises(ValueError, match="temporary database name"):
        asyncio.run(_drop_database(unreachable_factory.admin_url, unsafe_name))

    assert temporary_database_factory.exists("postgres") is True


def test_drop_rejects_a_bare_string_before_connecting(
    temporary_database_factory: TemporaryDatabaseFactory,
) -> None:
    """The public cleanup API requires a typed temporary-database target."""
    unreachable_factory = TemporaryDatabaseFactory(
        temporary_database_factory.admin_url.set(port=1)
    )

    with pytest.raises(TypeError, match="TemporaryDatabase"):
        unreachable_factory.drop("nexus_test_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")  # type: ignore[arg-type]


@pytest.mark.parametrize("unsafe_name", ["postgres", "nexus", "nexus_test_not-a-uuid"])
def test_drop_rejects_typed_unsafe_targets_before_connecting(
    temporary_database_factory: TemporaryDatabaseFactory,
    unsafe_name: str,
) -> None:
    """A typed wrapper cannot turn a shared or malformed name into a cleanup target."""
    unreachable_factory = TemporaryDatabaseFactory(
        temporary_database_factory.admin_url.set(port=1)
    )

    with pytest.raises(ValueError, match="temporary database name"):
        unreachable_factory.drop(TemporaryDatabase(name=unsafe_name, url=""))
