import asyncio

import pytest
from conftest import (
    TemporaryDatabase,
    TemporaryDatabaseFactory,
    _create_database,
    _drop_database,
)


class _InjectedSetupFailure(RuntimeError):
    """Deliberately interrupt setup after PostgreSQL has created a temporary database."""


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
