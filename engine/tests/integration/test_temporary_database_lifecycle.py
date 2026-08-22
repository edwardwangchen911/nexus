import pytest
from conftest import TemporaryDatabase, TemporaryDatabaseFactory


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
            temporary_database_factory.drop(created.name)
