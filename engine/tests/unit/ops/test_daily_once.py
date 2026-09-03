"""The manual daily CLI is explicit, local, bounded, and redacted."""

from uuid import UUID

import pytest
from nexus.ops.daily_once import (
    DailyAdvanceRequest,
    DailyOnceError,
    DailyOnceSettings,
    main,
    run_once,
)
from nexus.settings import EngineSettings


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "database",
    [
        None,
        "not-a-dsn",
        "sqlite:///tmp/test.db",
        "postgresql+asyncpg://user:secret@example.invalid/db",
        "postgresql+asyncpg://user:secret@127.0.0.1/db?host=example.invalid",
    ],
)
async def test_invalid_or_remote_database_fails_before_creating_engine(database, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("must reject before creating an engine")

    monkeypatch.setattr("nexus.ops.daily_once.create_async_engine", forbidden)
    request = DailyAdvanceRequest(action="advance", root_id=UUID(int=1))
    with pytest.raises(DailyOnceError, match="DAILY_ONCE_DATABASE_"):
        await run_once(EngineSettings(database_url=database), request)


def test_invalid_cli_does_not_echo_request_or_raw_errors(monkeypatch, capsys):
    monkeypatch.setattr(
        "sys.argv", ["daily_once", "advance", "--root-id", "sensitive-input-canary"]
    )
    with pytest.raises(SystemExit) as error:
        main()
    assert error.value.code == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert "DAILY_ONCE_REQUEST_INVALID" in output.err
    assert "sensitive-input-canary" not in output.err


@pytest.mark.asyncio
async def test_dependency_error_exposes_only_unknown_result_code(monkeypatch):
    def unavailable(*args, **kwargs):
        raise RuntimeError("private-dsn-and-account-canary")

    monkeypatch.setattr("nexus.ops.daily_once.create_async_engine", unavailable)
    with pytest.raises(DailyOnceError) as error:
        await run_once(
            EngineSettings(database_url="postgresql+asyncpg://user:secret@127.0.0.1/db"),
            DailyAdvanceRequest(action="advance", root_id=UUID(int=1)),
        )
    assert str(error.value) == "DAILY_ONCE_RESULT_UNKNOWN"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "missing", [None, "ENDPOINT", "REGION", "BUCKET", "ACCESS_KEY_ID", "SECRET_ACCESS_KEY"]
)
async def test_implicit_fixture_store_never_reaches_reader_or_worker(monkeypatch, missing):
    names = (
        "OBJECT_STORE_ENDPOINT",
        "OBJECT_STORE_REGION",
        "OBJECT_STORE_BUCKET",
        "OBJECT_STORE_ACCESS_KEY_ID",
        "OBJECT_STORE_SECRET_ACCESS_KEY",
    )
    for name in names:
        monkeypatch.delenv(name, raising=False)

    def forbidden(*args, **kwargs):
        pytest.fail("missing explicit storage must be rejected before reader or worker")

    monkeypatch.setattr("nexus.ops.daily_once.S3ProviderEvidenceStore", forbidden)
    monkeypatch.setattr("nexus.ops.daily_once.run_job_worker_once", forbidden)
    explicit = (
        {}
        if missing is None
        else {name: "synthetic-value" for name in names if name != f"OBJECT_STORE_{missing}"}
    )
    config = DailyOnceSettings(
        database_url="postgresql+asyncpg://user:secret@127.0.0.1/db",
        explicit_artifact_config=True,
        **explicit,
    )
    assert config.explicit_artifact_config is False
    with pytest.raises(DailyOnceError, match="DAILY_ONCE_ARTIFACT_READER_UNAVAILABLE"):
        await run_once(config, DailyAdvanceRequest(action="advance", root_id=UUID(int=1)))


def test_explicit_environment_storage_is_recognized_before_fixture_defaults(monkeypatch):
    for name in (
        "OBJECT_STORE_ENDPOINT",
        "OBJECT_STORE_REGION",
        "OBJECT_STORE_BUCKET",
        "OBJECT_STORE_ACCESS_KEY_ID",
        "OBJECT_STORE_SECRET_ACCESS_KEY",
    ):
        monkeypatch.setenv(name, "synthetic-value")
    assert DailyOnceSettings().explicit_artifact_config is True
