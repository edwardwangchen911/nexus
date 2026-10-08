"""Normal importer exposes fixed safe errors and refuses nonlocal database targets."""

from uuid import uuid4

import pytest
from nexus.ops.research_identity_binding_local import main, register_identity
from nexus.settings import EngineSettings


def test_invalid_cli_request_has_only_fixed_safe_error(monkeypatch, capsys):
    monkeypatch.setattr(
        "sys.argv", ["research-identity-import", "--identity-snapshot-id", "invalid"]
    )
    with pytest.raises(SystemExit) as error:
        main()
    assert error.value.code == 1
    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out.strip() == (
        '{"status": "unavailable", "code": "RESEARCH_IDENTITY_IMPORT_FAILED"}'
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("target", [None, "postgresql+asyncpg://localhost.invalid/synthetic"])
async def test_import_refuses_database_without_local_target(target, tmp_path, monkeypatch):
    def unexpected_connection(*args, **kwargs):
        raise AssertionError("database connection must not start")

    monkeypatch.setattr(
        "nexus.ops.research_identity_binding_local.session_factory", unexpected_connection
    )
    with pytest.raises(ValueError, match="RESEARCH_IDENTITY_LOCAL_DATABASE_REQUIRED"):
        await register_identity(
            EngineSettings(database_url=target),
            identity_snapshot_id=uuid4(),
            collection_dir=tmp_path,
        )
