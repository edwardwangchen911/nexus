"""Import preflight never echoes private arguments or reaches remote services."""

import json
import stat
from uuid import UUID

import pytest
from nexus.ops import alpaca_actions_import as entry
from nexus.settings import EngineSettings


@pytest.mark.parametrize(
    "args",
    [
        [],
        ["--instrument-id", "private-canary"],
        ["--unknown", "private-canary"],
    ],
)
def test_bad_cli_parameters_are_safe_before_settings_or_io(monkeypatch, capsys, args):
    monkeypatch.setattr("sys.argv", ["alpaca_actions_import", *args])

    def forbidden(*args, **kwargs):
        pytest.fail("bad CLI arguments reached local configuration")

    monkeypatch.setattr(entry, "load_local_environment", forbidden)
    assert entry.main() == 1
    captured = capsys.readouterr()
    assert not captured.out
    assert json.loads(captured.err) == {"status": "failed", "error": "ALPACA_ACTIONS_IMPORT_FAILED"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "database,endpoint",
    [
        (None, "http://localhost:9000"),
        ("postgresql+asyncpg://fake:fake@outside.invalid/test", "http://localhost:9000"),
        ("postgresql+asyncpg://fake:fake@localhost/test", "https://outside.invalid"),
        ("postgresql+asyncpg://fake:fake@localhost/test", "http://user:secret@localhost:9000"),
        ("postgresql+asyncpg://fake:fake@localhost/test", "http://localhost:9000/path"),
    ],
)
async def test_remote_or_implicit_services_fail_before_session(
    monkeypatch, tmp_path, database, endpoint
):
    def forbidden(*args, **kwargs):
        pytest.fail("invalid service coordinates reached database")

    monkeypatch.setattr(entry, "session_factory", forbidden)
    with pytest.raises(ValueError, match="^ALPACA_ACTIONS_LOCAL_SERVICES_REQUIRED$"):
        await entry.import_collection(
            EngineSettings(database_url=database, object_store_endpoint=endpoint),
            instrument_id=UUID(int=1),
            collection_dir=tmp_path,
        )


def test_existing_output_is_untouched_and_does_not_start_import(monkeypatch, tmp_path, capsys):
    tmp_path.chmod(0o700)
    output = tmp_path / "result.json"
    output.write_bytes(b"private-existing-result")

    def forbidden(*args, **kwargs):
        pytest.fail("existing output reached configuration")

    monkeypatch.setattr(entry, "load_local_environment", forbidden)
    monkeypatch.setattr(
        "sys.argv",
        [
            "alpaca_actions_import",
            "--repo-root",
            str(tmp_path),
            "--instrument-id",
            str(UUID(int=1)),
            "--collection-dir",
            str(tmp_path),
            "--output",
            str(output),
        ],
    )
    assert entry.main() == 1
    assert output.read_bytes() == b"private-existing-result"
    assert "private-existing-result" not in capsys.readouterr().err


@pytest.mark.parametrize("failed", [False, True])
def test_cli_outcome_ids_are_private_and_failure_never_echoes_exception(
    monkeypatch, tmp_path, capsys, failed
):
    tmp_path.chmod(0o700)
    output = tmp_path / "result.json"
    result = {
        "status": "verified",
        "evidence_id": str(UUID(int=9)),
        "research_snapshot_id": str(UUID(int=10)),
        "execution_locked": True,
    }

    async def imported(settings, *, instrument_id, collection_dir):
        assert instrument_id == UUID(int=1) and collection_dir == tmp_path
        if failed:
            raise RuntimeError("private-upstream-canary")
        return result

    monkeypatch.setattr(entry, "import_collection", imported)
    monkeypatch.setattr(entry, "load_local_environment", lambda *args: {})
    monkeypatch.setattr(
        "sys.argv",
        [
            "alpaca_actions_import",
            "--repo-root",
            str(tmp_path),
            "--instrument-id",
            str(UUID(int=1)),
            "--collection-dir",
            str(tmp_path),
            "--output",
            str(output),
        ],
    )
    assert entry.main() == (1 if failed else 0)
    captured = capsys.readouterr()
    assert "private-upstream-canary" not in captured.out + captured.err
    assert str(UUID(int=9)) not in captured.out + captured.err
    if failed:
        assert not output.exists()
    else:
        assert json.loads(output.read_bytes()) == result
        assert stat.S_IMODE(output.stat().st_mode) == 0o600
