"""Protected local target parsing; no network or real credentials."""

import pytest
from nexus.ops.economic_cycle import read_economic_database_url


@pytest.mark.parametrize(
    "query", ["host=example.invalid", "port=1234", "database=other", "ssl=disable"]
)
def test_url_query_cannot_override_local_economic_connection(tmp_path, query):
    name = "nexus_test_" + "0" * 32
    path = tmp_path / "url"
    path.write_text(f"postgresql+asyncpg://localhost/{name}?{query}")
    path.chmod(0o600)
    with pytest.raises(ValueError, match="ECONOMIC_PROTOCOL_DATABASE_MISMATCH"):
        read_economic_database_url(path, expected_database=name)


def test_plain_owned_local_url_is_accepted(tmp_path):
    name = "nexus_test_" + "0" * 32
    url = f"postgresql+asyncpg://127.0.0.1:55448/{name}"
    path = tmp_path / "url"
    path.write_text(url)
    path.chmod(0o600)
    assert read_economic_database_url(path, expected_database=name) == url


def test_start_cli_dispatches_only_registered_pair(monkeypatch, capsys, tmp_path):
    import json
    import sys

    import nexus.ops.economic_cycle as cli

    async def execute_start(**kwargs):
        assert str(kwargs["pair_slot_id"]) == "00000000-0000-0000-0000-000000000001"
        return {"status": "researching", "replayed": False}

    monkeypatch.setattr(cli, "execute_start", execute_start, raising=False)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "economic-cycle",
            "start",
            "--database-url-file",
            str(tmp_path / "url"),
            "--expected-database",
            "nexus_test_" + "0" * 32,
            "--pair-slot-id",
            "00000000-0000-0000-0000-000000000001",
        ],
    )
    assert cli.main() == 0
    assert json.loads(capsys.readouterr().out)["status"] == "researching"


def test_resume_cli_uses_persisted_pair_not_free_authority(monkeypatch, capsys, tmp_path):
    import json
    import sys

    import nexus.ops.economic_cycle as cli

    async def execute_resume(**kwargs):
        assert set(kwargs) == {"database_url_file", "expected_database", "pair_slot_id"}
        return {"status": "completed", "replayed": True}

    monkeypatch.setattr(cli, "execute_resume", execute_resume, raising=False)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "economic-cycle",
            "resume",
            "--database-url-file",
            str(tmp_path / "url"),
            "--expected-database",
            "nexus_test_" + "0" * 32,
            "--pair-slot-id",
            "00000000-0000-0000-0000-000000000001",
        ],
    )
    assert cli.main() == 0
    assert json.loads(capsys.readouterr().out)["status"] == "completed"


@pytest.mark.parametrize(
    "command_name,entrypoint",
    [
        ("freeze-window", "execute_freeze"),
        ("freeze-next-window", "execute_freeze_next"),
        ("continue-plans", "execute_continue_plans"),
    ],
)
def test_freeze_cli_has_no_caller_clock_or_window_authority(
    monkeypatch, capsys, tmp_path, command_name, entrypoint
):
    import json
    import sys

    import nexus.ops.economic_cycle as cli

    async def execute_freeze(**kwargs):
        assert set(kwargs) == {"database_url_file", "expected_database", "pair_slot_id"}
        return {"status": "frozen", "execution_locked": True}

    monkeypatch.setattr(cli, entrypoint, execute_freeze, raising=False)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "economic-cycle",
            command_name,
            "--database-url-file",
            str(tmp_path / "url"),
            "--expected-database",
            "nexus_test_" + "0" * 32,
            "--pair-slot-id",
            "00000000-0000-0000-0000-000000000001",
        ],
    )
    assert cli.main() == 0
    assert json.loads(capsys.readouterr().out)["status"] == "frozen"
    sys.argv.extend(["--window-start", "2026-10-10T14:00:00Z"])
    with pytest.raises(SystemExit) as error:
        cli.main()
    assert error.value.code == 2
