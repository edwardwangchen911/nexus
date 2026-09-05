import json
from pathlib import Path
from uuid import UUID

import pytest
from nexus.settings import EngineSettings


def test_exhibit_cli_uses_normal_configuration_without_model_key(tmp_path, monkeypatch, capsys):
    from nexus.ops import openai_local, sec_exhibit_local

    (tmp_path / "engine").mkdir()
    (tmp_path / "engine/pyproject.toml").write_text("")
    monkeypatch.chdir(tmp_path)
    calls = []

    def settings(root, *, load_model_key, backend):
        assert root == tmp_path and load_model_key is False
        return EngineSettings(database_url="postgresql+asyncpg://localhost/synthetic")

    async def importer(config, *, identity_snapshot_id, archives):
        calls.append((identity_snapshot_id, archives))
        return {"status": "created"}

    monkeypatch.setattr(openai_local, "local_settings", settings)
    monkeypatch.setattr(sec_exhibit_local, "import_sec_exhibit", importer)
    args = ["nexus-openai-local", "import-sec-exhibit", "--identity-snapshot-id", str(UUID(int=2))]
    for kind in ("submissions", "parent", "exhibit"):
        args.extend([f"--sec-{kind}", f"{kind}.raw", f"--sec-{kind}-receipt", f"{kind}.json"])
    monkeypatch.setattr("sys.argv", args)
    assert openai_local.main() == 0
    assert json.loads(capsys.readouterr().out)["status"] == "created"
    assert calls == [
        (
            UUID(int=2),
            [(Path(k + ".raw"), Path(k + ".json")) for k in ("submissions", "parent", "exhibit")],
        )
    ]


@pytest.mark.parametrize("extra", [[], ["--as-of", "2026-09-01T00:00:00+00:00"]])
def test_incomplete_exhibit_cli_rejects_before_configuration(extra, monkeypatch, capsys):
    from nexus.ops import openai_local

    def forbidden(*args, **kwargs):
        raise AssertionError("configuration must not be loaded")

    monkeypatch.setattr(openai_local, "local_settings", forbidden)
    monkeypatch.setattr("sys.argv", ["nexus-openai-local", "import-sec-exhibit", *extra])
    assert openai_local.main() == 1
    assert json.loads(capsys.readouterr().out)["code"] == "SEC_EXHIBIT_ARGUMENTS_INVALID"
