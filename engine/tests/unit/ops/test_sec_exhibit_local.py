import json
from pathlib import Path
from uuid import UUID

import pytest
from nexus.settings import EngineSettings


@pytest.mark.parametrize("quarter_metrics", [False, True])
def test_exhibit_cli_uses_normal_configuration_without_model_key(
    tmp_path, monkeypatch, capsys, quarter_metrics
):
    from nexus.ops import openai_local, sec_exhibit_local

    (tmp_path / "engine").mkdir()
    (tmp_path / "engine/pyproject.toml").write_text("")
    monkeypatch.chdir(tmp_path)
    calls = []

    def settings(root, *, load_model_key, backend):
        assert root == tmp_path and load_model_key is False
        return EngineSettings(database_url="postgresql+asyncpg://localhost/synthetic")

    async def importer(config, *, identity_snapshot_id, archives, include_quarter_metrics=False):
        calls.append((identity_snapshot_id, archives, include_quarter_metrics))
        return {"status": "created"}

    monkeypatch.setattr(openai_local, "local_settings", settings)
    monkeypatch.setattr(sec_exhibit_local, "import_sec_exhibit", importer)
    args = ["nexus-openai-local", "import-sec-exhibit", "--identity-snapshot-id", str(UUID(int=2))]
    for kind in ("submissions", "parent", "exhibit"):
        args.extend([f"--sec-{kind}", f"{kind}.raw", f"--sec-{kind}-receipt", f"{kind}.json"])
    if quarter_metrics:
        args.append("--quarter-metrics")
    monkeypatch.setattr("sys.argv", args)
    assert openai_local.main() == 0
    assert json.loads(capsys.readouterr().out)["status"] == "created"
    assert calls == [
        (
            UUID(int=2),
            [(Path(k + ".raw"), Path(k + ".json")) for k in ("submissions", "parent", "exhibit")],
            quarter_metrics,
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
