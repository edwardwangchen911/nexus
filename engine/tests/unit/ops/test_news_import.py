import json
import sys
from uuid import UUID

import pytest
from nexus.ops import news_import


def args(monkeypatch, path):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "news_import",
            "--instrument-id",
            str(UUID(int=1)),
            "--collection-dir",
            str(path.parent),
            "--output",
            str(path),
        ],
    )
    monkeypatch.setattr(news_import, "load_local_environment", lambda *_: {})


def test_result_is_written_to_reserved_file(monkeypatch, tmp_path, capsys):
    path = tmp_path / "result.completed.json"
    args(monkeypatch, path)

    async def run(*_):
        assert json.loads(path.read_text()) == {"status": "started"}
        return {"status": "stored", "snapshot_id": str(UUID(int=2))}

    monkeypatch.setattr(news_import, "run", run)
    news_import.main()
    assert json.loads(path.read_text())["status"] == "stored"
    assert json.loads(capsys.readouterr().out)["status"] == "stored"


def test_uncertain_commit_is_not_reported_as_no_write(monkeypatch, tmp_path, capsys):
    path = tmp_path / "result.json"
    args(monkeypatch, path)

    async def run(*_):
        raise ConnectionError("sensitive content must not escape")

    monkeypatch.setattr(news_import, "run", run)
    with pytest.raises(SystemExit):
        news_import.main()
    output = capsys.readouterr().out
    assert "sensitive" not in output
    assert json.loads(output)["status"] == "unknown"
    assert json.loads(path.read_text())["status"] == "started"
