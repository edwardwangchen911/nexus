"""Local SEC entrypoint rejects missing identity without loading model credentials."""

import json


def test_refresh_entrypoint_reports_missing_contact_without_reading_key(
    tmp_path,
    monkeypatch,
    capsys,
):
    from nexus.ops.openai_local import main

    (tmp_path / "engine").mkdir()
    (tmp_path / "engine/pyproject.toml").write_text("")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("SEC_CONTACT", raising=False)
    monkeypatch.setenv("OPENAI_API_KEY", "not a valid model key")
    monkeypatch.setattr(
        "sys.argv",
        [
            "nexus-openai-local",
            "refresh-research",
            "--instrument-id",
            "0198d500-1000-7000-8000-000000009001",
        ],
    )
    assert main() == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["code"] == "SEC_CONTACT_REQUIRED"
    assert payload["execution_locked"] is True
