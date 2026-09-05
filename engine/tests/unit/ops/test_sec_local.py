"""Local SEC entrypoint rejects missing identity without loading model credentials."""

import json

import pytest


def test_saved_projection_cli_uses_explicit_filing_without_loading_model_key(monkeypatch, capsys):
    from uuid import UUID

    from nexus.ops import openai_local, sec_local

    filing_id = UUID("00000000-0000-4000-8000-000000000001")
    settings = object()
    seen = []

    def load(root, **kwargs):
        assert kwargs["load_model_key"] is False
        return settings

    async def project(actual_settings, actual_id):
        assert actual_settings is settings and actual_id == filing_id
        seen.append(actual_id)
        return {
            "facts_created": 1,
            "provider_requests": 0,
            "model_requests": 0,
            "execution_locked": True,
        }

    monkeypatch.setattr(openai_local, "local_settings", load)
    monkeypatch.setattr(sec_local, "project_saved_sec_facts", project, raising=False)
    monkeypatch.setattr(
        "sys.argv", ["nexus-openai-local", "project-sec-facts", "--filing-id", str(filing_id)]
    )
    assert openai_local.main() == 0
    assert seen == [filing_id]
    result = json.loads(capsys.readouterr().out)
    assert result["facts_created"] == 1 and result["model_requests"] == 0


@pytest.mark.parametrize("extra", [[], ["--as-of", "2026-01-01T00:00:00+00:00"]])
def test_saved_projection_cli_cannot_backdate_or_guess_filing(monkeypatch, capsys, extra):
    from nexus.ops import openai_local

    monkeypatch.setattr(openai_local, "local_settings", lambda *a, **k: object())
    argv = ["nexus-openai-local", "project-sec-facts"]
    if extra:
        argv += ["--filing-id", "00000000-0000-4000-8000-000000000001", *extra]
    monkeypatch.setattr("sys.argv", argv)
    assert openai_local.main() == 1
    assert json.loads(capsys.readouterr().out)["code"] == "SEC_FACT_PROJECTION_ARGUMENTS_INVALID"


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


@pytest.mark.parametrize("action", ["refresh-research", "refresh-fund", "status"])
def test_disclosure_form_cannot_change_unrelated_command(action, tmp_path, monkeypatch, capsys):
    from nexus.ops.openai_local import main

    (tmp_path / "engine").mkdir()
    (tmp_path / "engine/pyproject.toml").write_text("")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.argv", ["nexus-openai-local", action, "--form", "8-K"])
    assert main() == 1
    assert json.loads(capsys.readouterr().out)["code"] == "SEC_LOCAL_DISCLOSURE_FORM_INVALID"


def test_8k_cli_reaches_contact_guard_without_loading_model_key(tmp_path, monkeypatch, capsys):
    from nexus.ops.openai_local import main

    (tmp_path / "engine").mkdir()
    (tmp_path / "engine/pyproject.toml").write_text("")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("SEC_CONTACT", raising=False)
    monkeypatch.setenv("OPENAI_API_KEY", "invalid key must not be loaded")
    monkeypatch.setattr(
        "sys.argv",
        [
            "nexus-openai-local",
            "refresh-disclosure",
            "--form",
            "8-K",
            "--instrument-id",
            "0198d500-1000-7000-8000-000000009001",
        ],
    )
    assert main() == 1
    assert json.loads(capsys.readouterr().out)["code"] == "SEC_CONTACT_REQUIRED"
