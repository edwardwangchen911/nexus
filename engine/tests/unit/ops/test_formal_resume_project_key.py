from types import SimpleNamespace
from uuid import uuid4

import pytest
from nexus.ops import agent_paper_once
from nexus.ops.platform_credentials import PLATFORM_KEY_PATH


@pytest.mark.parametrize("key_state", ["present", "missing", "invalid"])
def test_formal_resume_binds_private_project_environment(tmp_path, monkeypatch, capsys, key_state):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "infra").mkdir()
    (tmp_path / "infra/.env.local").write_text(
        "AGENT_RUNTIME_MODE=responses\nOPENAI_REASONING_MODEL=gpt-6.1-sol\n"
        "OPENAI_EXTRACTION_MODEL=gpt-6.1-sol\nNEXUS_AGENT_PAPER_CYCLE_ENABLED=true\n"
    )
    key = "synthetic-private-project-key"
    file = tmp_path / PLATFORM_KEY_PATH
    if key_state != "missing":
        file.parent.mkdir(parents=True, mode=0o700)
        file.write_text(key if key_state == "present" else "invalid key")
        file.chmod(0o600)
    calls = []

    async def observe(settings, request):
        calls.append(request.cycle_id)
        assert settings.openai_api_key.get_secret_value() == key
        return SimpleNamespace(model_dump=lambda **_: {"status": "readback"})

    monkeypatch.setattr(agent_paper_once, "resume_once", observe)
    monkeypatch.setenv("OPENAI_API_KEY", "ambient-must-not-bind")
    cycle = uuid4()
    code = agent_paper_once.main(
        [
            "resume",
            "--cycle-id",
            str(cycle),
            "--idempotency-key",
            "original",
            "--formal-stage-capacity",
        ]
    )
    captured = capsys.readouterr()
    assert (
        key not in captured.out + captured.err
        and "ambient-must-not-bind" not in captured.out + captured.err
    )
    assert calls == ([cycle] if key_state == "present" else [])
    assert code == (0 if key_state == "present" else 1)
    if key_state != "present":
        assert "AGENT_PAPER_RESULT_UNKNOWN" in (captured.out + captured.err)


def test_formal_resume_does_not_interpolate_ambient_budget(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "infra").mkdir()
    (tmp_path / "infra/.env.local").write_text(
        "AGENT_RUNTIME_MODE=responses\nOPENAI_REASONING_MODEL=gpt-6.1-sol\n"
        "OPENAI_EXTRACTION_MODEL=gpt-6.1-sol\nNEXUS_AGENT_PAPER_CYCLE_ENABLED=true\n"
        "AGENT_DAILY_BUDGET_USD=${FORMAL_RESUME_TEST_BUDGET}\n"
    )
    file = tmp_path / PLATFORM_KEY_PATH
    file.parent.mkdir(parents=True, mode=0o700)
    file.write_text("synthetic-private-project-key")
    file.chmod(0o600)
    monkeypatch.setenv("FORMAL_RESUME_TEST_BUDGET", "999")
    calls = []

    async def observe(settings, request):
        calls.append(request.cycle_id)
        return SimpleNamespace(model_dump=lambda **_: {"status": "readback"})

    monkeypatch.setattr(agent_paper_once, "resume_once", observe)
    code = agent_paper_once.main(
        [
            "resume",
            "--cycle-id",
            str(uuid4()),
            "--idempotency-key",
            "original",
            "--formal-stage-capacity",
        ]
    )
    assert calls == []
    assert code == 1
    assert "AGENT_PAPER_RESULT_UNKNOWN" in capsys.readouterr().out
