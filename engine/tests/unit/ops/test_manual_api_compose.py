"""Normal local API must receive the same manual admission scope as the host."""

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest


def api_environment(tmp_path: Path, keys: dict[str, str]) -> dict[str, str | None]:
    if shutil.which("docker") is None:
        pytest.skip("Docker Compose is needed for the local configuration contract")
    root = Path(__file__).resolve().parents[4]
    config = tmp_path / "isolated.env"
    config.write_text("\n".join(f"{key}={value}" for key, value in keys.items()))
    result = subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            str(config),
            "-f",
            str(root / "infra/compose.yaml"),
            "--profile",
            "app",
            "config",
            "--format",
            "json",
        ],
        env={
            key: value
            for key, value in os.environ.items()
            if key in {"PATH", "HOME", "DOCKER_CONFIG"}
        },
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)["services"]["api"]["environment"]


def test_api_manual_scope_is_resolved_from_protected_config(tmp_path: Path) -> None:
    keys = {
        "AGENT_RUNTIME_MODE": "codex_sdk",
        "NEXUS_AGENT_PAPER_CYCLE_ENABLED": "true",
        "NEXUS_AGENT_PAPER_MANUAL_ENABLED": "true",
        "NEXUS_AGENT_PAPER_ACCOUNT_ID": "0198f7a0-1111-7777-8111-111111111111",
        "NEXUS_AGENT_PAPER_CANDIDATE_BATCH_ID": "0198f7a0-1111-7777-8111-111111111112",
        "NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID": "0198f7a0-1111-7777-8111-111111111113",
        "NEXUS_FORMAL_BUDGET_PROTECTION_ENABLED": "true",
    }
    environment = api_environment(tmp_path, keys)
    assert {key: environment.get(key) for key in keys} == keys
    assert environment["PORTFOLIO_DECISION_RUNTIME_MODE"] == keys["AGENT_RUNTIME_MODE"]


def test_responses_host_scope_can_boot_keyless_api(tmp_path, monkeypatch):
    from nexus.main import create_engine_app
    from nexus.paper.manual_entry import manual_host_scope
    from nexus.settings import AgentRuntimeMode, EngineSettings
    from pydantic import ValidationError

    keys = {
        "NEXUS_ENV": "test",
        "DATABASE_URL": "postgresql+asyncpg://localhost/unused",
        "AGENT_RUNTIME_MODE": "responses",
        "AGENT_DAILY_BUDGET_USD": "1.7345",
        "OPENAI_EXTRACTION_MODEL": "gpt-6.1-sol",
        "OPENAI_REASONING_MODEL": "gpt-6.1-sol",
    }
    env = api_environment(tmp_path, keys)
    assert not env.get("OPENAI_API_KEY")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    for k, v in env.items():
        if v is not None:
            monkeypatch.setenv(k, v)
    with pytest.raises(ValidationError, match="Agent non-fixture mode requires OPENAI_API_KEY"):
        EngineSettings()
    host = EngineSettings(**(keys | {"OPENAI_API_KEY": "synthetic-host-only"}))
    api = create_engine_app().state.settings
    assert manual_host_scope(api) == manual_host_scope(host)
    assert api.agent_daily_budget_usd == host.agent_daily_budget_usd
    assert api.research_admission_mode is AgentRuntimeMode.RESPONSES
    assert api.agent_runtime_mode is AgentRuntimeMode.FIXTURE
    assert api.portfolio_decision_runtime_mode is AgentRuntimeMode.RESPONSES
    assert api.openai_api_key is None
    monkeypatch.setenv("OPENAI_API_KEY", "synthetic-inherited-key")
    assert create_engine_app().state.settings.openai_api_key is None


@pytest.mark.parametrize(
    ("keys", "research_mode", "portfolio_mode"),
    [
        ({}, "fixture", "fixture"),
        (
            {"AGENT_RUNTIME_MODE": "responses", "PORTFOLIO_DECISION_RUNTIME_MODE": ""},
            "responses",
            "responses",
        ),
        (
            {"AGENT_RUNTIME_MODE": "responses", "PORTFOLIO_DECISION_RUNTIME_MODE": "codex_sdk"},
            "responses",
            "codex_sdk",
        ),
    ],
)
def test_portfolio_mode_is_resolved_independently_of_api_credentials(
    tmp_path, keys, research_mode, portfolio_mode
):
    settings = api_environment(tmp_path, {**keys, "OPENAI_API_KEY": "synthetic-host-only"})
    assert settings["AGENT_RUNTIME_MODE"] == research_mode
    assert settings["PORTFOLIO_DECISION_RUNTIME_MODE"] == portfolio_mode
    assert "OPENAI_API_KEY" not in settings
