"""Normal local API must receive the same manual admission scope as the host."""

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest


def test_api_manual_scope_is_resolved_from_protected_config(tmp_path: Path) -> None:
    if shutil.which("docker") is None:
        pytest.skip("Docker Compose is needed for the local configuration contract")
    root = Path(__file__).resolve().parents[4]
    keys = {
        "AGENT_RUNTIME_MODE": "codex_sdk",
        "NEXUS_AGENT_PAPER_CYCLE_ENABLED": "true",
        "NEXUS_AGENT_PAPER_MANUAL_ENABLED": "true",
        "NEXUS_AGENT_PAPER_ACCOUNT_ID": "0198f7a0-1111-7777-8111-111111111111",
        "NEXUS_AGENT_PAPER_CANDIDATE_BATCH_ID": "0198f7a0-1111-7777-8111-111111111112",
        "NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID": "0198f7a0-1111-7777-8111-111111111113",
        "NEXUS_FORMAL_BUDGET_PROTECTION_ENABLED": "true",
    }
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
    environment = json.loads(result.stdout)["services"]["api"]["environment"]
    assert {key: environment.get(key) for key in keys} == keys
    assert environment["PORTFOLIO_DECISION_RUNTIME_MODE"] == keys["AGENT_RUNTIME_MODE"]
