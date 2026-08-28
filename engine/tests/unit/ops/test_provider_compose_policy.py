"""Compose capability separation for offline and production data providers."""

import re
from pathlib import Path

from nexus.ops.local_stack import LocalStackAction, build_local_stack_commands

ROOT = Path(__file__).parents[4]


def _service_block(compose: str, service: str) -> str:
    match = re.search(
        rf"(?ms)^  {re.escape(service)}:\n.*?(?=^  [a-z0-9-]+:\n|\Z)",
        compose,
    )
    assert match is not None
    return match.group(0)


def test_app_worker_defaults_to_fixture_without_provider_secrets() -> None:
    compose = (ROOT / "infra/compose.yaml").read_text()
    worker = _service_block(compose, "worker")

    assert "NEXUS_PROVIDER_RUNTIME_MODE=${NEXUS_PROVIDER_RUNTIME_MODE:-fixture}" in worker
    assert "ALPACA_API_KEY_ID" not in worker
    assert "ALPACA_API_SECRET_KEY" not in worker
    assert "FRED_API_KEY" not in worker


def test_provider_worker_is_a_separate_explicit_profile_and_secret_domain() -> None:
    compose = (ROOT / "infra/compose.yaml").read_text()
    provider = _service_block(compose, "provider-worker")

    assert 'profiles: ["provider"]' in provider
    assert "NEXUS_PROVIDER_RUNTIME_MODE=production" in provider
    for name in (
        "ALPACA_API_KEY_ID",
        "ALPACA_API_SECRET_KEY",
        "ALPACA_DATA_FEED",
        "ALPACA_DELIVERY_MODE",
        "SEC_CONTACT",
        "FRED_API_KEY",
    ):
        assert f"- {name}" in provider


def test_local_stack_never_starts_the_provider_profile(repo_root: Path) -> None:
    root = repo_root
    env = root / "infra/.env.local"
    commands = build_local_stack_commands(
        LocalStackAction.UP,
        repo_root=root,
        env_file=env,
        project_name="nexus-phase3-fixture",
    )

    assert all("provider" not in command.argv for command in commands)
    assert all("broker" not in command.argv for command in commands)
