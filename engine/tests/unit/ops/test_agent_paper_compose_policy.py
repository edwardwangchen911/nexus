"""Static least-capability contract for the opt-in Agent paper service."""

import re
from pathlib import Path

ROOT = Path(__file__).parents[4]


def _service_block(compose: str, service: str) -> str:
    match = re.search(
        rf"(?ms)^  {re.escape(service)}:\n.*?(?=^  [a-z0-9-]+:\n|\Z)",
        compose,
    )
    assert match is not None
    return match.group(0)


def test_agent_paper_service_is_opt_in_fail_closed_and_has_no_broker_capability() -> None:
    compose = (ROOT / "infra/compose.yaml").read_text(encoding="utf-8")
    service = _service_block(compose, "agent-paper-loop")
    object_store_init = _service_block(compose, "object-store-init")

    assert 'profiles: ["agent-paper"]' in service
    assert 'command: ["nexus-engine-agent-paper-loop"]' in service
    assert 'restart: "no"' in service
    assert "NEXUS_AGENT_PAPER_CYCLE_ENABLED=${NEXUS_AGENT_PAPER_CYCLE_ENABLED:-false}" in service
    assert "network_mode: host" in service
    assert "NEXUS_AGENT_PAPER_ACCOUNT_ID" in service
    assert "NEXUS_SCHWAB_APP_KEY" in service
    assert "NEXUS_SCHWAB_APP_SECRET" in service
    assert "FOSUN_GATEWAY_" not in service
    assert "NEXUS_BROKER_" not in service
    assert "broker-worker:" not in service
    assert 'profiles: ["app", "agent-paper"]' in object_store_init


def test_agent_paper_service_mounts_only_its_explicit_private_directories() -> None:
    compose = (ROOT / "infra/compose.yaml").read_text(encoding="utf-8")
    service = _service_block(compose, "agent-paper-loop")

    assert "../.secrets/nexus-schwab-local" in service
    assert "/app/.secrets/nexus-schwab-local" in service
    assert "read_only: true" in service
    assert "../.secrets/nexus-agent-paper" in service
    assert "/app/.secrets/nexus-agent-paper" in service
    assert "../.secrets/nexus-fosun-local" not in service
