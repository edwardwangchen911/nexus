import re
from pathlib import Path

WORKTREE_ROOT = Path(__file__).parents[4]


def test_compose_separates_broker_secrets_from_api_and_research_worker() -> None:
    compose = (WORKTREE_ROOT / "infra/compose.yaml").read_text()
    api_environment = _service_environment(compose, "api")
    research_environment = _service_environment(compose, "worker")
    broker_environment = _service_environment(compose, "broker-worker")

    forbidden = {
        "FOSUN_GATEWAY_BASE_URL",
        "FOSUN_GATEWAY_HMAC_CURRENT_KEY_ID",
        "FOSUN_GATEWAY_HMAC_CURRENT_SECRET",
        "FOSUN_GATEWAY_CLIENT_CERT",
        "FOSUN_GATEWAY_CLIENT_KEY",
        "FOSUN_GATEWAY_CLIENT_CA",
        "NEXUS_BROKER_EXTERNAL_ID_AEAD_CURRENT_KEY",
        "NEXUS_BROKER_EXTERNAL_ID_AEAD_CURRENT_KEY_VERSION",
        "NEXUS_BROKER_EXTERNAL_ID_AEAD_PREVIOUS_KEY",
        "NEXUS_BROKER_EXTERNAL_ID_AEAD_PREVIOUS_KEY_VERSION",
        "NEXUS_BROKER_EXTERNAL_ID_BLIND_INDEX_KEY",
    }
    assert api_environment.isdisjoint(forbidden)
    assert research_environment.isdisjoint(forbidden)
    assert forbidden <= broker_environment
    assert not any(name.startswith("OPENAI_") for name in broker_environment)
    assert "env_file:" not in _service_block(compose, "api")
    assert "env_file:" not in _service_block(compose, "worker")
    assert "env_file:" not in _service_block(compose, "broker-worker")


def test_compose_profiles_keep_broker_capability_out_of_the_app_profile() -> None:
    compose = (WORKTREE_ROOT / "infra/compose.yaml").read_text()

    assert _service_profiles(compose, "api") == {"app"}
    assert _service_profiles(compose, "worker") == {"app"}
    assert _service_profiles(compose, "broker-worker") == {"broker"}
    assert _service_profiles(compose, "fosun-gateway") == {"broker"}


def test_compose_worker_explicitly_selects_responses_runtime() -> None:
    compose = (WORKTREE_ROOT / "infra/compose.yaml").read_text()

    assert "- AGENT_RUNTIME_MODE=responses" in _service_block(compose, "worker")


def _service_block(compose: str, service: str) -> str:
    match = re.search(
        rf"(?ms)^  {re.escape(service)}:\n(?P<body>.*?)(?=^  [a-z0-9-]+:\n|\Z)",
        compose,
    )
    assert match is not None
    return match.group(0)


def _service_environment(compose: str, service: str) -> set[str]:
    return set(re.findall(r"(?m)^      - ([A-Z0-9_]+)$", _service_block(compose, service)))


def _service_profiles(compose: str, service: str) -> set[str]:
    block = _service_block(compose, service)
    match = re.search(r"(?m)^    profiles: \[(?P<profiles>[^]]+)\]$", block)
    assert match is not None
    return {profile.strip().strip('"') for profile in match.group("profiles").split(",")}
