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


def test_activation_artifact_is_mounted_only_into_the_provider_gateway_domain() -> None:
    compose = (WORKTREE_ROOT / "infra/compose.yaml").read_text()
    activation = {
        "FOSUN_ACTIVATION_PROBE_ARTIFACT",
        "FOSUN_ACTIVATION_CONTRACT_HASH",
    }

    assert activation <= _service_environment(compose, "fosun-gateway")
    for service in ("api", "worker", "broker-worker"):
        assert _service_environment(compose, service).isdisjoint(activation)


def test_compose_profiles_keep_broker_capability_out_of_the_app_profile() -> None:
    compose = (WORKTREE_ROOT / "infra/compose.yaml").read_text()

    assert _service_profiles(compose, "api") == {"app"}
    assert _service_profiles(compose, "worker") == {"app"}
    assert _service_profiles(compose, "broker-worker") == {"broker"}
    assert _service_profiles(compose, "fosun-gateway") == {"broker"}


def test_compose_worker_defaults_to_offline_fixture_runtime_but_is_overridable() -> None:
    compose = (WORKTREE_ROOT / "infra/compose.yaml").read_text()

    assert (
        "- AGENT_RUNTIME_MODE=${AGENT_RUNTIME_MODE:-fixture}"
        in _service_block(compose, "worker")
    )


def test_compose_host_ports_are_loopback_only_and_overridable() -> None:
    compose = (WORKTREE_ROOT / "infra/compose.yaml").read_text()

    expected = {
        "postgres": "127.0.0.1:${NEXUS_POSTGRES_HOST_PORT:-5432}:5432",
        "minio": "127.0.0.1:${NEXUS_MINIO_HOST_PORT:-9000}:9000",
        "api": "127.0.0.1:${NEXUS_API_HOST_PORT:-8000}:8000",
    }
    for service, binding in expected.items():
        block = _service_block(compose, service)
        assert f'      - "{binding}"' in block
    assert (
        '      - "127.0.0.1:${NEXUS_MINIO_CONSOLE_HOST_PORT:-9001}:9001"'
        in _service_block(compose, "minio")
    )


def test_compose_initializes_only_the_object_store_before_app_processes() -> None:
    compose = (WORKTREE_ROOT / "infra/compose.yaml").read_text()
    init = _service_block(compose, "object-store-init")

    assert 'command: ["nexus-engine-object-store-init"]' in init
    assert _service_profiles(compose, "object-store-init") == {"app"}
    assert _service_environment(compose, "object-store-init") == {
        "NEXUS_ENV",
        "OBJECT_STORE_ENDPOINT",
        "OBJECT_STORE_REGION",
        "OBJECT_STORE_BUCKET",
        "OBJECT_STORE_ACCESS_KEY_ID",
        "OBJECT_STORE_SECRET_ACCESS_KEY",
    }
    assert "condition: service_healthy" in init
    for service in ("api", "worker"):
        block = _service_block(compose, service)
        assert "object-store-init:" in block
        assert "condition: service_completed_successfully" in block


def test_engine_image_contains_migrations_and_packaged_contract_assets() -> None:
    dockerfile = (WORKTREE_ROOT / "infra/docker/engine.Dockerfile").read_text()

    assert "COPY contracts/strategy contracts/strategy" in dockerfile
    assert "COPY engine/alembic.ini engine/alembic.ini" in dockerfile
    assert "COPY engine/alembic engine/alembic" in dockerfile


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
