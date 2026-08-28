"""Static Phase 3A profile and Phase 3B activation-boundary checks."""

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).parents[4]


def test_app_profile_has_no_broker_simulation_service_or_secret_mapping() -> None:
    compose = (ROOT / "infra/compose.yaml").read_text()
    for service in ("api", "worker", "object-store-init"):
        block = _service_block(compose, service)
        assert "broker-simulation" not in _profiles(block)
        assert "FOSUN_SIMULATION_GATEWAY_" not in block
        assert "NEXUS_BROKER_SIMULATION_MODE" not in block


def test_simulation_profile_uses_distinct_process_and_host_secret_domains() -> None:
    compose = (ROOT / "infra/compose.yaml").read_text()
    gateway = _service_block(compose, "fosun-simulation-gateway")
    worker = _service_block(compose, "broker-simulation-worker")

    assert _profiles(gateway) == {"broker-simulation"}
    assert _profiles(worker) == {"broker-simulation"}
    assert 'command: ["nexus-fosun-simulation-fixture"]' in gateway
    assert 'command: ["nexus-engine-broker-worker"]' in worker
    assert "NEXUS_BROKER_SIMULATION_MODE=offline_fixture" in worker
    assert "fosun-simulation-gateway:" in worker

    distinct = {
        "FOSUN_SIMULATION_GATEWAY_BASE_URL",
        "FOSUN_SIMULATION_GATEWAY_HMAC_CURRENT_KEY_ID",
        "FOSUN_SIMULATION_GATEWAY_HMAC_CURRENT_SECRET",
        "FOSUN_SIMULATION_GATEWAY_CLIENT_CERT",
        "FOSUN_SIMULATION_GATEWAY_CLIENT_KEY",
        "FOSUN_SIMULATION_GATEWAY_CLIENT_CA",
        "FOSUN_SIMULATION_GATEWAY_SERVER_CERT",
        "FOSUN_SIMULATION_GATEWAY_SERVER_KEY",
        "FOSUN_SIMULATION_GATEWAY_SERVER_CLIENT_CA",
    }
    assert all(f"${{{name}" in gateway + worker for name in distinct)
    assert "FSOPENAPI_API_KEY" not in gateway
    assert "FSOPENAPI_CLIENT_PRIVATE_KEY" not in gateway
    assert "FSOPENAPI_SERVER_PUBLIC_KEY" not in gateway


def test_simulation_and_live_write_are_not_activated_by_repository_defaults() -> None:
    compose = (ROOT / "infra/compose.yaml").read_text()
    env_example = (ROOT / "infra/env.example").read_text()
    defaults = compose + env_example

    assert "SIMULATION_WRITE_ACTIVATED" not in defaults
    assert not re.search(
        r"(?m)^[A-Z0-9_]*SIMULATION_WRITE[A-Z0-9_]*=(?:1|true|yes)$",
        defaults,
        re.I,
    )
    assert not re.search(r"(?m)^[A-Z0-9_]*LIVE_WRITE[A-Z0-9_]*=", defaults)


def test_read_only_gateway_composition_cannot_compile_simulation_routes() -> None:
    source = (ROOT / "integrations/fosun_gateway/src/nexus_fosun_gateway/main.py").read_text()
    assert "simulation_router" not in source
    assert "fosun-simulation" not in source
    assert "read_only_router" in source


def test_tracked_files_contain_no_provider_credential_shaped_value() -> None:
    tracked = subprocess.run(
        ["git", "-C", str(ROOT), "ls-files", "-z"],
        check=True,
        capture_output=True,
    ).stdout.split(b"\0")
    provider_key = rb"(?<![A-Za-z0-9])a" + rb"k_[A-Za-z0-9_-]{24,}"
    private_key = rb"-----BEGIN " + rb"(?:RSA |EC )?PRIVATE KEY-----"
    configured_api_key = rb"FSOPENAPI_API_" + rb"KEY=[A-Za-z0-9_-]{12,}={0,2}"
    forbidden = re.compile(
        rb"(?:" + provider_key + rb"|" + private_key + rb"|" + configured_api_key + rb")"
    )
    findings: list[str] = []
    for raw_path in tracked:
        if not raw_path:
            continue
        path = ROOT / raw_path.decode()
        try:
            content = path.read_bytes()
        except (OSError, UnicodeError):
            continue
        if forbidden.search(content):
            findings.append(path.relative_to(ROOT).as_posix())
    assert findings == []


def _service_block(compose: str, service: str) -> str:
    match = re.search(
        rf"(?ms)^  {re.escape(service)}:\n.*?(?=^  [a-z0-9-]+:\n|\Z)",
        compose,
    )
    assert match is not None
    return match.group(0)


def _profiles(block: str) -> set[str]:
    match = re.search(r'(?m)^    profiles: \[(?P<profiles>[^]]+)\]$', block)
    assert match is not None
    return {value.strip().strip('"') for value in match.group("profiles").split(",")}
