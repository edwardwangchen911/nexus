"""Static Compose gates for the separately activated intraday monitor."""

import re
from pathlib import Path

ROOT = Path(__file__).parents[4]
INTRADAY_ENVIRONMENT_KEYS = {
    "NEXUS_INTRADAY_MONITOR_MODE",
    "NEXUS_INTRADAY_POLL_SECONDS",
    "NEXUS_INTRADAY_MAX_SYMBOLS",
    "ALPACA_INTRADAY_FEED",
    "ALPACA_INTRADAY_DELIVERY_MODE",
}


def _service_block(compose: str, service: str) -> str:
    match = re.search(
        rf"(?ms)^  {re.escape(service)}:\n.*?(?=^  [a-z0-9-]+:\n|\Z)",
        compose,
    )
    assert match is not None
    return match.group(0)


def test_intraday_monitor_is_only_configurable_in_provider_worker() -> None:
    compose = (ROOT / "infra/compose.yaml").read_text(encoding="utf-8")
    provider = _service_block(compose, "provider-worker")

    assert "NEXUS_INTRADAY_MONITOR_MODE=${NEXUS_INTRADAY_MONITOR_MODE:-disabled}" in provider
    assert "NEXUS_INTRADAY_POLL_SECONDS=${NEXUS_INTRADAY_POLL_SECONDS:-15}" in provider
    assert "NEXUS_INTRADAY_MAX_SYMBOLS=${NEXUS_INTRADAY_MAX_SYMBOLS:-10}" in provider
    assert "- ALPACA_INTRADAY_FEED" in provider
    assert "- ALPACA_INTRADAY_DELIVERY_MODE" in provider

    for service in (
        "api",
        "worker",
        "broker-worker",
        "fosun-gateway",
        "broker-simulation-worker",
        "fosun-simulation-gateway",
    ):
        block = _service_block(compose, service)
        assert not any(key in block for key in INTRADAY_ENVIRONMENT_KEYS)


def test_intraday_defaults_are_disabled_and_do_not_contain_credentials() -> None:
    environment = (ROOT / "infra/env.example").read_text(encoding="utf-8")

    assert "NEXUS_INTRADAY_MONITOR_MODE=disabled" in environment
    assert "NEXUS_INTRADAY_POLL_SECONDS=15" in environment
    assert "NEXUS_INTRADAY_MAX_SYMBOLS=10" in environment
    for key in INTRADAY_ENVIRONMENT_KEYS:
        assert environment.count(f"{key}=") == 1
    assert "NEXUS_INTRADAY_API_KEY" not in environment
    assert "NEXUS_INTRADAY_SECRET" not in environment


def test_intraday_runbook_preserves_external_activation_stop_boundary() -> None:
    runbook = (ROOT / "docs/runbooks/intraday-market-monitor.md").read_text(encoding="utf-8")

    assert "Intraday Monitor offline_ready / external_data_activation_blocked" in runbook
    assert "GET https://data.alpaca.markets/v2/stocks/snapshots" in runbook
    assert "逐次批准" in runbook
    assert "真实订单" in runbook
