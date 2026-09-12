"""Mac-local entry for the existing provider worker and protected configuration."""

from __future__ import annotations

import argparse
import asyncio
import os
from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path

from pydantic import ValidationError

from nexus.ops.fosun_local import load_local_environment
from nexus.provider_worker import run_provider_worker
from nexus.providers.market.schwab_oauth import (
    SchwabOAuthError,
    load_schwab_app_config,
    load_schwab_token,
)
from nexus.providers.settings import (
    IntradayMonitorMode,
    IntradayProvider,
    ProviderSettings,
    ProviderWorkerSettings,
)

_STORAGE_ALIASES = {
    "PROVIDER_DATABASE_URL": "DATABASE_URL",
    "PROVIDER_OBJECT_STORE_ENDPOINT": "OBJECT_STORE_ENDPOINT",
    "PROVIDER_OBJECT_STORE_REGION": "OBJECT_STORE_REGION",
    "PROVIDER_OBJECT_STORE_BUCKET": "OBJECT_STORE_BUCKET",
    "PROVIDER_OBJECT_STORE_ACCESS_KEY_ID": "OBJECT_STORE_ACCESS_KEY_ID",
    "PROVIDER_OBJECT_STORE_SECRET_ACCESS_KEY": "OBJECT_STORE_SECRET_ACCESS_KEY",
}


class LocalProviderWorkerConfigurationError(RuntimeError):
    """Value-free local startup rejection."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, required=True)
    return parser


def load_local_provider_worker_settings(
    repo_root: Path,
    process_environment: Mapping[str, str],
) -> tuple[ProviderWorkerSettings, ProviderSettings]:
    """Load ignored local config without mutating the process environment."""
    root = repo_root.resolve()
    environment = load_local_environment(root, process_environment)
    for provider_name, common_name in _STORAGE_ALIASES.items():
        if not environment.get(provider_name) and environment.get(common_name):
            environment[provider_name] = environment[common_name]
    environment.setdefault("PROVIDER_OBJECT_STORE_PREFIX", "provider/market")
    try:
        runtime = ProviderWorkerSettings.model_validate(environment)
        provider = ProviderSettings.model_validate(environment)
    except ValidationError:
        raise LocalProviderWorkerConfigurationError(
            "LOCAL_PROVIDER_WORKER_CONFIGURATION_INVALID"
        ) from None
    if (
        provider.intraday_monitor_mode is IntradayMonitorMode.PRODUCTION
        and provider.selected_intraday_provider is IntradayProvider.SCHWAB
    ):
        try:
            load_schwab_app_config(root, environment)
            load_schwab_token(root)
        except SchwabOAuthError:
            raise LocalProviderWorkerConfigurationError(
                "LOCAL_SCHWAB_AUTH_REQUIRED"
            ) from None
    return runtime, provider


async def run_local_provider_worker(
    repo_root: Path,
    *,
    process_environment: Mapping[str, str] | None = None,
    runner: Callable[..., Awaitable[None]] = run_provider_worker,
) -> None:
    runtime, provider = load_local_provider_worker_settings(
        repo_root,
        os.environ if process_environment is None else process_environment,
    )
    await runner(runtime, provider, run_forever=True, repo_root=repo_root.resolve())


def main() -> None:
    parser = build_parser()
    arguments = parser.parse_args()
    try:
        asyncio.run(run_local_provider_worker(arguments.repo_root))
    except LocalProviderWorkerConfigurationError as error:
        parser.error(str(error))
    except KeyboardInterrupt:
        pass
