from pathlib import Path

import pytest
from nexus.ops import provider_worker_local
from nexus.ops.provider_worker_local import (
    LocalProviderWorkerConfigurationError,
    load_local_provider_worker_settings,
)


def _write_environment(repo_root: Path, *, explicit_provider: bool = False) -> None:
    repo_root.joinpath("infra").mkdir(parents=True)
    values = {
        "DATABASE_URL": "postgresql+asyncpg://local:private@postgres/nexus",
        "NEXUS_POSTGRES_HOST_PORT": "55433",
        "OBJECT_STORE_ENDPOINT": "http://minio:9000",
        "NEXUS_MINIO_HOST_PORT": "59000",
        "OBJECT_STORE_REGION": "us-east-1",
        "OBJECT_STORE_BUCKET": "nexus",
        "OBJECT_STORE_ACCESS_KEY_ID": "common-access-private",
        "OBJECT_STORE_SECRET_ACCESS_KEY": "common-secret-private",
        "NEXUS_PROVIDER_RUNTIME_MODE": "production",
        "NEXUS_MARKET_PROVIDER": "fosun",
        "NEXUS_INTRADAY_PROVIDER": "schwab",
        "NEXUS_INTRADAY_MONITOR_MODE": "production",
        "NEXUS_INTRADAY_MAX_SYMBOLS": "30",
        "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_SECRET": "fosun-private",
        "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_KEY_ID": "market-local-v1",
        "SEC_CONTACT": "owner@example.com",
        "FRED_API_KEY": "fred-private",
        "NEXUS_SCHWAB_APP_KEY": "schwab-key-private",
        "NEXUS_SCHWAB_APP_SECRET": "schwab-secret-private",
        "NEXUS_SCHWAB_CALLBACK_URL": "https://127.0.0.1",
    }
    if explicit_provider:
        values.update(
            {
                "PROVIDER_DATABASE_URL": "postgresql+asyncpg://dedicated.invalid/nexus",
                "PROVIDER_OBJECT_STORE_ENDPOINT": "https://store.invalid",
                "PROVIDER_OBJECT_STORE_REGION": "region-explicit",
                "PROVIDER_OBJECT_STORE_BUCKET": "bucket-explicit",
                "PROVIDER_OBJECT_STORE_ACCESS_KEY_ID": "explicit-access-private",
                "PROVIDER_OBJECT_STORE_SECRET_ACCESS_KEY": "explicit-secret-private",
            }
        )
    repo_root.joinpath("infra/.env.local").write_text(
        "".join(f"{key}={value}\n" for key, value in values.items())
    )


def test_local_loader_reuses_common_storage_and_localizes_compose_hosts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_environment(tmp_path)
    monkeypatch.setattr(provider_worker_local, "load_schwab_token", lambda _root: object())
    monkeypatch.setattr(provider_worker_local, "load_schwab_app_config", lambda *_: object())

    runtime, provider = load_local_provider_worker_settings(tmp_path, {})

    assert runtime.database_url.endswith("@127.0.0.1:55433/nexus")
    assert runtime.object_store_endpoint == "http://127.0.0.1:59000"
    assert runtime.object_store_prefix == "provider/market"
    assert provider.market_provider.value == "fosun"
    assert provider.selected_intraday_provider.value == "schwab"


def test_local_loader_preserves_explicit_provider_alias_and_process_precedence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_environment(tmp_path, explicit_provider=True)
    monkeypatch.setattr(provider_worker_local, "load_schwab_token", lambda _root: object())
    monkeypatch.setattr(provider_worker_local, "load_schwab_app_config", lambda *_: object())

    runtime, provider = load_local_provider_worker_settings(
        tmp_path,
        {
            "PROVIDER_DATABASE_URL": "postgresql+asyncpg://process.invalid/nexus",
            "NEXUS_INTRADAY_MAX_SYMBOLS": "12",
        },
    )

    assert runtime.database_url == "postgresql+asyncpg://process.invalid/nexus"
    assert runtime.object_store_endpoint == "https://store.invalid"
    assert provider.intraday_max_symbols == 12


def test_local_loader_rejects_absent_schwab_secret_path_with_safe_code(
    tmp_path: Path,
) -> None:
    _write_environment(tmp_path)

    with pytest.raises(
        LocalProviderWorkerConfigurationError,
        match="LOCAL_SCHWAB_AUTH_REQUIRED",
    ):
        load_local_provider_worker_settings(tmp_path, {})


def test_local_loader_and_cli_never_expose_secret_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_environment(tmp_path)
    monkeypatch.setattr(provider_worker_local, "load_schwab_token", lambda _root: object())
    monkeypatch.setattr(provider_worker_local, "load_schwab_app_config", lambda *_: object())

    settings = load_local_provider_worker_settings(tmp_path, {})

    rendered = repr(settings)
    assert "private" not in rendered
    parser = provider_worker_local.build_parser()
    assert set(vars(parser.parse_args(["--repo-root", str(tmp_path)]))) == {"repo_root"}


@pytest.mark.asyncio
async def test_local_runner_passes_exact_repo_root_to_worker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_environment(tmp_path)
    monkeypatch.setattr(provider_worker_local, "load_schwab_token", lambda _root: object())
    monkeypatch.setattr(provider_worker_local, "load_schwab_app_config", lambda *_: object())
    captured: dict[str, object] = {}

    async def runner(runtime, provider, **kwargs):
        captured.update(runtime=runtime, provider=provider, **kwargs)

    await provider_worker_local.run_local_provider_worker(
        tmp_path,
        process_environment={},
        runner=runner,
    )

    assert captured["run_forever"] is True
    assert captured["repo_root"] == tmp_path.resolve()
