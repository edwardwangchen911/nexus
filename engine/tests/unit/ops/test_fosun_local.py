import stat
from pathlib import Path
from types import SimpleNamespace

import nexus.ops.fosun_local as fosun_local
import pytest
from nexus.ops.fosun_local import doctor_local_environment, safe_failure
from nexus.providers.broker.gateway_contracts import GatewayClientError


def _write_local_env(repo_root: Path) -> None:
    infra = repo_root / "infra"
    infra.mkdir()
    (infra / ".env.local").write_text(
        "DATABASE_URL=postgresql+asyncpg://local-canary\n"
        "OBJECT_STORE_ENDPOINT=http://127.0.0.1:9000\n"
        "OBJECT_STORE_REGION=us-east-1\n"
        "OBJECT_STORE_BUCKET=nexus-local\n"
        "OBJECT_STORE_ACCESS_KEY_ID=access-canary\n"
        "OBJECT_STORE_SECRET_ACCESS_KEY=secret-canary\n",
        encoding="utf-8",
    )


def test_local_environment_loads_ignored_env_and_derives_loopback_tls(tmp_path: Path) -> None:
    _write_local_env(tmp_path)

    environment = fosun_local.load_local_environment(tmp_path, {})

    secret_root = tmp_path / ".secrets" / "nexus-fosun-local"
    assert environment["DATABASE_URL"] == "postgresql+asyncpg://local-canary"
    assert environment["FOSUN_GATEWAY_BASE_URL"] == "https://127.0.0.1:8081"
    assert environment["FOSUN_MARKET_GATEWAY_BASE_URL"] == "https://127.0.0.1:8082"
    assert environment["FOSUN_GATEWAY_CLIENT_CERT"] == str(secret_root / "broker-client.pem")
    assert environment["FOSUN_GATEWAY_CLIENT_KEY"] == str(secret_root / "broker-client.key")
    assert environment["FOSUN_GATEWAY_CLIENT_CA"] == str(secret_root / "server-ca.pem")
    assert environment["FOSUN_MARKET_GATEWAY_CLIENT_CERT"] == str(secret_root / "market-client.pem")
    assert environment["FOSUN_MARKET_GATEWAY_CLIENT_KEY"] == str(secret_root / "market-client.key")
    assert environment["FOSUN_MARKET_GATEWAY_CLIENT_CA"] == str(secret_root / "server-ca.pem")


def test_explicit_environment_overrides_ignored_local_file(tmp_path: Path) -> None:
    _write_local_env(tmp_path)

    environment = fosun_local.load_local_environment(
        tmp_path,
        {"DATABASE_URL": "postgresql+asyncpg://explicit-canary"},
    )

    assert environment["DATABASE_URL"] == "postgresql+asyncpg://explicit-canary"


def test_local_environment_translates_compose_service_names_to_loopback(tmp_path: Path) -> None:
    infra = tmp_path / "infra"
    infra.mkdir()
    (infra / ".env.local").write_text(
        "DATABASE_URL=postgresql+asyncpg://user:password@postgres:5432/nexus\n"
        "OBJECT_STORE_ENDPOINT=http://minio:9000\n"
        "NEXUS_POSTGRES_HOST_PORT=55433\n"
        "NEXUS_MINIO_HOST_PORT=59000\n",
        encoding="utf-8",
    )

    environment = fosun_local.load_local_environment(tmp_path, {})

    assert environment["DATABASE_URL"] == (
        "postgresql+asyncpg://user:password@127.0.0.1:55433/nexus"
    )
    assert environment["OBJECT_STORE_ENDPOINT"] == "http://127.0.0.1:59000"


def test_invalid_local_host_port_fails_without_echoing_configuration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    infra = tmp_path / "infra"
    infra.mkdir()
    (infra / ".env.local").write_text(
        "DATABASE_URL=postgresql+asyncpg://user:secret-canary@postgres:5432/nexus\n"
        "NEXUS_POSTGRES_HOST_PORT=invalid-secret-canary\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    with pytest.raises(SystemExit):
        fosun_local.main(["doctor"])

    assert capsys.readouterr().err == (
        '{"category": "local", "code": "LOCAL_HOST_PORT_INVALID", "status": "failed"}\n'
    )


def test_init_creates_two_distinct_mode_600_hmac_identities(tmp_path: Path) -> None:
    result = fosun_local.initialize_local_hmac(tmp_path)

    identity_file = tmp_path / ".secrets" / "nexus-fosun-local" / "hmac.env"
    values = fosun_local.load_local_environment(tmp_path, {})
    assert result == {"status": "created", "hmac_identities": 2, "secrets_printed": False}
    assert stat.S_IMODE(identity_file.stat().st_mode) == 0o600
    assert values["FOSUN_GATEWAY_HMAC_CURRENT_KEY_ID"] == "broker-local-v1"
    assert values["FOSUN_MARKET_GATEWAY_HMAC_CURRENT_KEY_ID"] == "market-local-v1"
    assert len(values["FOSUN_GATEWAY_HMAC_CURRENT_SECRET"]) >= 32
    assert len(values["FOSUN_MARKET_GATEWAY_HMAC_CURRENT_SECRET"]) >= 32
    assert (
        values["FOSUN_GATEWAY_HMAC_CURRENT_SECRET"]
        != values["FOSUN_MARKET_GATEWAY_HMAC_CURRENT_SECRET"]
    )


def test_init_is_idempotent_and_never_rotates_existing_identities(tmp_path: Path) -> None:
    fosun_local.initialize_local_hmac(tmp_path)
    identity_file = tmp_path / ".secrets" / "nexus-fosun-local" / "hmac.env"
    original = identity_file.read_bytes()

    result = fosun_local.initialize_local_hmac(tmp_path)

    assert result == {"status": "exists", "hmac_identities": 2, "secrets_printed": False}
    assert identity_file.read_bytes() == original


def test_init_command_output_never_contains_generated_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    fosun_local.main(["init"])

    output = capsys.readouterr().out
    values = fosun_local.load_local_environment(tmp_path, {})
    assert '"status":"created"' in output
    assert values["FOSUN_GATEWAY_HMAC_CURRENT_SECRET"] not in output
    assert values["FOSUN_MARKET_GATEWAY_HMAC_CURRENT_SECRET"] not in output


def test_refresh_broker_command_uses_the_narrow_broker_only_entry(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    settings = object()

    async def refresh(received: object, *, identity_gap_dir: Path) -> dict[str, object]:
        assert identity_gap_dir == Path.cwd() / ".secrets/nexus-fosun-local/identity-gaps"
        assert received is settings
        return {"status": "ok", "mode": "broker_only"}

    monkeypatch.setattr(fosun_local, "load_local_environment", lambda *_args: {})
    monkeypatch.setattr(
        fosun_local,
        "LocalFosunBrokerRefreshSettings",
        SimpleNamespace(model_validate=lambda _environment: settings),
    )
    monkeypatch.setattr(fosun_local, "refresh_local_fosun_broker", refresh)

    fosun_local.main(["refresh-broker"])

    assert capsys.readouterr().out == '{"mode":"broker_only","status":"ok"}\n'


@pytest.mark.asyncio
async def test_refresh_resolves_pending_identity_then_refreshes_exactly_once(
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    refresh_results = iter(
        (
            {"status": "identity_pending", "snapshot_status": "PARTIAL"},
            {"status": "ok", "snapshot_status": "READY", "positions": 17},
        )
    )

    async def refresh(_settings: object) -> dict[str, object]:
        calls.append("refresh")
        return next(refresh_results)

    async def resolve(*args: object, **kwargs: object) -> dict[str, object]:
        calls.append("resolve")
        assert kwargs["output_dir"] == tmp_path / "pending"
        return {
            "status": "resolved",
            "resolved": 2,
            "remaining": 0,
            "portfolio_refresh_required": True,
            "execution_locked": True,
        }

    result = await fosun_local.refresh_local_fosun_resolving_pending(
        object(),
        {"FMP_API_KEY": "synthetic", "SEC_CONTACT": "nexus@example.com"},
        output_dir=tmp_path / "pending",
        identity_settings=object(),
        broker_settings=object(),
        refresh=refresh,
        resolve=resolve,
    )

    assert calls == ["refresh", "resolve", "refresh"]
    assert result == {
        "status": "ok",
        "snapshot_status": "READY",
        "positions": 17,
        "refresh_attempts": 2,
        "identity_resolution": {
            "status": "resolved",
            "resolved": 2,
            "remaining": 0,
            "portfolio_refresh_required": True,
            "execution_locked": True,
        },
    }


@pytest.mark.asyncio
async def test_refresh_stops_after_manual_identity_review_without_looping(tmp_path: Path) -> None:
    calls: list[str] = []

    async def refresh(_settings: object) -> dict[str, object]:
        calls.append("refresh")
        return {"status": "identity_pending", "snapshot_status": "PARTIAL"}

    async def resolve(*args: object, **kwargs: object) -> dict[str, object]:
        calls.append("resolve")
        return {
            "status": "manual_review_required",
            "resolved": 0,
            "remaining": 1,
            "portfolio_refresh_required": False,
            "execution_locked": True,
        }

    result = await fosun_local.refresh_local_fosun_resolving_pending(
        object(),
        {"FMP_API_KEY": "synthetic", "SEC_CONTACT": "nexus@example.com"},
        output_dir=tmp_path / "pending",
        identity_settings=object(),
        broker_settings=object(),
        refresh=refresh,
        resolve=resolve,
    )

    assert calls == ["refresh", "resolve"]
    assert result["status"] == "identity_pending"
    assert result["snapshot_status"] == "PARTIAL"
    assert result["refresh_attempts"] == 1
    assert result["identity_resolution"] == {
        "status": "manual_review_required",
        "resolved": 0,
        "remaining": 1,
        "portfolio_refresh_required": False,
        "execution_locked": True,
    }


@pytest.mark.asyncio
async def test_refresh_without_pending_identity_does_not_start_resolver(tmp_path: Path) -> None:
    resolver_called = False

    async def refresh(_settings: object) -> dict[str, object]:
        return {"status": "ok", "snapshot_status": "READY"}

    async def resolve(*args: object, **kwargs: object) -> dict[str, object]:
        nonlocal resolver_called
        resolver_called = True
        return {}

    result = await fosun_local.refresh_local_fosun_resolving_pending(
        object(),
        {},
        output_dir=tmp_path / "pending",
        identity_settings=object(),
        broker_settings=object(),
        refresh=refresh,
        resolve=resolve,
    )

    assert result == {"status": "ok", "snapshot_status": "READY"}
    assert resolver_called is False
    assert not (tmp_path / "pending").exists()


def test_refresh_command_uses_bounded_identity_orchestration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    refresh_settings = object()
    received: dict[str, object] = {}

    async def orchestrate(*args: object, **kwargs: object) -> dict[str, object]:
        received["args"] = args
        received["kwargs"] = kwargs
        return {"status": "ok", "snapshot_status": "READY"}

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        fosun_local,
        "load_local_environment",
        lambda *_args: {"FMP_API_KEY": "fmp-canary", "SEC_CONTACT": "nexus@example.com"},
    )
    monkeypatch.setattr(
        fosun_local,
        "LocalFosunRefreshSettings",
        SimpleNamespace(model_validate=lambda _environment: refresh_settings),
    )
    monkeypatch.setattr(
        fosun_local,
        "refresh_local_fosun_resolving_pending",
        orchestrate,
    )

    fosun_local.main(["refresh"])

    assert received["args"] == (
        refresh_settings,
        {"FMP_API_KEY": "fmp-canary", "SEC_CONTACT": "nexus@example.com"},
    )
    kwargs = received["kwargs"]
    assert isinstance(kwargs, dict)
    assert kwargs["output_dir"].parent == (
        tmp_path / ".secrets" / "nexus-fosun-local" / "pending-identity"
    )
    assert capsys.readouterr().out == '{"snapshot_status":"READY","status":"ok"}\n'


@pytest.mark.asyncio
async def test_status_database_boundary_returns_a_stable_redacted_failure() -> None:
    async def unavailable_reader(_database_url: str) -> dict[str, object]:
        raise OSError("database-location-canary")

    with pytest.raises(Exception) as exc_info:
        await fosun_local.read_local_status_safely(
            "postgresql+asyncpg://database-secret-canary",
            reader=unavailable_reader,
        )

    assert fosun_local.safe_failure(exc_info.value) == {
        "status": "failed",
        "category": "database",
        "code": "LOCAL_DATABASE_UNAVAILABLE",
    }
    assert "canary" not in repr(fosun_local.safe_failure(exc_info.value))


def test_doctor_reports_names_without_exposing_values() -> None:
    canary = "secret-value-must-not-appear"
    result = doctor_local_environment(
        {
            "DATABASE_URL": canary,
            "FOSUN_GATEWAY_CLIENT_CERT": "/missing/cert.pem",
        },
        is_file=lambda _path: False,
    )

    rendered = repr(result)
    assert result["status"] == "configuration_incomplete"
    assert "DATABASE_URL" not in result["missing_environment"]
    assert result["tls_files"] == {"FOSUN_GATEWAY_CLIENT_CERT": "missing"}
    assert canary not in rendered
    assert "/missing/cert.pem" not in rendered


def test_doctor_can_confirm_complete_configuration_without_network_or_database() -> None:
    environment = {
        name: "configured"
        for name in (
            "DATABASE_URL",
            "OBJECT_STORE_ENDPOINT",
            "OBJECT_STORE_REGION",
            "OBJECT_STORE_BUCKET",
            "OBJECT_STORE_ACCESS_KEY_ID",
            "OBJECT_STORE_SECRET_ACCESS_KEY",
            "FOSUN_GATEWAY_BASE_URL",
            "FOSUN_GATEWAY_HMAC_CURRENT_KEY_ID",
            "FOSUN_GATEWAY_HMAC_CURRENT_SECRET",
            "FOSUN_GATEWAY_CLIENT_CERT",
            "FOSUN_GATEWAY_CLIENT_KEY",
            "FOSUN_GATEWAY_CLIENT_CA",
            "FOSUN_MARKET_GATEWAY_BASE_URL",
            "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_KEY_ID",
            "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_SECRET",
            "FOSUN_MARKET_GATEWAY_CLIENT_CERT",
            "FOSUN_MARKET_GATEWAY_CLIENT_KEY",
            "FOSUN_MARKET_GATEWAY_CLIENT_CA",
            "FMP_API_KEY",
            "SEC_CONTACT",
        )
    }

    result = doctor_local_environment(environment, is_file=lambda _path: True)

    assert result == {
        "status": "ready",
        "configured_environment_count": 20,
        "missing_environment": (),
        "tls_files": {
            "FOSUN_GATEWAY_CLIENT_CA": "present",
            "FOSUN_GATEWAY_CLIENT_CERT": "present",
            "FOSUN_GATEWAY_CLIENT_KEY": "present",
            "FOSUN_MARKET_GATEWAY_CLIENT_CA": "present",
            "FOSUN_MARKET_GATEWAY_CLIENT_CERT": "present",
            "FOSUN_MARKET_GATEWAY_CLIENT_KEY": "present",
        },
        "network_checked": False,
        "database_checked": False,
    }


def test_safe_failure_classifies_gateway_errors_without_echoing_messages() -> None:
    result = safe_failure(GatewayClientError("provider-secret-canary"))

    assert result == {
        "status": "failed",
        "category": "gateway",
        "code": "LOCAL_READ_ONLY_REFRESH_FAILED",
    }
    assert "provider-secret-canary" not in repr(result)


@pytest.mark.parametrize(
    ("message", "category"),
    (
        ("gateway rejected request: PROVIDER_AUTHENTICATION_FAILED", "auth"),
        ("gateway rejected request: PROVIDER_PERMISSION_DENIED", "permission"),
        ("gateway rejected request: PROVIDER_RATE_LIMITED", "rate_limit"),
        ("gateway rejected request: PROVIDER_APPLICATION_ERROR", "provider"),
        ("gateway request failed", "transport"),
        ("gateway response contract rejected", "contract"),
    ),
)
def test_safe_failure_preserves_stable_gateway_categories(
    message: str,
    category: str,
) -> None:
    assert safe_failure(GatewayClientError(message))["category"] == category


def test_pending_latest_attempt_blocks_ready_snapshot_decisions() -> None:
    assert (
        fosun_local._decision_readiness_for_latest_attempt(
            "DECISION_READY",
            latest_run_status="PARTIAL",
            latest_failure_reason="IDENTITY_PENDING",
        )
        == "NOT_READY"
    )
    assert (
        fosun_local._decision_readiness_for_latest_attempt(
            "DECISION_READY",
            latest_run_status="READY",
            latest_failure_reason=None,
        )
        == "DECISION_READY"
    )
