from datetime import UTC, datetime

import pytest
from nexus.notifications.bark import BARK_EVENT_SCHEMAS
from nexus.notifications.worker import (
    BarkWorkerConfigurationError,
    build_bark_registry,
    load_bark_credentials,
    load_bark_worker_settings,
)
from nexus.ops.bark_notifications import build_parser


def test_notifications_are_disabled_without_configuration() -> None:
    settings = load_bark_worker_settings({})

    assert settings.enabled is False
    assert settings.database_url is None


@pytest.mark.parametrize(
    "forbidden",
    ("NEXUS_BARK_DEVICE_KEY", "NEXUS_BARK_ENCRYPTION_KEY"),
)
def test_direct_secret_values_are_rejected(forbidden: str) -> None:
    with pytest.raises(
        BarkWorkerConfigurationError,
        match="^BARK_DIRECT_SECRET_FORBIDDEN$",
    ):
        load_bark_worker_settings({forbidden: "must-not-be-accepted"})


@pytest.mark.parametrize(
    "server_url",
    (
        "https://127.0.0.1:8080",
        "http://bark-server:8080",
        "http://192.168.1.8:8080",
        "http://user:password@127.0.0.1:8080",
        "http://127.0.0.1:8080/not-push",
    ),
)
def test_enabled_worker_rejects_non_localhost_server(server_url: str, tmp_path) -> None:
    device = tmp_path / "device-key"
    encryption = tmp_path / "encryption-key"
    device.write_text("device-key")
    encryption.write_text("0123456789abcdef")
    device.chmod(0o600)
    encryption.chmod(0o600)

    with pytest.raises(
        BarkWorkerConfigurationError,
        match="^BARK_CONFIGURATION_INVALID$",
    ):
        load_bark_worker_settings(
            {
                "NEXUS_BARK_ENABLED": "true",
                "DATABASE_URL": "postgresql+asyncpg://nexus@postgres/nexus",
                "NEXUS_BARK_SERVER_URL": server_url,
                "NEXUS_BARK_DEVICE_KEY_FILE": str(device),
                "NEXUS_BARK_ENCRYPTION_KEY_FILE": str(encryption),
                "NEXUS_BARK_ACTIVATED_AT": "2026-09-15T06:00:00Z",
            }
        )


def test_enabled_worker_loads_only_protected_files(tmp_path) -> None:
    device = tmp_path / "device-key"
    encryption = tmp_path / "encryption-key"
    device.write_text("device-key\n")
    encryption.write_text("0123456789abcdef\n")
    device.chmod(0o600)
    encryption.chmod(0o600)
    settings = load_bark_worker_settings(
        {
            "NEXUS_BARK_ENABLED": "true",
            "DATABASE_URL": "postgresql+asyncpg://nexus@postgres/nexus",
            "NEXUS_BARK_SERVER_URL": "http://127.0.0.1:8080",
            "NEXUS_BARK_DEVICE_KEY_FILE": str(device),
            "NEXUS_BARK_ENCRYPTION_KEY_FILE": str(encryption),
            "NEXUS_BARK_ACTIVATED_AT": "2026-09-15T06:00:00Z",
            "NEXUS_BARK_READ_ONLY_URL": "http://nexus.local/paper",
        }
    )

    credentials = load_bark_credentials(settings)

    assert credentials.device_key == "device-key"
    assert credentials.encryption_key == b"0123456789abcdef"
    assert "device-key" not in repr(credentials)
    assert "0123456789abcdef" not in repr(credentials)
    assert settings.activated_at == datetime(2026, 9, 15, 6, tzinfo=UTC)
    assert "device-key" not in repr(settings)
    assert "0123456789abcdef" not in repr(settings)


@pytest.mark.parametrize(
    ("mode", "key", "error_code"),
    (
        (0o644, "0123456789abcdef", "BARK_SECRET_FILE_UNSAFE"),
        (0o600, "too-short", "BARK_ENCRYPTION_KEY_INVALID"),
        (0o600, "éééééééé", "BARK_ENCRYPTION_KEY_INVALID"),
    ),
)
def test_credentials_reject_unsafe_mode_or_invalid_key(
    tmp_path,
    mode: int,
    key: str,
    error_code: str,
) -> None:
    device = tmp_path / "device-key"
    encryption = tmp_path / "encryption-key"
    device.write_text("device-key")
    encryption.write_text(key)
    device.chmod(0o600)
    encryption.chmod(mode)
    settings = load_bark_worker_settings(
        {
            "NEXUS_BARK_ENABLED": "true",
            "DATABASE_URL": "postgresql+asyncpg://nexus@postgres/nexus",
            "NEXUS_BARK_SERVER_URL": "http://localhost:8080",
            "NEXUS_BARK_DEVICE_KEY_FILE": str(device),
            "NEXUS_BARK_ENCRYPTION_KEY_FILE": str(encryption),
            "NEXUS_BARK_ACTIVATED_AT": "2026-09-15T06:00:00Z",
        }
    )

    with pytest.raises(BarkWorkerConfigurationError, match=f"^{error_code}$"):
        load_bark_credentials(settings)


@pytest.mark.parametrize(
    ("activated_at", "read_only_url"),
    (
        ("2026-09-15T06:00:00", "http://nexus.local/paper"),
        ("2026-09-15T06:00:00Z", "http://nexus.local/orders/approve"),
        ("2026-09-15T06:00:00Z", "file:///paper"),
    ),
)
def test_worker_rejects_ambiguous_time_or_non_read_only_link(
    tmp_path,
    activated_at: str,
    read_only_url: str,
) -> None:
    device = tmp_path / "device-key"
    encryption = tmp_path / "encryption-key"
    device.write_text("device-key")
    encryption.write_text("0123456789abcdef")
    device.chmod(0o600)
    encryption.chmod(0o600)

    with pytest.raises(
        BarkWorkerConfigurationError,
        match="^BARK_CONFIGURATION_INVALID$",
    ):
        load_bark_worker_settings(
            {
                "NEXUS_BARK_ENABLED": "true",
                "DATABASE_URL": "postgresql+asyncpg://nexus@postgres/nexus",
                "NEXUS_BARK_SERVER_URL": "http://127.0.0.1:8080",
                "NEXUS_BARK_DEVICE_KEY_FILE": str(device),
                "NEXUS_BARK_ENCRYPTION_KEY_FILE": str(encryption),
                "NEXUS_BARK_ACTIVATED_AT": activated_at,
                "NEXUS_BARK_READ_ONLY_URL": read_only_url,
            }
        )


def test_registry_contains_only_the_closed_bark_event_contract() -> None:
    async def endpoint(message) -> None:
        del message

    registry = build_bark_registry(endpoint)

    assert registry.keys == frozenset(BARK_EVENT_SCHEMAS)
    assert all(
        registry.resolve(event_type, schema_version).consumer_idempotent
        for event_type, schema_version in BARK_EVENT_SCHEMAS
    )


def test_cli_exposes_no_secret_bearing_arguments() -> None:
    parser = build_parser()

    assert parser.parse_args([]).once is False
    assert parser.parse_args(["--once"]).once is True
    assert all(
        "key" not in option for action in parser._actions for option in action.option_strings
    )
