"""Gateway settings isolated from engine and provider implementations."""

import hmac
from enum import StrEnum
from pathlib import Path
from typing import Literal, Self
from urllib.parse import urlsplit

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Environment(StrEnum):
    DEVELOPMENT = "development"
    TEST = "test"
    PRODUCTION = "production"


BROKER_LISTENER_PORT = 8081
MARKET_LISTENER_PORT = 8082


class GatewaySettings(BaseSettings):
    """Only configuration names are modeled; no provider client is created here."""

    model_config = SettingsConfigDict(
        env_file=None,
        env_ignore_empty=True,
        extra="ignore",
        hide_input_in_errors=True,
        populate_by_name=True,
    )

    environment: Environment = Field(
        default=Environment.DEVELOPMENT,
        validation_alias="NEXUS_ENV",
    )
    capability: Literal["READ_ONLY"] | None = Field(
        default=None,
        validation_alias="FOSUN_GATEWAY_CAPABILITY",
    )
    transport_mode: Literal["fixture", "read_only"] = Field(
        default="fixture",
        validation_alias="FOSUN_GATEWAY_TRANSPORT_MODE",
    )
    replica_count: Literal[1] = Field(
        default=1,
        validation_alias="FOSUN_GATEWAY_REPLICA_COUNT",
    )
    replay_store_path: Path | None = Field(
        default=None,
        validation_alias="FOSUN_GATEWAY_REPLAY_STORE_PATH",
    )
    gateway_hmac_current_key_id: str | None = Field(
        default=None,
        validation_alias="FOSUN_GATEWAY_HMAC_CURRENT_KEY_ID",
    )
    gateway_hmac_current_secret: SecretStr | None = Field(
        default=None,
        validation_alias="FOSUN_GATEWAY_HMAC_CURRENT_SECRET",
    )
    gateway_hmac_previous_key_id: str | None = Field(
        default=None,
        validation_alias="FOSUN_GATEWAY_HMAC_PREVIOUS_KEY_ID",
    )
    gateway_hmac_previous_secret: SecretStr | None = Field(
        default=None,
        validation_alias="FOSUN_GATEWAY_HMAC_PREVIOUS_SECRET",
    )
    market_gateway_hmac_current_key_id: str | None = Field(
        default=None,
        validation_alias="FOSUN_MARKET_GATEWAY_HMAC_CURRENT_KEY_ID",
    )
    market_gateway_hmac_current_secret: SecretStr | None = Field(
        default=None,
        validation_alias="FOSUN_MARKET_GATEWAY_HMAC_CURRENT_SECRET",
    )
    market_gateway_hmac_previous_key_id: str | None = Field(
        default=None,
        validation_alias="FOSUN_MARKET_GATEWAY_HMAC_PREVIOUS_KEY_ID",
    )
    market_gateway_hmac_previous_secret: SecretStr | None = Field(
        default=None,
        validation_alias="FOSUN_MARKET_GATEWAY_HMAC_PREVIOUS_SECRET",
    )
    server_cert: Path | None = Field(
        default=None,
        validation_alias="FOSUN_GATEWAY_SERVER_CERT",
    )
    server_key: Path | None = Field(
        default=None,
        validation_alias="FOSUN_GATEWAY_SERVER_KEY",
    )
    server_broker_client_ca: Path | None = Field(
        default=None,
        validation_alias="FOSUN_GATEWAY_SERVER_BROKER_CLIENT_CA",
    )
    server_market_client_ca: Path | None = Field(
        default=None,
        validation_alias="FOSUN_GATEWAY_SERVER_MARKET_CLIENT_CA",
    )
    sdk_admitted: bool = Field(default=False, validation_alias="FOSUN_SDK_ADMITTED")
    provider_manifest: Path = Field(
        default=Path("contracts/providers/fosun/manifest.json"),
        validation_alias="FOSUN_PROVIDER_MANIFEST",
    )
    activation_probe_artifact: Path | None = Field(
        default=None,
        validation_alias="FOSUN_ACTIVATION_PROBE_ARTIFACT",
    )
    activation_probe_receipt: Path | None = Field(
        default=None,
        validation_alias="FOSUN_ACTIVATION_PROBE_RECEIPT",
    )
    activation_probe_publisher_public_key_b64u_file: Path | None = Field(
        default=None,
        validation_alias="FOSUN_ACTIVATION_PROBE_PUBLISHER_PUBLIC_KEY_B64U_FILE",
    )
    activation_contract_hash_file: Path | None = Field(
        default=None,
        validation_alias="FOSUN_ACTIVATION_CONTRACT_HASH_FILE",
    )
    fsopenapi_base_url: str | None = Field(
        default=None,
        validation_alias="FSOPENAPI_BASE_URL",
    )
    fsopenapi_api_key: SecretStr | None = Field(
        default=None,
        validation_alias="FSOPENAPI_API_KEY",
    )
    fsopenapi_client_private_key: SecretStr | None = Field(
        default=None,
        validation_alias="FSOPENAPI_CLIENT_PRIVATE_KEY",
    )
    fsopenapi_server_public_key: SecretStr | None = Field(
        default=None,
        validation_alias="FSOPENAPI_SERVER_PUBLIC_KEY",
    )

    def __repr_args__(self) -> "list[tuple[str | None, object]]":
        return [
            ("environment", self.environment),
            ("capability", self.capability),
            ("transport_mode", self.transport_mode),
        ]

    @field_validator("fsopenapi_base_url")
    @classmethod
    def provider_base_url_requires_https(cls, value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip()
        parsed = urlsplit(stripped)
        try:
            parsed.port
        except ValueError as error:
            raise ValueError("provider base URL port is invalid") from error
        if (
            parsed.scheme.lower() != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("provider base URL must be an unambiguous HTTPS URL")
        return stripped.rstrip("/")

    @field_validator("sdk_admitted", mode="before")
    @classmethod
    def sdk_admission_switch_is_exact(cls, value: object) -> bool:
        if value is True or value == "true":
            return True
        if value is False or value == "false":
            return False
        raise ValueError("SDK admission switch must be exactly true or false")

    @field_validator(
        "activation_probe_artifact",
        "activation_probe_receipt",
        "activation_probe_publisher_public_key_b64u_file",
        "activation_contract_hash_file",
    )
    @classmethod
    def activation_receipt_path_is_absolute(cls, value: Path | None) -> Path | None:
        if value is not None and not value.is_absolute():
            raise ValueError("activation probe paths must be absolute")
        return value

    @model_validator(mode="after")
    def apply_environment_policy(self) -> Self:
        if self.transport_mode == "read_only" and self.environment is not Environment.PRODUCTION:
            raise ValueError("read_only transport requires the production mTLS environment")
        if self.environment is Environment.PRODUCTION:
            if (
                self.capability is None
                or self.transport_mode != "read_only"
                or self.replay_store_path is None
                or self.gateway_hmac_current_key_id is None
                or self.gateway_hmac_current_secret is None
                or self.market_gateway_hmac_current_key_id is None
                or self.market_gateway_hmac_current_secret is None
                or self.server_cert is None
                or self.server_key is None
                or self.server_broker_client_ca is None
                or self.server_market_client_ca is None
                or "provider_manifest" not in self.model_fields_set
                or not self.provider_manifest.is_absolute()
                or not self.sdk_admitted
                or self.activation_probe_artifact is None
                or self.activation_probe_receipt is None
                or self.activation_probe_publisher_public_key_b64u_file is None
                or self.activation_contract_hash_file is None
                or self.fsopenapi_base_url is None
                or self.fsopenapi_api_key is None
                or self.fsopenapi_client_private_key is None
                or self.fsopenapi_server_public_key is None
            ):
                raise ValueError(
                    "production settings require admitted read_only transport, TLS server "
                    "identity, an activation contract, provider config, and a current gateway "
                    "broker and market HMAC identities"
                )
            if self.server_broker_client_ca == self.server_market_client_ca:
                raise ValueError("production listeners require distinct client CA paths")
        elif self.capability is None:
            self.capability = "READ_ONLY"
        if self.replay_store_path is not None and not self.replay_store_path.is_absolute():
            raise ValueError("gateway replay store path must be absolute")
        if self.transport_mode == "fixture" and self.gateway_hmac_current_key_id is None:
            self.gateway_hmac_current_key_id = "fixture-gateway-current"
        if self.transport_mode == "fixture" and self.gateway_hmac_current_secret is None:
            self.gateway_hmac_current_secret = SecretStr("fixture-gateway-current-secret-32-bytes")
        if self.transport_mode == "fixture" and self.market_gateway_hmac_current_key_id is None:
            self.market_gateway_hmac_current_key_id = "fixture-market-gateway-current"
        if self.transport_mode == "fixture" and self.market_gateway_hmac_current_secret is None:
            self.market_gateway_hmac_current_secret = SecretStr(
                "fixture-market-gateway-current-secret-32-bytes"
            )
        if self.transport_mode == "read_only" and (
            self.gateway_hmac_current_key_id is None or self.gateway_hmac_current_secret is None
        ):
            raise ValueError("read_only transport requires an explicit gateway HMAC identity")
        if (
            self.environment is Environment.TEST
            and self.transport_mode == "fixture"
            and self.gateway_hmac_previous_key_id is None
        ):
            self.gateway_hmac_previous_key_id = "fixture-gateway-previous"
            self.gateway_hmac_previous_secret = SecretStr(
                "fixture-gateway-previous-secret-32-bytes"
            )
        if (
            self.environment is Environment.TEST
            and self.transport_mode == "fixture"
            and self.market_gateway_hmac_previous_key_id is None
        ):
            self.market_gateway_hmac_previous_key_id = "fixture-market-gateway-previous"
            self.market_gateway_hmac_previous_secret = SecretStr(
                "fixture-market-gateway-previous-secret-32-bytes"
            )
        identities = (
            (
                "broker",
                self.gateway_hmac_current_key_id,
                self.gateway_hmac_current_secret,
                self.gateway_hmac_previous_key_id,
                self.gateway_hmac_previous_secret,
            ),
            (
                "market",
                self.market_gateway_hmac_current_key_id,
                self.market_gateway_hmac_current_secret,
                self.market_gateway_hmac_previous_key_id,
                self.market_gateway_hmac_previous_secret,
            ),
        )
        key_ids: list[str] = []
        secrets: list[str] = []
        for role, current_key_id, current_secret, previous_key_id, previous_secret in identities:
            _validate_hmac_rotation(
                role,
                current_key_id,
                current_secret,
                previous_key_id,
                previous_secret,
            )
            key_ids.extend(
                key_id for key_id in (current_key_id, previous_key_id) if key_id is not None
            )
            secrets.extend(
                value
                for value in (
                    _strong_hmac_secret(current_secret),
                    _strong_hmac_secret(previous_secret),
                )
                if value is not None
            )
        if len(set(key_ids)) != len(key_ids):
            raise ValueError("gateway HMAC key IDs must be unique across principals")
        for index, secret in enumerate(secrets):
            if any(
                hmac.compare_digest(secret.encode(), other.encode())
                for other in secrets[index + 1 :]
            ):
                raise ValueError("gateway HMAC secrets must be unique across principals")
        return self

    @classmethod
    def for_test(cls) -> Self:
        return cls.model_validate(
            {
                "NEXUS_ENV": "test",
                "FOSUN_GATEWAY_CAPABILITY": "READ_ONLY",
                "FOSUN_GATEWAY_TRANSPORT_MODE": "fixture",
                "FOSUN_GATEWAY_HMAC_CURRENT_KEY_ID": "fixture-gateway-current",
                "FOSUN_GATEWAY_HMAC_CURRENT_SECRET": ("fixture-gateway-current-secret-32-bytes"),
                "FOSUN_GATEWAY_HMAC_PREVIOUS_KEY_ID": "fixture-gateway-previous",
                "FOSUN_GATEWAY_HMAC_PREVIOUS_SECRET": ("fixture-gateway-previous-secret-32-bytes"),
                "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_KEY_ID": ("fixture-market-gateway-current"),
                "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_SECRET": (
                    "fixture-market-gateway-current-secret-32-bytes"
                ),
                "FOSUN_MARKET_GATEWAY_HMAC_PREVIOUS_KEY_ID": ("fixture-market-gateway-previous"),
                "FOSUN_MARKET_GATEWAY_HMAC_PREVIOUS_SECRET": (
                    "fixture-market-gateway-previous-secret-32-bytes"
                ),
            }
        )


def _strong_hmac_secret(secret: SecretStr | None) -> str | None:
    if secret is None:
        return None
    value = secret.get_secret_value()
    if value != value.strip() or len(value.encode()) < 32:
        raise ValueError("gateway HMAC secrets must be unpadded and at least 32 bytes")
    return value


def _validate_hmac_rotation(
    role: str,
    current_key_id: str | None,
    current_secret: SecretStr | None,
    previous_key_id: str | None,
    previous_secret: SecretStr | None,
) -> None:
    if (current_key_id is None) != (current_secret is None):
        raise ValueError(f"current {role} gateway HMAC identity must be complete")
    if (previous_key_id is None) != (previous_secret is None):
        raise ValueError(f"previous {role} gateway HMAC identity must be complete")
    if previous_key_id is not None and current_key_id is None:
        raise ValueError(f"previous {role} gateway HMAC identity requires a current identity")
