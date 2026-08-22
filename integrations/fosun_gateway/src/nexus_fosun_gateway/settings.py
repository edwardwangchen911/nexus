"""Gateway settings isolated from engine and provider implementations."""

import hmac
from enum import StrEnum
from pathlib import Path
from typing import Literal, Self

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Environment(StrEnum):
    DEVELOPMENT = "development"
    TEST = "test"
    PRODUCTION = "production"


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
    server_cert: Path | None = Field(
        default=None,
        validation_alias="FOSUN_GATEWAY_SERVER_CERT",
    )
    server_key: Path | None = Field(
        default=None,
        validation_alias="FOSUN_GATEWAY_SERVER_KEY",
    )
    server_client_ca: Path | None = Field(
        default=None,
        validation_alias="FOSUN_GATEWAY_SERVER_CLIENT_CA",
    )
    sdk_admitted: bool = Field(default=False, validation_alias="FOSUN_SDK_ADMITTED")
    provider_manifest: Path = Field(
        default=Path("contracts/providers/fosun/manifest.json"),
        validation_alias="FOSUN_PROVIDER_MANIFEST",
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

    @model_validator(mode="after")
    def apply_environment_policy(self) -> Self:
        if self.environment is Environment.PRODUCTION:
            if (
                self.capability is None
                or self.transport_mode != "read_only"
                or self.gateway_hmac_current_key_id is None
                or self.gateway_hmac_current_secret is None
                or self.server_cert is None
                or self.server_key is None
                or self.server_client_ca is None
                or not self.sdk_admitted
                or self.fsopenapi_base_url is None
                or self.fsopenapi_api_key is None
                or self.fsopenapi_client_private_key is None
                or self.fsopenapi_server_public_key is None
            ):
                raise ValueError(
                    "production settings require admitted read_only transport, TLS server "
                    "identity, provider config, and a current gateway HMAC identity"
                )
        elif self.capability is None:
            self.capability = "READ_ONLY"
        if self.transport_mode == "fixture" and self.gateway_hmac_current_key_id is None:
            self.gateway_hmac_current_key_id = "fixture-gateway-current"
        if self.transport_mode == "fixture" and self.gateway_hmac_current_secret is None:
            self.gateway_hmac_current_secret = SecretStr("fixture-gateway-current-secret-32-bytes")
        if (
            self.transport_mode == "read_only"
            and (
                self.gateway_hmac_current_key_id is None
                or self.gateway_hmac_current_secret is None
            )
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
        if (self.gateway_hmac_previous_key_id is None) != (
            self.gateway_hmac_previous_secret is None
        ):
            raise ValueError("previous gateway HMAC identity must be complete")
        if self.gateway_hmac_previous_key_id == self.gateway_hmac_current_key_id:
            raise ValueError("gateway HMAC key IDs must be distinct")
        current_value = _strong_hmac_secret(self.gateway_hmac_current_secret)
        previous_value = _strong_hmac_secret(self.gateway_hmac_previous_secret)
        if (
            current_value is not None
            and previous_value is not None
            and hmac.compare_digest(current_value.encode(), previous_value.encode())
        ):
            raise ValueError("gateway HMAC rotation secrets must be distinct")
        return self

    @classmethod
    def for_test(cls) -> Self:
        return cls.model_validate(
            {
                "NEXUS_ENV": "test",
                "FOSUN_GATEWAY_CAPABILITY": "READ_ONLY",
                "FOSUN_GATEWAY_TRANSPORT_MODE": "fixture",
                "FOSUN_GATEWAY_HMAC_CURRENT_KEY_ID": "fixture-gateway-current",
                "FOSUN_GATEWAY_HMAC_CURRENT_SECRET": (
                    "fixture-gateway-current-secret-32-bytes"
                ),
                "FOSUN_GATEWAY_HMAC_PREVIOUS_KEY_ID": "fixture-gateway-previous",
                "FOSUN_GATEWAY_HMAC_PREVIOUS_SECRET": (
                    "fixture-gateway-previous-secret-32-bytes"
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
