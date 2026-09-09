"""Secret-bearing settings constructed only after immutable manifest admission."""

import base64
import hmac
from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from nexus_fosun_gateway.settings import FOSUN_REST_BASE_URL

FOSUN_LOCAL_PROBE_HTTPS_PROXY_URL = "http://127.0.0.1:9090"


class ProbeProviderSettings(BaseSettings):
    """Minimal one-shot provider settings with hidden validation inputs."""

    model_config = SettingsConfigDict(
        env_file=None,
        env_ignore_empty=True,
        extra="ignore",
        hide_input_in_errors=True,
        populate_by_name=True,
    )

    provider_manifest: Path = Field(validation_alias="FOSUN_PROVIDER_MANIFEST")
    sdk_admitted: Literal[True] = Field(validation_alias="FOSUN_SDK_ADMITTED")
    output_dir: Path = Field(validation_alias="FOSUN_PROBE_OUTPUT_DIR")
    publisher_private_key_file: Path = Field(
        validation_alias="FOSUN_PROBE_PUBLISHER_PRIVATE_KEY_FILE"
    )
    publisher_public_key_b64u: str = Field(
        min_length=43,
        max_length=43,
        pattern=r"^[A-Za-z0-9_-]{43}$",
        validation_alias="FOSUN_ACTIVATION_PROBE_PUBLISHER_PUBLIC_KEY_B64U",
    )
    probe_https_proxy_url: str = Field(
        validation_alias="FOSUN_PROBE_HTTPS_PROXY_URL"
    )
    fsopenapi_base_url: str = Field(validation_alias="FSOPENAPI_BASE_URL")
    fsopenapi_api_key: SecretStr = Field(validation_alias="FSOPENAPI_API_KEY")
    fsopenapi_client_private_key: SecretStr = Field(
        validation_alias="FSOPENAPI_CLIENT_PRIVATE_KEY"
    )
    fsopenapi_server_public_key: SecretStr = Field(
        validation_alias="FSOPENAPI_SERVER_PUBLIC_KEY"
    )

    @field_validator("provider_manifest", "output_dir", "publisher_private_key_file")
    @classmethod
    def probe_paths_are_absolute(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("probe paths must be absolute")
        return value

    @field_validator("sdk_admitted", mode="before")
    @classmethod
    def sdk_admitted_accepts_only_exact_true(cls, value: object) -> object:
        if value is True or value == "true":
            return True
        raise ValueError("probe SDK admission switch must be exact true")

    @field_validator("publisher_public_key_b64u")
    @classmethod
    def publisher_public_key_is_canonical(cls, value: str) -> str:
        try:
            decoded = base64.b64decode(value + "=", altchars=b"-_", validate=True)
        except (ValueError, TypeError):
            raise ValueError("publisher public key must be canonical base64url") from None
        canonical = base64.urlsafe_b64encode(decoded).rstrip(b"=").decode("ascii")
        if len(decoded) != 32 or not hmac.compare_digest(canonical, value):
            raise ValueError("publisher public key must be canonical base64url")
        return value

    @field_validator("fsopenapi_base_url")
    @classmethod
    def provider_base_url_is_exact_https(cls, value: str) -> str:
        if value != FOSUN_REST_BASE_URL:
            raise ValueError("provider base URL must be the reviewed Fosun REST origin")
        return value

    @field_validator("probe_https_proxy_url")
    @classmethod
    def probe_https_proxy_is_exact_loopback(cls, value: str) -> str:
        if value != FOSUN_LOCAL_PROBE_HTTPS_PROXY_URL:
            raise ValueError("probe HTTPS proxy must be the reviewed local endpoint")
        return value

    @field_validator(
        "fsopenapi_api_key",
        "fsopenapi_client_private_key",
        "fsopenapi_server_public_key",
    )
    @classmethod
    def provider_material_is_present(cls, value: SecretStr) -> SecretStr:
        material = value.get_secret_value()
        if not material.strip() or len(material) > 64 * 1024:
            raise ValueError("provider material is invalid")
        return value

    def __repr_args__(self) -> list[tuple[str | None, object]]:
        return [
            ("provider_manifest", self.provider_manifest),
            ("sdk_admitted", self.sdk_admitted),
        ]


class PushProbeProviderSettings(BaseSettings):
    """Only the provider inputs needed by one bounded market push probe."""

    model_config = SettingsConfigDict(
        env_file=None,
        env_ignore_empty=True,
        extra="ignore",
        hide_input_in_errors=True,
        populate_by_name=True,
    )

    provider_manifest: Path = Field(validation_alias="FOSUN_PROVIDER_MANIFEST")
    sdk_admitted: Literal[True] = Field(validation_alias="FOSUN_SDK_ADMITTED")
    probe_https_proxy_url: str = Field(validation_alias="FOSUN_PROBE_HTTPS_PROXY_URL")
    fsopenapi_base_url: str = Field(validation_alias="FSOPENAPI_BASE_URL")
    fsopenapi_api_key: SecretStr = Field(validation_alias="FSOPENAPI_API_KEY")
    fsopenapi_client_private_key: SecretStr = Field(
        validation_alias="FSOPENAPI_CLIENT_PRIVATE_KEY"
    )
    fsopenapi_server_public_key: SecretStr = Field(
        validation_alias="FSOPENAPI_SERVER_PUBLIC_KEY"
    )

    @field_validator("provider_manifest")
    @classmethod
    def manifest_path_is_absolute(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("provider manifest path must be absolute")
        return value

    @field_validator("sdk_admitted", mode="before")
    @classmethod
    def sdk_admitted_accepts_only_exact_true(cls, value: object) -> object:
        if value is True or value == "true":
            return True
        raise ValueError("probe SDK admission switch must be exact true")

    @field_validator("fsopenapi_base_url")
    @classmethod
    def provider_base_url_is_exact_https(cls, value: str) -> str:
        if value != FOSUN_REST_BASE_URL:
            raise ValueError("provider base URL must be the reviewed Fosun REST origin")
        return value

    @field_validator("probe_https_proxy_url")
    @classmethod
    def probe_https_proxy_is_exact_loopback(cls, value: str) -> str:
        if value != FOSUN_LOCAL_PROBE_HTTPS_PROXY_URL:
            raise ValueError("probe HTTPS proxy must be the reviewed local endpoint")
        return value

    @field_validator(
        "fsopenapi_api_key",
        "fsopenapi_client_private_key",
        "fsopenapi_server_public_key",
    )
    @classmethod
    def provider_material_is_present(cls, value: SecretStr) -> SecretStr:
        material = value.get_secret_value()
        if not material.strip() or len(material) > 64 * 1024:
            raise ValueError("provider material is invalid")
        return value

    def __repr_args__(self) -> list[tuple[str | None, object]]:
        return [
            ("provider_manifest", self.provider_manifest),
            ("sdk_admitted", self.sdk_admitted),
        ]
