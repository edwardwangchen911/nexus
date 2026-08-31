"""Secret-bearing settings constructed only after immutable manifest admission."""

from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


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
    fsopenapi_base_url: str = Field(validation_alias="FSOPENAPI_BASE_URL")
    fsopenapi_api_key: SecretStr = Field(validation_alias="FSOPENAPI_API_KEY")
    fsopenapi_client_private_key: SecretStr = Field(
        validation_alias="FSOPENAPI_CLIENT_PRIVATE_KEY"
    )
    fsopenapi_server_public_key: SecretStr = Field(
        validation_alias="FSOPENAPI_SERVER_PUBLIC_KEY"
    )

    @field_validator("provider_manifest", "output_dir")
    @classmethod
    def probe_paths_are_absolute(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("probe paths must be absolute")
        return value

    @field_validator("fsopenapi_base_url")
    @classmethod
    def provider_base_url_is_exact_https(cls, value: str) -> str:
        if value != value.strip() or value != value.rstrip("/"):
            raise ValueError("provider base URL must be exact")
        parsed = urlsplit(value)
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
