"""Fail-closed configuration for external research-data providers."""

from __future__ import annotations

from enum import StrEnum
from typing import Literal, Self

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ProviderRuntimeMode(StrEnum):
    FIXTURE = "fixture"
    PRODUCTION = "production"


class ProviderSettings(BaseSettings):
    """Provider-only settings whose errors and representation hide all inputs."""

    model_config = SettingsConfigDict(
        env_file=None,
        env_ignore_empty=True,
        extra="ignore",
        hide_input_in_errors=True,
        populate_by_name=True,
    )

    runtime_mode: ProviderRuntimeMode = Field(
        default=ProviderRuntimeMode.FIXTURE,
        validation_alias="NEXUS_PROVIDER_RUNTIME_MODE",
    )
    alpaca_key_id: SecretStr | None = Field(
        default=None,
        validation_alias="ALPACA_API_KEY_ID",
    )
    alpaca_secret_key: SecretStr | None = Field(
        default=None,
        validation_alias="ALPACA_API_SECRET_KEY",
    )
    alpaca_feed: Literal["iex", "sip"] | None = Field(
        default=None,
        validation_alias="ALPACA_DATA_FEED",
    )
    alpaca_delivery_mode: Literal["real_time", "delayed"] | None = Field(
        default=None,
        validation_alias="ALPACA_DELIVERY_MODE",
    )
    sec_contact: str | None = Field(default=None, validation_alias="SEC_CONTACT")
    fred_api_key: SecretStr | None = Field(
        default=None,
        validation_alias="FRED_API_KEY",
    )

    def __repr_args__(self) -> list[tuple[str | None, object]]:
        return [("runtime_mode", self.runtime_mode)]

    @model_validator(mode="after")
    def require_production_authorities(self) -> Self:
        if self.runtime_mode is ProviderRuntimeMode.FIXTURE:
            return self
        required: tuple[tuple[str, object | None], ...] = (
            ("ALPACA_API_KEY_ID_REQUIRED", self.alpaca_key_id),
            ("ALPACA_API_SECRET_KEY_REQUIRED", self.alpaca_secret_key),
            ("ALPACA_DATA_FEED_REQUIRED", self.alpaca_feed),
            ("ALPACA_DELIVERY_MODE_REQUIRED", self.alpaca_delivery_mode),
            ("SEC_CONTACT_REQUIRED", self.sec_contact),
            ("FRED_API_KEY_REQUIRED", self.fred_api_key),
        )
        for error_code, value in required:
            if _missing(value):
                raise ValueError(error_code)
        assert self.sec_contact is not None
        if (
            self.sec_contact != self.sec_contact.strip()
            or "@" not in self.sec_contact
            or len(self.sec_contact.encode("utf-8")) > 254
            or any(ord(character) < 32 for character in self.sec_contact)
        ):
            raise ValueError("SEC_CONTACT_INVALID")
        return self


def _missing(value: object | None) -> bool:
    if value is None:
        return True
    if isinstance(value, SecretStr):
        return not value.get_secret_value().strip()
    if isinstance(value, str):
        return not value.strip()
    return False
