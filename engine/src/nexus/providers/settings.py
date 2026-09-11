"""Fail-closed configuration for external research-data providers."""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Literal, Self

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ProviderRuntimeMode(StrEnum):
    FIXTURE = "fixture"
    PRODUCTION = "production"


class IntradayMonitorMode(StrEnum):
    DISABLED = "disabled"
    PRODUCTION = "production"


class MarketProvider(StrEnum):
    FOSUN = "fosun"
    ALPACA = "alpaca"


class AlpacaMarketMode(StrEnum):
    DISABLED = "disabled"
    ENABLED = "enabled"


class ProviderWorkerSettings(BaseSettings):
    """Database and object-store settings isolated from Agent/HMAC secrets."""

    model_config = SettingsConfigDict(
        env_file=None,
        env_ignore_empty=True,
        extra="ignore",
        hide_input_in_errors=True,
        populate_by_name=False,
    )

    database_url: str = Field(validation_alias="PROVIDER_DATABASE_URL")
    object_store_endpoint: str = Field(
        validation_alias="PROVIDER_OBJECT_STORE_ENDPOINT"
    )
    object_store_region: str = Field(
        validation_alias="PROVIDER_OBJECT_STORE_REGION"
    )
    object_store_bucket: str = Field(
        validation_alias="PROVIDER_OBJECT_STORE_BUCKET"
    )
    object_store_access_key_id: SecretStr = Field(
        validation_alias="PROVIDER_OBJECT_STORE_ACCESS_KEY_ID"
    )
    object_store_secret_access_key: SecretStr = Field(
        validation_alias="PROVIDER_OBJECT_STORE_SECRET_ACCESS_KEY"
    )
    object_store_prefix: Literal["provider/market"] = Field(
        default="provider/market",
        validation_alias="PROVIDER_OBJECT_STORE_PREFIX",
    )

    def __repr_args__(self) -> list[tuple[str | None, object]]:
        return []

    @model_validator(mode="after")
    def require_nonempty_values(self) -> Self:
        values: tuple[object, ...] = (
            self.database_url,
            self.object_store_endpoint,
            self.object_store_region,
            self.object_store_bucket,
            self.object_store_access_key_id,
            self.object_store_secret_access_key,
            self.object_store_prefix,
        )
        if any(_missing(value) for value in values):
            raise ValueError("PROVIDER_WORKER_STORAGE_SETTINGS_REQUIRED")
        return self


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
    market_provider: MarketProvider = Field(
        default=MarketProvider.FOSUN,
        validation_alias="NEXUS_MARKET_PROVIDER",
    )
    alpaca_market_mode: AlpacaMarketMode = Field(
        default=AlpacaMarketMode.DISABLED,
        validation_alias="NEXUS_ALPACA_MARKET_MODE",
    )
    fosun_market_delivery_mode: Literal["unknown", "real_time", "delayed"] = Field(
        default="unknown",
        validation_alias="FOSUN_MARKET_DELIVERY_MODE",
    )
    fosun_market_gateway_origin: str | None = Field(
        default=None,
        validation_alias="FOSUN_MARKET_GATEWAY_BASE_URL",
    )
    fosun_market_gateway_key_id: str | None = Field(
        default=None,
        validation_alias="FOSUN_MARKET_GATEWAY_HMAC_CURRENT_KEY_ID",
    )
    fosun_market_gateway_secret: SecretStr | None = Field(
        default=None,
        validation_alias="FOSUN_MARKET_GATEWAY_HMAC_CURRENT_SECRET",
    )
    fosun_market_gateway_client_cert: Path | None = Field(
        default=None,
        validation_alias="FOSUN_MARKET_GATEWAY_CLIENT_CERT",
    )
    fosun_market_gateway_client_key: Path | None = Field(
        default=None,
        validation_alias="FOSUN_MARKET_GATEWAY_CLIENT_KEY",
    )
    fosun_market_gateway_client_ca: Path | None = Field(
        default=None,
        validation_alias="FOSUN_MARKET_GATEWAY_CLIENT_CA",
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
    intraday_monitor_mode: IntradayMonitorMode = Field(
        default=IntradayMonitorMode.DISABLED,
        validation_alias="NEXUS_INTRADAY_MONITOR_MODE",
    )
    intraday_poll_seconds: int = Field(
        default=15,
        ge=15,
        le=15,
        validation_alias="NEXUS_INTRADAY_POLL_SECONDS",
    )
    intraday_max_symbols: int = Field(
        default=10,
        ge=1,
        le=30,
        validation_alias="NEXUS_INTRADAY_MAX_SYMBOLS",
    )
    alpaca_intraday_feed: Literal["iex", "sip", "delayed_sip"] | None = Field(
        default=None,
        validation_alias="ALPACA_INTRADAY_FEED",
    )
    alpaca_intraday_delivery_mode: Literal["real_time", "delayed"] | None = Field(
        default=None,
        validation_alias="ALPACA_INTRADAY_DELIVERY_MODE",
    )
    sec_contact: str | None = Field(default=None, validation_alias="SEC_CONTACT")
    fred_api_key: SecretStr | None = Field(
        default=None,
        validation_alias="FRED_API_KEY",
    )

    def __repr_args__(self) -> list[tuple[str | None, object]]:
        return [
            ("runtime_mode", self.runtime_mode),
            ("market_provider", self.market_provider),
            ("alpaca_market_mode", self.alpaca_market_mode),
            ("intraday_monitor_mode", self.intraday_monitor_mode),
        ]

    @model_validator(mode="after")
    def require_production_authorities(self) -> Self:
        if self.intraday_monitor_mode is IntradayMonitorMode.PRODUCTION:
            if self.runtime_mode is not ProviderRuntimeMode.PRODUCTION:
                raise ValueError("INTRADAY_PRODUCTION_PROVIDER_MODE_REQUIRED")
            if self.market_provider is MarketProvider.FOSUN:
                if self.intraday_max_symbols > 10:
                    raise ValueError("FOSUN_INTRADAY_MAX_SYMBOLS_EXCEEDED")
            else:
                if self.alpaca_intraday_feed is None:
                    raise ValueError("ALPACA_INTRADAY_FEED_REQUIRED")
                if self.alpaca_intraday_delivery_mode is None:
                    raise ValueError("ALPACA_INTRADAY_DELIVERY_MODE_REQUIRED")
                expected_delivery = (
                    "delayed"
                    if self.alpaca_intraday_feed == "delayed_sip"
                    else "real_time"
                )
                if self.alpaca_intraday_delivery_mode != expected_delivery:
                    raise ValueError("ALPACA_INTRADAY_AUTHORITY_MISMATCH")
        if self.runtime_mode is ProviderRuntimeMode.FIXTURE:
            return self
        common: tuple[tuple[str, object | None], ...] = (
            ("SEC_CONTACT_REQUIRED", self.sec_contact),
            ("FRED_API_KEY_REQUIRED", self.fred_api_key),
        )
        for error_code, value in common:
            if _missing(value):
                raise ValueError(error_code)
        if self.market_provider is MarketProvider.FOSUN:
            if self.alpaca_market_mode is not AlpacaMarketMode.DISABLED:
                raise ValueError("ALPACA_MODE_REQUIRES_ALPACA_MARKET_PROVIDER")
            fosun_required: tuple[tuple[str, object | None], ...] = (
                ("FOSUN_MARKET_GATEWAY_BASE_URL_REQUIRED", self.fosun_market_gateway_origin),
                (
                    "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_KEY_ID_REQUIRED",
                    self.fosun_market_gateway_key_id,
                ),
                (
                    "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_SECRET_REQUIRED",
                    self.fosun_market_gateway_secret,
                ),
                (
                    "FOSUN_MARKET_GATEWAY_CLIENT_CERT_REQUIRED",
                    self.fosun_market_gateway_client_cert,
                ),
                (
                    "FOSUN_MARKET_GATEWAY_CLIENT_KEY_REQUIRED",
                    self.fosun_market_gateway_client_key,
                ),
                (
                    "FOSUN_MARKET_GATEWAY_CLIENT_CA_REQUIRED",
                    self.fosun_market_gateway_client_ca,
                ),
            )
            for error_code, value in fosun_required:
                if _missing(value):
                    raise ValueError(error_code)
        else:
            if self.alpaca_market_mode is not AlpacaMarketMode.ENABLED:
                raise ValueError("ALPACA_MARKET_MODE_DISABLED")
            alpaca_required: tuple[tuple[str, object | None], ...] = (
                ("ALPACA_API_KEY_ID_REQUIRED", self.alpaca_key_id),
                ("ALPACA_API_SECRET_KEY_REQUIRED", self.alpaca_secret_key),
                ("ALPACA_DATA_FEED_REQUIRED", self.alpaca_feed),
                ("ALPACA_DELIVERY_MODE_REQUIRED", self.alpaca_delivery_mode),
            )
            for error_code, value in alpaca_required:
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
