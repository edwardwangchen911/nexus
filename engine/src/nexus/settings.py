"""Runtime settings with safe development fixtures and redacted representations."""

from enum import StrEnum
from typing import Self

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Environment(StrEnum):
    DEVELOPMENT = "development"
    TEST = "test"
    PRODUCTION = "production"


class _SafeSettings(BaseSettings):
    """Prevent logging a settings object from exposing operational configuration."""

    model_config = SettingsConfigDict(
        env_file=None,
        env_ignore_empty=True,
        extra="ignore",
        hide_input_in_errors=True,
        populate_by_name=True,
    )

    def __repr_args__(self) -> "list[tuple[str | None, object]]":
        return []


class EngineSettings(_SafeSettings):
    environment: Environment = Field(
        default=Environment.DEVELOPMENT,
        validation_alias="NEXUS_ENV",
    )
    database_url: str | None = Field(default=None, validation_alias="DATABASE_URL")
    object_store_endpoint: str | None = Field(
        default=None,
        validation_alias="OBJECT_STORE_ENDPOINT",
    )
    object_store_region: str | None = Field(default=None, validation_alias="OBJECT_STORE_REGION")
    object_store_bucket: str | None = Field(default=None, validation_alias="OBJECT_STORE_BUCKET")
    object_store_access_key_id: SecretStr | None = Field(
        default=None,
        validation_alias="OBJECT_STORE_ACCESS_KEY_ID",
    )
    object_store_secret_access_key: SecretStr | None = Field(
        default=None,
        validation_alias="OBJECT_STORE_SECRET_ACCESS_KEY",
    )
    engine_hmac_secret: SecretStr | None = Field(
        default=None,
        validation_alias="NEXUS_ENGINE_HMAC_SECRET",
    )
    fosun_gateway_base_url: str | None = Field(
        default=None,
        validation_alias="FOSUN_GATEWAY_BASE_URL",
    )
    fosun_gateway_hmac_secret: SecretStr | None = Field(
        default=None,
        validation_alias="FOSUN_GATEWAY_HMAC_SECRET",
    )

    @model_validator(mode="after")
    def apply_environment_policy(self) -> Self:
        if self.environment is Environment.PRODUCTION:
            missing = [name for name, value in self._required_values().items() if value is None]
            if missing:
                raise ValueError(f"production settings require: {', '.join(missing)}")
            return self

        fixtures: dict[str, str | SecretStr] = {
            "database_url": "postgresql+asyncpg://nexus:nexus@postgres:5432/nexus",
            "object_store_endpoint": "http://minio:9000",
            "object_store_region": "us-east-1",
            "object_store_bucket": "nexus-fixtures",
            "object_store_access_key_id": SecretStr("fixture-object-store-access-key"),
            "object_store_secret_access_key": SecretStr("fixture-object-store-secret"),
            "engine_hmac_secret": SecretStr("fixture-engine-hmac-secret"),
            "fosun_gateway_base_url": "http://fosun-gateway:8081",
            "fosun_gateway_hmac_secret": SecretStr("fixture-gateway-hmac-secret"),
        }
        for name, value in fixtures.items():
            if getattr(self, name) is None:
                setattr(self, name, value)
        return self

    def _required_values(self) -> dict[str, object | None]:
        return {
            "DATABASE_URL": self.database_url,
            "OBJECT_STORE_ENDPOINT": self.object_store_endpoint,
            "OBJECT_STORE_REGION": self.object_store_region,
            "OBJECT_STORE_BUCKET": self.object_store_bucket,
            "OBJECT_STORE_ACCESS_KEY_ID": self.object_store_access_key_id,
            "OBJECT_STORE_SECRET_ACCESS_KEY": self.object_store_secret_access_key,
            "NEXUS_ENGINE_HMAC_SECRET": self.engine_hmac_secret,
            "FOSUN_GATEWAY_BASE_URL": self.fosun_gateway_base_url,
            "FOSUN_GATEWAY_HMAC_SECRET": self.fosun_gateway_hmac_secret,
        }

    @classmethod
    def for_test(cls) -> Self:
        return cls(environment=Environment.TEST)
