"""Gateway settings isolated from engine and provider implementations."""

from enum import StrEnum
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
    gateway_hmac_secret: SecretStr | None = Field(
        default=None,
        validation_alias="FOSUN_GATEWAY_HMAC_SECRET",
    )

    def __repr_args__(self) -> "list[tuple[str | None, object]]":
        return [("environment", self.environment), ("capability", self.capability)]

    @model_validator(mode="after")
    def apply_environment_policy(self) -> Self:
        if self.environment is Environment.PRODUCTION:
            if self.capability is None or self.gateway_hmac_secret is None:
                raise ValueError(
                    "production settings require FOSUN_GATEWAY_CAPABILITY and "
                    "FOSUN_GATEWAY_HMAC_SECRET"
                )
            return self
        if self.capability is None:
            self.capability = "READ_ONLY"
        if self.gateway_hmac_secret is None:
            self.gateway_hmac_secret = SecretStr("fixture-gateway-hmac-secret")
        return self

    @classmethod
    def for_test(cls) -> Self:
        return cls(environment=Environment.TEST)
