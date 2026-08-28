"""Credentials parsed only by the broker-worker composition root."""

import base64
import binascii
from enum import StrEnum
from pathlib import Path
from typing import Self

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from nexus.settings import Environment


class BrokerSimulationMode(StrEnum):
    OFFLINE_FIXTURE = "offline_fixture"
    EXTERNAL = "external"


class BrokerWorkerSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=None,
        env_ignore_empty=True,
        extra="ignore",
        hide_input_in_errors=True,
        populate_by_name=True,
    )

    environment: Environment = Field(
        default=Environment.DEVELOPMENT, validation_alias="NEXUS_ENV"
    )
    database_url: str = Field(validation_alias="DATABASE_URL")
    simulation_mode: BrokerSimulationMode = Field(
        default=BrokerSimulationMode.OFFLINE_FIXTURE,
        validation_alias="NEXUS_BROKER_SIMULATION_MODE",
    )
    gateway_origin: str | None = Field(
        default=None, validation_alias="FOSUN_GATEWAY_BASE_URL"
    )
    gateway_hmac_key_id: str | None = Field(
        default=None, validation_alias="FOSUN_GATEWAY_HMAC_CURRENT_KEY_ID"
    )
    gateway_hmac_secret: SecretStr | None = Field(
        default=None, validation_alias="FOSUN_GATEWAY_HMAC_CURRENT_SECRET"
    )
    client_cert: Path | None = Field(default=None, validation_alias="FOSUN_GATEWAY_CLIENT_CERT")
    client_key: Path | None = Field(default=None, validation_alias="FOSUN_GATEWAY_CLIENT_KEY")
    client_ca: Path | None = Field(default=None, validation_alias="FOSUN_GATEWAY_CLIENT_CA")
    external_id_current_key: SecretStr | None = Field(
        default=None,
        validation_alias="NEXUS_BROKER_EXTERNAL_ID_AEAD_CURRENT_KEY"
    )
    external_id_current_key_version: str | None = Field(
        default=None,
        validation_alias="NEXUS_BROKER_EXTERNAL_ID_AEAD_CURRENT_KEY_VERSION"
    )
    external_id_previous_key: SecretStr | None = Field(
        default=None,
        validation_alias="NEXUS_BROKER_EXTERNAL_ID_AEAD_PREVIOUS_KEY",
    )
    external_id_previous_key_version: str | None = Field(
        default=None,
        validation_alias="NEXUS_BROKER_EXTERNAL_ID_AEAD_PREVIOUS_KEY_VERSION",
    )
    external_id_blind_index_key: SecretStr | None = Field(
        default=None,
        validation_alias="NEXUS_BROKER_EXTERNAL_ID_BLIND_INDEX_KEY"
    )

    def __repr_args__(self) -> "list[tuple[str | None, object]]":
        return []

    @model_validator(mode="after")
    def broker_secrets_are_strong_and_distinct(self) -> Self:
        if self.simulation_mode is BrokerSimulationMode.OFFLINE_FIXTURE:
            return self
        required = {
            "gateway_origin": self.gateway_origin,
            "gateway_hmac_key_id": self.gateway_hmac_key_id,
            "gateway_hmac_secret": self.gateway_hmac_secret,
            "client_cert": self.client_cert,
            "client_key": self.client_key,
            "client_ca": self.client_ca,
            "external_id_current_key": self.external_id_current_key,
            "external_id_current_key_version": self.external_id_current_key_version,
            "external_id_blind_index_key": self.external_id_blind_index_key,
        }
        missing = tuple(name for name, value in required.items() if value is None)
        if missing:
            raise ValueError(
                "external broker settings require: " + ", ".join(sorted(missing))
            )
        assert self.gateway_hmac_secret is not None
        assert self.external_id_current_key is not None
        assert self.external_id_blind_index_key is not None
        assert self.gateway_hmac_key_id is not None
        assert self.external_id_current_key_version is not None
        hmac_secret = self.gateway_hmac_secret.get_secret_value()
        if hmac_secret != hmac_secret.strip() or len(hmac_secret.encode()) < 32:
            raise ValueError("gateway HMAC secret must be at least 32 unpadded bytes")
        current_key = _aead_key_bytes(self.external_id_current_key)
        blind_index_key = _aead_key_bytes(self.external_id_blind_index_key)
        if current_key == blind_index_key:
            raise ValueError("external ID AEAD and blind-index keys must differ")
        if (self.external_id_previous_key is None) != (
            self.external_id_previous_key_version is None
        ):
            raise ValueError(
                "NEXUS_BROKER_EXTERNAL_ID_AEAD_PREVIOUS_KEY and "
                "NEXUS_BROKER_EXTERNAL_ID_AEAD_PREVIOUS_KEY_VERSION must be configured together"
            )
        if self.external_id_previous_key is not None:
            previous_key = _aead_key_bytes(self.external_id_previous_key)
            if previous_key == blind_index_key:
                raise ValueError("external ID AEAD and blind-index keys must differ")
            if previous_key == current_key:
                raise ValueError("external ID current and previous key material must differ")
        if not self.gateway_hmac_key_id.strip() or not self.external_id_current_key_version.strip():
            raise ValueError("broker key identities must be non-empty")
        if self.external_id_previous_key_version is not None and (
            not self.external_id_previous_key_version.strip()
            or self.external_id_previous_key_version == self.external_id_current_key_version
        ):
            raise ValueError("external ID current and previous key versions must differ")
        return self

    @classmethod
    def for_test(
        cls,
        *,
        database_url: str,
        client_cert: Path,
        client_key: Path,
        client_ca: Path,
    ) -> Self:
        return cls.model_validate(
            {
                "NEXUS_ENV": "test",
                "DATABASE_URL": database_url,
                "NEXUS_BROKER_SIMULATION_MODE": "external",
                "FOSUN_GATEWAY_BASE_URL": "https://fosun-gateway.internal:8443",
                "FOSUN_GATEWAY_HMAC_CURRENT_KEY_ID": "synthetic-gateway-current",
                "FOSUN_GATEWAY_HMAC_CURRENT_SECRET": (
                    "synthetic-gateway-hmac-secret-at-least-32-bytes"
                ),
                "FOSUN_GATEWAY_CLIENT_CERT": client_cert,
                "FOSUN_GATEWAY_CLIENT_KEY": client_key,
                "FOSUN_GATEWAY_CLIENT_CA": client_ca,
                "NEXUS_BROKER_EXTERNAL_ID_AEAD_CURRENT_KEY": base64.urlsafe_b64encode(
                    b"synthetic-broker-aead-key-32byte"
                ).decode(),
                "NEXUS_BROKER_EXTERNAL_ID_AEAD_CURRENT_KEY_VERSION": "synthetic-v1",
                "NEXUS_BROKER_EXTERNAL_ID_BLIND_INDEX_KEY": base64.urlsafe_b64encode(
                    b"synthetic-blind-index-key-32byte"
                ).decode(),
            }
        )


def _aead_key_bytes(secret: SecretStr) -> bytes:
    try:
        aead_key = base64.b64decode(
            secret.get_secret_value(),
            altchars=b"-_",
            validate=True,
        )
    except (ValueError, binascii.Error) as error:
        raise ValueError("external ID AEAD key must be valid base64") from error
    if len(aead_key) != 32:
        raise ValueError("external ID AEAD key must be exactly 32 bytes")
    return aead_key
