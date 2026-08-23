from pathlib import Path

import pytest
from nexus.providers.broker.settings import BrokerWorkerSettings
from nexus.settings import EngineSettings
from pydantic import ValidationError


def test_general_engine_settings_do_not_own_broker_secrets() -> None:
    fields = EngineSettings.model_fields

    assert "fosun_gateway_hmac_secret" not in fields
    assert "broker_external_id_aead_key" not in fields
    assert "fosun_gateway_client_key" not in fields


def test_broker_worker_settings_are_redacted_and_role_scoped() -> None:
    settings = BrokerWorkerSettings.for_test(
        database_url="postgresql+asyncpg://synthetic:synthetic@127.0.0.1/postgres",
        client_cert=Path("/synthetic/client.crt"),
        client_key=Path("/synthetic/client.key"),
        client_ca=Path("/synthetic/ca.crt"),
    )

    rendered = repr(settings)
    assert "synthetic-broker-aead" not in rendered
    assert "synthetic-gateway-hmac" not in rendered
    assert rendered == "BrokerWorkerSettings()"


def test_previous_external_id_key_must_be_a_complete_distinct_pair() -> None:
    current = BrokerWorkerSettings.for_test(
        database_url="postgresql+asyncpg://synthetic:synthetic@127.0.0.1/postgres",
        client_cert=Path("/synthetic/client.crt"),
        client_key=Path("/synthetic/client.key"),
        client_ca=Path("/synthetic/ca.crt"),
    )

    with pytest.raises(ValidationError, match="PREVIOUS"):
        BrokerWorkerSettings.model_validate(
            {
                **current.model_dump(),
                "external_id_previous_key_version": "synthetic-v0",
            }
        )

    with pytest.raises(ValidationError, match="differ"):
        BrokerWorkerSettings.model_validate(
            {
                **current.model_dump(),
                "external_id_previous_key": current.external_id_current_key,
                "external_id_previous_key_version": (current.external_id_current_key_version),
            }
        )


def test_blind_index_key_must_be_independent_from_rotating_aead_keys() -> None:
    current = BrokerWorkerSettings.for_test(
        database_url="postgresql+asyncpg://synthetic:synthetic@127.0.0.1/postgres",
        client_cert=Path("/synthetic/client.crt"),
        client_key=Path("/synthetic/client.key"),
        client_ca=Path("/synthetic/ca.crt"),
    )

    with pytest.raises(ValidationError, match="must differ"):
        BrokerWorkerSettings.model_validate(
            {
                **current.model_dump(),
                "external_id_blind_index_key": current.external_id_current_key,
            }
        )


def test_current_previous_and_blind_keys_are_pairwise_distinct_bytes() -> None:
    current = BrokerWorkerSettings.for_test(
        database_url="postgresql+asyncpg://synthetic:synthetic@127.0.0.1/postgres",
        client_cert=Path("/synthetic/client.crt"),
        client_key=Path("/synthetic/client.key"),
        client_ca=Path("/synthetic/ca.crt"),
    )

    with pytest.raises(ValidationError, match="key material must differ"):
        BrokerWorkerSettings.model_validate(
            {
                **current.model_dump(),
                "external_id_previous_key": current.external_id_current_key,
                "external_id_previous_key_version": "synthetic-v0",
            }
        )
