from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from nexus.providers.broker.settings import BrokerWorkerSettings
from nexus.providers.broker.simulation_contracts import SimulationCreateOrderRequest
from nexus.providers.broker.simulation_gateway import (
    FixtureSimulationGatewayClient,
    HttpSimulationGatewayClient,
    SimulationHttpAdmission,
)


def _request() -> SimulationCreateOrderRequest:
    return SimulationCreateOrderRequest(
        command_id=UUID(int=1),
        account_binding_id=UUID(int=2),
        intent_order_id=UUID(int=3),
        idempotency_key="create-fixture-1",
        instrument_code="AAPL.US",
        side="buy",
        quantity="10",
        limit_price="200.00",
    )


@pytest.mark.asyncio
async def test_fixture_client_is_no_socket_and_scripted() -> None:
    client = FixtureSimulationGatewayClient(outcomes=("acknowledged", "rejected"))

    first = await client.create_order(_request())
    second = await client.create_order(
        _request().model_copy(update={"command_id": UUID(int=4)})
    )

    assert first.outcome == "acknowledged"
    assert second.outcome == "rejected"
    assert client.call_count == 2
    assert first.provider_order_digest is not None


def test_http_client_fails_before_constructing_transport_when_not_activated(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    constructed = False

    def forbidden_client(*args, **kwargs):
        nonlocal constructed
        constructed = True
        raise AssertionError("HTTP client must not be constructed")

    monkeypatch.setattr(httpx, "AsyncClient", forbidden_client)
    settings = BrokerWorkerSettings.for_test(
        database_url="postgresql+asyncpg://fixture:fixture@127.0.0.1/fixture",
        client_cert=tmp_path / "client.crt",
        client_key=tmp_path / "client.key",
        client_ca=tmp_path / "ca.crt",
    )
    admission = SimulationHttpAdmission(
        capability_status="SIMULATION_CONTRACT_ADMITTED",
        subject="fixture-contract-v1",
        valid_until=datetime.now(UTC) + timedelta(minutes=5),
        artifact_hash="a" * 64,
    )

    with pytest.raises(ValueError, match="SIMULATION_WRITE_NOT_ACTIVATED"):
        HttpSimulationGatewayClient(settings, admission=admission)

    assert constructed is False


def test_gateway_request_contract_rejects_generic_transport_inputs() -> None:
    with pytest.raises(ValueError):
        SimulationCreateOrderRequest.model_validate(
            {**_request().model_dump(), "url": "https://forbidden.invalid"}
        )
