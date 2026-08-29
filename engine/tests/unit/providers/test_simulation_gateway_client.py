import base64
import hashlib
import hmac
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
    second = await client.create_order(_request().model_copy(update={"command_id": UUID(int=4)}))

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


@pytest.mark.asyncio
async def test_http_client_uses_principal_bound_hmac_v2(tmp_path: Path) -> None:
    now = datetime(2026, 8, 29, 4, tzinfo=UTC)
    nonce = UUID("0198f7a0-2222-7777-8222-222222222291")
    request_id = UUID("0198f7a0-2222-7777-8222-222222222292")

    async def handler(request: httpx.Request) -> httpx.Response:
        body_hash = hashlib.sha256(request.content).hexdigest()
        canonical = "\n".join(
            (
                "hmac-v2",
                "POST",
                request.url.raw_path.decode(),
                request.headers["X-Nexus-Timestamp"],
                str(nonce),
                "nexus-broker-worker",
                str(request_id),
                body_hash,
            )
        )
        expected = (
            base64.urlsafe_b64encode(
                hmac.new(
                    b"synthetic-gateway-hmac-secret-at-least-32-bytes",
                    canonical.encode(),
                    hashlib.sha256,
                ).digest()
            )
            .rstrip(b"=")
            .decode()
        )
        assert request.headers["X-Nexus-Signature-Version"] == "hmac-v2"
        assert request.headers["X-Nexus-Signature"] == expected
        return httpx.Response(
            200,
            json={
                "command_id": str(_request().command_id),
                "outcome": "acknowledged",
                "safe_code": "FIXTURE_ACKNOWLEDGED",
                "provider_order_ref": "synthetic-order-001",
                "as_of": now.isoformat(),
            },
        )

    settings = BrokerWorkerSettings.for_test(
        database_url="postgresql+asyncpg://fixture:fixture@127.0.0.1/fixture",
        client_cert=tmp_path / "client.crt",
        client_key=tmp_path / "client.key",
        client_ca=tmp_path / "ca.crt",
    )
    admission = SimulationHttpAdmission(
        capability_status="SIMULATION_WRITE_ACTIVATED",
        subject="fixture-contract-v1",
        valid_until=now + timedelta(minutes=5),
        artifact_hash="a" * 64,
    )
    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = HttpSimulationGatewayClient(
        settings,
        admission=admission,
        external_id_digester=lambda value: hashlib.sha256(value.encode()).hexdigest(),
        client=http_client,
        now=lambda: now,
        wall_time=lambda: now.timestamp(),
        uuid_factory=iter((nonce, request_id)).__next__,
    )

    result = await client.create_order(_request())
    await http_client.aclose()

    assert result.outcome == "acknowledged"
