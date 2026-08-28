from datetime import UTC, datetime
from pathlib import Path

import pytest
from nexus.broker_simulation.jobs import (
    BrokerSimulationDispatchPayload,
    BrokerSimulationReconcilePayload,
    build_broker_job_registry,
)
from nexus.broker_worker import build_broker_runtime
from nexus.jobs.models import BROKER_JOB_TYPES, JobType, QueueName
from nexus.providers.broker.settings import BrokerSimulationMode, BrokerWorkerSettings
from nexus.providers.broker.simulation_gateway import FixtureSimulationGatewayClient
from pydantic import ValidationError


def test_broker_registry_is_closed_to_read_only_and_simulation_jobs() -> None:
    gateway = FixtureSimulationGatewayClient(outcomes=())
    registry = build_broker_job_registry(
        gateway=gateway,
        factory=None,
        clock=lambda: datetime(2026, 8, 28, 14, tzinfo=UTC),
    )

    assert registry.job_types == frozenset(job_type.value for job_type in BROKER_JOB_TYPES)
    assert registry.job_types == {
        JobType.BROKER_SYNC.value,
        JobType.BROKER_RECONCILE.value,
        JobType.BROKER_SIMULATION_DISPATCH.value,
        JobType.BROKER_SIMULATION_RECONCILE.value,
    }
    assert all(spec.queue is QueueName.BROKER for spec in registry.specs)


@pytest.mark.parametrize(
    ("model", "payload"),
    (
        (
            BrokerSimulationDispatchPayload,
            {"intent_id": "00000000-0000-0000-0000-000000000001", "url": "x"},
        ),
        (
            BrokerSimulationReconcilePayload,
            {
                "run_id": "00000000-0000-0000-0000-000000000001",
                "orders_hash": "a" * 64,
                "positions_hash": "b" * 64,
                "cash_hash": "c" * 64,
                "cash_flows_hash": "d" * 64,
                "account_id": "forbidden",
            },
        ),
    ),
)
def test_simulation_job_payloads_are_strict_and_location_free(model, payload) -> None:
    with pytest.raises(ValidationError):
        model.model_validate(payload)


@pytest.mark.asyncio
async def test_offline_runtime_needs_no_broker_credentials_or_http_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden_http_client(*args, **kwargs):
        del args, kwargs
        raise AssertionError("offline fixture constructed an HTTP Gateway client")

    monkeypatch.setattr(
        "nexus.providers.broker.simulation_gateway.HttpSimulationGatewayClient",
        forbidden_http_client,
    )
    settings = BrokerWorkerSettings.model_validate(
        {
            "NEXUS_ENV": "test",
            "DATABASE_URL": "postgresql+asyncpg://synthetic:synthetic@127.0.0.1/postgres",
            "NEXUS_BROKER_SIMULATION_MODE": "offline_fixture",
        }
    )
    gateway = FixtureSimulationGatewayClient(outcomes=())

    runtime = build_broker_runtime(settings, simulation_gateway=gateway)

    assert settings.simulation_mode is BrokerSimulationMode.OFFLINE_FIXTURE
    assert runtime.simulation_gateway is gateway
    assert runtime.gateway_client is None
    assert runtime.external_ids is None
    await runtime.aclose()


def test_external_mode_requires_the_existing_isolated_gateway_identity() -> None:
    with pytest.raises(ValidationError, match="external broker settings require"):
        BrokerWorkerSettings.model_validate(
            {
                "NEXUS_ENV": "test",
                "DATABASE_URL": "postgresql+asyncpg://synthetic:synthetic@127.0.0.1/postgres",
                "NEXUS_BROKER_SIMULATION_MODE": "external",
            }
        )

    settings = BrokerWorkerSettings.for_test(
        database_url="postgresql+asyncpg://synthetic:synthetic@127.0.0.1/postgres",
        client_cert=Path("/synthetic/client.crt"),
        client_key=Path("/synthetic/client.key"),
        client_ca=Path("/synthetic/ca.crt"),
    )
    assert settings.simulation_mode is BrokerSimulationMode.EXTERNAL
