from pathlib import Path

import pytest
from nexus.broker_worker import build_broker_runtime
from nexus.providers.broker.settings import BrokerWorkerSettings


class NoRequestGateway:
    def __init__(self) -> None:
        self.calls = 0


@pytest.mark.asyncio
async def test_broker_runtime_startup_does_not_call_provider(tmp_path: Path) -> None:
    gateway = NoRequestGateway()
    settings = BrokerWorkerSettings.for_test(
        database_url="postgresql+asyncpg://synthetic:synthetic@127.0.0.1/postgres",
        client_cert=tmp_path / "client.crt",
        client_key=tmp_path / "client.key",
        client_ca=tmp_path / "ca.crt",
    )
    runtime = build_broker_runtime(settings, gateway_client=gateway)
    assert gateway.calls == 0
    assert runtime.settings is settings
    await runtime.aclose()
