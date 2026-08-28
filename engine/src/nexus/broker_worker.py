"""Composition root for the only Engine process allowed to load broker secrets."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from nexus.broker_simulation.jobs import build_broker_job_registry
from nexus.jobs.models import QueueName
from nexus.jobs.worker import UnsafeJobError, run_job_worker_once
from nexus.persistence.migrations import (
    CURRENT_BROKER_WORKER_PROCESS_VERSION,
    CURRENT_MIGRATION_HEAD,
)
from nexus.persistence.models.broker_simulation import BrokerSimulationAdmissionRecord
from nexus.providers.broker.crypto import (
    ExternalIdCipher,
    ExternalIdCipherRing,
    ExternalIdEnvelope,
)
from nexus.providers.broker.fosun import FosunBrokerProvider, GatewayReader
from nexus.providers.broker.gateway_client import FosunGatewayClient
from nexus.providers.broker.settings import BrokerSimulationMode, BrokerWorkerSettings
from nexus.providers.broker.simulation_gateway import SimulationGateway


@dataclass(frozen=True, repr=False)
class BrokerRuntime:
    settings: BrokerWorkerSettings
    engine: AsyncEngine
    sessions: async_sessionmaker[AsyncSession]
    gateway_client: GatewayReader | None
    external_ids: ExternalIdCipherRing | None
    simulation_gateway: SimulationGateway | None

    def fosun_provider(
        self,
        *,
        account_id: UUID,
        external_id: ExternalIdEnvelope,
        instrument_ids: dict[str, UUID],
        cash_flow_kinds: dict[str, str],
    ) -> FosunBrokerProvider:
        if self.external_ids is None or self.gateway_client is None:
            raise RuntimeError("READ_ONLY_EXTERNAL_ACTIVATION_BLOCKED")
        plaintext_id = self.external_ids.decrypt(
            external_id, provider="fosun", account_id=account_id
        )
        return FosunBrokerProvider(
            account_id=account_id,
            external_account_id=plaintext_id,
            client=self.gateway_client,
            instrument_ids=instrument_ids,
            cash_flow_kinds=cash_flow_kinds,
        )

    async def aclose(self) -> None:
        closed: set[int] = set()
        for client in (self.gateway_client, self.simulation_gateway):
            if client is None or id(client) in closed:
                continue
            closed.add(id(client))
            close = getattr(client, "aclose", None)
            if close is not None:
                await close()
        await self.engine.dispose()


def build_broker_runtime(
    settings: BrokerWorkerSettings,
    *,
    gateway_client: GatewayReader | None = None,
    simulation_gateway: SimulationGateway | None = None,
) -> BrokerRuntime:
    """Build local clients and keyring without issuing a network or database request."""
    if settings.simulation_mode is BrokerSimulationMode.OFFLINE_FIXTURE:
        if simulation_gateway is None:
            raise ValueError("OFFLINE_FIXTURE_GATEWAY_REQUIRED")
        engine = create_async_engine(settings.database_url, pool_pre_ping=True)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        return BrokerRuntime(
            settings=settings,
            engine=engine,
            sessions=sessions,
            gateway_client=None,
            external_ids=None,
            simulation_gateway=simulation_gateway,
        )
    if (
        settings.external_id_current_key is None
        or settings.external_id_current_key_version is None
        or settings.external_id_blind_index_key is None
    ):
        raise ValueError("EXTERNAL_BROKER_IDENTITY_REQUIRED")
    engine = create_async_engine(settings.database_url, pool_pre_ping=True)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    current = ExternalIdCipher.from_base64_key(
        settings.external_id_current_key.get_secret_value(),
        key_version=settings.external_id_current_key_version,
        blind_index_key=settings.external_id_blind_index_key.get_secret_value(),
    )
    previous = None
    if (
        settings.external_id_previous_key is not None
        and settings.external_id_previous_key_version is not None
    ):
        previous = ExternalIdCipher.from_base64_key(
            settings.external_id_previous_key.get_secret_value(),
            key_version=settings.external_id_previous_key_version,
            blind_index_key=settings.external_id_blind_index_key.get_secret_value(),
        )
    return BrokerRuntime(
        settings=settings,
        engine=engine,
        sessions=sessions,
        gateway_client=(
            gateway_client
            if gateway_client is not None
            else FosunGatewayClient(settings)
        ),
        external_ids=ExternalIdCipherRing(current=current, previous=previous),
        simulation_gateway=simulation_gateway,
    )


async def run_broker_worker_once(
    runtime: BrokerRuntime,
    *,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> bool:
    """Claim at most one broker job with the mode-specific capability guard."""
    if runtime.simulation_gateway is None:
        raise ValueError("SIMULATION_GATEWAY_NOT_COMPOSED")
    guard = (
        _require_current_external_capability
        if runtime.settings.simulation_mode is BrokerSimulationMode.EXTERNAL
        else _offline_capability
    )
    now = clock()
    async with runtime.sessions() as session:
        await guard(session, now)
    registry = build_broker_job_registry(
        gateway=runtime.simulation_gateway,
        factory=runtime.sessions,
        clock=clock,
        capability_guard=guard,
    )
    return await run_job_worker_once(
        runtime.sessions,
        registry=registry,
        queue=QueueName.BROKER,
        worker_id="nexus-broker-worker",
        worker_started_at=now,
        process_version=CURRENT_BROKER_WORKER_PROCESS_VERSION,
        migration_head=CURRENT_MIGRATION_HEAD,
        clock=clock,
        claim_registered_only=True,
    )


async def run_broker_worker(
    settings: BrokerWorkerSettings | None = None,
    *,
    simulation_gateway: SimulationGateway | None = None,
    run_forever: bool = False,
) -> None:
    """Poll only the broker queue; ordinary local profiles do not start this process."""
    configured = settings or BrokerWorkerSettings.model_validate({})
    runtime = build_broker_runtime(
        configured,
        simulation_gateway=simulation_gateway,
    )
    try:
        if not run_forever:
            await asyncio.sleep(0)
            return
        while True:
            worked = await run_broker_worker_once(runtime)
            if not worked:
                await asyncio.sleep(0.5)
    finally:
        await runtime.aclose()


async def _offline_capability(_session: AsyncSession, _now: datetime) -> None:
    return None


async def _require_current_external_capability(
    session: AsyncSession, now: datetime
) -> None:
    admission = await session.scalar(
        select(BrokerSimulationAdmissionRecord).where(
            BrokerSimulationAdmissionRecord.capability_status
            == "SIMULATION_WRITE_ACTIVATED",
            BrokerSimulationAdmissionRecord.revoked.is_(False),
            BrokerSimulationAdmissionRecord.valid_from <= now,
            BrokerSimulationAdmissionRecord.valid_until > now,
        )
    )
    if admission is None:
        raise UnsafeJobError("SIMULATION_WRITE_NOT_ACTIVATED")


def main() -> None:
    asyncio.run(run_broker_worker())
