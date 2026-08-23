"""Composition root for the only Engine process allowed to load broker secrets."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import cast
from uuid import UUID

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from nexus.providers.broker.crypto import (
    ExternalIdCipher,
    ExternalIdCipherRing,
    ExternalIdEnvelope,
)
from nexus.providers.broker.fosun import FosunBrokerProvider, GatewayReader
from nexus.providers.broker.gateway_client import FosunGatewayClient
from nexus.providers.broker.settings import BrokerWorkerSettings


@dataclass(frozen=True, repr=False)
class BrokerRuntime:
    settings: BrokerWorkerSettings
    engine: AsyncEngine
    sessions: async_sessionmaker[AsyncSession]
    gateway_client: GatewayReader
    external_ids: ExternalIdCipherRing

    def fosun_provider(
        self,
        *,
        account_id: UUID,
        external_id: ExternalIdEnvelope,
        instrument_ids: dict[str, UUID],
        cash_flow_kinds: dict[str, str],
    ) -> FosunBrokerProvider:
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
        close = getattr(self.gateway_client, "aclose", None)
        if close is not None:
            await close()
        await self.engine.dispose()


def build_broker_runtime(
    settings: BrokerWorkerSettings,
    *,
    gateway_client: GatewayReader | None = None,
) -> BrokerRuntime:
    """Build local clients and keyring without issuing a network or database request."""
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
            else cast(GatewayReader, FosunGatewayClient(settings))
        ),
        external_ids=ExternalIdCipherRing(current=current, previous=previous),
    )


async def run_broker_worker(settings: BrokerWorkerSettings | None = None) -> None:
    """Validate and compose broker dependencies without automatically reading Provider data."""
    runtime = build_broker_runtime(settings or BrokerWorkerSettings.model_validate({}))
    try:
        await asyncio.sleep(0)
    finally:
        await runtime.aclose()


def main() -> None:
    asyncio.run(run_broker_worker())
