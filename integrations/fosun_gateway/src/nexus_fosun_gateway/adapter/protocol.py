"""Provider transport boundary."""

from collections.abc import Mapping
from datetime import datetime
from typing import Protocol

from nexus_fosun_gateway.domain import GatewayItem, GatewayPage
from nexus_fosun_gateway.policy.operations import GatewayOperation


class FosunTransport(Protocol):
    def is_ready(self) -> bool: ...

    def readiness_expires_at(self) -> datetime | None: ...

    async def aclose(self) -> None: ...

    async def execute(
        self,
        operation: GatewayOperation,
        payload: Mapping[str, object],
    ) -> GatewayPage[GatewayItem]: ...
