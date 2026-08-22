"""Provider transport boundary."""

from collections.abc import Mapping
from typing import Protocol

from nexus_fosun_gateway.domain import GatewayItem, GatewayPage
from nexus_fosun_gateway.policy.operations import GatewayOperation


class FosunTransport(Protocol):
    async def execute(
        self,
        operation: GatewayOperation,
        payload: Mapping[str, object],
    ) -> GatewayPage[GatewayItem]: ...
