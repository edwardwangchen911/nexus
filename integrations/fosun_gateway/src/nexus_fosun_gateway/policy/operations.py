"""Compile-time read-only operation and scoped runtime authorization."""

from typing import TYPE_CHECKING, Final, Literal

from nexus_fosun_gateway.activation import RuntimeAuthorization
from nexus_fosun_gateway.domain import GatewayError
from nexus_fosun_gateway.probe_contract import RuntimeOperation

if TYPE_CHECKING:
    from nexus_fosun_gateway.security.hmac import VerifiedGatewayActor

GatewayOperation = Literal[
    "fosun_capabilities",
    "fosun_sync_accounts",
    "fosun_sync_portfolio",
    "fosun_list_orders",
    "fosun_list_cash_flows",
    "fosun_get_quotes",
    "fosun_get_bars",
]
ActorOperation = GatewayOperation | RuntimeOperation

READ_ONLY_OPERATIONS: Final[frozenset[str]] = frozenset(
    {
        "fosun_capabilities",
        "fosun_sync_accounts",
        "fosun_sync_portfolio",
        "fosun_list_orders",
        "fosun_list_cash_flows",
        "fosun_get_quotes",
        "fosun_get_bars",
    }
)

BROKER_OPERATIONS: Final[frozenset[ActorOperation]] = frozenset(
    {
        "fosun_capabilities",
        "fosun_sync_accounts",
        "fosun_sync_portfolio",
        "fosun_sync_portfolio:cash",
        "fosun_sync_portfolio:holdings",
        "fosun_list_orders",
        "fosun_list_cash_flows",
    }
)

MARKET_OPERATIONS: Final[frozenset[ActorOperation]] = frozenset(
    {
        "fosun_capabilities",
        "fosun_get_quotes",
        "fosun_get_bars",
    }
)


def authorize_runtime_operation(
    operation: RuntimeOperation,
    actor: "VerifiedGatewayActor",
    runtime_authorization: RuntimeAuthorization | frozenset[RuntimeOperation] | None,
    *,
    transport_mode: Literal["fixture", "read_only", "local_read_only"],
) -> None:
    """Require both caller identity and immutable scoped evidence before transport."""
    if operation not in actor.allowed_operations:
        raise _operation_not_allowed()
    if transport_mode == "fixture":
        return
    authorized = (
        runtime_authorization
        if isinstance(runtime_authorization, frozenset)
        else (
            None
            if runtime_authorization is None
            else runtime_authorization.authorized_operation_set
        )
    )
    if authorized is None or operation not in authorized:
        raise _operation_not_allowed()


def _operation_not_allowed() -> GatewayError:
    return GatewayError(
        "OPERATION_NOT_ALLOWED",
        "Gateway operation is not available to this caller.",
        status_code=404,
        next_action="USE_AUTHORIZED_OPERATION",
    )
