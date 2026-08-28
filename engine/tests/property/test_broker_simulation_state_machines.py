import pytest
from hypothesis import given
from hypothesis import strategies as st
from nexus.broker_simulation.domain import (
    BrokerSimulationTransitionError,
    CommandStatus,
    IntentStatus,
    OrderStatus,
    require_command_transition,
    require_intent_transition,
    require_order_transition,
)


@given(st.sampled_from(list(CommandStatus)))
def test_no_command_status_can_transition_back_to_prepared(source: CommandStatus) -> None:
    if source is CommandStatus.PREPARED:
        return
    with pytest.raises(BrokerSimulationTransitionError):
        require_command_transition(source, CommandStatus.PREPARED)


@given(st.sampled_from(list(IntentStatus)))
def test_terminal_intents_have_no_outgoing_edges(target: IntentStatus) -> None:
    for source in (
        IntentStatus.COMPLETED,
        IntentStatus.PARTIALLY_COMPLETED,
        IntentStatus.FAILED,
        IntentStatus.RECONCILED,
        IntentStatus.REJECTED,
        IntentStatus.EXPIRED,
    ):
        with pytest.raises(BrokerSimulationTransitionError):
            require_intent_transition(source, target)


@given(st.sampled_from(list(OrderStatus)))
def test_terminal_orders_have_no_outgoing_edges(target: OrderStatus) -> None:
    for source in (
        OrderStatus.FILLED,
        OrderStatus.CANCELLED,
        OrderStatus.REJECTED,
        OrderStatus.EXPIRED,
    ):
        with pytest.raises(BrokerSimulationTransitionError):
            require_order_transition(source, target)
