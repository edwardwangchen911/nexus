from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.broker_simulation.domain import (
    ApprovalDocument,
    BrokerIntentDocument,
    BrokerOrderDocument,
    BrokerSimulationTransitionError,
    CommandStatus,
    IntentStatus,
    OrderStatus,
    canonical_document_hash,
    effective_approval_expiry,
    require_command_transition,
    require_intent_transition,
    require_order_transition,
)
from pydantic import ValidationError


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _order(**overrides: object) -> BrokerOrderDocument:
    values: dict[str, object] = {
        "instrument_id": UUID(int=11),
        "side": "buy",
        "quantity": 2,
        "limit_price": Decimal("12.34"),
        "currency": "USD",
        "order_type": "limit",
        "time_in_force": "DAY",
        "session": "regular",
        "sort_key": 1,
    }
    values.update(overrides)
    return BrokerOrderDocument(**values)


def _intent(**overrides: object) -> BrokerIntentDocument:
    order = _order()
    values: dict[str, object] = {
        "proposal_group_id": UUID(int=1),
        "strategy_decision_id": UUID(int=2),
        "portfolio_snapshot_id": UUID(int=3),
        "evidence_hashes": ("a" * 64, "b" * 64),
        "risk_policy_hash": "c" * 64,
        "execution_model_hash": "d" * 64,
        "account_binding_id": UUID(int=4),
        "orders": (order,),
        "total_notional": Decimal("24.68"),
    }
    values.update(overrides)
    return BrokerIntentDocument(**values)


def test_domain_documents_are_closed_frozen_and_exact() -> None:
    order = _order()
    with pytest.raises(ValidationError, match="frozen"):
        order.quantity = 3  # type: ignore[misc]
    with pytest.raises(ValidationError, match="Extra inputs"):
        _order(provider_order_id="forbidden")
    with pytest.raises(ValidationError, match="binary float"):
        _order(limit_price=12.34)
    with pytest.raises(ValidationError, match="whole shares"):
        _order(quantity=Decimal("1.5"))
    with pytest.raises(ValidationError, match="tick"):
        _order(limit_price=Decimal("12.345"))


def test_intent_requires_sorted_unique_orders_and_exact_notional() -> None:
    second = _order(instrument_id=UUID(int=12), sort_key=2)
    with pytest.raises(ValidationError, match="canonical sort order"):
        _intent(orders=(second, _order()))
    with pytest.raises(ValidationError, match="unique instruments"):
        _intent(orders=(_order(), _order(sort_key=2)), total_notional=Decimal("49.36"))
    with pytest.raises(ValidationError, match="total notional"):
        _intent(total_notional=Decimal("24.69"))


def test_canonical_hash_is_stable_and_rejects_binary_floats() -> None:
    intent = _intent()
    assert canonical_document_hash(intent) == canonical_document_hash(intent.model_dump())
    assert canonical_document_hash({"price": Decimal("12.3400")}) == canonical_document_hash(
        {"price": Decimal("12.34")}
    )
    assert len(canonical_document_hash(intent)) == 64
    with pytest.raises(TypeError, match="binary floats"):
        canonical_document_hash({"price": 12.34})


def test_approval_is_actor_and_body_hash_bound() -> None:
    approval = ApprovalDocument(
        intent_id=UUID(int=9),
        intent_version=1,
        intent_body_hash="a" * 64,
        request_body_hash="b" * 64,
        actor_id="private-user",
        idempotency_key="approve-001",
        approved_at=_utc("2026-08-28T14:00:00Z"),
        expires_at=_utc("2026-08-28T14:02:00Z"),
    )
    assert approval.approved_at.tzinfo is UTC
    with pytest.raises(ValidationError, match="after approval"):
        ApprovalDocument.model_validate(
            {
                **approval.model_dump(),
                "expires_at": approval.approved_at - timedelta(seconds=1),
            }
        )


def test_unknown_result_has_no_edge_back_to_sending() -> None:
    with pytest.raises(BrokerSimulationTransitionError, match="COMMAND_TRANSITION_INVALID"):
        require_command_transition(CommandStatus.RESULT_UNKNOWN, CommandStatus.SENDING)


@pytest.mark.parametrize(
    ("source", "target", "guard"),
    [
        (IntentStatus.PREPARED, IntentStatus.APPROVED, require_intent_transition),
        (CommandStatus.SENDING, CommandStatus.ACKNOWLEDGED, require_command_transition),
        (OrderStatus.WORKING, OrderStatus.PARTIALLY_FILLED, require_order_transition),
    ],
)
def test_explicit_forward_transitions_are_accepted(
    source: object, target: object, guard: object
) -> None:
    guard(source, target)  # type: ignore[operator]


def test_approval_expiry_uses_earliest_authority() -> None:
    assert effective_approval_expiry(
        prepared_at=_utc("2026-08-28T14:00:00Z"),
        quote_expires_at=_utc("2026-08-28T14:02:00Z"),
        capability_expires_at=_utc("2026-08-28T14:04:00Z"),
        session_close_at=_utc("2026-08-28T20:00:00Z"),
    ) == _utc("2026-08-28T14:02:00Z")


def test_approval_expiry_rejects_naive_time() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        effective_approval_expiry(
            prepared_at=datetime(2026, 8, 28, 14),
            quote_expires_at=_utc("2026-08-28T14:02:00Z"),
            capability_expires_at=_utc("2026-08-28T14:04:00Z"),
            session_close_at=_utc("2026-08-28T20:00:00Z"),
        )
