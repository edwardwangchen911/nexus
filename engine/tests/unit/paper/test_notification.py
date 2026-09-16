from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.paper.notification import (
    PaperNotificationError,
    PaperOrderNotificationSnapshot,
    notification_snapshot_payload,
    parse_notification_snapshot,
)
from pydantic import ValidationError

ORDER_ID = UUID("0199d500-1000-7000-8000-000000000101")
CYCLE_ID = UUID("0199d500-1000-7000-8000-000000000102")

FILLED_PAYLOAD: dict[str, object] = {
    "schema_version": "paper_notification_v1",
    "mode": "internal_paper",
    "order_id": str(ORDER_ID),
    "agent_cycle_id": str(CYCLE_ID),
    "occurred_at": "2026-09-16T02:15:00Z",
    "symbol": "AAPL",
    "side": "buy",
    "status": "filled",
    "requested_quantity": "12",
    "approved_quantity": "12",
    "filled_quantity": "12",
    "remaining_quantity": "0",
    "price": {"kind": "average_fill", "value": "187.36", "currency": "USD"},
    "notional": "2248.32",
    "fee": "0",
    "cash": {"kind": "actual_after", "available": "21406.18"},
    "position": {
        "kind": "actual_after",
        "quantity": "42",
        "portfolio_ratio": None,
    },
    "agent_reason": "盈利预期上修，价格突破近期区间。",
    "invalidation": "价格跌破既定阈值。",
    "risk_reason": None,
}

REJECTED_PAYLOAD: dict[str, object] = {
    **FILLED_PAYLOAD,
    "status": "risk_rejected",
    "requested_quantity": "20",
    "approved_quantity": "0",
    "filled_quantity": "0",
    "remaining_quantity": "0",
    "price": {"kind": "reference", "value": "187", "currency": "USD"},
    "notional": "3740",
    "fee": None,
    "cash": {"kind": "unchanged", "available": "23654.50"},
    "position": {
        "kind": "unchanged",
        "quantity": "30",
        "portfolio_ratio": "0.18",
    },
    "risk_reason": "成交后单一标的仓位将超过 20% 上限。",
}


def test_filled_snapshot_parses_exact_decimals_and_requires_actual_values() -> None:
    snapshot = PaperOrderNotificationSnapshot.model_validate(FILLED_PAYLOAD)

    assert snapshot.status == "filled"
    assert snapshot.price.kind == "average_fill"
    assert snapshot.cash.kind == "actual_after"
    assert snapshot.price.value == Decimal("187.36")
    assert snapshot.filled_quantity + snapshot.remaining_quantity == snapshot.approved_quantity
    assert snapshot.occurred_at == datetime(2026, 9, 16, 2, 15, tzinfo=UTC)


def test_rejected_snapshot_keeps_account_values_unchanged() -> None:
    snapshot = PaperOrderNotificationSnapshot.model_validate(REJECTED_PAYLOAD)

    assert snapshot.status == "risk_rejected"
    assert snapshot.approved_quantity == 0
    assert snapshot.cash.kind == "unchanged"
    assert snapshot.position.kind == "unchanged"
    assert snapshot.risk_reason is not None


@pytest.mark.parametrize(
    ("path", "value"),
    (
        (("price", "kind"), "reference"),
        (("cash", "kind"), "projected_after"),
        (("position", "kind"), "projected_after"),
        (("remaining_quantity",), "1"),
        (("occurred_at",), "2026-09-16T10:15:00+08:00"),
        (("requested_quantity",), 12.0),
    ),
)
def test_filled_snapshot_rejects_invalid_bindings_or_inexact_input(
    path: tuple[str, ...],
    value: object,
) -> None:
    payload = deepcopy(FILLED_PAYLOAD)
    target = payload
    for part in path[:-1]:
        nested = target[part]
        assert isinstance(nested, dict)
        target = nested
    target[path[-1]] = value

    with pytest.raises(ValidationError):
        PaperOrderNotificationSnapshot.model_validate(payload)


def test_rejected_snapshot_requires_reason_and_no_fill_or_fee() -> None:
    for field, value in (
        ("risk_reason", None),
        ("filled_quantity", "1"),
        ("fee", "0"),
    ):
        payload = deepcopy(REJECTED_PAYLOAD)
        payload[field] = value
        with pytest.raises(ValidationError):
            PaperOrderNotificationSnapshot.model_validate(payload)


@pytest.mark.parametrize("status", ("filled", "risk_rejected"))
def test_filled_or_rejected_snapshot_requires_positive_notional(status: str) -> None:
    payload = deepcopy(FILLED_PAYLOAD if status == "filled" else REJECTED_PAYLOAD)
    payload["notional"] = "0"

    with pytest.raises(ValidationError):
        PaperOrderNotificationSnapshot.model_validate(payload)


def test_snapshot_is_closed_and_currency_is_required() -> None:
    extra = deepcopy(FILLED_PAYLOAD)
    extra["account_id"] = "account-secret"
    with pytest.raises(ValidationError):
        PaperOrderNotificationSnapshot.model_validate(extra)

    missing_currency = deepcopy(FILLED_PAYLOAD)
    price = missing_currency["price"]
    assert isinstance(price, dict)
    del price["currency"]
    with pytest.raises(ValidationError):
        PaperOrderNotificationSnapshot.model_validate(missing_currency)


def test_snapshot_payload_round_trips_with_decimal_strings() -> None:
    snapshot = PaperOrderNotificationSnapshot.model_validate(FILLED_PAYLOAD)

    payload = notification_snapshot_payload(snapshot)
    raw = payload["notification_snapshot"]
    assert isinstance(raw, dict)
    assert raw["notional"] == "2248.32"
    assert parse_notification_snapshot(payload) == snapshot


def test_snapshot_payload_uses_plain_canonical_decimal_strings() -> None:
    payload = deepcopy(FILLED_PAYLOAD)
    payload["notional"] = Decimal("1E+3")
    snapshot = PaperOrderNotificationSnapshot.model_validate(payload)

    serialized = notification_snapshot_payload(snapshot)["notification_snapshot"]
    assert isinstance(serialized, dict)
    assert serialized["notional"] == "1000"


@pytest.mark.parametrize("filled_quantity", ("0", "5"))
def test_cancelled_snapshot_binds_price_and_fee_to_fill_facts(filled_quantity: str) -> None:
    payload = deepcopy(FILLED_PAYLOAD)
    payload.update(
        status="cancelled",
        filled_quantity=filled_quantity,
        remaining_quantity=str(12 - int(filled_quantity)),
        price={
            "kind": "average_fill" if filled_quantity != "0" else "reference",
            "value": "187.36",
            "currency": "USD",
        },
        fee="0" if filled_quantity != "0" else None,
        notional="100" if filled_quantity != "0" else "0",
        risk_reason="模拟执行窗口已结束。",
    )

    snapshot = PaperOrderNotificationSnapshot.model_validate(payload)

    assert snapshot.status == "cancelled"


@pytest.mark.parametrize(
    ("filled_quantity", "price_kind", "fee"),
    (
        ("5", "reference", "0"),
        ("5", "average_fill", None),
        ("0", "average_fill", None),
        ("0", "reference", "0"),
    ),
)
def test_cancelled_snapshot_rejects_price_or_fee_mismatch(
    filled_quantity: str,
    price_kind: str,
    fee: str | None,
) -> None:
    payload = deepcopy(FILLED_PAYLOAD)
    payload.update(
        status="cancelled",
        filled_quantity=filled_quantity,
        remaining_quantity=str(12 - int(filled_quantity)),
        price={"kind": price_kind, "value": "187.36", "currency": "USD"},
        fee=fee,
        notional="100" if filled_quantity != "0" else "0",
    )

    with pytest.raises(ValidationError):
        PaperOrderNotificationSnapshot.model_validate(payload)


@pytest.mark.parametrize(("filled_quantity", "notional"), (("5", "0"), ("0", "1")))
def test_cancelled_snapshot_binds_notional_to_fill_facts(
    filled_quantity: str,
    notional: str,
) -> None:
    payload = deepcopy(FILLED_PAYLOAD)
    payload.update(
        status="cancelled",
        filled_quantity=filled_quantity,
        remaining_quantity=str(12 - int(filled_quantity)),
        price={
            "kind": "average_fill" if filled_quantity != "0" else "reference",
            "value": "187.36",
            "currency": "USD",
        },
        fee="0" if filled_quantity != "0" else None,
        notional=notional,
    )

    with pytest.raises(ValidationError):
        PaperOrderNotificationSnapshot.model_validate(payload)


@pytest.mark.parametrize(
    "payload",
    (
        {},
        {"notification_snapshot": "not-an-object"},
        {"notification_snapshot": {**FILLED_PAYLOAD, "status": "submitted"}},
    ),
)
def test_snapshot_parser_returns_value_free_error(payload: dict[str, object]) -> None:
    with pytest.raises(PaperNotificationError, match="^PAPER_NOTIFICATION_INVALID$") as caught:
        parse_notification_snapshot(payload)

    assert "AAPL" not in str(caught.value)
