import pytest
from nexus.providers.broker.fosun_contracts import CashItem, PositionItem
from pydantic import ValidationError


@pytest.mark.parametrize("value", (1000.0, "NaN", "Infinity"))
def test_financial_fact_rejects_binary_float_or_non_finite_decimal(value: object) -> None:
    with pytest.raises((TypeError, ValidationError, ValueError)):
        CashItem.model_validate(
            {
                "kind": "cash",
                "external_account_id": "synthetic-account",
                "currency": "USD",
                "cash_balance": value,
                "available_cash": "900",
                "provider_as_of": None,
            }
        )


def test_position_contract_preserves_distinct_cost_and_price_fields() -> None:
    item = PositionItem.model_validate(
        {
            "kind": "position",
            "external_account_id": "synthetic-account",
            "instrument_code": "usAAPL",
            "currency": "USD",
            "quantity": "10",
            "available_quantity": "9",
            "frozen_quantity": "1",
            "average_cost": "100",
            "diluted_cost": "99",
            "current_price": "120",
            "provider_as_of": None,
        }
    )
    assert (item.average_cost, item.diluted_cost, item.current_price) == (100, 99, 120)
