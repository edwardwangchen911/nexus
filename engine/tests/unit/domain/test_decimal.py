from decimal import Decimal

import pytest
from nexus.domain.decimal import DecimalString, decimal_to_string
from pydantic import BaseModel, ValidationError


class _WireAmount(BaseModel):
    amount: DecimalString


def test_decimal_to_string_preserves_exact_decimal_wire_value() -> None:
    """A ledger Decimal must cross a JSON boundary as a non-floating string."""
    wire_value = decimal_to_string(Decimal("123.4500"))

    assert wire_value == "123.4500"
    assert isinstance(wire_value, str)


def test_decimal_string_rejects_scientific_notation() -> None:
    """Scientific notation could change a financial value's canonical wire representation."""
    with pytest.raises(ValidationError):
        _WireAmount(amount="1e2")
