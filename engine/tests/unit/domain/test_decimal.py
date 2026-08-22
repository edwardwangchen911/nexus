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


def test_decimal_to_string_uses_fixed_point_for_exponent_input() -> None:
    """Exponent-form ledger input must not escape as a non-canonical JSON number."""
    assert decimal_to_string(Decimal("1E+2")) == "100"


@pytest.mark.parametrize("value", [Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")])
def test_decimal_to_string_rejects_non_finite_values(value: Decimal) -> None:
    """Non-finite Decimal values are not valid financial wire values."""
    with pytest.raises(ValueError, match="finite"):
        decimal_to_string(value)


def test_decimal_to_string_rejects_float_input() -> None:
    """Accepting binary floats would silently lose ledger precision before serialization."""
    with pytest.raises(TypeError, match="Decimal"):
        decimal_to_string(1.25)  # type: ignore[arg-type]


def test_decimal_string_rejects_scientific_notation() -> None:
    """Scientific notation could change a financial value's canonical wire representation."""
    with pytest.raises(ValidationError):
        _WireAmount(amount="1e2")
