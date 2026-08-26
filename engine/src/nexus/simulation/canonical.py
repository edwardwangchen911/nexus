"""Canonical fixed-scale values used by deterministic simulations."""

from decimal import Decimal, InvalidOperation, localcontext

RESULT_QUANTUM = Decimal("0.000000000001")
_MAX_ABSOLUTE = Decimal("1e26")


def as_decimal(value: Decimal | int, *, field: str) -> Decimal:
    """Accept exact numeric inputs and reject binary floating point."""
    if isinstance(value, float):
        raise TypeError(f"{field} must not contain binary floats")
    if isinstance(value, bool) or not isinstance(value, Decimal | int):
        raise TypeError(f"{field} must be a Decimal")
    return Decimal(value)


def canonical_decimal(value: Decimal) -> Decimal:
    """Quantize to PostgreSQL NUMERIC(38,12) without silent overflow."""
    if not value.is_finite() or abs(value) >= _MAX_ABSOLUTE:
        raise ValueError("simulation decimal exceeds NUMERIC(38,12) authority")
    try:
        with localcontext() as context:
            context.prec = 50
            return value.quantize(RESULT_QUANTUM)
    except InvalidOperation as error:
        raise ValueError("simulation decimal cannot be canonicalized") from error


def canonical_input(value: Decimal | int, *, field: str) -> Decimal:
    return canonical_decimal(as_decimal(value, field=field))


def validated_decimal(value: Decimal | int, *, field: str) -> Decimal:
    """Validate exact calculation state without rounding intermediate values."""
    decimal = as_decimal(value, field=field)
    if not decimal.is_finite() or abs(decimal) >= _MAX_ABSOLUTE:
        raise ValueError(f"{field} exceeds NUMERIC(38,12) authority")
    return decimal
