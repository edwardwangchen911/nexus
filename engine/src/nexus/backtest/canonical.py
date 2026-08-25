"""Canonical fixed-scale values used by immutable backtest evidence."""

from decimal import Decimal, InvalidOperation, localcontext

RESULT_QUANTUM = Decimal("0.000000000001")
_MAX_ABSOLUTE = Decimal("1e26")


def canonical_decimal(value: Decimal) -> Decimal:
    """Quantize to PostgreSQL NUMERIC(38,12) without silent overflow or non-finite values."""
    if not value.is_finite() or abs(value) >= _MAX_ABSOLUTE:
        raise ValueError("backtest result decimal exceeds NUMERIC(38,12) authority")
    try:
        with localcontext() as context:
            context.prec = 50
            return value.quantize(RESULT_QUANTUM)
    except InvalidOperation as error:
        raise ValueError("backtest result decimal cannot be canonicalized") from error
