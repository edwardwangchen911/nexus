"""Decimal JSON wire-format primitives."""

import re
from decimal import Decimal
from typing import Annotated

from pydantic import StringConstraints

DecimalString = Annotated[
    str,
    StringConstraints(max_length=40, pattern=r"^-?(0|[1-9]\d*)(\.\d+)?$"),
]
_DECIMAL_STRING_PATTERN = re.compile(r"^-?(0|[1-9]\d*)(\.\d+)?$")


def decimal_to_string(value: Decimal) -> DecimalString:
    """Render a Decimal without converting it through a binary float."""
    if not isinstance(value, Decimal):
        raise TypeError("value must be a Decimal")
    if not value.is_finite():
        raise ValueError("value must be finite")
    result = format(value, "f")
    if _DECIMAL_STRING_PATTERN.fullmatch(result) is None:
        raise ValueError("value cannot be represented as a DecimalString")
    if len(result) > 40:
        raise ValueError("value cannot exceed 40 characters")
    return result
