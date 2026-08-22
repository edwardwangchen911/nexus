"""Decimal JSON wire-format primitives."""

from decimal import Decimal
from typing import Annotated

from pydantic import StringConstraints

DecimalString = Annotated[str, StringConstraints(pattern=r"^-?(0|[1-9]\d*)(\.\d+)?$")]


def decimal_to_string(value: Decimal) -> DecimalString:
    """Render a Decimal without converting it through a binary float."""
    return str(value)
