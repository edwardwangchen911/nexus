"""Exact Decimal transaction-cost policy shared by simulations."""

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, field_validator


class CostModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    commission_bps: Decimal
    spread_bps: Decimal
    slippage_bps: Decimal

    @field_validator("commission_bps", "spread_bps", "slippage_bps", mode="before")
    @classmethod
    def _reject_float(cls, value: object) -> object:
        if isinstance(value, float):
            raise TypeError("costs must not contain binary floats")
        return value

    @field_validator("commission_bps", "spread_bps", "slippage_bps")
    @classmethod
    def _bounded(cls, value: Decimal) -> Decimal:
        if not Decimal(0) <= value <= Decimal("500"):
            raise ValueError("base cost basis points must be between 0 and 500")
        return value

    @classmethod
    def zero(cls) -> "CostModel":
        return cls(
            commission_bps=Decimal(0),
            spread_bps=Decimal(0),
            slippage_bps=Decimal(0),
        )

    def estimate(self, notional: Decimal) -> Decimal:
        return (
            abs(notional)
            * (self.commission_bps + self.spread_bps + self.slippage_bps)
            / Decimal(10_000)
        )

    def scaled(self, multiplier: Decimal) -> "CostModel":
        """Return an exact sensitivity scenario without mutating policy."""
        if multiplier < 0:
            raise ValueError("cost multiplier must be nonnegative")
        values = (
            self.commission_bps * multiplier,
            self.spread_bps * multiplier,
            self.slippage_bps * multiplier,
        )
        if any(value > Decimal("1000") for value in values):
            raise ValueError("cost sensitivity exceeds the bounded scenario range")
        return CostModel.model_construct(
            commission_bps=values[0],
            spread_bps=values[1],
            slippage_bps=values[2],
        )
