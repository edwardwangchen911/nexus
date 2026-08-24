"""Decimal-only fail-closed budget reservation for local research runs."""

from decimal import Decimal, InvalidOperation

from pydantic import BaseModel, ConfigDict, PrivateAttr, field_validator


class BudgetExhausted(ValueError):
    """A cost guard that must resolve work to watch/abstention."""


class DailyBudget(BaseModel):
    """One process-local daily budget ledger; persistence records final usage separately."""

    model_config = ConfigDict(extra="forbid")

    limit_usd: Decimal
    _reserved_usd: Decimal = PrivateAttr(default=Decimal("0"))
    _spent_usd: Decimal = PrivateAttr(default=Decimal("0"))

    @field_validator("limit_usd")
    @classmethod
    def positive_finite_limit(cls, value: Decimal) -> Decimal:
        if not value.is_finite() or value <= 0:
            raise ValueError("limit_usd must be a positive finite Decimal")
        return value

    @classmethod
    def usd(cls, value: str) -> "DailyBudget":
        """Parse an explicit decimal string without accepting binary float budget values."""
        if not isinstance(value, str):
            raise TypeError("USD budget values must be decimal strings")
        try:
            parsed = Decimal(value)
        except InvalidOperation as error:
            raise ValueError("USD budget value must be decimal text") from error
        return cls(limit_usd=parsed)

    @property
    def spent_usd(self) -> Decimal:
        return self._spent_usd

    @property
    def remaining_usd(self) -> Decimal:
        return self.limit_usd - self._spent_usd - self._reserved_usd

    def reserve(self, worst_case_usd: Decimal, *, pricing_fresh: bool = True) -> Decimal:
        """Reserve before invocation; stale pricing and oversubscription fail closed."""
        if not pricing_fresh:
            raise BudgetExhausted("stale pricing cannot reserve research budget")
        self._require_cost(worst_case_usd)
        if worst_case_usd > self.remaining_usd:
            raise BudgetExhausted("daily research budget exhausted")
        self._reserved_usd += worst_case_usd
        return worst_case_usd

    def settle(self, reservation_usd: Decimal, actual_usd: Decimal) -> None:
        """Settle reported usage without allowing a post-hoc limit overrun."""
        self._require_cost(reservation_usd)
        self._require_cost(actual_usd)
        if reservation_usd > self._reserved_usd:
            raise BudgetExhausted("reservation is not available")
        if actual_usd > reservation_usd:
            raise BudgetExhausted("reported usage exceeds reservation")
        self._reserved_usd -= reservation_usd
        self._spent_usd += actual_usd

    def release(self, reservation_usd: Decimal) -> None:
        """Return one still-open reservation without changing settled usage."""
        self._require_cost(reservation_usd)
        if reservation_usd > self._reserved_usd:
            raise BudgetExhausted("reservation is not available")
        self._reserved_usd -= reservation_usd

    @staticmethod
    def _require_cost(value: Decimal) -> None:
        if not isinstance(value, Decimal) or not value.is_finite() or value < 0:
            raise ValueError("costs must be finite non-negative Decimals")
