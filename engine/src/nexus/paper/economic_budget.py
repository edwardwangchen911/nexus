"""All-or-none budget arithmetic; planning claims are never a reservation authority."""

from datetime import date
from decimal import Decimal
from typing import Literal

from pydantic import Field, model_validator

from nexus.paper.portfolio_economic_preflight import StrictModel


class ReservationClaim(StrictModel):
    key: str = Field(min_length=1, max_length=100)
    state: Literal["reserved", "usage_unknown", "settled", "released"]
    reserved_usd: Decimal = Field(ge=0, allow_inf_nan=False)
    settled_usd: Decimal | None = Field(default=None, ge=0, allow_inf_nan=False)

    @model_validator(mode="after")
    def consistent_usage(self) -> "ReservationClaim":
        if self.settled_usd is not None and self.settled_usd > self.reserved_usd:
            raise ValueError("ECONOMIC_BUDGET_USAGE_INVALID")
        if self.state == "released" and self.settled_usd not in (None, Decimal(0)):
            raise ValueError("ECONOMIC_BUDGET_RELEASE_INVALID")
        if self.state in ("reserved", "usage_unknown") and self.settled_usd is not None:
            raise ValueError("ECONOMIC_BUDGET_USAGE_INVALID")
        return self

    @property
    def occupied(self) -> Decimal:
        if self.state == "released":
            return Decimal(0)
        if self.state == "settled" and self.settled_usd is not None:
            return self.settled_usd
        # Missing settlement is unknown, never zero or released capacity.
        return self.reserved_usd


class FormalTaskClaim(StrictModel):
    key: str = Field(min_length=1, max_length=100)
    ceiling_usd: Decimal = Field(ge=0, allow_inf_nan=False)
    reservation_key: str | None = Field(default=None, min_length=1, max_length=100)


class BudgetRequest(StrictModel):
    business_date_utc: date
    global_limit_usd: Decimal = Field(ge=0, allow_inf_nan=False)
    research_cost_usd: Decimal = Field(ge=0, allow_inf_nan=False)
    synthesis_cost_usd: Decimal = Field(ge=0, allow_inf_nan=False)
    reservations: tuple[ReservationClaim, ...] = Field(max_length=1000)
    formal_tasks: tuple[FormalTaskClaim, ...] = Field(max_length=1000)
    formal_inventory_complete: bool

    @model_validator(mode="after")
    def unique_coverage(self) -> "BudgetRequest":
        keys = {item.key for item in self.reservations}
        refs = [item.reservation_key for item in self.formal_tasks if item.reservation_key]
        if (
            len(keys) != len(self.reservations)
            or len({item.key for item in self.formal_tasks}) != len(self.formal_tasks)
            or len(set(refs)) != len(refs)
            or not set(refs).issubset(keys)
        ):
            raise ValueError("ECONOMIC_BUDGET_COVERAGE_INVALID")
        return self


class BudgetResult(StrictModel):
    business_date_utc: date
    status: Literal["both_fit_arithmetic", "insufficient_for_both", "formal_inventory_unknown"]
    evidence_kind: Literal["planning_claims"] = "planning_claims"
    used_or_committed_usd: Decimal
    uncovered_formal_usd: Decimal | None
    remaining_after_formal_usd: Decimal | None
    per_lane_ceiling_usd: Decimal
    both_lane_ceiling_usd: Decimal
    allowed_model_jobs: Literal[0] = 0
    reserved: Literal[False] = False
    activation_permitted: Literal[False] = False
    execution_locked: Literal[True] = True


def calculate_budget(request: BudgetRequest) -> BudgetResult:
    per_lane = 2 * request.research_cost_usd + request.synthesis_cost_usd
    used = sum((entry.occupied for entry in request.reservations), Decimal(0))
    if not request.formal_inventory_complete:
        return BudgetResult(
            business_date_utc=request.business_date_utc,
            status="formal_inventory_unknown",
            used_or_committed_usd=used,
            uncovered_formal_usd=None,
            remaining_after_formal_usd=None,
            per_lane_ceiling_usd=per_lane,
            both_lane_ceiling_usd=2 * per_lane,
        )
    indexed = {entry.key: entry for entry in request.reservations}
    uncovered = Decimal(0)
    for task in request.formal_tasks:
        reservation = indexed.get(task.reservation_key) if task.reservation_key else None
        # A settled or released entry cannot cover a task still due to execute.
        covered = (
            reservation.occupied
            if reservation is not None and reservation.state in ("reserved", "usage_unknown")
            else Decimal(0)
        )
        uncovered += max(task.ceiling_usd - covered, Decimal(0))
    remaining = request.global_limit_usd - used - uncovered
    return BudgetResult(
        business_date_utc=request.business_date_utc,
        status="both_fit_arithmetic" if remaining >= 2 * per_lane else "insufficient_for_both",
        used_or_committed_usd=used,
        uncovered_formal_usd=uncovered,
        remaining_after_formal_usd=remaining,
        per_lane_ceiling_usd=per_lane,
        both_lane_ceiling_usd=2 * per_lane,
    )
