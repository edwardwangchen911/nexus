"""Fail-closed point-in-time SEC financial-fact normalization."""

from collections.abc import Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from nexus.domain.evidence import EvidenceRef
from nexus.domain.time import require_utc

CANONICAL_SEC_FACTS = frozenset(
    {
        "revenue",
        "gross_profit",
        "operating_income",
        "net_income",
        "operating_cash_flow",
        "capital_expenditure",
        "total_assets",
        "total_liabilities",
        "stockholders_equity",
        "diluted_shares",
        "diluted_eps",
    }
)


class FinancialFactSelectionError(ValueError):
    """A fact set cannot safely support a point-in-time decision."""


class FinancialFact(BaseModel):
    """One raw-preserving SEC fact, appended for every filing revision."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    instrument_id: UUID
    canonical_fact: str
    value: Decimal
    unit: str
    period_kind: Literal["instant", "duration"]
    period_start: date | None
    period_end: date
    fiscal_year: int | None
    fiscal_period: str | None
    form: str
    filed_at: datetime
    accepted_at: datetime
    accession_number: str
    raw_taxonomy: str
    raw_tag: str
    frame: str | None
    revision: str
    evidence_ref: EvidenceRef

    @field_validator("value", mode="before")
    @classmethod
    def _reject_binary_float(cls, value: object) -> object:
        if isinstance(value, float):
            raise TypeError("financial values must not be binary floats")
        return value

    @field_validator("canonical_fact")
    @classmethod
    def _canonical_fact_is_supported(cls, value: str) -> str:
        if value not in CANONICAL_SEC_FACTS:
            raise ValueError("unsupported canonical SEC fact")
        return value

    @field_validator("filed_at", "accepted_at")
    @classmethod
    def _utc_times(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="after")
    def _period_matches_kind(self) -> "FinancialFact":
        if self.period_kind == "instant" and self.period_start is not None:
            raise ValueError("instant facts must not contain period_start")
        if self.period_kind == "duration" and self.period_start is None:
            raise ValueError("duration facts require period_start")
        if self.period_start is not None and self.period_start > self.period_end:
            raise ValueError("period_start must not be after period_end")
        # SEC may assign the next business day's filing date to an evening acceptance.
        # Preserve both source values; filing date is not an event timestamp.
        return self


def select_financial_fact(
    facts: Sequence[FinancialFact], canonical_fact: str, decision_time: datetime
) -> FinancialFact | None:
    """Select the latest accepted compatible fact or explain why selection is unsafe."""
    decision_at = require_utc(decision_time)
    visible = [
        fact
        for fact in facts
        if fact.canonical_fact == canonical_fact
        and max(fact.accepted_at, fact.filed_at, fact.evidence_ref.available_at) <= decision_at
    ]
    if not visible:
        return None
    units = {fact.unit for fact in visible}
    if len(units) != 1:
        raise FinancialFactSelectionError("INCOMPATIBLE_UNIT")
    period_kinds = {fact.period_kind for fact in visible}
    if len(period_kinds) != 1:
        raise FinancialFactSelectionError("INCOMPATIBLE_PERIOD_KIND")
    if visible[0].period_kind == "duration":
        _reject_overlapping_duration_periods(visible)
    latest_key = max((fact.period_end, fact.accepted_at) for fact in visible)
    latest = [
        fact for fact in visible if (fact.period_end, fact.accepted_at) == latest_key
    ]
    if len(latest) != 1:
        raise FinancialFactSelectionError("UNRESOLVED_ACCEPTANCE_TIE")
    return latest[0]


def _reject_overlapping_duration_periods(facts: Sequence[FinancialFact]) -> None:
    periods = {
        (fact.period_start, fact.period_end) for fact in facts if fact.period_start is not None
    }
    ordered = sorted(periods, key=lambda period: (period[0], period[1]))
    for (_, earlier_end), (later_start, _) in zip(ordered, ordered[1:], strict=False):
        if later_start <= earlier_end:
            raise FinancialFactSelectionError("OVERLAPPING_DURATION_PERIODS")
