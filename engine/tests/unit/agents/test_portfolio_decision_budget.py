"""Shared daily budget admission for instrument and portfolio Agent runs."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.agents.budget import BudgetExhausted
from nexus.agents.persistence import reserve_daily_budget, reserve_portfolio_decision_budget
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    PortfolioDecisionBudgetReservationRecord,
)

_NOW = datetime(2026, 9, 10, 8, tzinfo=UTC)
_RUN_ID = UUID("018f0000-0000-7000-8000-000000000001")


class _BudgetSession:
    def __init__(self, usage: tuple[Decimal, ...]) -> None:
        self._usage = usage
        self.queries: list[str] = []
        self.added: list[object] = []

    async def execute(self, statement: object, parameters: object = None) -> None:
        return None

    async def scalar(self, statement: object, parameters: object = None) -> Decimal | bool | None:
        query = str(statement)
        self.queries.append(query)
        if "pg_try_advisory_xact_lock" in query:
            return True
        if "to_regclass" in query:
            return None  # Legacy, non-staged research in this fixture.
        if "formal_budget_bindings" in query:
            return None  # This legacy budget case has no protected formal binding.
        if "portfolio_decision_budget_reservations" in query:
            return self._usage[1]
        if "agent_portfolio_synthesis_budget_reservations" in query:
            return self._usage[2]
        raise AssertionError(f"Unexpected scalar query: {query}")

    async def scalars(self, statement: object) -> tuple[object, ...]:
        self.queries.append(str(statement))
        if "agent_daily_budget_reservations.business_date =" in str(statement):
            # Research now reads per-reservation facts so verified overrun can
            # contribute its lower bound; this case has ordinary settled usage.
            return (
                AgentDailyBudgetReservationRecord(
                    run_id=UUID(int=99),
                    business_date=_NOW.date(),
                    pricing_version="pricing-v1",
                    reserved_usd=self._usage[0],
                    settled_usd=self._usage[0],
                    state="settled",
                ),
            )
        return ()  # This legacy case has no economic pair holds.

    def add(self, value: object) -> None:
        self.added.append(value)

    async def flush(self) -> None:
        return None


@pytest.mark.asyncio
async def test_instrument_budget_counts_open_portfolio_decision_reservations() -> None:
    session = _BudgetSession((Decimal("1.00"), Decimal("0.75"), Decimal("0")))

    with pytest.raises(BudgetExhausted, match="daily research budget exhausted"):
        await reserve_daily_budget(
            session,  # type: ignore[arg-type]
            run_id=_RUN_ID,
            now=_NOW,
            daily_limit_usd=Decimal("3.00"),
            worst_case_usd=Decimal("1.26"),
            pricing_version="pricing-v1",
            pricing_fresh=True,
        )

    assert any("agent_daily_budget_reservations" in query for query in session.queries)
    assert any("portfolio_decision_budget_reservations" in query for query in session.queries)
    assert any(
        "agent_portfolio_synthesis_budget_reservations" in query for query in session.queries
    )
    assert session.added == []


@pytest.mark.asyncio
async def test_portfolio_decision_budget_uses_the_shared_daily_total() -> None:
    session = _BudgetSession((Decimal("1.00"), Decimal("0.75"), Decimal("0")))

    reservation = await reserve_portfolio_decision_budget(
        session,  # type: ignore[arg-type]
        run_id=_RUN_ID,
        now=_NOW,
        daily_limit_usd=Decimal("3.00"),
        worst_case_usd=Decimal("1.25"),
        pricing_version="pricing-v1",
        pricing_fresh=True,
    )

    assert isinstance(reservation, PortfolioDecisionBudgetReservationRecord)
    assert not isinstance(reservation, AgentDailyBudgetReservationRecord)
    assert reservation.business_date == _NOW.date()
    assert reservation.reserved_usd == Decimal("1.25")
    assert reservation.state == "reserved"
    assert session.added == [reservation]
