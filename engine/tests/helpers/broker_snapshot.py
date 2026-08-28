from decimal import Decimal
from uuid import UUID

from helpers.broker_seed import NOW, PORTFOLIO_ID
from nexus.providers.broker.base import (
    BrokerOrderFact,
    BrokerPosition,
    BrokerSyncResult,
    CashBalance,
    PortfolioSnapshot,
    SourceObservation,
)


def _snapshot(instrument_id: UUID, snapshot_id: UUID, status: str) -> PortfolioSnapshot:
    return PortfolioSnapshot(
        id=snapshot_id,
        portfolio_id=PORTFOLIO_ID,
        status=status,
        cash=(
            CashBalance(
                currency="USD",
                cash_balance=Decimal("1000"),
                available_cash=Decimal("900"),
                provider_as_of=NOW,
            ),
        ),
        positions=(
            BrokerPosition(
                instrument_id=instrument_id,
                currency="USD",
                quantity=Decimal("10"),
                available_quantity=Decimal("9"),
                frozen_quantity=Decimal("1"),
                avg_cost=Decimal("100"),
                diluted_cost=Decimal("99"),
                current_price=Decimal("120"),
                provider_as_of=NOW,
            ),
        ),
        open_orders=(
            BrokerOrderFact(
                provider_order_id="synthetic-order-1",
                instrument_id=instrument_id,
                side="buy",
                quantity=Decimal("2"),
                filled_quantity=Decimal("1"),
                price=Decimal("110"),
                status="partially_filled",
                updated_at=NOW,
            ),
        ),
        as_of=NOW,
        source_request_ids=(
            "synthetic-snapshot-request",
            "synthetic-snapshot-request",
            "synthetic-snapshot-request",
        ),
        source_observations=tuple(
            SourceObservation(
                source_request_id="synthetic-snapshot-request",
                response_as_of=NOW,
                quality="PASS",
            )
            for _ in range(3)
        ),
    )


def _sync_result(
    run_id: UUID,
    snapshot: PortfolioSnapshot,
    *,
    count: int = 1,
) -> BrokerSyncResult:
    traces: list[dict[str, object]] = []
    for attempt in range(1, count + 1):
        if snapshot.status != "READY" or attempt != count:
            traces.append(
                {
                    "attempt": attempt,
                    "phase": "cash_a",
                    "completed": False,
                    "reason": "synthetic_provider_failure",
                    "source_request_ids": (),
                    "source_observations": (),
                    "raw_facts": {},
                }
            )
            continue
        phases = (
            "cash_a",
            "positions_a",
            "orders_a",
            "positions_b",
            "orders_b",
            "cash_b",
        )
        facts = {
            "cash_a": tuple(item.model_dump(mode="json") for item in snapshot.cash),
            "positions_a": tuple(item.model_dump(mode="json") for item in snapshot.positions),
            "orders_a": tuple(item.model_dump(mode="json") for item in snapshot.open_orders),
            "positions_b": tuple(item.model_dump(mode="json") for item in snapshot.positions),
            "orders_b": tuple(item.model_dump(mode="json") for item in snapshot.open_orders),
            "cash_b": tuple(item.model_dump(mode="json") for item in snapshot.cash),
            "_derived_nav": {
                "read_a": (_nav_evidence(),),
                "read_b": (_nav_evidence(),),
            },
        }
        observations = tuple(
            {
                "phase": phase,
                "source_request_id": "synthetic-snapshot-request",
                "response_as_of": NOW.isoformat(),
                "quality": "PASS",
            }
            for phase in phases
        )
        traces.append(
            {
                "attempt": attempt,
                "phase": "cash_b",
                "completed": True,
                "reason": "reconciled",
                "source_request_ids": tuple("synthetic-snapshot-request" for _ in phases),
                "source_observations": observations,
                "raw_facts": facts,
            }
        )
    return BrokerSyncResult(
        sync_run_id=run_id,
        attempt_count=count,
        status=snapshot.status,
        portfolio_snapshot=snapshot,
        attempt_traces=tuple(traces),
    )


def _nav_evidence() -> dict[str, object]:
    return {
        "currency": "USD",
        "value": "2200",
        "provider_as_of": NOW.isoformat(),
        "source_request_ids": ("synthetic-snapshot-request",),
    }
