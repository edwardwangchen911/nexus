from collections import deque
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.providers.broker.base import (
    BrokerCashFlow,
    BrokerPage,
    BrokerProviderError,
    DateRange,
)
from nexus.reconciliation.cash_flows import fetch_cash_flows

ACCOUNT_ID = UUID("0198d400-2000-7000-8000-000000000001")
SYNC_RUN_ID = UUID("0198d400-2000-7000-8000-000000000002")


def flow(
    provider_id: str | None,
    *,
    amount: str = "10.00",
    occurred_at: datetime = datetime(2026, 8, 22, 17, tzinfo=UTC),
) -> BrokerCashFlow:
    return BrokerCashFlow(
        provider_cash_flow_id=provider_id,
        currency="USD",
        amount=Decimal(amount),
        occurred_at=occurred_at,
        kind="deposit",
    )


class ScriptedCashFlowProvider:
    def __init__(self, pages: list[BrokerPage[BrokerCashFlow] | Exception]) -> None:
        self.pages = deque(pages)
        self.calls: list[tuple[DateRange, str | None, int]] = []

    async def read_cash_flows(
        self,
        account_id: UUID,
        window: DateRange,
        cursor: str | None,
        *,
        sync_run_id: UUID,
        limit: int,
    ) -> BrokerPage[BrokerCashFlow]:
        assert account_id == ACCOUNT_ID
        assert sync_run_id == SYNC_RUN_ID
        self.calls.append((window, cursor, limit))
        scripted = self.pages.popleft()
        if isinstance(scripted, Exception):
            raise scripted
        return scripted


def page(
    items: tuple[BrokerCashFlow, ...],
    *,
    cursor: str | None = None,
    quality: str = "PASS",
) -> BrokerPage[BrokerCashFlow]:
    return BrokerPage(
        items=items,
        has_more=cursor is not None,
        next_cursor=cursor,
        source_request_id=f"synthetic-cash-flow-{cursor or 'end'}",
        as_of=datetime(2026, 8, 23, 4, tzinfo=UTC),
        quality=quality,
    )


@pytest.mark.asyncio
async def test_cash_flow_cursor_is_per_fetch_and_window_overlaps_three_calendar_days() -> None:
    provider = ScriptedCashFlowProvider(
        [
            page((flow("flow-1"),), cursor="1"),
            page(
                (
                    flow(
                        "flow-2",
                        occurred_at=datetime(2026, 8, 23, 2, tzinfo=UTC),
                    ),
                )
            ),
        ]
    )

    result = await fetch_cash_flows(
        ACCOUNT_ID,
        provider,
        sync_run_id=SYNC_RUN_ID,
        initial_start=date(2026, 1, 1),
        end=date(2026, 8, 23),
        previous_watermark=date(2026, 8, 20),
    )

    assert result.quality == "PASS"
    assert result.pagination_complete is True
    assert result.watermark_date == date(2026, 8, 23)
    assert [call[0] for call in provider.calls] == [
        DateRange(start=date(2026, 8, 17), end=date(2026, 8, 23)),
    ] * 2
    assert [call[1] for call in provider.calls] == [None, "1"]
    assert {call[2] for call in provider.calls} == {100}
    assert result.facts[0].provider_business_date == date(2026, 8, 23)


@pytest.mark.asyncio
async def test_terminal_fiftieth_cash_flow_page_is_accepted() -> None:
    pages = [
        page(
            (flow(f"flow-{index}"),),
            cursor=str(index + 1) if index < 49 else None,
        )
        for index in range(50)
    ]
    provider = ScriptedCashFlowProvider(pages)

    result = await fetch_cash_flows(
        ACCOUNT_ID,
        provider,
        sync_run_id=SYNC_RUN_ID,
        initial_start=date(2026, 8, 1),
        end=date(2026, 8, 23),
        previous_watermark=None,
    )

    assert result.quality == "PASS"
    assert result.pagination_complete is True
    assert result.watermark_date == date(2026, 8, 23)
    assert len(result.facts) == 50
    assert len(result.source_observations) == 50
    assert len(provider.calls) == 50


@pytest.mark.asyncio
async def test_continuation_after_fiftieth_cash_flow_page_is_incomplete() -> None:
    provider = ScriptedCashFlowProvider(
        [
            page((flow(f"flow-{index}"),), cursor=str(index + 1))
            for index in range(50)
        ]
    )

    result = await fetch_cash_flows(
        ACCOUNT_ID,
        provider,
        sync_run_id=SYNC_RUN_ID,
        initial_start=date(2026, 8, 1),
        end=date(2026, 8, 23),
        previous_watermark=None,
    )

    assert result.quality == "PARTIAL"
    assert result.pagination_complete is False
    assert result.watermark_date is None
    assert len(result.facts) == 50
    assert len(result.source_observations) == 50
    assert len(provider.calls) == 50


@pytest.mark.asyncio
async def test_fallback_identity_is_versioned_and_marks_only_flow_quality_partial() -> None:
    provider = ScriptedCashFlowProvider([page((flow(None),))])

    first = await fetch_cash_flows(
        ACCOUNT_ID,
        provider,
        sync_run_id=SYNC_RUN_ID,
        initial_start=date(2026, 8, 1),
        end=date(2026, 8, 23),
        previous_watermark=None,
    )
    second = await fetch_cash_flows(
        ACCOUNT_ID,
        ScriptedCashFlowProvider([page((flow(None),))]),
        sync_run_id=SYNC_RUN_ID,
        initial_start=date(2026, 8, 1),
        end=date(2026, 8, 23),
        previous_watermark=None,
    )

    assert first.quality == "PARTIAL"
    assert first.pagination_complete is True
    assert first.watermark_date == date(2026, 8, 23)
    assert first.facts[0].identity_quality == "FALLBACK_PARTIAL"
    assert first.facts[0].identity_version == "broker_cash_flow_fallback_v1"
    assert first.facts[0].provider_cash_flow_id == second.facts[0].provider_cash_flow_id


@pytest.mark.asyncio
async def test_interrupted_pagination_keeps_facts_but_never_advances_watermark() -> None:
    provider = ScriptedCashFlowProvider(
        [page((flow("flow-1"),), cursor="1"), BrokerProviderError("unavailable")]
    )

    result = await fetch_cash_flows(
        ACCOUNT_ID,
        provider,
        sync_run_id=SYNC_RUN_ID,
        initial_start=date(2026, 8, 1),
        end=date(2026, 8, 23),
        previous_watermark=None,
    )

    assert result.quality == "PARTIAL"
    assert result.pagination_complete is False
    assert result.watermark_date is None
    assert len(result.facts) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid_page",
    [
        page((flow("duplicate"), flow("duplicate"))),
        page((), cursor="1"),
        page(tuple(flow(f"flow-{index}") for index in range(101))),
    ],
)
async def test_duplicate_empty_continuation_or_oversize_response_fails_closed(
    invalid_page: BrokerPage[BrokerCashFlow],
) -> None:
    result = await fetch_cash_flows(
        ACCOUNT_ID,
        ScriptedCashFlowProvider([invalid_page]),
        sync_run_id=SYNC_RUN_ID,
        initial_start=date(2026, 8, 1),
        end=date(2026, 8, 23),
        previous_watermark=None,
    )

    assert result.quality == "PARTIAL"
    assert result.pagination_complete is False
    assert result.watermark_date is None
