from datetime import UTC, datetime

import pytest
from nexus.providers.broker.base import BrokerPage
from nexus.reconciliation.pagination import AtomicWindowInvalidated, collect_pages

NOW = datetime(2026, 8, 23, 4, 0, tzinfo=UTC)


def _one_item_pages(total: int, calls: list[str | None]):
    async def fetch(cursor: str | None) -> BrokerPage[int]:
        calls.append(cursor)
        offset = int(cursor or "0")
        next_cursor = str(offset + 1) if offset + 1 < total else None
        return BrokerPage[int](
            items=(offset,),
            has_more=next_cursor is not None,
            next_cursor=next_cursor,
            source_request_id=f"synthetic-page-{offset}",
            as_of=NOW,
        )

    return fetch


@pytest.mark.asyncio
async def test_pagination_accepts_terminal_fiftieth_page_and_advances_each_cursor() -> None:
    calls: list[str | None] = []

    items, pages = await collect_pages(
        _one_item_pages(50, calls),
        identity=lambda item: item,
        observe=lambda page: None,
        attempt_deadline=1,
        monotonic=lambda: 0,
    )

    assert len(items) == len(pages) == 50
    assert calls == [None, *(str(index) for index in range(1, 50))]


@pytest.mark.asyncio
async def test_pagination_rejects_a_continuation_after_page_fifty() -> None:
    with pytest.raises(AtomicWindowInvalidated, match="page count"):
        await collect_pages(
            _one_item_pages(51, []),
            identity=lambda item: item,
            observe=lambda page: None,
            attempt_deadline=1,
            monotonic=lambda: 0,
        )
