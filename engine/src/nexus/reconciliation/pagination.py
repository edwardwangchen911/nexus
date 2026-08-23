"""Deterministic bounds for one broker logical paginated read."""

from collections.abc import Awaitable, Callable
from typing import TypeVar

from nexus.providers.broker.base import BrokerPage, BrokerProviderError

PAGE_LIMIT = 100
MAX_PAGES_PER_LOGICAL_READ = 50
MAX_FACTS_PER_LOGICAL_READ = 5_000
ItemT = TypeVar("ItemT")


class AtomicWindowInvalidated(BrokerProviderError):
    """The Provider responded, but the current atomic attempt is unusable."""


async def collect_pages(
    fetch: Callable[[str | None], Awaitable[BrokerPage[ItemT]]],
    *,
    identity: Callable[[ItemT], object],
    observe: Callable[[BrokerPage[ItemT]], None],
    attempt_deadline: float,
    monotonic: Callable[[], float],
) -> tuple[tuple[ItemT, ...], tuple[BrokerPage[ItemT], ...]]:
    cursor: str | None = None
    seen_cursors: set[str] = set()
    seen_identities: set[object] = set()
    items: list[ItemT] = []
    pages: list[BrokerPage[ItemT]] = []
    for _ in range(MAX_PAGES_PER_LOGICAL_READ):
        require_before_deadline(attempt_deadline, monotonic)
        page = await fetch(cursor)
        observe(page)
        require_before_deadline(attempt_deadline, monotonic)
        if len(page.items) > PAGE_LIMIT:
            raise AtomicWindowInvalidated("provider page exceeds the fixed limit")
        if page.has_more and not page.items:
            raise AtomicWindowInvalidated("empty continuation page")
        for item in page.items:
            item_identity = identity(item)
            if item_identity in seen_identities:
                raise AtomicWindowInvalidated("duplicate fact across pages")
            seen_identities.add(item_identity)
        items.extend(page.items)
        pages.append(page)
        if len(items) > MAX_FACTS_PER_LOGICAL_READ:
            raise AtomicWindowInvalidated("provider facts exceed the fixed limit")
        if not page.has_more:
            return tuple(items), tuple(pages)
        next_cursor = page.next_cursor
        if next_cursor is None or next_cursor == cursor or next_cursor in seen_cursors:
            raise AtomicWindowInvalidated("pagination cursor contradiction")
        seen_cursors.add(next_cursor)
        cursor = next_cursor
    raise AtomicWindowInvalidated("pagination page count exceeds the fixed limit")


def require_before_deadline(
    attempt_deadline: float,
    monotonic: Callable[[], float],
) -> None:
    if monotonic() > attempt_deadline:
        raise AtomicWindowInvalidated("atomic window deadline expired")
