"""Order same-host account work before opening any database transaction.

This is scheduling only. Every caller still performs its original persisted
pause, history, lease and budget checks under the existing database locks.
"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import UUID
from weakref import WeakKeyDictionary, WeakValueDictionary

from sqlalchemy.engine import make_url

from nexus.settings import EngineSettings

_Key = tuple[str | None, int, str | None, UUID]
_TURNS: WeakKeyDictionary[asyncio.AbstractEventLoop, WeakValueDictionary[_Key, asyncio.Lock]] = (
    WeakKeyDictionary()
)


@asynccontextmanager
async def account_transaction_turn(
    settings: EngineSettings | None, *, account_id: UUID | None = None
) -> AsyncIterator[None]:
    """Wait without DB resources; release only after the caller's owned cleanup."""
    owner = (
        account_id
        if account_id is not None
        else (settings.agent_paper_account_id if settings else None)
    )
    if settings is None or not settings.database_url or owner is None:
        yield
        return
    url = make_url(settings.database_url)
    key = (url.host, url.port or 5432, url.database, owner)
    loop = asyncio.get_running_loop()
    turns = _TURNS.setdefault(loop, WeakValueDictionary())
    # No await separates lookup and registration. A holder/waiter keeps this
    # exact lock alive; idle entries and completed event loops are not retained.
    lock = turns.get(key)
    if lock is None:
        lock = asyncio.Lock()
        turns[key] = lock
    async with lock:
        yield
