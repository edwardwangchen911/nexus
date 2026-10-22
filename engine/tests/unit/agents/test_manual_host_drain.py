"""Unconfirmed SDK cleanup cannot assert a locally terminated host."""

import asyncio
from types import SimpleNamespace

import pytest

from nexus.agents import codex_sdk
from nexus.paper.manual_host_exit import host_drain_scope


@pytest.mark.asyncio
async def test_cancel_during_sdk_close_prevents_host_exit():
    closing = asyncio.Event()
    released = asyncio.Event()

    async def close():
        closing.set()
        await released.wait()

    with host_drain_scope() as state:
        task = asyncio.create_task(codex_sdk._close_sdk_client(SimpleNamespace(close=close)))
        await closing.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        released.set()
        assert not state.confirmed
