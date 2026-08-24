"""Provider units are fixture-only: direct socket entry points must never work."""

import asyncio
import socket

import anyio
import pytest


@pytest.fixture(autouse=True)
def deny_provider_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def blocked(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise AssertionError("provider unit tests forbid network access")

    async def blocked_async(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise AssertionError("provider unit tests forbid network access")

    monkeypatch.setattr(socket, "create_connection", blocked)
    monkeypatch.setattr(socket.socket, "connect", blocked)
    monkeypatch.setattr(socket.socket, "connect_ex", blocked)
    monkeypatch.setattr(asyncio, "open_connection", blocked_async)
    monkeypatch.setattr(asyncio.BaseEventLoop, "create_connection", blocked_async)
    monkeypatch.setattr(anyio, "connect_tcp", blocked_async)
