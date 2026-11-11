"""Offline test targets cannot silently inherit the user's live model transport."""

import os
import sys
from collections.abc import Awaitable, Callable, Coroutine, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
from typing import Any

import httpx
from sqlalchemy.engine import URL, make_url

_ISOLATED = ContextVar("isolated_model_io", default=False)
_LIVE_HTTP_SEND = httpx.AsyncClient.send


def offline_model_io_required() -> bool:
    # Importing a test helper directly does not run pytest's autouse fixtures.
    return _ISOLATED.get() or "pytest" in sys.modules or "PYTEST_CURRENT_TEST" in os.environ


@contextmanager
def model_io_scope(database_url: str | URL | None) -> Iterator[None]:
    database = make_url(database_url).database if database_url is not None else None
    isolated = bool(database and database.startswith("nexus_test_"))
    token = _ISOLATED.set(_ISOLATED.get() or isolated)
    try:
        yield
    finally:
        _ISOLATED.reset(token)


def guard_worker_model_io[**P, T](
    call: Callable[P, Awaitable[T]],
) -> Callable[P, Coroutine[Any, Any, T]]:
    @wraps(call)
    async def guarded(*args: P.args, **kwargs: P.kwargs) -> T:
        settings = args[0] if args else kwargs.get("settings")
        with model_io_scope(getattr(settings, "database_url", None)):
            return await call(*args, **kwargs)

    return guarded


def require_offline_http_transport(
    client: httpx.AsyncClient, *, url: httpx.URL | str | None = None
) -> None:
    if (
        offline_model_io_required()
        and (url is None or httpx.URL(url).host not in {"127.0.0.1", "::1"})
        and isinstance(client._transport, httpx.AsyncHTTPTransport)
        and getattr(client.send, "__func__", None) is _LIVE_HTTP_SEND
    ):
        raise ValueError("MODEL_OFFLINE_TRANSPORT_REQUIRED")


def require_offline_openai_transport(client: object) -> None:
    transport = getattr(client, "_client", None)
    if isinstance(transport, httpx.AsyncClient):
        require_offline_http_transport(transport, url=getattr(client, "base_url", None))
