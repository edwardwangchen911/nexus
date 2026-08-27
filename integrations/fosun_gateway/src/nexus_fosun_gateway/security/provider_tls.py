"""Force TLS verification across the two reviewed frozen-SDK HTTP call surfaces."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


class _VerifiedSessionProxy:
    def __init__(self, delegate: object, request: Callable[..., object]) -> None:
        self._delegate = delegate
        self._request = request

    def request(self, *args: object, **kwargs: object) -> object:
        kwargs["verify"] = True
        kwargs["allow_redirects"] = False
        return self._request(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)


class _VerifiedRequestsProxy:
    def __init__(self, delegate: object, post: Callable[..., object]) -> None:
        self._delegate = delegate
        self._post = post

    def post(self, *args: object, **kwargs: object) -> object:
        kwargs["verify"] = True
        kwargs["allow_redirects"] = False
        return self._post(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)


def enforce_sdk_tls_verification(client: object, auth_module: object) -> None:
    """Patch the exact v1.2.0 surfaces or reject the SDK before any network call."""
    session = getattr(client, "_http_session", None)
    session_request = getattr(session, "request", None)
    requests_module = getattr(auth_module, "requests", None)
    session_post = getattr(requests_module, "post", None)
    if not callable(session_request) or not callable(session_post):
        raise RuntimeError("reviewed SDK TLS verification surface is unavailable")
    setattr(client, "_http_session", _VerifiedSessionProxy(session, session_request))
    setattr(
        auth_module,
        "requests",
        _VerifiedRequestsProxy(requests_module, session_post),
    )
