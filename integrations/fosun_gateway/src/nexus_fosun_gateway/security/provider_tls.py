"""Force TLS verification across the two reviewed frozen-SDK HTTP call surfaces."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

_SDK_SOCKET_TIMEOUT_SECONDS = (5.0, 5.0)
_MAX_PROVIDER_RESPONSE_BYTES = 4 * 1024 * 1024
_PROVIDER_RESPONSE_CHUNK_BYTES = 64 * 1024


class _VerifiedSessionProxy:
    def __init__(self, delegate: object, request: Callable[..., object]) -> None:
        self._delegate = delegate
        self._request = request

    def request(self, *args: object, **kwargs: object) -> object:
        kwargs["verify"] = True
        kwargs["allow_redirects"] = False
        kwargs["timeout"] = _SDK_SOCKET_TIMEOUT_SECONDS
        kwargs["stream"] = True
        return _read_bounded_response(self._request(*args, **kwargs))

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)


class _VerifiedRequestsProxy:
    def __init__(self, delegate: object, post: Callable[..., object]) -> None:
        self._delegate = delegate
        self._post = post

    def post(self, *args: object, **kwargs: object) -> object:
        kwargs["verify"] = True
        kwargs["allow_redirects"] = False
        kwargs["timeout"] = _SDK_SOCKET_TIMEOUT_SECONDS
        kwargs["stream"] = True
        return _read_bounded_response(self._post(*args, **kwargs))

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)


def enforce_sdk_tls_verification(client: object, auth_module: object) -> None:
    """Patch the exact v1.2.0 surfaces or reject the SDK before any network call."""
    session = getattr(client, "_http_session", None)
    session_request = getattr(session, "request", None)
    requests_module = getattr(auth_module, "requests", None)
    session_factory = getattr(requests_module, "Session", None)
    if (
        not callable(session_request)
        or not hasattr(session, "trust_env")
        or not callable(session_factory)
    ):
        raise RuntimeError("reviewed SDK TLS verification surface is unavailable")
    isolated_auth_session = session_factory()
    isolated_auth_post = getattr(isolated_auth_session, "post", None)
    if not callable(isolated_auth_post) or not hasattr(
        isolated_auth_session, "trust_env"
    ):
        raise RuntimeError("reviewed SDK TLS verification surface is unavailable")
    setattr(session, "trust_env", False)
    setattr(isolated_auth_session, "trust_env", False)
    setattr(client, "_http_session", _VerifiedSessionProxy(session, session_request))
    setattr(
        auth_module,
        "requests",
        _VerifiedRequestsProxy(requests_module, isolated_auth_post),
    )


def _read_bounded_response(response: object) -> object:
    """Materialize at most one bounded SDK response before its JSON parser runs."""
    headers = getattr(response, "headers", None)
    headers_get = getattr(headers, "get", None)
    iter_content = getattr(response, "iter_content", None)
    close = getattr(response, "close", None)
    if not callable(headers_get) or not callable(iter_content) or not callable(close):
        raise RuntimeError("reviewed SDK bounded response surface is unavailable")

    content_length = headers_get("Content-Length")
    if content_length is not None:
        try:
            declared_length = int(content_length)
        except (TypeError, ValueError):
            close()
            raise RuntimeError("provider response length is invalid") from None
        if declared_length < 0 or declared_length > _MAX_PROVIDER_RESPONSE_BYTES:
            close()
            raise RuntimeError("provider response exceeded bounded size")

    payload = bytearray()
    try:
        for chunk in iter_content(chunk_size=_PROVIDER_RESPONSE_CHUNK_BYTES):
            if not isinstance(chunk, bytes):
                raise RuntimeError("reviewed SDK bounded response surface is unavailable")
            payload.extend(chunk)
            if len(payload) > _MAX_PROVIDER_RESPONSE_BYTES:
                raise RuntimeError("provider response exceeded bounded size")
    except Exception:
        close()
        raise
    setattr(response, "_content", bytes(payload))
    setattr(response, "_content_consumed", True)
    return response
