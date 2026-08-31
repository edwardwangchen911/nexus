"""Force TLS verification across the two reviewed frozen-SDK HTTP call surfaces."""

from __future__ import annotations

from collections.abc import Callable
from threading import Lock
from typing import Any, Literal, Protocol

from nexus_fosun_gateway.security.dependency_logging import (
    enforce_sdk_dependency_log_boundary,
)

_SDK_SOCKET_TIMEOUT_SECONDS = (5.0, 5.0)
_MAX_PROVIDER_RESPONSE_BYTES = 4 * 1024 * 1024
_PROVIDER_RESPONSE_CHUNK_BYTES = 64 * 1024
HttpSurface = Literal["auth", "business"]


class ProviderHttpObserver(Protocol):
    """Value-free hooks at the two reviewed physical HTTP surfaces."""

    def before_dispatch(self, surface: HttpSurface) -> None: ...

    def after_response(self, surface: HttpSurface, status_code: int) -> None: ...


class ProbeTlsControls:
    """Idempotently close both fresh sessions owned by one probe process."""

    def __init__(
        self,
        business_session: object,
        auth_session: object,
        *,
        client: object,
        auth_module: object,
        business_proxy: object,
        auth_proxy: object,
        observer: ProviderHttpObserver,
    ) -> None:
        self._business_session = business_session
        self._auth_session = auth_session
        self._client = client
        self._auth_module = auth_module
        self._business_proxy = business_proxy
        self._auth_proxy = auth_proxy
        self._observer = observer
        self._lock = Lock()
        self._closed = False

    def attests(self, client: object, observer: ProviderHttpObserver) -> bool:
        """Prove both currently installed proxies are bound to this exact observer."""
        return (
            client is self._client
            and observer is self._observer
            and getattr(self._client, "_http_session", None) is self._business_proxy
            and getattr(self._auth_module, "requests", None) is self._auth_proxy
            and getattr(self._business_proxy, "_observer", None) is observer
            and getattr(self._auth_proxy, "_observer", None) is observer
        )

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
        failed = False
        for session in (self._business_session, self._auth_session):
            close = getattr(session, "close", None)
            if not callable(close):
                failed = True
                continue
            try:
                close()
            except Exception:
                failed = True
        if failed:
            raise RuntimeError("probe HTTP sessions could not be closed")


class _VerifiedSessionProxy:
    def __init__(
        self,
        delegate: object,
        request: Callable[..., object],
        observer: ProviderHttpObserver | None = None,
    ) -> None:
        self._delegate = delegate
        self._request = request
        self._observer = observer

    def request(self, *args: object, **kwargs: object) -> object:
        enforce_sdk_dependency_log_boundary()
        kwargs["verify"] = True
        kwargs["allow_redirects"] = False
        kwargs["timeout"] = _SDK_SOCKET_TIMEOUT_SECONDS
        kwargs["stream"] = True
        _before_dispatch(self._observer, "business")
        response = _read_bounded_response(self._request(*args, **kwargs))
        _after_response(self._observer, "business", response)
        return response

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)


class _VerifiedRequestsProxy:
    def __init__(
        self,
        delegate: object,
        post: Callable[..., object],
        observer: ProviderHttpObserver | None = None,
    ) -> None:
        self._delegate = delegate
        self._post = post
        self._observer = observer

    def post(self, *args: object, **kwargs: object) -> object:
        enforce_sdk_dependency_log_boundary()
        kwargs["verify"] = True
        kwargs["allow_redirects"] = False
        kwargs["timeout"] = _SDK_SOCKET_TIMEOUT_SECONDS
        kwargs["stream"] = True
        _before_dispatch(self._observer, "auth")
        response = _read_bounded_response(self._post(*args, **kwargs))
        _after_response(self._observer, "auth", response)
        return response

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)


def enforce_sdk_tls_verification(client: object, auth_module: object) -> None:
    """Patch the exact v1.2.0 surfaces or reject the SDK before any network call."""
    _install_sdk_tls_verification(client, auth_module, observer=None, probe_mode=False)


def enforce_probe_tls_controls(
    client: object,
    auth_module: object,
    *,
    observer: ProviderHttpObserver,
) -> ProbeTlsControls:
    """Install counted, no-retry, closable TLS surfaces for one probe only."""
    business_session, auth_session = _install_sdk_tls_verification(
        client,
        auth_module,
        observer=observer,
        probe_mode=True,
    )
    return ProbeTlsControls(
        business_session,
        auth_session,
        client=client,
        auth_module=auth_module,
        business_proxy=getattr(client, "_http_session"),
        auth_proxy=getattr(auth_module, "requests"),
        observer=observer,
    )


def _install_sdk_tls_verification(
    client: object,
    auth_module: object,
    *,
    observer: ProviderHttpObserver | None,
    probe_mode: bool,
) -> tuple[object, object]:
    enforce_sdk_dependency_log_boundary()
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
    enforce_sdk_dependency_log_boundary()
    isolated_auth_post = getattr(isolated_auth_session, "post", None)
    if not callable(isolated_auth_post) or not hasattr(
        isolated_auth_session, "trust_env"
    ):
        raise RuntimeError("reviewed SDK TLS verification surface is unavailable")
    if probe_mode:
        try:
            _force_zero_https_retries(session, requests_module)
            _force_zero_https_retries(isolated_auth_session, requests_module)
            _require_closable(session)
            _require_closable(isolated_auth_session)
        except Exception:
            _close_quietly(session)
            _close_quietly(isolated_auth_session)
            raise RuntimeError("reviewed SDK probe HTTP surface is unavailable") from None
    setattr(session, "trust_env", False)
    setattr(isolated_auth_session, "trust_env", False)
    business_proxy = _VerifiedSessionProxy(session, session_request, observer)
    auth_proxy = _VerifiedRequestsProxy(requests_module, isolated_auth_post, observer)
    setattr(client, "_http_session", business_proxy)
    setattr(auth_module, "requests", auth_proxy)
    return session, isolated_auth_session


def _force_zero_https_retries(session: object, requests_module: object) -> None:
    mount = getattr(session, "mount", None)
    get_adapter = getattr(session, "get_adapter", None)
    adapters = getattr(requests_module, "adapters", None)
    adapter_type = getattr(adapters, "HTTPAdapter", None)
    if not callable(mount) or not callable(get_adapter) or not callable(adapter_type):
        raise RuntimeError("reviewed SDK probe HTTP surface is unavailable")
    existing_adapter = get_adapter("https://")
    existing_retries = getattr(existing_adapter, "max_retries", None)
    retry_factory = getattr(existing_retries, "new", None)
    if not callable(retry_factory):
        raise RuntimeError("reviewed SDK probe HTTP surface is unavailable")
    try:
        zero_retries = retry_factory(
            total=0,
            connect=0,
            read=0,
            redirect=0,
            status=0,
            other=0,
        )
        replacement = adapter_type(max_retries=zero_retries)
        mount("https://", replacement)
        installed_retries = getattr(get_adapter("https://"), "max_retries", None)
    except Exception:
        raise RuntimeError("reviewed SDK probe HTTP surface is unavailable") from None
    if not _all_retry_dimensions_are_zero(installed_retries):
        raise RuntimeError("reviewed SDK probe HTTP surface is unavailable")
    old_close = getattr(existing_adapter, "close", None)
    if callable(old_close):
        old_close()


def _all_retry_dimensions_are_zero(retries: object) -> bool:
    return all(
        getattr(retries, name, None) == 0
        for name in ("total", "connect", "read", "redirect", "status", "other")
    )


def _require_closable(session: object) -> None:
    if not callable(getattr(session, "close", None)):
        raise RuntimeError("reviewed SDK probe HTTP surface is unavailable")


def _close_quietly(session: object) -> None:
    close = getattr(session, "close", None)
    if callable(close):
        try:
            close()
        except Exception:
            pass


def _before_dispatch(
    observer: ProviderHttpObserver | None,
    surface: HttpSurface,
) -> None:
    if observer is not None:
        observer.before_dispatch(surface)


def _after_response(
    observer: ProviderHttpObserver | None,
    surface: HttpSurface,
    response: object,
) -> None:
    if observer is None:
        return
    status_code = getattr(response, "status_code", None)
    if not isinstance(status_code, int) or isinstance(status_code, bool):
        _close_quietly(response)
        raise RuntimeError("reviewed SDK probe HTTP response is unavailable")
    try:
        observer.after_response(surface, status_code)
    except Exception:
        _close_quietly(response)
        raise


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
