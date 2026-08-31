"""Force TLS verification across the two reviewed frozen-SDK HTTP call surfaces."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from threading import Lock
from typing import Any, Literal, Protocol

from nexus_fosun_gateway.adapter.refusal import NonRetryableTransportRefusal
from nexus_fosun_gateway.security.dependency_logging import (
    enforce_sdk_dependency_log_boundary,
)

_SDK_SOCKET_TIMEOUT_SECONDS = (5.0, 5.0)
_MAX_PROVIDER_RESPONSE_BYTES = 4 * 1024 * 1024
_PROVIDER_RESPONSE_CHUNK_BYTES = 64 * 1024
HttpSurface = Literal["auth", "business"]
_MISSING_ATTRIBUTE = object()


class ProviderHttpObserver(Protocol):
    """Value-free hooks at the two reviewed physical HTTP surfaces."""

    def before_dispatch(self, surface: HttpSurface) -> None: ...

    def after_response(self, surface: HttpSurface, status_code: int) -> None: ...


class _ProbeDispatchGate:
    def __init__(self) -> None:
        self._lock = Lock()
        self._revoked = False
        self._last_linearized_attempt = 0
        self._inflight_attempts: set[int] = set()
        self._revoked_after_attempt: int | None = None

    def require_open(self) -> None:
        with self._lock:
            if self._revoked:
                raise NonRetryableTransportRefusal("probe HTTP dispatch is revoked")

    def dispatch(self, operation: Callable[[], object]) -> object:
        """Grant and record one attempt under a short lock, then run it unlocked."""
        attempt = self._linearize_attempt()
        try:
            return operation()
        finally:
            self._complete_attempt(attempt)

    def _linearize_attempt(self) -> int:
        with self._lock:
            if (
                self._revoked
                or self._inflight_attempts
            ):
                raise NonRetryableTransportRefusal("probe HTTP dispatch is revoked")
            self._last_linearized_attempt += 1
            attempt = self._last_linearized_attempt
            self._inflight_attempts.add(attempt)
            return attempt

    def _complete_attempt(self, attempt: int) -> None:
        with self._lock:
            self._inflight_attempts.discard(attempt)

    def revoke(self) -> bool:
        """Revoke admission and report whether one granted attempt remains in flight."""
        with self._lock:
            self._revoked = True
            self._revoked_after_attempt = self._last_linearized_attempt
            return bool(self._inflight_attempts)


@dataclass(frozen=True, slots=True)
class _InstalledTlsSurfaces:
    business_session: object
    auth_session: object
    requests_module: object
    business_proxy: object
    auth_proxy: object
    business_trust_env: object
    auth_trust_env: object
    dispatch_gate: _ProbeDispatchGate | None


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
        dispatch_gate: _ProbeDispatchGate | None = None,
    ) -> None:
        self._business_session = business_session
        self._auth_session = auth_session
        self._client = client
        self._auth_module = auth_module
        self._business_proxy = business_proxy
        self._auth_proxy = auth_proxy
        self._observer = observer
        self._dispatch_gate = dispatch_gate
        self._lock = Lock()
        self._closed = False
        self._close_failed = False

    def attests(self, client: object, observer: ProviderHttpObserver) -> bool:
        """Prove both currently installed proxies are bound to this exact observer."""
        return (
            client is self._client
            and observer is self._observer
            and getattr(self._client, "_http_session", None) is self._business_proxy
            and getattr(self._auth_module, "requests", None) is self._auth_proxy
            and getattr(self._business_proxy, "_observer", None) is observer
            and getattr(self._auth_proxy, "_observer", None) is observer
            and getattr(self._business_proxy, "_dispatch_gate", None)
            is self._dispatch_gate
            and getattr(self._auth_proxy, "_dispatch_gate", None)
            is self._dispatch_gate
        )

    def close(self) -> None:
        with self._lock:
            if self._closed:
                if self._close_failed:
                    raise RuntimeError("probe HTTP sessions could not be closed")
                return
            self._closed = True
            had_inflight_attempt = False
            if self._dispatch_gate is not None:
                had_inflight_attempt = self._dispatch_gate.revoke()
            failure: BaseException | None = None
            for session in (self._business_session, self._auth_session):
                close = getattr(session, "close", None)
                if not callable(close):
                    if failure is None:
                        failure = RuntimeError("probe HTTP session is not closable")
                    continue
                try:
                    close()
                except BaseException as error:
                    if failure is None:
                        failure = error
            if isinstance(failure, KeyboardInterrupt | SystemExit):
                self._close_failed = True
                raise failure
            if failure is not None or had_inflight_attempt:
                self._close_failed = True
                raise RuntimeError("probe HTTP sessions could not be closed")


class _VerifiedSessionProxy:
    def __init__(
        self,
        delegate: object,
        request: Callable[..., object],
        observer: ProviderHttpObserver | None = None,
        dispatch_gate: _ProbeDispatchGate | None = None,
    ) -> None:
        self._delegate = delegate
        self._request = request
        self._observer = observer
        self._dispatch_gate = dispatch_gate

    def request(self, *args: object, **kwargs: object) -> object:
        enforce_sdk_dependency_log_boundary()
        kwargs["verify"] = True
        kwargs["allow_redirects"] = False
        kwargs["timeout"] = _SDK_SOCKET_TIMEOUT_SECONDS
        kwargs["stream"] = True
        _require_dispatch_open(self._dispatch_gate)
        _before_dispatch(self._observer, "business")
        return _dispatch_verified_request(
            self._dispatch_gate,
            self._request,
            args,
            kwargs,
            self._observer,
            "business",
        )

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)


class _VerifiedRequestsProxy:
    def __init__(
        self,
        delegate: object,
        post: Callable[..., object],
        observer: ProviderHttpObserver | None = None,
        dispatch_gate: _ProbeDispatchGate | None = None,
    ) -> None:
        self._delegate = delegate
        self._post = post
        self._observer = observer
        self._dispatch_gate = dispatch_gate

    def post(self, *args: object, **kwargs: object) -> object:
        enforce_sdk_dependency_log_boundary()
        kwargs["verify"] = True
        kwargs["allow_redirects"] = False
        kwargs["timeout"] = _SDK_SOCKET_TIMEOUT_SECONDS
        kwargs["stream"] = True
        _require_dispatch_open(self._dispatch_gate)
        _before_dispatch(self._observer, "auth")
        return _dispatch_verified_request(
            self._dispatch_gate,
            self._post,
            args,
            kwargs,
            self._observer,
            "auth",
        )

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
    installation = _install_sdk_tls_verification(
        client,
        auth_module,
        observer=observer,
        probe_mode=True,
    )
    try:
        return ProbeTlsControls(
            installation.business_session,
            installation.auth_session,
            client=client,
            auth_module=auth_module,
            business_proxy=installation.business_proxy,
            auth_proxy=installation.auth_proxy,
            observer=observer,
            dispatch_gate=installation.dispatch_gate,
        )
    except BaseException as error:
        _rollback_complete_installation(installation, client, auth_module)
        if isinstance(error, KeyboardInterrupt | SystemExit):
            raise
        raise RuntimeError("reviewed SDK probe HTTP surface is unavailable") from None


def _install_sdk_tls_verification(
    client: object,
    auth_module: object,
    *,
    observer: ProviderHttpObserver | None,
    probe_mode: bool,
) -> _InstalledTlsSurfaces:
    enforce_sdk_dependency_log_boundary()
    session: object | None = None
    requests_module: object | None = None
    isolated_auth_session: object | None = None
    session_trust_env: object = _MISSING_ATTRIBUTE
    auth_trust_env: object = _MISSING_ATTRIBUTE
    try:
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
            _force_zero_https_retries(session, requests_module)
            _force_zero_https_retries(isolated_auth_session, requests_module)
            _require_closable(session)
            _require_closable(isolated_auth_session)
        session_trust_env = getattr(session, "trust_env")
        auth_trust_env = getattr(isolated_auth_session, "trust_env")
        dispatch_gate = _ProbeDispatchGate() if probe_mode else None
        setattr(session, "trust_env", False)
        setattr(isolated_auth_session, "trust_env", False)
        business_proxy = _VerifiedSessionProxy(
            session,
            session_request,
            observer,
            dispatch_gate,
        )
        auth_proxy = _VerifiedRequestsProxy(
            requests_module,
            isolated_auth_post,
            observer,
            dispatch_gate,
        )
        setattr(client, "_http_session", business_proxy)
        setattr(auth_module, "requests", auth_proxy)
    except BaseException as error:
        if session is not None:
            _restore_attribute_quietly(client, "_http_session", session)
        if requests_module is not None:
            _restore_attribute_quietly(auth_module, "requests", requests_module)
        if session is not None and session_trust_env is not _MISSING_ATTRIBUTE:
            _restore_attribute_quietly(session, "trust_env", session_trust_env)
        if (
            isolated_auth_session is not None
            and auth_trust_env is not _MISSING_ATTRIBUTE
        ):
            _restore_attribute_quietly(
                isolated_auth_session,
                "trust_env",
                auth_trust_env,
            )
        if probe_mode:
            _close_quietly(session)
            _close_quietly(isolated_auth_session)
        elif isolated_auth_session is not None:
            _close_quietly(isolated_auth_session)
        if isinstance(error, KeyboardInterrupt | SystemExit):
            raise
        if probe_mode:
            raise RuntimeError("reviewed SDK probe HTTP surface is unavailable") from None
        raise
    assert isolated_auth_session is not None
    assert session is not None
    assert requests_module is not None
    return _InstalledTlsSurfaces(
        business_session=session,
        auth_session=isolated_auth_session,
        requests_module=requests_module,
        business_proxy=business_proxy,
        auth_proxy=auth_proxy,
        business_trust_env=session_trust_env,
        auth_trust_env=auth_trust_env,
        dispatch_gate=dispatch_gate,
    )


def _require_dispatch_open(dispatch_gate: _ProbeDispatchGate | None) -> None:
    if dispatch_gate is not None:
        dispatch_gate.require_open()


def _dispatch_verified_request(
    dispatch_gate: _ProbeDispatchGate | None,
    delegate: Callable[..., object],
    args: tuple[object, ...],
    kwargs: dict[str, object],
    observer: ProviderHttpObserver | None,
    surface: HttpSurface,
) -> object:
    def run_verified_request() -> object:
        response = _read_bounded_response(delegate(*args, **kwargs))
        _after_response(observer, surface, response)
        return response

    if dispatch_gate is None:
        return run_verified_request()
    return dispatch_gate.dispatch(run_verified_request)


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
        except BaseException:
            pass


def _restore_attribute_quietly(target: object, name: str, value: object) -> None:
    try:
        setattr(target, name, value)
    except BaseException:
        pass


def _rollback_complete_installation(
    installation: _InstalledTlsSurfaces,
    client: object,
    auth_module: object,
) -> None:
    if installation.dispatch_gate is not None:
        installation.dispatch_gate.revoke()
    _restore_attribute_quietly(
        client,
        "_http_session",
        installation.business_session,
    )
    _restore_attribute_quietly(
        auth_module,
        "requests",
        installation.requests_module,
    )
    _restore_attribute_quietly(
        installation.business_session,
        "trust_env",
        installation.business_trust_env,
    )
    _restore_attribute_quietly(
        installation.auth_session,
        "trust_env",
        installation.auth_trust_env,
    )
    _close_quietly(installation.business_session)
    _close_quietly(installation.auth_session)


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
    except BaseException:
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
    except BaseException:
        close()
        raise
    setattr(response, "_content", bytes(payload))
    setattr(response, "_content_consumed", True)
    return response
