import contextlib
import http.client
import io
import logging
import threading
from collections.abc import Callable
from io import BytesIO
from types import SimpleNamespace

import nexus_fosun_gateway.security.provider_tls as provider_tls_module
import pytest
import requests
from nexus_fosun_gateway.adapter.refusal import NonRetryableTransportRefusal
from nexus_fosun_gateway.security.provider_tls import (
    _MAX_PROVIDER_RESPONSE_BYTES,
    ProbeTlsControls,
    ProviderHttpObserver,
    enforce_probe_tls_controls,
    enforce_sdk_tls_verification,
)
from urllib3.connectionpool import HTTPSConnectionPool
from urllib3.response import HTTPResponse


class _RecordingObserver(ProviderHttpObserver):
    def __init__(self, events: list[tuple[str, object]]) -> None:
        self._events = events

    def before_dispatch(self, surface: str) -> None:
        self._events.append(("before", surface))

    def after_response(self, surface: str, status_code: int) -> None:
        self._events.append(("after", (surface, status_code)))


def test_frozen_sdk_http_paths_force_tls_no_proxy_no_redirect_and_socket_timeout() -> None:
    observed: list[tuple[str, object, object, object, object, object]] = []

    class FakeResponse:
        headers: dict[str, str] = {}

        def __init__(self) -> None:
            self._content = b""
            self._content_consumed = False

        def iter_content(self, *, chunk_size: int) -> tuple[bytes, ...]:
            assert chunk_size > 0
            return (b'{"code":0}',)

        def close(self) -> None:
            return None

    class FakeSession:
        trust_env = True

        def request(self, *_args: object, **kwargs: object) -> object:
            observed.append(
                (
                    "request",
                    kwargs.get("verify"),
                    kwargs.get("allow_redirects"),
                    kwargs.get("timeout"),
                    kwargs.get("stream"),
                    self.trust_env,
                )
            )
            return FakeResponse()

        def post(self, *_args: object, **kwargs: object) -> object:
            observed.append(
                (
                    "auth",
                    kwargs.get("verify"),
                    kwargs.get("allow_redirects"),
                    kwargs.get("timeout"),
                    kwargs.get("stream"),
                    self.trust_env,
                )
            )
            return FakeResponse()

    class FakeRequests:
        class RequestException(Exception):
            pass

        Session = FakeSession

        def post(self, *_args: object, **kwargs: object) -> object:
            pytest.fail("module-level requests.post must not be used")

    client = SimpleNamespace(_http_session=FakeSession())
    auth_module = SimpleNamespace(requests=FakeRequests())

    enforce_sdk_tls_verification(client, auth_module)
    client._http_session.request(
        "POST", "https://provider.invalid", verify=False, timeout=None
    )
    auth_module.requests.post(
        "https://provider.invalid", verify=False, timeout=999
    )

    assert observed == [
        ("request", True, False, (5.0, 5.0), True, False),
        ("auth", True, False, (5.0, 5.0), True, False),
    ]


def test_probe_tls_controls_count_physical_edges_and_disable_adapter_retries() -> None:
    events: list[tuple[str, object]] = []
    sessions: list[object] = []

    class FakeResponse:
        headers: dict[str, str] = {}
        status_code = 200

        def iter_content(self, *, chunk_size: int) -> tuple[bytes, ...]:
            assert chunk_size > 0
            return (b'{"code":0}',)

        def close(self) -> None:
            events.append(("response_close", "called"))

    class FakeProbeSession:
        trust_env = True

        def __init__(self) -> None:
            self.closed = False
            self._adapters = {
                "https://": requests.adapters.HTTPAdapter(max_retries=3),
            }
            sessions.append(self)

        def mount(self, prefix: str, adapter: object) -> None:
            self._adapters[prefix] = adapter

        def get_adapter(self, prefix: str) -> object:
            return self._adapters[prefix]

        def request(self, *_args: object, **_kwargs: object) -> object:
            events.append(("delegate", "business"))
            return FakeResponse()

        def post(self, *_args: object, **_kwargs: object) -> object:
            events.append(("delegate", "auth"))
            return FakeResponse()

        def close(self) -> None:
            self.closed = True

    class FakeRequests:
        adapters = requests.adapters
        Session = FakeProbeSession

    business_session = FakeProbeSession()
    client = SimpleNamespace(_http_session=business_session)
    auth_module = SimpleNamespace(requests=FakeRequests())
    observer = _RecordingObserver(events)
    controls = enforce_probe_tls_controls(
        client,
        auth_module,
        observer=observer,
    )

    assert controls.attests(client, observer) is True
    assert controls.attests(client, _RecordingObserver([])) is False

    client._http_session.request("POST", "https://provider.invalid")
    auth_module.requests.post("https://provider.invalid")

    assert events == [
        ("before", "business"),
        ("delegate", "business"),
        ("after", ("business", 200)),
        ("before", "auth"),
        ("delegate", "auth"),
        ("after", ("auth", 200)),
    ]
    for session in sessions:
        retries = session.get_adapter("https://").max_retries
        assert (
            retries.total,
            retries.connect,
            retries.read,
            retries.redirect,
            retries.status,
            retries.other,
        ) == (0, 0, 0, 0, 0, 0)

    controls.close()
    controls.close()
    assert all(session.closed for session in sessions)
    with pytest.raises(NonRetryableTransportRefusal):
        client._http_session.request("POST", "https://provider.invalid")
    with pytest.raises(NonRetryableTransportRefusal):
        auth_module.requests.post("https://provider.invalid")

    installed_proxy = client._http_session
    client._http_session = business_session
    assert controls.attests(client, observer) is False
    client._http_session = installed_proxy


def test_probe_tls_controls_close_both_sessions_when_first_close_is_interrupted() -> None:
    closed: list[str] = []

    class InterruptingSession:
        def close(self) -> None:
            closed.append("business")
            raise KeyboardInterrupt

    class RecordingSession:
        def close(self) -> None:
            closed.append("auth")

    observer = _RecordingObserver([])
    client = SimpleNamespace(_http_session=object())
    auth_module = SimpleNamespace(requests=object())
    controls = ProbeTlsControls(
        InterruptingSession(),
        RecordingSession(),
        client=client,
        auth_module=auth_module,
        business_proxy=client._http_session,
        auth_proxy=auth_module.requests,
        observer=observer,
    )

    with pytest.raises(KeyboardInterrupt):
        controls.close()

    assert closed == ["business", "auth"]


def test_probe_tls_construction_failure_closes_every_created_session() -> None:
    closed: list[str] = []

    class BusinessSession:
        trust_env = True

        def request(self) -> None:
            return None

        def close(self) -> None:
            closed.append("business")

    class InvalidAuthSession:
        trust_env = True

        def close(self) -> None:
            closed.append("auth")

    class FakeRequests:
        Session = InvalidAuthSession

    client = SimpleNamespace(_http_session=BusinessSession())
    auth_module = SimpleNamespace(requests=FakeRequests())

    with pytest.raises(RuntimeError, match="probe HTTP surface"):
        enforce_probe_tls_controls(
            client,
            auth_module,
            observer=_RecordingObserver([]),
        )

    assert closed == ["business", "auth"]


@pytest.mark.parametrize(
    "failure_stage",
    ["auth_trust_env", "auth_surface", "controls_construction"],
)
def test_probe_tls_installation_failure_restores_surfaces_and_closes_sessions(
    failure_stage: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    closed: list[str] = []

    class BusinessSession:
        trust_env = True

        def __init__(self) -> None:
            self._adapters = {
                "https://": requests.adapters.HTTPAdapter(max_retries=2)
            }

        def mount(self, prefix: str, adapter: object) -> None:
            self._adapters[prefix] = adapter

        def get_adapter(self, prefix: str) -> object:
            return self._adapters[prefix]

        def request(self) -> None:
            return None

        def close(self) -> None:
            closed.append("business")

    class AuthSession(BusinessSession):
        def __init__(self) -> None:
            self._trust_env = True
            super().__init__()

        @property
        def trust_env(self) -> bool:
            return self._trust_env

        @trust_env.setter
        def trust_env(self, value: bool) -> None:
            self._trust_env = value
            if failure_stage == "auth_trust_env" and value is False:
                raise RuntimeError("synthetic setter drift")

        def post(self) -> None:
            return None

        def close(self) -> None:
            closed.append("auth")

    class FakeRequests:
        adapters = requests.adapters
        Session = AuthSession

    class AuthModule:
        def __init__(self) -> None:
            self._requests = FakeRequests()
            self.original_requests = self._requests

        @property
        def requests(self) -> object:
            return self._requests

        @requests.setter
        def requests(self, value: object) -> None:
            self._requests = value
            if failure_stage == "auth_surface" and value is not self.original_requests:
                raise RuntimeError("synthetic surface drift")

    business_session = BusinessSession()
    client = SimpleNamespace(_http_session=business_session)
    auth_module = AuthModule()
    if failure_stage == "controls_construction":
        def fail_controls(*_args: object, **_kwargs: object) -> object:
            raise RuntimeError("synthetic controls drift")

        monkeypatch.setattr(provider_tls_module, "ProbeTlsControls", fail_controls)

    with pytest.raises(RuntimeError, match="probe HTTP surface"):
        enforce_probe_tls_controls(
            client,
            auth_module,
            observer=_RecordingObserver([]),
        )

    assert client._http_session is business_session
    assert auth_module.requests is auth_module.original_requests
    assert business_session.trust_env is True
    assert closed == ["business", "auth"]


def test_probe_tls_close_revokes_dispatch_waiting_inside_observer() -> None:
    entered = threading.Event()
    release = threading.Event()
    delegated = False
    sessions: list[object] = []
    errors: list[BaseException] = []

    class BlockingObserver:
        def before_dispatch(self, surface: str) -> None:
            assert surface == "business"
            entered.set()
            assert release.wait(timeout=2)

        def after_response(self, surface: str, status_code: int) -> None:
            raise AssertionError((surface, status_code))

    class FakeProbeSession:
        trust_env = True

        def __init__(self) -> None:
            self.closed = False
            self._adapters = {
                "https://": requests.adapters.HTTPAdapter(max_retries=2)
            }
            sessions.append(self)

        def mount(self, prefix: str, adapter: object) -> None:
            self._adapters[prefix] = adapter

        def get_adapter(self, prefix: str) -> object:
            return self._adapters[prefix]

        def request(self, *_args: object, **_kwargs: object) -> object:
            nonlocal delegated
            delegated = True
            raise AssertionError("revoked probe must not reach delegate")

        def post(self, *_args: object, **_kwargs: object) -> object:
            raise AssertionError("unused")

        def close(self) -> None:
            self.closed = True

    class FakeRequests:
        adapters = requests.adapters
        Session = FakeProbeSession

    client = SimpleNamespace(_http_session=FakeProbeSession())
    auth_module = SimpleNamespace(requests=FakeRequests())
    controls = enforce_probe_tls_controls(
        client,
        auth_module,
        observer=BlockingObserver(),
    )

    def invoke() -> None:
        try:
            client._http_session.request("POST", "https://provider.invalid")
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=invoke)
    thread.start()
    assert entered.wait(timeout=1)
    controls.close()
    release.set()
    thread.join(timeout=2)

    assert thread.is_alive() is False
    assert delegated is False
    assert len(errors) == 1
    assert all(session.closed for session in sessions)


def test_probe_tls_close_wins_race_before_atomic_delegate_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dispatch_entered = threading.Event()
    release_dispatch = threading.Event()
    delegated = False
    errors: list[BaseException] = []

    class FakeResponse:
        headers: dict[str, str] = {}
        status_code = 200

        def iter_content(self, *, chunk_size: int) -> tuple[bytes, ...]:
            del chunk_size
            return (b"{}",)

        def close(self) -> None:
            return None

    class FakeProbeSession:
        trust_env = True

        def __init__(self) -> None:
            self._adapters = {
                "https://": requests.adapters.HTTPAdapter(max_retries=2)
            }

        def mount(self, prefix: str, adapter: object) -> None:
            self._adapters[prefix] = adapter

        def get_adapter(self, prefix: str) -> object:
            return self._adapters[prefix]

        def request(self, *_args: object, **_kwargs: object) -> object:
            nonlocal delegated
            delegated = True
            return FakeResponse()

        def post(self, *_args: object, **_kwargs: object) -> object:
            raise AssertionError("unused")

        def close(self) -> None:
            return None

    class FakeRequests:
        adapters = requests.adapters
        Session = FakeProbeSession

    client = SimpleNamespace(_http_session=FakeProbeSession())
    auth_module = SimpleNamespace(requests=FakeRequests())
    controls = enforce_probe_tls_controls(
        client,
        auth_module,
        observer=_RecordingObserver([]),
    )
    gate = client._http_session._dispatch_gate
    real_dispatch = gate.dispatch

    def pause_before_dispatch(operation: Callable[[], object]) -> object:
        dispatch_entered.set()
        assert release_dispatch.wait(timeout=2)
        return real_dispatch(operation)

    monkeypatch.setattr(gate, "dispatch", pause_before_dispatch)

    def invoke() -> None:
        try:
            client._http_session.request("POST", "https://provider.invalid")
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=invoke)
    thread.start()
    assert dispatch_entered.wait(timeout=1)
    controls.close()
    release_dispatch.set()
    thread.join(timeout=2)

    assert thread.is_alive() is False
    assert delegated is False
    assert len(errors) == 1


def test_probe_tls_close_reports_failure_for_ticket_granted_before_delegate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ticket_registered = threading.Event()
    release_ticket = threading.Event()
    delegate_calls = 0
    request_errors: list[BaseException] = []

    class FakeResponse:
        headers: dict[str, str] = {}
        status_code = 200

        def iter_content(self, *, chunk_size: int) -> tuple[bytes, ...]:
            del chunk_size
            return (b"{}",)

        def close(self) -> None:
            return None

    class FakeProbeSession:
        trust_env = True

        def __init__(self) -> None:
            self._adapters = {
                "https://": requests.adapters.HTTPAdapter(max_retries=2)
            }

        def mount(self, prefix: str, adapter: object) -> None:
            self._adapters[prefix] = adapter

        def get_adapter(self, prefix: str) -> object:
            return self._adapters[prefix]

        def request(self, *_args: object, **_kwargs: object) -> object:
            nonlocal delegate_calls
            delegate_calls += 1
            return FakeResponse()

        def post(self, *_args: object, **_kwargs: object) -> object:
            raise AssertionError("unused")

        def close(self) -> None:
            return None

    class FakeRequests:
        adapters = requests.adapters
        Session = FakeProbeSession

    client = SimpleNamespace(_http_session=FakeProbeSession())
    controls = enforce_probe_tls_controls(
        client,
        SimpleNamespace(requests=FakeRequests()),
        observer=_RecordingObserver([]),
    )
    gate = client._http_session._dispatch_gate
    real_linearize = gate._linearize_attempt

    def pause_after_ticket() -> int:
        attempt = real_linearize()
        ticket_registered.set()
        assert release_ticket.wait(timeout=2)
        return attempt

    monkeypatch.setattr(gate, "_linearize_attempt", pause_after_ticket)

    def invoke() -> None:
        try:
            client._http_session.request("POST", "https://provider.invalid")
        except BaseException as error:
            request_errors.append(error)

    request_thread = threading.Thread(target=invoke)
    request_thread.start()
    assert ticket_registered.wait(timeout=1)

    with pytest.raises(RuntimeError, match="sessions could not be closed"):
        controls.close()
    assert delegate_calls == 0
    with pytest.raises(NonRetryableTransportRefusal):
        client._http_session.request("POST", "https://provider.invalid")

    release_ticket.set()
    request_thread.join(timeout=2)

    assert request_thread.is_alive() is False
    assert delegate_calls == 1
    assert request_errors == []
    assert gate._inflight_attempts == set()


def test_probe_tls_close_does_not_wait_for_linearized_response_headers() -> None:
    delegate_started = threading.Event()
    release_delegate = threading.Event()
    close_attempted = threading.Event()
    close_finished = threading.Event()
    errors: list[BaseException] = []
    close_errors: list[BaseException] = []
    sessions: list[object] = []

    class FakeResponse:
        headers: dict[str, str] = {}
        status_code = 200

        def iter_content(self, *, chunk_size: int) -> tuple[bytes, ...]:
            del chunk_size
            return (b"{}",)

        def close(self) -> None:
            return None

    class FakeProbeSession:
        trust_env = True

        def __init__(self) -> None:
            self.closed = False
            self._adapters = {
                "https://": requests.adapters.HTTPAdapter(max_retries=2)
            }
            sessions.append(self)

        def mount(self, prefix: str, adapter: object) -> None:
            self._adapters[prefix] = adapter

        def get_adapter(self, prefix: str) -> object:
            return self._adapters[prefix]

        def request(self, *_args: object, **_kwargs: object) -> object:
            delegate_started.set()
            assert release_delegate.wait(timeout=2)
            return FakeResponse()

        def post(self, *_args: object, **_kwargs: object) -> object:
            raise AssertionError("unused")

        def close(self) -> None:
            self.closed = True

    class FakeRequests:
        adapters = requests.adapters
        Session = FakeProbeSession

    client = SimpleNamespace(_http_session=FakeProbeSession())
    controls = enforce_probe_tls_controls(
        client,
        SimpleNamespace(requests=FakeRequests()),
        observer=_RecordingObserver([]),
    )
    gate = client._http_session._dispatch_gate

    def invoke() -> None:
        try:
            client._http_session.request("POST", "https://provider.invalid")
        except BaseException as error:
            errors.append(error)

    def close() -> None:
        close_attempted.set()
        try:
            controls.close()
        except BaseException as error:
            close_errors.append(error)
        finally:
            close_finished.set()

    request_thread = threading.Thread(target=invoke)
    request_thread.start()
    assert delegate_started.wait(timeout=1)
    assert gate._last_linearized_attempt == 1
    assert gate._inflight_attempts == {1}
    close_thread = threading.Thread(target=close)
    close_thread.start()
    assert close_attempted.wait(timeout=1)
    assert close_finished.wait(timeout=0.2)
    assert gate._revoked_after_attempt == 1
    with pytest.raises(NonRetryableTransportRefusal):
        client._http_session.request("POST", "https://provider.invalid")
    release_delegate.set()
    request_thread.join(timeout=2)
    close_thread.join(timeout=2)

    assert request_thread.is_alive() is False
    assert close_thread.is_alive() is False
    assert close_finished.is_set()
    assert gate._inflight_attempts == set()
    assert errors == []
    assert len(close_errors) == 1
    assert isinstance(close_errors[0], RuntimeError)
    assert all(session.closed for session in sessions)
    with pytest.raises(RuntimeError, match="sessions could not be closed"):
        controls.close()


def test_probe_tls_close_does_not_wait_for_a_stuck_streamed_body() -> None:
    body_started = threading.Event()
    release_body = threading.Event()
    close_finished = threading.Event()
    delegate_calls = 0
    errors: list[BaseException] = []
    close_errors: list[BaseException] = []
    sessions: list[object] = []

    class BlockingBodyResponse:
        headers: dict[str, str] = {}
        status_code = 200

        def iter_content(self, *, chunk_size: int) -> tuple[bytes, ...]:
            del chunk_size
            body_started.set()
            assert release_body.wait(timeout=2)
            return (b"{}",)

        def close(self) -> None:
            return None

    class FakeProbeSession:
        trust_env = True

        def __init__(self) -> None:
            self.closed = False
            self._adapters = {
                "https://": requests.adapters.HTTPAdapter(max_retries=2)
            }
            sessions.append(self)

        def mount(self, prefix: str, adapter: object) -> None:
            self._adapters[prefix] = adapter

        def get_adapter(self, prefix: str) -> object:
            return self._adapters[prefix]

        def request(self, *_args: object, **_kwargs: object) -> object:
            nonlocal delegate_calls
            delegate_calls += 1
            return BlockingBodyResponse()

        def post(self, *_args: object, **_kwargs: object) -> object:
            raise AssertionError("unused")

        def close(self) -> None:
            self.closed = True

    class FakeRequests:
        adapters = requests.adapters
        Session = FakeProbeSession

    client = SimpleNamespace(_http_session=FakeProbeSession())
    controls = enforce_probe_tls_controls(
        client,
        SimpleNamespace(requests=FakeRequests()),
        observer=_RecordingObserver([]),
    )

    def invoke() -> None:
        try:
            client._http_session.request("POST", "https://provider.invalid")
        except BaseException as error:
            errors.append(error)

    request_thread = threading.Thread(target=invoke)
    request_thread.start()
    assert body_started.wait(timeout=1)

    def close() -> None:
        try:
            controls.close()
        except BaseException as error:
            close_errors.append(error)
        finally:
            close_finished.set()

    close_thread = threading.Thread(target=close)
    close_thread.start()
    assert close_finished.wait(timeout=0.2)
    with pytest.raises(NonRetryableTransportRefusal):
        client._http_session.request("POST", "https://provider.invalid")
    release_body.set()
    request_thread.join(timeout=2)
    close_thread.join(timeout=2)

    assert request_thread.is_alive() is False
    assert close_thread.is_alive() is False
    assert delegate_calls == 1
    assert errors == []
    assert len(close_errors) == 1
    assert isinstance(close_errors[0], RuntimeError)
    assert all(session.closed for session in sessions)
    with pytest.raises(RuntimeError, match="sessions could not be closed"):
        controls.close()


@pytest.mark.parametrize("failure", [RuntimeError("fixed"), KeyboardInterrupt()])
def test_probe_dispatch_ticket_is_released_for_every_delegate_exit(
    failure: BaseException,
) -> None:
    gate = provider_tls_module._ProbeDispatchGate()

    def fail() -> object:
        raise failure

    with pytest.raises(type(failure)):
        gate.dispatch(fail)

    assert gate._inflight_attempts == set()
    assert gate.revoke() is False
    with pytest.raises(NonRetryableTransportRefusal):
        gate.dispatch(lambda: object())


def test_concurrent_probe_tls_close_waits_for_revocation_and_session_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    revoke_entered = threading.Event()
    release_revoke = threading.Event()
    first_finished = threading.Event()
    second_finished = threading.Event()

    class FakeProbeSession:
        trust_env = True

        def __init__(self) -> None:
            self._adapters = {
                "https://": requests.adapters.HTTPAdapter(max_retries=2)
            }

        def mount(self, prefix: str, adapter: object) -> None:
            self._adapters[prefix] = adapter

        def get_adapter(self, prefix: str) -> object:
            return self._adapters[prefix]

        def request(self, *_args: object, **_kwargs: object) -> object:
            raise AssertionError("unused")

        def post(self, *_args: object, **_kwargs: object) -> object:
            raise AssertionError("unused")

        def close(self) -> None:
            return None

    class FakeRequests:
        adapters = requests.adapters
        Session = FakeProbeSession

    client = SimpleNamespace(_http_session=FakeProbeSession())
    controls = enforce_probe_tls_controls(
        client,
        SimpleNamespace(requests=FakeRequests()),
        observer=_RecordingObserver([]),
    )
    gate = client._http_session._dispatch_gate
    real_revoke = gate.revoke

    def pause_revoke() -> None:
        revoke_entered.set()
        assert release_revoke.wait(timeout=2)
        real_revoke()

    monkeypatch.setattr(gate, "revoke", pause_revoke)
    first = threading.Thread(
        target=lambda: (controls.close(), first_finished.set())
    )
    second = threading.Thread(
        target=lambda: (controls.close(), second_finished.set())
    )

    first.start()
    assert revoke_entered.wait(timeout=1)
    second.start()
    assert second_finished.wait(timeout=0.1) is False
    release_revoke.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert first_finished.is_set()
    assert second_finished.is_set()
    with pytest.raises(NonRetryableTransportRefusal):
        client._http_session.request("POST", "https://provider.invalid")


def test_probe_tls_observer_refusal_prevents_delegate_call() -> None:
    delegated = False

    class RefusingObserver:
        def before_dispatch(self, surface: str) -> None:
            del surface
            raise RuntimeError("fixed refusal")

        def after_response(self, surface: str, status_code: int) -> None:
            raise AssertionError((surface, status_code))

    class FakeResponse:
        headers: dict[str, str] = {}
        status_code = 200

        def iter_content(self, *, chunk_size: int) -> tuple[bytes, ...]:
            del chunk_size
            return (b'{"code":0}',)

        def close(self) -> None:
            return None

    class FakeProbeSession:
        trust_env = True

        def __init__(self) -> None:
            self._adapters = {"https://": requests.adapters.HTTPAdapter(max_retries=2)}

        def mount(self, prefix: str, adapter: object) -> None:
            self._adapters[prefix] = adapter

        def get_adapter(self, prefix: str) -> object:
            return self._adapters[prefix]

        def request(self, *_args: object, **_kwargs: object) -> object:
            nonlocal delegated
            delegated = True
            return FakeResponse()

        def post(self, *_args: object, **_kwargs: object) -> object:
            nonlocal delegated
            delegated = True
            return FakeResponse()

        def close(self) -> None:
            return None

    class FakeRequests:
        adapters = requests.adapters
        Session = FakeProbeSession

    client = SimpleNamespace(_http_session=FakeProbeSession())
    auth_module = SimpleNamespace(requests=FakeRequests())
    controls = enforce_probe_tls_controls(
        client,
        auth_module,
        observer=RefusingObserver(),
    )
    try:
        with pytest.raises(RuntimeError, match="fixed refusal"):
            client._http_session.request("POST", "https://provider.invalid")
    finally:
        controls.close()

    assert delegated is False


def test_real_requests_and_urllib3_wire_edges_never_log_provider_canaries(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    canaries = {
        "host": "sdk-wire-log-canary.invalid",
        "query": "api_key=synthetic-wire-secret",
        "authorization": "Bearer synthetic-wire-token",
        "body": "synthetic-wire-body",
        "account": "synthetic-account-001",
    }
    observed: list[tuple[str, str, object, object]] = []

    def fake_make_request(
        pool: HTTPSConnectionPool,
        connection: object,
        method: str,
        url: str,
        body: object = None,
        headers: object = None,
        **_kwargs: object,
    ) -> HTTPResponse:
        assert http.client.HTTPConnection.debuglevel == 0
        assert http.client.HTTPSConnection.debuglevel == 0
        observed.append((method, url, body, headers))
        payload = b'{"code":0}'
        return HTTPResponse(
            body=BytesIO(payload),
            headers={"Content-Length": str(len(payload))},
            status=200,
            preload_content=False,
            decode_content=False,
            pool=pool,
            connection=connection,  # type: ignore[arg-type]
            request_method=method,
            request_url=url,
        )

    monkeypatch.setattr(HTTPSConnectionPool, "_make_request", fake_make_request)
    monkeypatch.setattr(
        "urllib3.util.connection.create_connection",
        lambda *_args, **_kwargs: pytest.fail("real socket must not be opened"),
    )

    client = SimpleNamespace(_http_session=requests.Session())
    auth_module = SimpleNamespace(requests=requests)
    enforce_sdk_tls_verification(client, auth_module)

    captured = io.StringIO()
    direct_handler = logging.StreamHandler(captured)
    dependency_names = (
        "fsopenapi",
        "fsopenapi.auth",
        "requests",
        "requests.sessions",
        "urllib3",
        "urllib3.connectionpool",
    )
    for name in dependency_names:
        logger = logging.getLogger(name)
        monkeypatch.setattr(logger, "filters", [])
        monkeypatch.setattr(logger, "handlers", [direct_handler])
        monkeypatch.setattr(logger, "disabled", False)
        monkeypatch.setattr(logger, "propagate", True)
        monkeypatch.setattr(logger, "level", logging.DEBUG)
    monkeypatch.setattr(http.client.HTTPConnection, "debuglevel", 1)
    monkeypatch.setattr(http.client.HTTPSConnection, "debuglevel", 1)
    caplog.set_level(logging.DEBUG)
    stdout = io.StringIO()
    stderr = io.StringIO()
    headers = {
        "Authorization": canaries["authorization"],
        "X-Account": canaries["account"],
    }
    url = f"https://{canaries['host']}/private?{canaries['query']}"
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        client._http_session.request(
            "POST",
            url,
            headers=headers,
            data=canaries["body"],
        )
        auth_module.requests.post(
            url,
            headers=headers,
            data=canaries["body"],
        )

    assert len(observed) == 2
    assert all(canaries["query"] in request[1] for request in observed)
    assert all(request[2] == canaries["body"] for request in observed)
    rendered = "\n".join((captured.getvalue(), caplog.text, stdout.getvalue(), stderr.getvalue()))
    for canary in canaries.values():
        assert canary not in rendered


@pytest.mark.parametrize("surface", ["request", "auth"])
def test_frozen_sdk_http_paths_reject_oversized_response_before_json(
    surface: str,
) -> None:
    class OversizedResponse:
        headers = {"Content-Length": str(_MAX_PROVIDER_RESPONSE_BYTES + 1)}

        def __init__(self) -> None:
            self.closed = False
            self.iterated = False

        def iter_content(self, *, chunk_size: int) -> tuple[bytes, ...]:
            del chunk_size
            self.iterated = True
            return (b"must-not-be-read",)

        def close(self) -> None:
            self.closed = True

    response = OversizedResponse()

    class FakeSession:
        trust_env = True

        def request(self, *_args: object, **_kwargs: object) -> object:
            return response

        def post(self, *_args: object, **_kwargs: object) -> object:
            return response

    class FakeRequests:
        Session = FakeSession

    client = SimpleNamespace(_http_session=FakeSession())
    auth_module = SimpleNamespace(requests=FakeRequests())
    enforce_sdk_tls_verification(client, auth_module)

    with pytest.raises(RuntimeError, match="response exceeded bounded size"):
        if surface == "request":
            client._http_session.request("POST", "https://provider.invalid")
        else:
            auth_module.requests.post("https://provider.invalid")

    assert response.closed is True
    assert response.iterated is False


def test_frozen_sdk_http_path_rejects_oversized_chunked_response() -> None:
    class ChunkedResponse:
        headers: dict[str, str] = {}

        def __init__(self) -> None:
            self.closed = False

        def iter_content(self, *, chunk_size: int) -> tuple[bytes, ...]:
            del chunk_size
            return (b"a" * _MAX_PROVIDER_RESPONSE_BYTES, b"b")

        def close(self) -> None:
            self.closed = True

    response = ChunkedResponse()

    class FakeSession:
        trust_env = True

        def request(self, *_args: object, **_kwargs: object) -> object:
            return response

        def post(self, *_args: object, **_kwargs: object) -> object:
            return response

    class FakeRequests:
        Session = FakeSession

    client = SimpleNamespace(_http_session=FakeSession())
    auth_module = SimpleNamespace(requests=FakeRequests())
    enforce_sdk_tls_verification(client, auth_module)

    with pytest.raises(RuntimeError, match="response exceeded bounded size"):
        client._http_session.request("POST", "https://provider.invalid")

    assert response.closed is True


@pytest.mark.parametrize(
    ("client", "auth_module"),
    [
        (SimpleNamespace(), SimpleNamespace(requests=object())),
        (SimpleNamespace(_http_session=object()), SimpleNamespace(requests=object())),
        (
            SimpleNamespace(_http_session=SimpleNamespace(request=lambda: None)),
            SimpleNamespace(),
        ),
        (
            SimpleNamespace(_http_session=SimpleNamespace(request=lambda: None)),
            SimpleNamespace(requests=object()),
        ),
        (
            SimpleNamespace(
                _http_session=SimpleNamespace(request=lambda: None, trust_env=True)
            ),
            SimpleNamespace(
                requests=SimpleNamespace(post=lambda: None, Session=object())
            ),
        ),
    ],
)
def test_sdk_tls_remediation_rejects_any_subject_surface_drift(
    client: object,
    auth_module: object,
) -> None:
    with pytest.raises(RuntimeError, match="TLS verification surface"):
        enforce_sdk_tls_verification(client, auth_module)
