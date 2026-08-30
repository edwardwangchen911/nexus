import contextlib
import http.client
import io
import logging
from io import BytesIO
from types import SimpleNamespace

import pytest
import requests
from nexus_fosun_gateway.security.provider_tls import (
    _MAX_PROVIDER_RESPONSE_BYTES,
    enforce_sdk_tls_verification,
)
from urllib3.connectionpool import HTTPSConnectionPool
from urllib3.response import HTTPResponse


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
