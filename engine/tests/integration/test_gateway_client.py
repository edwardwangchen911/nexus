import base64
import hashlib
import hmac
import json
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import UUID

import httpx
import nexus.providers.broker.gateway_client as gateway_client_module
import pytest
from nexus.providers.broker.gateway_client import FosunGatewayClient, GatewayClientError
from nexus.providers.broker.settings import BrokerWorkerSettings

NOW = datetime(2026, 8, 23, 4, 0, tzinfo=UTC)
NONCE = UUID("0198d400-3000-7000-8000-000000000001")
REQUEST_ID = UUID("0198d400-3000-7000-8000-000000000002")
HMAC_SECRET = "synthetic-gateway-hmac-secret-at-least-32-bytes"


def settings(tmp_path: Path) -> BrokerWorkerSettings:
    return BrokerWorkerSettings.for_test(
        database_url="postgresql+asyncpg://synthetic:synthetic@127.0.0.1:5432/synthetic",
        client_cert=tmp_path / "client.crt",
        client_key=tmp_path / "client.key",
        client_ca=tmp_path / "ca.crt",
    )


@pytest.mark.asyncio
async def test_client_reads_accounts_through_the_fixed_read_only_route(
    tmp_path: Path,
) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == (
            "https://fosun-gateway.internal:8443/internal/v1/read-only/fosun_sync_accounts"
        )
        assert json.loads(request.content) == {}
        _assert_independent_signature(request)
        return httpx.Response(
            200,
            json={
                "items": [],
                "source_request_id": "synthetic-accounts-request",
                "has_more": False,
                "next_cursor": None,
                "as_of": NOW.isoformat(),
                "quality": "PARTIAL",
            },
        )

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = FosunGatewayClient(
        settings(tmp_path),
        client=http_client,
        wall_time=lambda: NOW.timestamp(),
        uuid_factory=iter((NONCE, REQUEST_ID)).__next__,
    )

    page = await client.read_accounts()
    await http_client.aclose()

    assert page.source_request_id == "synthetic-accounts-request"


@pytest.mark.asyncio
async def test_client_uses_only_fixed_origin_path_mtls_hmac_and_exact_payload(
    tmp_path: Path,
) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == (
            "https://fosun-gateway.internal:8443/internal/v1/read-only/fosun_sync_portfolio"
        )
        assert json.loads(request.content) == {
            "account_id": "synthetic-account-001",
            "cursor": None,
            "limit": 100,
            "read_kind": "holdings",
        }
        _assert_independent_signature(request)
        return httpx.Response(
            200,
            json={
                "items": [],
                "source_request_id": "synthetic-gateway-request",
                "has_more": False,
                "next_cursor": None,
                "as_of": NOW.isoformat(),
                "quality": "PARTIAL",
            },
        )

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = FosunGatewayClient(
        settings(tmp_path),
        client=http_client,
        wall_time=lambda: NOW.timestamp(),
        uuid_factory=iter((NONCE, REQUEST_ID)).__next__,
    )

    page = await client.read_holdings(
        "synthetic-account-001",
        cursor=None,
        limit=100,
    )
    await http_client.aclose()

    assert page.source_request_id == "synthetic-gateway-request"
    assert page.quality == "PARTIAL"


@pytest.mark.asyncio
async def test_redirects_and_oversize_gateway_responses_fail_closed(tmp_path: Path) -> None:
    responses = iter(
        (
            httpx.Response(307, headers={"location": "https://attacker.invalid/"}),
            httpx.Response(200, content=b"{" + b"x" * 1_100_000 + b"}"),
        )
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        del request
        return next(responses)

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = FosunGatewayClient(settings(tmp_path), client=http_client)

    with pytest.raises(GatewayClientError, match="gateway response rejected"):
        await client.read_cash("synthetic-account-001")
    with pytest.raises(GatewayClientError, match="gateway response rejected"):
        await client.read_cash_flows(
            "synthetic-account-001",
            date_from=date(2026, 8, 1),
            date_to=date(2026, 8, 23),
            cursor=None,
            limit=100,
        )
    await http_client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "content_type",
    (
        "application/json-patch+json",
        "text/application/json",
        "application/json; profile=synthetic",
        "application/json; charset=latin-1",
    ),
)
async def test_gateway_response_requires_exact_json_media_type(
    tmp_path: Path, content_type: str
) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(200, headers={"content-type": content_type}, content=b"{}")

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    with pytest.raises(GatewayClientError, match="response rejected"):
        await FosunGatewayClient(settings(tmp_path), client=http_client).read_cash(
            "synthetic-account-001"
        )
    await http_client.aclose()


@pytest.mark.asyncio
async def test_gateway_error_exposes_only_allowlisted_sanitized_code(tmp_path: Path) -> None:
    responses = iter(
        (
            httpx.Response(
                502,
                json={
                    "error": {
                        "code": "PROVIDER_MALFORMED_RESPONSE",
                        "message": "must not cross the client boundary",
                        "next_action": "REVIEW_PROVIDER_CONTRACT",
                    },
                    "request_id": "synthetic-request",
                },
            ),
            httpx.Response(
                502,
                json={
                    "error": {"code": "UNREVIEWED_ERROR"},
                    "secret": "must-not-appear",
                },
            ),
        )
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        del request
        return next(responses)

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = FosunGatewayClient(settings(tmp_path), client=http_client)

    with pytest.raises(
        GatewayClientError,
        match=r"^gateway rejected request: PROVIDER_MALFORMED_RESPONSE$",
    ):
        await client.read_cash("synthetic-account-001")
    with pytest.raises(GatewayClientError, match=r"^gateway response rejected$") as error:
        await client.read_cash("synthetic-account-001")
    assert "secret" not in str(error.value)
    await http_client.aclose()


@pytest.mark.asyncio
async def test_content_length_is_rejected_before_stream_and_chunked_body_has_hard_cap(
    tmp_path: Path,
) -> None:
    class ExplodingStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            raise AssertionError("oversized Content-Length must reject before body iteration")
            yield b""  # pragma: no cover

    class OversizeChunks(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"x" * 600_000
            yield b"y" * 600_000

    responses = iter(
        (
            httpx.Response(
                200,
                headers={
                    "content-type": "application/json",
                    "content-length": "1048577",
                },
                stream=ExplodingStream(),
            ),
            httpx.Response(
                200,
                headers={"content-type": "application/json"},
                stream=OversizeChunks(),
            ),
        )
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        del request
        return next(responses)

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = FosunGatewayClient(settings(tmp_path), client=http_client)
    with pytest.raises(GatewayClientError, match="response rejected"):
        await client.read_cash("synthetic-account-001")
    with pytest.raises(GatewayClientError, match="response rejected"):
        await client.read_cash("synthetic-account-001")
    await http_client.aclose()


def test_gateway_origin_cannot_contain_path_query_userinfo_or_http(tmp_path: Path) -> None:
    for origin in (
        "http://fosun-gateway.internal:8443",
        "https://user@fosun-gateway.internal:8443",
        "https://fosun-gateway.internal:8443/proxy",
        "https://fosun-gateway.internal:8443?target=elsewhere",
    ):
        with pytest.raises(ValueError, match="gateway origin"):
            FosunGatewayClient(settings(tmp_path).model_copy(update={"gateway_origin": origin}))


def test_default_transport_is_mtls_bounded_and_does_not_trust_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def capture_client(**kwargs: object) -> object:
        captured.update(kwargs)
        return object()

    ssl_context = object()
    monkeypatch.setattr(gateway_client_module.httpx, "AsyncClient", capture_client)
    monkeypatch.setattr(
        gateway_client_module,
        "_mtls_context",
        lambda *_args: ssl_context,
    )

    FosunGatewayClient(settings(tmp_path))

    assert captured["verify"] is ssl_context
    assert "cert" not in captured
    assert captured["follow_redirects"] is False
    assert captured["trust_env"] is False
    assert captured["timeout"] == httpx.Timeout(10.0)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("cursor", "limit"),
    [
        (None, True),
        (None, 0),
        (None, 101),
        ("-1", 100),
        ("1" * 13, 100),
    ],
)
async def test_list_pagination_inputs_are_fixed_and_strict(
    tmp_path: Path,
    cursor: str | None,
    limit: int,
) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError(f"invalid pagination made a request: {request.url}")

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = FosunGatewayClient(settings(tmp_path), client=http_client)

    with pytest.raises(GatewayClientError, match="pagination"):
        await client.read_orders(
            "synthetic-account-001",
            cursor=cursor,
            limit=limit,
        )
    await http_client.aclose()


def _assert_independent_signature(request: httpx.Request) -> None:
    timestamp = request.headers["X-Nexus-Timestamp"]
    nonce = request.headers["X-Nexus-Nonce"]
    body_hash = hashlib.sha256(request.content).hexdigest()
    canonical = "\n".join(
        (
            "hmac-v2",
            "POST",
            request.url.raw_path.decode(),
            timestamp,
            nonce,
            "nexus-broker-worker",
            str(REQUEST_ID),
            body_hash,
        )
    )
    signature = (
        base64.urlsafe_b64encode(
            hmac.new(HMAC_SECRET.encode(), canonical.encode(), hashlib.sha256).digest()
        )
        .rstrip(b"=")
        .decode()
    )
    assert request.headers["X-Nexus-Key-Id"] == "synthetic-gateway-current"
    assert request.headers["X-Nexus-Signature-Version"] == "hmac-v2"
    assert request.headers["X-Nexus-Body-SHA256"] == body_hash
    assert request.headers["X-Nexus-Signature"] == signature
    assert request.headers["X-Nexus-Actor-Id"] == "nexus-broker-worker"
    assert request.headers["X-Request-Id"] == str(REQUEST_ID)
