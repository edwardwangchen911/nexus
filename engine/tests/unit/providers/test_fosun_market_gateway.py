"""Fixed, raw-first Fosun market gateway client contracts."""

import asyncio
import base64
import hashlib
import hmac
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal, cast
from uuid import UUID

import httpx
import pytest
from nexus.providers.market.fosun_gateway import (
    FosunMarketGatewayClient,
    FosunMarketGatewayError,
    FosunMarketGatewayIdentity,
    RawGatewayResponse,
    parse_bar_page,
    parse_quote_page,
)
from pydantic import SecretStr

NOW = datetime(2026, 8, 29, 14, 31, tzinfo=UTC)
NONCE = UUID("0198d400-3000-7000-8000-000000000011")
REQUEST_ID = UUID("0198d400-3000-7000-8000-000000000012")
HMAC_SECRET = "synthetic-market-hmac-secret-at-least-32-bytes"


def _identity(
    tmp_path: Path,
    *,
    origin: str = "https://fosun-gateway.internal:8443",
) -> FosunMarketGatewayIdentity:
    return FosunMarketGatewayIdentity(
        origin=origin,
        key_id="synthetic-market-current",
        secret=SecretStr(HMAC_SECRET),
        client_cert=tmp_path / "market.crt",
        client_key=tmp_path / "market.key",
        client_ca=tmp_path / "ca.crt",
    )


def _quote_payload(*, as_of: datetime = NOW) -> bytes:
    return json.dumps(
        {
            "items": [
                {
                    "kind": "quote",
                    "instrument_code": "usAAPL",
                    "price": "227.1600",
                    "as_of": as_of.isoformat(),
                },
                {
                    "kind": "quote",
                    "instrument_code": "usMSFT",
                    "price": None,
                    "as_of": as_of.isoformat(),
                },
            ],
            "source_request_id": "synthetic-provider-request",
            "has_more": False,
            "next_cursor": None,
            "as_of": as_of.isoformat(),
            "quality": "PARTIAL",
        },
        separators=(",", ":"),
    ).encode()


def _bar_payload(
    *,
    interval: str = "day",
    as_of: datetime = NOW,
    timestamp: str = "2026-08-28T20:00:00+00:00",
) -> bytes:
    return json.dumps(
        {
            "items": [
                {
                    "kind": "bar",
                    "instrument_code": "usAAPL",
                    "interval": interval,
                    "timestamp": timestamp,
                    "open": "100.10",
                    "high": "103.20",
                    "low": "99.90",
                    "close": "102.30",
                    "volume": "123456",
                }
            ],
            "source_request_id": "synthetic-provider-request",
            "has_more": False,
            "next_cursor": None,
            "as_of": as_of.isoformat(),
            "quality": "PARTIAL",
        },
        separators=(",", ":"),
    ).encode()


@pytest.mark.asyncio
async def test_quotes_use_only_fixed_path_hmac_v2_and_preserve_exact_bytes(
    tmp_path: Path,
) -> None:
    payload = _quote_payload()

    async def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == (
            "https://fosun-gateway.internal:8443/internal/v1/read-only/fosun_get_quotes"
        )
        assert request.content == b'{"codes":["usAAPL","usMSFT"]}'
        _assert_signature(request, path=request.url.raw_path.decode())
        return httpx.Response(
            200,
            headers={"content-type": "application/json"},
            content=payload,
        )

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = FosunMarketGatewayClient(
        _identity(tmp_path),
        client=http_client,
        wall_time=lambda: NOW.timestamp(),
        now=lambda: NOW,
        uuid_factory=iter((NONCE, REQUEST_ID)).__next__,
    )

    response = await client.read_quotes_raw(("usAAPL", "usMSFT"))
    await http_client.aclose()

    assert response.raw_payload == payload
    assert response.operation == "fosun_get_quotes"
    assert response.gateway_request_id == REQUEST_ID
    assert response.received_at == NOW


@pytest.mark.asyncio
async def test_bars_use_only_fixed_path_and_exact_canonical_request(tmp_path: Path) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.raw_path.decode() == "/internal/v1/read-only/fosun_get_bars"
        assert json.loads(request.content) == {
            "code": "usAAPL",
            "end_time": "2026-08-29T14:31:00.000000Z",
            "ktype": "minute",
            "limit": 2,
            "start_time": "2026-08-29T14:29:00.000000Z",
        }
        return httpx.Response(
            200,
            headers={"content-type": "application/json"},
            content=_bar_payload(interval="minute"),
        )

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = FosunMarketGatewayClient(
        _identity(tmp_path),
        client=http_client,
        now=lambda: NOW,
        uuid_factory=iter((NONCE, REQUEST_ID)).__next__,
    )

    response = await client.read_bars_raw(
        code="usAAPL",
        ktype="minute",
        start_time=datetime(2026, 8, 29, 14, 29, tzinfo=UTC),
        end_time=datetime(2026, 8, 29, 14, 31, tzinfo=UTC),
        limit=2,
    )
    await http_client.aclose()

    assert response.operation == "fosun_get_bars"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "call",
    [
        lambda client: client.read_quotes_raw(("usMSFT", "usAAPL")),
        lambda client: client.read_quotes_raw(("usAAPL", "usAAPL")),
        lambda client: client.read_quotes_raw(("../AAPL",)),
        lambda client: client.read_bars_raw(
            code="usAAPL",
            ktype="minute",
            start_time=datetime(2026, 8, 29, 14, 31, tzinfo=UTC),
            end_time=datetime(2026, 8, 29, 14, 29, tzinfo=UTC),
            limit=2,
        ),
        lambda client: client.read_bars_raw(
            code="usAAPL",
            ktype="day",
            start_time=datetime(2026, 8, 1, tzinfo=UTC),
            end_time=datetime(2026, 8, 29, tzinfo=UTC),
            limit=True,
        ),
    ],
)
async def test_invalid_market_requests_fail_before_http(
    tmp_path: Path,
    call,
) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail(f"invalid request reached HTTP: {request.url}")

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = FosunMarketGatewayClient(_identity(tmp_path), client=http_client)

    with pytest.raises(FosunMarketGatewayError):
        await call(client)
    await http_client.aclose()


@pytest.mark.parametrize(
    "origin",
    (
        "http://fosun-gateway.internal:8443",
        "https://user@fosun-gateway.internal:8443",
        "https://fosun-gateway.internal:8443/proxy",
        "https://fosun-gateway.internal:8443?target=elsewhere",
    ),
)
def test_market_gateway_origin_is_exact_https_origin(tmp_path: Path, origin: str) -> None:
    with pytest.raises(ValueError, match="origin"):
        FosunMarketGatewayClient(
            _identity(tmp_path, origin=origin),
            client=cast(httpx.AsyncClient, object()),
        )


def test_default_transport_is_mtls_bounded_and_does_not_trust_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def capture_client(**kwargs: object) -> object:
        captured.update(kwargs)
        return object()

    ssl_context = object()
    monkeypatch.setattr("nexus.providers.market.fosun_gateway.httpx.AsyncClient", capture_client)
    monkeypatch.setattr(
        "nexus.providers.market.fosun_gateway._mtls_context",
        lambda *_args: ssl_context,
    )

    FosunMarketGatewayClient(_identity(tmp_path))

    assert captured == {
        "verify": ssl_context,
        "timeout": httpx.Timeout(10.0),
        "follow_redirects": False,
        "trust_env": False,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "content_type", "body"),
    [
        (307, "application/json", b"{}"),
        (200, "text/plain", b"{}"),
        (200, "application/json", b"x" * 1_048_577),
    ],
)
async def test_transport_rejects_redirect_media_type_and_oversize(
    tmp_path: Path,
    status: int,
    content_type: str,
    body: bytes,
) -> None:
    http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                status,
                headers={"content-type": content_type},
                content=body,
            )
        )
    )
    client = FosunMarketGatewayClient(_identity(tmp_path), client=http_client)

    with pytest.raises(FosunMarketGatewayError, match="response rejected"):
        await client.read_quotes_raw(("usAAPL",))
    await http_client.aclose()


@pytest.mark.asyncio
async def test_transport_enforces_one_absolute_response_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class SlowChunks(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"{"
            await asyncio.sleep(0.05)
            yield b"}"

    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "application/json"},
            stream=SlowChunks(),
        )

    monkeypatch.setattr(
        "nexus.providers.market.fosun_gateway._MAX_REQUEST_SECONDS",
        0.01,
        raising=False,
    )
    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = FosunMarketGatewayClient(_identity(tmp_path), client=http_client)

    with pytest.raises(FosunMarketGatewayError, match="request failed"):
        await client.read_quotes_raw(("usAAPL",))
    await http_client.aclose()


def test_quote_parser_preserves_decimal_and_accepts_bounded_gateway_receipt_time() -> None:
    raw = RawGatewayResponse(
        operation="fosun_get_quotes",
        gateway_request_id=REQUEST_ID,
        request_body_sha256="a" * 64,
        raw_payload=_quote_payload(),
        sent_at=NOW,
        received_at=NOW + timedelta(milliseconds=250),
    )

    page = parse_quote_page(raw, expected_codes=("usAAPL", "usMSFT"))

    assert str(page.items[0].price) == "227.1600"
    assert page.items[1].price is None
    with pytest.raises(FosunMarketGatewayError, match="contract rejected"):
        parse_quote_page(raw, expected_codes=("usAAPL",))


@pytest.mark.parametrize("operation", ("fosun_get_quotes", "fosun_get_bars"))
def test_market_parser_rejects_gateway_receipt_time_outside_hmac_window(
    operation: Literal["fosun_get_quotes", "fosun_get_bars"],
) -> None:
    gateway_as_of = NOW - timedelta(seconds=61)
    raw = RawGatewayResponse(
        operation=operation,
        gateway_request_id=REQUEST_ID,
        request_body_sha256="a" * 64,
        raw_payload=(
            _quote_payload(as_of=gateway_as_of)
            if operation == "fosun_get_quotes"
            else _bar_payload(as_of=gateway_as_of)
        ),
        sent_at=NOW,
        received_at=NOW + timedelta(seconds=1),
    )

    with pytest.raises(FosunMarketGatewayError, match="contract rejected"):
        if operation == "fosun_get_quotes":
            parse_quote_page(raw, expected_codes=("usAAPL", "usMSFT"))
        else:
            parse_bar_page(
                raw,
                expected_code="usAAPL",
                expected_ktype="day",
                start=datetime(2026, 8, 1, tzinfo=UTC),
                end=datetime(2026, 8, 29, tzinfo=UTC),
                limit=10,
            )


@pytest.mark.parametrize("delay", [True, False, None, "missing", 0, 1, "true", "false"])
def test_bar_parser_retains_strict_provider_delay_without_inference(delay) -> None:
    payload = json.loads(_bar_payload())
    if delay != "missing":
        payload["provider_reported_delay"] = delay
    raw = RawGatewayResponse(
        operation="fosun_get_bars",
        gateway_request_id=REQUEST_ID,
        request_body_sha256="a" * 64,
        raw_payload=json.dumps(payload).encode(),
        sent_at=NOW - timedelta(milliseconds=250),
        received_at=NOW,
    )

    def parse():
        return parse_bar_page(
            raw,
            expected_code="usAAPL",
            expected_ktype="day",
            start=datetime(2026, 8, 1, tzinfo=UTC),
            end=datetime(2026, 8, 29, tzinfo=UTC),
            limit=10,
        )

    if delay is None or type(delay) is bool or delay == "missing":
        page = parse()
        assert page.provider_reported_delay is (None if delay == "missing" else delay)
        assert page.quality == "PARTIAL"
    else:
        with pytest.raises(FosunMarketGatewayError):
            parse()


@pytest.mark.parametrize(
    "field,value",
    [
        ("provider_raw_turnover", True),
        ("provider_raw_turnover", -1),
        ("provider_raw_turnover", 1.5),
        ("provider_raw_turnover", "1"),
        ("provider_scale_power", True),
        ("provider_scale_power", -1),
        ("provider_scale_power", 13),
        ("provider_scale_power", 1.5),
    ],
)
def test_bar_parser_rejects_invalid_source_numeric_metadata(field, value):
    payload = json.loads(_bar_payload())
    target = payload["items"][0] if field == "provider_raw_turnover" else payload
    target[field] = value
    raw = RawGatewayResponse(
        operation="fosun_get_bars",
        gateway_request_id=REQUEST_ID,
        request_body_sha256="a" * 64,
        raw_payload=json.dumps(payload).encode(),
        sent_at=NOW - timedelta(milliseconds=250),
        received_at=NOW,
    )
    with pytest.raises(FosunMarketGatewayError):
        parse_bar_page(
            raw,
            expected_code="usAAPL",
            expected_ktype="day",
            start=datetime(2026, 8, 1, tzinfo=UTC),
            end=datetime(2026, 8, 29, tzinfo=UTC),
            limit=10,
        )


def test_daily_parser_uses_session_date_for_request_window() -> None:
    raw = RawGatewayResponse(
        operation="fosun_get_bars",
        gateway_request_id=REQUEST_ID,
        request_body_sha256="a" * 64,
        raw_payload=_bar_payload(timestamp="2026-08-21T00:00:00+00:00"),
        sent_at=NOW - timedelta(milliseconds=250),
        received_at=NOW,
    )

    page = parse_bar_page(
        raw,
        expected_code="usAAPL",
        expected_ktype="day",
        start=datetime(2026, 8, 21, 20, tzinfo=UTC),
        end=datetime(2026, 8, 22, 20, tzinfo=UTC),
        limit=10,
    )

    assert page.items[0].timestamp.date().isoformat() == "2026-08-21"


@pytest.mark.parametrize(
    "payload",
    (
        b'{"items":[],"items":[]}',
        b'{"items":[{"kind":"quote","instrument_code":"usAAPL","price":NaN,'
        b'"as_of":"2026-08-29T14:31:00Z"}],"source_request_id":"x",'
        b'"has_more":false,"next_cursor":null,"as_of":"2026-08-29T14:31:00Z",'
        b'"quality":"PARTIAL"}',
    ),
)
def test_quote_parser_rejects_duplicate_keys_and_json_constants(payload: bytes) -> None:
    raw = RawGatewayResponse(
        operation="fosun_get_quotes",
        gateway_request_id=REQUEST_ID,
        request_body_sha256="a" * 64,
        raw_payload=payload,
        sent_at=NOW - timedelta(milliseconds=250),
        received_at=NOW,
    )

    with pytest.raises(FosunMarketGatewayError, match="contract rejected"):
        parse_quote_page(raw, expected_codes=("usAAPL",))


def test_bar_parser_revalidates_operation_code_interval_window_and_ohlcv() -> None:
    raw = RawGatewayResponse(
        operation="fosun_get_bars",
        gateway_request_id=REQUEST_ID,
        request_body_sha256="a" * 64,
        raw_payload=_bar_payload(),
        sent_at=NOW,
        received_at=NOW + timedelta(milliseconds=250),
    )

    page = parse_bar_page(
        raw,
        expected_code="usAAPL",
        expected_ktype="day",
        start=datetime(2026, 8, 1, tzinfo=UTC),
        end=datetime(2026, 8, 29, tzinfo=UTC),
        limit=10,
    )

    assert page.items[0].close.as_tuple().exponent == -2
    with pytest.raises(FosunMarketGatewayError, match="contract rejected"):
        parse_bar_page(
            raw,
            expected_code="usMSFT",
            expected_ktype="day",
            start=datetime(2026, 8, 1, tzinfo=UTC),
            end=datetime(2026, 8, 29, tzinfo=UTC),
            limit=10,
        )


def _assert_signature(request: httpx.Request, *, path: str) -> None:
    body_hash = hashlib.sha256(request.content).hexdigest()
    canonical = "\n".join(
        (
            "hmac-v2",
            "POST",
            path,
            request.headers["X-Nexus-Timestamp"],
            request.headers["X-Nexus-Nonce"],
            "nexus-provider-worker",
            str(REQUEST_ID),
            body_hash,
        )
    )
    expected = (
        base64.urlsafe_b64encode(
            hmac.new(HMAC_SECRET.encode(), canonical.encode(), hashlib.sha256).digest()
        )
        .rstrip(b"=")
        .decode()
    )
    assert request.headers["X-Nexus-Signature-Version"] == "hmac-v2"
    assert request.headers["X-Nexus-Key-Id"] == "synthetic-market-current"
    assert request.headers["X-Nexus-Actor-Id"] == "nexus-provider-worker"
    assert request.headers["X-Request-Id"] == str(REQUEST_ID)
    assert request.headers["X-Nexus-Body-SHA256"] == body_hash
    assert request.headers["X-Nexus-Signature"] == expected
