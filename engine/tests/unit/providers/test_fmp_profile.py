"""FMP profile parsing never promotes financial or sector-risk authority."""

import importlib
import logging
from decimal import Decimal

import httpx
import pytest
from pydantic import SecretStr


def provider():
    return importlib.import_module("nexus.providers.fmp_profile")


def test_profile_parser_preserves_raw_precision_and_optional_unknowns():
    result = provider().parse_profile(
        b'[{"symbol":"AAPL","companyName":"Synthetic","industry":null,'
        b'"price":1.123456789012345678901,"future":{"enabled":false}}]',
        "AAPL",
    )
    assert result["price"] == Decimal("1.123456789012345678901")
    assert result["industry"] is None and "sector" not in result
    assert result["future"] == {"enabled": False}


@pytest.mark.parametrize(
    "raw",
    [
        b"[]",
        b"{}",
        b'[{"symbol":"MSFT"}]',
        b'[{"symbol":"AAPL"},{"symbol":"AAPL"}]',
        b'[{"symbol":"AAPL","symbol":"AAPL"}]',
        b'[{"symbol":"AAPL","price":NaN}]',
        b'[{"symbol":"AAPL","industry":true}]',
        b'[{"symbol":"AAPL","cik":42}]',
    ],
)
def test_profile_requires_one_exact_identity_and_typed_fields(raw):
    with pytest.raises(ValueError, match="FMP_PROFILE_PAYLOAD_INVALID"):
        provider().parse_profile(raw, "AAPL")


@pytest.mark.parametrize("symbol", ["", "aapl", "AAPL,MSFT", "../AAPL", "AAPL?apikey=x"])
def test_profile_symbol_is_single_and_not_a_query(symbol):
    with pytest.raises(ValueError, match="FMP_PROFILE_SYMBOL_INVALID"):
        provider().validate_symbol(symbol)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [302, 401, 429, 500])
async def test_profile_transport_stops_at_first_error_without_secret_logs(status, caplog):
    calls = []

    def respond(request):
        calls.append(request)
        return httpx.Response(
            status, headers={"location": "https://example.com/"}, content=b"synthetic-secret"
        )

    with caplog.at_level(logging.DEBUG):
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            with pytest.raises(RuntimeError):
                await provider().fetch_profile(
                    client, symbol="AAPL", key=SecretStr("synthetic-secret")
                )
    assert len(calls) == 1
    assert str(calls[0].url) == "https://financialmodelingprep.com/stable/profile?symbol=AAPL"
    assert calls[0].headers["apikey"] == "synthetic-secret"
    assert "synthetic-secret" not in caplog.text and "apikey" not in caplog.text


@pytest.mark.asyncio
async def test_profile_success_uses_header_and_returns_exact_raw_without_secret_logs(caplog):
    raw = b'[{"symbol":"AAPL","price":1.234567890123456789}]'
    calls = []

    def respond(request):
        calls.append(request)
        return httpx.Response(200, content=raw)

    with caplog.at_level(logging.DEBUG):
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            result = await provider().fetch_profile(
                client, symbol="AAPL", key=SecretStr("synthetic-secret")
            )
    assert result == raw
    assert len(calls) == 1
    assert str(calls[0].url) == "https://financialmodelingprep.com/stable/profile?symbol=AAPL"
    assert calls[0].headers["apikey"] == "synthetic-secret"
    assert "synthetic-secret" not in caplog.text


@pytest.mark.asyncio
async def test_profile_transport_refuses_oversized_response():
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, content=b" " * (1024 * 1024 + 1)),
        )
    ) as client:
        with pytest.raises(RuntimeError, match="RESPONSE_TOO_LARGE"):
            await provider().fetch_profile(client, symbol="AAPL", key=SecretStr("synthetic-secret"))
