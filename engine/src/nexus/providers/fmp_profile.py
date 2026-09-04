"""One fixed-origin FMP profile lookup; no market or risk authority inference."""

import json
import re
from decimal import Decimal
from typing import Any

import httpx
from pydantic import SecretStr

from nexus.providers.http import AllowedOrigin, FixedOriginHttpClient, HttpRequestPolicy

ORIGIN = "https://financialmodelingprep.com"
PATH = "/stable/profile"
MAX_RAW_BYTES = 1024 * 1024
PROFILE_FIELDS = ("companyName", "description", "cik", "isin", "cusip", "industry", "sector")


def validate_symbol(symbol: str) -> str:
    if re.fullmatch(r"[A-Z][A-Z0-9.-]{0,14}", symbol) is None:
        raise ValueError("FMP_PROFILE_SYMBOL_INVALID")
    return symbol


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def _invalid_constant(value: str) -> None:
    raise ValueError


def parse_profile(raw: bytes, symbol: str) -> dict[str, Any]:
    validate_symbol(symbol)
    try:
        if not 0 < len(raw) <= MAX_RAW_BYTES:
            raise ValueError
        result = json.loads(
            raw, parse_float=Decimal, parse_constant=_invalid_constant, object_pairs_hook=_unique
        )
        if not isinstance(result, list) or len(result) != 1 or not isinstance(result[0], dict):
            raise ValueError
        row = result[0]
        if row.get("symbol") != symbol:
            raise ValueError
        for field in PROFILE_FIELDS:
            value = row.get(field)
            if value is not None and (
                not isinstance(value, str)
                or len(value) > (16000 if field == "description" else 1000)
            ):
                raise ValueError
        return row
    except (ValueError, TypeError, RecursionError, UnicodeError) as error:
        raise ValueError("FMP_PROFILE_PAYLOAD_INVALID") from error


async def fetch_profile(client: httpx.AsyncClient, *, symbol: str, key: SecretStr) -> bytes:
    validate_symbol(symbol)
    secret = key.get_secret_value()
    if not secret or not secret.isascii() or any(not 33 <= ord(char) <= 126 for char in secret):
        raise ValueError("FMP_PROFILE_CREDENTIAL_REQUIRED")
    fixed = FixedOriginHttpClient(
        provider_name="fmp",
        origin=AllowedOrigin(scheme="https", host="financialmodelingprep.com"),
        policy=HttpRequestPolicy(max_attempts=1, max_response_bytes=MAX_RAW_BYTES),
        client=client,
    )
    return await fixed.get_bytes(
        ("stable", "profile"), (("symbol", symbol),), (("apikey", secret),)
    )
