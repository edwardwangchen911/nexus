"""Strict public SEC identity mappings shared by collection and frozen readback."""

import json
from typing import Any, NoReturn

_EXCHANGES = {"Nasdaq": "XNAS", "NYSE": "XNYS", "NYSE American": "XASE"}


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("IDENTITY_COLLECTION_JSON_INVALID")
        result[key] = value
    return result


def _constant(value: str) -> NoReturn:
    raise ValueError("IDENTITY_COLLECTION_JSON_INVALID")


def parse_identity_source(raw: bytes) -> dict[str, Any]:
    value = json.loads(raw, object_pairs_hook=_unique, parse_constant=_constant)
    if not isinstance(value, dict):
        raise ValueError("IDENTITY_COLLECTION_JSON_INVALID")
    return value


def directory_binding(directory: dict[str, Any], symbol: str) -> tuple[str, str]:
    if directory.get("fields") != ["cik", "name", "ticker", "exchange"]:
        raise ValueError("IDENTITY_COLLECTION_MAPPING_INVALID")
    rows = directory.get("data")
    if not isinstance(rows, list) or any(not isinstance(r, list) or len(r) != 4 for r in rows):
        raise ValueError("IDENTITY_COLLECTION_MAPPING_INVALID")
    matches = [r for r in rows if r[2] == symbol]
    if len(matches) != 1:
        raise ValueError("IDENTITY_COLLECTION_MAPPING_INVALID")
    cik, _, _, exchange = matches[0]
    if type(cik) is not int or not 0 < cik < 10**10 or exchange not in _EXCHANGES:
        raise ValueError("IDENTITY_COLLECTION_MAPPING_INVALID")
    return str(cik).zfill(10), _EXCHANGES[exchange]


def require_submissions_binding(
    document: dict[str, Any], symbol: str, cik: str, exchange: str
) -> None:
    issuer = document.get("cik")
    if type(issuer) not in (str, int) or not str(issuer).isascii() or not str(issuer).isdigit():
        raise ValueError("IDENTITY_COLLECTION_ISSUER_INVALID")
    tickers, exchanges = document.get("tickers"), document.get("exchanges")
    if (
        str(issuer).zfill(10) != cik
        or not isinstance(tickers, list)
        or not isinstance(exchanges, list)
        or len(tickers) != len(exchanges)
        or not all(isinstance(v, str) for v in tickers + exchanges)
    ):
        raise ValueError("IDENTITY_COLLECTION_ISSUER_INVALID")
    matches = [
        (t, _EXCHANGES.get(e)) for t, e in zip(tickers, exchanges, strict=True) if t == symbol
    ]
    if matches != [(symbol, exchange)]:
        raise ValueError("IDENTITY_COLLECTION_ISSUER_INVALID")
