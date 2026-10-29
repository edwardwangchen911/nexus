"""Bounded allowlisted diagnostics; never retain exception prose or error bodies."""

import json
from typing import cast, get_args

from openai_codex.errors import JsonRpcError

from nexus.agents.committee import SdkProviderErrorCode


def rpc_error_code(error: BaseException) -> int | None:
    if isinstance(error, JsonRpcError):
        code = error.code
        if type(code) is int and -32768 <= code <= -32000:
            return code
    return None


def provider_error_code(message: str) -> SdkProviderErrorCode | None:
    """Only a complete JSON error envelope, never a code quoted inside prose.

    The allowlist is the pinned OpenAI ResponseError code vocabulary. Unknown,
    malformed, duplicated or oversized error bodies remain unclassified.
    """
    if len(message) > 8192:
        return None

    def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("DUPLICATE_DIAGNOSTIC_KEY")
            result[key] = value
        return result

    def reject_constant(value: str) -> None:
        raise ValueError("INVALID_DIAGNOSTIC_JSON_CONSTANT")

    try:
        if len(message.encode("utf-8")) > 8192:
            return None
        body = json.loads(message, object_pairs_hook=unique_object, parse_constant=reject_constant)
    except (ValueError, RecursionError, UnicodeError):
        return None
    if not isinstance(body, dict):
        return None
    error = body.get("error", body)
    if not isinstance(error, dict):
        return None
    code = error.get("code")
    if type(code) is str and code in get_args(SdkProviderErrorCode.__value__):
        return cast(SdkProviderErrorCode, code)
    return None


def typed_http_status(info: object) -> int | None:
    """Read only the pinned SDK's typed transport-error status fields."""
    for field in (
        "http_connection_failed",
        "response_stream_connection_failed",
        "response_stream_disconnected",
        "response_too_many_failed_attempts",
    ):
        details = getattr(info, field, None)
        status = getattr(details, "http_status_code", None)
        if type(status) is int and 100 <= status <= 599:
            return status
    return None
