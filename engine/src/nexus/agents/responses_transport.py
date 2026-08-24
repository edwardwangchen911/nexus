"""Strict normalization for official Responses SDK objects and the test seam."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

from openai.types.responses.response import Response
from openai.types.responses.response_function_tool_call import ResponseFunctionToolCall
from openai.types.responses.response_output_message import ResponseOutputMessage
from openai.types.responses.response_output_text import ResponseOutputText
from openai.types.responses.response_reasoning_item import ResponseReasoningItem


@dataclass(frozen=True)
class NormalizedUsage:
    input_tokens: int
    output_tokens: int


@dataclass(frozen=True)
class NormalizedResponse:
    output: tuple[Mapping[str, object], ...]
    output_text: str | None
    usage: NormalizedUsage


class ResponseShapeError(ValueError):
    """A provider response failed the allowlisted shape after optional usage was read."""

    def __init__(self, message: str, *, usage: NormalizedUsage | None = None) -> None:
        super().__init__(message)
        self.usage = usage


def normalize_response(value: object) -> NormalizedResponse:
    """Copy only trusted fields from an official object or the strict mapping seam."""
    if isinstance(value, Response):
        return _normalize_typed(value)
    if isinstance(value, Mapping):
        return _normalize_mapping(value)
    raise ResponseShapeError("Responses result has an unsupported transport type")


def _normalize_typed(value: Response) -> NormalizedResponse:
    usage = _typed_usage(value)
    if value.error is not None or value.status != "completed":
        raise ResponseShapeError("Responses result is not completed", usage=usage)
    output: list[Mapping[str, object]] = []
    text_parts: list[str] = []
    for item in value.output:
        if isinstance(item, ResponseReasoningItem):
            if item.status not in (None, "completed"):
                raise ResponseShapeError("Responses reasoning item is incomplete", usage=usage)
            continue
        if isinstance(item, ResponseFunctionToolCall):
            if (
                item.status != "completed"
                or not item.call_id
                or not item.name
                or not isinstance(item.arguments, str)
            ):
                raise ResponseShapeError("Responses function call is malformed", usage=usage)
            output.append(
                {
                    "type": "function_call",
                    "call_id": item.call_id,
                    "name": item.name,
                    "arguments": item.arguments,
                }
            )
            continue
        if not isinstance(item, ResponseOutputMessage) or item.status != "completed":
            raise ResponseShapeError("Responses output item is not allowlisted", usage=usage)
        output.append({"type": "message"})
        for content in item.content:
            if not isinstance(content, ResponseOutputText):
                raise ResponseShapeError("Responses message content is not text", usage=usage)
            text_parts.append(content.text)
    return NormalizedResponse(
        output=tuple(output),
        output_text="".join(text_parts) if text_parts else None,
        usage=usage,
    )


def _typed_usage(value: Response) -> NormalizedUsage:
    usage = value.usage
    if usage is None:
        raise ResponseShapeError("Responses usage is required")
    return NormalizedUsage(
        input_tokens=_token_count(usage.input_tokens),
        output_tokens=_token_count(usage.output_tokens),
    )


def _normalize_mapping(value: Mapping[object, object]) -> NormalizedResponse:
    allowed = {"output", "output_text", "usage"}
    if not all(isinstance(key, str) for key in value) or set(value) - allowed:
        raise ResponseShapeError("Responses mapping contains untrusted fields")
    raw_usage = value.get("usage")
    if not isinstance(raw_usage, Mapping) or set(raw_usage) != {
        "input_tokens",
        "output_tokens",
    }:
        raise ResponseShapeError("Responses usage is required")
    usage = NormalizedUsage(
        input_tokens=_token_count(raw_usage.get("input_tokens")),
        output_tokens=_token_count(raw_usage.get("output_tokens")),
    )
    raw_output = value.get("output")
    if not isinstance(raw_output, list):
        raise ResponseShapeError("Responses output items are required", usage=usage)
    output: list[Mapping[str, object]] = []
    for raw_item in raw_output:
        if not isinstance(raw_item, Mapping):
            raise ResponseShapeError("Responses output item is malformed", usage=usage)
        item = cast(Mapping[object, object], raw_item)
        item_type = item.get("type")
        if item_type == "message" and set(item) == {"type"}:
            output.append({"type": "message"})
            continue
        expected = {"type", "call_id", "name", "arguments"}
        if item_type != "function_call" or set(item) != expected:
            raise ResponseShapeError("Responses output item is not allowlisted", usage=usage)
        call_id, name, arguments = item.get("call_id"), item.get("name"), item.get("arguments")
        if not all(isinstance(field, str) and field for field in (call_id, name, arguments)):
            raise ResponseShapeError("Responses function call is malformed", usage=usage)
        output.append(
            {
                "type": "function_call",
                "call_id": cast(str, call_id),
                "name": cast(str, name),
                "arguments": cast(str, arguments),
            }
        )
    raw_text = value.get("output_text")
    if raw_text is not None and not isinstance(raw_text, str):
        raise ResponseShapeError("Responses structured output text is malformed", usage=usage)
    return NormalizedResponse(output=tuple(output), output_text=raw_text, usage=usage)


def _token_count(value: object) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ResponseShapeError("Responses token usage must be a non-negative integer")
    return value
