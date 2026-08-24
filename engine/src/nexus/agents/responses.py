"""Official Responses API adapter with an injected client seam for fixture-only tests."""

from collections.abc import Mapping
from decimal import Decimal
from typing import Protocol, cast

from nexus.agents.models import ResearchRequest
from nexus.agents.tools import ReadOnlyToolRegistry, response_function_tools


class _ResponsesClient(Protocol):
    responses: object


class ResponsesResearchModel:
    """Use only strict JSON schema output and fixed custom function definitions."""

    def __init__(
        self,
        *,
        client: _ResponsesClient,
        model_id: str,
        timeout_seconds: int,
        input_token_usd: Decimal = Decimal("0.001"),
        output_token_usd: Decimal = Decimal("0.003"),
    ) -> None:
        if not model_id or timeout_seconds <= 0:
            raise ValueError("configured model_id and absolute timeout are required")
        self._client = client
        self._model_id = model_id
        self._timeout_seconds = timeout_seconds
        self._input_token_usd = input_token_usd
        self._output_token_usd = output_token_usd

    async def respond(
        self, request: ResearchRequest, tools: ReadOnlyToolRegistry
    ) -> Mapping[str, object]:
        if tuple(tools.names) != tuple(name for name in response_function_tools()):
            raise ValueError("fixed tool registry required")
        responses = getattr(self._client, "responses")
        create = getattr(responses, "create")
        result = await create(
            model=self._model_id,
            timeout=self._timeout_seconds,
            input=[{"role": "user", "content": request.model_dump(mode="json")}],
            tools=list(response_function_tools().values()),
            text={
                "format": {
                    "type": "json_schema",
                    "name": "research_intent_v1",
                    "strict": True,
                    "schema": _intent_schema(),
                }
            },
        )
        if not isinstance(result, Mapping):
            raise ValueError("Responses result must be a mapping at the transport seam")
        usage = result.get("usage")
        if not isinstance(usage, Mapping):
            raise ValueError("Responses usage is required")
        input_tokens = _token_count(usage.get("input_tokens"))
        output_tokens = _token_count(usage.get("output_tokens"))
        return {
            "intent": {},
            "cost_usd": format(
                Decimal(input_tokens) * self._input_token_usd
                + Decimal(output_tokens) * self._output_token_usd,
                "f",
            ),
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
        }


def _token_count(value: object) -> int:
    if not isinstance(value, int) or value < 0:
        raise ValueError("Responses token usage must be a non-negative integer")
    return value


def _intent_schema() -> Mapping[str, object]:
    from json import loads
    from pathlib import Path

    return cast(Mapping[str, object], loads(
        (Path(__file__).resolve().parents[4] / "contracts/agent/research-intent-v1.schema.json")
        .read_text(encoding="utf-8")
    ))
