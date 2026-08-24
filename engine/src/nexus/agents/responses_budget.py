"""Pre-call aggregate allowance shared by an entire durable research run."""

import json
from collections.abc import Mapping
from dataclasses import dataclass

from nexus.agents.responses_transport import NormalizedUsage


@dataclass(frozen=True)
class UsageSnapshot:
    input_tokens: int = 0
    output_tokens: int = 0
    tool_calls: int = 0
    provider_calls: int = 0


@dataclass(frozen=True)
class CallAllowance:
    input_token_ceiling: int
    max_output_tokens: int


class RunUsageAllowance:
    """Authorize each provider call without exceeding one reserved run maximum."""

    def __init__(
        self, *, max_input_tokens: int, max_output_tokens: int, max_tool_calls: int
    ) -> None:
        self._max_input_tokens = max_input_tokens
        self._max_output_tokens = max_output_tokens
        self._max_tool_calls = max_tool_calls
        self._max_provider_calls = 9 + max_tool_calls
        self._usage = UsageSnapshot()

    @property
    def usage(self) -> UsageSnapshot:
        return self._usage

    def authorize(self, request: Mapping[str, object]) -> CallAllowance:
        if self._usage.provider_calls >= self._max_provider_calls:
            raise ValueError("provider call allowance exhausted")
        remaining_output = self._max_output_tokens - self._usage.output_tokens
        if remaining_output <= 0:
            raise ValueError("output allowance exhausted before provider call")
        input_ceiling = len(
            json.dumps(request, sort_keys=True, separators=(",", ":"), default=str).encode()
        )
        if self._usage.input_tokens + input_ceiling > self._max_input_tokens:
            raise ValueError("input allowance exhausted before provider call")
        self._usage = UsageSnapshot(
            input_tokens=self._usage.input_tokens,
            output_tokens=self._usage.output_tokens,
            tool_calls=self._usage.tool_calls,
            provider_calls=self._usage.provider_calls + 1,
        )
        return CallAllowance(
            input_token_ceiling=input_ceiling,
            max_output_tokens=remaining_output,
        )

    def record_usage(self, allowance: CallAllowance, usage: NormalizedUsage) -> None:
        self._usage = UsageSnapshot(
            input_tokens=self._usage.input_tokens + usage.input_tokens,
            output_tokens=self._usage.output_tokens + usage.output_tokens,
            tool_calls=self._usage.tool_calls,
            provider_calls=self._usage.provider_calls,
        )
        if usage.input_tokens > allowance.input_token_ceiling:
            raise ValueError("provider input usage exceeds pre-call ceiling")
        if usage.output_tokens > allowance.max_output_tokens:
            raise ValueError("aggregate output token limit exceeded")
        if self._usage.input_tokens > self._max_input_tokens:
            raise ValueError("aggregate input token limit exceeded")
        if self._usage.output_tokens > self._max_output_tokens:
            raise ValueError("aggregate output token limit exceeded")

    def record_tool_calls(self, count: int) -> None:
        self._usage = UsageSnapshot(
            input_tokens=self._usage.input_tokens,
            output_tokens=self._usage.output_tokens,
            tool_calls=self._usage.tool_calls + count,
            provider_calls=self._usage.provider_calls,
        )
        if self._usage.tool_calls > self._max_tool_calls:
            raise ValueError("aggregate tool call limit exceeded")
