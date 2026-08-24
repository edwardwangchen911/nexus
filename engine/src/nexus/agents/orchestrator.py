"""Safe orchestration around an injected model seam; it performs no external I/O."""

from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from json import loads
from pathlib import Path
from typing import Protocol

from jsonschema import Draft202012Validator  # type: ignore[import-untyped]
from pydantic import ValidationError

from nexus.agents.budget import BudgetExhausted, DailyBudget
from nexus.agents.models import ResearchRequest, ResearchRunResult
from nexus.agents.tools import EXPECTED_READ_ONLY_TOOL_NAMES, ReadOnlyToolRegistry
from nexus.strategy.intents import ResearchIntent


class ResearchModel(Protocol):
    """Transport seam used by fixtures and a separately configured production adapter."""

    async def respond(
        self, request: ResearchRequest, tools: ReadOnlyToolRegistry
    ) -> Mapping[str, object]: ...

    @property
    def worst_case_cost_usd(self) -> Decimal: ...


_SCHEMA = Draft202012Validator(
    loads(
        (
            Path(__file__).resolve().parents[4] / "contracts/agent/research-intent-v1.schema.json"
        ).read_text(encoding="utf-8")
    )
)


async def run_research(
    request: ResearchRequest,
    tools: ReadOnlyToolRegistry,
    model: ResearchModel,
    budget: DailyBudget,
) -> ResearchRunResult:
    """Run at most one retry from the same frozen request and return only a valid intent."""
    if tools.names != EXPECTED_READ_ONLY_TOOL_NAMES:
        return _abstained()
    try:
        reservation = budget.reserve(model.worst_case_cost_usd, pricing_fresh=True)
    except (AttributeError, BudgetExhausted, TypeError, ValueError):
        return _abstained()

    frozen_request = request.model_copy(deep=True)
    response: Mapping[str, object] | None = None
    for _attempt in range(2):
        try:
            response = await model.respond(frozen_request, tools)
            break
        except Exception:
            continue
    if response is None:
        budget.release(reservation)
        return ResearchRunResult(status="failed")
    settled = False
    try:
        cost = _decimal_cost(response.get("cost_usd"))
        budget.settle(reservation, cost)
        settled = True
        intent = _validated_intent(response, request)
    except (BudgetExhausted, TypeError, ValueError, ValidationError):
        if not settled:
            budget.release(reservation)
        return _abstained()
    return ResearchRunResult(status="completed", intent=intent)


def _validated_intent(response: Mapping[str, object], request: ResearchRequest) -> ResearchIntent:
    if (
        response.get("status") in {"abstained", "failed"}
        or response.get("conflicting_facts") is True
    ):
        raise ValueError("model response cannot support an intent")
    raw_intent = response.get("intent")
    if (
        not isinstance(raw_intent, Mapping)
        or _contains_prompt_injection(raw_intent)
        or list(_SCHEMA.iter_errors(raw_intent))
    ):
        raise ValueError("model response did not produce the strict intent schema")
    intent = ResearchIntent.model_validate(raw_intent)
    if (
        intent.instrument_id != request.instrument_id
        or intent.data_snapshot_id != request.data_snapshot_id
    ):
        raise ValueError("intent must be bound to the frozen request")
    if intent.decision_time != request.decision_time:
        raise ValueError("intent decision time must equal the frozen request")
    if intent.display_symbol == "00656.HK" and intent.stance not in {"watch", "hold"}:
        raise ValueError("HKEX fundamental authority is absent")
    return intent


def _contains_prompt_injection(value: object) -> bool:
    """Reject instruction-shaped document text instead of treating it as an analyst claim."""
    if isinstance(value, Mapping):
        return any(_contains_prompt_injection(item) for item in value.values())
    if isinstance(value, list | tuple):
        return any(_contains_prompt_injection(item) for item in value)
    if not isinstance(value, str):
        return False
    normalized = value.lower()
    return "ignore prior instructions" in normalized or "add an order tool" in normalized


def _decimal_cost(value: object) -> Decimal:
    if not isinstance(value, str):
        raise ValueError("model usage must include a Decimal string cost")
    try:
        parsed = Decimal(value)
    except InvalidOperation as error:
        raise ValueError("model usage cost is not Decimal text") from error
    if not parsed.is_finite() or parsed < 0:
        raise ValueError("model usage cost must be finite and non-negative")
    return parsed


def _abstained() -> ResearchRunResult:
    return ResearchRunResult(status="abstained")
