"""Fixed read-only tool boundary for the Strategy R&D Agent."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from nexus.evolution.agent_tools import (
    EXPECTED_STRATEGY_RND_TOOL_NAMES,
    StrategyRNDToolContext,
    StrategyRNDToolError,
    StrategyRNDTools,
    strategy_rnd_function_tools,
)

NOW = datetime(2026, 8, 27, 12, tzinfo=UTC)
PARENT_ID = UUID("019d0000-0000-7000-8000-000000004001")
LINEAGE_ID = UUID("019d0000-0000-7000-8000-000000004002")
POLICY_ID = UUID("019d0000-0000-7000-8000-000000004003")
SUMMARY_ID = UUID("019d0000-0000-7000-8000-000000004004")
SEALED_ID = UUID("019d0000-0000-7000-8000-000000004005")
CURRENT_ID = UUID("019d0000-0000-7000-8000-000000004006")


def _context() -> StrategyRNDToolContext:
    return StrategyRNDToolContext(
        parent_strategy_definition_id=PARENT_ID,
        champion_generation=1,
        research_lineage_id=LINEAGE_ID,
        evolution_policy_id=POLICY_ID,
        training_validation_summary_id=SUMMARY_ID,
        as_of=NOW,
        sealed_experiment_ids=(SEALED_ID,),
        current_family_experiment_ids=(CURRENT_ID,),
    )


def _parent_result() -> dict[str, object]:
    return {
        "strategy_definition_id": str(PARENT_ID),
        "strategy_version": "baseline_v1",
        "expression_hash": "a" * 64,
        "registered_parameters": {
            "min_adv20_usd": "50000000",
            "min_close": "5",
            "target_weight": "0.125",
        },
        "ranking_feature": "momentum_126_skip5",
        "positive_return_filter_enabled": True,
    }


def test_catalogue_contains_only_four_read_only_strategy_tools() -> None:
    tools = StrategyRNDTools(context=_context())

    assert tools.names == EXPECTED_STRATEGY_RND_TOOL_NAMES
    assert tuple(strategy_rnd_function_tools()) == EXPECTED_STRATEGY_RND_TOOL_NAMES
    assert all(
        definition["type"] == "function"
        and definition["strict"] is True
        and definition["name"] == name
        for name, definition in strategy_rnd_function_tools().items()
    )
    assert not any("broker" in name or "order" in name for name in tools.names)


@pytest.mark.parametrize(
    "payload",
    (
        {
            "strategy_definition_id": PARENT_ID,
            "champion_generation": 1,
            "as_of": NOW + timedelta(seconds=1),
        },
        {
            "strategy_definition_id": PARENT_ID,
            "champion_generation": 1,
            "as_of": NOW,
            "broker_account_id": "forbidden",
        },
        {
            "strategy_definition_id": PARENT_ID,
            "champion_generation": 1,
            "as_of": NOW,
            "url": "https://untrusted.invalid",
        },
        {
            "strategy_definition_id": PARENT_ID,
            "champion_generation": 1,
            "as_of": NOW,
            "python": "exec('forbidden')",
        },
    ),
)
def test_parent_tool_rejects_nonfrozen_or_sensitive_payload_before_executor(
    payload: dict[str, object],
) -> None:
    calls = 0

    def executor(_: dict[str, object]) -> dict[str, object]:
        nonlocal calls
        calls += 1
        return _parent_result()

    tools = StrategyRNDTools(
        context=_context(), executors={"get_strategy_parent": executor}
    )

    with pytest.raises(StrategyRNDToolError):
        tools.validate("get_strategy_parent", payload)
    assert calls == 0


@pytest.mark.asyncio
async def test_tool_executes_only_after_exact_context_and_output_validation() -> None:
    observed: list[dict[str, object]] = []

    def executor(payload: dict[str, object]) -> dict[str, object]:
        observed.append(payload)
        return _parent_result()

    tools = StrategyRNDTools(
        context=_context(), executors={"get_strategy_parent": executor}
    )
    result = await tools.execute(
        "get_strategy_parent",
        {
            "strategy_definition_id": str(PARENT_ID),
            "champion_generation": 1,
            "as_of": NOW,
        },
    )

    assert result["strategy_version"] == "baseline_v1"
    assert observed == [
        {
            "strategy_definition_id": PARENT_ID,
            "champion_generation": 1,
            "as_of": NOW,
        }
    ]


def test_current_family_oos_and_unsealed_experiments_are_never_queryable() -> None:
    tools = StrategyRNDTools(context=_context())

    for experiment_id in (CURRENT_ID, UUID("019d0000-0000-7000-8000-000000004099")):
        with pytest.raises(StrategyRNDToolError, match="sealed experiment"):
            tools.validate(
                "get_sealed_experiment_summary",
                {"experiment_id": experiment_id, "as_of": NOW},
            )


@pytest.mark.asyncio
async def test_untyped_or_sensitive_tool_result_is_rejected() -> None:
    tools = StrategyRNDTools(
        context=_context(),
        executors={
            "get_strategy_parent": lambda _: {
                **_parent_result(),
                "broker_order": "must not cross the boundary",
            }
        },
    )

    with pytest.raises(StrategyRNDToolError, match="result"):
        await tools.execute(
            "get_strategy_parent",
            {
                "strategy_definition_id": PARENT_ID,
                "champion_generation": 1,
                "as_of": NOW,
            },
        )
