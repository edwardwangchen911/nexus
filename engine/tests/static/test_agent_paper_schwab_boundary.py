"""Static safety boundaries for the Schwab Agent paper chain."""

import ast
from pathlib import Path

import pytest
from nexus.api.schemas.paper import AgentPaperCycleResponse
from nexus.settings import EngineSettings
from pydantic import ValidationError

ENGINE_ROOT = Path(__file__).parents[2]
CHAIN_FILES = (
    ENGINE_ROOT / "src/nexus/ops/agent_paper_once.py",
    ENGINE_ROOT / "src/nexus/ops/schwab_market_stream.py",
    ENGINE_ROOT / "src/nexus/paper/agent_cycle.py",
    ENGINE_ROOT / "src/nexus/paper/agent_market.py",
    ENGINE_ROOT / "src/nexus/paper/execution_service.py",
    ENGINE_ROOT / "src/nexus/paper/read_service.py",
    ENGINE_ROOT / "src/nexus/providers/market/schwab_price_history.py",
)
FORBIDDEN_IMPORT_PREFIXES = (
    "nexus.providers.trading",
    "nexus.providers.broker",
    "nexus.broker_simulation",
    "nexus.shadow",
    "nexus.evolution.shadow",
)


def _tree(path: Path) -> ast.AST:
    return ast.parse(path.read_text(), filename=str(path))


def _imports(tree: ast.AST) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            names.add(node.module)
    return names


def test_schwab_agent_paper_chain_has_no_trading_or_shadow_dependency() -> None:
    """A new paper dependency must not cross into broker orders or either Shadow domain."""
    imports = set().union(*(_imports(_tree(path)) for path in CHAIN_FILES))

    assert not {
        name
        for name in imports
        if name.startswith(FORBIDDEN_IMPORT_PREFIXES)
    }

    string_literals = {
        node.value
        for path in CHAIN_FILES
        for node in ast.walk(_tree(path))
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }
    assert not {
        value
        for value in string_literals
        if "trader/v1" in value.lower() or "/orders" in value.lower()
    }


def test_schwab_agent_paper_http_policies_are_single_attempt() -> None:
    """A retry-policy edit must not multiply one manual Agent paper acquisition."""
    policy_calls: list[ast.Call] = []
    for path in CHAIN_FILES:
        policy_calls.extend(
            node
            for node in ast.walk(_tree(path))
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "HttpRequestPolicy"
        )

    assert policy_calls
    assert all(
        any(
            keyword.arg == "max_attempts"
            and isinstance(keyword.value, ast.Constant)
            and keyword.value.value == 1
            for keyword in call.keywords
        )
        for call in policy_calls
    )


def test_agent_paper_is_default_off_and_readback_cannot_unlock_execution() -> None:
    """Configuration omission or API input must never enable execution authority."""
    assert EngineSettings(_env_file=None).agent_paper_cycle_enabled is False

    with pytest.raises(ValidationError):
        AgentPaperCycleResponse.model_validate(
            {
                "cycle_id": "00000000-0000-0000-0000-000000000001",
                "status": "completed",
                "result_code": "NO_ORDER_REQUIRED",
                "decision_session": "2026-09-14",
                "decision_time": "2026-09-14T12:00:00Z",
                "decision_slot_at": "2026-09-14T12:00:00Z",
                "execution_session": "2026-09-14",
                "execution_locked": False,
                "candidate_count": 0,
                "candidates": [],
                "decision": None,
                "decision_market": None,
                "execution_market": {
                    "source": "schwab",
                    "feed": "price_history",
                    "delivery_mode": "unknown",
                    "window_minutes": 5,
                    "status": "pending",
                },
                "order_ids": [],
            }
        )
