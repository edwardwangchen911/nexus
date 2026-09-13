import ast
from pathlib import Path

PAPER_ROOT = Path(__file__).parents[2] / "src/nexus/paper"


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text())
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            names.add(node.module)
    return names


def test_agent_paper_modules_do_not_import_broker_order_code() -> None:
    imports = set().union(
        *(_imports(PAPER_ROOT / name) for name in ("agent_cycle.py", "agent_market.py"))
    )

    assert not {
        name
        for name in imports
        if name.startswith(("nexus.providers.broker", "nexus.broker_simulation"))
    }
