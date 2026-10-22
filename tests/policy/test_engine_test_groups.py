"""CI must preserve pytest selection and failures when distributing Engine work."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.ci.engine_test_groups import MERGE_SELECTORS, selectors_for

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "scripts/ci/engine_test_groups.py"


def test_merge_groups_cover_inventory_once_without_manual_suite() -> None:
    selected = [
        selector
        for group in ("fast", "pg-a", "pg-b")
        for selector in selectors_for(group)
    ]
    assert selected == list(MERGE_SELECTORS)
    assert len(selected) == len(set(selected))
    assert all(selectors_for(group) for group in ("fast", "pg-a", "pg-b"))
    assert "engine/tests" not in selected


@pytest.mark.parametrize("exit_code", [0, 1, 2, 5])
def test_runner_preserves_pytest_exit_code(tmp_path: Path, exit_code: int) -> None:
    # A subprocess double avoids executing thousands of tests inside policy CI.
    (tmp_path / "pytest.py").write_text(
        "import json, pathlib, sys\n"
        "pathlib.Path('args.json').write_text(json.dumps(sys.argv[1:]))\n"
        f"raise SystemExit({exit_code})\n"
    )
    result = subprocess.run(
        [sys.executable, str(RUNNER), "fast"],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(tmp_path)},
        check=False,
    )
    assert (tmp_path / "args.json").exists(), "Runner must execute pytest"
    assert result.returncode == exit_code
    args = json.loads((tmp_path / "args.json").read_text())
    assert args[:5] == ["-q", "--import-mode=importlib", "-m", "not performance", "--durations=30"]
    assert args[-5:] == [
        "engine/tests/unit",
        "engine/tests/contract",
        "engine/tests/evals",
        "engine/tests/property",
        "engine/tests/test_health.py",
    ]


def test_manual_full_suite_keeps_performance_tests(tmp_path: Path) -> None:
    (tmp_path / "pytest.py").write_text(
        "import json, pathlib, sys\n"
        "pathlib.Path('args.json').write_text(json.dumps(sys.argv[1:]))\n"
    )
    result = subprocess.run(
        [sys.executable, str(RUNNER), "full"],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(tmp_path)},
        check=False,
    )
    assert result.returncode == 0
    args = json.loads((tmp_path / "args.json").read_text())
    assert "-m" not in args
    assert args[-1] == "engine/tests"


@pytest.mark.parametrize("result", ["success", "failure", "cancelled", "skipped", ""])
def test_aggregate_job_fails_closed(result: str) -> None:
    workflow = (ROOT / ".github/workflows/ci.yml").read_text()
    block = workflow.split("  engine-postgres:\n", 1)[1].split("\n  e2e:", 1)[0]
    assert "needs: engine-tests" in block
    assert "if: always()" in block
    script = block.split("run: |\n", 1)[1]
    script = "\n".join(line[10:] for line in script.splitlines())
    completed = subprocess.run(
        ["bash", "-e", "-c", script],
        env={**os.environ, "ENGINE_RESULT": result},
        check=False,
    )
    assert (completed.returncode == 0) is (result == "success")
