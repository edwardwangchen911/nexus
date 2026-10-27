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
MERGE_GROUPS = ("fast", "pg-a", "pg-b", "pg-c", "pg-d", "pg-e", "pg-f", "pg-g")


def test_merge_groups_cover_inventory_once_without_manual_suite() -> None:
    selected = [selector for group in MERGE_GROUPS for selector in selectors_for(group)]
    assert selected == list(MERGE_SELECTORS)
    assert len(selected) == len(set(selected))
    assert all(selectors_for(group) for group in MERGE_GROUPS)
    assert "engine/tests" not in selected


def test_workflow_runs_every_merge_group_and_keeps_modules_together() -> None:
    workflow = (ROOT / ".github/workflows/ci.yml").read_text()
    matrix = next(line for line in workflow.splitlines() if "group: ${{ fromJSON(" in line)
    assert json.loads(matrix.split(" || '", 1)[1].split("'", 1)[0]) == list(MERGE_GROUPS)
    assert "github.event_name == 'workflow_dispatch' && '[\"full\"]'" in matrix
    module_groups: dict[str, str] = {}
    for group in MERGE_GROUPS[1:]:
        for selector in selectors_for(group):
            module = selector.split("::", 1)[0]
            assert module_groups.setdefault(module, group) == group


@pytest.mark.parametrize("group", MERGE_GROUPS[1:])
def test_postgres_runner_selects_group_and_propagates_failure(tmp_path: Path, group: str) -> None:
    (tmp_path / "pytest.py").write_text(
        "import json, os, pathlib, sys\n"
        "pathlib.Path('args.json').write_text(json.dumps(sys.argv[1:]))\n"
        "pathlib.Path('group.txt').write_text(os.environ['ENGINE_TEST_GROUP'])\n"
        "raise SystemExit(1)\n"
    )
    result = subprocess.run(
        [sys.executable, str(RUNNER), group],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(tmp_path)},
        check=False,
    )
    assert (tmp_path / "args.json").exists(), "Runner must execute pytest"
    assert result.returncode == 1
    args = json.loads((tmp_path / "args.json").read_text())
    assert args[:5] == ["-q", "--import-mode=importlib", "-m", "not performance", "--durations=30"]
    assert "-vv" in args
    assert f"--junitxml=test-results/engine-{group}.xml" in args
    assert args[-len(selectors_for(group)) :] == list(selectors_for(group))
    assert (tmp_path / "group.txt").read_text() == group


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


@pytest.mark.parametrize("group", ("pg-b", "pg-e", "pg-f", "pg-g"))
def test_postgres_progress_survives_a_failed_test(tmp_path: Path, group: str) -> None:
    case = tmp_path / "test_progress_case.py"
    case.write_text("def test_passes():\n    assert True\n\ndef test_fails():\n    assert False\n")
    completed = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "scripts.ci.engine_test_progress", str(case)],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(ROOT), "ENGINE_TEST_GROUP": group},
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 1
    events = [
        json.loads(line)
        for line in (tmp_path / f"test-results/engine-{group}.jsonl").read_text().splitlines()
    ]
    assert [
        row["outcome"] for row in events if row["event"] == "report" and row["when"] == "call"
    ] == ["passed", "failed"]
    assert len([row for row in events if row["event"] == "start"]) == 2
    assert events[-1]["event"] == "finish" and events[-1]["exitstatus"] == 1
    assert all("longrepr" not in row and "environment" not in row for row in events)


def test_progress_keeps_one_output_when_a_test_changes_directory(tmp_path: Path) -> None:
    case = tmp_path / "test_directory_case.py"
    case.write_text(
        "def test_changes_directory(tmp_path, monkeypatch):\n"
        "    monkeypatch.chdir(tmp_path)\n"
        "    assert True\n"
    )
    completed = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "scripts.ci.engine_test_progress", str(case)],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(ROOT), "ENGINE_TEST_GROUP": "fast"},
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0
    events = [
        json.loads(line)
        for line in (tmp_path / "test-results/engine-fast.jsonl").read_text().splitlines()
    ]
    assert [row["when"] for row in events if row["event"] == "report"] == [
        "setup",
        "call",
        "teardown",
    ]
    assert events[-1]["event"] == "finish" and events[-1]["exitstatus"] == 0


@pytest.mark.parametrize("group", ("", "pg-h", "pg-gg", "../outside", "pg-g/../../outside"))
def test_progress_rejects_unknown_or_unsafe_group(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, group: str
) -> None:
    from scripts.ci import engine_test_progress

    output = tmp_path / "test-results"
    monkeypatch.setattr(engine_test_progress, "OUTPUT_DIRECTORY", output)
    monkeypatch.setenv("ENGINE_TEST_GROUP", group)
    engine_test_progress.pytest_runtest_logstart("test_example.py::test_passes", ("", None, ""))
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("group", ("pg-c", "pg-e", "pg-f", "pg-g"))
def test_postgres_progress_retains_last_started_case_after_cancellation(
    tmp_path: Path, group: str
) -> None:
    import time

    case = tmp_path / "test_cancel_case.py"
    case.write_text(
        "def test_00_completed():\n    assert True\n\n"
        "def test_01_inflight():\n    import threading\n    threading.Event().wait()\n"
    )
    process = subprocess.Popen(
        [sys.executable, "-m", "pytest", "-q", "-p", "scripts.ci.engine_test_progress", str(case)],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(ROOT), "ENGINE_TEST_GROUP": group},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    progress = tmp_path / f"test-results/engine-{group}.jsonl"
    events = []
    try:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if progress.exists():
                events = [
                    json.loads(line)
                    for line in progress.read_text().splitlines(keepends=True)
                    if line.endswith("\n")
                ]
                if any(
                    row["event"] == "start" and row["nodeid"].endswith("test_01_inflight")
                    for row in events
                ):
                    break
            assert process.poll() is None, "fixture exited before its in-flight marker"
            time.sleep(0.02)
        assert any(
            row["event"] == "start" and row["nodeid"].endswith("test_01_inflight") for row in events
        )
        process.terminate()
        process.wait(timeout=5)
        final = [json.loads(line) for line in progress.read_text().splitlines()]
        assert any(
            row["event"] == "report" and row["when"] == "call" and row["outcome"] == "passed"
            for row in final
        )
        assert not any(row["event"] == "finish" for row in final)
        assert not any(
            row["event"] == "report"
            and row["when"] == "call"
            and row["nodeid"].endswith("test_01_inflight")
            for row in final
        )
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)


def test_diagnostic_notification_delivery_regressions_are_in_merge_gate() -> None:
    selected = [selector for group in MERGE_GROUPS[1:] for selector in selectors_for(group)]
    for path in (
        "engine/tests/integration/test_bark_outbox_pg.py",
        "engine/tests/integration/test_outbox.py",
    ):
        assert selected.count(path) == 1
