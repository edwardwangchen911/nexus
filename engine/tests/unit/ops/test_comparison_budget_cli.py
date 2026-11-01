"""Only the known safe round-capacity reason crosses the CLI boundary."""

import json
from uuid import uuid4

import pytest
from nexus.agents.budget import BudgetExhausted
from nexus.ops import research_comparison as cli


@pytest.mark.parametrize(
    ("detail", "expected"),
    [
        ("COMPARISON_ROUND_BUDGET_EXHAUSTED", "COMPARISON_ROUND_BUDGET_EXHAUSTED"),
        ("private budget diagnostic", "COMPARISON_BUDGET_EXHAUSTED"),
    ],
)
def test_normal_admission_reports_safe_capacity_reason_without_retry(
    monkeypatch, capsys, detail, expected
):
    calls = 0

    async def reject(args):
        nonlocal calls
        calls += 1
        raise BudgetExhausted(detail)

    monkeypatch.setattr(cli, "native_command", reject)
    monkeypatch.setattr(
        "sys.argv",
        [
            "research-comparison",
            "admit-position",
            "--repo-root",
            ".",
            "--position",
            "1",
            "--source",
            "source",
            "--prepared",
            "prepared",
            "--manifest",
            "manifest",
            "--model-config-id",
            str(uuid4()),
        ],
    )
    assert cli.main() == 1
    assert calls == 1
    assert json.loads(capsys.readouterr().out) == {"status": "rejected", "reason": expected}
