# ruff: noqa: E501
"""Strategy-target authority is deterministic and fail closed."""

from decimal import Decimal
from uuid import UUID

import pytest
from nexus.optimizer.strategy_target import DeterministicSelectionAuthority, build_strategy_target


def test_strategy_target_requires_explicit_baseline_selection_authority() -> None:
    """Inferring selected instruments from Agent stance or universe membership must fail closed."""
    existing = UUID("0198d500-1000-7000-8000-000000000301")
    missing = build_strategy_target({existing: Decimal("1")}, authority=None)
    assert missing.evidence_complete is False
    assert missing.targets == {existing: Decimal("1")}

    authority = DeterministicSelectionAuthority(
        policy_version="baseline_v1",
        selected_weights={existing: Decimal("1")},
        evidence_hash="a" * 64,
    )
    selected = build_strategy_target({existing: Decimal("1")}, authority=authority)
    assert selected.evidence_complete is True
    assert selected.targets == {existing: Decimal("1")}
    with pytest.raises(ValueError, match="sum to one"):
        DeterministicSelectionAuthority(
            policy_version="baseline_v1",
            selected_weights={existing: Decimal("0.2")},
            evidence_hash="a" * 64,
        )
