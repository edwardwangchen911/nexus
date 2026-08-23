# ruff: noqa: E501
"""Strategy-target authority is deterministic and fail closed."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.optimizer.strategy_target import (
    DeterministicSelectionAuthority,
    SelectionCandidate,
    build_strategy_target,
)


def test_strategy_target_requires_explicit_baseline_selection_authority() -> None:
    """Inferring selected instruments from Agent stance or universe membership must fail closed."""
    existing = UUID("0198d500-1000-7000-8000-000000000301")
    missing = build_strategy_target(((existing, Decimal("1")),), authority=None)
    assert missing.evidence_complete is False
    assert missing.targets == ((existing, Decimal("1")),)

    authority = DeterministicSelectionAuthority(
        policy_version="baseline_v1",
        decision_at=datetime(2026, 8, 24, tzinfo=UTC),
        data_snapshot_id=UUID("0198d500-1000-7000-8000-000000000302"),
        available_at=datetime(2026, 8, 24, tzinfo=UTC),
        evidence_refs=("selection:fixture",),
        candidates=(SelectionCandidate(existing, ("candidate:fixture",)),),
        selected_instrument_ids=(existing,),
        evidence_hash="a" * 64,
    )
    selected = build_strategy_target(((existing, Decimal("1")),), authority=authority)
    assert selected.evidence_complete is True
    assert selected.targets == ((existing, Decimal("0.1250")),)
    assert selected.cash_target_weight == Decimal("0.8750")
    with pytest.raises(ValueError, match="deterministic candidates"):
        DeterministicSelectionAuthority(
            policy_version="baseline_v1",
            decision_at=datetime(2026, 8, 24, tzinfo=UTC),
            data_snapshot_id=UUID("0198d500-1000-7000-8000-000000000302"),
            available_at=datetime(2026, 8, 24, tzinfo=UTC),
            evidence_refs=("selection:fixture",),
            candidates=(),
            selected_instrument_ids=(existing,),
            evidence_hash="a" * 64,
        )
