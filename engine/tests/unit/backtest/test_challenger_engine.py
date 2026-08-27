"""Restricted challenger backtest entry-point guards."""

from __future__ import annotations

from typing import cast

import pytest
from nexus.backtest.engine import (
    BacktestProtocol,
    PointInTimeDataset,
    run_challenger_backtest_v1,
)
from nexus.strategy.dsl import baseline_feature_registry, baseline_v1_document, compile_strategy


def test_challenger_entry_rejects_baseline_before_reading_data() -> None:
    baseline = compile_strategy(baseline_v1_document(), baseline_feature_registry())

    with pytest.raises(ValueError, match="challenger strategy"):
        run_challenger_backtest_v1(
            baseline,
            cast(PointInTimeDataset, object()),
            cast(BacktestProtocol, object()),
            experiment_attempt_count=2,
        )


def test_challenger_entry_rejects_forged_hash_before_reading_data() -> None:
    forged = compile_strategy(
        {**baseline_v1_document(), "version": "challenger_fixture"},
        baseline_feature_registry(),
    ).model_copy(update={"expression_hash": "f" * 64})

    with pytest.raises(ValueError, match="INTEGRITY"):
        run_challenger_backtest_v1(
            forged,
            cast(PointInTimeDataset, object()),
            cast(BacktestProtocol, object()),
            experiment_attempt_count=2,
        )
