"""The evolution interpreter never accepts history offsets from candidate documents."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from hypothesis import given
from hypothesis import strategies as st
from nexus.strategy.baseline import (
    BaselineInstrumentSnapshot,
    BaselineSectorExposure,
    StrategyDataSnapshot,
)
from nexus.strategy.dsl import baseline_v1_document, compile_strategy, evolution_feature_registry_v1
from nexus.strategy.interpreter import CompiledStrategyEvaluationError, evaluate_compiled_strategy


@given(st.integers(min_value=-10_000, max_value=-1))
def test_negative_feature_offsets_are_rejected_instead_of_reading_unmodeled_history(
    offset: int,
) -> None:
    document = baseline_v1_document()
    document["version"] = "challenger_offset"
    document["root"]["args"][0]["offset"] = offset
    strategy = compile_strategy(document, evolution_feature_registry_v1())
    friday = date(2026, 8, 21)
    closes = tuple(Decimal("100") for _ in range(200))
    snapshot = StrategyDataSnapshot(
        decision_session=friday,
        next_session=friday + timedelta(days=3),
        instruments=(
            BaselineInstrumentSnapshot(
                instrument_id=UUID(int=1),
                symbol="S1",
                asset_type="stock",
                sector_exposures=(
                    BaselineSectorExposure(
                        sector="information_technology", weight=Decimal(1)
                    ),
                ),
                raw_close=Decimal("100"),
                closes=closes,
                dollar_volumes=tuple(Decimal("60000000") for _ in range(20)),
            ),
        ),
    )

    with pytest.raises(CompiledStrategyEvaluationError, match="OFFSET_NOT_SUPPORTED"):
        evaluate_compiled_strategy(strategy, snapshot, friday)
