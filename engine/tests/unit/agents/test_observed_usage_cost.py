"""Pricing observed work cannot truncate it to execution admission limits."""

from decimal import Decimal

import pytest
from nexus.agents.synthesis import observed_usage_cost, usage_cost
from nexus.persistence.models.agents import AgentModelConfigRecord


def model():
    return AgentModelConfigRecord(
        max_input_tokens=96000,
        max_output_tokens=8192,
        input_usd_per_million=Decimal("12.5"),
        output_usd_per_million=Decimal("100"),
    )


def test_overlimit_is_exact_financial_fact_and_still_rejected_by_execution():
    assert observed_usage_cost(model(), 20_000_000, 9000) == Decimal("250.9")
    with pytest.raises(ValueError, match="immutable model limits"):
        usage_cost(model(), 20_000_000, 9000)
    assert usage_cost(model(), 20, 9) == observed_usage_cost(model(), 20, 9)


@pytest.mark.parametrize("tokens", [(-1, 0), (0, -1), (True, 0), (0, None), (1.5, 0)])
def test_observed_usage_rejects_non_counts(tokens):
    with pytest.raises(ValueError):
        observed_usage_cost(model(), *tokens)
