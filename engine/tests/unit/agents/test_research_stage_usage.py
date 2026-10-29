"""Trusted stage prefixes never grant whole-run settlement authority."""

import pytest
from nexus.agents.research_stages.protocol import known_usage_lower_bound, usage_known


@pytest.mark.parametrize("prefix", [(0, 0), (10, 0), (0, 5), (10, 5)])
@pytest.mark.parametrize(
    "verified,eligible", [(True, True), (None, True), (False, True), (True, False)]
)
def test_verified_prefix_lower_bound_does_not_make_unknown_successor_known(
    prefix, verified, eligible
):
    rows = [
        {
            "verified": verified,
            "eligible": eligible,
            "input_tokens": prefix[0],
            "output_tokens": prefix[1],
        },
        {"verified": None, "eligible": True, "input_tokens": None, "output_tokens": None},
    ]
    usage = known_usage_lower_bound(rows)
    assert (usage.input_tokens, usage.output_tokens) == (
        prefix if verified is True and eligible else (0, 0)
    )
    assert usage.provider_calls == 2
    assert not usage_known(rows)
    if verified is True and eligible:
        assert usage_known(rows[:1])
