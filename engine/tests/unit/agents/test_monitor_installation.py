"""A replacement consumes only a valid frozen quote, never an inferred false."""

from datetime import timedelta
from types import SimpleNamespace

import pytest
from nexus.monitoring.installation import consumed_baseline
from unit.agents.test_research_monitoring import NOW, condition


@pytest.mark.parametrize(
    "change, expected",
    [
        ({}, "true"),
        ({"bid": "98", "ask": "100"}, "false"),
        ({"observed_at": NOW - timedelta(seconds=61)}, "unknown"),
        ({"received_at": NOW + timedelta(seconds=1)}, "unknown"),
        ({"bid": "0"}, "unknown"),
        ({"ask": "90"}, "unknown"),
        ({"bid": "NaN"}, "unknown"),
    ],
)
def test_replacement_baseline_uses_original_freshness_and_double_sided_price(change, expected):
    context = SimpleNamespace(
        decision_time=NOW,
        decision_quote=SimpleNamespace(
            **{
                "bid": "99",
                "ask": "101",
                "observed_at": NOW,
                "available_at": NOW,
                "received_at": NOW,
                **change,
            }
        ),
    )
    assert consumed_baseline(condition(), context) == expected


@pytest.mark.parametrize("later", [False, True])
def test_return_to_prior_condition_consumes_baseline_without_eating_later_check(later):
    from nexus.monitoring.evaluation import EpisodeState
    from nexus.monitoring.installation import consume_episode_baseline

    previous = EpisodeState(sequence=1, latched=False, observed_false=True)
    updated, applied = consume_episode_baseline(
        previous,
        "true",
        last_checked_at=NOW + timedelta(seconds=1 if later else -1),
        observed_at=NOW,
    )
    assert applied is (not later)
    assert updated == (
        previous if later else EpisodeState(sequence=2, latched=True, observed_false=True)
    )
