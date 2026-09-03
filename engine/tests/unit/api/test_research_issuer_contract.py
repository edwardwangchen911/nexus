"""Old research summaries remain readable without issuer observations."""

from nexus.api.schemas.agents import ResearchInputsSummary


def test_old_summary_defaults_issuer_observations_to_empty():
    summary = ResearchInputsSummary(
        display_symbol=None, filings=(), facts=(), total_facts=0, truncated=False
    )
    assert summary.model_dump(mode="json")["issuer_distributions"] == []
