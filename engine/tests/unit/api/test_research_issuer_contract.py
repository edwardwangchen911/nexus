"""Old research summaries remain readable without issuer observations."""

from uuid import UUID

from nexus.api.research_inputs import _fund_observation_response
from nexus.api.schemas.agents import ResearchInputsSummary
from nexus.persistence.models.research import ResearchEvidenceRef

from engine.tests.unit.research.test_ssga_fund_observations import parse


def test_old_summary_defaults_issuer_observations_to_empty():
    summary = ResearchInputsSummary(
        display_symbol=None, filings=(), facts=(), total_facts=0, truncated=False
    )
    assert summary.model_dump(mode="json")["issuer_distributions"] == []
    assert summary.model_dump(mode="json")["issuer_fund_observations"] == []
    assert summary.model_dump(mode="json")["issuer_nav_histories"] == []


def test_fund_projection_bounds_rows_without_renormalizing_original_percentages():
    original = parse()
    # Projection-only synthetic input: no persistence or claim about original workbook coverage.
    document = original.model_copy(update={
        "industries": tuple(original.industries[0].model_copy(update={"label": f"Class {i}"})
                            for i in range(51)),
        "holdings": tuple(original.holdings[0].model_copy(update={"name": f"Holding {i}"})
                          for i in range(101)),
    })
    evidence = ResearchEvidenceRef(id=UUID(int=1), revision="a" * 64, content_hash="a" * 64)
    response = _fund_observation_response(document, evidence)
    assert response.total_industries == 51 and len(response.industries) == 50
    assert response.total_holdings == 101 and len(response.holdings) == 100
    assert response.truncated
    assert response.industries[0].weight_percent == "100.00"
    assert response.holdings[0].weight_percent == "-0.125"
    assert response.holdings[-1].name == "Holding 99"
    assert response.denominator is None and response.residual_classification is None
    assert not response.sector_risk_eligible and not response.historical_backtest_eligible
