"""An economic explanation cannot turn an incomplete comparison into a recommendation."""

import pytest
from nexus.api.errors import ErrorCode
from nexus.api.schemas import optimization
from pydantic import ValidationError


@pytest.mark.parametrize("status", ["incomparable", "policy_required"])
def test_incomplete_comparison_requires_a_reason_and_no_recommendation(status):
    model = optimization.EconomicComparisonResponse
    value = model(status=status, recommendation=None, reasons=("COST_EVIDENCE_INCOMPLETE",))
    assert value.recommendation is None
    for changes in ({"recommendation": "hold_current"}, {"reasons": ()}):
        with pytest.raises(ValidationError):
            model.model_validate({**value.model_dump(), **changes})


def test_compared_response_requires_a_real_variant():
    model = optimization.EconomicComparisonResponse
    assert model(status="compared", recommendation="hold_current", reasons=()).recommendation
    with pytest.raises(ValidationError):
        model(status="compared", recommendation=None, reasons=())


def test_economic_explanation_cannot_supply_execution_permission():
    with pytest.raises(ValidationError):
        optimization.EconomicComparisonResponse.model_validate(
            {"status": "compared", "recommendation": "hold_current", "reasons": (),
             "execution_allowed": True}
        )


def test_old_group_responses_do_not_default_to_a_hold_recommendation():
    field = optimization.OptimizationProposalGroupResponse.model_fields["economic_comparison"]
    assert not field.is_required()
    assert field.default is None


def test_scenario_spread_replay_has_a_stable_error_without_expanding_public_models():
    assert (
        ErrorCode.SCENARIO_SPREAD_EVIDENCE_REPLAY_INVALID.value
        == "SCENARIO_SPREAD_EVIDENCE_REPLAY_INVALID"
    )
    assert "spread_evidence_manifest" not in optimization.PortfolioScenarioResponse.model_fields
