"""Optional provider DTOs never grant financial or execution authority."""

import pytest
from nexus.api.schemas import agents
from pydantic import ValidationError


def profile():
    return dict(
        evidence_ref_id="00000000-0000-0000-0000-000000000001",
        source="fmp_company_profile",
        revision="a" * 64,
        content_hash="a" * 64,
        raw_content_hash="b" * 64,
        receipt_content_hash="c" * 64,
        symbol="AAPL",
        company_name=None,
        description=None,
        cik=None,
        isin=None,
        cusip=None,
        industry=None,
        sector=None,
        source_url="https://financialmodelingprep.com/stable/profile?symbol=AAPL",
        received_at="2026-09-04T00:00:00Z",
        available_at="2026-09-04T00:00:00Z",
        source_published_at=None,
        valid_until=None,
        sector_taxonomy=None,
        revision_semantics="local_content_hash",
        sector_risk_eligible=False,
        financial_fact_eligible=False,
        price_authority_eligible=False,
        historical_backtest_eligible=False,
        execution_locked=True,
    )


def macro():
    return dict(
        observation_id="00000000-0000-0000-0000-000000000001",
        evidence_ref_id="00000000-0000-0000-0000-000000000002",
        calendar_evidence_ref_id="00000000-0000-0000-0000-000000000003",
        source="fred",
        revision="synthetic-local-revision",
        content_hash="a" * 64,
        raw_content_hash="b" * 64,
        series="DGS10",
        observed_on="2026-09-01",
        vintage="2026-09-04",
        value="4.25",
        unit="Percent",
        received_at="2026-09-04T00:00:00Z",
        available_at="2026-09-04T00:00:00Z",
        revision_semantics="local_content_hash",
        historical_backtest_eligible=False,
        execution_locked=True,
    )


def test_old_summary_defaults_provider_observations_to_empty():
    result = agents.ResearchInputsSummary.model_validate(
        dict(
            display_symbol=None,
            filings=[],
            facts=[],
            total_facts=0,
            truncated=False,
        )
    )
    assert result.company_profiles == () and result.macro_observations == ()


@pytest.mark.parametrize("value", ["0", "-0", "-0.125", "4.25", "0.000000000001"])
def test_macro_values_preserve_source_units_and_decimal_strings(value):
    result = agents.ResearchInputMacroObservation.model_validate({**macro(), "value": value})
    assert result.model_dump(mode="json")["value"] == value and result.unit == "Percent"


@pytest.mark.parametrize(
    "change",
    [
        {"financial_fact_eligible": True},
        {"execution_locked": False},
        {
            "source_url": "https://financialmodelingprep.com/stable/profile?symbol=AAPL&apikey=secret"
        },
        {"account_id": "private"},
        {"content_hash": "invalid"},
    ],
)
def test_profile_contract_refuses_promotion_and_sensitive_extra_fields(change):
    with pytest.raises(ValidationError):
        agents.ResearchInputCompanyProfile.model_validate({**profile(), **change})


@pytest.mark.parametrize("value", [1.25, "NaN", "1e3", "0." + "0" * 40 + "1"])
def test_macro_contract_refuses_noncanonical_decimal(value):
    with pytest.raises(ValidationError):
        agents.ResearchInputMacroObservation.model_validate({**macro(), "value": value})
