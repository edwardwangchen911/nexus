"""Cash-flow components remain as disclosed, never a guessed signed bridge."""

import json
from copy import deepcopy
from decimal import Decimal

import pytest
from nexus.providers.sec import normalize_company_facts
from unit.providers.test_sec import ACCESSION, ARCHIVED_ACCESSION, FIXTURES, _evidence, _identity

FIELDS = [
    ("DepreciationDepletionAndAmortization", "depreciation_depletion_amortization"),
    ("ShareBasedCompensation", "share_based_compensation"),
    ("DeferredIncomeTaxExpenseBenefit", "deferred_income_tax_expense_benefit"),
    ("GainLossOnInvestments", "investment_gain_loss"),
    ("OtherNoncashIncomeExpense", "other_noncash_income_expense"),
    ("IncreaseDecreaseInAccountsReceivable", "cash_flow_change_accounts_receivable"),
    ("IncreaseDecreaseInInventories", "cash_flow_change_inventories"),
    ("IncreaseDecreaseInAccountsPayable", "cash_flow_change_accounts_payable"),
    (
        "IncreaseDecreaseInAccruedLiabilitiesAndOtherOperatingLiabilities",
        "cash_flow_change_accrued_operating_liabilities",
    ),
    (
        "IncreaseDecreaseInPrepaidDeferredExpenseAndOtherAssets",
        "cash_flow_change_prepaid_other_assets",
    ),
    (
        "IncreaseDecreaseInOtherNoncurrentLiabilities",
        "cash_flow_change_other_noncurrent_liabilities",
    ),
]


@pytest.mark.parametrize("tag,canonical", FIELDS)
def test_cash_flow_component_preserves_raw_basis_and_signed_value(tag, canonical):
    body = json.loads((FIXTURES / "companyfacts.json").read_bytes())
    original = deepcopy(body["facts"]["us-gaap"]["Revenues"])
    for row in original["units"]["USD"]:
        row["val"] = -123
    body["facts"]["us-gaap"][tag] = original
    facts = normalize_company_facts(
        json.dumps(body).encode(),
        (
            (FIXTURES / "submissions.json").read_bytes(),
            (FIXTURES / "submissions-001.json").read_bytes(),
        ),
        instrument=_identity(),
        evidence_by_accession={
            ACCESSION: _evidence(),
            ARCHIVED_ACCESSION: _evidence(ARCHIVED_ACCESSION),
        },
    )
    found = [f for f in facts if f.canonical_fact == canonical]
    assert len(found) == 2
    assert all(
        f.raw_taxonomy == "us-gaap"
        and f.raw_tag == tag
        and f.value == Decimal("-123")
        and f.unit == "USD"
        and f.period_kind == "duration"
        and f.period_start is not None
        for f in found
    )
    assert len([f for f in facts if f.canonical_fact == "revenue"]) == 2
