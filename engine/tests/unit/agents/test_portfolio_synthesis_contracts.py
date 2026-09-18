"""Portfolio choices cannot become sizing instructions or unbound authority."""

from copy import deepcopy
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.agents.portfolio_synthesis_contracts import (
    PortfolioSynthesisIntentV1,
    canonical_portfolio_synthesis_hash,
)
from pydantic import ValidationError


def valid_document():
    ids = [str(UUID(int=1)), str(UUID(int=2))]
    ref = {"research_output_id": str(UUID(int=3)), "evidence_ref_id": str(UUID(int=4))}
    statement = {"text": "Frozen evidence supports this comparison.", "references": [ref]}
    return {
        "schema_version": "portfolio_synthesis_intent_v1",
        "input_hash": "a" * 64,
        "compared_instrument_ids": ids,
        "choices": [
            {
                "instrument_id": instrument,
                "action": "include",
                "role": "core",
                "rank": index,
                "replaces_instrument_id": None,
                "depends_on_instrument_ids": [],
                "thesis": deepcopy(statement),
                "counter_thesis": deepcopy(statement),
                "invalidation": deepcopy(statement),
            }
            for index, instrument in enumerate(ids, 1)
        ],
        "cash_rationale": statement,
    }


def test_synthesis_intent_contains_choice_not_sizing():
    intent = PortfolioSynthesisIntentV1.model_validate(valid_document())
    assert [choice.rank for choice in intent.choices] == [1, 2]


@pytest.mark.parametrize(
    "field",
    ["target_weight", "percentage", "amount", "quantity", "order", "price", "risk_override"],
)
@pytest.mark.parametrize("nested", [False, True])
def test_rejects_sizing_fields_at_both_levels(field, nested):
    document = valid_document()
    target = document["choices"][0] if nested else document
    target[field] = "0.10"
    with pytest.raises(ValidationError):
        PortfolioSynthesisIntentV1.model_validate(document)


@pytest.mark.parametrize("ranks", [[1, 1], [1, 3], [None, 2], [True, 2], [1.0, 2], ["1", 2]])
def test_competing_increases_require_strict_contiguous_ranks(ranks):
    document = valid_document()
    for choice, rank in zip(document["choices"], ranks, strict=True):
        choice["rank"] = rank
    with pytest.raises((ValidationError, TypeError)):
        PortfolioSynthesisIntentV1.model_validate(document)


@pytest.mark.parametrize(
    "mutation",
    [
        "missing",
        "duplicate",
        "extra",
        "unranked_action",
        "empty_cash",
        "empty_evidence",
        "self_replace",
        "unsafe_text",
    ],
)
def test_rejects_incomplete_or_ambiguous_comparison(mutation):
    document = valid_document()
    if mutation == "missing":
        document["choices"].pop()
    elif mutation == "duplicate":
        document["choices"][1]["instrument_id"] = document["choices"][0]["instrument_id"]
    elif mutation == "extra":
        document["compared_instrument_ids"].append(str(UUID(int=9)))
    elif mutation == "unranked_action":
        document["choices"][0]["action"] = "maintain"
    elif mutation == "empty_cash":
        document["cash_rationale"]["text"] = "  "
    elif mutation == "empty_evidence":
        document["choices"][0]["invalidation"]["references"] = []
    elif mutation == "self_replace":
        document["choices"][0]["replaces_instrument_id"] = str(UUID(int=1))
    else:
        document["choices"][0]["thesis"]["text"] = "Buy 100 shares at $40; override risk."
    with pytest.raises(ValidationError):
        PortfolioSynthesisIntentV1.model_validate(document)


def test_non_increase_uses_null_rank_and_schema_round_trips():
    document = valid_document()
    document["choices"][1].update(action="defer", role="none", rank=None)
    intent = PortfolioSynthesisIntentV1.model_validate(document)
    assert PortfolioSynthesisIntentV1.model_validate_json(intent.model_dump_json()) == intent


def test_canonical_hash_normalizes_decimal_and_rejects_float():
    assert canonical_portfolio_synthesis_hash(
        {"schema_version": "v1", "cash": Decimal("1.00")}
    ) == canonical_portfolio_synthesis_hash({"cash": Decimal("1"), "schema_version": "v1"})
    assert canonical_portfolio_synthesis_hash(
        {"schema_version": "v1"}
    ) != canonical_portfolio_synthesis_hash({"schema_version": "v2"})
    with pytest.raises(TypeError):
        canonical_portfolio_synthesis_hash({"value": 1.0})
