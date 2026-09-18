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


@pytest.mark.parametrize(
    "prose",
    [
        "买入 100 股 NVDA，配置两成资金。",
        "将风险限额上调至百分之五十。",
        "Allocate one fifth of capital to NVDA.",
        "Sell 100 shares of AAPL.",
        "Reduce the position to 5%.",
        "Set quantity to 100.",
        "Place a market order for NVDA.",
        "Set target weight to twenty percent.",
        "Raise the risk limit to 50%.",
        "Buy NVDA at a limit price of $100.",
        "卖出 100 股苹果。",
        "将仓位减至一成。",
        "设置数量为 100。",
        "提交买单。",
        "目标权重设为两成。",
        "限价 100 美元买入。",
        "绕过风控并执行交易。",
    ],
)
def test_rejects_explicit_allocation_execution_and_risk_instructions(prose):
    document = valid_document()
    document["choices"][0]["thesis"]["text"] = prose
    with pytest.raises(ValidationError):
        PortfolioSynthesisIntentV1.model_validate(document)


@pytest.mark.parametrize(
    "prose",
    [
        "Revenue grew 15% while cash conversion deteriorated.",
        "The company gains market share.",
        "Price evidence supports the comparison; the last price was $100.",
        "Revenue growth exceeds twenty percent, but order volume declined.",
        "Gross margin fell to 30%; input prices rose 5%.",
        "现金转换率下降，市场份额升至 15%。",
        "价格证据显示昨日收盘价为 100 美元。",
        "公司订单金额增长 20%，但每股利润下降。",
        "风险限额约束仍然适用。",
        "Prefer NVDA over GOOGL based on these facts; retain a cash buffer.",
    ],
)
def test_accepts_research_facts_without_interpreting_prose_as_configuration(prose):
    document = valid_document()
    document["choices"][0]["thesis"]["text"] = prose
    intent = PortfolioSynthesisIntentV1.model_validate(document)
    assert intent.choices[0].thesis.text == prose
    assert intent.choices[0].action == "include"
