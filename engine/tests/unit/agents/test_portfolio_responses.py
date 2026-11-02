import json
from types import SimpleNamespace

import pytest
from nexus.agents.platform_portfolio import compile_request
from nexus.agents.platform_pricing import sol_price_record
from nexus.agents.platform_profile import sol_business_model_fields
from nexus.agents.portfolio_responses import _observe_terminal
from nexus.agents.responses_stream import ResponsesStreamFacts
from unit.agents.test_http_stage_lifecycle import terminal
from unit.agents.test_platform_usage import usage
from unit.agents.test_sol_pricing import sol_price_record as price_fixture


def test_frozen_request_survives_jsonb_key_reordering_and_keeps_array_order():
    model = SimpleNamespace(**sol_business_model_fields(price_fixture()))
    original = {
        "instructions": "观察",
        "input_hash": "a" * 64,
        "frozen_input": {"z": [{"b": 1, "a": "原价"}], "a": ["second", "first"]},
    }
    roundtrip = json.loads(json.dumps(original, sort_keys=True))
    wire = compile_request(model, original)
    assert wire == compile_request(model, roundtrip)
    data = json.loads(wire)
    assert data["text"]["format"]["name"] == "portfolio_synthesis"
    assert data["text"]["format"]["strict"] and data["store"] is False and data["tools"] == []
    roundtrip["frozen_input"]["a"].reverse()
    assert wire != compile_request(model, roundtrip)
    with pytest.raises(ValueError, match="PORTFOLIO_PLATFORM_INPUT_TOO_LARGE"):
        compile_request(model, original | {"instructions": "x" * 64001})


@pytest.mark.parametrize("missing", [False, True])
def test_receipt_keeps_counter_evidence_without_copying_provider_extras(missing):
    event = terminal()
    value = event["response"]
    raw = usage()
    if missing:
        raw["input_tokens_details"].pop("cache_write_tokens_details")
    raw["unrelated"] = "synthetic-private-canary"
    value.update(model="gpt-6.1-sol", service_tier="default", usage=raw)
    facts = ResponsesStreamFacts()
    facts.observe(event)
    result = _observe_terminal(value, facts, sol_price_record(price_fixture()))
    assert (result.usage is None) == missing
    assert result.evidence["verified_totals"] == [12000, 1000]
    assert "synthetic-private-canary" not in json.dumps(result.evidence)
    assert result.evidence["intent_hash"] is None
    assert bool(result.evidence["usage_issues"]) == missing
