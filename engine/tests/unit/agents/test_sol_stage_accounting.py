"""Sol native stage fee projection; ownership/terminal proof remains external."""

import hashlib
import json
from copy import deepcopy
from decimal import Decimal, localcontext

import pytest
from nexus.agents.platform_pricing import sol_price_record
from nexus.agents.platform_stage_accounting import platform_stage_accounting
from unit.agents.test_platform_stage_accounting import row
from unit.agents.test_sol_pricing import sol_price_record as record_fixture


def sol_row(stage=1):
    value = row(stage)
    record = sol_price_record(record_fixture())
    value["platform_binding"].update(
        version=2, price_version=record.version, price_record_hash=record.content_hash
    )
    response = json.loads(value["platform_response"])
    response["model"] = record.model_id
    update_response(value, response)
    return value


def update_response(value, response):
    value["platform_response"] = json.dumps(response)
    value["response_hash"] = hashlib.sha256(value["platform_response"].encode()).hexdigest()


def project(rows, record=None):
    return platform_stage_accounting(
        rows, price_record=record_fixture() if record is None else record
    )


def test_sol_three_stage_exact_accounting_consumes_original_prices_and_raw_usage():
    rows = [sol_row(i) for i in (1, 2, 3)]
    before = deepcopy(rows)
    with localcontext() as context:
        context.prec = 2
        actual = project(rows)
    assert actual.known_cost_usd == actual.exact_cost_usd == Decimal("0.0897")
    assert (actual.input_tokens, actual.output_tokens) == (36000, 3000)
    assert not actual.limit_exceeded and rows == before


def test_common_known_usage_requires_the_original_sol_model_price():
    from types import SimpleNamespace

    from nexus.agents.platform_profile import sol_business_model_fields
    from nexus.agents.research_stages.protocol import usage_known

    rows = [sol_row()]
    model = SimpleNamespace(**sol_business_model_fields(record_fixture()))
    assert usage_known(rows, model=model)
    assert not usage_known(rows)
    replacement = SimpleNamespace(
        **sol_business_model_fields(record_fixture() | {"output_usd_per_million": "20"})
    )
    assert not usage_known(rows, model=replacement)
    raw = json.loads(rows[0]["platform_response"])
    del raw["usage"]["input_tokens_details"]["cache_write_tokens"]
    update_response(rows[0], raw)
    assert not usage_known(rows, model=model)


@pytest.mark.parametrize(
    "kind",
    [
        "hash",
        "record_hash",
        "version",
        "price_version",
        "missing_raw",
        "unverified",
        "tier",
        "cache",
        "model",
    ],
)
def test_sol_unknown_keeps_known_prefix_without_claiming_full_settlement(kind):
    rows = [sol_row(), sol_row(2)]
    current = rows[1]
    if kind == "hash":
        current["response_hash"] = "b" * 64
    elif kind == "record_hash":
        current["platform_binding"]["price_record_hash"] = "b" * 64
    elif kind == "version":
        current["platform_binding"]["version"] = 1
    elif kind == "price_version":
        current["platform_binding"]["price_version"] = "unbound"
    elif kind == "missing_raw":
        current["platform_response"] = None
    elif kind == "unverified":
        current["verified"] = False
    else:
        raw = json.loads(current["platform_response"])
        if kind == "tier":
            raw["service_tier"] = None
        elif kind == "cache":
            del raw["usage"]["input_tokens_details"]["cache_write_tokens"]
        else:
            raw["model"] = "gpt-5.6-terra"
        update_response(current, raw)
    actual = project(rows)
    assert actual.known_cost_usd == Decimal("0.0299")
    assert actual.exact_cost_usd is None


def test_new_price_cannot_reprice_old_stage_or_enter_legacy_accounting():
    rows = [sol_row()]
    assert platform_stage_accounting(rows).exact_cost_usd is None
    changed = record_fixture() | {"output_usd_per_million": "20"}
    actual = project(rows, changed)
    assert actual.exact_cost_usd is None and actual.known_cost_usd == 0
    assert project([row()]).exact_cost_usd is None


def test_trusted_sol_usage_over_frozen_bound_keeps_true_cost_and_stops_following_stages():
    rows = [sol_row()]
    rows[0]["platform_binding"]["input_upper_bound"] = 11999
    actual = project(rows)
    assert actual.limit_exceeded and actual.exact_cost_usd == Decimal("0.0299")


@pytest.mark.parametrize("status", ["failed", "incomplete"])
def test_terminal_failure_can_have_known_usage_without_becoming_success(status):
    rows = [sol_row()]
    raw = json.loads(rows[0]["platform_response"])
    raw["status"] = status
    update_response(rows[0], raw)
    assert project(rows).exact_cost_usd == Decimal("0.0299")
    assert json.loads(rows[0]["platform_response"])["status"] == status
