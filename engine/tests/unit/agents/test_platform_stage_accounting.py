"""One frozen evidence reader for exact cache costs and unknown reservation holds."""

import hashlib
import json
from copy import deepcopy
from decimal import Decimal

import pytest
from nexus.agents.platform_stage_accounting import platform_stage_accounting
from nexus.agents.platform_usage import PLATFORM_PRICE_VERSION
from unit.agents.test_platform_usage import usage


def row(stage=1):
    payload = json.dumps(
        {
            "model": "gpt-5.6-terra",
            "service_tier": "default",
            "status": "completed",
            "usage": usage(),
        }
    )
    return {
        "stage": stage,
        "wire_hash": "a" * 64,
        "response_hash": hashlib.sha256(payload.encode()).hexdigest(),
        "input_tokens": 12000,
        "output_tokens": 1000,
        "verified": True,
        "eligible": True,
        "platform_binding": {
            "version": 1,
            "price_version": PLATFORM_PRICE_VERSION,
            "body_sha256": "a" * 64,
            "input_upper_bound": 20000,
            "cache_mode": "implicit",
            "cache_ttl": "30m",
        },
        "platform_response": payload,
    }


def test_exact_cost_is_not_the_reservation_scalar_and_raw_evidence_unchanged():
    rows = [row(i) for i in range(1, 4)]
    before = deepcopy(rows)
    result = platform_stage_accounting(rows)
    assert result.exact_cost_usd == result.known_cost_usd == Decimal("0.0969")
    assert (result.input_tokens, result.output_tokens) == (36000, 3000)
    assert result.limit_exceeded is False and rows == before


@pytest.mark.parametrize(
    "kind",
    [
        "missing_cache",
        "missing_raw",
        "wrong_hash",
        "wrong_counts",
        "unverified",
        "ineligible",
        "wrong_tier",
    ],
)
def test_unknown_never_becomes_exact_or_frees_a_previous_known_prefix(kind):
    rows = [row(), row(2)]
    r = rows[1]
    payload = json.loads(r["platform_response"])
    if kind == "missing_cache":
        del payload["usage"]["input_tokens_details"]["cache_write_tokens"]
    if kind == "wrong_tier":
        payload["service_tier"] = "flex"
    if kind in ("missing_cache", "wrong_tier"):
        r["platform_response"] = json.dumps(payload)
        r["response_hash"] = hashlib.sha256(r["platform_response"].encode()).hexdigest()
    if kind == "missing_raw":
        r["platform_response"] = None
    if kind == "wrong_hash":
        r["response_hash"] = "b" * 64
    if kind == "wrong_counts":
        r["input_tokens"] = 1
    if kind == "unverified":
        r["verified"] = False
    if kind == "ineligible":
        r["eligible"] = False
    result = platform_stage_accounting(rows)
    assert result.exact_cost_usd is None
    assert result.known_cost_usd == Decimal("0.0323")


def test_trusted_input_or_cumulative_output_overrun_is_a_fence():
    r = row()
    r["platform_binding"]["input_upper_bound"] = 11999
    assert platform_stage_accounting([r]).limit_exceeded
    rows = [row(i) for i in range(1, 4)]
    for r in rows:
        r["output_tokens"] = 3000
    assert platform_stage_accounting(rows).limit_exceeded


def test_no_evidence_does_not_prove_zero_usage():
    assert platform_stage_accounting([]).exact_cost_usd is None
