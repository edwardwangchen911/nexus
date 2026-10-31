"""Offline pricing evidence for the opt-in Terra/default/implicit-30m profile."""

from copy import deepcopy
from decimal import Decimal

import pytest
from nexus.agents.platform_usage import (
    PLATFORM_PRICE_VERSION,
    platform_reservation_usd,
    price_platform_usage,
)
from openai.types.responses.response_usage import ResponseUsage


def usage():
    return {
        "input_tokens": 12000,
        "input_tokens_details": {
            "cached_tokens": 4000,
            "cache_write_tokens": 7000,
            "cache_write_tokens_details": {"ttl_30m_tokens": 7000, "ttl_12h_tokens": 0},
        },
        "output_tokens": 1000,
        "output_tokens_details": {"reasoning_tokens": 900},
        "total_tokens": 13000,
    }


def price(raw, **overrides):
    basis = dict(
        model="gpt-5.6-terra", service_tier="default", cache_mode="implicit", cache_ttl="30m"
    )
    return price_platform_usage(raw, **(basis | overrides))


def test_sdk_model_extra_round_trip_preserves_writes_and_exact_exclusive_cost():
    raw = usage()
    raw["input_tokens_details"]["unrecognized_evidence"] = {"synthetic_only": True}
    native = ResponseUsage.model_validate(raw, strict=True)
    # The installed SDK does not declare these fields, but keeps the original extras.
    assert native.input_tokens_details.model_extra["cache_write_tokens"] == 7000
    assert native.input_tokens_details.model_extra["cache_write_tokens_details"] == {
        "ttl_30m_tokens": 7000,
        "ttl_12h_tokens": 0,
    }
    dumped = native.model_dump(mode="python")
    assert dumped == raw
    result = price(dumped)
    assert result.totals_verified
    assert (result.input_tokens, result.output_tokens, result.total_tokens) == (12000, 1000, 13000)
    assert result.ordinary_input_tokens == 1000
    assert result.cached_tokens == 4000
    assert result.cache_write_tokens == result.cache_write_30m_tokens == 7000
    assert result.cache_write_12h_tokens == 0
    assert result.reasoning_tokens == 900
    # 1000*2 + 4000*.20 + 7000*2.50 + ALL 1000 output*12, not just 100 visible tokens.
    assert result.exact_cost_usd == Decimal("0.0323")
    assert result.price_version == PLATFORM_PRICE_VERSION
    assert result.issues == ()
    assert result.raw_usage == raw
    dumped["input_tokens_details"]["cache_write_tokens"] = 1
    assert result.raw_usage == raw


@pytest.mark.parametrize(
    "counts,expected",
    [
        ((12000, 0, 0, 0), "0.024"),
        ((12000, 12000, 0, 0), "0.0024"),
        ((12000, 0, 12000, 0), "0.030"),
        ((0, 0, 0, 1000), "0.012"),
        ((0, 0, 0, 0), "0"),
    ],
)
def test_each_billable_class_is_counted_once(counts, expected):
    raw = usage()
    inp, cached, written, out = counts
    raw.update(input_tokens=inp, output_tokens=out, total_tokens=inp + out)
    raw["output_tokens_details"]["reasoning_tokens"] = out
    details = raw["input_tokens_details"]
    details.update(cached_tokens=cached, cache_write_tokens=written)
    details["cache_write_tokens_details"]["ttl_30m_tokens"] = written
    assert price(raw).exact_cost_usd == Decimal(expected)


DETAIL_PATHS = [
    ("input_tokens_details", "cached_tokens"),
    ("input_tokens_details", "cache_write_tokens"),
    ("input_tokens_details", "cache_write_tokens_details", "ttl_30m_tokens"),
    ("input_tokens_details", "cache_write_tokens_details", "ttl_12h_tokens"),
    ("output_tokens_details", "reasoning_tokens"),
]


def parent(raw, path):
    for part in path[:-1]:
        raw = raw[part]
    return raw


@pytest.mark.parametrize("path", DETAIL_PATHS)
@pytest.mark.parametrize("invalid", [None, True, -1, 2.5, "7000"])
def test_invalid_categories_keep_verified_totals_and_original_evidence(path, invalid):
    raw = usage()
    parent(raw, path)[path[-1]] = invalid
    original = deepcopy(raw)
    result = price(raw)
    assert result.totals_verified
    assert (result.input_tokens, result.output_tokens) == (12000, 1000)
    assert result.exact_cost_usd is None
    assert "PLATFORM_USAGE_DETAILS_UNKNOWN" in result.issues
    assert result.raw_usage == raw == original


@pytest.mark.parametrize(
    "path",
    DETAIL_PATHS
    + [
        ("input_tokens_details",),
        ("input_tokens_details", "cache_write_tokens_details"),
        ("output_tokens_details",),
    ],
)
def test_absent_categories_are_unknown_even_under_fixed_request_ttl(path):
    raw = usage()
    del parent(raw, path)[path[-1]]
    result = price(raw)
    assert result.totals_verified
    assert result.output_tokens == 1000
    assert result.exact_cost_usd is None
    assert "PLATFORM_USAGE_DETAILS_UNKNOWN" in result.issues
    assert result.raw_usage == raw


@pytest.mark.parametrize("change", ["overlap", "ttl_sum", "reasoning_exceeds_output"])
def test_contradictory_categories_do_not_discard_independent_total_counts(change):
    raw = usage()
    if change == "overlap":
        raw["input_tokens_details"]["cached_tokens"] = 6000
    elif change == "ttl_sum":
        raw["input_tokens_details"]["cache_write_tokens_details"]["ttl_30m_tokens"] = 6999
    else:
        raw["output_tokens_details"]["reasoning_tokens"] = 1001
    result = price(raw)
    assert result.totals_verified
    assert result.exact_cost_usd is None
    assert "PLATFORM_USAGE_DETAILS_CONFLICT" in result.issues
    assert result.raw_usage == raw


def test_observed_12h_writes_are_unknown_even_when_request_was_30m():
    raw = usage()
    raw["input_tokens_details"]["cache_write_tokens_details"].update(
        ttl_30m_tokens=6000, ttl_12h_tokens=1000
    )
    result = price(raw)
    assert result.totals_verified and result.cache_write_12h_tokens == 1000
    assert result.exact_cost_usd is None
    assert "PLATFORM_CACHE_TTL_UNSUPPORTED" in result.issues


@pytest.mark.parametrize("key", ["input_tokens", "output_tokens", "total_tokens"])
@pytest.mark.parametrize("invalid", [None, False, -1, 1.1, "12000"])
def test_invalid_total_is_not_coerced_or_reconstructed(key, invalid):
    raw = usage()
    raw[key] = invalid
    result = price(raw)
    assert not result.totals_verified
    assert getattr(result, key) is None
    assert result.exact_cost_usd is None
    assert "PLATFORM_USAGE_TOTALS_UNKNOWN" in result.issues
    assert result.raw_usage == raw
    if key != "output_tokens":
        assert result.output_tokens == 1000


def test_total_conflict_preserves_reported_counts_without_verifying_them():
    raw = usage()
    raw["total_tokens"] = 12999
    result = price(raw)
    assert not result.totals_verified
    assert (result.input_tokens, result.output_tokens, result.total_tokens) == (12000, 1000, 12999)
    assert result.exact_cost_usd is None
    assert "PLATFORM_USAGE_TOTALS_CONFLICT" in result.issues


@pytest.mark.parametrize("raw", [None, [], {}, {"input_tokens": 12000, "output_tokens": 1000}])
def test_missing_usage_does_not_become_zero_cost(raw):
    result = price(raw)
    assert not result.totals_verified
    assert result.exact_cost_usd is None
    assert result.raw_usage == raw


@pytest.mark.parametrize(
    "override",
    [
        {"model": "gpt-5.6-sol"},
        {"model": None},
        {"service_tier": "priority"},
        {"service_tier": None},
        {"cache_mode": "explicit"},
        {"cache_mode": None},
        {"cache_ttl": "12h"},
        {"cache_ttl": None},
    ],
)
def test_profile_mismatch_preserves_usage_without_claiming_price(override):
    result = price(usage(), **override)
    assert result.totals_verified
    assert result.exact_cost_usd is None
    assert "PLATFORM_PROFILE_MISMATCH" in result.issues


@pytest.mark.parametrize("inp,expected", [(272000, Decimal("0.556")), (272001, None)])
def test_price_tier_uses_one_request_input_without_clipping_observed_tokens(inp, expected):
    raw = usage()
    raw["input_tokens"] = inp
    raw["total_tokens"] = inp + 1000
    raw["input_tokens_details"].update(cached_tokens=0, cache_write_tokens=0)
    raw["input_tokens_details"]["cache_write_tokens_details"]["ttl_30m_tokens"] = 0
    result = price(raw)
    assert result.totals_verified and result.input_tokens == inp
    assert result.exact_cost_usd == expected
    if expected is None:
        assert "PLATFORM_PRICE_INPUT_TIER_UNSUPPORTED" in result.issues


def test_reservation_is_an_upper_bound_not_actual_settlement_or_total_chain_tier():
    assert platform_reservation_usd((12000,), 1000) == Decimal("0.042")
    assert platform_reservation_usd((12000,), 1000) > price(usage()).exact_cost_usd
    assert platform_reservation_usd((272000, 272000, 272000), 8192) == Decimal("2.138304")
    assert platform_reservation_usd((0,), 0) == 0


@pytest.mark.parametrize(
    "bounds,output",
    [
        ((), 1000),
        ((272001,), 1000),
        ((-1,), 1000),
        ((True,), 1000),
        ((12.0,), 1000),
        ((None,), 1000),
        ((1,), -1),
        ((1,), True),
        ((1,), 1.5),
    ],
)
def test_reservation_rejects_unproved_upper_bounds(bounds, output):
    with pytest.raises(ValueError, match="^PLATFORM_RESERVATION_INVALID$"):
        platform_reservation_usd(bounds, output)


def test_errors_and_repr_do_not_echo_original_extra_values():
    raw = usage()
    raw["input_tokens_details"]["cache_write_tokens"] = "synthetic-private-marker"
    result = price(raw)
    assert result.raw_usage == raw
    assert "synthetic-private-marker" not in repr(result)
    assert result.exact_cost_usd is None
