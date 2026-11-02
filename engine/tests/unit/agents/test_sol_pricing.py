"""Synthetic Sol price records; no provider, registration or funding authority."""

from copy import deepcopy
from decimal import Decimal, localcontext

import pytest
from nexus.agents.platform_usage import platform_reservation_usd, price_platform_usage
from unit.agents.test_platform_usage import usage


def sol_price_record():
    return {
        "schema_version": "platform-sol-pricing-v1",
        "model_id": "gpt-6.1-sol",
        "service_tier": "default",
        "cache_mode": "implicit",
        "cache_ttl": "30m",
        "currency": "USD",
        "source_url": "https://developers.openai.com/api/docs/pricing?latest-pricing=standard",
        "source_evidence_sha256": "a" * 64,
        "verified_at": "2026-11-02T02:00:00Z",
        "expires_at": "2026-11-09T02:00:00Z",
        "ordinary_input_usd_per_million": "2",
        "cache_read_usd_per_million": "0.10",
        "cache_write_30m_usd_per_million": "2.50",
        "output_usd_per_million": "10",
    }


def sol_price(raw=None, *, record=None, **overrides):
    return price_platform_usage(
        usage() if raw is None else raw,
        **(
            {
                "model": "gpt-6.1-sol",
                "service_tier": "default",
                "cache_mode": "implicit",
                "cache_ttl": "30m",
            }
            | overrides
        ),
        price_record=sol_price_record() if record is None else record,
    )


def test_sol_prices_exclusive_cache_classes_and_all_output_without_changing_terra():
    raw = usage()
    original = deepcopy(raw)
    result = sol_price(raw)
    assert result.exact_cost_usd == Decimal("0.0299")
    assert result.totals_verified and result.reasoning_tokens == 900
    assert result.issues == () and result.raw_usage == raw == original
    legacy = price_platform_usage(
        raw, model="gpt-5.6-terra", service_tier="default", cache_mode="implicit", cache_ttl="30m"
    )
    assert legacy.exact_cost_usd == Decimal("0.0323")
    assert str(legacy.exact_cost_usd) == "0.0323000"
    assert result.price_version != legacy.price_version


def test_sol_reservation_uses_worst_cache_class_and_one_shared_output_allowance():
    assert platform_reservation_usd(
        (272000, 272000, 272000), 8192, price_record=sol_price_record()
    ) == Decimal("2.12192")
    changed = sol_price_record() | {"ordinary_input_usd_per_million": "3"}
    assert platform_reservation_usd((12000,), 1000, price_record=changed) == Decimal("0.046")
    assert sol_price(record=changed).exact_cost_usd == Decimal("0.0309")


def test_renewed_price_is_a_distinct_record_not_a_new_hardcoded_capability():
    old = sol_price_record()
    renewed = old | {
        "verified_at": "2026-11-09T02:00:00Z",
        "expires_at": "2026-11-16T02:00:00Z",
        "source_evidence_sha256": "b" * 64,
    }
    first, second = sol_price(record=old), sol_price(record=renewed)
    assert first.price_version != second.price_version
    assert first.exact_cost_usd == second.exact_cost_usd == Decimal("0.0299")
    assert sol_price(record=old) == first  # No current clock in historical settlement.


@pytest.mark.parametrize(
    "change",
    [
        {"model_id": "gpt-5.6-terra"},
        {"service_tier": "flex"},
        {"cache_ttl": "12h"},
        {"source_url": "https://example.invalid/prices"},
        {"source_evidence_sha256": ""},
        {"output_usd_per_million": 10},
        {"output_usd_per_million": "NaN"},
        {"output_usd_per_million": "-1"},
        {"output_usd_per_million": "0.0000001"},
        {"expires_at": "2026-11-02T02:00:00Z"},
        {"expires_at": "2027-11-02T02:00:00Z"},
        {"verified_at": "2026-11-02T02:00:00"},
        {"unexpected": True},
    ],
)
def test_invalid_price_keeps_counts_but_cannot_price_or_reserve(change):
    record = sol_price_record() | change
    result = sol_price(record=record)
    assert result.totals_verified and result.output_tokens == 1000
    assert result.exact_cost_usd is None
    assert "PLATFORM_PRICE_RECORD_INVALID" in result.issues
    with pytest.raises(ValueError, match="PLATFORM_PRICE_RECORD_INVALID"):
        platform_reservation_usd((12000,), 1000, price_record=record)


@pytest.mark.parametrize(
    "actual",
    [
        {"model": None},
        {"model": "gpt-5.6-terra"},
        {"service_tier": None},
        {"service_tier": "priority"},
        {"cache_mode": None},
        {"cache_ttl": "12h"},
    ],
)
def test_bound_sol_price_does_not_supply_missing_or_wrong_response_identity(actual):
    result = sol_price(**actual)
    assert result.exact_cost_usd is None and "PLATFORM_PROFILE_MISMATCH" in result.issues
    assert result.totals_verified


@pytest.mark.parametrize("bad", ["missing", "overlap", "long_context", "12h"])
def test_sol_keeps_unknown_classification_and_tier_protections(bad):
    raw = usage()
    if bad == "missing":
        del raw["input_tokens_details"]["cache_write_tokens"]
    elif bad == "overlap":
        raw["input_tokens_details"]["cached_tokens"] = 6000
    elif bad == "long_context":
        raw.update(input_tokens=272001, total_tokens=273001)
    else:
        raw["input_tokens_details"]["cache_write_tokens_details"].update(
            ttl_30m_tokens=6000, ttl_12h_tokens=1000
        )
    result = sol_price(raw)
    assert result.totals_verified and result.exact_cost_usd is None
    assert result.raw_usage == raw


def test_prices_use_exact_ledger_precision_independent_of_decimal_context():
    with localcontext() as context:
        context.prec = 2
        assert sol_price().exact_cost_usd == Decimal("0.0299")
        assert platform_reservation_usd(
            (272000,) * 3, 8192, price_record=sol_price_record()
        ) == Decimal("2.12192")
