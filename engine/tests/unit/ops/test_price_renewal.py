"""A renewed source can never silently change capability or settlement economics."""

from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.agents.platform_profile import sol_business_model_fields
from nexus.agents.platform_schema import COMPACT_PLATFORM_UNITS_VERSION
from nexus.ops.price_renewal import require_equivalent_price_renewal
from nexus.persistence.models.agents import AgentModelConfigRecord
from unit.agents.test_sol_pricing import sol_price_record


def pair():
    old_price = sol_price_record()
    new_price = old_price | {
        "verified_at": "2026-11-09T03:00:00Z",
        "expires_at": "2026-11-16T03:00:00Z",
        "source_evidence_sha256": "b" * 64,
    }
    return tuple(
        AgentModelConfigRecord(
            id=UUID(int=index),
            created_at=datetime(2026, 11, index, tzinfo=UTC),
            **sol_business_model_fields(price, units_version=COMPACT_PLATFORM_UNITS_VERSION),
        )
        for index, price in enumerate((old_price, new_price), 1)
    )


def test_only_fresh_source_identity_changes_and_original_remains_accountable():
    old, new = pair()
    before = deepcopy(old.platform_profile), deepcopy(new.platform_profile)
    result = require_equivalent_price_renewal(old, new)
    assert result["old_model_id"] == str(old.id)
    assert result["new_model_id"] == str(new.id)
    assert result["old_model_hash"] == old.config_hash
    assert result["new_model_hash"] == new.config_hash
    assert result["old_price_hash"] != result["new_price_hash"]
    assert (old.platform_profile, new.platform_profile) == before


@pytest.mark.parametrize(
    "rate",
    [
        "ordinary_input_usd_per_million",
        "cache_read_usd_per_million",
        "cache_write_30m_usd_per_million",
        "output_usd_per_million",
    ],
)
def test_even_valid_same_model_different_rate_is_not_renewal(rate):
    old, new = pair()
    price = new.platform_profile["price_record"] | {rate: "11"}
    changed = AgentModelConfigRecord(
        id=new.id, **sol_business_model_fields(price, units_version=COMPACT_PLATFORM_UNITS_VERSION)
    )
    with pytest.raises(ValueError, match="PRICE_RENEWAL_INVALID"):
        require_equivalent_price_renewal(old, changed)


@pytest.mark.parametrize(
    "field,value",
    [
        ("max_input_tokens", 900000),
        ("max_output_tokens", 9000),
        ("max_tool_calls", 1),
        ("input_usd_per_million", Decimal("1")),
        ("config_hash", "c" * 64),
    ],
)
def test_tampered_capability_or_hash_rejected(field, value):
    old, new = pair()
    setattr(new, field, value)
    with pytest.raises(ValueError, match="PRICE_RENEWAL_INVALID"):
        require_equivalent_price_renewal(old, new)


def test_valid_output_format_change_is_not_a_price_renewal():
    old, new = pair()
    changed = AgentModelConfigRecord(
        id=new.id, **sol_business_model_fields(new.platform_profile["price_record"])
    )
    with pytest.raises(ValueError, match="PRICE_RENEWAL_INVALID"):
        require_equivalent_price_renewal(old, changed)


def test_same_exact_rates_with_different_decimal_spelling_keep_both_original_hashes():
    old, new = pair()
    price = new.platform_profile["price_record"] | {
        "ordinary_input_usd_per_million": "2.00",
        "cache_read_usd_per_million": "0.1",
        "cache_write_30m_usd_per_million": "2.5",
        "output_usd_per_million": "10.000000",
    }
    changed = AgentModelConfigRecord(
        id=new.id, **sol_business_model_fields(price, units_version=COMPACT_PLATFORM_UNITS_VERSION)
    )
    result = require_equivalent_price_renewal(old, changed)
    assert result["new_model_hash"] == changed.config_hash
    assert changed.platform_profile["price_record"] == price


@pytest.mark.parametrize(
    "fault", ["same_id", "same_record", "backdated", "old_tampered", "unknown_profile_field"]
)
def test_nonrenewal_or_invalid_historical_identity_rejected(fault):
    old, new = pair()
    if fault == "same_id":
        new.id = old.id
    elif fault == "same_record":
        new = AgentModelConfigRecord(
            id=new.id,
            **sol_business_model_fields(
                old.platform_profile["price_record"], units_version=COMPACT_PLATFORM_UNITS_VERSION
            ),
        )
    elif fault == "backdated":
        old, new = new, old
    elif fault == "old_tampered":
        old.config_hash = "c" * 64
    else:
        new.platform_profile["future_permission"] = True
    with pytest.raises(ValueError, match="PRICE_RENEWAL_INVALID"):
        require_equivalent_price_renewal(old, new)
