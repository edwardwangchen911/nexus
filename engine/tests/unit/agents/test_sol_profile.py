"""Normal Sol profile identity and historical fee consumption, no dispatch."""

from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest
from nexus.agents import platform_profile as profiles
from nexus.agents.platform_pricing import sol_price_record
from nexus.agents.platform_stage_accounting import require_platform_stage_cost
from unit.agents.test_sol_pricing import sol_price_record as record_fixture
from unit.agents.test_sol_stage_accounting import sol_row


def test_normal_sol_profile_freezes_prices_and_does_not_reuse_experiment_authority():
    original = record_fixture()
    fields = profiles.sol_business_model_fields(original)
    model = SimpleNamespace(**fields)
    assert model.model_id == "gpt-6.1-sol"
    assert model.input_usd_per_million == Decimal("2.5")
    assert model.output_usd_per_million == Decimal("10")
    assert (model.max_input_tokens, model.max_output_tokens, model.max_tool_calls) == (
        816000,
        8192,
        0,
    )
    assert profiles.sol_business_model_matches(model)
    assert not profiles.platform_model_matches(model)
    assert model.platform_profile["price_record"] == original
    assert model.platform_profile["wire_profile"]["effort"] == "medium"
    assert require_platform_stage_cost(model, [sol_row()], 12000, 1000) == Decimal("0.0299")


@pytest.mark.parametrize(
    "field,value",
    [
        ("model_id", "gpt-5.6-terra"),
        ("max_output_tokens", 9000),
        ("max_input_tokens", 900000),
        ("input_usd_per_million", Decimal("2")),
        ("output_usd_per_million", Decimal("12")),
        ("config_hash", "b" * 64),
        ("version", "unbound"),
        ("pricing_expires_at", datetime(2030, 1, 1, tzinfo=UTC)),
    ],
)
def test_model_mutation_cannot_change_bound_sol_settlement(field, value):
    fields = profiles.sol_business_model_fields(record_fixture())
    fields[field] = value
    model = SimpleNamespace(**fields)
    assert not profiles.sol_business_model_matches(model)
    with pytest.raises(ValueError, match="PLATFORM_USAGE_ACCOUNTING_UNCONFIRMED"):
        require_platform_stage_cost(model, [sol_row()], 12000, 1000)


def test_renewal_freezes_new_identity_and_old_record_remains_accountable():
    old = profiles.sol_business_model_fields(record_fixture())
    before = deepcopy(old)
    new_record = record_fixture() | {
        "verified_at": "2026-11-09T02:00:00Z",
        "expires_at": "2026-11-16T02:00:00Z",
    }
    new = profiles.sol_business_model_fields(new_record)
    assert new["version"] != old["version"] and new["config_hash"] != old["config_hash"]
    assert old == before
    assert require_platform_stage_cost(SimpleNamespace(**old), [sol_row()], 12000, 1000) == Decimal(
        "0.0299"
    )
    with pytest.raises(ValueError, match="PLATFORM_USAGE_ACCOUNTING_UNCONFIRMED"):
        require_platform_stage_cost(SimpleNamespace(**new), [sol_row()], 12000, 1000)


def test_price_window_covers_original_execution_deadline_but_not_historical_readback():
    record = sol_price_record(record_fixture())
    record.require_current(
        now=datetime(2026, 11, 2, 3, tzinfo=UTC), deadline=datetime(2026, 11, 2, 4, tzinfo=UTC)
    )
    for now, deadline in [
        (datetime(2026, 11, 2, 1, tzinfo=UTC), datetime(2026, 11, 2, 4, tzinfo=UTC)),
        (record.expires_at, datetime(2026, 11, 10, tzinfo=UTC)),
        (datetime(2026, 11, 2, 3, tzinfo=UTC), datetime(2026, 11, 10, tzinfo=UTC)),
    ]:
        with pytest.raises(ValueError, match="PLATFORM_PRICE_WINDOW_INVALID"):
            record.require_current(now=now, deadline=deadline)
