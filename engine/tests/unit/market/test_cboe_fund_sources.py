import importlib
import json
from datetime import timedelta
from hashlib import sha256

import pytest
from helpers.cboe_fund_sources import OBJECTIVE, STRATEGY, directories, source_chain_contents
from helpers.cboe_source_chain import NOW


def validate(contents=None, directory_contents=None):
    module = importlib.import_module("nexus.market.cboe_fund_sources")
    return module.validate_cboe_fund_sources(
        symbol="TEST",
        contents=contents or source_chain_contents(),
        directory_contents=directory_contents or directories(),
        now=NOW,
    )


def test_missing_directory_record_keeps_exact_sec_binding_and_leverage_classification():
    result = validate()
    assert result.fund_directory_status == "absent"
    assert result.strategy_kind == "daily_2x_long_single_stock"
    assert result.asset_type == "equity_etf" and result.leverage_factor == 2
    assert result.underlying_symbol == "BASE"
    assert result.identity_registration_ready is False
    assert result.source_chain.submission.class_name == "Synthetic Daily 2X Long ETF"


def test_later_exact_fund_directory_row_can_confirm_same_class():
    result = validate(
        directory_contents=directories(fund=[[123456, "S000123456", "C000123456", "TEST"]])
    )
    assert result.fund_directory_status == "exact_match"


def test_official_full_fee_heading_does_not_truncate_before_fees_phrase_in_objective():
    values = list(source_chain_contents())
    values[2] = values[2].replace(
        b"<p>Fees and Expenses</p>", b"<p>Fees and Expenses of the Fund</p>"
    )
    receipt = json.loads(values[3])
    receipt["sha256"] = sha256(values[2]).hexdigest()
    receipt["byte_length"] = len(values[2])
    values[3] = json.dumps(receipt).encode()
    result = validate(contents=tuple(values))
    assert "before fees and expenses" in result.objective
    assert "two times (200%)" in result.objective


@pytest.mark.parametrize(
    "rows",
    [
        [[654321, "S000123456", "C000123456", "TEST"]],
        [[123456, "S000999999", "C000123456", "TEST"]],
        [[123456, "S000123456", "C000999999", "TEST"]],
        [[123456, "S000123456", "C000123456", "TEST"]] * 2,
        [[123456, "S000123456", "C000123456"]],
        [],
    ],
)
def test_conflicting_duplicate_partial_or_empty_fund_directory_rejects(rows):
    with pytest.raises(ValueError, match="CBOE_FUND_SOURCES_INVALID"):
        validate(directory_contents=directories(fund=rows))


def test_company_directory_collision_is_not_ignored():
    with pytest.raises(ValueError, match="CBOE_FUND_SOURCES_INVALID"):
        validate(
            directory_contents=directories(company=[[123456, "Synthetic Trust", "TEST", "Nasdaq"]])
        )


@pytest.mark.parametrize(
    "objective,strategy",
    [
        (OBJECTIVE.replace("two times (200%)", "one time (100%)"), STRATEGY),
        (OBJECTIVE.replace("daily", "monthly"), STRATEGY),
        (
            OBJECTIVE,
            STRATEGY.replace(
                "swap agreements and/or listed options contracts", "commodity futures contracts"
            ),
        ),
        (OBJECTIVE.replace("share price", "bond price"), STRATEGY),
    ],
)
def test_strategy_not_in_supported_single_stock_leverage_contract_rejected(objective, strategy):
    with pytest.raises(ValueError, match="CBOE_FUND_SOURCES_INVALID"):
        validate(contents=source_chain_contents(objective, strategy))


def test_directory_receipt_stale_or_source_mismatch_is_not_current_evidence():
    values = list(directories())
    receipt = json.loads(values[1])
    receipt["received_at"] = (NOW - timedelta(days=2)).isoformat()
    values[1] = json.dumps(receipt).encode()
    with pytest.raises(ValueError, match="CBOE_FUND_SOURCES_INVALID"):
        validate(directory_contents=tuple(values))


def _replace_submission(contents, old, new):
    values = list(contents)
    values[2] = values[2].replace(old, new)
    receipt = json.loads(values[3])
    receipt["sha256"] = sha256(values[2]).hexdigest()
    receipt["byte_length"] = len(values[2])
    values[3] = json.dumps(receipt).encode()
    return tuple(values)


@pytest.mark.parametrize(
    "heading",
    [
        "Investment Objective",
        "Fees and Expenses",
        "Principal Investment Strategies",
        "Principal Investment Risks",
    ],
)
def test_duplicate_classification_heading_is_not_silently_ignored(heading):
    contents = _replace_submission(
        source_chain_contents(),
        b"</body>",
        ("<p>" + heading + "</p></body>").encode(),
    )
    with pytest.raises(ValueError, match="CBOE_FUND_SOURCES_INVALID"):
        validate(contents=contents)


def test_second_conflicting_objective_and_strategy_cannot_use_first_sections():
    extra = (
        b"<p>Investment Objective</p><p>The Fund seeks inverse daily exposure to ELSE.</p>"
        b"<p>Fees and Expenses</p><p>Principal Investment Strategies</p>"
        b"<p>The Fund seeks negative daily returns using an inverse strategy.</p>"
        b"<p>Principal Investment Risks</p>"
    )
    contents = _replace_submission(source_chain_contents(), b"</body>", extra + b"</body>")
    with pytest.raises(ValueError, match="CBOE_FUND_SOURCES_INVALID"):
        validate(contents=contents)


@pytest.mark.parametrize(
    "rows",
    [
        [[123456, "S000123456", "C000123456", "OTHER"]],
        [
            [123456, "S000123456", "C000123456", "TEST"],
            [123456, "S000123456", "C000123456", "OTHER"],
        ],
        [[654321, "S000999999", "C000123456", "OTHER"]],
    ],
)
def test_current_class_cannot_map_to_a_different_or_multiple_tickers(rows):
    with pytest.raises(ValueError, match="CBOE_FUND_SOURCES_INVALID"):
        validate(directory_contents=directories(fund=rows))


@pytest.mark.parametrize(
    "objective,strategy",
    [
        (OBJECTIVE.replace("seeks", "does not seek"), STRATEGY),
        (OBJECTIVE.replace("two times", "negative two times"), STRATEGY),
        (OBJECTIVE.replace("two times", "minus two times"), STRATEGY),
        (
            OBJECTIVE
            + " The exposure also includes the daily percentage change in the share price "
            "of Another Issuer (NYSE: ELSE).",
            STRATEGY,
        ),
        (OBJECTIVE + " The Fund instead seeks three times (300%) daily results.", STRATEGY),
        (OBJECTIVE, STRATEGY.replace("attempts to achieve", "does not attempt to achieve")),
        (OBJECTIVE, STRATEGY.replace("two times", "negative two times")),
        (
            OBJECTIVE,
            STRATEGY + " The Fund instead seeks three times (300%) daily results.",
        ),
        (
            OBJECTIVE,
            STRATEGY + " The Fund does not seek to achieve two times (200%) daily results.",
        ),
        (
            OBJECTIVE,
            STRATEGY + " The Fund also seeks exposure to the daily percentage change in the share "
            "price of Another Issuer (NYSE: ELSE).",
        ),
        (
            OBJECTIVE,
            STRATEGY + " The Fund instead seeks monthly investment results of two times (200%).",
        ),
        (
            OBJECTIVE,
            STRATEGY
            + " The Fund seeks the inverse of the daily percentage change in the share price "
            "of the Underlying Security.",
        ),
        (
            OBJECTIVE + " The Fund also follows Example Issuer (Nasdaq: BASE).",
            STRATEGY,
        ),
    ],
)
def test_negated_inverse_multiple_stock_or_conflicting_target_is_rejected(objective, strategy):
    with pytest.raises(ValueError, match="CBOE_FUND_SOURCES_INVALID"):
        validate(contents=source_chain_contents(objective, strategy))


def test_daily_period_disclaimer_preserves_positive_single_stock_target():
    objective = (
        OBJECTIVE + " The Fund does not seek to achieve its stated investment objective "
        "over a period of time greater than a single day."
    )
    result = validate(contents=source_chain_contents(objective, STRATEGY))
    assert result.underlying_symbol == "BASE"
    assert result.strategy_kind == "daily_2x_long_single_stock"
