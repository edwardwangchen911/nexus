"""Synthetic current UIT originals; no captured business data."""

from datetime import UTC, datetime

import pytest
from helpers.current_uit_identity import originals
from nexus.market.current_uit_identity import validate_uit_originals


def test_current_uit_uses_registered_ticker_and_preserves_provider_name():
    result = validate_uit_originals(symbol="SPY", contents=originals(), now=datetime.now(UTC))
    assert result.identity.legal_structure == "unit_investment_trust"
    assert result.identity.exchange == "ARCX"
    assert result.identity.fmp_name_matches_sec is False
    assert "series_id" not in result.identity.model_dump()


@pytest.mark.parametrize(
    "change",
    ["ticker", "cik", "form", "name", "listing", "bond", "future", "duplicate", "wrong_link"],
)
def test_current_uit_rejects_unbound_or_non_equity_originals(change):
    with pytest.raises(ValueError, match="CURRENT_ETF_IDENTITY_INVALID"):
        validate_uit_originals(symbol="SPY", contents=originals(change), now=datetime.now(UTC))


@pytest.mark.parametrize("case", ["digest", "future_receipt", "detached_strategy"])
def test_uit_rejects_changed_custody_or_detached_subject(case):
    import json
    from hashlib import sha256

    values = list(originals())
    if case == "digest":
        values[6] += b"changed"
    elif case == "future_receipt":
        receipt = json.loads(values[3])
        receipt["received_at"] = "2099-01-01T00:00:00+00:00"
        values[3] = json.dumps(receipt).encode()
    else:
        values[6] = values[6].replace(
            "The Trust’s Investments and Portfolio Turnover".encode(), b"Another fund portfolio"
        )
        receipt = json.loads(values[7])
        receipt["sha256"] = sha256(values[6]).hexdigest()
        receipt["byte_length"] = len(values[6])
        values[7] = json.dumps(receipt).encode()
    with pytest.raises(ValueError, match="CURRENT_ETF_IDENTITY_INVALID"):
        validate_uit_originals(symbol="SPY", contents=tuple(values), now=datetime.now(UTC))


def test_dia_independent_djia_objective_and_trademarked_former_name():
    from helpers.current_uit_identity import dia_originals

    proof = validate_uit_originals(symbol="DIA", contents=dia_originals(), now=datetime.now(UTC))
    assert proof.identity.symbol == "DIA" and proof.identity.exchange == "ARCX"
    assert "DJIA" in proof.identity.objective and "S&P 500" not in proof.identity.objective
    assert " SM " not in proof.identity.former_name


@pytest.mark.parametrize("case", ["spy_objective", "spy_strategy", "wrong_cover", "wrong_former"])
def test_dia_rejects_cross_fund_objective_and_identity(case):
    import json
    from hashlib import sha256

    from helpers.current_uit_identity import dia_originals

    values = list(dia_originals())
    if case == "spy_objective":
        values[6] = values[6].replace(
            "Dow Jones Industrial Average (the “DJIA” or “Index”)".encode(),
            "S&amp;P 500 Index (the “Index”)".encode(),
        )
    elif case == "spy_strategy":
        values[6] = values[6].replace(b"included in the DJIA", b"included in the Index")
    elif case == "wrong_cover":
        values[6] = values[6].replace(b"DIA", b"SPY")
    else:
        values[6] = values[6].replace(b"Average SM", b"Average OTHER")
    receipt = json.loads(values[7])
    receipt["sha256"] = sha256(values[6]).hexdigest()
    receipt["byte_length"] = len(values[6])
    values[7] = json.dumps(receipt).encode()
    with pytest.raises(ValueError, match="CURRENT_ETF_IDENTITY_INVALID"):
        validate_uit_originals(symbol="DIA", contents=tuple(values), now=datetime.now(UTC))


def test_spy_still_rejects_djia_objective_with_unchanged_own_cover():
    import json
    from hashlib import sha256

    values = list(originals())
    values[6] = values[6].replace(
        "S&amp;P 500 Index (the “Index”)".encode(),
        "Dow Jones Industrial Average (the “DJIA” or “Index”)".encode(),
    )
    receipt = json.loads(values[7])
    receipt["sha256"] = sha256(values[6]).hexdigest()
    receipt["byte_length"] = len(values[6])
    values[7] = json.dumps(receipt).encode()
    with pytest.raises(ValueError, match="CURRENT_ETF_IDENTITY_INVALID"):
        validate_uit_originals(symbol="SPY", contents=tuple(values), now=datetime.now(UTC))
