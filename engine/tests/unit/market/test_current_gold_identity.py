from datetime import UTC, datetime

import pytest
from helpers.current_gold_identity import gold_originals
from nexus.market.current_gold_identity import validate_gold_originals


@pytest.mark.parametrize("change", [None, "viewer"])
def test_gold_trust_preserves_beneficial_share_and_non_equity_type(change):
    proof = validate_gold_originals(
        symbol="GLD", contents=gold_originals(change), now=datetime.now(UTC)
    )
    assert proof.identity.asset_type == "non_equity_etf" and proof.identity.exchange == "ARCX"
    assert proof.identity.underlying_asset_class == "gold_bullion"
    assert len(proof.identity.registration_facts) == 4
    assert "series_id" not in proof.identity.model_dump()


@pytest.mark.parametrize(
    "change",
    [
        "ticker",
        "exchange",
        "stock",
        "treasury",
        "subject",
        "registered",
        "same_row",
        "wrong_viewer",
        "multi_doc",
        "cross_cik",
        "cross_accession",
        "encoded",
        "symbol_format",
        "symbol_target",
        "cik_format",
        "cik_target",
        "cik_tuple",
        "ix_ancestor",
    ],
)
def test_gold_rejects_mismatched_registration_or_gold_subject(change):
    with pytest.raises(ValueError, match="CURRENT_ETF_IDENTITY_INVALID"):
        validate_gold_originals(
            symbol="GLD", contents=gold_originals(change), now=datetime.now(UTC)
        )
