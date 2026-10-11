"""Synthetic archive and broker facts only; no real account content."""

import importlib
from datetime import timedelta

import pytest
from helpers.cboe_fund_sources import directories, source_chain_contents
from helpers.cboe_holding_evidence import pages
from helpers.cboe_source_chain import NOW


def freeze(accounts=None, holdings=None):
    a, h = pages()
    module = importlib.import_module("nexus.market.cboe_holding_evidence")
    return module.freeze_cboe_holding_evidence(
        symbol="TEST",
        contents=source_chain_contents(),
        directory_contents=directories(),
        accounts_page=accounts or a,
        holdings_page=holdings or h,
        now=NOW,
    )


def test_fresh_broker_binding_and_official_sources_roundtrip_without_account_details():
    m = importlib.import_module("nexus.market.cboe_holding_evidence")
    evidence = freeze()
    assert evidence.identity.exchange == "BATS"
    assert evidence.identity.strategy_kind == "daily_2x_long_single_stock"
    assert evidence.identity.legal_name == "Synthetic Daily 2X Long ETF"
    assert len(evidence.artifact_sha256) == 18
    assert "synthetic-private-account" not in evidence.model_dump_json()
    assert "quantity" not in evidence.model_dump_json()
    assert evidence.new_position_eligible is False
    assert evidence.universe_published is False
    assert evidence.execution_locked is True
    result = m.read_cboe_holding_evidence(
        evidence.model_dump(mode="json"),
        contents=source_chain_contents(),
        directory_contents=directories(),
        now=NOW + timedelta(days=2),
    )
    assert result == evidence


@pytest.mark.parametrize(
    "change",
    [
        {"quality": "PASS"},
        {"has_more": True, "next_cursor": "synthetic-next"},
        {"as_of": NOW - timedelta(minutes=6)},
    ],
)
def test_invalid_broker_page_cannot_create_evidence(change):
    _, h = pages()
    with pytest.raises(ValueError, match="CBOE_HOLDING_EVIDENCE_INVALID"):
        freeze(holdings=h.model_copy(update=change))


@pytest.mark.parametrize(
    "change",
    [
        {"new_position_eligible": True},
        {"universe_published": True},
        {"execution_locked": False},
        {"historical_backtest_eligible": True},
        {"schema_version": "current_held_etf_identity_v1"},
        {"source_revision": "0" * 64},
        {"checked_at": (NOW + timedelta(minutes=1)).isoformat()},
        {"artifact_sha256": ["0" * 64] * 18},
        {"identity": {"symbol": "OTHER"}},
    ],
)
def test_tampered_payload_is_rejected(change):
    m = importlib.import_module("nexus.market.cboe_holding_evidence")
    payload = freeze().model_dump(mode="json")
    payload.update(change)
    with pytest.raises(ValueError, match="CBOE_HOLDING_EVIDENCE_INVALID"):
        m.read_cboe_holding_evidence(
            payload, contents=source_chain_contents(), directory_contents=directories(), now=NOW
        )


def test_changed_original_and_stale_frozen_broker_binding_are_rejected():
    m = importlib.import_module("nexus.market.cboe_holding_evidence")
    payload = freeze().model_dump(mode="json")
    contents = list(source_chain_contents())
    contents[2] += b"\n"
    with pytest.raises(ValueError, match="CBOE_HOLDING_EVIDENCE_INVALID"):
        m.read_cboe_holding_evidence(
            payload, contents=tuple(contents), directory_contents=directories(), now=NOW
        )
    payload["holding"]["holdings_as_of"] = (NOW - timedelta(minutes=6)).isoformat()
    with pytest.raises(ValueError, match="CBOE_HOLDING_EVIDENCE_INVALID"):
        m.read_cboe_holding_evidence(
            payload, contents=source_chain_contents(), directory_contents=directories(), now=NOW
        )


@pytest.mark.parametrize(
    "change",
    [
        {"quantity": "0"},
        {"external_account_id": "another-synthetic-account"},
        {"instrument_code": "usOTHER"},
        {"currency": "HKD"},
    ],
)
def test_unheld_or_mismatched_security_cannot_bind(change):
    _, h = pages()
    item = {**h.items[0], **change}
    with pytest.raises(ValueError, match="CBOE_HOLDING_EVIDENCE_INVALID"):
        freeze(holdings=h.model_copy(update={"items": (item,)}))


def test_ambiguous_holding_or_accounts_are_rejected():
    a, h = pages()
    for kwargs in (
        {"holdings": h.model_copy(update={"items": h.items * 2})},
        {"accounts": a.model_copy(update={"items": a.items * 2})},
    ):
        with pytest.raises(ValueError, match="CBOE_HOLDING_EVIDENCE_INVALID"):
            freeze(**kwargs)


def test_frozen_binding_code_and_digest_are_checked():
    m = importlib.import_module("nexus.market.cboe_holding_evidence")
    for change in ({"provider_code": "usOTHER"}, {"sha256": "0" * 64}):
        payload = freeze().model_dump(mode="json")
        payload["holding"].update(change)
        with pytest.raises(ValueError, match="CBOE_HOLDING_EVIDENCE_INVALID"):
            m.read_cboe_holding_evidence(
                payload, contents=source_chain_contents(), directory_contents=directories(), now=NOW
            )
