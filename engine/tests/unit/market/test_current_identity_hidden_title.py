"""One exact multi-security cover where the only hidden title is unrelated to the target."""

import pytest
from helpers.current_identity import profile
from helpers.current_identity_hidden_title import cover
from nexus.market.current_identity import parse_current_common_stock


def test_target_record_never_borrows_the_unrelated_hidden_title():
    identity = parse_current_common_stock(symbol="AAPL", fmp_raw=profile(), sec_raw=cover())
    assert identity.symbol == "AAPL"
    assert identity.sec_title == "Common Stock"
    target = [fact for fact in identity.facts if fact.get("selected_listing") is True]
    hidden = [
        fact for fact in identity.facts if fact.get("evidence_role") == "unrelated_hidden_title"
    ]
    assert {fact["name"] for fact in target} == {
        "dei:Security12bTitle",
        "dei:TradingSymbol",
        "dei:SecurityExchangeName",
    }
    assert len(hidden) == 1
    assert hidden[0]["selected_listing"] is False
    assert hidden[0]["identity_linked"] is False
    assert all(
        fact.get("selected_listing") is False
        for fact in identity.facts
        if fact.get("name") != "dei:EntityCentralIndexKey" and fact not in target
    )


@pytest.mark.parametrize(
    ("before", "after"),
    [
        (b'contextRef="security-7">Senior Notes', b'contextRef="stock">Senior Notes'),
        (b">SYN1</ix:nonNumeric>", b">AAPL</ix:nonNumeric>"),
        (b">Common Stock</ix:nonNumeric>", b">Senior Notes</ix:nonNumeric>"),
        (
            b'id="exchange" name="dei:SecurityExchangeName" format="ixt-sec:exchnameen" '
            b'contextRef="stock"',
            b'id="exchange" name="dei:SecurityExchangeName" format="ixt-sec:exchnameen" '
            b'contextRef="security-1"',
        ),
        (b'id="security-7-title"', b'id="security-7-title" target="other"'),
        (
            b'name="dei:Security12bTitle" contextRef="security-7"',
            b'name="dei:TradingSymbol" contextRef="security-7"',
        ),
        (
            b">SYN7</ix:nonNumeric>",
            b'>SYN7</ix:nonNumeric><ix:nonNumeric id="extra" '
            b'name="dei:TradingSymbol" contextRef="security-7">OTHER</ix:nonNumeric>',
        ),
        (
            b"<xbrli:endDate>2025-12-31</xbrli:endDate>",
            b"<xbrli:endDate>2024-12-31</xbrli:endDate>",
        ),
    ],
)
def test_unrelated_hidden_title_shape_does_not_relax_identity_binding(before, after):
    raw = cover()
    assert before in raw
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_INVALID"):
        parse_current_common_stock(
            symbol="AAPL", fmp_raw=profile(), sec_raw=raw.replace(before, after, 1)
        )


def test_hidden_title_must_not_become_visible_unscoped_material():
    raw = cover().replace(
        b'<ix:nonNumeric id="security-7-title" name="dei:Security12bTitle" '
        b'contextRef="security-7">Senior Notes</ix:nonNumeric>',
        b'</ix:hidden><ix:nonNumeric id="security-7-title" name="dei:Security12bTitle" '
        b'contextRef="security-7">Senior Notes</ix:nonNumeric><ix:hidden>',
    )
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_INVALID"):
        parse_current_common_stock(symbol="AAPL", fmp_raw=profile(), sec_raw=raw)


def test_each_complete_non_target_row_requires_the_exact_listing_fact_names():
    raw = cover().replace(
        b'id="security-1-symbol" name="dei:TradingSymbol"',
        b'id="security-1-symbol" name="dei:Security12bTitle"',
    )
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_INVALID"):
        parse_current_common_stock(symbol="AAPL", fmp_raw=profile(), sec_raw=raw)


def test_custom_members_are_unique_by_expanded_name_not_lexical_prefix():
    raw = cover().replace(
        b'xmlns:sample="https://example.invalid/2025"',
        b'xmlns:sample="https://example.invalid/2025" '
        b'xmlns:alias="https://example.invalid/2025"',
    ).replace(b"sample:Security2Member", b"alias:Security1Member")
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_INVALID"):
        parse_current_common_stock(symbol="AAPL", fmp_raw=profile(), sec_raw=raw)
