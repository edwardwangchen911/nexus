"""Synthetic separate preferred registration; no captured issuer facts."""

import re

import pytest
from helpers.current_identity import archives, filing, profile
from nexus.market.current_identity import load_identity_archives, parse_current_common_stock


def cover():
    raw = filing().replace(
        b"<html ",
        b'<html xmlns:sample="https://example.invalid/2025" '
        b'xmlns:ixt-sec="http://www.sec.gov/inlineXBRL/transformation/2015-08-31" ',
    )
    raw = raw.replace(
        b'<xbrldi:explicitMember dimension="dei:EntityListingsExchangeAxis">'
        b"exch:XNYS</xbrldi:explicitMember>",
        b"",
    )
    context = re.search(rb'<xbrli:context id="stock">.*?</xbrli:context>', raw, re.S)[0]
    other = context.replace(b'id="stock"', b'id="preferred"').replace(
        b"us-gaap:CommonStockMember", b"sample:SeparateSecurityMember"
    )
    hidden = (
        b'<ix:hidden><ix:nonNumeric id="preferred-symbol" name="dei:TradingSymbol" '
        b'contextRef="preferred">SYN.PA</ix:nonNumeric></ix:hidden>'
    )
    raw = raw.replace(b"</ix:header>", other + hidden + b"</ix:header>")
    raw = raw.replace(b'id="exchange"', b'id="exchange" format="ixt-sec:exchnameen"')
    return raw.replace(
        b"</table>",
        b"""<tr>
<td><ix:nonNumeric id="preferred-title" name="dei:Security12bTitle"
 contextRef="preferred">Depositary Shares representing Preferred Stock</ix:nonNumeric></td>
<td><span style="-sec-ix-hidden:preferred-symbol">SYN PR A*</span></td>
<td><ix:nonNumeric id="preferred-exchange" name="dei:SecurityExchangeName"
 contextRef="preferred" format="ixt-sec:exchnameen">New York Stock Exchange</ix:nonNumeric></td>
</tr></table>""",
    )


@pytest.mark.parametrize("styled", [False, True])
def test_visible_target_uses_normal_archive_reader_and_preserves_unequal_side_evidence(
    tmp_path, styled
):
    raw = cover()
    if styled:
        raw = raw.replace(
            b">SYN PR A*</span>",
            b'><span style="color:#000000;'
            b'white-space:pre-wrap;font-kerning:none;min-width:fit-content;">'
            b"SYN PR A*</span></span>",
        )
    fm, sc = archives(tmp_path, sec=raw)
    identity = load_identity_archives(symbol="AAPL", fmp_archive=fm, sec_archive=sc).identity
    assert identity.symbol == "AAPL" and identity.context_id == "stock"
    assert identity.sec_title == "Common Stock" and identity.exchange == "XNYS"
    hidden = next(f for f in identity.facts if f["id"] == "preferred-symbol")
    assert hidden["value"] == "SYN.PA"
    assert hidden["display_reference"]["value"] == "SYN PR A*"
    assert hidden["display_reference"]["matches_fact_value"] is False
    assert hidden["selected_listing"] is False
    assert len(identity.facts) == 7


@pytest.mark.parametrize(
    "before,after",
    [
        (b">SYN.PA<", b">AAPL<"),
        (b'id="symbol"', b'id="symbol" style="display:none"'),
        (b'contextRef="preferred"', b'contextRef="stock"'),
        (b"-sec-ix-hidden:preferred-symbol", b"-sec-ix-hidden:missing"),
        (b"<span style=", b'<span hidden="hidden" style='),
        (b'<span style="', b'<span style="display:none;'),
        (b"</span>", b'</span><span style="-sec-ix-hidden:preferred-symbol">SYN PA</span>'),
        (
            b"-sec-ix-hidden:preferred-symbol",
            b"-sec-ix-hidden:preferred-symbol;-sec-ix-hidden:preferred-symbol",
        ),
        (b"<td><span", b"</tr><tr><td><span"),
        (b'id="symbol"', b'id="symbol" target="other"'),
        (b'id="preferred-symbol"', b'id="preferred-symbol" tupleRef="other"'),
        (b'id="preferred-symbol"', b'id="preferred-symbol" format="other:format"'),
        (b"<ix:hidden>", b'<ix:hidden xml:base="https://example.invalid/">'),
        (b"<ix:hidden>", b'<ix:hidden target="other">'),
        (b">AAPL</ix:nonNumeric>", b"><ix:exclude>AAPL</ix:exclude></ix:nonNumeric>"),
        (
            b'<span style="-sec-ix-hidden:preferred-symbol">SYN PR A*</span>',
            b'<ix:exclude><span style="-sec-ix-hidden:preferred-symbol">'
            b"SYN PR A*</span></ix:exclude>",
        ),
        (b">SYN PR A*</span>", b"><ix:exclude>SYN PR A*</ix:exclude></span>"),
        (b">SYN PR A*</span>", b'><span style="display:none">SYN PR A*</span></span>'),
        (b">AAPL</ix:nonNumeric>", b'><span style="display:none">AAPL</span></ix:nonNumeric>'),
        (b">AAPL</ix:nonNumeric>", b'><span hidden="hidden">AAPL</span></ix:nonNumeric>'),
        (b">AAPL</ix:nonNumeric>", b'><span style="visibility:hidden">AAPL</span></ix:nonNumeric>'),
        (
            b">Common Stock</ix:nonNumeric>",
            b">Capital Stock, par value $0.01 per share</ix:nonNumeric>",
        ),
        (
            b">AAPL</ix:nonNumeric>",
            b">AAPL</ix:nonNumeric><ix:nonNumeric "
            b'id="duplicate-symbol" name="dei:TradingSymbol" contextRef="stock">'
            b"AAPL</ix:nonNumeric>",
        ),
        (
            b"<ix:hidden>",
            b'<ix:hidden><ix:nonNumeric id="unbound-symbol" '
            b'name="dei:TradingSymbol" contextRef="preferred">SYN.PB</ix:nonNumeric>',
        ),
        (b'id="preferred-symbol"', b'id="preferred-symbol" xsi:nil="true"'),
        (b'id="preferred-symbol"', b'id="preferred-symbol" continuedAt="continuation"'),
        (b"ixt-sec:exchnameen", b"ixt-sec:unknown"),
        (b"http://fasb.org/us-gaap/2025", b"https://example.invalid/us-gaap"),
        (b"https://example.invalid/2025", b"http://fasb.org/us-gaap/2025"),
        (b"Depositary Shares representing Preferred Stock", b"Common Stock"),
        (b'<td><ix:nonNumeric id="exchange"', b'</tr><tr><td><ix:nonNumeric id="exchange"'),
    ],
)
def test_unaccounted_hidden_or_ambiguous_scope_is_rejected(before, after):
    raw = cover()
    assert before in raw
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_INVALID"):
        parse_current_common_stock(
            symbol="AAPL", fmp_raw=profile(), sec_raw=raw.replace(before, after)
        )


@pytest.mark.parametrize(
    "before,after",
    [
        (b"0000123456", b"0000654321"),
        (b"2025-01-01", b"2024-01-01"),
        (b"sample:SeparateSecurityMember", b"us-gaap:CommonStockMember"),
    ],
)
def test_other_context_does_not_borrow_target_identity_or_period(before, after):
    raw = cover()
    other = re.search(rb'<xbrli:context id="preferred">.*?</xbrli:context>', raw, re.S)[0]
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_INVALID"):
        parse_current_common_stock(
            symbol="AAPL",
            fmp_raw=profile(),
            sec_raw=raw.replace(other, other.replace(before, after)),
        )
