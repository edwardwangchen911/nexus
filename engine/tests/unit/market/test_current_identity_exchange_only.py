"""Synthetic single registration with an unlinked hidden exchange observation."""

import re

import pytest
from helpers.current_identity import archives, filing, profile
from nexus.market.current_identity import load_identity_archives, parse_current_common_stock


def cover():
    raw = re.sub(
        rb'<xbrldi:explicitMember\s+dimension="us-gaap:StatementClassOfStockAxis">'
        rb"us-gaap:CommonStockMember</xbrldi:explicitMember>",
        b"",
        filing(),
    ).replace(b">Common Stock<", b">Common Shares<")
    raw = raw.replace(
        b"<html ", b'<html xmlns:ixt-sec="http://www.sec.gov/inlineXBRL/transformation/2015-08-31" '
    )
    context = re.search(rb'<xbrli:context id="stock">.*?</xbrli:context>', raw, re.S)[0]
    issuer = re.sub(rb"<xbrli:segment>.*?</xbrli:segment>", b"", context, flags=re.S).replace(
        b'id="stock"', b'id="issuer"'
    )
    other = context.replace(b'id="stock"', b'id="other"').replace(b"exch:XNYS", b"exch:XCHI")
    hidden = b"""<ix:hidden><ix:nonNumeric id="other-exchange" name="dei:SecurityExchangeName"
 contextRef="other" format="ixt-sec:exchnameen">Chicago Stock Exchange, Inc</ix:nonNumeric>
</ix:hidden>"""
    return (
        raw.replace(b"</ix:header>", issuer + other + hidden + b"</ix:header>")
        .replace(b'contextRef="stock">0000123456', b'contextRef="issuer">0000123456')
        .replace(b'id="exchange"', b'id="exchange" format="ixt-sec:exchnameen"')
    )


def test_single_visible_exchange_only_archive_preserves_unlinked_raw_evidence(tmp_path):
    fm, sc = archives(tmp_path, sec=cover())
    result = load_identity_archives(symbol="AAPL", fmp_archive=fm, sec_archive=sc).identity
    assert result.symbol == "AAPL" and result.exchange == "XNYS"
    assert result.sec_title == "Common Shares"
    assert [
        (f["attrs"]["dimension"], f["value"])
        for f in result.context["fields"]
        if f["tag"] == "xbrldi:explicitmember"
    ] == [("dei:EntityListingsExchangeAxis", "exch:XNYS")]
    assert len(result.facts) == 5
    aux = next(f for f in result.facts if f["id"] == "other-exchange")
    assert aux["value"] == "Chicago Stock Exchange, Inc"
    assert aux["visible"] is False and aux["identity_linked"] is False
    assert aux["evidence_role"] == "unlinked_exchange"
    assert aux["format"] == "ixt-sec:exchnameen"
    assert aux["context_evidence"]["id"] == "other"
    assert {f["value"] for f in result.facts if f["name"] == "dei:TradingSymbol"} == {"AAPL"}


@pytest.mark.parametrize(
    "before,after",
    [
        (b">Common Shares<", b">Preferred Shares<"),
        (b">Common Shares<", b">Common Stock<"),
        (b">Common Shares<", b">Capital Stock<"),
        (b">Common Shares<", b">Common Shares Notes<"),
        (b">Common Shares<", b">Common Shares Depositary<"),
        (b">Common Shares<", b">Common Shares Class A<"),
        (b">Common Shares<", b">Not Common Shares<"),
        (b"exch:XCHI", b"exch:XNAS"),
        (b"dei:EntityListingsExchangeAxis", b"us-gaap:StatementClassOfStockAxis"),
        (b'id="symbol"', b'id="symbol" style="display:none"'),
        (b">AAPL</ix:nonNumeric>", b'><span hidden="hidden">AAPL</span></ix:nonNumeric>'),
        (b'id="symbol"', b'id="symbol" target="other"'),
        (b"<table>", b'<table xml:base="https://example.invalid/">'),
        (b"<table>", b"<table><tr><td><table>"),
        (b'id="other-exchange"', b'id="other-exchange" tupleRef="other"'),
        (b'id="other-exchange"', b'id="other-exchange" xsi:nil="true"'),
        (b'id="other-exchange"', b'id="other-exchange" continuedAt="tail"'),
        (b'format="ixt-sec:exchnameen"', b'format="other:exchnameen"'),
        (
            b'xmlns:ixt-sec="http://www.sec.gov/inlineXBRL/transformation/2015-08-31"',
            b'xmlns:ixt-sec="https://example.invalid/transform"',
        ),
        (b"http://xbrl.sec.gov/exch/2025", b"https://example.invalid/exch"),
        (b"http://xbrl.sec.gov/dei/2025", b"https://example.invalid/dei"),
        (b"Chicago Stock Exchange, Inc", b"CHX"),
        (b"Chicago Stock Exchange, Inc", b"NASDAQ"),
        (b'<td><ix:nonNumeric id="exchange"', b'</tr><tr><td><ix:nonNumeric id="exchange"'),
        (b"</body>", b'<span style="-sec-ix-hidden:other-exchange">CHX</span></body>'),
        (
            b">AAPL</ix:nonNumeric>",
            b'>AAPL</ix:nonNumeric><ix:nonNumeric id="duplicate" name="dei:TradingSymbol" '
            b'contextRef="stock">AAPL</ix:nonNumeric>',
        ),
        (b"</ix:header>", b'<ix:relationship fromRefs="symbol" toRefs="footnote"/></ix:header>'),
        (b"</ix:header>", b'<ix:relationship fromRefs="footnote" toRefs="issuer"/></ix:header>'),
        (
            b"</ix:header>",
            b'<ix:relationship fromRefs="other-exchange" toRefs="footnote"/></ix:header>',
        ),
        (b"<ix:hidden>", b'<ix:hidden target="other">'),
        (b"<table>", b"<ix:exclude><table>"),
        (b'id="title"', b'id="title" format="ixt-sec:exchnameen"'),
        (b'id="exchange" format="ixt-sec:exchnameen"', b'id="exchange"'),
    ],
)
def test_single_exchange_only_rejects_unsafe_or_expanded_scope(before, after):
    raw = cover()
    assert before in raw
    if after == b"<table><tr><td><table>":
        raw = raw.replace(b"</table>", b"</table></td></tr></table>")
    if after == b"<ix:exclude><table>":
        raw = raw.replace(b"</table>", b"</table></ix:exclude>")
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        parse_current_common_stock(
            symbol="AAPL", fmp_raw=profile(), sec_raw=raw.replace(before, after)
        )


@pytest.mark.parametrize("context", ["stock", "other", "issuer"])
@pytest.mark.parametrize("change", ["cik", "period", "axis", "ancestor", "child", "extra_text"])
def test_single_exchange_only_rejects_context_mismatch(context, change):
    raw = cover()
    before = re.search(
        rb'<xbrli:context id="' + context.encode() + rb'">.*?</xbrli:context>', raw, re.S
    )[0]
    if change == "cik":
        after = before.replace(b"0000123456", b"0000654321")
    elif change == "period":
        after = before.replace(b"2025-01-01", b"2024-01-01")
    elif change == "axis":
        after = before.replace(
            b"</xbrli:entity>",
            b'<xbrli:segment><xbrldi:explicitMember dimension="us-gaap:StatementClassOfStockAxis">'
            b"us-gaap:CommonStockMember</xbrldi:explicitMember></xbrli:segment></xbrli:entity>",
        )
    elif change == "ancestor":
        after = b"<ix:exclude>" + before + b"</ix:exclude>"
    elif change == "extra_text":
        after = before.replace(b"<xbrli:entity>", b"<xbrli:entity>unexpected text")
    else:
        after = before.replace(
            b"<xbrli:entity>", b'<xbrli:entity xml:base="https://example.invalid/">'
        )
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        parse_current_common_stock(
            symbol="AAPL", fmp_raw=profile(), sec_raw=raw.replace(before, after)
        )
