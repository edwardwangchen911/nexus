"""Synthetic unlinked exchange evidence for normal storage integration."""

import re

from helpers.current_identity import filing


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
