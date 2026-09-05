"""Synthetic eight-row cover with one unrelated hidden title; no captured issuer data."""

import re

from helpers.current_identity import filing


def cover() -> bytes:
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
    extra_contexts = b"".join(
        context.replace(b'id="stock"', f'id="security-{index}"'.encode()).replace(
            b"us-gaap:CommonStockMember", f"sample:Security{index}Member".encode()
        )
        for index in range(1, 8)
    )
    cik_fact = re.search(rb'<ix:nonNumeric id="cik".*?</ix:nonNumeric>', raw, re.S)[0]
    raw = raw.replace(cik_fact, b"")
    hidden = (
        b"<ix:hidden>"
        + cik_fact
        + b'<ix:nonNumeric id="security-7-title" name="dei:Security12bTitle" '
        b'contextRef="security-7">Senior Notes</ix:nonNumeric></ix:hidden>'
    )
    raw = raw.replace(b"</ix:header>", extra_contexts + hidden + b"</ix:header>")
    raw = raw.replace(
        b'id="exchange" name="dei:SecurityExchangeName"',
        b'id="exchange" name="dei:SecurityExchangeName" format="ixt-sec:exchnameen"',
    )
    rows = []
    for index in range(1, 8):
        title = (
            b""
            if index == 7
            else (
                f'<td><ix:nonNumeric id="security-{index}-title" '
                f'name="dei:Security12bTitle" contextRef="security-{index}">'
                f"Class {index} Security</ix:nonNumeric></td>"
            ).encode()
        )
        rows.append(
            b"<tr>"
            + title
            + (
                f'<td><ix:nonNumeric id="security-{index}-symbol" name="dei:TradingSymbol" '
                f'contextRef="security-{index}">SYN{index}</ix:nonNumeric></td>'
                f'<td><ix:nonNumeric id="security-{index}-exchange" '
                f'name="dei:SecurityExchangeName" contextRef="security-{index}" '
                'format="ixt-sec:exchnameen">'
                "New York Stock Exchange</ix:nonNumeric></td></tr>"
            ).encode()
        )
    return raw.replace(b"</table>", b"".join(rows) + b"</table>")
