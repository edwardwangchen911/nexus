"""Synthetic registration title/context combinations for stored-reader regression."""

import re

from helpers.current_identity import filing


def title_cover(*, plain):
    raw = filing()
    if plain:
        return re.sub(rb"<xbrli:segment>.*?</xbrli:segment>", b"", raw, flags=re.S).replace(
            b">Common Stock<", b">Class A Common Stock<"
        )
    raw = raw.replace(b"us-gaap:CommonStockMember", b"us-gaap:CommonClassAMember")
    raw = raw.replace(
        b'<xbrldi:explicitMember dimension="dei:EntityListingsExchangeAxis">'
        b"exch:XNYS</xbrldi:explicitMember>",
        b"",
    )
    context = re.search(rb'<xbrli:context id="stock">.*?</xbrli:context>', raw, re.S)[0]
    notes = re.sub(rb"<xbrli:segment>.*?</xbrli:segment>", b"", context, flags=re.S)
    raw = raw.replace(
        b"</ix:header>", notes.replace(b'id="stock"', b'id="notes"') + b"</ix:header>"
    )
    row = re.search(rb"<table><tr>.*?</tr></table>", raw, re.S)[0]
    row = row.replace(b">Common Stock<", b">Notes due 2030<").replace(b">AAPL<", b">SYN2030<")
    row = row.replace(b'contextRef="stock"', b'contextRef="notes"')
    for name in (b"title", b"symbol", b"exchange"):
        row = row.replace(b'id="' + name + b'"', b'id="note-' + name + b'"')
    return raw.replace(b"</body>", row + b"</body>")
