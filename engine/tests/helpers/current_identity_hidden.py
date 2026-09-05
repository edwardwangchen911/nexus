"""Synthetic hidden ancillary registration for normal storage integration."""

import re

from helpers.current_identity import filing


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

