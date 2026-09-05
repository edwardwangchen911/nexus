"""Synthetic gold trust archives with closed registered cover facts."""

import json
import re
from hashlib import sha256

from helpers.current_identity import filing
from helpers.current_uit_identity import originals


def gold_originals(change=None):
    values = [raw.replace(b"SPY", b"GLD") for raw in originals()]
    sub = json.loads(values[2])
    sub["name"] = "SYNTHETIC GOLD TRUST"
    sub["filings"]["recent"]["form"] = ["10-K"]
    values[2] = json.dumps(sub).encode()
    values[4] = values[4].replace(b"Form 497", b"Form 10-K")
    if change in {"viewer", "wrong_viewer", "multi_doc", "cross_cik", "cross_accession", "encoded"}:
        prefix = b"https://example.com/ix?doc=" if change == "wrong_viewer" else b"/ix?doc="
        values[4] = values[4].replace(
            b'href="https://www.sec.gov/Archives/', b'href="' + prefix + b"/Archives/"
        )
    if change == "multi_doc":
        values[4] = values[4].replace(b'sample.htm">', b'sample.htm&doc=/Archives/other.htm">')
    if change == "cross_cik":
        values[4] = values[4].replace(b"/data/123456/", b"/data/999999/")
    if change == "cross_accession":
        values[4] = values[4].replace(b"/000012345626000001/", b"/000012345626000002/")
    if change == "encoded":
        values[4] = values[4].replace(b"?doc=/Archives/", b"?doc=%2FArchives/")
    body = (
        filing()
        .replace(b"AAPL", b"GLD")
        .replace(b"Common Stock", b"Synthetic Gold Shares")
        .replace(b"New York Stock Exchange", b"NYSE Arca, Inc.")
    )
    body = re.sub(rb"<xbrli:segment>.*?</xbrli:segment>", b"", body, flags=re.S)
    business = """<p>FORM 10-K</p><p>Item 1. Business Synthetic Gold Trust (the “Trust”) is an
investment trust, formed on November 12, 2004 under New York law pursuant to a trust indenture
(the “Trust Indenture”).
The Trust holds gold and from time to time issues Synthetic Gold Shares (the “Shares”) in Baskets
in exchange for deposits of gold and distributes gold in connection with redemptions of Baskets.
The investment objective of the Trust is for the Shares to reflect the performance of the price of
gold bullion, less the Trust’s expenses.
The Shares represent units of fractional undivided beneficial interest
in and ownership of the Trust.
The Trust is not registered as an investment company under the Investment Company Act of 1940
and is not required to register under such act.</p><p>Item 1A. Risk Factors Details.</p>""".encode()
    body = body.replace(b"</body>", business + b"</body>")
    if change == "ticker":
        body = body.replace(b">GLD<", b">GLD.A<")
    if change == "exchange":
        body = body.replace(b"NYSE Arca, Inc.", b"NASDAQ")
    if change == "stock":
        body = body.replace(b"gold bullion", b"common stocks")
    if change == "treasury":
        body = body.replace(b"gold bullion", b"Treasury bonds")
    if change == "subject":
        body = body.replace(b"Business Synthetic Gold Trust", b"Business Another Trust")
    if change == "registered":
        body = body.replace(b"is not registered as", b"is registered as")
    if change == "same_row":
        body = body.replace(
            b'<td><ix:nonNumeric id="exchange"', b'</tr><tr><td><ix:nonNumeric id="exchange"'
        )
    if change in {"symbol_format", "symbol_target", "cik_format", "cik_target", "cik_tuple"}:
        fact, attr = change.split("_")
        attribute = {
            "format": b'format="unknown:transform"',
            "target": b'target="other-document"',
            "tuple": b'tupleRef="other-tuple"',
        }[attr]
        body = body.replace(
            b'id="' + fact.encode() + b'"', b'id="' + fact.encode() + b'" ' + attribute
        )
    if change == "ix_ancestor":
        body = body.replace(b"<table>", b"<ix:unknown><table>").replace(
            b"</table>", b"</table></ix:unknown>"
        )
    values[6] = body
    for i in (0, 2, 4, 6):
        receipt = json.loads(values[i + 1])
        receipt["raw_sha256" if i == 0 else "sha256"] = sha256(values[i]).hexdigest()
        receipt["raw_byte_length" if i == 0 else "byte_length"] = len(values[i])
        values[i + 1] = json.dumps(receipt).encode()
    return tuple(values)
