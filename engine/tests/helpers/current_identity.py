"""Handwritten public-symbol fixtures; no captured issuer/account payloads."""

import json
import re
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

CIK = "0000123456"
URL = "https://www.sec.gov/Archives/edgar/data/123456/000012345626000001/sample.htm"


def profile(**updates):
    row = dict(
        symbol="AAPL",
        cik=CIK,
        currency="USD",
        exchange="NYSE",
        isEtf=False,
        isFund=False,
        isAdr=False,
        isin="US0000000001",
    )
    return json.dumps([{**row, **updates}]).encode()


def filing():
    return b"""<html xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"
 xmlns:xbrli="http://www.xbrl.org/2003/instance"
 xmlns:xbrldi="http://xbrl.org/2006/xbrldi"
 xmlns:dei="http://xbrl.sec.gov/dei/2025"
 xmlns:us-gaap="http://fasb.org/us-gaap/2025"
 xmlns:exch="http://xbrl.sec.gov/exch/2025"><body>
<ix:header><xbrli:context id="stock"><xbrli:entity>
<xbrli:identifier scheme="http://www.sec.gov/CIK">0000123456</xbrli:identifier>
<xbrli:segment>
<xbrldi:explicitMember\
 dimension="us-gaap:StatementClassOfStockAxis">us-gaap:CommonStockMember</xbrldi:explicitMember>
<xbrldi:explicitMember dimension="dei:EntityListingsExchangeAxis">exch:XNYS</xbrldi:explicitMember>
</xbrli:segment></xbrli:entity><xbrli:period>
<xbrli:startDate>2025-01-01</xbrli:startDate><xbrli:endDate>2025-12-31</xbrli:endDate>
</xbrli:period></xbrli:context></ix:header>
<ix:nonNumeric id="cik" name="dei:EntityCentralIndexKey"\
 contextRef="stock">0000123456</ix:nonNumeric>
<table><tr>
<td><ix:nonNumeric id="title" name="dei:Security12bTitle" contextRef="stock">Common\
 Stock</ix:nonNumeric></td>
<td><ix:nonNumeric id="symbol" name="dei:TradingSymbol" contextRef="stock">AAPL</ix:nonNumeric></td>
<td><ix:nonNumeric id="exchange" name="dei:SecurityExchangeName"\
 contextRef="stock">New York Stock Exchange</ix:nonNumeric></td>
</tr></table></body></html>"""


def multiple_registrant_filing():
    raw = filing().replace(b"<html ", b'<html xmlns:sample="https://example.invalid/2025" ')
    raw = raw.replace(
        b'<xbrldi:explicitMember dimension="dei:EntityListingsExchangeAxis">'
        b"exch:XNYS</xbrldi:explicitMember>",
        b"",
    )
    stock_context = re.search(rb'<xbrli:context id="stock">.*?</xbrli:context>', raw, re.S)[0]
    primary = re.sub(rb"<xbrli:segment>.*?</xbrli:segment>", b"", stock_context, flags=re.S)
    primary = primary.replace(b'id="stock"', b'id="primary"')
    other = primary.replace(b'id="primary"', b'id="other"').replace(
        b"</xbrli:entity>",
        b"<xbrli:segment><xbrldi:explicitMember "
        b'dimension="dei:LegalEntityAxis">sample:OtherMember</xbrldi:explicitMember>'
        b"</xbrli:segment></xbrli:entity>",
    )
    raw = raw.replace(b"</ix:header>", primary + other + b"</ix:header>")
    raw = raw.replace(b'contextRef="stock">0000123456', b'contextRef="primary">0000123456')
    return raw.replace(
        b"</body>",
        b"""
<ix:nonNumeric id="primary-name" name="dei:EntityRegistrantName"
 contextRef="primary">Primary Example</ix:nonNumeric>
<ix:nonNumeric id="primary-tax" name="dei:EntityTaxIdentificationNumber"
 contextRef="primary">00-0000001</ix:nonNumeric>
<ix:nonNumeric id="other-cik" name="dei:EntityCentralIndexKey"
 contextRef="other">0000654321</ix:nonNumeric>
<ix:nonNumeric id="other-name" name="dei:EntityRegistrantName"
 contextRef="other">Other Example</ix:nonNumeric>
<ix:nonNumeric id="other-tax" name="dei:EntityTaxIdentificationNumber"
 contextRef="other">00-0000002</ix:nonNumeric>
</body>""",
    )


def archives(root: Path, *, fmp=None, sec=None, received=None):
    received = received or datetime.now(UTC).isoformat()
    fmp = fmp or profile()
    sec = sec or filing()
    fm, sc = root / "fmp", root / "sec"
    fm.mkdir(mode=0o700, parents=True)
    sc.mkdir(mode=0o700)
    documents = {
        fm / "profile.json": fmp,
        fm / "receipt.json": json.dumps(
            dict(
                schema="fmp_profile_receipt_v1",
                source="fmp",
                origin="https://financialmodelingprep.com",
                path="/stable/profile",
                symbol="AAPL",
                raw_sha256=sha256(fmp).hexdigest(),
                raw_byte_length=len(fmp),
                received_at=received,
            )
        ).encode(),
        sc / "sample.htm": sec,
        sc / "receipt.json": json.dumps(
            dict(
                source_url=URL,
                received_at=received,
                sha256=sha256(sec).hexdigest(),
                byte_length=len(sec),
                http_status=200,
            )
        ).encode(),
    }
    for path, content in documents.items():
        path.write_bytes(content)
        path.chmod(0o600)
    return fm, sc


class MemoryStore:
    def __init__(self):
        self.objects = {}

    async def put_if_absent(self, *, key, content):
        if key in self.objects:
            assert self.objects[key] == content
        self.objects[key] = content
        return "s3://synthetic/" + key

    async def read_raw(self, key, *, uri):
        assert uri == "s3://synthetic/" + key
        return self.objects[key]
