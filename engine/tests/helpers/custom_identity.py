"""Synthetic custom-member taxonomy, with no captured issuer data."""

import json
import re
from datetime import UTC, datetime
from hashlib import sha256

from helpers.current_identity import URL, archives, filing

BASE = URL.rsplit("/", 1)[0] + "/"
NS = "https://example.invalid/2025"
XL = "http://www.w3.org/1999/xlink"
LINK = "http://www.xbrl.org/2003/linkbase"


def custom_documents():
    html = filing().replace(b"<html ", f'<html xmlns:sample="{NS}" '.encode())
    html = html.replace(b"us-gaap:CommonStockMember", b"sample:ExampleMember")
    html = re.sub(
        rb'<xbrldi:explicitMember dimension="dei:EntityListingsExchangeAxis">'
        rb".*?</xbrldi:explicitMember>",
        b"",
        html,
    )
    html = html.replace(
        b"</ix:header>",
        b'<link:schemaRef xmlns:link="http://www.xbrl.org/2003/linkbase" '
        b'xmlns:xlink="http://www.w3.org/1999/xlink" xlink:href="sample.xsd"/></ix:header>',
    )
    # Root-bound namespaces match the existing strict cover reader.
    html = html.replace(b"<html ", f'<html xmlns:link="{LINK}" xmlns:xlink="{XL}" '.encode())
    schema = f'''<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
      xmlns:link="{LINK}" xmlns:xlink="{XL}" xmlns:xbrli="http://www.xbrl.org/2003/instance"
      xmlns:dtr-types="http://www.xbrl.org/dtr/type/2024-01-31" targetNamespace="{NS}">
      <xs:annotation><xs:appinfo>
      <link:linkbaseRef xlink:role="http://www.xbrl.org/2003/role/definitionLinkbaseRef"
       xlink:href="sample_def.xml"/>
      <link:linkbaseRef xlink:role="http://www.xbrl.org/2003/role/labelLinkbaseRef"
       xlink:href="sample_lab.xml"/>
      </xs:appinfo></xs:annotation>
      <xs:element id="sample_ExampleMember" name="ExampleMember" abstract="true" nillable="true"
       type="dtr-types:domainItemType" substitutionGroup="xbrli:item" xbrli:periodType="duration"/>
      </xs:schema>'''.encode()
    definition = f'''<link:linkbase xmlns:link="{LINK}" xmlns:xlink="{XL}">
      <link:definitionLink xlink:role="https://example.invalid/role/Stock">
      <link:loc xlink:label="axis" xlink:href="https://xbrl.fasb.org/us-gaap/2025/elts/us-gaap-2025.xsd#us-gaap_StatementClassOfStockAxis"/>
      <link:loc xlink:label="domain" xlink:href="https://xbrl.fasb.org/us-gaap/2025/elts/us-gaap-2025.xsd#us-gaap_ClassOfStockDomain"/>
      <link:loc xlink:label="member" xlink:href="sample.xsd#sample_ExampleMember"/>
      <link:definitionArc xlink:from="axis" xlink:to="domain" xlink:arcrole="http://xbrl.org/int/dim/arcrole/dimension-domain"/>
      <link:definitionArc xlink:from="domain" xlink:to="member" xlink:arcrole="http://xbrl.org/int/dim/arcrole/domain-member"/>
      </link:definitionLink></link:linkbase>'''.encode()
    label = f'''<link:linkbase xmlns:link="{LINK}" xmlns:xlink="{XL}">
      <link:labelLink xlink:role="http://www.xbrl.org/2003/role/link">
      <link:loc xlink:label="member" xlink:href="sample.xsd#sample_ExampleMember"/>
      <link:labelArc xlink:from="member" xlink:to="text" xlink:arcrole="http://www.xbrl.org/2003/arcrole/concept-label"/>
      <link:label xlink:label="text" xlink:role="http://www.xbrl.org/2003/role/terseLabel"
       xml:lang="en-US">Common Stock</link:label>
      <link:label xlink:label="text" xlink:role="http://www.xbrl.org/2003/role/label"
       xml:lang="en-US">Common Stock [Member]</link:label>
      </link:labelLink></link:linkbase>'''.encode()
    html = html.replace(b"<link:schemaRef ", b'<link:schemaRef xlink:type="simple" ')
    schema = schema.replace(b"<link:linkbaseRef ", b'<link:linkbaseRef xlink:type="simple" ')
    transformed = []
    for raw in (definition, label):
        for tag, kind in [
            ("definitionLink", "extended"),
            ("labelLink", "extended"),
            ("loc", "locator"),
            ("definitionArc", "arc"),
            ("labelArc", "arc"),
            ("label", "resource"),
        ]:
            raw = raw.replace(
                ("<link:" + tag + " ").encode(),
                ("<link:" + tag + ' xlink:type="' + kind + '" ').encode(),
            )
        transformed.append(raw)
    return html, schema, *transformed


def custom_archives(root, *, updates=None):
    html, *documents = custom_documents()
    for index, before, after in updates or []:
        if index == 0:
            assert before in html
            html = html.replace(before, after)
        else:
            assert before in documents[index - 1]
            documents[index - 1] = documents[index - 1].replace(before, after)
    fm, sc = archives(root, sec=html)
    folder = root / "taxonomy"
    folder.mkdir(mode=0o700)
    for name, source, raw in zip(
        ("schema", "definition", "label"),
        ("sample.xsd", "sample_def.xml", "sample_lab.xml"),
        documents,
        strict=True,
    ):
        receipt = dict(
            source_url=BASE + source,
            sha256=sha256(raw).hexdigest(),
            byte_length=len(raw),
            http_status=200,
            received_at=datetime.now(UTC).isoformat(),
        )
        for path, content in (
            (folder / (name + ".xml"), raw),
            (folder / (name + "-receipt.json"), json.dumps(receipt).encode()),
        ):
            path.write_bytes(content)
            path.chmod(0o600)
    return fm, sc, folder
