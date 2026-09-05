"""One explicitly archived custom stock member, not a general taxonomy resolver."""

from __future__ import annotations

import io
import json
import os
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, cast
from urllib.parse import urljoin, urlsplit

from nexus.domain.time import require_utc
from nexus.market.current_identity import (
    MAX_SEC_BYTES,
    SEC_URL,
    CurrentCommonStockIdentity,
    IdentityInputs,
    _context,
    _Cover,
    _period,
    cik,
    invalid,
    read_identity_archive_contents,
    validate_identity_originals,
)
from nexus.ops.local_recovery import write_private
from nexus.providers.fmp_profile import _unique, parse_profile
from nexus.research.fmp_profile_archive import _read

SCHEMA = "current_custom_member_v1"
NAMES = ("schema", "definition", "label")
XL = "{http://www.w3.org/1999/xlink}"
LINK = "{http://www.xbrl.org/2003/linkbase}"
XS = "{http://www.w3.org/2001/XMLSchema}"
DOMAIN = "http://xbrl.org/int/dim/arcrole/"
ROLE = "http://www.xbrl.org/2003/role/"
XML_LANG = "{http://www.w3.org/XML/1998/namespace}lang"


@dataclass(frozen=True)
class CustomIdentityInputs:
    base: IdentityInputs
    taxonomy_contents: tuple[bytes, ...]
    taxonomy_received: tuple[datetime, ...]

    @property
    def identity(self) -> CurrentCommonStockIdentity:
        return self.base.identity

    @property
    def sec_url(self) -> str:
        return self.base.sec_url

    @property
    def revision(self) -> str:
        return sha256(
            (
                SCHEMA
                + ":"
                + self.base.revision
                + ":"
                + ":".join(sha256(b).hexdigest() for b in self.taxonomy_contents)
            ).encode()
        ).hexdigest()


def _xml(raw: bytes) -> tuple[ET.Element, dict[str, str]]:
    text = raw.decode("utf-8-sig")
    if (
        len(raw) > MAX_SEC_BYTES
        or "\x00" in text
        or "<!DOCTYPE" in text.upper()
        or "<!ENTITY" in text.upper()
    ):
        raise invalid()
    namespaces: dict[str, str] = {}
    for _, binding in ET.iterparse(io.StringIO(text), events=("start-ns",)):
        prefix, uri = cast(tuple[str, str], binding)
        if prefix in namespaces and namespaces[prefix] != uri:
            raise invalid()
        namespaces[prefix] = uri
    root = ET.fromstring(text)
    ids = [n.get("id") for n in root.iter() if n.get("id")]
    if len(ids) != len(set(ids)) or any(
        "{http://www.w3.org/XML/1998/namespace}base" in n.attrib for n in root.iter()
    ):
        raise invalid()
    return root, namespaces


def _qname(value: str, namespaces: dict[str, str]) -> tuple[str, str]:
    if not re.fullmatch(r"[A-Za-z_][\w.-]*:[A-Za-z_][\w.-]*", value):
        raise invalid()
    prefix, local = value.split(":")
    return namespaces[prefix], local


def linked_url(base: str, href: str, suffix: str) -> str:
    folder = base.rsplit("/", 1)[0] + "/"
    url = urljoin(base, href)
    if not url.startswith(folder) or not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9._-]*" + re.escape(suffix), url[len(folder) :]
    ):
        raise invalid()
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.netloc != "www.sec.gov"
        or parsed.query
        or parsed.fragment
    ):
        raise invalid()
    return url


class _References(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.refs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "link:schemaref":
            if dict(attrs).get("xlink:type") != "simple":
                raise invalid()
            self.refs.append(dict(attrs).get("xlink:href") or "")

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.handle_starttag(tag, attrs)


def member_scope(symbol: str, contents: tuple[bytes, bytes, bytes, bytes]) -> dict[str, Any]:
    fmp, _, html, receipt = contents
    parser = _Cover()
    parser.feed(html.decode("utf-8-sig"))
    parser.close()
    profile = parse_profile(fmp, symbol)
    identifier = cik(profile["cik"])
    symbols = [f for f in parser.facts if f["name"] == "dei:TradingSymbol" and f["value"] == symbol]
    if len(symbols) != 1:
        raise invalid()
    stock = symbols[0]
    context = parser.contexts[stock["context"]]
    dims = _context(context, identifier)
    if (
        set(dims) != {"us-gaap:StatementClassOfStockAxis"}
        or stock["context"] not in parser.class_contexts
    ):
        raise invalid()
    qname = dims["us-gaap:StatementClassOfStockAxis"]
    namespace, local = _qname(
        qname, {k.removeprefix("xmlns:"): v for k, v in parser.namespaces.items() if v}
    )
    if qname.split(":")[0] in {"us-gaap", "dei", "xbrli", "srt", "exch"} or re.search(
        r"(?:fasb\.org|xbrl\.org|sec\.gov)", namespace
    ):
        raise invalid()
    row = [f for f in parser.facts if f["row"] == stock["row"]]
    if stock["row"] is None or len(row) != 3 or {f["context"] for f in row} != {stock["context"]}:
        raise invalid()
    title = next(f["value"] for f in row if f["name"] == "dei:Security12bTitle")
    ciks = [f for f in parser.facts if f["name"] == "dei:EntityCentralIndexKey"]
    if (
        len(ciks) != 1
        or cik(ciks[0]["value"]) != identifier
        or _period(parser.contexts[ciks[0]["context"]]) != _period(context)
    ):
        raise invalid()
    references = _References()
    references.feed(html.decode("utf-8-sig"))
    if (
        len(references.refs) != 1
        or parser.namespaces.get("xmlns:link") != LINK[1:-1]
        or parser.namespaces.get("xmlns:xlink") != XL[1:-1]
    ):
        raise invalid()
    sec_url = json.loads(receipt, object_pairs_hook=_unique)["source_url"]
    match = SEC_URL.fullmatch(sec_url)
    if not match or cik(match[1]) != identifier:
        raise invalid()
    return dict(
        qname=qname,
        namespace=namespace,
        local=local,
        title=title,
        schema_url=linked_url(sec_url, references.refs[0], ".xsd"),
        gaap_namespace=parser.namespaces.get("xmlns:us-gaap"),
    )


def schema_links(
    raw: bytes, scope: dict[str, Any]
) -> tuple[ET.Element, dict[str, str], dict[str, str]]:
    root, ns = _xml(raw)
    if root.tag != XS + "schema" or root.get("targetNamespace") != scope["namespace"]:
        raise invalid()
    elements = [e for e in root.findall(XS + "element") if e.get("name") == scope["local"]]
    if len(elements) != 1:
        raise invalid()
    element = elements[0]
    if (
        not element.get("id")
        or element.get("abstract") != "true"
        or element.get("nillable") != "true"
        or element.get("{http://www.xbrl.org/2003/instance}periodType") != "duration"
        or _qname(element.get("substitutionGroup", ""), ns)
        != ("http://www.xbrl.org/2003/instance", "item")
        or _qname(element.get("type", ""), ns)
        != ("http://www.xbrl.org/dtr/type/2024-01-31", "domainItemType")
    ):
        raise invalid()
    urls = {}
    for name in ("definition", "label"):
        refs = [
            r
            for r in root.iter(LINK + "linkbaseRef")
            if r.get(XL + "role") == ROLE + name + "LinkbaseRef"
        ]
        if len(refs) != 1 or refs[0].get(XL + "type") != "simple":
            raise invalid()
        urls[name] = linked_url(scope["schema_url"], refs[0].get(XL + "href", ""), ".xml")
    if len(set(urls.values())) != 2:
        raise invalid()
    return element, ns, urls


def _locations(link: ET.Element, base: str) -> dict[str, str]:
    result = {}
    for n in link.findall(LINK + "loc"):
        label = n.get(XL + "label")
        if (
            not label
            or label in result
            or not n.get(XL + "href")
            or n.get(XL + "type") != "locator"
        ):
            raise invalid()
        result[label] = urljoin(base, n.get(XL + "href", ""))
    return result


def _plain_arc(arc: ET.Element) -> None:
    allowed = {XL + "type", XL + "from", XL + "to", XL + "arcrole", "order"}
    if (
        set(arc.attrib) - allowed
        or arc.get(XL + "type") != "arc"
        or not arc.get(XL + "from")
        or not arc.get(XL + "to")
    ):
        raise invalid()


def validate_member_definition(
    scope: dict[str, Any], schema: bytes, definition: bytes, labels: bytes
) -> dict[str, Any]:
    element, _, urls = schema_links(schema, scope)
    target = scope["schema_url"] + "#" + element.get("id", "")
    definition_root, _ = _xml(definition)
    label_root, _ = _xml(labels)
    if definition_root.tag != LINK + "linkbase" or label_root.tag != LINK + "linkbase":
        raise invalid()
    year = re.fullmatch(r"http://fasb.org/us-gaap/([0-9]{4})", scope["gaap_namespace"])
    if not year:
        raise invalid()

    def standard(href: str, name: str) -> bool:
        return href in {
            f"{scheme}://xbrl.fasb.org/us-gaap/{year[1]}/elts/us-gaap-{year[1]}.xsd#us-gaap_{name}"
            for scheme in ("http", "https")
        }

    matches = []
    for link in definition_root.findall(LINK + "definitionLink"):
        if link.get(XL + "type") != "extended":
            raise invalid()
        loc = _locations(link, urls["definition"])
        incoming = [
            a
            for a in link.findall(LINK + "definitionArc")
            if loc.get(a.get(XL + "to", "")) == target or loc.get(a.get(XL + "from", "")) == target
        ]
        if not incoming:
            continue
        if len(incoming) != 1:
            raise invalid()
        arc = incoming[0]
        _plain_arc(arc)
        if (
            arc.get(XL + "arcrole") != DOMAIN + "domain-member"
            or loc.get(arc.get(XL + "to", "")) != target
            or not standard(loc.get(arc.get(XL + "from", ""), ""), "ClassOfStockDomain")
        ):
            raise invalid()
        domain_edges = [
            a
            for a in link.findall(LINK + "definitionArc")
            if loc.get(a.get(XL + "to", "")) == loc[arc.get(XL + "from", "")]
        ]
        axes = [a for a in domain_edges if a.get(XL + "arcrole") == DOMAIN + "dimension-domain"]
        defaults = [
            a for a in domain_edges if a.get(XL + "arcrole") == DOMAIN + "dimension-default"
        ]
        if len(axes) != 1 or len(defaults) > 1 or len(domain_edges) != len(axes) + len(defaults):
            raise invalid()
        for default in defaults:
            _plain_arc(default)
            if default.get(XL + "from") != axes[0].get(XL + "from"):
                raise invalid()
        _plain_arc(axes[0])
        if (
            axes[0].get(XL + "arcrole") != DOMAIN + "dimension-domain"
            or not standard(loc.get(axes[0].get(XL + "from", ""), ""), "StatementClassOfStockAxis")
            or not link.get(XL + "role")
        ):
            raise invalid()
        matches.append(link.get(XL + "role"))
    if len(matches) != 1:
        raise invalid()
    resources: list[ET.Element] = []
    label_arcs = 0
    for link in label_root.findall(LINK + "labelLink"):
        if link.get(XL + "type") != "extended":
            raise invalid()
        loc = _locations(link, urls["label"])
        for arc in link.findall(LINK + "labelArc"):
            if loc.get(arc.get(XL + "from", "")) != target:
                continue
            _plain_arc(arc)
            label_arcs += 1
            if arc.get(XL + "arcrole") != "http://www.xbrl.org/2003/arcrole/concept-label":
                raise invalid()
            resources.extend(
                n for n in link.findall(LINK + "label") if n.get(XL + "label") == arc.get(XL + "to")
            )
    if label_arcs != 1 or not resources:
        raise invalid()
    roles = []
    for resource in resources:
        role = resource.get(XL + "role")
        text = "".join(resource.itertext())
        if (
            resource.get(XL + "type") != "resource"
            or list(resource)
            or resource.get(XML_LANG) != "en-US"
            or role not in {ROLE + "terseLabel", ROLE + "label", ROLE + "documentation"}
        ):
            raise invalid()
        expected = scope["title"] if role == ROLE + "terseLabel" else scope["title"] + " [Member]"
        if text != expected or role in roles:
            raise invalid()
        roles.append(role)
    if ROLE + "terseLabel" not in roles:
        raise invalid()
    return {
        **scope,
        "member_id": element.get("id"),
        "definition_role": matches[0],
        "definition_url": urls["definition"],
        "label_url": urls["label"],
    }


def validate_custom_identity_originals(
    *,
    symbol: str,
    contents: tuple[bytes, bytes, bytes, bytes],
    taxonomy_contents: tuple[bytes, ...],
    now: datetime,
) -> CustomIdentityInputs:
    try:
        if len(taxonomy_contents) != 6:
            raise invalid()
        scope = member_scope(symbol, contents)
        _, _, urls = schema_links(taxonomy_contents[0], scope)
        urls["schema"] = scope["schema_url"]
        received = []
        for i, name in enumerate(NAMES):
            raw, receipt = taxonomy_contents[2 * i : 2 * i + 2]
            r = json.loads(receipt, object_pairs_hook=_unique)
            expected = dict(
                source_url=urls[name],
                sha256=sha256(raw).hexdigest(),
                byte_length=len(raw),
                http_status=200,
                received_at=r["received_at"],
            )
            if (
                r != expected
                or type(r["byte_length"]) is not int
                or type(r["http_status"]) is not int
            ):
                raise invalid()
            time = require_utc(datetime.fromisoformat(r["received_at"]))
            if time > require_utc(now):
                raise invalid()
            received.append(time)
        evidence = validate_member_definition(scope, *taxonomy_contents[::2])
        base = validate_identity_originals(
            symbol=symbol, contents=contents, now=now, _member_definition=evidence
        )
        return CustomIdentityInputs(base, taxonomy_contents, tuple(received))
    except (
        OSError,
        ValueError,
        TypeError,
        KeyError,
        StopIteration,
        ET.ParseError,
        RecursionError,
    ) as error:
        raise ValueError("CURRENT_IDENTITY_TAXONOMY_INVALID") from error


def load_custom_identity_archives(
    *,
    symbol: str,
    fmp_archive: Path,
    sec_archive: Path,
    taxonomy_archive: Path,
    now: datetime | None = None,
) -> CustomIdentityInputs:
    try:
        cutoff = require_utc(now or datetime.now(UTC))
        contents = read_identity_archive_contents(
            symbol=symbol, fmp_archive=fmp_archive, sec_archive=sec_archive, now=cutoff
        )
        fd = os.open(taxonomy_archive, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            names = [n + s for n in NAMES for s in (".xml", "-receipt.json")]
            if os.fstat(fd).st_mode & 0o077 or set(os.listdir(fd)) != set(names):
                raise invalid()
            raw = tuple(_read(fd, n, 8192 if n.endswith(".json") else MAX_SEC_BYTES) for n in names)
        finally:
            os.close(fd)
        return validate_custom_identity_originals(
            symbol=symbol, contents=contents, taxonomy_contents=raw, now=cutoff
        )
    except (OSError, ValueError, TypeError, KeyError, RecursionError) as error:
        raise ValueError("CURRENT_IDENTITY_TAXONOMY_INVALID") from error


def archive_member_taxonomy(
    *,
    symbol: str,
    fmp_archive: Path,
    sec_archive: Path,
    originals: tuple[Path, ...],
    output_dir: Path,
) -> None:
    """Package explicit originals without changing source receipts or using the network."""
    if len(originals) != 6:
        raise ValueError("CURRENT_IDENTITY_TAXONOMY_INVALID")
    contents = []
    for index, path in enumerate(originals):
        fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            if os.fstat(fd).st_mode & 0o077:
                raise invalid()
            contents.append(_read(fd, path.name, 8192 if index % 2 else MAX_SEC_BYTES))
        finally:
            os.close(fd)
    now = datetime.now(UTC)
    base = read_identity_archive_contents(
        symbol=symbol, fmp_archive=fmp_archive, sec_archive=sec_archive, now=now
    )
    validate_custom_identity_originals(
        symbol=symbol, contents=base, taxonomy_contents=tuple(contents), now=now
    )
    output_dir.mkdir(mode=0o700)
    names = [n + s for n in NAMES for s in (".xml", "-receipt.json")]
    for name, raw in zip(names, contents, strict=True):
        write_private(output_dir / name, raw)
