import importlib

import pytest
from helpers.current_identity import archives
from helpers.custom_identity import custom_archives

_DOCUMENTATION = (
    b'<link:label xlink:type="resource" xlink:label="text" '
    b'xlink:role="http://www.xbrl.org/2003/role/documentation" '
    b'xml:lang="en-US">Common Stock</link:label>'
)
_FALLBACK = [
    (3, b">Common Stock<", b">Common Stock [Member]<"),
    (3, b"</link:labelLink>", _DOCUMENTATION + b"</link:labelLink>"),
]


@pytest.mark.parametrize("kind", ["artifact", "snapshot"])
def test_custom_identity_schema_names_fit_actual_persistence_columns(kind):
    from nexus.market.current_identity_storage import TAXONOMY_SCHEMAS
    from nexus.market.custom_identity import SCHEMA
    from nexus.persistence.models.data import DataArtifact, DataSnapshot

    artifact_limit = DataArtifact.__table__.c.schema_version.type.length
    snapshot_limit = DataSnapshot.__table__.c.schema_version.type.length
    if kind == "artifact":
        assert all(len(value) <= artifact_limit for value in TAXONOMY_SCHEMAS)
    else:
        assert len(SCHEMA) <= snapshot_limit


def test_custom_member_requires_explicit_bound_taxonomy_and_keeps_original_member(tmp_path):
    module = importlib.import_module("nexus.market.custom_identity")
    old = importlib.import_module("nexus.market.current_identity")
    fm, sc, tx = custom_archives(tmp_path)
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        old.load_identity_archives(symbol="AAPL", fmp_archive=fm, sec_archive=sc)
    result = module.load_custom_identity_archives(
        symbol="AAPL", fmp_archive=fm, sec_archive=sc, taxonomy_archive=tx
    )
    assert result.identity.exchange == "XNYS"
    assert result.identity.sec_title == "Common Stock"
    assert result.identity.context["fields"][1]["value"] == "sample:ExampleMember"
    assert len(result.taxonomy_contents) == 6
    assert len(result.base.contents) == 4


@pytest.mark.parametrize(
    "updates",
    [
        [
            (
                1,
                b'targetNamespace="https://example.invalid/2025"',
                b'targetNamespace="https://example.invalid/wrong"',
            )
        ],
        [(2, b"#sample_ExampleMember", b"#sample_OtherMember")],
        [(2, b'xlink:from="domain" xlink:to="member"', b'xlink:from="axis" xlink:to="member"')],
        [(2, b'xlink:from="domain"', b'use="prohibited" xlink:from="domain"')],
        [(2, b'xlink:from="domain"', b'priority="1" xlink:from="domain"')],
        [
            (
                2,
                b'xlink:from="domain"',
                b'xmlns:xbrldt="http://xbrl.org/2005/xbrldt" '
                b'xbrldt:targetRole="other" xlink:from="domain"',
            )
        ],
        [(3, b">Common Stock<", b">Common Stock Class B<")],
        [(3, b">Common Stock [Member]<", b">Preferred Stock [Member]<")],
        [(3, b'xml:lang="en-US"', b'xml:lang="fr"')],
        [
            (
                1,
                b"<xs:schema ",
                b'<!DOCTYPE schema [<!ENTITY x SYSTEM "file:///etc/passwd">]><xs:schema ',
            )
        ],
        [(1, b"sample_def.xml", b"https://example.invalid/sample_def.xml")],
        [(2, b'xlink:to="domain"', b'xlink:to="domain" xlink:role="other"')],
        [(1, b"</xs:schema>", b'<xs:element name="ExampleMember" id="duplicate"/></xs:schema>')],
        [(1, b'id="sample_ExampleMember"', b'id="different"')],
        [(1, b'type="dtr-types:domainItemType"', b'type="xbrli:stringItemType"')],
        [(2, b"sample.xsd#sample_ExampleMember", b"../other/sample.xsd#sample_ExampleMember")],
        [
            (
                2,
                b"</link:definitionLink>",
                b'<link:definitionArc xlink:from="domain" xlink:to="member" xlink:arcrole="http://xbrl.org/int/dim/arcrole/domain-member"/></link:definitionLink>',
            )
        ],
        [
            (
                3,
                b"</link:labelLink>",
                b'<link:labelArc xlink:from="member" xlink:to="text" xlink:arcrole="http://www.xbrl.org/2003/arcrole/concept-label"/></link:labelLink>',
            )
        ],
        [(0, b"sample:ExampleMember", b"sample:OtherMember")],
        [(0, b">Common Stock<", b">Preferred Stock<"), (3, b"Common Stock", b"Preferred Stock")],
        [
            (0, b">Common Stock<", b">Common Stock Depositary Units<"),
            (3, b"Common Stock", b"Common Stock Depositary Units"),
        ],
        [(2, b'xlink:type="arc"', b'xlink:type="unknown"')],
        [(2, b'xlink:type="locator"', b'xlink:type="resource"')],
        [(3, b'xlink:type="resource"', b'xlink:type="locator"')],
    ],
)
@pytest.mark.parametrize("fallback", [False, True])
def test_custom_member_rejects_unbound_or_ambiguous_semantics(tmp_path, updates, fallback):
    module = importlib.import_module("nexus.market.custom_identity")
    fm, sc, tx = custom_archives(tmp_path, updates=(_FALLBACK if fallback else []) + updates)
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.load_custom_identity_archives(
            symbol="AAPL", fmp_archive=fm, sec_archive=sc, taxonomy_archive=tx
        )


@pytest.mark.parametrize(
    "before,after",
    [
        (b'/role/label"', b'/role/verboseLabel"'),
        (b'/role/terseLabel"', b'/role/label"'),
        (b'/role/documentation"', b'/role/label"'),
        (b">Common Stock<", b">Common Stock, par value $2<"),
        (b">Common Stock<", b">Common Stock Class B<"),
        (b">Common Stock<", b">Preferred Stock<"),
        (b">Common Stock<", b">Common Stock nonvoting<"),
        (b">Common Stock<", b"><span>Common Stock</span><"),
        (b"sample.xsd#sample_ExampleMember", b"sample.xsd#sample_OtherMember"),
        (b"</link:labelLink>", _DOCUMENTATION + b"</link:labelLink>"),
    ],
)
def test_suffix_fallback_never_trusts_other_roles_or_changed_meaning(tmp_path, before, after):
    from nexus.market.custom_identity import load_custom_identity_archives

    fm, sc, tx = custom_archives(tmp_path, updates=[*_FALLBACK, (3, before, after)])
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        load_custom_identity_archives(
            symbol="AAPL", fmp_archive=fm, sec_archive=sc, taxonomy_archive=tx
        )


@pytest.mark.parametrize("fallback,role", [(False, "label"), (True, "label"), (True, "terseLabel")])
def test_missing_label_roles_preserve_legacy_but_cannot_authorize_fallback(
    tmp_path, fallback, role
):
    import re

    from helpers.custom_identity import custom_documents
    from nexus.market.custom_identity import load_custom_identity_archives

    label = custom_documents()[3]
    if fallback:
        label = label.replace(b">Common Stock<", b">Common Stock [Member]<")
    node = next(
        raw
        for raw in re.findall(rb"<link:label\s[^>]*>.*?</link:label>", label, re.S)
        if ("/role/" + role + '"').encode() in raw
    )
    fm, sc, tx = custom_archives(
        tmp_path, updates=[*(_FALLBACK if fallback else []), (3, node, b"")]
    )
    if fallback:
        with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
            load_custom_identity_archives(
                symbol="AAPL", fmp_archive=fm, sec_archive=sc, taxonomy_archive=tx
            )
    else:
        result = load_custom_identity_archives(
            symbol="AAPL", fmp_archive=fm, sec_archive=sc, taxonomy_archive=tx
        )
        assert result.identity.sec_title == "Common Stock"


def test_custom_member_rejects_tampered_receipt_or_raw(tmp_path):
    module = importlib.import_module("nexus.market.custom_identity")
    fm, sc, tx = custom_archives(tmp_path)
    (tx / "schema.xml").write_bytes(b"changed")
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.load_custom_identity_archives(
            symbol="AAPL", fmp_archive=fm, sec_archive=sc, taxonomy_archive=tx
        )


def test_old_archive_json_has_no_custom_default_fields(tmp_path):
    module = importlib.import_module("nexus.market.current_identity")
    fm, sc = archives(tmp_path)
    result = module.load_identity_archives(symbol="AAPL", fmp_archive=fm, sec_archive=sc)
    assert len(result.contents) == 4
    assert all("member_definition" not in f for f in result.identity.facts)


@pytest.mark.parametrize(
    "terse_member,documentation_member",
    [(False, True), (True, False), (True, True), (False, False)],
)
def test_normal_archive_cli_preserves_all_six_originals(
    tmp_path, monkeypatch, capsys, terse_member, documentation_member
):
    title = b"Common Stock"
    documentation = (
        b'<link:label xlink:type="resource" xlink:label="text" '
        b'xlink:role="http://www.xbrl.org/2003/role/documentation" xml:lang="en-US">'
        + title
        + (b" [Member]" if documentation_member else b"")
        + b"</link:label>"
    )
    updates = [(3, b"</link:labelLink>", documentation + b"</link:labelLink>")]
    if terse_member:
        updates.insert(0, (3, b">Common Stock<", b">Common Stock [Member]<"))
    fm, sc, tx = custom_archives(tmp_path, updates=updates)
    module = importlib.import_module("nexus.ops.instrument_identity_local")
    destination = tmp_path / "packaged"
    args = [
        "identity",
        "--repo-root",
        str(tmp_path),
        "archive-taxonomy",
        "--symbol",
        "AAPL",
        "--fmp-archive",
        str(fm),
        "--sec-archive",
        str(sc),
        "--output-dir",
        str(destination),
    ]
    for name in ("schema", "definition", "label"):
        args.extend(
            [
                "--" + name,
                str(tx / (name + ".xml")),
                "--" + name + "-receipt",
                str(tx / (name + "-receipt.json")),
            ]
        )
    monkeypatch.setattr("sys.argv", args)
    module.main()
    assert "archived" in capsys.readouterr().out
    assert {p.name: p.read_bytes() for p in tx.iterdir()} == {
        p.name: p.read_bytes() for p in destination.iterdir()
    }
    from nexus.market.custom_identity import load_custom_identity_archives

    result = load_custom_identity_archives(
        symbol="AAPL", fmp_archive=fm, sec_archive=sc, taxonomy_archive=destination
    )
    baseline = tmp_path / "legacy"
    baseline.mkdir(mode=0o700)
    old_fm, old_sc, old_tx = custom_archives(baseline)
    old = load_custom_identity_archives(
        symbol="AAPL", fmp_archive=old_fm, sec_archive=old_sc, taxonomy_archive=old_tx
    )
    assert result.identity.model_dump_json() == old.identity.model_dump_json()


def test_custom_member_distinguishes_domain_edge_from_default_edge(tmp_path):
    module = importlib.import_module("nexus.market.custom_identity")
    default = (
        b'<link:definitionArc xlink:type="arc" xlink:from="axis" xlink:to="domain" '
        b'xlink:arcrole="http://xbrl.org/int/dim/arcrole/dimension-default"/>'
    )
    fm, sc, tx = custom_archives(
        tmp_path, updates=[(2, b"</link:definitionLink>", default + b"</link:definitionLink>")]
    )
    result = module.load_custom_identity_archives(
        symbol="AAPL", fmp_archive=fm, sec_archive=sc, taxonomy_archive=tx
    )
    assert result.identity.sec_title == "Common Stock"


@pytest.mark.parametrize("case", ["future", "url", "hash", "missing", "symlink", "extra"])
def test_custom_member_receipt_and_directory_are_strict(tmp_path, case):
    import json

    module = importlib.import_module("nexus.market.custom_identity")
    fm, sc, tx = custom_archives(tmp_path)
    p = tx / "schema-receipt.json"
    doc = json.loads(p.read_bytes())
    if case == "future":
        doc["received_at"] = "2999-01-01T00:00:00+00:00"
    elif case == "url":
        doc["source_url"] = doc["source_url"].replace("/123456/", "/654321/")
    elif case == "hash":
        doc["sha256"] = "0" * 64
    elif case == "missing":
        p.unlink()
    elif case == "symlink":
        p.rename(tx / "saved.json")
        p.symlink_to(tx / "saved.json")
    else:
        (tx / "extra.json").write_text("{}")
    if case in {"future", "url", "hash"}:
        p.write_text(json.dumps(doc))
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.load_custom_identity_archives(
            symbol="AAPL", fmp_archive=fm, sec_archive=sc, taxonomy_archive=tx
        )
