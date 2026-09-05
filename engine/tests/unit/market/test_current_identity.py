import importlib
import re
from datetime import UTC, datetime, timedelta

import pytest
from helpers.current_identity import archives, filing, profile
from helpers.current_identity import multiple_registrant_filing as _multiple_registrant_cover


def test_explicit_same_security_identity():
    module = importlib.import_module("nexus.market.current_identity")
    result = module.parse_current_common_stock(symbol="AAPL", fmp_raw=profile(), sec_raw=filing())
    assert result.cik == "0000123456"
    assert result.exchange == "XNYS"
    assert result.context_id == "stock"
    assert result.sec_title == "Common Stock"


@pytest.mark.parametrize(
    "before,after",
    [
        (b">AAPL<", b">MSFT<"),
        (b'contextRef="stock">AAPL', b'contextRef="bond">AAPL'),
        (b'id="exchange"', b'id="title"'),
        (b"us-gaap:CommonStockMember", b"us-gaap:PreferredStockMember"),
        (b">Common Stock<", b">Preferred Stock<"),
        (b">Common Stock<", b">Common Stock Warrants<"),
        (b">exch:XNYS<", b">exch:XNAS<"),
        (b">0000123456<", b">0000654321<"),
        (b'<td><ix:nonNumeric id="exchange"', b'</tr><tr><td><ix:nonNumeric id="exchange"'),
        (
            b"</body>",
            b'<ix:nonNumeric id="again" name="dei:TradingSymbol" '
            b'contextRef="stock">AAPL</ix:nonNumeric></body>',
        ),
    ],
)
def test_rejects_cross_security_or_unsupported_structure(before, after):
    module = importlib.import_module("nexus.market.current_identity")
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.parse_current_common_stock(
            symbol="AAPL", fmp_raw=profile(), sec_raw=filing().replace(before, after)
        )


@pytest.mark.parametrize(
    "updates",
    [dict(isEtf=0), dict(isFund=None), dict(isAdr=True), dict(currency="HKD"), dict(cik="12.3")],
)
def test_rejects_bad_fmp_identity(updates):
    module = importlib.import_module("nexus.market.current_identity")
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.parse_current_common_stock(
            symbol="AAPL", fmp_raw=profile(**updates), sec_raw=filing()
        )


@pytest.mark.parametrize("case", ["future", "tampered", "symlink", "duplicate_receipt_key"])
def test_strict_local_archives(tmp_path, case):
    module = importlib.import_module("nexus.market.current_identity")
    received = (datetime.now(UTC) + timedelta(days=1)).isoformat() if case == "future" else None
    fm, sc = archives(tmp_path, received=received)
    if case == "tampered":
        (sc / "sample.htm").write_bytes(b"changed")
    elif case == "symlink":
        (sc / "sample.htm").rename(sc / "target")
        (sc / "sample.htm").symlink_to(sc / "target")
    elif case == "duplicate_receipt_key":
        path = sc / "receipt.json"
        path.write_text(path.read_text().replace("{", '{"http_status":200,', 1))
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.load_identity_archives(symbol="AAPL", fmp_archive=fm, sec_archive=sc)


def test_nested_unknown_fact_cannot_hide_security_suffix():
    module = importlib.import_module("nexus.market.current_identity")
    raw = filing().replace(
        b">Common Stock</ix:nonNumeric>",
        b'><ix:nonNumeric name="example:Other" contextRef="stock">Common Stock'
        b"</ix:nonNumeric> Warrants</ix:nonNumeric>",
    )
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.parse_current_common_stock(symbol="AAPL", fmp_raw=profile(), sec_raw=raw)


def test_late_sibling_namespace_does_not_bind_cover_facts():
    module = importlib.import_module("nexus.market.current_identity")
    raw = filing().replace(b'xmlns:dei="http://xbrl.sec.gov/dei/2025"', b"")
    raw = raw.replace(b"</body>", b'<div xmlns:dei="http://xbrl.sec.gov/dei/2025"></div></body>')
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.parse_current_common_stock(symbol="AAPL", fmp_raw=profile(), sec_raw=raw)


def test_accession_submitter_can_differ_from_issuer(tmp_path):
    module = importlib.import_module("nexus.market.current_identity")
    fm, sc = archives(tmp_path)
    receipt = sc / "receipt.json"
    receipt.write_text(receipt.read_text().replace("000012345626000001", "000099999926000001"))
    assert (
        module.load_identity_archives(symbol="AAPL", fmp_archive=fm, sec_archive=sc).identity.cik
        == "0000123456"
    )


def test_archive_issuer_directory_must_match_context(tmp_path):
    module = importlib.import_module("nexus.market.current_identity")
    fm, sc = archives(tmp_path)
    receipt = sc / "receipt.json"
    receipt.write_text(receipt.read_text().replace("/data/123456/", "/data/654321/"))
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.load_identity_archives(symbol="AAPL", fmp_archive=fm, sec_archive=sc)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [200, 201, 503])
async def test_normal_sec_collector_one_request_preserves_actual_status(tmp_path, status):
    import json

    import httpx
    from helpers.current_identity import URL

    module = importlib.import_module("nexus.ops.instrument_identity_local")
    calls = []

    def respond(request):
        calls.append(request)
        return httpx.Response(status, content=filing())

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        if status == 200:
            await module.collect_sec(
                source_url=URL,
                contact="test@example.invalid",
                output_dir=tmp_path / "sec",
                client=client,
            )
            assert json.loads((tmp_path / "sec/receipt.json").read_text())["http_status"] == 200
            assert (tmp_path / "sec/sample.htm").read_bytes() == filing()
        else:
            with pytest.raises((ValueError, RuntimeError)):
                await module.collect_sec(
                    source_url=URL,
                    contact="test@example.invalid",
                    output_dir=tmp_path / "sec",
                    client=client,
                )
            assert not (tmp_path / "sec/receipt.json").exists()
    assert len(calls) == 1
    assert str(calls[0].url) == URL


@pytest.mark.parametrize(
    "case,expected_code",
    [
        ("status", "CURRENT_IDENTITY_SEC_STATUS_REJECTED"),
        ("redirect", "CURRENT_IDENTITY_SEC_REDIRECT_REJECTED"),
        ("size", "CURRENT_IDENTITY_SEC_RESPONSE_TOO_LARGE"),
        ("deadline", "CURRENT_IDENTITY_SEC_DEADLINE_EXCEEDED"),
        ("budget", "CURRENT_IDENTITY_SEC_RETRY_BUDGET_EXHAUSTED"),
        ("unknown", "CURRENT_IDENTITY_FAILED"),
        ("unknown_policy", "CURRENT_IDENTITY_FAILED"),
    ],
)
def test_sec_cli_preserves_only_safe_policy_failures_without_retry(
    tmp_path, monkeypatch, capsys, case, expected_code
):
    import json
    import sys

    import httpx
    from helpers.current_identity import URL
    from nexus.providers.http import HttpPolicyError

    module = importlib.import_module("nexus.ops.instrument_identity_local")
    requests = []
    sensitive_error = "https://example.invalid/private?secret=synthetic_secret private_body"

    def respond(request):
        requests.append(request)
        if case == "deadline":
            raise TimeoutError(sensitive_error)
        if case == "unknown":
            raise RuntimeError(sensitive_error)
        if case == "unknown_policy":
            raise HttpPolicyError("STATUS_REJECTED " + sensitive_error)
        if case == "size":
            return httpx.Response(200, headers={"content-length": str(8 * 1024 * 1024 + 1)})
        status = {"status": 403, "redirect": 302, "budget": 503}[case]
        return httpx.Response(status, text=sensitive_error)

    client = httpx.AsyncClient(transport=httpx.MockTransport(respond), follow_redirects=False)
    monkeypatch.setattr(module.httpx, "AsyncClient", lambda **kwargs: client)
    monkeypatch.setattr(
        module, "load_local_environment", lambda *_: {"SEC_CONTACT": "test@example.invalid"}
    )
    directory = tmp_path / "sec"
    monkeypatch.setattr(
        sys,
        "argv",
        ["identity", "collect-sec", "--source-url", URL, "--output-dir", str(directory)],
    )
    with pytest.raises(SystemExit) as exit_info:
        module.main()
    assert exit_info.value.code == 1
    output = capsys.readouterr()
    assert json.loads(output.out) == {"status": "failed", "code": expected_code}
    assert not any(
        value in output.out + output.err
        for value in ("example.invalid", "synthetic_secret", "private_body")
    )
    assert len(requests) == 1
    assert client.is_closed
    assert directory.is_dir()
    assert list(directory.iterdir()) == []


@pytest.mark.parametrize(
    "before,after", [(b"xbrli:entity", b"example:entity"), (b"table>", b"div>")]
)
def test_context_entity_and_registration_table_must_be_supported(before, after):
    module = importlib.import_module("nexus.market.current_identity")
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.parse_current_common_stock(
            symbol="AAPL", fmp_raw=profile(), sec_raw=filing().replace(before, after)
        )


def _cover_without_exchange_axis():
    return (
        filing()
        .replace(b'xmlns:exch="http://xbrl.sec.gov/exch/2025"', b"")
        .replace(
            b'<xbrldi:explicitMember dimension="dei:EntityListingsExchangeAxis">'
            b"exch:XNYS</xbrldi:explicitMember>",
            b"",
        )
        .replace(b"New York Stock Exchange", b"The Nasdaq Stock Market LLC")
    )


def test_common_stock_cover_can_bind_exchange_by_same_registration_row():
    module = importlib.import_module("nexus.market.current_identity")
    result = module.parse_current_common_stock(
        symbol="AAPL",
        fmp_raw=profile(exchange="NASDAQ"),
        sec_raw=_cover_without_exchange_axis(),
    )
    assert result.exchange == "XNAS"
    assert not any(
        f["attrs"].get("dimension") == "dei:EntityListingsExchangeAxis"
        for f in result.context["fields"]
    )


@pytest.mark.parametrize(
    "case", ["wrong_exchange", "wrong_class", "unbound_axis", "bad_namespace", "unknown_axis"]
)
def test_exchange_without_axis_still_requires_exact_security_evidence(case):
    module = importlib.import_module("nexus.market.current_identity")
    raw = _cover_without_exchange_axis()
    if case == "wrong_exchange":
        raw = raw.replace(b"The Nasdaq Stock Market LLC", b"New York Stock Exchange")
    elif case == "wrong_class":
        raw = raw.replace(b"us-gaap:CommonStockMember", b"us-gaap:PreferredStockMember")
    elif case == "unbound_axis":
        raw = raw.replace(
            b"</xbrli:segment>",
            b'<xbrldi:explicitMember dimension="dei:EntityListingsExchangeAxis">'
            b"exch:XNAS</xbrldi:explicitMember></xbrli:segment>",
        )
    elif case == "unknown_axis":
        raw = raw.replace(
            b"</xbrli:segment>",
            b'<xbrldi:explicitMember dimension="dei:OtherAxis">'
            b"us-gaap:CommonStockMember</xbrldi:explicitMember></xbrli:segment>",
        )
    else:
        raw = raw.replace(b"<html ", b'<html xmlns:exch="http://example.invalid/exch" ')
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.parse_current_common_stock(
            symbol="AAPL", fmp_raw=profile(exchange="NASDAQ"), sec_raw=raw
        )


def _cover_without_segment():
    return re.sub(rb"<xbrli:segment>.*?</xbrli:segment>", b"", filing(), flags=re.S)


def test_plain_entity_duration_cover_loads_without_inventing_stock_dimension(tmp_path):
    module = importlib.import_module("nexus.market.current_identity")
    fm, sc = archives(tmp_path, sec=_cover_without_segment())
    result = module.load_identity_archives(symbol="AAPL", fmp_archive=fm, sec_archive=sc)
    assert result.identity.sec_title == "Common Stock"
    assert result.identity.exchange == "XNYS"
    assert [f["tag"] for f in result.identity.context["fields"]] == [
        "xbrli:identifier",
        "xbrli:startdate",
        "xbrli:enddate",
    ]


@pytest.mark.parametrize(
    "before,after",
    [
        (b"</xbrli:entity>", b"<xbrli:segment></xbrli:segment></xbrli:entity>"),
        (b"</xbrli:period>", b"</xbrli:period><xbrli:scenario></xbrli:scenario>"),
        (
            b"</xbrli:entity>",
            b'<xbrldi:typedMember dimension="dei:OtherAxis">'
            b"<dei:Other>Preferred</dei:Other></xbrldi:typedMember></xbrli:entity>",
        ),
        (
            b"</xbrli:entity>",
            b'<xbrldi:explicitMember dimension="dei:OtherAxis">'
            b"us-gaap:CommonStockMember</xbrldi:explicitMember></xbrli:entity>",
        ),
        (b"</xbrli:entity>", b"<dei:Other>Preferred</dei:Other></xbrli:entity>"),
        (b"<xbrli:period>", b'<xbrli:period other="unknown">'),
        (b"<xbrli:period>", b"<xbrli:period>Preferred"),
        (b"<xbrli:startDate>", b"<dei:Other><xbrli:startDate>"),
        (b"</xbrli:entity>", b"</xbrli:entity><xbrli:entity></xbrli:entity>"),
    ],
)
def test_plain_cover_rejects_unproven_context_structure(before, after):
    module = importlib.import_module("nexus.market.current_identity")
    raw = _cover_without_segment().replace(before, after)
    if before == b"<xbrli:startDate>":
        raw = raw.replace(b"</xbrli:startDate>", b"</xbrli:startDate></dei:Other>")
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.parse_current_common_stock(symbol="AAPL", fmp_raw=profile(), sec_raw=raw)


@pytest.mark.parametrize(
    "before,after",
    [
        (b">Common Stock<", b">Preferred Stock<"),
        (b">Common Stock<", b">Common Stock Notes<"),
        (b">Common Stock<", b">Common Stock Warrants<"),
        (b">0000123456<", b">0000654321<"),
        (b"New York Stock Exchange", b"The Nasdaq Stock Market LLC"),
        (b'<td><ix:nonNumeric id="exchange"', b'</tr><tr><td><ix:nonNumeric id="exchange"'),
        (b'contextRef="stock">AAPL', b'contextRef="other">AAPL'),
        (
            b"</body>",
            b'<ix:nonNumeric id="again" name="dei:TradingSymbol" '
            b'contextRef="stock">AAPL</ix:nonNumeric></body>',
        ),
    ],
)
def test_plain_cover_keeps_same_security_and_exchange_guards(before, after):
    module = importlib.import_module("nexus.market.current_identity")
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.parse_current_common_stock(
            symbol="AAPL",
            fmp_raw=profile(),
            sec_raw=_cover_without_segment().replace(before, after),
        )


@pytest.mark.parametrize(
    "label",
    [
        "The NASDAQ Global Select Market",
        "Nasdaq Global Select Market",
        "The Nasdaq Global Select Market",
        "NASDAQ Global Select Market",
        "Nasdaq Stock Market LLC",
    ],
)
def test_global_select_exact_cover_alias_binds_nasdaq_not_separate_exchange(label):
    module = importlib.import_module("nexus.market.current_identity")
    raw = _cover_without_exchange_axis().replace(b"The Nasdaq Stock Market LLC", label.encode())
    result = module.parse_current_common_stock(
        symbol="AAPL", fmp_raw=profile(exchange="NASDAQ"), sec_raw=raw
    )
    assert result.exchange == "XNAS"
    assert result.sec_exchange == label


@pytest.mark.parametrize(
    "label",
    [
        b"The NASDAQ Global Select Market Other",
        b"Not The NASDAQ Global Select Market",
        b"The-NASDAQ-Global-Select-Market",
        b"New York Stock Exchange",
    ],
)
def test_global_select_alias_is_not_a_substring_or_fuzzy_match(label):
    module = importlib.import_module("nexus.market.current_identity")
    raw = _cover_without_exchange_axis().replace(b"The Nasdaq Stock Market LLC", label)
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.parse_current_common_stock(
            symbol="AAPL", fmp_raw=profile(exchange="NASDAQ"), sec_raw=raw
        )


def _class_a_cover():
    return (
        filing()
        .replace(b"us-gaap:CommonStockMember", b"us-gaap:CommonClassAMember")
        .replace(b">Common Stock<", b">Class A Common Stock<")
        .replace(
            b'<xbrldi:explicitMember dimension="dei:EntityListingsExchangeAxis">'
            b"exch:XNYS</xbrldi:explicitMember>",
            b"",
        )
    )


def test_class_a_archive_preserves_explicit_class_and_registration_title(tmp_path):
    module = importlib.import_module("nexus.market.current_identity")
    fm, sc = archives(tmp_path, sec=_class_a_cover())
    identity = module.load_identity_archives(symbol="AAPL", fmp_archive=fm, sec_archive=sc).identity
    assert identity.sec_title == "Class A Common Stock"
    assert identity.symbol == "AAPL"
    assert identity.exchange == "XNYS"
    assert [
        f["value"] for f in identity.context["fields"] if f["tag"] == "xbrldi:explicitmember"
    ] == ["us-gaap:CommonClassAMember"]


@pytest.mark.parametrize(
    "before,after",
    [
        (b"us-gaap:CommonClassAMember", b"us-gaap:CommonStockMember"),
        (b"us-gaap:CommonClassAMember", b"us-gaap:CommonClassBMember"),
        (b">Class A Common Stock<", b">Common Stock<"),
        (b">Class A Common Stock<", b">Class B Common Stock<"),
        (b">Class A Common Stock<", b">Class A Common Stock / Class B Common Stock<"),
        (b">Class A Common Stock<", b">Class A Preferred Stock<"),
        (b">Class A Common Stock<", b">Class A Common Stock Notes<"),
        (b">Class A Common Stock<", b">Class A Common Stock Warrants<"),
        (b">Class A Common Stock<", b">Class A Common Stock Fund<"),
        (
            b"</xbrli:segment>",
            b'<xbrldi:explicitMember dimension="dei:OtherAxis">'
            b"us-gaap:CommonClassAMember</xbrldi:explicitMember></xbrli:segment>",
        ),
        (
            b"</xbrli:segment>",
            b'<xbrldi:typedMember dimension="dei:OtherAxis">'
            b"<dei:Other>Preferred</dei:Other></xbrldi:typedMember></xbrli:segment>",
        ),
        (b"</xbrli:period>", b"</xbrli:period><xbrli:scenario></xbrli:scenario>"),
        (b"</xbrli:segment>", b"<dei:Other>Preferred</dei:Other></xbrli:segment>"),
        (b"<xbrli:segment>", b'<xbrli:segment other="unknown">'),
        (b"<xbrli:segment>", b"<xbrli:segment>Preferred"),
        (b'contextRef="stock">AAPL', b'contextRef="other">AAPL'),
        (b'<td><ix:nonNumeric id="exchange"', b'</tr><tr><td><ix:nonNumeric id="exchange"'),
        (b"New York Stock Exchange", b"Nasdaq Stock Market LLC"),
        (b">AAPL<", b">MSFT<"),
    ],
)
def test_class_a_requires_exact_class_security_structure_and_exchange(before, after):
    module = importlib.import_module("nexus.market.current_identity")
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.parse_current_common_stock(
            symbol="AAPL", fmp_raw=profile(), sec_raw=_class_a_cover().replace(before, after)
        )


def test_class_a_cannot_substitute_another_fmp_symbol_with_same_cik():
    module = importlib.import_module("nexus.market.current_identity")
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.parse_current_common_stock(
            symbol="AAPL", fmp_raw=profile(symbol="MSFT"), sec_raw=_class_a_cover()
        )


def test_plain_context_cannot_imply_a_class_a_member_from_title():
    module = importlib.import_module("nexus.market.current_identity")
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.parse_current_common_stock(
            symbol="AAPL",
            fmp_raw=profile(),
            sec_raw=_cover_without_segment().replace(b">Common Stock<", b">Class A Common Stock<"),
        )


def test_multiple_registrants_bind_default_entity_without_discarding_other_cik(tmp_path):
    module = importlib.import_module("nexus.market.current_identity")
    fm, sc = archives(tmp_path, sec=_multiple_registrant_cover())
    identity = module.load_identity_archives(symbol="AAPL", fmp_archive=fm, sec_archive=sc).identity
    assert identity.cik == "0000123456"
    assert identity.symbol == "AAPL"
    assert {f["value"] for f in identity.facts if f["name"] == "dei:EntityCentralIndexKey"} == {
        "0000123456",
        "0000654321",
    }


def test_default_registrant_is_not_selected_by_cik_fact_document_order():
    module = importlib.import_module("nexus.market.current_identity")
    raw = _multiple_registrant_cover()
    primary = re.search(rb'<ix:nonNumeric id="cik".*?</ix:nonNumeric>', raw, re.S)[0]
    raw = raw.replace(primary, b"").replace(b"</body>", primary + b"</body>")
    assert (
        module.parse_current_common_stock(symbol="AAPL", fmp_raw=profile(), sec_raw=raw).cik
        == "0000123456"
    )


def test_two_other_ciks_cannot_alias_the_same_expanded_legal_member():
    module = importlib.import_module("nexus.market.current_identity")
    raw = _multiple_registrant_cover().replace(
        b"<html ", b'<html xmlns:alias="https://example.invalid/2025" '
    )
    context = re.search(rb'<xbrli:context id="other">.*?</xbrli:context>', raw, re.S)[0]
    third = context.replace(b'id="other"', b'id="third"').replace(
        b"sample:OtherMember", b"alias:OtherMember"
    )
    facts = re.search(rb'<ix:nonNumeric id="other-cik".*?(?=</body>)', raw, re.S)[0]
    third_facts = (
        facts.replace(b'id="other-', b'id="third-')
        .replace(b'contextRef="other"', b'contextRef="third"')
        .replace(b"0000654321", b"0000999999")
        .replace(b"Other Example", b"Third Example")
    )
    raw = raw.replace(b"</ix:header>", third + b"</ix:header>").replace(
        b"</body>", third_facts + b"</body>"
    )
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.parse_current_common_stock(symbol="AAPL", fmp_raw=profile(), sec_raw=raw)


@pytest.mark.parametrize(
    "case",
    [
        "unbound_member",
        "bad_member_qname",
        "standard_member",
        "standard_namespace_alias",
        "wrong_axis",
        "typed_member",
        "scenario",
        "extra_element",
        "unscoped_other_cik",
        "duplicate_target",
        "duplicate_other",
        "wrong_primary_period",
        "wrong_other_period",
        "wrong_context_entity",
        "missing_name",
        "missing_tax",
        "name_wrong_context",
        "duplicate_name",
        "wrong_stock_context",
        "wrong_stock_symbol",
        "wrong_fmp_symbol",
        "cross_registration_row",
        "same_scope_two_ciks",
    ],
)
def test_multiple_registrant_scope_must_be_complete_and_unambiguous(case):
    module = importlib.import_module("nexus.market.current_identity")
    raw = _multiple_registrant_cover()
    fm = profile()
    if case == "unbound_member":
        raw = raw.replace(b'xmlns:sample="https://example.invalid/2025"', b"")
    elif case == "bad_member_qname":
        raw = raw.replace(b"sample:OtherMember", b"sample:OtherMember:Invalid")
    elif case == "standard_member":
        raw = raw.replace(b"sample:OtherMember", b"us-gaap:ParentCompanyMember")
    elif case == "standard_namespace_alias":
        raw = raw.replace(b"https://example.invalid/2025", b"http://fasb.org/us-gaap/2025")
    elif case == "wrong_axis":
        raw = raw.replace(b"dei:LegalEntityAxis", b"dei:OtherAxis")
    elif case == "typed_member":
        raw = raw.replace(
            b'<xbrli:segment><xbrldi:explicitMember dimension="dei:LegalEntityAxis">',
            b'<xbrli:segment><xbrldi:typedMember dimension="dei:OtherAxis">'
            b"sample:OtherMember</xbrldi:typedMember>"
            b'<xbrldi:explicitMember dimension="dei:LegalEntityAxis">',
        )
    elif case in {"scenario", "extra_element", "wrong_other_period", "wrong_context_entity"}:
        other = re.search(rb'<xbrli:context id="other">.*?</xbrli:context>', raw, re.S)[0]
        changed = other
        if case == "scenario":
            changed = other.replace(
                b"</xbrli:context>", b"<xbrli:scenario></xbrli:scenario></xbrli:context>"
            )
        elif case == "extra_element":
            changed = other.replace(
                b"</xbrli:segment>", b"<sample:Unknown></sample:Unknown></xbrli:segment>"
            )
        elif case == "wrong_other_period":
            changed = other.replace(b"2025-12-31", b"2025-12-30")
        else:
            changed = other.replace(b"0000123456", b"0000999999")
        raw = raw.replace(other, changed)
    elif case == "unscoped_other_cik":
        raw = raw.replace(b'contextRef="other">0000654321', b'contextRef="primary">0000654321')
    elif case == "duplicate_target":
        raw = raw.replace(b">0000654321<", b">0000123456<")
    elif case == "duplicate_other":
        raw = raw.replace(
            b"</body>",
            b'<ix:nonNumeric id="other-again" name="dei:EntityCentralIndexKey" '
            b'contextRef="other">0000654321</ix:nonNumeric></body>',
        )
    elif case == "wrong_primary_period":
        primary = re.search(rb'<xbrli:context id="primary">.*?</xbrli:context>', raw, re.S)[0]
        raw = raw.replace(primary, primary.replace(b"2025-12-31", b"2025-12-30"))
    elif case in {"missing_name", "missing_tax"}:
        key = b"other-name" if case == "missing_name" else b"other-tax"
        raw = re.sub(rb'<ix:nonNumeric id="' + key + rb'".*?</ix:nonNumeric>', b"", raw, flags=re.S)
    elif case == "name_wrong_context":
        raw = raw.replace(
            b'contextRef="other">Other Example', b'contextRef="primary">Other Example'
        )
    elif case == "duplicate_name":
        raw = raw.replace(
            b"</body>",
            b'<ix:nonNumeric id="other-name-again" name="dei:EntityRegistrantName" '
            b'contextRef="other">Other Example</ix:nonNumeric></body>',
        )
    elif case == "wrong_stock_context":
        raw = raw.replace(b'contextRef="stock">AAPL', b'contextRef="other">AAPL')
    elif case == "wrong_stock_symbol":
        raw = raw.replace(b">AAPL<", b">MSFT<")
    elif case == "wrong_fmp_symbol":
        fm = profile(symbol="MSFT")
    elif case == "cross_registration_row":
        raw = raw.replace(
            b'<td><ix:nonNumeric id="exchange"', b'</tr><tr><td><ix:nonNumeric id="exchange"'
        )
    else:
        raw = raw.replace(
            b"</body>",
            b'<ix:nonNumeric id="third-cik" name="dei:EntityCentralIndexKey" '
            b'contextRef="other">0000999999</ix:nonNumeric></body>',
        )
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.parse_current_common_stock(symbol="AAPL", fmp_raw=fm, sec_raw=raw)
