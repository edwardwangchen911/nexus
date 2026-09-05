import importlib
import re
from datetime import UTC, datetime, timedelta

import pytest
from helpers.current_identity import archives, dual_listing_filing, filing, profile
from helpers.current_identity import multiple_registrant_filing as _multiple_registrant_cover


def test_explicit_same_security_identity():
    module = importlib.import_module("nexus.market.current_identity")
    result = module.parse_current_common_stock(symbol="AAPL", fmp_raw=profile(), sec_raw=filing())
    assert result.cik == "0000123456"
    assert result.exchange == "XNYS"
    assert result.context_id == "stock"
    assert result.sec_title == "Common Stock"


def test_dual_listing_rejects_cik_fact_from_another_period():
    raw = dual_listing_filing()
    context = re.search(rb'<xbrli:context id="stock">.*?</xbrli:context>', raw, re.S)[0]
    old = context.replace(b'id="stock"', b'id="issuer"').replace(b"2025-01-01", b"2024-01-01")
    raw = raw.replace(b"</ix:header>", old + b"</ix:header>").replace(
        b'contextRef="stock">0000123456', b'contextRef="issuer">0000123456'
    )
    module = importlib.import_module("nexus.market.current_identity")
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.parse_current_common_stock(symbol="AAPL", fmp_raw=profile(), sec_raw=raw)


def test_dual_listing_uses_exact_visible_listing_and_preserves_auxiliary_evidence(tmp_path):
    module = importlib.import_module("nexus.market.current_identity")
    fm, sc = archives(tmp_path, sec=dual_listing_filing())
    result = module.load_identity_archives(symbol="AAPL", fmp_archive=fm, sec_archive=sc).identity
    assert result.context_id == "stock"
    assert result.exchange == "XNYS"
    assert result.sec_title == "Common Stock"
    assert result.sec_exchange == "New York Stock Exchange"
    assert len(result.facts) == 7
    listings = [f for f in result.facts if "listing_context" in f]
    assert len(listings) == 6
    assert {f["listing_context"]["id"] for f in listings} == {"stock", "chicago"}
    assert sum(f["selected_listing"] for f in listings) == 3
    hidden = next(f for f in listings if f["id"] == "chicago-exchange")
    assert hidden["visible"] is False and hidden["value"] == "CHX"


@pytest.mark.parametrize(
    "before,after",
    [
        (b'contextRef="chicago">CHX', b'contextRef="chicago">NASDAQ'),
        (b"exch:XCHI", b"exch:XNAS"),
        (b'id="symbol"', b'id="symbol" style="display:none"'),
        (
            b">Common Stock</ix:nonNumeric></ix:nonNumeric>",
            b">Common Stock</ix:nonNumeric> Preferred</ix:nonNumeric>",
        ),
        (
            b'contextRef="chicago"><ix:nonNumeric id="symbol"',
            b'contextRef="stock"><ix:nonNumeric id="symbol"',
        ),
        (b'<td><ix:nonNumeric id="exchange"', b'</tr><tr><td><ix:nonNumeric id="exchange"'),
        (b">AAPL</ix:nonNumeric>", b"><ix:exclude>AAPL</ix:exclude></ix:nonNumeric>"),
        (
            b"</body>",
            b'<table><tr><td><ix:nonNumeric id="extra" name="dei:TradingSymbol" '
            b'contextRef="stock">AAPL</ix:nonNumeric></td></tr></table></body>',
        ),
    ],
)
def test_dual_listing_rejects_ambiguous_or_mismatched_scope(before, after):
    raw = dual_listing_filing()
    assert before in raw
    module = importlib.import_module("nexus.market.current_identity")
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.parse_current_common_stock(
            symbol="AAPL", fmp_raw=profile(), sec_raw=raw.replace(before, after)
        )


def _dual_exchange_transform():
    return (
        dual_listing_filing()
        .replace(
            b"<html ",
            b'<html xmlns:ixt-sec="http://www.sec.gov/inlineXBRL/transformation/2015-08-31" ',
        )
        .replace(b'id="exchange"', b'id="exchange" format="ixt-sec:exchnameen"')
    )


def test_dual_listing_accepts_exact_sec_exchange_transform():
    module = importlib.import_module("nexus.market.current_identity")
    result = module.parse_current_common_stock(
        symbol="AAPL", fmp_raw=profile(), sec_raw=_dual_exchange_transform()
    )
    assert result.exchange == "XNYS" and result.sec_exchange == "New York Stock Exchange"


@pytest.mark.parametrize(
    "before,after",
    [
        (
            b"http://www.sec.gov/inlineXBRL/transformation/2015-08-31",
            b"https://example.invalid/transformation",
        ),
        (b"ixt-sec:exchnameen", b"ixt-sec:unknown"),
        (b'id="title"', b'id="title" format="ixt-sec:exchnameen"'),
        (b'contextRef="chicago">CHX', b'contextRef="chicago" target="other">CHX'),
    ],
)
def test_dual_listing_rejects_unverified_transform(before, after):
    module = importlib.import_module("nexus.market.current_identity")
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.parse_current_common_stock(
            symbol="AAPL",
            fmp_raw=profile(),
            sec_raw=_dual_exchange_transform().replace(before, after),
        )


@pytest.mark.parametrize(
    "before,after",
    [
        (b"0000123456", b"0000654321"),
        (b"2025-01-01", b"2024-01-01"),
        (b"us-gaap:CommonStockMember", b"us-gaap:PreferredStockMember"),
        (b"</xbrli:entity>", b"<xbrli:scenario></xbrli:scenario></xbrli:entity>"),
    ],
)
def test_dual_listing_rejects_secondary_entity_period_class_or_shape_mismatch(before, after):
    raw = dual_listing_filing()
    context = re.search(rb'<xbrli:context id="chicago">.*?</xbrli:context>', raw, re.S)[0]
    assert before in context
    module = importlib.import_module("nexus.market.current_identity")
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.parse_current_common_stock(
            symbol="AAPL",
            fmp_raw=profile(),
            sec_raw=raw.replace(context, context.replace(before, after)),
        )


def test_dual_listing_does_not_choose_exchange_without_fmp_intersection():
    module = importlib.import_module("nexus.market.current_identity")
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.parse_current_common_stock(
            symbol="AAPL", fmp_raw=profile(exchange="NASDAQ"), sec_raw=dual_listing_filing()
        )


def test_dual_listing_capital_stock_requires_explicit_common_member():
    module = importlib.import_module("nexus.market.current_identity")
    raw = dual_listing_filing().replace(
        b">Common Stock<", b">Capital Stock, par value $.01 per share<"
    )
    result = module.parse_current_common_stock(symbol="AAPL", fmp_raw=profile(), sec_raw=raw)
    assert result.sec_title == "Capital Stock, par value $.01 per share"
    assert result.exchange == "XNYS"


@pytest.mark.parametrize(
    "title",
    [
        b"Capital Stock",
        b"Capital Stock, par value $.01 per share Preferred",
        b"Capital Stock, par value $.01 per share Class B",
        b"Capital Stock Units",
        b"Capital Stock, par value $.01 per share Depositary",
        b"Capital Stock, par value $.01 per share; additional rights",
        b"Capital Stock, par value $-0.01 per share",
        b"Capital Stock, par value $NaN per share",
        b"Capital Stock, par value $0.00 per share",
        b"Capital Stock, par value $1e-2 per share",
    ],
)
def test_dual_listing_rejects_unproven_capital_stock_title(title):
    module = importlib.import_module("nexus.market.current_identity")
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.parse_current_common_stock(
            symbol="AAPL",
            fmp_raw=profile(),
            sec_raw=dual_listing_filing().replace(b">Common Stock<", b">" + title + b"<"),
        )


def test_single_listing_capital_stock_policy_is_unchanged():
    module = importlib.import_module("nexus.market.current_identity")
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.parse_current_common_stock(
            symbol="AAPL",
            fmp_raw=profile(),
            sec_raw=filing().replace(
                b">Common Stock<", b">Capital Stock, par value $.01 per share<"
            ),
        )


@pytest.mark.parametrize(
    "before,after",
    [
        (b">Common Stock<", b">Common Stock Class B<"),
        (b"<table><tr>", b"<ix:unknown><table><tr>"),
    ],
)
def test_dual_listing_rejects_class_tail_or_unknown_ix_ancestor(before, after):
    raw = dual_listing_filing().replace(before, after)
    if b"<ix:unknown>" in raw:
        raw = raw.replace(b"</table>", b"</table></ix:unknown>")
    module = importlib.import_module("nexus.market.current_identity")
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.parse_current_common_stock(symbol="AAPL", fmp_raw=profile(), sec_raw=raw)


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
    identity = module.parse_current_common_stock(
        symbol="AAPL",
        fmp_raw=profile(),
        sec_raw=_cover_without_segment().replace(b">Common Stock<", b">Class A Common Stock<"),
    )
    assert identity.sec_title == "Class A Common Stock"
    assert not any(f["tag"] == "xbrldi:explicitmember" for f in identity.context["fields"])


def _class_title_compat_cover(plain):
    if plain:
        return _cover_without_segment().replace(b">Common Stock<", b">Class A Common Stock<")
    return _class_a_cover().replace(b">Class A Common Stock<", b">Common Stock<")


@pytest.mark.parametrize("plain", [False, True])
def test_class_title_compat_normal_archive_retains_original_context_and_title(tmp_path, plain):
    module = importlib.import_module("nexus.market.current_identity")
    fm, sc = archives(tmp_path, sec=_class_title_compat_cover(plain))
    identity = module.load_identity_archives(symbol="AAPL", fmp_archive=fm, sec_archive=sc).identity
    assert identity.sec_title == ("Class A Common Stock" if plain else "Common Stock")
    dimensions = [
        f["value"] for f in identity.context["fields"] if f["tag"] == "xbrldi:explicitmember"
    ]
    assert dimensions == ([] if plain else ["us-gaap:CommonClassAMember"])
    assert len(identity.facts) == 4


@pytest.mark.parametrize("plain", [False, True])
@pytest.mark.parametrize(
    "before,after",
    [
        (b'id="symbol"', b'id="symbol" format="ixt-sec:exchnameen"'),
        (b'id="exchange"', b'id="exchange" format="unknown:transform"'),
        (b'id="symbol"', b'id="symbol" target="other"'),
        (b'id="symbol"', b'id="symbol" tupleRef="other"'),
        (b"<table>", b'<table xml:base="https://example.invalid/">'),
        (b">AAPL</ix:nonNumeric>", b'><span hidden="hidden">AAPL</span></ix:nonNumeric>'),
        (b'<td><ix:nonNumeric id="symbol"', b'<td><ix:exclude><ix:nonNumeric id="symbol"'),
        (b'<td><ix:nonNumeric id="exchange"', b'</tr><tr><td><ix:nonNumeric id="exchange"'),
        (b'contextRef="stock">AAPL', b'contextRef="other">AAPL'),
        (b"http://fasb.org/us-gaap/2025", b"https://example.invalid/gaap"),
        (
            b">AAPL</ix:nonNumeric>",
            b'>AAPL</ix:nonNumeric><ix:nonNumeric id="duplicate" '
            b'name="dei:TradingSymbol" contextRef="stock">AAPL</ix:nonNumeric>',
        ),
    ],
)
def test_class_title_compat_rejects_unsafe_target_scope(plain, before, after):
    raw = _class_title_compat_cover(plain)
    assert before in raw
    if after.startswith(b"<td><ix:exclude>"):
        raw = raw.replace(b">AAPL</ix:nonNumeric></td>", b">AAPL</ix:nonNumeric></ix:exclude></td>")
    module = importlib.import_module("nexus.market.current_identity")
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.parse_current_common_stock(
            symbol="AAPL", fmp_raw=profile(), sec_raw=raw.replace(before, after)
        )


def _with_note_row(raw, *, same_context=False, duplicate_target=False):
    ctx = re.search(rb'<xbrli:context id="stock">.*?</xbrli:context>', raw, re.S)[0]
    note_ctx = ctx.replace(b'id="stock"', b'id="notes"')
    note_ctx = re.sub(rb"<xbrli:segment>.*?</xbrli:segment>", b"", note_ctx, flags=re.S)
    raw = raw.replace(b"</ix:header>", note_ctx + b"</ix:header>")
    row = re.search(rb"<table><tr>.*?</tr></table>", raw, re.S)[0]
    note = row.replace(b">Common Stock<", b">Notes due 2030<").replace(
        b">Class A Common Stock<", b">Notes due 2030<"
    )
    for identifier in (b"title", b"symbol", b"exchange"):
        note = note.replace(b'id="' + identifier + b'"', b'id="note-' + identifier + b'"')
    if not same_context:
        note = note.replace(b'contextRef="stock"', b'contextRef="notes"')
    if not duplicate_target:
        note = note.replace(b">AAPL<", b">SYN2030<")
    return raw.replace(b"</body>", note + b"</body>")


def test_generic_class_a_target_keeps_separate_notes_out_of_target_proof():
    module = importlib.import_module("nexus.market.current_identity")
    identity = module.parse_current_common_stock(
        symbol="AAPL", fmp_raw=profile(), sec_raw=_with_note_row(_class_title_compat_cover(False))
    )
    assert identity.sec_title == "Common Stock"
    assert len(identity.facts) == 4
    assert not any(f["id"].startswith("note-") for f in identity.facts)


@pytest.mark.parametrize(
    "plain,same,duplicate", [(True, False, False), (False, True, False), (False, False, True)]
)
def test_class_title_compat_does_not_ignore_ambiguous_other_security(plain, same, duplicate):
    module = importlib.import_module("nexus.market.current_identity")
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.parse_current_common_stock(
            symbol="AAPL",
            fmp_raw=profile(),
            sec_raw=_with_note_row(
                _class_title_compat_cover(plain), same_context=same, duplicate_target=duplicate
            ),
        )


@pytest.mark.parametrize("plain", [False, True])
def test_class_title_compat_rejects_cik_fact_from_other_period(plain):
    raw = _class_title_compat_cover(plain)
    context = re.search(rb'<xbrli:context id="stock">.*?</xbrli:context>', raw, re.S)[0]
    old = context.replace(b'id="stock"', b'id="issuer"').replace(b"2025-01-01", b"2024-01-01")
    raw = raw.replace(b"</ix:header>", old + b"</ix:header>").replace(
        b'contextRef="stock">0000123456', b'contextRef="issuer">0000123456'
    )
    module = importlib.import_module("nexus.market.current_identity")
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.parse_current_common_stock(symbol="AAPL", fmp_raw=profile(), sec_raw=raw)


@pytest.mark.parametrize("plain", [False, True])
def test_class_title_compat_does_not_interpret_unrelated_inline_relationship(plain):
    raw = _class_title_compat_cover(plain).replace(
        b"</ix:header>",
        b'<ix:relationship fromRefs="unrelated-fact" toRefs="unrelated-footnote" '
        b'arcrole="http://www.xbrl.org/2003/arcrole/fact-footnote" '
        b'linkRole="http://www.xbrl.org/2003/role/link" order="1"/>'
        b"</ix:header>",
    )
    module = importlib.import_module("nexus.market.current_identity")
    identity = module.parse_current_common_stock(symbol="AAPL", fmp_raw=profile(), sec_raw=raw)
    assert len(identity.facts) == 4


@pytest.mark.parametrize("plain", [False, True])
@pytest.mark.parametrize("reference", ["symbol", "title", "exchange", "cik", "stock"])
@pytest.mark.parametrize("attribute", ["fromRefs", "toRefs"])
def test_class_title_compat_rejects_external_relationship_to_identity(plain, reference, attribute):
    raw = _class_title_compat_cover(plain).replace(
        b"</ix:header>",
        f'<ix:relationship {attribute}="unrelated {reference}" order="1"/></ix:header>'.encode(),
    )
    module = importlib.import_module("nexus.market.current_identity")
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.parse_current_common_stock(symbol="AAPL", fmp_raw=profile(), sec_raw=raw)


@pytest.mark.parametrize("plain", [False, True])
@pytest.mark.parametrize(
    "attribute",
    ['order="1"', 'xml:base="https://example.invalid/"', 'target="other"', 'tupleRef="other"'],
)
@pytest.mark.parametrize("node", ["ix:header", "xbrli:entity", "table"])
def test_class_title_compat_rejects_identity_context_ancestor_or_child_attributes(
    plain, attribute, node
):
    raw = _class_title_compat_cover(plain).replace(
        f"<{node}>".encode(), f"<{node} {attribute}>".encode()
    )
    module = importlib.import_module("nexus.market.current_identity")
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.parse_current_common_stock(symbol="AAPL", fmp_raw=profile(), sec_raw=raw)


@pytest.mark.parametrize("plain", [False, True])
def test_class_title_compat_rejects_unknown_inline_context_ancestor(plain):
    raw = (
        _class_title_compat_cover(plain)
        .replace(b"<xbrli:context", b"<ix:exclude><xbrli:context")
        .replace(b"</xbrli:context>", b"</xbrli:context></ix:exclude>")
    )
    module = importlib.import_module("nexus.market.current_identity")
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.parse_current_common_stock(symbol="AAPL", fmp_raw=profile(), sec_raw=raw)


@pytest.mark.parametrize("plain", [False, True])
@pytest.mark.parametrize(
    "title",
    [
        "Class B Common Stock",
        "Class A Common Stock / Class B",
        "Common Stock Class B",
        "Capital Stock, par value $0.01 per share",
        "Not Common Stock",
        "Common Stock Notes",
        "Class A Common Stock Preferred",
        "Class A Common Stock Depositary Shares",
        "Common Stock Warrants",
        "Common Stock Fund",
    ],
)
def test_class_title_compat_rejects_conflicting_or_disguised_titles(plain, title):
    raw = _class_title_compat_cover(plain)
    old = b">Class A Common Stock<" if plain else b">Common Stock<"
    module = importlib.import_module("nexus.market.current_identity")
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_"):
        module.parse_current_common_stock(
            symbol="AAPL", fmp_raw=profile(), sec_raw=raw.replace(old, b">" + title.encode() + b"<")
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
