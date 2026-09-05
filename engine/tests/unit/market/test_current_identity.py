import importlib
import re
from datetime import UTC, datetime, timedelta

import pytest
from helpers.current_identity import archives, filing, profile


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
