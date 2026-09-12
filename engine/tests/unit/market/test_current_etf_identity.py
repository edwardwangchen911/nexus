"""Synthetic fund disclosures, never captured issuer or account records."""

from datetime import UTC, datetime

import pytest
from helpers.current_etf_identity import originals, share_class_document


@pytest.mark.asyncio
async def test_current_held_equity_etf_can_be_validated_without_joining_fixed_universe():
    from nexus.market.current_etf_identity import validate_current_held_etf_originals

    symbol = "SYNX"
    document = b"""<html><body>FORM 497K Synthetic Held Equity Fund SYNX | NYSE Arca
Summary Prospectus Investment Objective The Synthetic Held Equity Fund seeks to track
an index composed of broadly diversified U.S. equities. Fees and Expenses
Purchase and Sale of Fund Shares The Fund is an exchange-traded fund.
Tax Information</body></html>"""
    result = await validate_current_held_etf_originals(
        symbol=symbol,
        contents=originals(
            symbol=symbol,
            company_name="Synthetic Held Equity Fund",
            document=document,
        ),
        now=datetime.now(UTC),
    )

    assert result.identity.symbol == symbol
    assert result.identity.asset_type == "equity_etf"
    with pytest.raises(ValueError, match="CURRENT_ETF_IDENTITY_INVALID"):
        from nexus.market.current_etf_identity import member_for

        member_for(symbol)


@pytest.mark.asyncio
async def test_current_held_equity_etf_accepts_company_index_497k_wording():
    from nexus.market.current_etf_identity import validate_current_held_etf_originals

    symbol = "SYNX"
    document = b"""<html><body>497K Summary Prospectus May 1, 2026
SYNX Synthetic Held Equity ETF Summary Prospectus Principal U.S. Listing Exchange: NYSE Arca.
Investment Objective The Fund seeks to replicate the performance of an index of companies
involved in synthetic materials production. Fees and Expenses Costs apply.
Principal Investment Strategies The Fund normally invests at least 80% of its total assets in
securities that comprise the index. The index tracks the performance of companies involved in
synthetic materials production. Principal Investment Risks Market risks apply.
Purchase and Sale of Fund Shares Shares trade on an exchange. Tax Information.</body></html>"""

    result = await validate_current_held_etf_originals(
        symbol=symbol,
        contents=originals(
            symbol=symbol,
            company_name="Synthetic Held Equity Fund",
            document=document,
        ),
        now=datetime.now(UTC),
    )

    assert result.identity.symbol == symbol
    assert result.identity.asset_type == "equity_etf"


@pytest.mark.asyncio
async def test_current_held_equity_etf_rejects_company_debt_index_wording():
    from nexus.market.current_etf_identity import validate_current_held_etf_originals

    symbol = "SYNX"
    document = b"""<html><body>497K Summary Prospectus May 1, 2026
SYNX Synthetic Debt ETF Summary Prospectus Principal U.S. Listing Exchange: NYSE Arca.
Investment Objective The Fund seeks to track a fixed income index of companies.
Fees and Expenses Costs apply. Principal Investment Strategies The Fund invests in debt
securities issued by companies in the index. Principal Investment Risks Credit risks apply.
Purchase and Sale of Fund Shares Shares trade on an exchange. Tax Information.</body></html>"""

    with pytest.raises(ValueError, match="CURRENT_ETF_IDENTITY_INVALID"):
        await validate_current_held_etf_originals(
            symbol=symbol,
            contents=originals(
                symbol=symbol,
                company_name="Synthetic Debt Fund",
                document=document,
            ),
            now=datetime.now(UTC),
        )


@pytest.mark.asyncio
async def test_current_held_validation_fails_closed_when_fixed_definition_is_unavailable(
    monkeypatch,
):
    from nexus.market import current_etf_identity as identity

    monkeypatch.setattr(
        identity,
        "load_us_liquid_v1_definition",
        lambda _effective: (_ for _ in ()).throw(ValueError("definition unavailable")),
    )
    with pytest.raises(ValueError, match="CURRENT_ETF_IDENTITY_INVALID"):
        await identity.validate_current_held_etf_originals(
            symbol="SYNX",
            contents=originals(
                symbol="SYNX",
                company_name="Synthetic Held Equity Fund",
                document=b"""<html><body>FORM 497K Synthetic Held Equity Fund SYNX | NYSE Arca
Summary Prospectus Investment Objective The Synthetic Held Equity Fund seeks to track
an index composed of broadly diversified U.S. equities. Fees and Expenses
Purchase and Sale of Fund Shares The Fund is an exchange-traded fund.
Tax Information</body></html>""",
            ),
            now=datetime.now(UTC),
        )


@pytest.mark.asyncio
async def test_current_held_nasdaq_equity_fund_uses_bound_497k_strategy():
    import json
    from hashlib import sha256

    from nexus.market.current_etf_identity import validate_current_held_etf_originals

    symbol = "SYNX"
    name = "Synthetic Daily Equity Fund"
    document = f"""<html><body>497K Summary Prospectus February 27, 2026
{symbol} {name} The Nasdaq Stock Market LLC Before you invest.
Investment Objective The Fund seeks daily investment results tied to an underlying security.
Fund Fees and Expenses Expenses apply.
Principal Investment Strategy The Fund uses swap agreements and may invest in equity securities.
The underlying security is common stock of a United States company.
Principal Investment Risks Publicly issued equity securities, including common stocks, are subject
to market risks that may cause their prices to fluctuate over time. Fluctuations in the value of
equity securities in which the Fund invests, and/or has exposure to will affect the Fund.
Unlike most ETFs, the Fund currently intends to effect creations and redemptions for cash.
Performance information.
Purchase and Sale of Fund Shares Individual shares trade in the secondary market through a
broker-dealer. Only Authorized Participants may transact directly with the Fund.
Tax Information Taxes apply.</body></html>""".encode()
    values = list(originals(symbol=symbol, exchange="NASDAQ", company_name=name, document=document))
    values[4] = f"""<html><body><div id="seriesDiv"><table>
<tr><td>Series S000000001</td><td></td><td>{name}</td></tr>
<tr><td>Class/Contract C000000002</td><td></td><td>{name}</td><td>{symbol}</td></tr>
</table></div><a href="https://www.sec.gov/Archives/edgar/data/123456/000012345626000001/sample.htm">497K</a>
</body></html>""".encode()
    receipts = json.loads(values[6])
    for receipt, raw in zip(receipts, values[3:6], strict=True):
        receipt["sha256"] = sha256(raw).hexdigest()
    values[6] = json.dumps(receipts).encode()

    result = await validate_current_held_etf_originals(
        symbol=symbol, contents=tuple(values), now=datetime.now(UTC)
    )
    assert result.identity.exchange == "XNAS"
    assert result.identity.asset_type == "equity_etf"
    assert result.identity.symbol == symbol


@pytest.mark.asyncio
async def test_current_held_nasdaq_identity_accepts_bound_equity_wording_without_product_template():
    import json
    from hashlib import sha256

    from nexus.market.current_etf_identity import validate_current_held_etf_originals

    symbol = "SYNX"
    name = "Synthetic Select Equity Fund"
    document = f"""<html><body>Summary Prospectus February 27, 2026
{symbol} {name} is an exchange-traded fund listed on The Nasdaq Stock Market LLC.
Important information for investors.
Investment Objective The Fund seeks daily results linked to shares of a United States company.
Fund Fees and Expenses Expenses apply.
Principal Investment Strategies The Fund obtains equity exposure through securities and swaps.
Principal Risks Investing in equity securities involves market risk.
</body></html>""".encode()
    values = list(originals(symbol=symbol, exchange="NASDAQ", company_name=name, document=document))
    values[4] = f"""<html><body><div id="seriesDiv"><table>
<tr><td>Series S000000001</td><td></td><td>{name}</td></tr>
<tr><td>Class/Contract C000000002</td><td></td><td>{name}</td><td>{symbol}</td></tr>
</table></div><a href="https://www.sec.gov/Archives/edgar/data/123456/000012345626000001/sample.htm">497K</a>
</body></html>""".encode()
    receipts = json.loads(values[6])
    for receipt, raw in zip(receipts, values[3:6], strict=True):
        receipt["sha256"] = sha256(raw).hexdigest()
    values[6] = json.dumps(receipts).encode()

    result = await validate_current_held_etf_originals(
        symbol=symbol, contents=tuple(values), now=datetime.now(UTC)
    )

    assert result.identity.exchange == "XNAS"
    assert result.identity.asset_type == "equity_etf"
    assert "equity exposure" in result.identity.strategy_statement


def test_current_held_etf_proof_has_fail_closed_scope_flags():
    from nexus.market.current_etf_identity_storage import CurrentHeldEtfProof

    assert {
        "scope",
        "holding",
        "universe_published",
        "new_position_eligible",
        "historical_backtest_eligible",
        "execution_locked",
    } <= CurrentHeldEtfProof.model_fields.keys()


@pytest.mark.asyncio
async def test_bound_equity_fund_preserves_coarse_exchange_and_unknown_receipt():
    from nexus.market.current_etf_identity import validate_etf_originals

    result = await validate_etf_originals(symbol="IWM", contents=originals(), now=datetime.now(UTC))
    assert result.identity.exchange == "ARCX"
    assert result.identity.fmp_exchange == "AMEX"
    assert result.identity.asset_type == "equity_etf"
    assert result.identity.mapping_original_received_at is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        dict(is_etf=False),
        dict(exchange="NASDAQ"),
        dict(index_class="C000000003"),
        dict(
            document=b"<html><body>IWM | NYSE Arca Summary Prospectus "
            b"Underlying Synthetic Equity Fund is an exchange-traded fund.</body></html>"
        ),
        dict(
            document=b"<html><body>FORM 497K Synthetic Equity Fund "
            b"IWM | NYSE Arca Summary Prospectus "
            b"Investment Objective tracks bonds. Fees and Expenses "
            b"Purchase and Sale of Fund Shares The Fund is an exchange-traded fund. "
            b"Tax Information</body></html>"
        ),
    ],
)
async def test_inconsistent_or_insufficient_sources_rejected(changes):
    from nexus.market.current_etf_identity import validate_etf_originals

    with pytest.raises(ValueError, match="CURRENT_ETF_IDENTITY_INVALID"):
        await validate_etf_originals(
            symbol="IWM", contents=originals(**changes), now=datetime.now(UTC)
        )


@pytest.mark.asyncio
async def test_receipt_tamper_and_unsupported_type():
    from nexus.market.current_etf_identity import validate_etf_originals

    contents = list(originals())
    contents[5] += b" altered"
    for symbol, values in [("IWM", tuple(contents)), ("TLT", originals())]:
        with pytest.raises(ValueError, match="CURRENT_ETF_IDENTITY_INVALID"):
            await validate_etf_originals(symbol=symbol, contents=values, now=datetime.now(UTC))


@pytest.mark.asyncio
async def test_repeated_prospectus_reference_is_not_another_fund():
    from nexus.market.current_etf_identity import validate_etf_originals

    doc = originals()[5].replace(
        b"Investment Objective", b"See this Summary Prospectus. Investment Objective"
    )
    result = await validate_etf_originals(
        symbol="IWM", contents=originals(document=doc), now=datetime.now(UTC)
    )
    assert result.identity.symbol == "IWM"


@pytest.mark.asyncio
@pytest.mark.parametrize("http_ok", [False, True])
async def test_normal_collector_complete_archive_or_first_error(monkeypatch, tmp_path, http_ok):
    import io
    import json
    from types import SimpleNamespace

    import httpx
    from helpers.current_etf_identity import archives
    from nexus.ops import etf_identity_local as ops
    from nexus.settings import EngineSettings

    fm, sec, mapping = archives(tmp_path / "source")
    binding_path = sec / "mapping-binding.json"
    binding = json.loads(binding_path.read_text())
    binding.update(
        key=f"provider/market/raw/sec/2026-01-03/ticker-directory/{binding['sha256']}.json",
        version_id="synthetic-version",
    )
    binding["stored_at"] = "2026-01-03T00:00:00.123000+00:00"
    binding_path.write_text(json.dumps(binding))

    class Client:
        def list_object_versions(self, **kwargs):
            return dict(
                IsTruncated=False,
                Versions=[
                    dict(
                        Key=binding["key"],
                        VersionId="synthetic-version",
                        LastModified=datetime.fromisoformat(binding["stored_at"]),
                    )
                ],
            )

        def get_object(self, **kwargs):
            assert kwargs["VersionId"] == "synthetic-version"
            return dict(
                Body=io.BytesIO(mapping.read_bytes()),
                LastModified=datetime.fromisoformat(binding["stored_at"]).replace(microsecond=0),
                VersionId="synthetic-version",
            )

    storage = SimpleNamespace(_prefix="provider/market", _bucket="synthetic", _client=Client())
    monkeypatch.setattr(ops, "S3ProviderEvidenceStore", lambda *args, **kwargs: storage)
    calls = []

    def handle(request):
        calls.append(str(request.url))
        return (
            httpx.Response(200, content=originals()[len(calls) + 2])
            if http_ok
            else httpx.Response(403, content=b"Access denied")
        )

    original = httpx.AsyncClient
    monkeypatch.setattr(
        ops.httpx,
        "AsyncClient",
        lambda **kwargs: original(transport=httpx.MockTransport(handle), **kwargs),
    )
    settings = EngineSettings(
        database_url="postgresql+asyncpg://test:test@127.0.0.1:55436/test"
    ).model_copy(update={"object_store_endpoint": "http://127.0.0.1:9000"})
    output = tmp_path / "collected"

    async def collect():
        await ops.collect_sec(
            settings,
            symbol="IWM",
            accession="0000123456-26-000001",
            contact="offline@example.invalid",
            mapping_file=mapping,
            mapping_archive=sec,
            output_dir=output,
        )

    if http_ok:
        await collect()
        from nexus.market.current_etf_identity_storage import load_etf_archives

        result = await load_etf_archives(
            symbol="IWM",
            fmp_archive=fm,
            sec_archive=output,
            mapping_file=mapping,
            now=datetime.now(UTC),
        )
        assert result.identity.exchange == "ARCX"
        assert len(calls) == 3
        assert len(json.loads((output / "receipts.json").read_text())) == 3
    else:
        with pytest.raises(RuntimeError, match="SEC request retry budget exhausted"):
            await collect()
        assert len(calls) == 1
        assert (output / "response-1.raw").read_bytes() == b"Access denied"
        assert not (output / "receipts.json").exists()
    binding["stored_at"] = "2026-01-04T00:00:00+00:00"
    binding_path.write_text(json.dumps(binding))
    # The reader compares actual metadata, not a self-reported JSON timestamp.
    storage._client = SimpleNamespace(
        list_object_versions=lambda **kwargs: dict(
            IsTruncated=False,
            Versions=[
                dict(
                    Key=binding["key"],
                    VersionId="synthetic-version",
                    LastModified=datetime(2026, 1, 3, tzinfo=UTC),
                )
            ],
        ),
        get_object=lambda **kwargs: dict(
            Body=io.BytesIO(mapping.read_bytes()),
            LastModified=datetime(2026, 1, 3, tzinfo=UTC),
            VersionId="synthetic-version",
        ),
    )
    with pytest.raises(ValueError, match="CURRENT_ETF_MAPPING_ORIGIN_INVALID"):
        await ops.verify_mapping_origin(storage, mapping_file=mapping, sec_archive=sec)


@pytest.mark.asyncio
async def test_share_class_legal_name_comes_from_exact_sec_binding_with_provider_conflict_visible():
    from nexus.market.current_etf_identity import validate_etf_originals

    values = originals(
        symbol="VTI",
        company_name="Synthetic Wrong Provider Label",
        index_name="Synthetic Total Stock Index Fund",
        document=share_class_document(),
    )
    result = await validate_etf_originals(symbol="VTI", contents=values, now=datetime.now(UTC))
    assert result.identity.legal_name == "Synthetic Total Stock Index Fund ETF Shares"
    assert result.identity.fmp_name == "Synthetic Wrong Provider Label"
    assert result.identity.fmp_name_matches_sec is False
    old = await validate_etf_originals(symbol="IWM", contents=originals(), now=datetime.now(UTC))
    assert "fmp_name" not in old.identity.model_dump()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name,replace",
    [
        ("Other Index Fund", None),
        (
            "Synthetic Total Stock Index Fund",
            (
                b"Synthetic Total Stock Index Fund (the Fund)",
                b"Synthetic Total Stock Index Fund II (the Fund)",
            ),
        ),
        (
            "Synthetic Total Stock Index Fund",
            (
                b"Synthetic Total Stock Index Fund (the Fund)",
                b"Synthetic Total Stock Index Fund Preferred (the Fund)",
            ),
        ),
        ("Synthetic Total Stock Index Fund", (b"ETF Shares (VTI)", b"ETF Shares (SPY)")),
        ("Synthetic Total Stock Index Fund", (b"U.S. stock market", b"U.S. bond market")),
    ],
)
async def test_share_class_mismatch_not_overridden_by_provider_label(name, replace):
    from nexus.market.current_etf_identity import validate_etf_originals

    document = share_class_document()
    if replace:
        document = document.replace(*replace)
    with pytest.raises(ValueError, match="CURRENT_ETF_IDENTITY_INVALID"):
        await validate_etf_originals(
            symbol="VTI",
            contents=originals(
                symbol="VTI",
                company_name="Synthetic Wrong Provider Label",
                index_name=name,
                document=document,
            ),
            now=datetime.now(UTC),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change", ["other_prospectus", "other_strategy_subject", "duplicate_objective"]
)
async def test_share_class_rejects_mixed_or_duplicate_fund_sections(change):
    from nexus.market.current_etf_identity import validate_etf_originals

    document = share_class_document()
    if change == "other_prospectus":
        document += document.replace(b"Synthetic Total Stock", b"Synthetic Other").replace(
            b"VTI", b"SPY"
        )
    elif change == "other_strategy_subject":
        document = document.replace(
            b"The Fund tracks an index", b"Synthetic Other Fund tracks an index"
        )
    else:
        document += (
            b"<p>Investment Objective Other Fund tracks stocks. Fees and Expenses Other.</p>"
        )
    with pytest.raises(ValueError, match="CURRENT_ETF_IDENTITY_INVALID"):
        await validate_etf_originals(
            symbol="VTI",
            contents=originals(
                symbol="VTI",
                company_name="Synthetic Wrong Provider Label",
                index_name="Synthetic Total Stock Index Fund",
                document=document,
            ),
            now=datetime.now(UTC),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("index_symbol", ["VTI.A", "VTI-A"])
async def test_share_class_index_requires_entire_ticker(index_symbol):
    from nexus.market.current_etf_identity import validate_etf_originals

    with pytest.raises(ValueError, match="CURRENT_ETF_IDENTITY_INVALID"):
        await validate_etf_originals(
            symbol="VTI",
            contents=originals(
                symbol="VTI",
                index_symbol=index_symbol,
                company_name="Synthetic Wrong Provider Label",
                index_name="Synthetic Total Stock Index Fund",
                document=share_class_document(),
            ),
            now=datetime.now(UTC),
        )


@pytest.mark.asyncio
async def test_nasdaq_reclassified_current_etf_preserves_blank_index_ticker():
    from helpers.current_etf_identity import nasdaq_originals
    from nexus.market.current_etf_identity import validate_etf_originals

    result = await validate_etf_originals(
        symbol="QQQ", contents=nasdaq_originals(), now=datetime.now(UTC)
    )
    assert result.identity.exchange == "XNAS"
    assert result.identity.filing_index_ticker is None
    assert result.identity.reclassification_date.isoformat() == "2025-12-19"
    assert result.identity.fmp_name_matches_sec is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "ticker_suffix",
        "class_name",
        "duplicate",
        "other_objective",
        "other_strategy",
        "future_reclassification",
        "bond",
        "wrong_exchange",
    ],
)
async def test_nasdaq_etf_rejects_mixed_identity_or_wrong_current_classification(change):
    from helpers.current_etf_identity import nasdaq_originals
    from nexus.market.current_etf_identity import validate_etf_originals

    kwargs = {}
    document = nasdaq_originals()[5]
    if change == "ticker_suffix":
        kwargs["index_ticker"] = "QQQ.A"
    elif change == "class_name":
        kwargs["class_name"] = "Other Trust, Series 1"
    elif change == "duplicate":
        document += document
    elif change == "other_objective":
        document = document.replace(b"(the Fund or Trust)", b"II (the Fund or Trust)")
    elif change == "other_strategy":
        document = document.replace(b"The Fund will invest", b"Other Fund will invest")
    elif change == "future_reclassification":
        document = document.replace(b"December 19, 2025", b"December 19, 2099")
    elif change == "bond":
        document = document.replace(b"common stocks and tracking stocks", b"bonds and loans")
    else:
        document = document.replace(b"The Nasdaq Stock Market LLC", b"NYSE Arca")
    with pytest.raises(ValueError, match="CURRENT_ETF_IDENTITY_INVALID"):
        await validate_etf_originals(
            symbol="QQQ",
            contents=nasdaq_originals(document=document, **kwargs),
            now=datetime.now(UTC),
        )


@pytest.mark.asyncio
async def test_nasdaq_index_ignores_unrelated_nested_layout_tables():
    import json
    from hashlib import sha256

    from helpers.current_etf_identity import nasdaq_originals
    from nexus.market.current_etf_identity import validate_etf_originals

    values = list(nasdaq_originals())
    values[4] = values[4].replace(
        b"<body>", b"<body><table><tr><td><table><tr><td>Layout</td></tr></table></td></tr></table>"
    )
    receipts = json.loads(values[6])
    receipts[1]["sha256"] = sha256(values[4]).hexdigest()
    values[6] = json.dumps(receipts).encode()
    result = await validate_etf_originals(
        symbol="QQQ", contents=tuple(values), now=datetime.now(UTC)
    )
    assert result.identity.filing_index_ticker is None


@pytest.mark.asyncio
@pytest.mark.parametrize("variant", ["omitted_header_end", "duplicate_scope", "unclosed_target"])
async def test_nasdaq_index_optional_header_end_keeps_target_strict(variant):
    import json
    from hashlib import sha256

    from helpers.current_etf_identity import nasdaq_originals
    from nexus.market.current_etf_identity import validate_etf_originals

    values = list(nasdaq_originals())
    if variant == "omitted_header_end":
        values[4] = values[4].replace(
            b"<table>",
            b"<table><tr><td>Status</td><td>Name</td><td>Ticker Symbol</td><tr></tr>",
        )
    elif variant == "duplicate_scope":
        values[4] = values[4].replace(b"</body>", b'<div id="seriesDiv"></div></body>')
    else:
        values[4] = values[4].replace(b"</tr>", b"", 1)
    receipts = json.loads(values[6])
    receipts[1]["sha256"] = sha256(values[4]).hexdigest()
    values[6] = json.dumps(receipts).encode()
    if variant == "omitted_header_end":
        result = await validate_etf_originals(
            symbol="QQQ", contents=tuple(values), now=datetime.now(UTC)
        )
        assert result.identity.filing_index_ticker is None
    else:
        with pytest.raises(ValueError, match="CURRENT_ETF_IDENTITY_INVALID"):
            await validate_etf_originals(
                symbol="QQQ", contents=tuple(values), now=datetime.now(UTC)
            )


def test_treasury_collection_preserves_non_equity_definition():
    from nexus.market.current_etf_identity import member_for

    member = member_for("TLT")
    assert member.asset_type == "non_equity_etf" and member.risk_bucket == "US_TREASURY"
    assert member_for("GLD").risk_bucket == "GOLD"


@pytest.mark.asyncio
@pytest.mark.parametrize("spacing", [None, "alias_spacing"])
async def test_treasury_explicit_source_identity_keeps_non_equity(spacing):
    from helpers.current_etf_identity import treasury_originals
    from nexus.market.current_etf_identity import validate_etf_originals

    proof = await validate_etf_originals(
        symbol="TLT",
        contents=treasury_originals(spacing),
        now=datetime.now(UTC),
        allow_treasury=True,
    )
    assert proof.identity.asset_type == "non_equity_etf" and proof.identity.exchange == "XNAS"
    with pytest.raises(ValueError, match="CURRENT_ETF_IDENTITY_INVALID"):
        await validate_etf_originals(
            symbol="TLT", contents=treasury_originals(spacing), now=datetime.now(UTC)
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["class", "ticker", "stock", "corporate", "threshold"])
async def test_treasury_rejects_wrong_subject_type_or_source_threshold(change):
    from helpers.current_etf_identity import treasury_originals
    from nexus.market.current_etf_identity import validate_etf_originals

    with pytest.raises(ValueError, match="CURRENT_ETF_IDENTITY_INVALID"):
        await validate_etf_originals(
            symbol="TLT",
            contents=treasury_originals(change),
            now=datetime.now(UTC),
            allow_treasury=True,
        )


def test_treasury_rejects_wrong_definition_risk_bucket(monkeypatch):
    from dataclasses import replace
    from types import SimpleNamespace

    import nexus.market.current_etf_identity as module

    wrong = replace(module.member_for("TLT"), risk_bucket="US_LARGE_CAP")
    monkeypatch.setattr(
        module, "load_us_liquid_v1_definition", lambda day: SimpleNamespace(members=[wrong])
    )
    with pytest.raises(ValueError):
        module.member_for("TLT")


@pytest.mark.asyncio
async def test_treasury_explicit_route_does_not_accept_equity_fund():
    from helpers.current_etf_identity import originals
    from nexus.market.current_etf_identity import validate_etf_originals

    with pytest.raises(ValueError, match="CURRENT_ETF_IDENTITY_INVALID"):
        await validate_etf_originals(
            symbol="IWM", contents=originals(), now=datetime.now(UTC), allow_treasury=True
        )
