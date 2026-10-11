import importlib
from hashlib import sha256

import pytest
from helpers.held_fund_submission import BODY, NOW, URL, submission


def parse(raw, **kwargs):
    module = importlib.import_module("nexus.market.held_fund_submission")
    return module.parse_single_class_submission(
        raw=raw, source_url=URL, symbol="TEST", now=NOW, **kwargs
    )


def test_missing_directory_ticker_binds_exact_single_class_component():
    result = parse(submission())
    assert (result.cik, result.series_id, result.class_id) == (
        "0000123456",
        "S000123456",
        "C000123456",
    )
    assert result.symbol == "TEST"
    assert result.class_name == "Synthetic Daily 2X ETF"
    assert result.component_filename == "fund.htm"
    assert result.component_raw == b"\n" + BODY + b"\n"
    assert result.component_sha256 == sha256(result.component_raw).hexdigest()
    assert result.binding_basis == "single_class_submission_and_unique_497k_cover"
    assert result.header_ticker_status == "absent"


@pytest.mark.parametrize(
    "old,new",
    [
        (b"<OWNER-CIK>0000123456", b"<OWNER-CIK>0000654321"),
        (b"<SERIES-ID>S000123456", b"<SERIES-ID>S000123456\n<SERIES-ID>S000999999"),
        (
            b"<CLASS-CONTRACT-ID>C000123456",
            b"<CLASS-CONTRACT-ID>C000123456\n<CLASS-CONTRACT-ID>C000999999",
        ),
        (b"<CLASS-CONTRACT>", b"<CLASS-CONTRACT><CLASS-CONTRACT>"),
        (
            b"CENTRAL INDEX KEY: 0000123456",
            b"CENTRAL INDEX KEY: 0000123456\nCENTRAL INDEX KEY: 0000123456",
        ),
        (b"<TYPE>GRAPHIC", b"<TYPE>497K"),
        (b"PUBLIC DOCUMENT COUNT: 2", b"PUBLIC DOCUMENT COUNT: 1"),
        (b"</SEC-DOCUMENT>", b""),
        (b"<SEQUENCE>1", b"<SEQUENCE>3"),
        (b"Trading Symbol: TEST", b"Trading Symbol: OTHER"),
        (b"Trading Symbol: TEST", b"Trading Symbol: TEST Trading Symbol: OTHER"),
        (b"<p>Synthetic Daily 2X ETF</p>", b"<p>Synthetic Daily ETF</p>"),
        (
            b"<CLASS-CONTRACT-ID>C000123456",
            b"<CLASS-CONTRACT-TICKER-SYMBOL>OTHER\n<CLASS-CONTRACT-ID>C000123456",
        ),
        (b"Listed on Cboe BZX Exchange, Inc.", b"Listed on a stock exchange"),
    ],
)
def test_ambiguous_inconsistent_or_truncated_chain_is_rejected(old, new):
    with pytest.raises(ValueError, match="HELD_FUND_SUBMISSION_INVALID"):
        parse(submission().replace(old, new))


def test_future_filing_rejected():
    with pytest.raises(ValueError, match="HELD_FUND_SUBMISSION_INVALID"):
        parse(submission().replace(b"20260422", b"20270422"))


def test_url_and_submission_accession_must_agree():
    with pytest.raises(ValueError, match="HELD_FUND_SUBMISSION_INVALID"):
        parse(submission().replace(b"0001234567-26-000001", b"0001234567-26-000002"))


@pytest.mark.parametrize(
    "old,new",
    [
        (b"</SEC-HEADER>", b"<CLASS-CONTRACT-ID>C000999999\n</SEC-HEADER>"),
        (b"</SEC-HEADER>", b"<OWNER-CIK>0000123456\n</SEC-HEADER>"),
        (b"20260422170815", b"20261011170815"),
        (b"</html>", b""),
        (b"<p>Trading Symbol: TEST</p>", b"<p>Underlying Trading Symbol: TEST</p>"),
    ],
)
def test_unbound_fields_future_acceptance_and_truncated_cover_rejected(old, new):
    with pytest.raises(ValueError, match="HELD_FUND_SUBMISSION_INVALID"):
        parse(submission().replace(old, new))
