"""An archived profile is current background only, never financial or taxonomy proof."""

import importlib
import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from uuid import UUID

import httpx
import pytest
from pydantic import SecretStr

INSTRUMENT = UUID(int=17)


@pytest.mark.asyncio
async def test_collect_then_read_archive_preserves_identity_unknowns_and_exact_bytes(tmp_path):
    module = importlib.import_module("nexus.ops.fmp_profile_local")
    archive = importlib.import_module("nexus.research.fmp_profile_archive")
    raw = b'[{"symbol":"AAPL","companyName":"Synthetic","industry":null,"price":1.25}]'
    calls = []

    def respond(request):
        calls.append(request)
        return httpx.Response(200, content=raw)

    directory = tmp_path / "profile"
    before = datetime.now(UTC)
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        result = await module.collect_profile(
            symbol="AAPL",
            key=SecretStr("synthetic-secret"),
            output_dir=directory,
            client=client,
        )
    assert result["status"] == "collected" and len(calls) == 1
    assert calls[0].url.path == "/stable/profile"
    assert dict(calls[0].url.params) == {"symbol": "AAPL", "apikey": "synthetic-secret"}
    assert (directory / "profile.json").read_bytes() == raw
    receipt = (directory / "receipt.json").read_bytes()
    assert b"synthetic-secret" not in receipt and b"apikey" not in receipt
    doc = archive.load_profile_archive(directory, instrument_id=INSTRUMENT, symbol="AAPL").document
    assert before <= doc.available_at <= datetime.now(UTC)
    assert doc.available_at == doc.received_at
    assert doc.company_name == "Synthetic" and doc.industry is None and doc.sector is None
    assert doc.source_published_at is None and doc.valid_until is None
    assert doc.sector_taxonomy is None and doc.execution_locked
    assert not any(
        (
            doc.sector_risk_eligible,
            doc.financial_fact_eligible,
            doc.price_authority_eligible,
            doc.historical_backtest_eligible,
        )
    )
    assert doc.raw_content_hash == sha256(raw).hexdigest()
    assert doc.receipt_content_hash == sha256(receipt).hexdigest()
    assert "price" not in doc.model_dump()
    assert doc.source_url == "https://financialmodelingprep.com/stable/profile?symbol=AAPL"
    assert doc.provider_object_id == f"profile:{INSTRUMENT}:AAPL"
    with pytest.raises(ValueError, match="FMP_PROFILE_ARCHIVE_INVALID"):
        archive.load_profile_archive(directory, instrument_id=INSTRUMENT, symbol="MSFT")
    with pytest.raises(ValueError, match="FMP_PROFILE_ARCHIVE_INVALID"):
        archive.load_profile_archive(directory, instrument_id=INSTRUMENT, symbol="AAPL", now=before)
    for path in directory.iterdir():
        assert path.stat().st_mode & 0o077 == 0


@pytest.mark.asyncio
async def test_malformed_success_response_is_archived_before_parse_failure(tmp_path):
    module = importlib.import_module("nexus.ops.fmp_profile_local")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, content=b"invalid JSON"),
        )
    ) as client:
        directory = tmp_path / "bad"
        with pytest.raises(ValueError):
            await module.collect_profile(
                symbol="AAPL",
                key=SecretStr("synthetic-secret"),
                output_dir=directory,
                client=client,
            )
    assert (directory / "profile.json").read_bytes() == b"invalid JSON"
    assert (directory / "receipt.json").exists()


def test_canonical_cannot_claim_source_clocks_or_promote_eligibility():
    model = importlib.import_module("nexus.research.fmp_profile").FmpCompanyProfile
    now = datetime.now(UTC)
    base = dict(
        instrument_id=INSTRUMENT,
        symbol="AAPL",
        received_at=now,
        available_at=now,
        source_url="https://financialmodelingprep.com/stable/profile?symbol=AAPL",
        raw_content_hash="a" * 64,
        raw_byte_length=100,
        receipt_content_hash="b" * 64,
        receipt_byte_length=100,
    )
    for change in (
        {
            "source_url": "https://financialmodelingprep.com/stable/profile?symbol=AAPL&apikey=canary"
        },
        {"source_published_at": now},
        {"sector_taxonomy": "NEXUS_SECTOR_V1"},
        {"sector_risk_eligible": True},
        {"financial_fact_eligible": True},
        {"price_authority_eligible": True},
        {"historical_backtest_eligible": True},
        {"available_at": now - timedelta(seconds=1)},
        {"company_name": 123},
    ):
        with pytest.raises(ValueError):
            model.model_validate({**base, **change})
    doc = model.model_validate(base)
    assert json.loads(doc.canonical_bytes())["industry"] is None
