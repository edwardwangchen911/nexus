import asyncio
import json
import socket
import warnings
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic
from uuid import UUID

import anyio
import nexus.providers.sec as sec_provider
import pytest
from nexus.domain.evidence import EvidenceRef
from nexus.providers.sec import (
    SecCompanyFactsClient,
    SecInstrumentIdentity,
    SecNormalizationError,
    SecProviderError,
    SecRateLimiter,
    SecRawDocument,
    normalize_company_facts,
)

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "sec"
INSTRUMENT_ID = UUID("0198d500-1000-7000-8000-000000000014")
ACCESSION = "0000320193-26-000001"
ARCHIVED_ACCESSION = "0000320193-25-000123"


class FixtureSecTransport:
    def __init__(self) -> None:
        self.paths: list[str] = []
        self.headers: list[dict[str, str]] = []

    async def get(self, path: str, headers: dict[str, str]) -> bytes:
        self.paths.append(path)
        self.headers.append(headers)
        if "companyfacts" in path:
            return (FIXTURES / "companyfacts.json").read_bytes()
        if path == "/submissions/CIK0000320193.json":
            return (FIXTURES / "submissions.json").read_bytes()
        if path == "/submissions/CIK0000320193-submissions-001.json":
            return (FIXTURES / "submissions-001.json").read_bytes()
        raise AssertionError(f"unexpected SEC path: {path}")


class BlockingSecTransport:
    async def get(self, path: str, headers: dict[str, str]) -> bytes:
        del path, headers
        await asyncio.Event().wait()
        raise AssertionError("blocked SEC transport must be cancelled at the deadline")


def _identity(
    *, symbol: str = "AAPL", exchange: str = "NASDAQ", currency: str = "USD"
) -> SecInstrumentIdentity:
    return SecInstrumentIdentity(
        instrument_id=INSTRUMENT_ID,
        cik="320193",
        symbol=symbol,
        exchange=exchange,
        currency=currency,
    )


def _evidence(accession: str = ACCESSION) -> EvidenceRef:
    digest = "a" * 64
    return EvidenceRef(
        source="sec",
        object_id=accession,
        available_at=datetime(2026, 8, 20, 20, tzinfo=UTC),
        location=f"memory://sha256/{digest}",
        content_hash=digest,
    )


@pytest.mark.asyncio
async def test_sec_client_uses_configured_contact_and_committed_fixture_payloads() -> None:
    transport = FixtureSecTransport()
    client = SecCompanyFactsClient(
        transport,
        contact="research@example.test",
        limiter=SecRateLimiter(monotonic_clock=lambda: 0.0, sleeper=lambda _: None),
        now=lambda: datetime(2026, 8, 24, tzinfo=UTC),
    )

    company_facts, submissions = await client.fetch_company_facts_and_submissions("320193")

    assert transport.paths == [
        "/api/xbrl/companyfacts/CIK0000320193.json",
        "/submissions/CIK0000320193.json",
    ]
    assert transport.headers == [
        {"User-Agent": "NEXUS research@example.test", "Accept-Encoding": "gzip, deflate"},
        {"User-Agent": "NEXUS research@example.test", "Accept-Encoding": "gzip, deflate"},
    ]
    assert company_facts.raw_payload == (FIXTURES / "companyfacts.json").read_bytes()
    assert submissions.raw_payload == (FIXTURES / "submissions.json").read_bytes()


@pytest.mark.asyncio
async def test_sec_client_fetches_archived_submission_metadata_under_the_same_budget() -> None:
    transport = FixtureSecTransport()
    client = SecCompanyFactsClient(
        transport,
        contact="research@example.test",
        limiter=SecRateLimiter(monotonic_clock=lambda: 0.0, sleeper=lambda _: None),
        now=lambda: datetime(2026, 8, 24, tzinfo=UTC),
    )

    company_facts, submission_history = await client.fetch_company_facts_and_submission_history(
        "320193"
    )

    assert company_facts.raw_payload == (FIXTURES / "companyfacts.json").read_bytes()
    assert [document.raw_payload for document in submission_history] == [
        (FIXTURES / "submissions.json").read_bytes(),
        (FIXTURES / "submissions-001.json").read_bytes(),
    ]
    assert transport.paths == [
        "/api/xbrl/companyfacts/CIK0000320193.json",
        "/submissions/CIK0000320193.json",
        "/submissions/CIK0000320193-submissions-001.json",
    ]


@pytest.mark.asyncio
async def test_sec_bulk_bootstrap_rejects_an_unbounded_cik_batch() -> None:
    client = SecCompanyFactsClient(
        FixtureSecTransport(),
        contact="research@example.test",
        limiter=SecRateLimiter(monotonic_clock=lambda: 0.0, sleeper=lambda _: None),
    )

    with pytest.raises(SecProviderError, match="bootstrap batch exceeds limit"):
        await client.bootstrap_company_facts(["320193"] * 101)


def test_sec_normalization_joins_committed_submissions_acceptance_metadata_for_all_tags() -> None:
    facts = normalize_company_facts(
        (FIXTURES / "companyfacts.json").read_bytes(),
        (
            (FIXTURES / "submissions.json").read_bytes(),
            (FIXTURES / "submissions-001.json").read_bytes(),
        ),
        instrument=_identity(),
        evidence_by_accession={
            ACCESSION: _evidence(),
            ARCHIVED_ACCESSION: _evidence(ARCHIVED_ACCESSION),
        },
    )

    assert {fact.canonical_fact for fact in facts} == {
        "revenue",
        "gross_profit",
        "operating_income",
        "net_income",
        "operating_cash_flow",
        "capital_expenditure",
        "total_assets",
        "total_liabilities",
        "stockholders_equity",
        "diluted_eps",
        "diluted_shares",
    }
    current_facts = [fact for fact in facts if fact.accession_number == ACCESSION]
    assert len(current_facts) == 11
    assert all(fact.accepted_at == datetime(2026, 8, 20, 20, tzinfo=UTC) for fact in current_facts)
    assert all(
        fact.revision == "0000320193-26-000001@2026-08-20T20:00:00+00:00"
        for fact in current_facts
    )
    assert {fact.raw_tag for fact in facts} == {
        "Revenues",
        "GrossProfit",
        "OperatingIncomeLoss",
        "NetIncomeLoss",
        "NetCashProvidedByUsedInOperatingActivities",
        "PaymentsToAcquirePropertyPlantAndEquipment",
        "Assets",
        "Liabilities",
        "StockholdersEquity",
        "EarningsPerShareDiluted",
        "WeightedAverageNumberOfDilutedSharesOutstanding",
    }


def test_sec_normalization_uses_archived_submission_acceptance_metadata_without_raw_revision(
) -> None:
    facts = normalize_company_facts(
        (FIXTURES / "companyfacts.json").read_bytes(),
        (
            (FIXTURES / "submissions.json").read_bytes(),
            (FIXTURES / "submissions-001.json").read_bytes(),
        ),
        instrument=_identity(),
        evidence_by_accession={
            ACCESSION: _evidence(ACCESSION),
            ARCHIVED_ACCESSION: _evidence(ARCHIVED_ACCESSION),
        },
    )

    archived = [fact for fact in facts if fact.accession_number == ARCHIVED_ACCESSION]
    assert len(archived) == 1
    assert archived[0].accepted_at == datetime(2025, 8, 20, 20, tzinfo=UTC)
    assert archived[0].revision == "0000320193-25-000123@2025-08-20T20:00:00+00:00"


def test_contract_revenue_is_normalized_without_losing_its_source_basis() -> None:
    payload = json.loads((FIXTURES / "companyfacts.json").read_bytes())
    tags = payload["facts"]["us-gaap"]
    tag = "RevenueFromContractWithCustomerExcludingAssessedTax"
    tags[tag] = tags.pop("Revenues")
    raw = json.dumps(payload).encode()
    facts = normalize_company_facts(
        raw,
        (
            (FIXTURES / "submissions.json").read_bytes(),
            (FIXTURES / "submissions-001.json").read_bytes(),
        ),
        instrument=_identity(),
        evidence_by_accession={
            ACCESSION: _evidence(),
            ARCHIVED_ACCESSION: _evidence(ARCHIVED_ACCESSION),
        },
    )
    revenue = [fact for fact in facts if fact.canonical_fact == "revenue"]
    assert len(revenue) == 2
    assert [str(fact.value) for fact in revenue] == ["100", "90"]
    assert {fact.raw_tag for fact in revenue} == {tag}
    assert all(fact.raw_taxonomy == "us-gaap" for fact in revenue)
    assert all(fact.period_kind == "duration" and fact.unit == "USD" for fact in revenue)
    original = normalize_company_facts(
        (FIXTURES / "companyfacts.json").read_bytes(),
        (
            (FIXTURES / "submissions.json").read_bytes(),
            (FIXTURES / "submissions-001.json").read_bytes(),
        ),
        instrument=_identity(),
        evidence_by_accession={
            ACCESSION: _evidence(),
            ARCHIVED_ACCESSION: _evidence(ARCHIVED_ACCESSION),
        },
    )
    # Only the reported tag differs; values, periods, revisions and provenance survive.
    assert {
        (fact.canonical_fact, fact.accession_number): fact.model_dump(exclude={"raw_tag"})
        for fact in facts
    } == {
        (fact.canonical_fact, fact.accession_number): fact.model_dump(exclude={"raw_tag"})
        for fact in original
    }
    assert json.dumps(payload).encode() == raw


@pytest.mark.parametrize(
    ("taxonomy", "tag"),
    [
        ("custom", "RevenueFromContractWithCustomerExcludingAssessedTax"),
        ("us-gaap", "RevenueFromContractWithCustomerIncludingAssessedTax"),
        ("us-gaap", "RevenueFromContractWithCustomerExcludingAssessedTaxExtra"),
    ],
)
def test_contract_revenue_mapping_does_not_guess_other_taxonomies_or_bases(
    taxonomy: str,
    tag: str,
) -> None:
    with pytest.raises(SecNormalizationError, match="AMBIGUOUS_OR_UNKNOWN_TAG"):
        sec_provider.canonical_fact_for_raw_tag(taxonomy, tag)


@pytest.mark.parametrize("contract_value", [100, 80])
def test_distinct_revenue_bases_for_the_same_period_are_not_arbitrarily_selected(
    contract_value: int,
) -> None:
    payload = json.loads((FIXTURES / "companyfacts.json").read_bytes())
    tags = payload["facts"]["us-gaap"]
    contract_entry = dict(tags["Revenues"]["units"]["USD"][0], val=contract_value)
    tags["RevenueFromContractWithCustomerExcludingAssessedTax"] = {
        "units": {"USD": [contract_entry]},
    }
    with pytest.raises(SecNormalizationError, match="AMBIGUOUS_REVENUE_BASIS"):
        normalize_company_facts(
            json.dumps(payload).encode(),
            (
                (FIXTURES / "submissions.json").read_bytes(),
                (FIXTURES / "submissions-001.json").read_bytes(),
            ),
            instrument=_identity(),
            evidence_by_accession={
                ACCESSION: _evidence(),
                ARCHIVED_ACCESSION: _evidence(ARCHIVED_ACCESSION),
            },
        )


@pytest.mark.parametrize(
    ("symbol", "exchange", "currency"),
    [("00656.HK", "HKEX", "HKD"), ("AAPL", "HKEX", "HKD")],
)
def test_sec_normalization_rejects_non_us_task7_instruments(
    symbol: str, exchange: str, currency: str
) -> None:
    with pytest.raises(SecNormalizationError, match="US_INSTRUMENT_REQUIRED"):
        normalize_company_facts(
            (FIXTURES / "companyfacts.json").read_bytes(),
            (FIXTURES / "submissions.json").read_bytes(),
            instrument=_identity(symbol=symbol, exchange=exchange, currency=currency),
            evidence_by_accession={ACCESSION: _evidence()},
        )


def test_sec_normalization_rejects_missing_authoritative_acceptance_metadata() -> None:
    with pytest.raises(SecNormalizationError, match="MISSING_ACCEPTANCE_TIME"):
        normalize_company_facts(
            (FIXTURES / "companyfacts.json").read_bytes(),
            b'{"filings":{"recent":{"accessionNumber":["0000320193-26-000001"]}}}',
            instrument=_identity(),
            evidence_by_accession={ACCESSION: _evidence()},
        )


def test_sec_normalization_rejects_noncanonical_accession_number() -> None:
    submissions = json.loads((FIXTURES / "submissions.json").read_bytes())
    submissions["filings"]["recent"]["accessionNumber"][0] = f"{ACCESSION}?query"

    with pytest.raises(SecNormalizationError, match="INVALID_ACCESSION_NUMBER"):
        normalize_company_facts(
            (FIXTURES / "companyfacts.json").read_bytes(),
            json.dumps(submissions).encode(),
            instrument=_identity(),
            evidence_by_accession={ACCESSION: _evidence()},
        )


def test_sec_raw_document_rejects_naive_receipt_time() -> None:
    with pytest.raises(ValueError, match="UTC"):
        SecRawDocument(raw_payload=b"fixture", received_at=datetime(2026, 8, 24))


def test_sec_raw_document_canonicalizes_an_aware_receipt_time_to_utc() -> None:
    document = SecRawDocument(
        raw_payload=b"fixture",
        received_at=datetime.fromisoformat("2026-08-24T08:00:00+08:00"),
    )

    assert document.received_at == datetime(2026, 8, 24, tzinfo=UTC)


@pytest.mark.asyncio
async def test_provider_unit_network_entry_points_are_denied() -> None:
    with pytest.raises(AssertionError, match="forbid network"):
        socket.create_connection(("sec.example", 443))
    network_socket = socket.socket()
    try:
        with pytest.raises(AssertionError, match="forbid network"):
            network_socket.connect(("sec.example", 443))
        with pytest.raises(AssertionError, match="forbid network"):
            network_socket.connect_ex(("sec.example", 443))
    finally:
        network_socket.close()
    with pytest.raises(AssertionError, match="forbid network"):
        await asyncio.get_running_loop().create_connection(lambda: object(), "sec.example", 443)
    with pytest.raises(AssertionError, match="forbid network"):
        await anyio.connect_tcp("sec.example", 443)


@pytest.mark.asyncio
async def test_default_process_limiter_makes_concurrent_reservations_across_clients(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    waits: list[float] = []

    async def sleeper(duration: float) -> None:
        waits.append(duration)

    limiter = SecRateLimiter(monotonic_clock=lambda: 0.0, sleeper=sleeper)
    monkeypatch.setattr(sec_provider, "_PROCESS_LIMITER", limiter)
    def fetch_from_an_independent_event_loop() -> None:
        async def fetch() -> None:
            await SecCompanyFactsClient(
                FixtureSecTransport(),
                contact="research@example.test",
                deadline_seconds=5,
                monotonic_clock=lambda: 0.0,
            ).fetch_company_facts("320193")

        asyncio.run(fetch())

    await asyncio.gather(
        *(asyncio.to_thread(fetch_from_an_independent_event_loop) for _ in range(10))
    )

    assert len(waits) == 5
    assert set(waits) == {1.0}


@pytest.mark.asyncio
async def test_sec_deadline_cancels_a_blocked_transport_after_one_absolute_budget() -> None:
    client = SecCompanyFactsClient(
        BlockingSecTransport(),
        contact="research@example.test",
        limiter=SecRateLimiter(monotonic_clock=monotonic, sleeper=lambda _: None),
        monotonic_clock=monotonic,
        deadline_seconds=0.01,
    )

    with pytest.raises(SecProviderError, match="deadline exceeded"):
        await client.fetch_company_facts("320193")


@pytest.mark.asyncio
async def test_sec_deadline_includes_a_reserved_limiter_wait() -> None:
    blocked_wait = asyncio.Event()
    limiter = SecRateLimiter(monotonic_clock=monotonic, sleeper=lambda _: blocked_wait.wait())
    for _ in range(5):
        await limiter.acquire()
    client = SecCompanyFactsClient(
        FixtureSecTransport(),
        contact="research@example.test",
        limiter=limiter,
        monotonic_clock=monotonic,
        deadline_seconds=0.01,
    )

    with pytest.raises(SecProviderError, match="deadline exceeded"):
        await client.fetch_company_facts("320193")


@pytest.mark.asyncio
async def test_sec_expired_deadline_does_not_create_an_unawaited_coroutine() -> None:
    client = SecCompanyFactsClient(
        FixtureSecTransport(),
        contact="research@example.test",
        monotonic_clock=lambda: 1.0,
    )

    async def unexpected_awaitable() -> bytes:
        raise AssertionError("an expired deadline must not invoke the awaitable factory")

    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        with pytest.raises(SecProviderError, match="deadline exceeded"):
            await client._await_before_deadline(unexpected_awaitable, 1.0)


@pytest.mark.asyncio
async def test_sec_deadline_helper_receives_factories_before_creating_awaitables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = SecCompanyFactsClient(
        FixtureSecTransport(),
        contact="research@example.test",
        limiter=SecRateLimiter(monotonic_clock=lambda: 0.0, sleeper=lambda _: None),
        monotonic_clock=lambda: 0.0,
    )

    async def assert_factory(awaitable_factory: object, deadline: float) -> bytes | None:
        assert callable(awaitable_factory)
        return await awaitable_factory()

    monkeypatch.setattr(client, "_await_before_deadline", assert_factory)

    await client.fetch_company_facts("320193")
