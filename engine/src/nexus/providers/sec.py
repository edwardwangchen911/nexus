"""Offline SEC evidence retrieval and acceptance-time-safe fact normalization."""

import asyncio
import inspect
import json
from collections import deque
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from decimal import Decimal
from threading import Lock
from typing import Protocol
from uuid import UUID

from nexus.domain.evidence import EvidenceRef
from nexus.domain.time import require_utc
from nexus.research.financial_facts import FinancialFact


class SecTransport(Protocol):
    """Narrow, credential-free SEC HTTP seam."""

    async def get(self, path: str, headers: dict[str, str]) -> bytes: ...


class SecProviderError(RuntimeError):
    """A bounded SEC request could not yield a valid raw document."""


class SecNormalizationError(ValueError):
    """A raw SEC payload cannot safely form a point-in-time financial fact."""


_SEC_TAG_TO_CANONICAL_FACT = {
    ("us-gaap", "Revenues"): "revenue",
    ("us-gaap", "GrossProfit"): "gross_profit",
    ("us-gaap", "OperatingIncomeLoss"): "operating_income",
    ("us-gaap", "NetIncomeLoss"): "net_income",
    ("us-gaap", "NetCashProvidedByUsedInOperatingActivities"): "operating_cash_flow",
    ("us-gaap", "PaymentsToAcquirePropertyPlantAndEquipment"): "capital_expenditure",
    ("us-gaap", "Assets"): "total_assets",
    ("us-gaap", "Liabilities"): "total_liabilities",
    ("us-gaap", "StockholdersEquity"): "stockholders_equity",
    ("us-gaap", "EarningsPerShareDiluted"): "diluted_eps",
    ("us-gaap", "WeightedAverageNumberOfDilutedSharesOutstanding"): "diluted_shares",
}
_US_EXCHANGES = frozenset({"AMEX", "ARCA", "NASDAQ", "NYSE", "NYSEARCA"})


def canonical_fact_for_raw_tag(taxonomy: str, tag: str) -> str:
    """Map only a known unique SEC tag; callers retain the raw taxonomy and tag."""
    try:
        return _SEC_TAG_TO_CANONICAL_FACT[(taxonomy, tag)]
    except KeyError as error:
        raise SecNormalizationError("AMBIGUOUS_OR_UNKNOWN_TAG") from error


@dataclass(frozen=True)
class SecInstrumentIdentity:
    """The Task-7 identity attributes required to admit a CIK to SEC ingestion."""

    instrument_id: UUID
    cik: str
    symbol: str
    exchange: str
    currency: str

    def assert_us_sec_eligible(self) -> None:
        if (
            not self.cik.isdigit()
            or len(self.cik) > 10
            or self.symbol.upper() == "00656.HK"
            or self.symbol.upper().endswith(".HK")
            or self.exchange.upper() not in _US_EXCHANGES
            or self.currency.upper() != "USD"
        ):
            raise SecNormalizationError("US_INSTRUMENT_REQUIRED")


@dataclass(frozen=True)
class SecRawDocument:
    """Exact source bytes plus a canonical UTC receipt time for archival code."""

    raw_payload: bytes
    received_at: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.raw_payload, bytes):
            raise SecProviderError("SEC raw payload must be bytes")
        try:
            object.__setattr__(self, "received_at", require_utc(self.received_at))
        except ValueError as error:
            raise ValueError("SEC received_at must be canonical UTC") from error


class SecRateLimiter:
    """A lock-protected rolling-window reservation limiter.

    Reservations are made while holding a short thread lock, then any wait happens
    after releasing it.  This lets one module-global limiter safely cover clients
    created from different event loops in the same process.
    """

    def __init__(
        self,
        *,
        monotonic_clock: Callable[[], float],
        sleeper: Callable[[float], Awaitable[None] | None],
    ) -> None:
        self._clock = monotonic_clock
        self._sleeper = sleeper
        self._reservations: deque[float] = deque()
        self._lock = Lock()

    def _reserve(self, deadline: float | None) -> float:
        with self._lock:
            now = self._clock()
            while self._reservations and now - self._reservations[0] >= 1:
                self._reservations.popleft()
            reserved_at = now
            if len(self._reservations) >= 5:
                reserved_at = max(now, self._reservations[-5] + 1)
            if deadline is not None and reserved_at >= deadline:
                raise SecProviderError("SEC request deadline exceeded")
            self._reservations.append(reserved_at)
            return max(0.0, reserved_at - now)

    async def acquire(self, *, deadline: float | None = None) -> None:
        """Reserve an SEC slot, refusing waits that extend past ``deadline``."""
        wait_for = self._reserve(deadline)
        if wait_for == 0:
            return
        result = self._sleeper(wait_for)
        if inspect.isawaitable(result):
            await result
        if deadline is not None and self._clock() >= deadline:
            raise SecProviderError("SEC request deadline exceeded")


def _system_monotonic() -> float:
    from time import monotonic

    return monotonic()


async def _system_sleep(seconds: float) -> None:
    await asyncio.sleep(seconds)


_PROCESS_LIMITER = SecRateLimiter(monotonic_clock=_system_monotonic, sleeper=_system_sleep)


class SecCompanyFactsClient:
    """Retrieve raw company-facts/submissions evidence through an injected seam only."""

    def __init__(
        self,
        transport: SecTransport,
        *,
        contact: str,
        limiter: SecRateLimiter | None = None,
        now: Callable[[], datetime] | None = None,
        max_attempts: int = 3,
        deadline_seconds: float = 30,
        monotonic_clock: Callable[[], float] = _system_monotonic,
    ) -> None:
        if "@" not in contact or max_attempts < 1 or max_attempts > 3 or deadline_seconds <= 0:
            raise ValueError("SEC contact and retry budget must be configured")
        self._transport = transport
        self._contact = contact
        self._limiter = limiter or _PROCESS_LIMITER
        self._now = now or (lambda: datetime.now(UTC))
        self._max_attempts = max_attempts
        self._deadline_seconds = deadline_seconds
        self._monotonic = monotonic_clock

    @staticmethod
    def _validate_cik(cik: str) -> str:
        if not cik.isdigit() or len(cik) > 10:
            raise SecProviderError("SEC CIK must contain at most ten digits")
        return cik.zfill(10)

    async def _await_before_deadline(
        self, awaitable: Awaitable[bytes] | Awaitable[None], deadline: float
    ) -> bytes | None:
        remaining = deadline - self._monotonic()
        if remaining <= 0:
            raise SecProviderError("SEC request deadline exceeded")
        try:
            return await asyncio.wait_for(awaitable, timeout=remaining)
        except TimeoutError as error:
            raise SecProviderError("SEC request deadline exceeded") from error

    async def _fetch_path(self, path: str, deadline: float) -> SecRawDocument:
        headers = {"User-Agent": f"NEXUS {self._contact}", "Accept-Encoding": "gzip, deflate"}
        last_error: Exception | None = None
        for _ in range(self._max_attempts):
            try:
                await self._await_before_deadline(
                    self._limiter.acquire(deadline=deadline), deadline
                )
                payload = await self._await_before_deadline(
                    self._transport.get(path, headers), deadline
                )
                if not payload:
                    raise SecProviderError("SEC response payload is empty")
                return SecRawDocument(raw_payload=payload, received_at=require_utc(self._now()))
            except SecProviderError as error:
                if "deadline exceeded" in str(error):
                    raise
                last_error = error
            except Exception as error:
                last_error = error
        raise SecProviderError("SEC request retry budget exhausted") from last_error

    async def fetch_company_facts(self, cik: str) -> SecRawDocument:
        padded_cik = self._validate_cik(cik)
        deadline = self._monotonic() + self._deadline_seconds
        return await self._fetch_path(f"/api/xbrl/companyfacts/CIK{padded_cik}.json", deadline)

    async def fetch_submissions(self, cik: str) -> SecRawDocument:
        padded_cik = self._validate_cik(cik)
        deadline = self._monotonic() + self._deadline_seconds
        return await self._fetch_path(f"/submissions/CIK{padded_cik}.json", deadline)

    async def fetch_company_facts_and_submissions(
        self, cik: str
    ) -> tuple[SecRawDocument, SecRawDocument]:
        """Fetch the two authoritative payloads under one bounded request budget."""
        padded_cik = self._validate_cik(cik)
        deadline = self._monotonic() + self._deadline_seconds
        company_facts = await self._fetch_path(
            f"/api/xbrl/companyfacts/CIK{padded_cik}.json", deadline
        )
        submissions = await self._fetch_path(f"/submissions/CIK{padded_cik}.json", deadline)
        return company_facts, submissions

    async def bootstrap_company_facts(self, ciks: Iterable[str]) -> tuple[SecRawDocument, ...]:
        """Explicit bulk-bootstrap seam; callers provide bounded CIK batches."""
        return tuple([await self.fetch_company_facts(cik) for cik in ciks])


def normalize_company_facts(
    company_facts_payload: bytes,
    submissions_payload: bytes,
    *,
    instrument: SecInstrumentIdentity,
    evidence_by_accession: Mapping[str, EvidenceRef],
) -> tuple[FinancialFact, ...]:
    """Join Company Facts with submissions ``acceptanceDateTime`` by accession.

    SEC Company Facts records have a filed date but no authoritative acceptance
    timestamp.  The submissions payload is therefore mandatory; this function never
    derives acceptance from filed or economic period dates.
    """
    instrument.assert_us_sec_eligible()
    company_facts = _load_object(company_facts_payload, "INVALID_COMPANY_FACTS_PAYLOAD")
    source_cik = _required_string(company_facts, "cik", "CIK_MISMATCH")
    if source_cik.zfill(10) != instrument.cik.zfill(10):
        raise SecNormalizationError("CIK_MISMATCH")
    accepted_by_accession = _accepted_submission_metadata(submissions_payload)
    facts_root = company_facts.get("facts")
    if not isinstance(facts_root, Mapping):
        raise SecNormalizationError("MISSING_COMPANY_FACTS")

    normalized: list[FinancialFact] = []
    for taxonomy, tags in facts_root.items():
        if not isinstance(taxonomy, str) or not isinstance(tags, Mapping):
            raise SecNormalizationError("INVALID_TAXONOMY")
        for raw_tag, tagged_fact in tags.items():
            if not isinstance(raw_tag, str):
                raise SecNormalizationError("INVALID_TAG")
            try:
                canonical_fact = canonical_fact_for_raw_tag(taxonomy, raw_tag)
            except SecNormalizationError:
                continue
            if not isinstance(tagged_fact, Mapping):
                raise SecNormalizationError("INVALID_TAGGED_FACT")
            units = tagged_fact.get("units")
            if not isinstance(units, Mapping) or len(units) != 1:
                raise SecNormalizationError("AMBIGUOUS_UNIT")
            unit, entries = next(iter(units.items()))
            if not isinstance(unit, str) or not isinstance(entries, list):
                raise SecNormalizationError("AMBIGUOUS_UNIT")
            for entry in entries:
                if not isinstance(entry, Mapping):
                    raise SecNormalizationError("INVALID_FACT_ENTRY")
                normalized.append(
                    _normalize_company_fact_entry(
                        entry,
                        canonical_fact=canonical_fact,
                        taxonomy=taxonomy,
                        raw_tag=raw_tag,
                        unit=unit,
                        instrument_id=instrument.instrument_id,
                        accepted_by_accession=accepted_by_accession,
                        evidence_by_accession=evidence_by_accession,
                    )
                )
    if not normalized:
        raise SecNormalizationError("NO_CANONICAL_FACTS")
    return tuple(normalized)


def _normalize_company_fact_entry(
    entry: Mapping[str, object],
    *,
    canonical_fact: str,
    taxonomy: str,
    raw_tag: str,
    unit: str,
    instrument_id: UUID,
    accepted_by_accession: Mapping[str, tuple[datetime, str]],
    evidence_by_accession: Mapping[str, EvidenceRef],
) -> FinancialFact:
    accession = _required_string(entry, "accn", "MISSING_ACCESSION")
    metadata = accepted_by_accession.get(accession)
    if metadata is None:
        raise SecNormalizationError("MISSING_ACCEPTANCE_TIME")
    accepted_at, submission_form = metadata
    evidence_ref = evidence_by_accession.get(accession)
    if evidence_ref is None or evidence_ref.source != "sec" or evidence_ref.object_id != accession:
        raise SecNormalizationError("EVIDENCE_OWNER_MISMATCH")
    form = _required_string(entry, "form", "MISSING_FORM")
    if form != submission_form:
        raise SecNormalizationError("SUBMISSION_FORM_MISMATCH")
    filed_at = _filing_date_to_utc(_required_string(entry, "filed", "MISSING_FILED_DATE"))
    period_end = _parse_date(_required_string(entry, "end", "MISSING_PERIOD_END"))
    raw_start = entry.get("start")
    period_start = _parse_date(raw_start) if isinstance(raw_start, str) else None
    revision = _required_string(entry, "revision", "MISSING_REVISION")
    value = entry.get("val")
    if isinstance(value, float) or not isinstance(value, Decimal | int | str):
        raise SecNormalizationError("VALUE_MUST_BE_DECIMAL")
    try:
        decimal_value = Decimal(str(value))
    except Exception as error:
        raise SecNormalizationError("VALUE_MUST_BE_DECIMAL") from error
    if not decimal_value.is_finite():
        raise SecNormalizationError("VALUE_MUST_BE_FINITE")
    fiscal_year = entry.get("fy")
    if fiscal_year is not None and (
        not isinstance(fiscal_year, int) or isinstance(fiscal_year, bool)
    ):
        raise SecNormalizationError("INVALID_FISCAL_YEAR")
    fiscal_period = entry.get("fp")
    if fiscal_period is not None and not isinstance(fiscal_period, str):
        raise SecNormalizationError("INVALID_FISCAL_PERIOD")
    frame = entry.get("frame")
    if frame is not None and not isinstance(frame, str):
        raise SecNormalizationError("INVALID_FRAME")
    return FinancialFact(
        instrument_id=instrument_id,
        canonical_fact=canonical_fact,
        value=decimal_value,
        unit=unit,
        period_kind="duration" if period_start is not None else "instant",
        period_start=period_start,
        period_end=period_end,
        fiscal_year=fiscal_year,
        fiscal_period=fiscal_period,
        form=form,
        filed_at=filed_at,
        accepted_at=accepted_at,
        accession_number=accession,
        raw_taxonomy=taxonomy,
        raw_tag=raw_tag,
        frame=frame,
        revision=revision,
        evidence_ref=evidence_ref,
    )


def _accepted_submission_metadata(payload: bytes) -> dict[str, tuple[datetime, str]]:
    submissions = _load_object(payload, "INVALID_SUBMISSIONS_PAYLOAD")
    filings = submissions.get("filings")
    recent = filings.get("recent") if isinstance(filings, Mapping) else None
    if not isinstance(recent, Mapping):
        raise SecNormalizationError("MISSING_ACCEPTANCE_TIME")
    accessions = recent.get("accessionNumber")
    accepted_times = recent.get("acceptanceDateTime")
    forms = recent.get("form")
    if (
        not isinstance(accessions, list)
        or not isinstance(accepted_times, list)
        or not isinstance(forms, list)
    ):
        raise SecNormalizationError("MISSING_ACCEPTANCE_TIME")
    if len(accessions) != len(accepted_times) or len(accessions) != len(forms):
        raise SecNormalizationError("MISSING_ACCEPTANCE_TIME")
    metadata: dict[str, tuple[datetime, str]] = {}
    for accession, accepted, form in zip(accessions, accepted_times, forms, strict=True):
        if (
            not isinstance(accession, str)
            or not isinstance(accepted, str)
            or not isinstance(form, str)
        ):
            raise SecNormalizationError("MISSING_ACCEPTANCE_TIME")
        try:
            accepted_at = require_utc(datetime.fromisoformat(accepted.replace("Z", "+00:00")))
        except ValueError as error:
            raise SecNormalizationError("MISSING_ACCEPTANCE_TIME") from error
        if accession in metadata:
            raise SecNormalizationError("AMBIGUOUS_ACCEPTANCE_TIME")
        metadata[accession] = (accepted_at, form)
    return metadata


def _load_object(payload: bytes, error_code: str) -> Mapping[str, object]:
    if not isinstance(payload, bytes):
        raise SecNormalizationError(error_code)
    try:
        decoded = json.loads(payload, parse_float=Decimal)
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        raise SecNormalizationError(error_code) from error
    if not isinstance(decoded, Mapping):
        raise SecNormalizationError(error_code)
    return decoded


def _required_string(payload: Mapping[str, object], key: str, error_code: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise SecNormalizationError(error_code)
    return value


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise SecNormalizationError("INVALID_PERIOD_DATE") from error


def _filing_date_to_utc(value: str) -> datetime:
    return datetime.combine(_parse_date(value), time.min, UTC)
