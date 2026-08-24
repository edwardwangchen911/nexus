"""Dependency-injected SEC transport with process-global rate control."""

import inspect
from collections import deque
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol


class SecTransport(Protocol):
    """Narrow, credential-free SEC HTTP seam."""

    async def get(self, path: str, headers: dict[str, str]) -> bytes: ...


class SecProviderError(RuntimeError):
    """A bounded SEC request could not yield a valid raw document."""


class SecNormalizationError(ValueError):
    """A raw SEC taxonomy tag cannot safely map to one canonical fact."""


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


def canonical_fact_for_raw_tag(taxonomy: str, tag: str) -> str:
    """Map only a known unique SEC tag; callers must preserve the raw fields too."""
    try:
        return _SEC_TAG_TO_CANONICAL_FACT[(taxonomy, tag)]
    except KeyError as error:
        raise SecNormalizationError("AMBIGUOUS_OR_UNKNOWN_TAG") from error


@dataclass(frozen=True)
class SecRawDocument:
    """Exact source bytes plus the receipt time used by archival code."""

    raw_payload: bytes
    received_at: datetime


class SecRateLimiter:
    """At most five acquisitions per rolling second for the process-level client."""

    def __init__(
        self,
        *,
        monotonic_clock: Callable[[], float],
        sleeper: Callable[[float], Awaitable[None] | None],
    ) -> None:
        self._clock = monotonic_clock
        self._sleeper = sleeper
        self._requests: deque[float] = deque()

    async def acquire(self) -> None:
        while True:
            now = self._clock()
            while self._requests and now - self._requests[0] >= 1:
                self._requests.popleft()
            if len(self._requests) < 5:
                self._requests.append(now)
                return
            wait_for = max(0.0, 1 - (now - self._requests[0]))
            result = self._sleeper(wait_for)
            if inspect.isawaitable(result):
                await result


def _system_monotonic() -> float:
    from time import monotonic

    return monotonic()


async def _system_sleep(seconds: float) -> None:
    import asyncio

    await asyncio.sleep(seconds)


_PROCESS_LIMITER = SecRateLimiter(monotonic_clock=_system_monotonic, sleeper=_system_sleep)


class SecCompanyFactsClient:
    """Retrieve raw company facts through an injected transport only."""

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

    async def fetch_company_facts(self, cik: str) -> SecRawDocument:
        if not cik.isdigit() or len(cik) > 10:
            raise SecProviderError("SEC CIK must contain at most ten digits")
        path = f"/api/xbrl/companyfacts/CIK{cik.zfill(10)}.json"
        headers = {"User-Agent": f"NEXUS {self._contact}", "Accept-Encoding": "gzip, deflate"}
        last_error: Exception | None = None
        deadline = self._monotonic() + self._deadline_seconds
        for _ in range(self._max_attempts):
            if self._monotonic() >= deadline:
                raise SecProviderError("SEC request deadline exceeded") from last_error
            await self._limiter.acquire()
            if self._monotonic() >= deadline:
                raise SecProviderError("SEC request deadline exceeded") from last_error
            try:
                payload = await self._transport.get(path, headers)
                if not payload:
                    raise SecProviderError("SEC response payload is empty")
                return SecRawDocument(raw_payload=payload, received_at=self._now())
            except SecProviderError:
                raise
            except Exception as error:
                last_error = error
        raise SecProviderError("SEC request retry budget exhausted") from last_error

    async def bootstrap_company_facts(self, ciks: Iterable[str]) -> tuple[SecRawDocument, ...]:
        """Explicit bulk-bootstrap seam; callers provide bounded CIK batches."""
        return tuple([await self.fetch_company_facts(cik) for cik in ciks])
