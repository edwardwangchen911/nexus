from datetime import UTC, datetime

import pytest
from nexus.providers.sec import SecCompanyFactsClient, SecProviderError, SecRateLimiter


class FixtureSecTransport:
    def __init__(self) -> None:
        self.paths: list[str] = []
        self.headers: list[dict[str, str]] = []

    async def get(self, path: str, headers: dict[str, str]) -> bytes:
        self.paths.append(path)
        self.headers.append(headers)
        return b'{"cik":"0000320193","facts":{}}'


class FailingSecTransport:
    async def get(self, path: str, headers: dict[str, str]) -> bytes:
        del path, headers
        raise RuntimeError("fixture transport failure")


@pytest.mark.asyncio
async def test_sec_client_uses_configured_contact_and_retains_raw_fixture_payload() -> None:
    transport = FixtureSecTransport()
    client = SecCompanyFactsClient(
        transport,
        contact="research@example.test",
        limiter=SecRateLimiter(monotonic_clock=lambda: 0.0, sleeper=lambda _: None),
        now=lambda: datetime(2026, 8, 24, tzinfo=UTC),
    )

    document = await client.fetch_company_facts("320193")

    assert transport.paths == ["/api/xbrl/companyfacts/CIK0000320193.json"]
    assert transport.headers == [
        {"User-Agent": "NEXUS research@example.test", "Accept-Encoding": "gzip, deflate"}
    ]
    assert document.raw_payload == b'{"cik":"0000320193","facts":{}}'


@pytest.mark.asyncio
async def test_sec_process_rate_limiter_waits_after_five_requests_without_network() -> None:
    now = [0.0]
    waits: list[float] = []

    async def sleeper(duration: float) -> None:
        waits.append(duration)
        now[0] += duration

    limiter = SecRateLimiter(monotonic_clock=lambda: now[0], sleeper=sleeper)
    for _ in range(6):
        await limiter.acquire()

    assert waits == [1.0]


@pytest.mark.asyncio
async def test_sec_client_stops_retrying_when_the_injected_deadline_expires() -> None:
    clock_values = iter((0.0, 0.0, 0.0, 1.0))
    client = SecCompanyFactsClient(
        FailingSecTransport(),
        contact="research@example.test",
        limiter=SecRateLimiter(monotonic_clock=lambda: 0.0, sleeper=lambda _: None),
        monotonic_clock=lambda: next(clock_values),
        deadline_seconds=0.5,
    )

    with pytest.raises(SecProviderError, match="deadline exceeded"):
        await client.fetch_company_facts("320193")
