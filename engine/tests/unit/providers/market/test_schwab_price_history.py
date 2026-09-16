"""Bounded Schwab price-history provider contracts."""

import asyncio
import base64
import json
from datetime import UTC, datetime, timedelta
from importlib import import_module
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from nexus.providers.base import BarsRequest, InstrumentId
from nexus.providers.http import HttpPolicyError
from nexus.providers.market import schwab_oauth

from tests.support.provider_fakes import FixedInstrumentSymbolResolver

AAPL_ID = InstrumentId(UUID("fd52a392-130e-4ef8-83e2-01ee6f2b175a"))
START = datetime(2026, 9, 14, 13, 30, tzinfo=UTC)
END = START + timedelta(minutes=5)
RECEIVED_AT = END + timedelta(seconds=30)


def _request(**updates: object) -> BarsRequest:
    values: dict[str, object] = {
        "instrument_id": AAPL_ID,
        "start": START,
        "end": END,
        "interval": "1Min",
        "adjustment": "raw",
    }
    values.update(updates)
    return BarsRequest.model_validate(values)


def _save_live_token(repo_root: Path, *, expires_at: datetime | None = None) -> None:
    schwab_oauth.save_schwab_token(
        repo_root,
        schwab_oauth.SchwabOAuthToken(
            access_token="access-canary",
            refresh_token="refresh-canary",
            obtained_at=RECEIVED_AT - timedelta(minutes=10),
            expires_at=expires_at or RECEIVED_AT + timedelta(minutes=10),
        ),
    )


def _write_schwab_app_config(repo_root: Path) -> None:
    infra = repo_root / "infra"
    infra.mkdir(exist_ok=True)
    (infra / ".env.local").write_text(
        "NEXUS_SCHWAB_APP_KEY=app-key-canary\n"
        "NEXUS_SCHWAB_APP_SECRET=app-secret-canary\n"
        "NEXUS_SCHWAB_CALLBACK_URL=https://127.0.0.1\n",
        encoding="utf-8",
    )


def _candle(observed_at: datetime, *, close: object = 100) -> dict[str, object]:
    return {
        "open": 100,
        "high": 105,
        "low": 95,
        "close": close,
        "volume": 1000,
        "datetime": int(observed_at.timestamp() * 1000),
    }


def _valid_payload(*, symbol: str = "AAPL") -> bytes:
    return json.dumps(
        {
            "symbol": symbol,
            "empty": False,
            "candles": [_candle(START + timedelta(minutes=index)) for index in range(5)],
        },
        separators=(",", ":"),
    ).encode()


async def _fetch_raw(
    repo_root: Path,
    raw: bytes,
    *,
    bar_request: BarsRequest | None = None,
    symbol: str = "AAPL",
    status: int = 200,
    headers: dict[str, str] | None = None,
):
    module = import_module("nexus.providers.market.schwab_price_history")
    _save_live_token(repo_root)
    calls = 0

    def respond(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(status, content=raw, headers=headers)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond), follow_redirects=False
    ) as client:
        provider = module.SchwabPriceHistoryProvider(
            repo_root,
            client,
            symbol_resolver=FixedInstrumentSymbolResolver(symbol),
            now=lambda: RECEIVED_AT,
        )
        page = await provider.fetch_bars(bar_request or _request())
    return page, calls


def test_live_access_token_is_returned_only_before_expiry() -> None:
    """An expired stored token must never authorize a market-data request."""
    require_token = getattr(schwab_oauth, "require_live_schwab_access_token", None)
    assert require_token is not None
    now = datetime(2026, 9, 14, 1, tzinfo=UTC)
    live = schwab_oauth.SchwabOAuthToken(
        access_token="access-canary",
        refresh_token="refresh-canary",
        obtained_at=now - timedelta(minutes=10),
        expires_at=now + timedelta(minutes=10),
    )
    expired = schwab_oauth.SchwabOAuthToken(
        access_token="expired-canary",
        refresh_token="refresh-canary",
        obtained_at=now - timedelta(hours=1),
        expires_at=now,
    )

    assert require_token(live, now=now) == "access-canary"
    with pytest.raises(schwab_oauth.SchwabOAuthError, match="^SCHWAB_OAUTH_ACCESS_TOKEN_EXPIRED$"):
        require_token(expired, now=now)


@pytest.mark.asyncio
@pytest.mark.parametrize("explicit", [False, True])
async def test_provider_refreshes_expiring_token_once_before_price_history_request(
    tmp_path: Path,
    monkeypatch,
    explicit: bool,
) -> None:
    for name in (
        "NEXUS_SCHWAB_APP_KEY",
        "NEXUS_SCHWAB_APP_SECRET",
        "NEXUS_SCHWAB_CALLBACK_URL",
    ):
        monkeypatch.delenv(name, raising=False)
    module = import_module("nexus.providers.market.schwab_price_history")
    _write_schwab_app_config(tmp_path)
    _save_live_token(tmp_path, expires_at=RECEIVED_AT + timedelta(minutes=4))
    requests: list[httpx.Request] = []
    environment = {
        "NEXUS_SCHWAB_APP_KEY": "registered-key",
        "NEXUS_SCHWAB_APP_SECRET": "registered-secret",
        "NEXUS_SCHWAB_CALLBACK_URL": "https://127.0.0.1",
    }
    if explicit:
        from nexus.ops.agent_paper_loop import load_agent_paper_service_environment

        env_file = tmp_path / "infra/.env.service"
        env_file.write_text("\n".join(f"{name}={value}" for name, value in environment.items()))
        environment = load_agent_paper_service_environment(tmp_path, {}, env_file=env_file)
        monkeypatch.setenv("NEXUS_SCHWAB_APP_KEY", "process-key")
        monkeypatch.setenv("NEXUS_SCHWAB_APP_SECRET", "process-secret")
        monkeypatch.setenv("NEXUS_SCHWAB_CALLBACK_URL", "https://process.invalid")

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/v1/oauth/token":
            expected = (
                "registered-key:registered-secret"
                if explicit
                else "app-key-canary:app-secret-canary"
            )
            assert (
                request.headers["Authorization"]
                == "Basic " + base64.b64encode(expected.encode()).decode()
            )
            return httpx.Response(
                200,
                json={
                    "access_token": "refreshed-access-canary",
                    "refresh_token": "rotated-refresh-canary",
                    "expires_in": 1800,
                    "token_type": "Bearer",
                },
            )
        assert request.url.path == "/marketdata/v1/pricehistory"
        assert request.headers["Authorization"] == "Bearer refreshed-access-canary"
        return httpx.Response(200, content=_valid_payload())

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond), follow_redirects=False
    ) as client:
        provider = module.SchwabPriceHistoryProvider(
            tmp_path,
            client,
            symbol_resolver=FixedInstrumentSymbolResolver("AAPL"),
            now=lambda: RECEIVED_AT,
            **({"process_environment": environment} if explicit else {}),
        )
        page = await provider.fetch_bars(_request())

    stored = schwab_oauth.load_schwab_token(tmp_path)
    assert [request.method for request in requests] == ["POST", "GET"]
    assert stored.access_token == "refreshed-access-canary"
    assert stored.refresh_token == "rotated-refresh-canary"
    assert len(page.items) == 5


@pytest.mark.asyncio
async def test_provider_refresh_failure_keeps_token_and_skips_price_history(
    tmp_path: Path,
) -> None:
    module = import_module("nexus.providers.market.schwab_price_history")
    _write_schwab_app_config(tmp_path)
    _save_live_token(tmp_path, expires_at=RECEIVED_AT)
    token_path = tmp_path / ".secrets" / "nexus-schwab-local" / "tokens.json"
    original = token_path.read_bytes()
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(401, content=b"private-provider-body-canary")

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond), follow_redirects=False
    ) as client:
        provider = module.SchwabPriceHistoryProvider(
            tmp_path,
            client,
            symbol_resolver=FixedInstrumentSymbolResolver("AAPL"),
            now=lambda: RECEIVED_AT,
        )
        with pytest.raises(
            schwab_oauth.SchwabOAuthError,
            match="^SCHWAB_OAUTH_REFRESH_HTTP_401$",
        ):
            await provider.fetch_bars(_request())

    assert [request.method for request in requests] == ["POST"]
    assert token_path.read_bytes() == original


@pytest.mark.asyncio
async def test_concurrent_providers_serialize_refresh_token_rotation(
    tmp_path: Path,
) -> None:
    module = import_module("nexus.providers.market.schwab_price_history")
    _write_schwab_app_config(tmp_path)
    _save_live_token(tmp_path, expires_at=RECEIVED_AT + timedelta(minutes=4))
    refresh_requests = 0
    price_requests = 0

    async def respond(request: httpx.Request) -> httpx.Response:
        nonlocal price_requests, refresh_requests
        if request.url.path == "/v1/oauth/token":
            refresh_requests += 1
            await asyncio.sleep(0.05)
            return httpx.Response(
                200,
                json={
                    "access_token": "refreshed-access-canary",
                    "refresh_token": "rotated-refresh-canary",
                    "expires_in": 1800,
                    "token_type": "Bearer",
                },
            )
        price_requests += 1
        assert request.headers["Authorization"] == "Bearer refreshed-access-canary"
        return httpx.Response(200, content=_valid_payload())

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond), follow_redirects=False
    ) as client:
        providers = [
            module.SchwabPriceHistoryProvider(
                tmp_path,
                client,
                symbol_resolver=FixedInstrumentSymbolResolver("AAPL"),
                now=lambda: RECEIVED_AT,
            )
            for _ in range(2)
        ]
        await asyncio.gather(*(provider.fetch_bars(_request()) for provider in providers))

    assert refresh_requests == 1
    assert price_requests == 2


@pytest.mark.asyncio
async def test_cancellation_waits_for_refresh_commit_before_stopping(
    tmp_path: Path,
) -> None:
    module = import_module("nexus.providers.market.schwab_price_history")
    _write_schwab_app_config(tmp_path)
    _save_live_token(tmp_path, expires_at=RECEIVED_AT)
    refresh_started = asyncio.Event()
    release_refresh = asyncio.Event()
    requests: list[httpx.Request] = []

    async def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.path == "/v1/oauth/token"
        refresh_started.set()
        await release_refresh.wait()
        return httpx.Response(
            200,
            json={
                "access_token": "refreshed-access-canary",
                "refresh_token": "rotated-refresh-canary",
                "expires_in": 1800,
                "token_type": "Bearer",
            },
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond), follow_redirects=False
    ) as client:
        provider = module.SchwabPriceHistoryProvider(
            tmp_path,
            client,
            symbol_resolver=FixedInstrumentSymbolResolver("AAPL"),
            now=lambda: RECEIVED_AT,
        )
        task = asyncio.create_task(provider.fetch_bars(_request()))
        await refresh_started.wait()
        task.cancel()
        release_refresh.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    stored = schwab_oauth.load_schwab_token(tmp_path)
    assert [request.method for request in requests] == ["POST"]
    assert stored.access_token == "refreshed-access-canary"
    assert stored.refresh_token == "rotated-refresh-canary"


@pytest.mark.asyncio
async def test_provider_issues_one_frozen_request_and_normalizes_only_the_window(
    tmp_path: Path,
) -> None:
    """A changed endpoint, query, evidence byte, or time boundary must break the contract."""
    module = import_module("nexus.providers.market.schwab_price_history")
    _save_live_token(tmp_path)
    payload = {
        "symbol": "AAPL",
        "empty": False,
        "previousClose": 99.5,
        "previousCloseDate": int((START - timedelta(days=1)).timestamp() * 1000),
        "candles": [
            _candle(START - timedelta(minutes=1)),
            *[_candle(START + timedelta(minutes=index), close=100 + index) for index in range(5)],
            _candle(END),
        ],
    }
    raw = json.dumps(payload, separators=(",", ":")).encode()
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "GET"
        assert request.url == httpx.URL(
            "https://api.schwabapi.com/marketdata/v1/pricehistory",
            params=(
                ("symbol", "AAPL"),
                ("frequencyType", "minute"),
                ("frequency", "1"),
                ("startDate", str(int(START.timestamp() * 1000))),
                ("endDate", str(int(END.timestamp() * 1000))),
                ("needExtendedHoursData", "true"),
                ("needPreviousClose", "true"),
            ),
        )
        assert request.headers["Authorization"] == "Bearer access-canary"
        assert request.headers["Accept"] == "application/json"
        return httpx.Response(200, content=raw)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond), follow_redirects=False
    ) as client:
        provider = module.SchwabPriceHistoryProvider(
            tmp_path,
            client,
            symbol_resolver=FixedInstrumentSymbolResolver("AAPL"),
            now=lambda: RECEIVED_AT,
        )
        page = await provider.fetch_bars(_request())

    assert len(requests) == 1
    assert page.raw_payload == raw
    assert page.next_cursor is None
    assert page.as_of == RECEIVED_AT
    assert len(page.items) == 5
    assert [bar.observed_at for bar in page.items] == [
        START + timedelta(minutes=index) for index in range(5)
    ]
    assert [str(bar.close) for bar in page.items] == [str(100 + index) for index in range(5)]
    assert all(bar.instrument_id == AAPL_ID for bar in page.items)
    assert all(bar.session.isoformat() == "2026-09-14" for bar in page.items)
    assert all(bar.available_at == RECEIVED_AT for bar in page.items)
    assert all(bar.received_at == RECEIVED_AT for bar in page.items)
    assert page.provenance.model_dump() == {
        "source": "schwab",
        "feed": "price_history",
        "adjustment": "raw",
        "interval": "1Min",
        "mode": "read_only",
        "delivery_mode": "unknown",
    }


@pytest.mark.parametrize(
    ("bar_request", "cursor"),
    (
        (_request(interval="1Day"), None),
        (_request(adjustment="all"), None),
        (_request(end=START), None),
        (_request(end=START + timedelta(minutes=4)), None),
        (
            _request(
                start=START + timedelta(seconds=30),
                end=END + timedelta(seconds=30),
            ),
            None,
        ),
        (_request(), "unexpected-page"),
    ),
)
@pytest.mark.asyncio
async def test_unsupported_request_shapes_fail_before_credentials_are_sent(
    tmp_path: Path,
    bar_request: BarsRequest,
    cursor: str | None,
) -> None:
    """Only one raw five-minute page may reach the credentialed endpoint."""
    module = import_module("nexus.providers.market.schwab_price_history")
    _save_live_token(tmp_path)
    calls = 0

    def respond(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, content=_valid_payload())

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond), follow_redirects=False
    ) as client:
        provider = module.SchwabPriceHistoryProvider(
            tmp_path,
            client,
            symbol_resolver=FixedInstrumentSymbolResolver("AAPL"),
            now=lambda: RECEIVED_AT,
        )
        with pytest.raises(module.MarketProviderPayloadError):
            await provider.fetch_bars(bar_request, cursor)

    assert calls == 0


@pytest.mark.parametrize(
    "raw",
    (
        b"",
        b"not-json",
        b'{"symbol":"AAPL","empty":true,"candles":[]}',
        b'{"symbol":"MSFT","empty":false,"candles":[]}',
        b'{"symbol":"AAPL","symbol":"AAPL","empty":false,"candles":[]}',
        json.dumps(
            {
                "symbol": "AAPL",
                "empty": False,
                "candles": [_candle(START + timedelta(minutes=index)) for index in range(4)],
            }
        ).encode(),
        json.dumps(
            {
                "symbol": "AAPL",
                "empty": False,
                "candles": [
                    {
                        **_candle(START + timedelta(minutes=index)),
                        "datetime": str(int((START + timedelta(minutes=index)).timestamp() * 1000)),
                    }
                    for index in range(5)
                ],
            }
        ).encode(),
        json.dumps(
            {
                "symbol": "AAPL",
                "empty": False,
                "candles": [
                    _candle(START),
                    _candle(START + timedelta(minutes=1)),
                    _candle(START + timedelta(minutes=1)),
                    _candle(START + timedelta(minutes=3)),
                    _candle(START + timedelta(minutes=4)),
                ],
            }
        ).encode(),
        json.dumps(
            {
                "symbol": "AAPL",
                "empty": False,
                "candles": [
                    _candle(START),
                    _candle(START + timedelta(minutes=2)),
                    _candle(START + timedelta(minutes=1)),
                    _candle(START + timedelta(minutes=3)),
                    _candle(START + timedelta(minutes=4)),
                ],
            }
        ).encode(),
    ),
)
@pytest.mark.asyncio
async def test_malformed_or_incomplete_response_is_rejected(
    tmp_path: Path,
    raw: bytes,
) -> None:
    """Malformed identity or minute membership must never become fill evidence."""
    module = import_module("nexus.providers.market.schwab_price_history")
    with pytest.raises(module.MarketProviderPayloadError):
        await _fetch_raw(tmp_path, raw)


@pytest.mark.parametrize(
    "overrides",
    (
        {"open": 0},
        {"high": 99},
        {"low": 101},
        {"close": -1},
        {"volume": -1},
    ),
)
@pytest.mark.asyncio
async def test_invalid_ohlcv_is_rejected(
    tmp_path: Path,
    overrides: dict[str, object],
) -> None:
    """Non-positive prices, inverted ranges, and negative volume cannot price fills."""
    module = import_module("nexus.providers.market.schwab_price_history")
    candles = [_candle(START + timedelta(minutes=index)) for index in range(5)]
    candles[2].update(overrides)
    raw = json.dumps({"symbol": "AAPL", "empty": False, "candles": candles}).encode()

    with pytest.raises(module.MarketProviderPayloadError):
        await _fetch_raw(tmp_path, raw)


@pytest.mark.parametrize(
    "bar_request",
    (
        _request(
            start=datetime(2026, 9, 14, 3, 58, tzinfo=UTC),
            end=datetime(2026, 9, 14, 4, 3, tzinfo=UTC),
        ),
        _request(
            start=RECEIVED_AT + timedelta(seconds=30),
            end=RECEIVED_AT + timedelta(minutes=5, seconds=30),
        ),
    ),
)
@pytest.mark.asyncio
async def test_cross_session_or_future_window_is_rejected(
    tmp_path: Path,
    bar_request: BarsRequest,
) -> None:
    """One fill window must be a received fact from one New York session."""
    module = import_module("nexus.providers.market.schwab_price_history")
    raw = json.dumps(
        {
            "symbol": "AAPL",
            "empty": False,
            "candles": [
                _candle(bar_request.start + timedelta(minutes=index)) for index in range(5)
            ],
        }
    ).encode()

    with pytest.raises(module.MarketProviderPayloadError):
        await _fetch_raw(tmp_path, raw, bar_request=bar_request)


@pytest.mark.asyncio
async def test_json_numbers_remain_exact_decimals(tmp_path: Path) -> None:
    """A JSON decimal must not pass through a binary float before normalization."""
    raw = _valid_payload().replace(b'"close":100', b'"close":100.10000000000000001', 1)

    page, _ = await _fetch_raw(tmp_path, raw)

    assert str(page.items[0].close) == "100.10000000000000001"


@pytest.mark.parametrize(
    ("status", "headers", "error"),
    (
        (302, {"location": "https://evil.example/steal"}, "REDIRECT_REJECTED"),
        (200, {"content-length": str(8 * 1024 * 1024 + 1)}, "RESPONSE_TOO_LARGE"),
    ),
)
@pytest.mark.asyncio
async def test_redirect_and_oversize_response_are_rejected_after_one_get(
    tmp_path: Path,
    status: int,
    headers: dict[str, str],
    error: str,
) -> None:
    """Credentialed acquisition cannot redirect, retry, or exceed the archive bound."""
    with pytest.raises(HttpPolicyError, match=f"^{error}$"):
        await _fetch_raw(tmp_path, b"{}", status=status, headers=headers)


@pytest.mark.asyncio
async def test_retryable_status_still_uses_exactly_one_get(tmp_path: Path) -> None:
    """A paper-window read cannot silently multiply external requests."""
    module = import_module("nexus.providers.market.schwab_price_history")
    _save_live_token(tmp_path)
    calls = 0

    def respond(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond), follow_redirects=False
    ) as client:
        provider = module.SchwabPriceHistoryProvider(
            tmp_path,
            client,
            symbol_resolver=FixedInstrumentSymbolResolver("AAPL"),
            now=lambda: RECEIVED_AT,
        )
        with pytest.raises(HttpPolicyError, match="^RETRY_BUDGET_EXHAUSTED$"):
            await provider.fetch_bars(_request())

    assert calls == 1


@pytest.mark.parametrize("symbol", ("aapl", "BRK/B", "AAPL?token=x", "", "A" * 16))
@pytest.mark.asyncio
async def test_unresolved_or_unsafe_ticker_fails_before_http(
    tmp_path: Path,
    symbol: str,
) -> None:
    """Resolver output must remain one validated US ticker, never a query escape."""
    module = import_module("nexus.providers.market.schwab_price_history")
    _save_live_token(tmp_path)
    calls = 0

    def respond(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, content=_valid_payload(symbol=symbol))

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond), follow_redirects=False
    ) as client:
        provider = module.SchwabPriceHistoryProvider(
            tmp_path,
            client,
            symbol_resolver=FixedInstrumentSymbolResolver(symbol),
            now=lambda: RECEIVED_AT,
        )
        with pytest.raises(module.MarketProviderPayloadError):
            await provider.fetch_bars(_request())

    assert calls == 0
