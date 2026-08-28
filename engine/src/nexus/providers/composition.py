"""Explicit fixture/production provider composition without fallback."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import cast

from sqlalchemy.ext.asyncio import AsyncSession

from nexus.providers.base import MarketDataProvider
from nexus.providers.fred import FredSeriesClient
from nexus.providers.fred_http import FredHttpTransport
from nexus.providers.http import AllowedOrigin, HttpRequestPolicy
from nexus.providers.market.alpaca import AlpacaMarketDataProvider
from nexus.providers.market.alpaca_http import (
    AlpacaHttpTransport,
    FixedOriginBytes,
    InstrumentSymbolResolver,
    SqlInstrumentSymbolResolver,
)
from nexus.providers.market.alpaca_snapshot import AlpacaSnapshotProvider
from nexus.providers.market.fixture import FixtureMarketDataProvider
from nexus.providers.sec import SecCompanyFactsClient
from nexus.providers.sec_http import SecHttpTransport
from nexus.providers.settings import IntradayMonitorMode, ProviderRuntimeMode, ProviderSettings

HttpClientFactory = Callable[..., FixedOriginBytes]
MarketProviderFactory = Callable[[object], MarketDataProvider]


@dataclass(frozen=True, repr=False)
class ProviderBundle:
    mode: ProviderRuntimeMode
    market_provider_factory: MarketProviderFactory
    sec_client: SecCompanyFactsClient
    fred_client: FredSeriesClient

    def market_provider(self, session: object) -> MarketDataProvider:
        return self.market_provider_factory(session)

    def with_mode(self, mode: ProviderRuntimeMode) -> ProviderBundle:
        return replace(self, mode=mode)


ProductionBundleBuilder = Callable[
    [ProviderSettings, HttpClientFactory],
    ProviderBundle,
]


def build_provider_bundle(
    settings: ProviderSettings,
    *,
    http_client_factory: HttpClientFactory,
    production_builder: ProductionBundleBuilder | None = None,
) -> ProviderBundle:
    """Build only the selected runtime mode and never fall back across modes."""
    if settings.runtime_mode is ProviderRuntimeMode.FIXTURE:
        return _fixture_bundle()
    builder = production_builder or _production_bundle
    bundle = builder(settings, http_client_factory)
    if bundle.mode is not ProviderRuntimeMode.PRODUCTION:
        raise RuntimeError("PRODUCTION_PROVIDER_BUNDLE_MODE_INVALID")
    return bundle


def build_alpaca_market_provider(
    settings: ProviderSettings,
    *,
    client: FixedOriginBytes,
    symbol_resolver: InstrumentSymbolResolver,
) -> AlpacaMarketDataProvider:
    """Bind exact production Alpaca authorities to one session-owned resolver."""
    if settings.runtime_mode is not ProviderRuntimeMode.PRODUCTION:
        raise RuntimeError("PRODUCTION_PROVIDER_CONFIGURATION_REQUIRED")
    key_id = settings.alpaca_key_id
    secret = settings.alpaca_secret_key
    feed = settings.alpaca_feed
    delivery_mode = settings.alpaca_delivery_mode
    if key_id is None or secret is None or feed is None or delivery_mode is None:
        raise RuntimeError("PRODUCTION_PROVIDER_CONFIGURATION_INCOMPLETE")
    return AlpacaMarketDataProvider(
        AlpacaHttpTransport(
            client,
            key_id=key_id,
            secret=secret,
            feed=feed,
            symbol_resolver=symbol_resolver,
        ),
        feed=feed,
        delivery_mode=delivery_mode,
    )


def build_alpaca_snapshot_provider(
    settings: ProviderSettings,
    *,
    client: FixedOriginBytes,
    symbol_resolver: InstrumentSymbolResolver,
) -> AlpacaSnapshotProvider:
    """Bind the separately gated display-only Snapshot operation."""
    if settings.intraday_monitor_mode is not IntradayMonitorMode.PRODUCTION:
        raise RuntimeError("INTRADAY_MONITOR_DISABLED")
    if settings.runtime_mode is not ProviderRuntimeMode.PRODUCTION:
        raise RuntimeError("PRODUCTION_PROVIDER_CONFIGURATION_REQUIRED")
    key_id = settings.alpaca_key_id
    secret = settings.alpaca_secret_key
    feed = settings.alpaca_intraday_feed
    delivery_mode = settings.alpaca_intraday_delivery_mode
    if key_id is None or secret is None or feed is None or delivery_mode is None:
        raise RuntimeError("INTRADAY_PROVIDER_CONFIGURATION_INCOMPLETE")
    expected_delivery = "delayed" if feed == "delayed_sip" else "real_time"
    if delivery_mode != expected_delivery:
        raise RuntimeError("ALPACA_INTRADAY_AUTHORITY_MISMATCH")
    return AlpacaSnapshotProvider(
        client,
        key_id=key_id,
        secret=secret,
        symbol_resolver=symbol_resolver,
        feed=feed,
        delivery_mode=delivery_mode,
    )


def _production_bundle(
    settings: ProviderSettings,
    http_client_factory: HttpClientFactory,
) -> ProviderBundle:
    alpaca_client = http_client_factory(
        provider_name="alpaca",
        origin=AllowedOrigin(scheme="https", host="data.alpaca.markets"),
        policy=HttpRequestPolicy(max_attempts=3),
    )
    sec_client = http_client_factory(
        provider_name="sec",
        origin=AllowedOrigin(scheme="https", host="data.sec.gov"),
        policy=HttpRequestPolicy(max_attempts=1),
    )
    fred_client = http_client_factory(
        provider_name="fred",
        origin=AllowedOrigin(scheme="https", host="api.stlouisfed.org"),
        policy=HttpRequestPolicy(max_attempts=3),
    )
    sec_contact = settings.sec_contact
    fred_api_key = settings.fred_api_key
    if sec_contact is None or fred_api_key is None:
        raise RuntimeError("PRODUCTION_PROVIDER_CONFIGURATION_INCOMPLETE")

    def market_provider(session: object) -> MarketDataProvider:
        if not hasattr(session, "execute"):
            raise RuntimeError("ASYNC_SESSION_REQUIRED")
        return build_alpaca_market_provider(
            settings,
            client=alpaca_client,
            symbol_resolver=SqlInstrumentSymbolResolver(cast(AsyncSession, session)),
        )

    return ProviderBundle(
        mode=ProviderRuntimeMode.PRODUCTION,
        market_provider_factory=market_provider,
        sec_client=SecCompanyFactsClient(
            SecHttpTransport(sec_client),
            contact=sec_contact,
        ),
        fred_client=FredSeriesClient(
            FredHttpTransport(fred_client, fred_api_key),
            now=lambda: datetime.now(UTC),
        ),
    )


class _EmptySecFixtureTransport:
    async def get(self, path: str, headers: dict[str, str]) -> bytes:
        del path, headers
        return b"{}"


class _EmptyFredFixtureTransport:
    async def get(self, path: str) -> bytes:
        del path
        return b"{}"


def _fixture_bundle() -> ProviderBundle:
    def now() -> datetime:
        return datetime.now(UTC)

    return ProviderBundle(
        mode=ProviderRuntimeMode.FIXTURE,
        market_provider_factory=lambda _session: FixtureMarketDataProvider({}),
        sec_client=SecCompanyFactsClient(
            _EmptySecFixtureTransport(),
            contact="fixture@example.invalid",
            now=now,
        ),
        fred_client=FredSeriesClient(_EmptyFredFixtureTransport(), now=now),
    )
