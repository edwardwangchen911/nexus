"""Explicit fixture/production provider composition without fallback."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime

from nexus.providers.base import MarketDataProvider
from nexus.providers.fred import FredSeriesClient
from nexus.providers.market.alpaca import AlpacaMarketDataProvider
from nexus.providers.market.alpaca_http import (
    AlpacaHttpTransport,
    FixedOriginBytes,
    InstrumentSymbolResolver,
)
from nexus.providers.market.fixture import FixtureMarketDataProvider
from nexus.providers.sec import SecCompanyFactsClient
from nexus.providers.settings import ProviderRuntimeMode, ProviderSettings

HttpClientFactory = Callable[..., object]
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
    if production_builder is None:
        raise RuntimeError("PRODUCTION_PROVIDER_BUNDLE_UNAVAILABLE")
    bundle = production_builder(settings, http_client_factory)
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
