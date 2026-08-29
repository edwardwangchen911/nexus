"""Explicit fixture/production provider composition without fallback."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
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
from nexus.providers.market.fosun import (
    FosunMarketDataProvider,
    FosunMarketGateway,
    FosunSnapshotProvider,
)
from nexus.providers.market.fosun_gateway import (
    FosunMarketGatewayClient,
    FosunMarketGatewayIdentity,
)
from nexus.providers.sec import SecCompanyFactsClient
from nexus.providers.sec_http import SecHttpTransport
from nexus.providers.settings import (
    IntradayMonitorMode,
    MarketProvider,
    ProviderRuntimeMode,
    ProviderSettings,
)

HttpClientFactory = Callable[..., FixedOriginBytes]
MarketProviderFactory = Callable[[object], MarketDataProvider]
SnapshotProviderFactory = Callable[[object], object]
FosunGatewayFactory = Callable[[FosunMarketGatewayIdentity], FosunMarketGateway]


@dataclass(frozen=True, repr=False)
class ProviderBundle:
    mode: ProviderRuntimeMode
    market_source: str
    market_provider_factory: MarketProviderFactory
    snapshot_provider_factory: SnapshotProviderFactory | None
    sec_client: SecCompanyFactsClient
    fred_client: FredSeriesClient
    owned_close: Callable[[], Awaitable[None]] | None = None

    def market_provider(self, session: object) -> MarketDataProvider:
        return self.market_provider_factory(session)

    def with_mode(self, mode: ProviderRuntimeMode) -> ProviderBundle:
        return replace(self, mode=mode)

    def snapshot_provider(self, session: object) -> object:
        if self.snapshot_provider_factory is None:
            raise RuntimeError("INTRADAY_MONITOR_DISABLED")
        return self.snapshot_provider_factory(session)

    async def aclose(self) -> None:
        if self.owned_close is not None:
            await self.owned_close()


ProductionBundleBuilder = Callable[
    [ProviderSettings, HttpClientFactory],
    ProviderBundle,
]


def build_provider_bundle(
    settings: ProviderSettings,
    *,
    http_client_factory: HttpClientFactory,
    production_builder: ProductionBundleBuilder | None = None,
    fosun_gateway_factory: FosunGatewayFactory = FosunMarketGatewayClient,
) -> ProviderBundle:
    """Build only the selected runtime mode and never fall back across modes."""
    if settings.runtime_mode is ProviderRuntimeMode.FIXTURE:
        return _fixture_bundle()
    bundle = (
        production_builder(settings, http_client_factory)
        if production_builder is not None
        else _production_bundle(settings, http_client_factory, fosun_gateway_factory)
    )
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
    fosun_gateway_factory: FosunGatewayFactory,
) -> ProviderBundle:
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

    def resolver(session: object) -> SqlInstrumentSymbolResolver:
        if not hasattr(session, "execute"):
            raise RuntimeError("ASYNC_SESSION_REQUIRED")
        return SqlInstrumentSymbolResolver(cast(AsyncSession, session))

    if settings.market_provider is MarketProvider.FOSUN:
        identity = _fosun_identity(settings)
        gateway = fosun_gateway_factory(identity)

        def market_provider(session: object) -> MarketDataProvider:
            return FosunMarketDataProvider(
                gateway,
                symbol_resolver=resolver(session),
                delivery_mode=settings.fosun_market_delivery_mode,
            )

        def snapshot_provider(session: object) -> object:
            if settings.intraday_monitor_mode is not IntradayMonitorMode.PRODUCTION:
                raise RuntimeError("INTRADAY_MONITOR_DISABLED")
            return FosunSnapshotProvider(
                gateway,
                symbol_resolver=resolver(session),
                delivery_mode=settings.fosun_market_delivery_mode,
            )

        market_source = "fosun"
        owned_close = gateway.aclose
    else:
        alpaca_client = http_client_factory(
            provider_name="alpaca",
            origin=AllowedOrigin(scheme="https", host="data.alpaca.markets"),
            policy=HttpRequestPolicy(max_attempts=3),
        )

        def market_provider(session: object) -> MarketDataProvider:
            return build_alpaca_market_provider(
                settings,
                client=alpaca_client,
                symbol_resolver=resolver(session),
            )

        def snapshot_provider(session: object) -> object:
            return build_alpaca_snapshot_provider(
                settings,
                client=alpaca_client,
                symbol_resolver=resolver(session),
            )

        market_source = "alpaca"
        owned_close = None

    return ProviderBundle(
        mode=ProviderRuntimeMode.PRODUCTION,
        market_source=market_source,
        market_provider_factory=market_provider,
        snapshot_provider_factory=snapshot_provider,
        sec_client=SecCompanyFactsClient(
            SecHttpTransport(sec_client),
            contact=sec_contact,
        ),
        fred_client=FredSeriesClient(
            FredHttpTransport(fred_client, fred_api_key),
            now=lambda: datetime.now(UTC),
        ),
        owned_close=owned_close,
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
        market_source="fixture",
        market_provider_factory=lambda _session: FixtureMarketDataProvider({}),
        snapshot_provider_factory=None,
        sec_client=SecCompanyFactsClient(
            _EmptySecFixtureTransport(),
            contact="fixture@example.invalid",
            now=now,
        ),
        fred_client=FredSeriesClient(_EmptyFredFixtureTransport(), now=now),
    )


def _fosun_identity(settings: ProviderSettings) -> FosunMarketGatewayIdentity:
    origin = settings.fosun_market_gateway_origin
    key_id = settings.fosun_market_gateway_key_id
    secret = settings.fosun_market_gateway_secret
    client_cert = settings.fosun_market_gateway_client_cert
    client_key = settings.fosun_market_gateway_client_key
    client_ca = settings.fosun_market_gateway_client_ca
    if (
        origin is None
        or key_id is None
        or secret is None
        or client_cert is None
        or client_key is None
        or client_ca is None
    ):
        raise RuntimeError("FOSUN_MARKET_PROVIDER_CONFIGURATION_INCOMPLETE")
    return FosunMarketGatewayIdentity(
        origin=origin,
        key_id=key_id,
        secret=secret,
        client_cert=client_cert,
        client_key=client_key,
        client_ca=client_ca,
    )
