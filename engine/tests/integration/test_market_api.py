import asyncio
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from urllib.parse import urlencode
from uuid import UUID, uuid4

import httpx
import pytest
from helpers.hmac import TEST_HMAC_KEY_ID, TEST_HMAC_SECRET, build_test_hmac_headers
from nexus.main import create_engine_app
from nexus.market.database_series import load_intraday_series
from nexus.market.intraday_ingestion import canonical_quote_observation
from nexus.market.intraday_metrics import (
    IntradayAuthorityView,
    IntradayQuoteView,
    IntradayTradeView,
)
from nexus.persistence.models.data import DataArtifact, ProviderCapability
from nexus.persistence.models.market import (
    Instrument,
    InstrumentAlias,
    LatestQuote,
    UniverseMember,
    UniverseSnapshot,
    Watchlist,
    WatchlistMember,
)
from nexus.persistence.models.runtime import IntegrationStatus
from nexus.persistence.session import session_factory
from nexus.providers.base import InstrumentId, PriceBar, PriceSeries
from nexus.settings import EngineSettings
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from uuid6 import uuid7

_DECISION_TIME = datetime(2026, 8, 24, 14, 0, tzinfo=UTC)
_AAPL_ID = UUID("38df053d-1e87-5938-b279-a1ef5ab4643e")
_MSFT_ID = UUID("f15da43d-5a56-5f8d-a860-c1494c79fc96")
_NVDA_ID = UUID("d7a35b5a-ca07-51a7-941a-e996495dbed9")
_UNIVERSE_ID = UUID("a76624e0-fcad-5e4f-93cd-e4b63281ea3c")
_CONCURRENT_REQUEST_TIMEOUT_SECONDS = 5.0


def _settings(database_url: str) -> EngineSettings:
    return EngineSettings(
        NEXUS_ENV="test",
        DATABASE_URL=database_url,
        NEXUS_ENGINE_HMAC_CURRENT_KEY_ID=TEST_HMAC_KEY_ID,
        NEXUS_ENGINE_HMAC_CURRENT_SECRET=TEST_HMAC_SECRET,
    )


def _app(database_url: str):
    app = create_engine_app(_settings(database_url))
    app.state.utc_clock = lambda: _DECISION_TIME
    return app


def _headers(method: str, path_and_query: str, *, nonce: UUID | None = None) -> dict[str, str]:
    return build_test_hmac_headers(
        method,
        path_and_query,
        b"",
        nonce=str(nonce or uuid7()),
    )


@asynccontextmanager
async def _client(app, *, publish_universe: bool = True) -> AsyncIterator[httpx.AsyncClient]:
    try:
        if publish_universe:
            async with app.state.session_factory() as session:
                await session.execute(
                    text(
                        "UPDATE universe_snapshots SET published_at = :published_at "
                        "WHERE id = :snapshot_id AND published_at IS NULL"
                    ),
                    {
                        "published_at": _DECISION_TIME - timedelta(seconds=1),
                        "snapshot_id": _UNIVERSE_ID,
                    },
                )
                await session.commit()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            yield client
    finally:
        await app.state.session_factory.kw["bind"].dispose()


async def _seed_instrument(
    database_url: str,
    *,
    instrument_id: UUID,
    symbol: str,
    tick_size: Decimal | None = Decimal("0.01"),
    valid_from: date = date(2020, 1, 1),
    valid_to: date | None = None,
    exchange: str = "XNYS",
    currency: str = "USD",
    add_to_universe: bool = True,
    universe_effective_from: date = date(2026, 1, 1),
) -> None:
    factory = session_factory(_settings(database_url))
    try:
        async with factory() as session:
            session.add(
                Instrument(
                    id=instrument_id,
                    asset_type="common_stock",
                    exchange=exchange,
                    currency=currency,
                    tick_size=tick_size,
                    risk_bucket="EQUITY",
                )
            )
            session.add(
                InstrumentAlias(
                    instrument_id=instrument_id,
                    symbol=symbol,
                    alias_type="ticker",
                    valid_from=valid_from,
                    valid_to=valid_to,
                )
            )
            if add_to_universe:
                snapshot = await session.get(UniverseSnapshot, _UNIVERSE_ID)
                if snapshot is None:
                    session.add(
                        UniverseSnapshot(
                            id=_UNIVERSE_ID,
                            key="us_liquid",
                            version="us_liquid_v1",
                            effective_from=universe_effective_from,
                            source="versioned_fixture",
                            content_hash="e" * 64,
                            published_at=None,
                        )
                    )
                    await session.flush()
                session.add(
                    UniverseMember(
                        universe_snapshot_id=_UNIVERSE_ID,
                        instrument_id=instrument_id,
                        inclusion_reason="versioned fixture",
                    )
                )
            await session.commit()
    finally:
        await factory.kw["bind"].dispose()


async def _seed_local_fosun_universe(
    database_url: str,
    *,
    instrument_ids: tuple[UUID, ...],
) -> None:
    factory = session_factory(_settings(database_url))
    try:
        async with factory() as session:
            snapshot_id = uuid4()
            snapshot = UniverseSnapshot(
                id=snapshot_id,
                key="fosun_local_holdings",
                version="holdings-test",
                effective_from=_DECISION_TIME.date(),
                source="fosun_local_read_only",
                content_hash="f" * 64,
                published_at=None,
            )
            session.add(snapshot)
            await session.flush()
            session.add_all(
                UniverseMember(
                    universe_snapshot_id=snapshot_id,
                    instrument_id=instrument_id,
                    inclusion_reason="local read-only holding market display",
                )
                for instrument_id in instrument_ids
            )
            await session.flush()
            snapshot.published_at = _DECISION_TIME - timedelta(seconds=1)
            await session.commit()
    finally:
        await factory.kw["bind"].dispose()


async def _seed_quote(
    database_url: str,
    *,
    instrument_id: UUID,
    source: str = "fixture",
    feed: str = "fixture",
    delivery_mode: str = "fixture",
    last: Decimal | None = Decimal("100.00"),
    observed_at: datetime = _DECISION_TIME - timedelta(seconds=10),
    available_at: datetime | None = None,
    received_at: datetime | None = None,
    quality_flags: Sequence[str] = (),
) -> None:
    factory = session_factory(_settings(database_url))
    try:
        async with factory() as session:
            visible_at = available_at or observed_at + timedelta(seconds=1)
            received_at = received_at or visible_at
            event_kind = "fixture" if source == "fixture" else "trade"
            raw_payload = (
                f"market-api-fixture:{instrument_id}:{source}:{feed}:"
                f"{delivery_mode}:{observed_at.isoformat()}:{last}"
            ).encode()
            revision = "1" if source == "fixture" else sha256(raw_payload).hexdigest()
            canonical = canonical_quote_observation(
                instrument_id=instrument_id,
                source=source,
                feed=feed,
                delivery_mode=delivery_mode,
                event_kind=event_kind,
                bid=None,
                ask=None,
                last=last,
                observed_at=observed_at,
                available_at=visible_at,
                received_at=received_at,
                revision=revision,
            )
            artifact = None
            if source != "fixture":
                raw_digest = sha256(raw_payload).hexdigest()
                artifact = DataArtifact(
                    uri=f"memory://sha256/{raw_digest}",
                    sha256=raw_digest,
                    byte_size=len(raw_payload),
                    mime_type="application/json",
                    schema_version="market_api_fixture_v1",
                    source=source,
                    observed_at=observed_at,
                    available_at=visible_at,
                    received_at=received_at,
                )
                session.add(artifact)
                await session.flush()
            session.add(
                LatestQuote(
                    instrument_id=instrument_id,
                    source=source,
                    feed=feed,
                    delivery_mode=delivery_mode,
                    bid=None,
                    ask=None,
                    last=last,
                    observed_at=observed_at,
                    available_at=visible_at,
                    received_at=received_at,
                    revision=revision,
                    quality_flags=list(quality_flags),
                    event_kind=event_kind,
                    content_hash=sha256(canonical).hexdigest(),
                    evidence_status="fixture" if source == "fixture" else "verified",
                    data_artifact_id=None if artifact is None else artifact.id,
                )
            )
            await session.commit()
    finally:
        await factory.kw["bind"].dispose()


async def _seed_fosun_admission(
    database_url: str,
    *,
    operation: str,
    contract_hash: str,
    delivery_mode: str = "unknown",
    admitted_contract_hash: str | None = None,
    expires_at: datetime | None = None,
    created_at: datetime = _DECISION_TIME - timedelta(hours=1),
) -> tuple[UUID, UUID, int, datetime]:
    capability_id = uuid4()
    effective_expiry = expires_at or datetime.now(UTC) + timedelta(days=1)
    factory = session_factory(_settings(database_url))
    try:
        async with factory() as session:
            session.add(
                ProviderCapability(
                    id=capability_id,
                    provider="fosun",
                    capability="READ_ONLY",
                    contract_hash=contract_hash,
                    expires_at=effective_expiry,
                    schema_version="1",
                    payload={
                        "operation": operation,
                        "market_scope": "US",
                        "delivery_mode": delivery_mode,
                    },
                    created_at=created_at,
                )
            )
            status = await session.scalar(
                select(IntegrationStatus).where(IntegrationStatus.provider == "fosun")
            )
            if status is None:
                status = IntegrationStatus(
                    provider="fosun",
                    status="read_only",
                    admitted_contract_hash=(admitted_contract_hash or contract_hash),
                    mode_changed_at=created_at,
                    created_at=created_at,
                )
                session.add(status)
            await session.flush()
            result = (capability_id, status.id, status.version, effective_expiry)
            await session.commit()
            return result
    finally:
        await factory.kw["bind"].dispose()


async def _seed_fosun_quote_v2(
    database_url: str,
    *,
    instrument_id: UUID,
    last: Decimal = Decimal("123.45"),
    delivery_mode: str = "unknown",
    contract_hash: str = "a" * 64,
    received_at: datetime = _DECISION_TIME - timedelta(seconds=10),
    admission: tuple[UUID, UUID, int, datetime] | None = None,
    operation: str = "fosun_get_quotes",
    artifact_schema: str = "fosun_gateway_market_snapshot_v1",
    observed_at: datetime | None = None,
) -> tuple[UUID, UUID, int, datetime]:
    if admission is None:
        admission = await _seed_fosun_admission(
            database_url,
            operation=operation,
            contract_hash=contract_hash,
            delivery_mode=delivery_mode,
        )
    capability_id, status_id, status_version, _ = admission
    effective_observed_at = observed_at or received_at
    raw_payload = (
        f"fosun-market-api-v2:{instrument_id}:{delivery_mode}:{received_at.isoformat()}:{last}"
    ).encode()
    digest = sha256(raw_payload).hexdigest()
    canonical = canonical_quote_observation(
        instrument_id=instrument_id,
        source="fosun",
        feed="l1",
        delivery_mode=delivery_mode,
        event_kind="snapshot",
        bid=None,
        ask=None,
        last=last,
        observed_at=effective_observed_at,
        available_at=received_at,
        received_at=received_at,
        revision=digest,
    )
    factory = session_factory(_settings(database_url))
    try:
        async with factory() as session:
            artifact = DataArtifact(
                uri=f"memory://sha256/{digest}",
                sha256=digest,
                byte_size=len(raw_payload),
                mime_type="application/json",
                schema_version=artifact_schema,
                source="fosun",
                observed_at=effective_observed_at,
                available_at=received_at,
                received_at=received_at,
            )
            session.add(artifact)
            await session.flush()
            session.add(
                LatestQuote(
                    instrument_id=instrument_id,
                    source="fosun",
                    feed="l1",
                    delivery_mode=delivery_mode,
                    bid=None,
                    ask=None,
                    last=last,
                    observed_at=effective_observed_at,
                    available_at=received_at,
                    received_at=received_at,
                    revision=digest,
                    quality_flags=[
                        "FOSUN_PROVIDER_TIMESTAMP_UNAVAILABLE",
                        "INTRADAY_BID_ASK_UNAVAILABLE",
                    ],
                    event_kind="snapshot",
                    content_hash=sha256(canonical).hexdigest(),
                    evidence_status="verified",
                    data_artifact_id=artifact.id,
                    provider_capability_id=capability_id,
                    provider_status_id=status_id,
                    provider_status_version=status_version,
                    provider_contract_hash=contract_hash,
                    provider_authorized_at=received_at + timedelta(seconds=1),
                )
            )
            await session.commit()
    finally:
        await factory.kw["bind"].dispose()
    return admission


@pytest.mark.asyncio
async def test_market_requires_hmac_authentication(migrated_database_url: str) -> None:
    """Removing the service-actor dependency would expose private market facts."""
    app = _app(migrated_database_url)
    async with _client(app) as client:
        response = await client.get("/internal/v1/market")

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "UNAUTHENTICATED"


@pytest.mark.asyncio
async def test_market_limit_is_capped_at_100(migrated_database_url: str) -> None:
    """Removing the API bound would allow an unbounded internal market read."""
    path = "/internal/v1/market?limit=101"
    app = _app(migrated_database_url)
    async with _client(app) as client:
        response = await client.get(path, headers=_headers("GET", path))

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "VALIDATION_ERROR"


@pytest.mark.asyncio
async def test_market_never_uses_fixture_or_future_alpaca_as_current_fallback(
    migrated_database_url: str,
) -> None:
    """Float conversion, future aliases, future quotes, or missing provenance break this."""
    await _seed_instrument(
        migrated_database_url,
        instrument_id=_AAPL_ID,
        symbol="AAPL",
        valid_to=_DECISION_TIME.date(),
    )
    await _seed_instrument(
        migrated_database_url,
        instrument_id=_MSFT_ID,
        symbol="EXPIRED",
        valid_to=_DECISION_TIME.date() - timedelta(days=1),
    )
    factory = session_factory(_settings(migrated_database_url))
    try:
        async with factory() as session:
            session.add(
                InstrumentAlias(
                    instrument_id=_AAPL_ID,
                    symbol="FUTURE",
                    alias_type="ticker",
                    valid_from=_DECISION_TIME.date() + timedelta(days=1),
                    valid_to=None,
                )
            )
            await session.commit()
    finally:
        await factory.kw["bind"].dispose()
    await _seed_quote(
        migrated_database_url,
        instrument_id=_AAPL_ID,
        last=Decimal("123.4500"),
        observed_at=_DECISION_TIME - timedelta(seconds=8),
    )
    await _seed_quote(
        migrated_database_url,
        instrument_id=_AAPL_ID,
        source="alpaca",
        feed="iex",
        delivery_mode="real_time",
        last=Decimal("999.00"),
        observed_at=_DECISION_TIME + timedelta(minutes=1),
        available_at=_DECISION_TIME + timedelta(minutes=1),
    )
    path = "/internal/v1/market?limit=100"
    app = _app(migrated_database_url)

    async with _client(app) as client:
        response = await client.get(path, headers=_headers("GET", path))

    assert response.status_code == 200
    assert response.json()["data"] == {
        "items": [
            {
                "instrument_id": str(_AAPL_ID),
                "symbol": "AAPL",
                "price": None,
                "source": None,
                "feed": None,
                "delivery_mode": "unknown",
                "as_of": None,
                "source_observed_at": None,
                "received_at": None,
                "quality": "FAIL",
                "quality_reasons": ["QUOTE_UNAVAILABLE"],
                "watchlisted": False,
            }
        ],
        "next_cursor": None,
        "selected_series": None,
        "intraday_monitor": None,
    }
    assert response.json()["meta"]["as_of"] == "2026-08-24T14:00:00Z"


@pytest.mark.asyncio
async def test_market_never_promotes_stale_fixture_to_current_price(
    migrated_database_url: str,
) -> None:
    """A stale or absent quote must remain evidence, not become a fresh synthetic value."""
    await _seed_instrument(migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL")
    await _seed_instrument(migrated_database_url, instrument_id=_MSFT_ID, symbol="MSFT")
    await _seed_quote(
        migrated_database_url,
        instrument_id=_AAPL_ID,
        last=Decimal("100.00"),
        observed_at=_DECISION_TIME - timedelta(seconds=46),
    )
    path = "/internal/v1/market"
    app = _app(migrated_database_url)

    async with _client(app) as client:
        response = await client.get(path, headers=_headers("GET", path))

    items = {item["symbol"]: item for item in response.json()["data"]["items"]}
    assert items["AAPL"]["price"] is None
    assert items["AAPL"]["source"] is None
    assert items["AAPL"]["quality"] == "FAIL"
    assert items["AAPL"]["quality_reasons"] == ["QUOTE_UNAVAILABLE"]
    assert items["MSFT"]["price"] is None
    assert items["MSFT"]["source"] is None
    assert items["MSFT"]["quality"] == "FAIL"
    assert items["MSFT"]["quality_reasons"] == ["QUOTE_UNAVAILABLE"]


@pytest.mark.asyncio
async def test_market_excludes_quote_received_after_decision_time(
    migrated_database_url: str,
) -> None:
    await _seed_instrument(migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL")
    await _seed_quote(
        migrated_database_url,
        instrument_id=_AAPL_ID,
        source="alpaca",
        feed="iex",
        delivery_mode="real_time",
        observed_at=_DECISION_TIME - timedelta(seconds=10),
        available_at=_DECISION_TIME - timedelta(seconds=5),
        received_at=_DECISION_TIME + timedelta(seconds=1),
    )
    path = "/internal/v1/market"
    app = _app(migrated_database_url)

    async with _client(app) as client:
        response = await client.get(path, headers=_headers("GET", path))

    item = response.json()["data"]["items"][0]
    assert item["price"] is None
    assert item["quality_reasons"] == ["QUOTE_UNAVAILABLE"]


@pytest.mark.asyncio
async def test_market_never_cross_checks_or_falls_back_to_legacy_sources(
    migrated_database_url: str,
) -> None:
    """Alpaca and fixture facts remain historical evidence, never a current fallback."""
    await _seed_instrument(migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL")
    await _seed_quote(
        migrated_database_url,
        instrument_id=_AAPL_ID,
        source="alpaca",
        feed="iex",
        delivery_mode="real_time",
        last=Decimal("100.00"),
    )
    await _seed_quote(
        migrated_database_url,
        instrument_id=_AAPL_ID,
        source="fixture",
        feed="fixture",
        last=Decimal("101.00"),
    )
    path = "/internal/v1/market"
    app = _app(migrated_database_url)

    async with _client(app) as client:
        response = await client.get(path, headers=_headers("GET", path))

    item = response.json()["data"]["items"][0]
    assert item["price"] is None
    assert item["source"] is None
    assert item["quality"] == "FAIL"
    assert item["quality_reasons"] == ["QUOTE_UNAVAILABLE"]


@pytest.mark.asyncio
async def test_market_does_not_compare_disabled_sources_at_any_timestamp(
    migrated_database_url: str,
) -> None:
    """Disabled sources cannot influence current selection or quality."""
    await _seed_instrument(migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL")
    await _seed_quote(
        migrated_database_url,
        instrument_id=_AAPL_ID,
        source="alpaca",
        feed="iex",
        delivery_mode="real_time",
        last=Decimal("100.00"),
        observed_at=_DECISION_TIME - timedelta(seconds=2),
    )
    await _seed_quote(
        migrated_database_url,
        instrument_id=_AAPL_ID,
        source="fixture",
        feed="fixture",
        last=Decimal("101.00"),
        observed_at=_DECISION_TIME - timedelta(seconds=8),
    )
    path = "/internal/v1/market"
    app = _app(migrated_database_url)

    async with _client(app) as client:
        response = await client.get(path, headers=_headers("GET", path))

    item = response.json()["data"]["items"][0]
    assert item["price"] is None
    assert item["quality"] == "FAIL"
    assert item["quality_reasons"] == ["QUOTE_UNAVAILABLE"]


@pytest.mark.asyncio
async def test_market_enforces_iex_quote_policy_and_rejects_delayed_sip_as_quote(
    migrated_database_url: str,
) -> None:
    """Legacy constraints remain enforced, but valid Alpaca is still not a fallback."""
    await _seed_instrument(migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL")
    await _seed_quote(
        migrated_database_url,
        instrument_id=_AAPL_ID,
        source="alpaca",
        feed="iex",
        delivery_mode="real_time",
        last=Decimal("100.00"),
        observed_at=_DECISION_TIME - timedelta(seconds=2),
    )
    with pytest.raises(IntegrityError, match="intraday_delivery_pair"):
        await _seed_quote(
            migrated_database_url,
            instrument_id=_AAPL_ID,
            source="alpaca",
            feed="sip",
            delivery_mode="delayed",
            last=Decimal("999.00"),
            observed_at=_DECISION_TIME - timedelta(seconds=1),
        )
    path = "/internal/v1/market"
    app = _app(migrated_database_url)

    async with _client(app) as client:
        response = await client.get(path, headers=_headers("GET", path))

    item = response.json()["data"]["items"][0]
    assert item["price"] is None
    assert item["feed"] is None
    assert item["delivery_mode"] == "unknown"
    assert item["quality_reasons"] == ["QUOTE_UNAVAILABLE"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("second_price", "seconds_apart"),
    [
        (Decimal("100.5000"), 5),
        (Decimal("100.5001"), 5),
        (Decimal("101.00"), 5.000001),
    ],
)
async def test_market_legacy_comparison_boundaries_cannot_enable_fallback(
    migrated_database_url: str,
    second_price: Decimal,
    seconds_apart: float,
) -> None:
    """Legacy cross-source values are ignored regardless of time or price distance."""
    await _seed_instrument(migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL")
    first_observed_at = _DECISION_TIME - timedelta(seconds=1)
    await _seed_quote(
        migrated_database_url,
        instrument_id=_AAPL_ID,
        source="alpaca",
        feed="iex",
        delivery_mode="real_time",
        last=Decimal("100.00"),
        observed_at=first_observed_at,
    )
    await _seed_quote(
        migrated_database_url,
        instrument_id=_AAPL_ID,
        last=second_price,
        observed_at=first_observed_at - timedelta(seconds=seconds_apart),
    )
    path = "/internal/v1/market"
    app = _app(migrated_database_url)

    async with _client(app) as client:
        response = await client.get(path, headers=_headers("GET", path))

    item = response.json()["data"]["items"][0]
    assert item["price"] is None
    assert item["delivery_mode"] == "unknown"
    assert item["quality"] == "FAIL"
    assert item["quality_reasons"] == ["QUOTE_UNAVAILABLE"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("admission_state", "expected_quality_reason"),
    [
        ("missing", "QUOTE_UNAVAILABLE"),
        ("valid", None),
    ],
)
async def test_fosun_quote_requires_its_own_valid_operation_admission(
    migrated_database_url: str,
    admission_state: str,
    expected_quality_reason: str | None,
) -> None:
    """A missing or wrong-operation admission cannot authorize quotes."""
    await _seed_instrument(migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL")
    if admission_state != "missing":
        await _seed_fosun_quote_v2(
            migrated_database_url,
            instrument_id=_AAPL_ID,
            last=Decimal("123.45"),
        )
    path = "/internal/v1/market"
    app = _app(migrated_database_url)
    async with _client(app) as client:
        response = await client.get(path, headers=_headers("GET", path))

    item = response.json()["data"]["items"][0]
    if expected_quality_reason is None:
        assert item["price"] == "123.450000000000"
        assert item["source"] == "fosun"
        assert item["feed"] == "l1"
        assert item["delivery_mode"] == "unknown"
        assert item["quality"] == "PARTIAL"
        assert item["quality_reasons"] == [
            "FOSUN_PROVIDER_TIMESTAMP_UNAVAILABLE",
            "INTRADAY_BID_ASK_UNAVAILABLE",
        ]
    else:
        assert item["price"] is None
        assert item["source"] is None
        assert item["quality"] == "FAIL"
        assert item["quality_reasons"] == [expected_quality_reason]


@pytest.mark.asyncio
async def test_authority_bound_fosun_push_quote_is_displayed_only(
    migrated_database_url: str,
) -> None:
    await _seed_instrument(migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL")
    received_at = _DECISION_TIME - timedelta(seconds=10)
    await _seed_fosun_quote_v2(
        migrated_database_url,
        instrument_id=_AAPL_ID,
        last=Decimal("124.56"),
        received_at=received_at,
        observed_at=received_at - timedelta(seconds=5),
        operation="fosun_subscribe_quotes",
        artifact_schema="fosun_push_quote_archive_v1",
    )
    path = "/internal/v1/market"
    app = _app(migrated_database_url)
    async with _client(app) as client:
        response = await client.get(path, headers=_headers("GET", path))

    item = response.json()["data"]["items"][0]
    assert item["price"] == "124.560000000000"
    assert item["source"] == "fosun"
    assert item["quality"] == "PARTIAL"
    assert item["quality_reasons"] == [
        "FOSUN_PROVIDER_TIMESTAMP_UNAVAILABLE",
        "INTRADAY_BID_ASK_UNAVAILABLE",
    ]
    factory = session_factory(_settings(migrated_database_url))
    try:
        async with factory() as session:
            intraday = await load_intraday_series(session, _AAPL_ID, _DECISION_TIME)
    finally:
        await factory.kw["bind"].dispose()
    assert intraday.trade is None
    assert intraday.quote is None


@pytest.mark.asyncio
async def test_fosun_quote_replays_after_its_frozen_capability_expires(
    migrated_database_url: str,
) -> None:
    """Current capability expiry must not rewrite an already-authorized quote."""
    await _seed_instrument(migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL")
    _, _, _, expiry = await _seed_fosun_quote_v2(
        migrated_database_url,
        instrument_id=_AAPL_ID,
        last=Decimal("123.45"),
    )
    path = "/internal/v1/market"
    app = _app(migrated_database_url)
    app.state.utc_clock = lambda: expiry

    async with _client(app) as client:
        response = await client.get(path, headers=_headers("GET", path))

    item = response.json()["data"]["items"][0]
    assert item["price"] == "123.450000000000"
    assert item["source"] == "fosun"


@pytest.mark.asyncio
async def test_fosun_quote_replay_ignores_later_status_transition(
    migrated_database_url: str,
) -> None:
    """A later current-mode change must not rewrite a frozen quote's past verdict."""
    await _seed_instrument(migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL")
    _, status_id, status_version, _ = await _seed_fosun_quote_v2(
        migrated_database_url,
        instrument_id=_AAPL_ID,
        last=Decimal("123.45"),
    )
    path = "/internal/v1/market"
    app = _app(migrated_database_url)
    async with _client(app) as client:
        before = await client.get(path, headers=_headers("GET", path))

    factory = session_factory(_settings(migrated_database_url))
    try:
        async with factory() as session:
            await session.execute(
                text(
                    "UPDATE integration_status "
                    "SET status='disabled', version=:next_version, "
                    "mode_changed_at=:changed_at, capability_invalidated_at=:changed_at, "
                    "admitted_contract_hash=NULL "
                    "WHERE id=:status_id"
                ),
                {
                    "next_version": status_version + 1,
                    "changed_at": _DECISION_TIME + timedelta(seconds=1),
                    "status_id": status_id,
                },
            )
            await session.commit()
    finally:
        await factory.kw["bind"].dispose()

    async with _client(app) as client:
        replay = await client.get(path, headers=_headers("GET", path))

    assert replay.status_code == before.status_code == 200
    assert replay.json() == before.json()


@pytest.mark.asyncio
async def test_fosun_quote_uses_matching_older_authority_not_newest_bars_authority(
    migrated_database_url: str,
) -> None:
    """A newer bars capability must neither hide nor grant quote authority."""
    await _seed_instrument(migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL")
    await _seed_fosun_quote_v2(
        migrated_database_url,
        instrument_id=_AAPL_ID,
        last=Decimal("123.45"),
    )
    await _seed_fosun_admission(
        migrated_database_url,
        operation="fosun_get_bars",
        contract_hash="b" * 64,
        created_at=_DECISION_TIME - timedelta(hours=1),
    )
    path = "/internal/v1/market"
    app = _app(migrated_database_url)

    async with _client(app) as client:
        response = await client.get(path, headers=_headers("GET", path))

    item = response.json()["data"]["items"][0]
    assert item["price"] == "123.450000000000"
    assert item["source"] == "fosun"
    assert item["quality"] == "PARTIAL"


@pytest.mark.asyncio
async def test_market_cursor_is_stable_and_invalid_or_foreign_values_fail_closed(
    migrated_database_url: str,
) -> None:
    """An unstable order or permissive cursor decoder causes duplicates or cross-route reuse."""
    for instrument_id, symbol in (
        (_AAPL_ID, "AAPL"),
        (_MSFT_ID, "MSFT"),
        (_NVDA_ID, "NVDA"),
    ):
        await _seed_instrument(migrated_database_url, instrument_id=instrument_id, symbol=symbol)
    app = _app(migrated_database_url)

    async with _client(app) as client:
        first_path = "/internal/v1/market?limit=2"
        first = await client.get(first_path, headers=_headers("GET", first_path))
        cursor = first.json()["data"]["next_cursor"]
        assert isinstance(cursor, str) and cursor
        assert "MSFT" not in cursor
        app.state.utc_clock = lambda: _DECISION_TIME + timedelta(minutes=1)
        second_query = urlencode({"limit": 2, "cursor": cursor})
        second_path = f"/internal/v1/market?{second_query}"
        second = await client.get(second_path, headers=_headers("GET", second_path))
        malformed_path = "/internal/v1/market?cursor=not-a-market-cursor"
        malformed = await client.get(malformed_path, headers=_headers("GET", malformed_path))
        foreign_path = "/internal/v1/market?cursor=eyJzY29wZSI6Im90aGVyIn0"
        foreign = await client.get(foreign_path, headers=_headers("GET", foreign_path))
        cross_filter_query = urlencode({"limit": 2, "cursor": cursor, "watchlist": "true"})
        cross_filter_path = f"/internal/v1/market?{cross_filter_query}"
        cross_filter = await client.get(
            cross_filter_path, headers=_headers("GET", cross_filter_path)
        )

    assert [item["symbol"] for item in first.json()["data"]["items"]] == [
        "AAPL",
        "MSFT",
    ]
    assert [item["symbol"] for item in second.json()["data"]["items"]] == ["NVDA"]
    assert second.json()["data"]["next_cursor"] is None
    assert second.json()["meta"]["as_of"] == "2026-08-24T14:00:00Z"
    for rejected in (malformed, foreign, cross_filter):
        assert rejected.status_code == 422
        assert rejected.json()["error"]["code"] == "VALIDATION_ERROR"


@pytest.mark.asyncio
async def test_market_and_watchlist_are_bound_to_published_us_liquid_v1_members(
    migrated_database_url: str,
) -> None:
    """Aliases and even an accidental HK member cannot expand the Phase 0-1 universe."""
    await _seed_instrument(migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL")
    await _seed_instrument(
        migrated_database_url,
        instrument_id=_MSFT_ID,
        symbol="MSFT",
        add_to_universe=False,
    )
    hk_id = uuid4()
    await _seed_instrument(
        migrated_database_url,
        instrument_id=hk_id,
        symbol="00656",
        exchange="XHKG",
        currency="HKD",
    )
    app = _app(migrated_database_url)
    market_path = "/internal/v1/market"
    nonmember_path = f"/internal/v1/watchlist/{_MSFT_ID}"
    hk_path = f"/internal/v1/watchlist/{hk_id}"
    selected_nonmember_path = f"/internal/v1/market?selected_instrument_id={_MSFT_ID}"

    async with _client(app) as client:
        market = await client.get(market_path, headers=_headers("GET", market_path))
        nonmember = await client.put(nonmember_path, headers=_headers("PUT", nonmember_path))
        hk = await client.put(hk_path, headers=_headers("PUT", hk_path))
        selected_nonmember = await client.get(
            selected_nonmember_path,
            headers=_headers("GET", selected_nonmember_path),
        )

    assert [item["symbol"] for item in market.json()["data"]["items"]] == ["AAPL"]
    assert nonmember.status_code == hk.status_code == selected_nonmember.status_code == 404
    assert nonmember.json()["error"]["code"] == "NOT_FOUND"
    assert hk.json()["error"]["code"] == "NOT_FOUND"


@pytest.mark.asyncio
async def test_market_prefers_latest_local_fosun_holdings_universe(
    migrated_database_url: str,
) -> None:
    await _seed_instrument(
        migrated_database_url,
        instrument_id=_AAPL_ID,
        symbol="AAPL",
        add_to_universe=False,
    )
    await _seed_instrument(
        migrated_database_url,
        instrument_id=_MSFT_ID,
        symbol="MSFT",
        add_to_universe=False,
    )
    await _seed_local_fosun_universe(
        migrated_database_url,
        instrument_ids=(_AAPL_ID, _MSFT_ID),
    )
    admission = await _seed_fosun_quote_v2(
        migrated_database_url,
        instrument_id=_AAPL_ID,
    )
    await _seed_fosun_quote_v2(
        migrated_database_url,
        instrument_id=_MSFT_ID,
        admission=admission,
    )
    app = _app(migrated_database_url)
    path = "/internal/v1/market"

    async with _client(app, publish_universe=False) as client:
        market = await client.get(path, headers=_headers("GET", path))

    assert market.status_code == 200
    assert [item["symbol"] for item in market.json()["data"]["items"]] == [
        "AAPL",
        "MSFT",
    ]
    assert {item["source"] for item in market.json()["data"]["items"]} == {"fosun"}


@pytest.mark.asyncio
async def test_market_fails_closed_until_us_liquid_v1_is_published(
    migrated_database_url: str,
) -> None:
    """A draft membership is not decision-time authority."""
    await _seed_instrument(migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL")
    path = "/internal/v1/market"
    app = _app(migrated_database_url)

    async with _client(app, publish_universe=False) as client:
        market = await client.get(path, headers=_headers("GET", path))

    assert market.status_code == 200
    assert market.json()["data"]["items"] == []


@pytest.mark.asyncio
async def test_market_excludes_future_effective_us_liquid_v1(
    migrated_database_url: str,
) -> None:
    """Publication does not make a future-effective snapshot visible early."""
    await _seed_instrument(
        migrated_database_url,
        instrument_id=_AAPL_ID,
        symbol="AAPL",
        universe_effective_from=_DECISION_TIME.date() + timedelta(days=1),
    )
    path = "/internal/v1/market"
    app = _app(migrated_database_url)

    async with _client(app) as client:
        market = await client.get(path, headers=_headers("GET", path))

    assert market.status_code == 200
    assert market.json()["data"]["items"] == []


@pytest.mark.asyncio
async def test_market_excludes_us_liquid_v1_published_after_decision_time(
    migrated_database_url: str,
) -> None:
    """A future publication timestamp must not leak a universe into this decision time."""
    await _seed_instrument(migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL")
    factory = session_factory(_settings(migrated_database_url))
    try:
        async with factory() as session:
            await session.execute(
                text(
                    "UPDATE universe_snapshots SET published_at = :published_at "
                    "WHERE id = :snapshot_id"
                ),
                {
                    "published_at": _DECISION_TIME + timedelta(microseconds=1),
                    "snapshot_id": _UNIVERSE_ID,
                },
            )
            await session.commit()
    finally:
        await factory.kw["bind"].dispose()
    path = "/internal/v1/market"
    app = _app(migrated_database_url)

    async with _client(app, publish_universe=False) as client:
        market = await client.get(path, headers=_headers("GET", path))

    assert market.status_code == 200
    assert market.json()["data"]["items"] == []


@pytest.mark.asyncio
async def test_selected_series_uses_repository_authority_and_excludes_future_bars(
    migrated_database_url: str,
) -> None:
    """Bypassing Task 7 would expose a future revision through the chart payload."""
    await _seed_instrument(migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL")
    await _seed_fosun_admission(
        migrated_database_url,
        operation="fosun_get_bars",
        contract_hash="a" * 64,
        delivery_mode="real_time",
    )
    visible = PriceBar(
        instrument_id=InstrumentId(_AAPL_ID),
        session=date(2026, 8, 21),
        open="100.00",
        high="102.00",
        low="99.00",
        close="101.00",
        volume="1000",
        available_at=_DECISION_TIME - timedelta(days=1),
        revision="1",
    )
    future = visible.model_copy(
        update={
            "close": Decimal("999.00"),
            "available_at": _DECISION_TIME + timedelta(microseconds=1),
            "revision": "2",
        }
    )

    async def offline_loader(
        instrument_id: UUID, decision_time: datetime
    ) -> tuple[PriceSeries, ...]:
        assert instrument_id == _AAPL_ID
        assert decision_time == _DECISION_TIME
        return (
            PriceSeries(
                source="fosun",
                feed="l1",
                adjustment="raw",
                bars=(future, visible),
                mode="read_only",
                delivery_mode="real_time",
                contract_hash="a" * 64,
                quality="PASS",
            ),
        )

    selected_path = f"/internal/v1/market?selected_instrument_id={_AAPL_ID}"
    app = _app(migrated_database_url)
    app.state.market_price_series_loader = offline_loader
    async with _client(app) as client:
        response = await client.get(selected_path, headers=_headers("GET", selected_path))

    series = response.json()["data"]["selected_series"]
    assert series["source"] == "fosun"
    assert series["feed"] == "l1"
    assert series["delivery_mode"] == "real_time"
    assert series["quality"] == "PARTIAL"
    assert series["quality_reasons"] == ["FUTURE_DATA_EXCLUDED"]
    assert [bar["close"] for bar in series["bars"]] == ["101.00"]


@pytest.mark.asyncio
async def test_selected_series_missing_seam_returns_explicit_no_history(
    migrated_database_url: str,
) -> None:
    """A missing offline seam must not be filled from the latest quote or a network call."""
    await _seed_instrument(migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL")
    await _seed_quote(
        migrated_database_url,
        instrument_id=_AAPL_ID,
        last=Decimal("123.45"),
    )
    path = f"/internal/v1/market?selected_instrument_id={_AAPL_ID}"
    app = _app(migrated_database_url)

    async with _client(app) as client:
        response = await client.get(path, headers=_headers("GET", path))

    selected = response.json()["data"]["selected_series"]
    assert selected == {
        "instrument_id": str(_AAPL_ID),
        "source": None,
        "feed": None,
        "delivery_mode": None,
        "adjustment": None,
        "interval": None,
        "quality": "FAIL",
        "quality_reasons": ["NO_HISTORY_DATA"],
        "bars": [],
    }


@pytest.mark.asyncio
async def test_selected_instrument_returns_strict_intraday_monitor_without_network(
    migrated_database_url: str,
) -> None:
    await _seed_instrument(migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL")
    observed_at = _DECISION_TIME - timedelta(minutes=1)
    bar = PriceBar(
        instrument_id=InstrumentId(_AAPL_ID),
        session=_DECISION_TIME.date(),
        open="102.90",
        high="103.10",
        low="102.80",
        close="103.00",
        volume="1200",
        observed_at=observed_at,
        available_at=_DECISION_TIME - timedelta(seconds=15),
        received_at=_DECISION_TIME - timedelta(seconds=14),
        revision="1",
    )

    async def offline_intraday_loader(
        instrument_id: UUID,
        decision_time: datetime,
    ) -> IntradayAuthorityView:
        assert instrument_id == _AAPL_ID
        assert decision_time == _DECISION_TIME
        return IntradayAuthorityView(
            instrument_id=_AAPL_ID,
            source="alpaca",
            feed="iex",
            delivery_mode="real_time",
            bars=(bar,),
            trade=IntradayTradeView(
                last="103.00",
                observed_at=_DECISION_TIME - timedelta(seconds=10),
                available_at=_DECISION_TIME - timedelta(seconds=9),
                quality_flags=(),
            ),
            quote=IntradayQuoteView(
                bid="102.98",
                ask="103.02",
                observed_at=_DECISION_TIME - timedelta(seconds=9),
                available_at=_DECISION_TIME - timedelta(seconds=8),
                quality_flags=(),
            ),
        )

    path = f"/internal/v1/market?selected_instrument_id={_AAPL_ID}"
    app = _app(migrated_database_url)
    app.state.market_intraday_series_loader = offline_intraday_loader
    async with _client(app) as client:
        response = await client.get(path, headers=_headers("GET", path))

    assert response.status_code == 200
    monitor = response.json()["data"]["intraday_monitor"]
    assert monitor["status"] == "partial"
    assert monitor["source"] == "alpaca"
    assert monitor["last"] == "103.00"
    assert monitor["spread_bps"] == "3.883495145631"
    assert monitor["return_5m"] is None
    assert monitor["sample_count"] == 1
    assert len(monitor["bars"]) == 1


@pytest.mark.asyncio
async def test_selected_series_rejects_one_minute_only_history_explicitly(
    migrated_database_url: str,
) -> None:
    """Task 8 must not label intraday dates as a daily OHLCV chart."""
    await _seed_instrument(migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL")
    bar = PriceBar(
        instrument_id=InstrumentId(_AAPL_ID),
        session=date(2026, 8, 21),
        open="100",
        high="101",
        low="99",
        close="100",
        volume="1000",
        available_at=_DECISION_TIME - timedelta(days=1),
        revision="1",
    )

    async def offline_loader(
        _instrument_id: UUID, _decision_time: datetime
    ) -> tuple[PriceSeries, ...]:
        return (
            PriceSeries(
                source="fixture",
                feed="fixture",
                adjustment="split",
                interval="1Min",
                bars=(bar,),
                mode="fixture",
                delivery_mode="fixture",
                quality="PASS",
            ),
        )

    path = f"/internal/v1/market?selected_instrument_id={_AAPL_ID}"
    app = _app(migrated_database_url)
    app.state.market_price_series_loader = offline_loader
    async with _client(app) as client:
        response = await client.get(path, headers=_headers("GET", path))

    selected = response.json()["data"]["selected_series"]
    assert selected["interval"] is None
    assert selected["bars"] == []
    assert selected["quality"] == "FAIL"
    assert selected["quality_reasons"] == ["NO_DAILY_HISTORY_DATA"]


@pytest.mark.asyncio
async def test_selected_series_uses_only_daily_candidate_from_mixed_intervals(
    migrated_database_url: str,
) -> None:
    """A higher-priority 1Min series cannot displace Task 8 daily evidence."""
    await _seed_instrument(migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL")
    await _seed_fosun_admission(
        migrated_database_url,
        operation="fosun_get_bars",
        contract_hash="a" * 64,
        delivery_mode="real_time",
    )
    bar = PriceBar(
        instrument_id=InstrumentId(_AAPL_ID),
        session=date(2026, 8, 21),
        open="100",
        high="101",
        low="99",
        close="100",
        volume="1000",
        available_at=_DECISION_TIME - timedelta(days=1),
        revision="1",
    )

    async def offline_loader(
        _instrument_id: UUID, _decision_time: datetime
    ) -> tuple[PriceSeries, ...]:
        return (
            PriceSeries(
                source="alpaca",
                feed="sip",
                adjustment="split",
                interval="1Min",
                bars=(bar,),
                mode="fixture",
                delivery_mode="fixture",
                quality="PASS",
            ),
            PriceSeries(
                source="fosun",
                feed="l1",
                adjustment="raw",
                interval="1Day",
                bars=(bar,),
                mode="read_only",
                delivery_mode="real_time",
                contract_hash="a" * 64,
                quality="PASS",
            ),
        )

    path = f"/internal/v1/market?selected_instrument_id={_AAPL_ID}"
    app = _app(migrated_database_url)
    app.state.market_price_series_loader = offline_loader
    async with _client(app) as client:
        response = await client.get(path, headers=_headers("GET", path))

    selected = response.json()["data"]["selected_series"]
    assert selected["source"] == "fosun"
    assert selected["interval"] == "1Day"
    assert len(selected["bars"]) == 1


@pytest.mark.asyncio
async def test_selected_series_is_deterministically_bounded_to_latest_500_bars(
    migrated_database_url: str,
) -> None:
    """Removing the response cap would make one chart request return unbounded history."""
    await _seed_instrument(migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL")
    await _seed_fosun_admission(
        migrated_database_url,
        operation="fosun_get_bars",
        contract_hash="a" * 64,
        delivery_mode="real_time",
    )
    first_session = date(2025, 1, 1)
    bars = tuple(
        PriceBar(
            instrument_id=InstrumentId(_AAPL_ID),
            session=first_session + timedelta(days=index),
            open=str(100 + index),
            high=str(101 + index),
            low=str(99 + index),
            close=str(100 + index),
            volume=str(1000 + index),
            available_at=datetime(2025, 1, 1, tzinfo=UTC) + timedelta(days=index),
            revision="1",
        )
        for index in range(501)
    )

    async def offline_loader(
        _instrument_id: UUID, _decision_time: datetime
    ) -> tuple[PriceSeries, ...]:
        return (
            PriceSeries(
                source="fosun",
                feed="l1",
                adjustment="raw",
                bars=bars,
                mode="read_only",
                delivery_mode="real_time",
                contract_hash="a" * 64,
                quality="PASS",
            ),
        )

    path = f"/internal/v1/market?selected_instrument_id={_AAPL_ID}"
    app = _app(migrated_database_url)
    app.state.market_price_series_loader = offline_loader
    async with _client(app) as client:
        response = await client.get(path, headers=_headers("GET", path))

    selected = response.json()["data"]["selected_series"]
    assert len(selected["bars"]) == 500
    assert selected["bars"][0]["session"] == "2025-01-02"
    assert selected["bars"][-1]["session"] == "2026-05-16"
    assert selected["quality"] == "PARTIAL"
    assert selected["quality_reasons"] == ["HISTORY_TRUNCATED"]


@pytest.mark.asyncio
async def test_selected_series_unknown_instrument_is_not_found(
    migrated_database_url: str,
) -> None:
    """Returning an empty chart for an unknown identifier would hide a caller bug."""
    unknown_id = uuid4()
    path = f"/internal/v1/market?selected_instrument_id={unknown_id}"
    app = _app(migrated_database_url)
    async with _client(app) as client:
        response = await client.get(path, headers=_headers("GET", path))

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "NOT_FOUND"
    assert response.json()["error"]["details"] == {"instrument_id": str(unknown_id)}


@pytest.mark.asyncio
async def test_watchlist_put_and_delete_have_idempotent_set_semantics(
    migrated_database_url: str,
) -> None:
    """Insert/delete row-count semantics must not leak through the final representation."""
    await _seed_instrument(migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL")
    path = f"/internal/v1/watchlist/{_AAPL_ID}"
    app = _app(migrated_database_url)

    async with _client(app) as client:
        first_put = await client.put(path, headers=_headers("PUT", path))
        duplicate_put = await client.put(path, headers=_headers("PUT", path))
        market_path = "/internal/v1/market"
        listed_present = await client.get(market_path, headers=_headers("GET", market_path))
        first_delete = await client.delete(path, headers=_headers("DELETE", path))
        absent_delete = await client.delete(path, headers=_headers("DELETE", path))
        listed_absent = await client.get(market_path, headers=_headers("GET", market_path))

    expected_true = {"instrument_id": str(_AAPL_ID), "watchlisted": True}
    expected_false = {"instrument_id": str(_AAPL_ID), "watchlisted": False}
    assert first_put.status_code == duplicate_put.status_code == 200
    assert first_put.json()["data"] == duplicate_put.json()["data"] == expected_true
    assert listed_present.json()["data"]["items"][0]["watchlisted"] is True
    assert first_delete.status_code == absent_delete.status_code == 200
    assert first_delete.json()["data"] == absent_delete.json()["data"] == expected_false
    assert listed_absent.json()["data"]["items"][0]["watchlisted"] is False


@pytest.mark.asyncio
async def test_watchlist_unknown_instrument_is_not_found(
    migrated_database_url: str,
) -> None:
    """A foreign key failure must be normalized instead of creating an implicit instrument."""
    unknown_id = uuid4()
    path = f"/internal/v1/watchlist/{unknown_id}"
    app = _app(migrated_database_url)
    async with _client(app) as client:
        response = await client.put(path, headers=_headers("PUT", path))

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "NOT_FOUND"
    assert response.json()["error"]["details"] == {"instrument_id": str(unknown_id)}


@pytest.mark.asyncio
async def test_watchlist_fails_closed_when_default_name_has_a_foreign_id(
    migrated_database_url: str,
) -> None:
    """A conflicting name must not silently redirect membership to another list."""
    await _seed_instrument(migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL")
    factory = session_factory(_settings(migrated_database_url))
    try:
        async with factory() as session:
            session.add(Watchlist(id=uuid4(), name="default_private"))
            await session.commit()
    finally:
        await factory.kw["bind"].dispose()
    path = f"/internal/v1/watchlist/{_AAPL_ID}"
    app = _app(migrated_database_url)

    async with _client(app) as client:
        response = await client.put(path, headers=_headers("PUT", path))

    assert response.status_code == 500
    assert response.json()["error"]["code"] == "INTERNAL_ERROR"
    factory = session_factory(_settings(migrated_database_url))
    try:
        async with factory() as session:
            count = await session.scalar(select(func.count()).select_from(WatchlistMember))
    finally:
        await factory.kw["bind"].dispose()
    assert count == 0


@pytest.mark.asyncio
async def test_market_watchlist_boolean_filter_is_applied_in_repository_query(
    migrated_database_url: str,
) -> None:
    """A UI-only filter would leak unfiltered pagination and inconsistent counts."""
    await _seed_instrument(migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL")
    await _seed_instrument(migrated_database_url, instrument_id=_MSFT_ID, symbol="MSFT")
    membership_path = f"/internal/v1/watchlist/{_AAPL_ID}"
    app = _app(migrated_database_url)

    async with _client(app) as client:
        added = await client.put(membership_path, headers=_headers("PUT", membership_path))
        true_path = "/internal/v1/market?watchlist=true"
        listed_true = await client.get(true_path, headers=_headers("GET", true_path))
        false_path = "/internal/v1/market?watchlist=false"
        listed_false = await client.get(false_path, headers=_headers("GET", false_path))

    assert added.status_code == 200
    assert [item["symbol"] for item in listed_true.json()["data"]["items"]] == ["AAPL"]
    assert [item["symbol"] for item in listed_false.json()["data"]["items"]] == ["MSFT"]


@pytest.mark.asyncio
async def test_concurrent_duplicate_watchlist_puts_leave_one_membership(
    migrated_database_url: str,
) -> None:
    """Removing PostgreSQL conflict authority would allow duplicate membership or a 500."""
    await _seed_instrument(migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL")
    path = f"/internal/v1/watchlist/{_AAPL_ID}"
    app = _app(migrated_database_url)

    async with _client(app) as client:
        responses = await asyncio.wait_for(
            asyncio.gather(
                *(
                    client.put(path, headers=_headers("PUT", path))
                    for _ in range(8)
                ),
            ),
            timeout=_CONCURRENT_REQUEST_TIMEOUT_SECONDS,
        )

    assert [response.status_code for response in responses] == [200] * 8
    assert all(response.json()["data"]["watchlisted"] for response in responses)
    factory = session_factory(_settings(migrated_database_url))
    try:
        async with factory() as session:
            count = await session.scalar(
                select(func.count())
                .select_from(WatchlistMember)
                .where(WatchlistMember.instrument_id == _AAPL_ID)
            )
            watchlist_count = await session.scalar(select(func.count()).select_from(Watchlist))
    finally:
        await factory.kw["bind"].dispose()
    assert count == 1
    assert watchlist_count == 1
