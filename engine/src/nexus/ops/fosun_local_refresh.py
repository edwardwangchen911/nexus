"""One-shot Mac-local ingestion of the five reviewed Fosun read surfaces."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import sys
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Literal, Self
from uuid import UUID, uuid5

import exchange_calendars as xcals  # type: ignore[import-untyped]
from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from nexus.domain.ids import new_uuid7
from nexus.jobs.market_authority import ProviderMarketBinding
from nexus.market.ingestion import archive_fosun_price_page_raw, ingest_price_page
from nexus.market.intraday_ingestion import canonical_quote_observation
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.data import DataArtifact, ProviderCapability
from nexus.persistence.models.market import (
    Instrument,
    InstrumentAlias,
    LatestQuote,
    UniverseMember,
    UniverseSnapshot,
)
from nexus.persistence.models.portfolio import (
    Account,
    BrokerSyncRun,
    CashSnapshot,
    Portfolio,
    PortfolioSnapshotRecord,
    PositionSnapshot,
)
from nexus.persistence.models.runtime import IntegrationStatus
from nexus.providers.base import BarsRequest, InstrumentId
from nexus.providers.broker.base import BrokerPosition
from nexus.providers.broker.fosun_contracts import AccountItem, CashItem, PositionItem
from nexus.providers.broker.gateway_client import FosunGatewayClient
from nexus.providers.broker.gateway_contracts import GatewayClientError, GatewayResponsePage
from nexus.providers.broker.settings import BrokerWorkerSettings
from nexus.providers.market.fosun import (
    FosunMarketDataProvider,
    encode_fosun_snapshot_evidence,
)
from nexus.providers.market.fosun_gateway import (
    FosunMarketGatewayClient,
    FosunMarketGatewayIdentity,
    RawGatewayResponse,
    parse_bar_page,
    parse_quote_page,
)
from nexus.reconciliation.facts import (
    ORDER_HASH_VERSION,
    POSITION_HASH_VERSION,
    canonical_order_hash,
    canonical_position_hash,
)

_LOCAL_CONTRACT_HASH = hashlib.sha256(b"nexus.mac-local-fosun-read-only.v1").hexdigest()
_INSTRUMENT_NAMESPACE = UUID("639bd8e1-d250-4f58-a88c-370b6bbc621b")
_UNIVERSE_ID = UUID("40cd56b4-c849-5ea7-b463-19699231f521")
_LOCAL_RUNTIME_HOURS = 12


class LocalFosunRefreshSettings(BaseSettings):
    """Only DB, object-store and two Gateway client identities enter this worker."""

    model_config = SettingsConfigDict(
        env_file=None,
        env_ignore_empty=True,
        extra="ignore",
        hide_input_in_errors=True,
    )

    database_url: str = Field(validation_alias="DATABASE_URL")
    object_store_endpoint: str = Field(validation_alias="OBJECT_STORE_ENDPOINT")
    object_store_region: str = Field(validation_alias="OBJECT_STORE_REGION")
    object_store_bucket: str = Field(validation_alias="OBJECT_STORE_BUCKET")
    object_store_access_key_id: SecretStr = Field(validation_alias="OBJECT_STORE_ACCESS_KEY_ID")
    object_store_secret_access_key: SecretStr = Field(
        validation_alias="OBJECT_STORE_SECRET_ACCESS_KEY"
    )
    object_store_prefix: Literal["provider/market"] = "provider/market"
    broker_gateway_origin: str = Field(validation_alias="FOSUN_GATEWAY_BASE_URL")
    broker_gateway_key_id: str = Field(validation_alias="FOSUN_GATEWAY_HMAC_CURRENT_KEY_ID")
    broker_gateway_secret: SecretStr = Field(validation_alias="FOSUN_GATEWAY_HMAC_CURRENT_SECRET")
    broker_client_cert: Path = Field(validation_alias="FOSUN_GATEWAY_CLIENT_CERT")
    broker_client_key: Path = Field(validation_alias="FOSUN_GATEWAY_CLIENT_KEY")
    broker_client_ca: Path = Field(validation_alias="FOSUN_GATEWAY_CLIENT_CA")
    market_gateway_origin: str = Field(validation_alias="FOSUN_MARKET_GATEWAY_BASE_URL")
    market_gateway_key_id: str = Field(validation_alias="FOSUN_MARKET_GATEWAY_HMAC_CURRENT_KEY_ID")
    market_gateway_secret: SecretStr = Field(
        validation_alias="FOSUN_MARKET_GATEWAY_HMAC_CURRENT_SECRET"
    )
    market_client_cert: Path = Field(validation_alias="FOSUN_MARKET_GATEWAY_CLIENT_CERT")
    market_client_key: Path = Field(validation_alias="FOSUN_MARKET_GATEWAY_CLIENT_KEY")
    market_client_ca: Path = Field(validation_alias="FOSUN_MARKET_GATEWAY_CLIENT_CA")
    market_delivery_mode: Literal["unknown"] = Field(
        default="unknown",
        validation_alias="FOSUN_MARKET_DELIVERY_MODE",
    )
    market_symbol: str = Field(default="usAAPL", validation_alias="FOSUN_LOCAL_SYMBOL")

    @field_validator("market_symbol")
    @classmethod
    def _market_symbol_is_us_code(cls, value: str) -> str:
        symbol = value.removeprefix("us")
        if not value.startswith("us") or not symbol or not symbol.replace(".", "").isalnum():
            raise ValueError("local market symbol is invalid")
        return value

    @model_validator(mode="after")
    def _identities_are_distinct_and_local(self) -> Self:
        if (
            self.broker_gateway_key_id == self.market_gateway_key_id
            or self.broker_gateway_secret.get_secret_value()
            == self.market_gateway_secret.get_secret_value()
        ):
            raise ValueError("local Gateway identities must remain distinct")
        for path in (
            self.broker_client_cert,
            self.broker_client_key,
            self.broker_client_ca,
            self.market_client_cert,
            self.market_client_key,
            self.market_client_ca,
        ):
            if not path.is_absolute() or not path.is_file():
                raise ValueError("local Gateway TLS identity is unavailable")
        return self

    def __repr_args__(self) -> list[tuple[str | None, object]]:
        return []


async def refresh_local_fosun(
    settings: LocalFosunRefreshSettings,
    *,
    now: datetime | None = None,
) -> dict[str, object]:
    """Read exactly five business operations, then persist one display-only snapshot."""
    checked_at = (now or datetime.now(UTC)).astimezone(UTC)
    request_start_at, request_end_at, calendar_rows = _daily_window(checked_at)
    expected_sessions = tuple(
        row[0] for row in calendar_rows if row[0] <= request_end_at.date()
    )
    bar_limit = len(expected_sessions)
    if not 1 <= bar_limit <= 500:
        raise ValueError("LOCAL_KLINE_BUDGET_INVALID")
    broker = FosunGatewayClient(_broker_settings(settings))
    market = FosunMarketGatewayClient(_market_identity(settings))
    try:
        accounts_page = await broker.read_accounts()
        try:
            accounts = tuple(AccountItem.model_validate(item) for item in accounts_page.items)
        except Exception:
            raise RuntimeError("LOCAL_ACCOUNT_CONTRACT_INVALID") from None
        if not accounts or accounts_page.has_more:
            raise ValueError("LOCAL_ACCOUNT_SCOPE_AMBIGUOUS")
        external_account_id = accounts[0].external_account_id
        cash_page = await broker.read_cash(external_account_id)
        try:
            cash = tuple(CashItem.model_validate(item) for item in cash_page.items)
        except Exception:
            raise RuntimeError("LOCAL_CASH_CONTRACT_INVALID") from None
        if not cash or cash_page.has_more:
            raise ValueError("LOCAL_CASH_SCOPE_INVALID")
        holdings_page = await broker.read_holdings(
            external_account_id,
            cursor=None,
            limit=100,
        )
        try:
            holdings = tuple(PositionItem.model_validate(item) for item in holdings_page.items)
        except Exception:
            raise RuntimeError("LOCAL_HOLDINGS_CONTRACT_INVALID") from None
        if holdings_page.has_more:
            raise ValueError("LOCAL_HOLDINGS_BUDGET_EXCEEDED")
        if any(item.external_account_id != external_account_id for item in cash) or any(
            item.external_account_id != external_account_id for item in holdings
        ):
            raise ValueError("LOCAL_ACCOUNT_SCOPE_MISMATCH")
        quote_raw = await market.read_quotes_raw((settings.market_symbol,))
        try:
            quote_page = parse_quote_page(quote_raw, expected_codes=(settings.market_symbol,))
        except Exception:
            raise RuntimeError("LOCAL_QUOTE_CONTRACT_INVALID") from None
        if len(quote_page.items) != 1 or quote_page.items[0].price is None:
            raise ValueError("LOCAL_QUOTE_UNAVAILABLE")
        bars_raw = await market.read_bars_raw(
            code=settings.market_symbol,
            ktype="day",
            start_time=request_start_at,
            end_time=request_end_at,
            limit=bar_limit,
        )
        try:
            bar_page = parse_bar_page(
                bars_raw,
                expected_code=settings.market_symbol,
                expected_ktype="day",
                start=request_start_at,
                end=request_end_at,
                limit=bar_limit,
            )
        except Exception:
            raise RuntimeError("LOCAL_KLINE_CONTRACT_INVALID") from None
        actual_sessions = tuple(item.timestamp.date() for item in bar_page.items)
        if not bar_page.items or actual_sessions != expected_sessions:
            raise ValueError("LOCAL_KLINE_WINDOW_INCOMPLETE")
    finally:
        await broker.aclose()
        await market.aclose()

    engine = create_async_engine(settings.database_url, pool_pre_ping=True)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    store = S3ProviderEvidenceStore(settings)
    try:
        try:
            async with sessions() as session, session.begin():
                result = await _persist_refresh(
                    session,
                    store,
                    settings=settings,
                    accounts_page=accounts_page,
                    cash_page=cash_page,
                    cash=cash,
                    holdings_page=holdings_page,
                    holdings=holdings,
                    quote_raw=quote_raw,
                    bars_raw=bars_raw,
                    request_start_at=request_start_at,
                    request_end_at=request_end_at,
                    calendar_rows=calendar_rows,
                    authorized_at=datetime.now(UTC),
                )
        except Exception as error:
            if isinstance(error, GatewayClientError):
                raise
            raise RuntimeError("LOCAL_PERSISTENCE_FAILED") from None
        return result
    finally:
        await engine.dispose()


async def _persist_refresh(
    session: AsyncSession,
    store: S3ProviderEvidenceStore,
    *,
    settings: LocalFosunRefreshSettings,
    accounts_page: GatewayResponsePage,
    cash_page: GatewayResponsePage,
    cash: tuple[CashItem, ...],
    holdings_page: GatewayResponsePage,
    holdings: tuple[PositionItem, ...],
    quote_raw: RawGatewayResponse,
    bars_raw: RawGatewayResponse,
    request_start_at: datetime,
    request_end_at: datetime,
    calendar_rows: tuple[tuple[date, datetime, datetime], ...],
    authorized_at: datetime,
) -> dict[str, object]:
    account_page_as_of = accounts_page.as_of
    cash_page_as_of = cash_page.as_of
    holdings_page_as_of = holdings_page.as_of
    cash_request_id = cash_page.source_request_id
    holdings_request_id = holdings_page.source_request_id
    instrument_ids: dict[str, UUID] = {}
    for item in holdings:
        instrument_ids[item.instrument_code] = await _ensure_instrument(
            session,
            item.instrument_code,
            currency=item.currency,
            valid_from=request_start_at.date(),
        )
    market_instrument_id = await _ensure_instrument(
        session,
        settings.market_symbol,
        currency="USD",
        valid_from=request_start_at.date(),
    )
    await _ensure_market_universe(
        session,
        instrument_id=market_instrument_id,
        published_at=min(account_page_as_of, quote_raw.received_at),
        effective_from=request_start_at.date(),
    )
    account = await session.scalar(
        select(Account).where(Account.provider == "fosun").order_by(Account.id).limit(2)
    )
    if account is None:
        account = Account(provider="fosun", status="ACTIVE")
        session.add(account)
        await session.flush()
    portfolio = await session.scalar(select(Portfolio).where(Portfolio.account_id == account.id))
    if portfolio is None:
        portfolio = Portfolio(account_id=account.id, name="复星只读账户")
        session.add(portfolio)
        await session.flush()
    positions = tuple(
        BrokerPosition(
            instrument_id=instrument_ids[item.instrument_code],
            currency=item.currency,
            quantity=item.quantity,
            available_quantity=item.available_quantity,
            frozen_quantity=item.frozen_quantity,
            avg_cost=item.average_cost,
            diluted_cost=item.diluted_cost,
            current_price=item.current_price,
            provider_as_of=item.provider_as_of,
        )
        for item in holdings
    )
    sync_run = BrokerSyncRun(
        account_id=account.id,
        portfolio_id=portfolio.id,
        trigger_kind="AUTOMATIC",
        actor_id="nexus-mac-local-read-only",
        route="local-fosun-refresh",
        idempotency_key=str(new_uuid7()),
        request_hash=hashlib.sha256(str(authorized_at).encode()).hexdigest(),
        status="PARTIAL",
        accepted_at=min(cash_page_as_of, holdings_page_as_of),
        completed_at=authorized_at,
        attempt_count=1,
        failure_reason=None,
        cash_flow_quality="NOT_RUN",
    )
    session.add(sync_run)
    await session.flush()
    snapshot_as_of = max(cash_page_as_of, holdings_page_as_of)
    snapshot = PortfolioSnapshotRecord(
        portfolio_id=portfolio.id,
        sync_run_id=sync_run.id,
        stale_from_snapshot_id=None,
        status="PARTIAL",
        as_of=snapshot_as_of,
        position_hash_version=POSITION_HASH_VERSION,
        position_hash=canonical_position_hash(positions),
        order_hash_version=ORDER_HASH_VERSION,
        order_hash=canonical_order_hash(()),
        source_request_ids=[cash_request_id, holdings_request_id],
        source_observations=[
            {
                "kind": "cash",
                "source": "fosun",
                "as_of": cash_page_as_of.isoformat(),
            },
            {
                "kind": "holdings",
                "source": "fosun",
                "as_of": holdings_page_as_of.isoformat(),
            },
        ],
    )
    session.add(snapshot)
    await session.flush()
    session.add_all(
        CashSnapshot(
            portfolio_snapshot_id=snapshot.id,
            currency=item.currency,
            cash_balance=item.cash_balance,
            available_cash=item.available_cash,
            provider_as_of=item.provider_as_of,
        )
        for item in cash
    )
    session.add_all(
        PositionSnapshot(
            portfolio_snapshot_id=snapshot.id,
            instrument_id=position.instrument_id,
            currency=position.currency,
            quantity=position.quantity,
            available_quantity=position.available_quantity,
            frozen_quantity=position.frozen_quantity,
            avg_cost=position.avg_cost,
            diluted_cost=position.diluted_cost,
            current_price=position.current_price,
            provider_as_of=position.provider_as_of,
        )
        for position in positions
    )
    quote_binding, bars_binding = await _ensure_market_bindings(
        session,
        authorized_at=authorized_at,
        delivery_mode=settings.market_delivery_mode,
    )
    quote_item = parse_quote_page(
        quote_raw,
        expected_codes=(settings.market_symbol,),
    ).items[0]
    quote_evidence = encode_fosun_snapshot_evidence((((settings.market_symbol,), quote_raw),))
    quote_digest = hashlib.sha256(quote_evidence).hexdigest()
    quote_key = _raw_key(quote_raw, quote_digest)
    quote_uri = await store.archive_raw(quote_key, quote_evidence)
    quote_artifact = DataArtifact(
        uri=quote_uri,
        sha256=quote_digest,
        byte_size=len(quote_evidence),
        mime_type="application/json",
        schema_version="fosun_gateway_market_snapshot_v1",
        source="fosun",
        observed_at=quote_raw.received_at,
        available_at=quote_raw.received_at,
        received_at=quote_raw.received_at,
    )
    session.add(quote_artifact)
    await session.flush()
    canonical_quote = canonical_quote_observation(
        instrument_id=market_instrument_id,
        source="fosun",
        feed="l1",
        delivery_mode=settings.market_delivery_mode,
        event_kind="snapshot",
        bid=None,
        ask=None,
        last=quote_item.price,
        observed_at=quote_raw.received_at,
        available_at=quote_raw.received_at,
        received_at=quote_raw.received_at,
        revision=quote_digest,
    )
    session.add(
        LatestQuote(
            instrument_id=market_instrument_id,
            source="fosun",
            feed="l1",
            delivery_mode=settings.market_delivery_mode,
            bid=None,
            ask=None,
            last=quote_item.price,
            observed_at=quote_raw.received_at,
            available_at=quote_raw.received_at,
            received_at=quote_raw.received_at,
            revision=quote_digest,
            quality_flags=[
                "FOSUN_PROVIDER_TIMESTAMP_UNAVAILABLE",
                "INTRADAY_BID_ASK_UNAVAILABLE",
            ],
            event_kind="snapshot",
            content_hash=hashlib.sha256(canonical_quote).hexdigest(),
            evidence_status="verified",
            data_artifact_id=quote_artifact.id,
            **quote_binding.model_dump(),
        )
    )
    calendar_authority = await _insert_calendar(
        session,
        rows=calendar_rows,
        range_start=request_start_at.date(),
        range_end=authorized_at.date(),
        published_at=min(quote_raw.received_at, bars_raw.received_at),
    )
    archived_bars = await archive_fosun_price_page_raw(
        session,
        store,
        bars_raw,
        commit=False,
    )
    bar_request = BarsRequest(
        instrument_id=InstrumentId(market_instrument_id),
        start=request_start_at,
        end=request_end_at,
        interval="1Day",
        adjustment="raw",
    )
    bar_provider = FosunMarketDataProvider(
        _ClosedMarketGateway(),
        symbol_resolver=_UnusedSymbolResolver(),
        delivery_mode=settings.market_delivery_mode,
    )
    normalized_bars = await bar_provider.normalize_bars(
        bar_request,
        bars_raw,
        symbol=settings.market_symbol.removeprefix("us"),
    )
    ingestion = await ingest_price_page(
        session,
        store,
        normalized_bars,
        provider_binding=bars_binding,
        raw_artifact=archived_bars.raw_artifact,
        calendar_authority_id=calendar_authority.id,
        gateway_request_start_at=request_start_at,
        gateway_request_end_at=request_end_at,
        commit=False,
    )
    if ingestion.manifest.delivery_mode != "unknown":
        raise ValueError("LOCAL_KLINE_DISPLAY_MODE_INVALID")
    return {
        "status": "ok",
        "source": "fosun",
        "cash_currencies": len(cash),
        "positions": len(positions),
        "quotes": 1,
        "bars": len(normalized_bars.items),
        "portfolio_as_of": snapshot_as_of.isoformat(),
        "market_as_of": max(quote_raw.received_at, bars_raw.received_at).isoformat(),
    }


async def _ensure_instrument(
    session: AsyncSession,
    code: str,
    *,
    currency: str,
    valid_from: date,
) -> UUID:
    symbol, exchange = _symbol_and_exchange(code)
    existing = await session.scalar(
        select(InstrumentAlias).where(
            InstrumentAlias.symbol == symbol,
            InstrumentAlias.alias_type == "ticker",
            InstrumentAlias.valid_from <= valid_from,
        )
    )
    if existing is not None:
        return existing.instrument_id
    instrument_id = uuid5(_INSTRUMENT_NAMESPACE, code)
    session.add(
        Instrument(
            id=instrument_id,
            asset_type="common_stock",
            exchange=exchange,
            currency=currency,
            tick_size=Decimal("0.001") if exchange == "XHKG" else Decimal("0.01"),
            risk_bucket="hk_equity" if exchange == "XHKG" else "EQUITY",
        )
    )
    session.add(
        InstrumentAlias(
            instrument_id=instrument_id,
            symbol=symbol,
            alias_type="ticker",
            valid_from=valid_from,
            valid_to=None,
        )
    )
    await session.flush()
    return instrument_id


async def _ensure_market_universe(
    session: AsyncSession,
    *,
    instrument_id: UUID,
    published_at: datetime,
    effective_from: date,
) -> None:
    universe = await session.scalar(
        select(UniverseSnapshot).where(
            UniverseSnapshot.key == "us_liquid",
            UniverseSnapshot.version == "us_liquid_v1",
        )
    )
    if universe is None:
        digest = hashlib.sha256(str(instrument_id).encode()).hexdigest()
        universe = UniverseSnapshot(
            id=_UNIVERSE_ID,
            key="us_liquid",
            version="us_liquid_v1",
            effective_from=effective_from,
            source="fosun_local_read_only",
            content_hash=digest,
            published_at=None,
        )
        session.add(universe)
        await session.flush()
        session.add(
            UniverseMember(
                universe_snapshot_id=universe.id,
                instrument_id=instrument_id,
                inclusion_reason="local read-only market display",
            )
        )
        await session.flush()
        universe.published_at = published_at
        await session.flush()
        return
    member = await session.scalar(
        select(UniverseMember.id).where(
            UniverseMember.universe_snapshot_id == universe.id,
            UniverseMember.instrument_id == instrument_id,
        )
    )
    if member is None:
        raise ValueError("LOCAL_MARKET_UNIVERSE_IMMUTABLE")


async def _ensure_market_bindings(
    session: AsyncSession,
    *,
    authorized_at: datetime,
    delivery_mode: str,
) -> tuple[ProviderMarketBinding, ProviderMarketBinding]:
    created_at = authorized_at - timedelta(seconds=1)
    expires_at = authorized_at + timedelta(hours=_LOCAL_RUNTIME_HOURS)
    status = IntegrationStatus(
        provider="fosun",
        status="read_only",
        last_success_at=authorized_at,
        version=1,
        mode_changed_at=created_at,
        capability_invalidated_at=None,
        admitted_contract_hash=_LOCAL_CONTRACT_HASH,
        created_at=created_at,
    )
    session.add(status)
    quote_capability = ProviderCapability(
        provider="fosun",
        capability="READ_ONLY",
        contract_hash=_LOCAL_CONTRACT_HASH,
        expires_at=expires_at,
        schema_version="1",
        payload={
            "operation": "fosun_get_quotes",
            "market_scope": "US",
            "delivery_mode": delivery_mode,
        },
        created_at=created_at,
    )
    bars_capability = ProviderCapability(
        provider="fosun",
        capability="READ_ONLY",
        contract_hash=_LOCAL_CONTRACT_HASH,
        expires_at=expires_at,
        schema_version="1",
        payload={
            "operation": "fosun_get_bars",
            "market_scope": "US",
            "delivery_mode": delivery_mode,
        },
        created_at=created_at,
    )
    session.add_all((quote_capability, bars_capability))
    await session.flush()
    return (
        ProviderMarketBinding(
            provider_capability_id=quote_capability.id,
            provider_status_id=status.id,
            provider_status_version=status.version,
            provider_contract_hash=_LOCAL_CONTRACT_HASH,
            provider_authorized_at=authorized_at,
        ),
        ProviderMarketBinding(
            provider_capability_id=bars_capability.id,
            provider_status_id=status.id,
            provider_status_version=status.version,
            provider_contract_hash=_LOCAL_CONTRACT_HASH,
            provider_authorized_at=authorized_at,
        ),
    )


async def _insert_calendar(
    session: AsyncSession,
    *,
    rows: tuple[tuple[date, datetime, datetime], ...],
    range_start: date,
    range_end: date,
    published_at: datetime,
) -> ExchangeCalendarAuthorityRecord:
    digest = hashlib.sha256("\n".join(item[0].isoformat() for item in rows).encode()).hexdigest()
    authority = ExchangeCalendarAuthorityRecord(
        exchange="XNYS",
        version=f"fosun-local-{digest[:16]}",
        source="exchange_calendars",
        range_start=range_start,
        range_end=range_end,
        query_parameters={"calendar": "XNYS", "mode": "local_read_only"},
        completeness_status="complete",
        content_hash=digest,
        published_at=published_at,
    )
    session.add(authority)
    await session.flush()
    session.add_all(
        ExchangeCalendarSessionRecord(
            calendar_authority_id=authority.id,
            session=session_date,
            market_open_at=market_open,
            market_close_at=market_close,
            decision_at=market_close,
        )
        for session_date, market_open, market_close in rows
    )
    await session.flush()
    return authority


def _daily_window(
    now: datetime,
) -> tuple[datetime, datetime, tuple[tuple[date, datetime, datetime], ...]]:
    calendar = xcals.get_calendar("XNYS")
    candidates = calendar.sessions_in_range(now.date() - timedelta(days=10), now.date())
    completed = [
        session
        for session in candidates
        if calendar.session_close(session).to_pydatetime().astimezone(UTC)
        <= now - timedelta(minutes=30)
    ]
    if not completed:
        raise ValueError("LOCAL_COMPLETED_SESSION_UNAVAILABLE")
    request_end_at = datetime.combine(completed[-1].date(), time.min, tzinfo=UTC)
    request_start_at = request_end_at - timedelta(days=400)
    sessions = calendar.sessions_in_range(request_start_at.date(), now.date())
    rows = tuple(
        (
            session.date(),
            calendar.session_open(session).to_pydatetime().astimezone(UTC),
            calendar.session_close(session).to_pydatetime().astimezone(UTC),
        )
        for session in sessions
    )
    return request_start_at, request_end_at, rows


def _symbol_and_exchange(code: str) -> tuple[str, str]:
    if code.startswith("us") and len(code) > 2:
        return code[2:], "XNYS"
    if code.startswith("hk") and len(code) > 2:
        return f"{code[2:]}.HK", "XHKG"
    raise ValueError("LOCAL_INSTRUMENT_CODE_UNSUPPORTED")


def _raw_key(response: RawGatewayResponse, digest: str) -> str:
    date_path = response.received_at.strftime("%Y/%m/%d")
    return f"raw/market/fosun/{date_path}/{response.gateway_request_id}/{digest}.json"


def _broker_settings(settings: LocalFosunRefreshSettings) -> BrokerWorkerSettings:
    return BrokerWorkerSettings.model_validate(
        {
            "NEXUS_ENV": "development",
            "DATABASE_URL": settings.database_url,
            "NEXUS_BROKER_SIMULATION_MODE": "offline_fixture",
            "FOSUN_GATEWAY_BASE_URL": settings.broker_gateway_origin,
            "FOSUN_GATEWAY_HMAC_CURRENT_KEY_ID": settings.broker_gateway_key_id,
            "FOSUN_GATEWAY_HMAC_CURRENT_SECRET": settings.broker_gateway_secret,
            "FOSUN_GATEWAY_CLIENT_CERT": settings.broker_client_cert,
            "FOSUN_GATEWAY_CLIENT_KEY": settings.broker_client_key,
            "FOSUN_GATEWAY_CLIENT_CA": settings.broker_client_ca,
        }
    )


def _market_identity(settings: LocalFosunRefreshSettings) -> FosunMarketGatewayIdentity:
    return FosunMarketGatewayIdentity(
        origin=settings.market_gateway_origin,
        key_id=settings.market_gateway_key_id,
        secret=settings.market_gateway_secret,
        client_cert=settings.market_client_cert,
        client_key=settings.market_client_key,
        client_ca=settings.market_client_ca,
    )


class _ClosedMarketGateway:
    async def read_quotes_raw(self, _codes: tuple[str, ...]) -> RawGatewayResponse:
        raise RuntimeError("LOCAL_NETWORK_BUDGET_CLOSED")

    async def read_bars_raw(self, **_kwargs: object) -> RawGatewayResponse:
        raise RuntimeError("LOCAL_NETWORK_BUDGET_CLOSED")

    async def aclose(self) -> None:
        return None


class _UnusedSymbolResolver:
    async def resolve(self, _instrument_id: InstrumentId, _as_of: date) -> str:
        raise RuntimeError("LOCAL_SYMBOL_ALREADY_FROZEN")


def _safe_failure_code(error: Exception) -> str:
    message = str(error)
    if re.fullmatch(r"LOCAL_[A-Z0-9_]{1,59}", message):
        return message
    return "LOCAL_READ_ONLY_REFRESH_FAILED"


def main() -> None:
    try:
        result = asyncio.run(
            refresh_local_fosun(LocalFosunRefreshSettings())  # type: ignore[call-arg]
        )
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as error:
        code = _safe_failure_code(error)
        sys.stderr.write(f'{{"status":"failed","code":"{code}"}}\n')
        raise SystemExit(1) from None
    sys.stdout.write(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    main()
