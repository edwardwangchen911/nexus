"""One-shot Mac-local ingestion of the five reviewed Fosun read surfaces."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Literal, Protocol, Self
from uuid import UUID, uuid5

import exchange_calendars as xcals  # type: ignore[import-untyped]
from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from nexus.diagnostics.artifacts import ArtifactByteReader
from nexus.diagnostics.service import diagnose_and_persist, preview_snapshot_diagnostic
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
    BrokerCashFlowWatermark,
    BrokerSyncRun,
    Portfolio,
    PortfolioSnapshotRecord,
)
from nexus.persistence.models.runtime import IntegrationStatus
from nexus.providers.base import BarsRequest, InstrumentId
from nexus.providers.broker.base import BrokerSyncResult
from nexus.providers.broker.fosun import FosunBrokerProvider, GatewayReader
from nexus.providers.broker.fosun_contracts import AccountItem, PositionItem
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
from nexus.reconciliation.broker_snapshot import freeze_broker_snapshot
from nexus.reconciliation.cash_flows import CashFlowSyncResult, fetch_cash_flows
from nexus.reconciliation.snapshot_persistence import finalize_broker_sync

_LOCAL_CONTRACT_HASH = hashlib.sha256(b"nexus.mac-local-fosun-read-only.v2").hexdigest()
_INSTRUMENT_NAMESPACE = UUID("639bd8e1-d250-4f58-a88c-370b6bbc621b")
_UNIVERSE_ID = UUID("40cd56b4-c849-5ea7-b463-19699231f521")
_LOCAL_CAPABILITY_DAYS = 365
_MAX_US_HOLDING_SYMBOLS = 12
_MAX_PROVIDER_REQUESTS = 22
_US_PROVIDER_CODE = re.compile(r"^us[A-Z][A-Z0-9.-]{0,14}$")


class _MarketReader(Protocol):
    async def read_quotes_raw(self, codes: tuple[str, ...]) -> RawGatewayResponse: ...

    async def read_bars_raw(
        self,
        *,
        code: str,
        ktype: Literal["day", "minute"],
        start_time: datetime,
        end_time: datetime,
        limit: int,
    ) -> RawGatewayResponse: ...


@dataclass(frozen=True)
class _MarketSnapshotReads:
    codes: tuple[str, ...]
    quote_raw: RawGatewayResponse
    bars_raw: tuple[tuple[str, RawGatewayResponse], ...]
    request_counts: dict[str, int]


@dataclass(frozen=True)
class _LocalBrokerIdentity:
    account_id: UUID
    portfolio_id: UUID
    sync_run_id: UUID
    instrument_ids: dict[str, UUID]
    previous_cash_flow_watermark: date | None


class LocalOrderReadError(RuntimeError):
    """Preserve only a fixed category across the local reconciliation boundary."""

    def __init__(self, error: GatewayClientError) -> None:
        super().__init__("LOCAL_ORDER_LIST_READ_FAILED")
        self.category = _gateway_failure_category(str(error))

    def safe_output(self) -> dict[str, str]:
        return {
            "status": "failed",
            "code": "LOCAL_ORDER_LIST_READ_FAILED",
            "category": self.category,
            "operation": "OrderList",
        }


class _SinglePageBrokerGateway:
    """Keep the one-shot local broker request budget fixed and explicit."""

    def __init__(self, inner: GatewayReader) -> None:
        self._inner = inner

    async def read_cash(self, external_account_id: str) -> GatewayResponsePage:
        return self._require_single_page(await self._inner.read_cash(external_account_id))

    async def read_holdings(
        self,
        external_account_id: str,
        *,
        cursor: str | None,
        limit: int,
    ) -> GatewayResponsePage:
        return self._require_single_page(
            await self._inner.read_holdings(
                external_account_id,
                cursor=cursor,
                limit=limit,
            )
        )

    async def read_orders(
        self,
        external_account_id: str,
        *,
        cursor: str | None,
        limit: int,
    ) -> GatewayResponsePage:
        try:
            return self._require_single_page(
                await self._inner.read_orders(
                    external_account_id,
                    cursor=cursor,
                    limit=limit,
                )
            )
        except GatewayClientError as error:
            # This local one-shot error must not be reduced to generic snapshot failure.
            # Never attach the response, request, account, or original exception text.
            raise LocalOrderReadError(error) from None

    async def read_cash_flows(
        self,
        external_account_id: str,
        *,
        date_from: date,
        date_to: date,
        cursor: str | None,
        limit: int,
    ) -> GatewayResponsePage:
        return self._require_single_page(
            await self._inner.read_cash_flows(
                external_account_id,
                date_from=date_from,
                date_to=date_to,
                cursor=cursor,
                limit=limit,
            )
        )

    @staticmethod
    def _require_single_page(page: GatewayResponsePage) -> GatewayResponsePage:
        if page.has_more:
            raise GatewayClientError("gateway pagination rejected")
        return page


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
    """Read the bounded holdings-driven surface, then persist one display snapshot."""
    checked_at = (now or datetime.now(UTC)).astimezone(UTC)
    request_start_at, request_end_at, calendar_rows = _daily_window(checked_at)
    expected_sessions = tuple(row[0] for row in calendar_rows if row[0] <= request_end_at.date())
    bar_limit = len(expected_sessions)
    if not 1 <= bar_limit <= 500:
        raise ValueError("LOCAL_KLINE_BUDGET_INVALID")
    engine = None
    market = FosunMarketGatewayClient(_market_identity(settings))
    broker = FosunGatewayClient(_broker_settings(settings))
    try:
        accounts_page = await broker.read_accounts()
        try:
            accounts = tuple(AccountItem.model_validate(item) for item in accounts_page.items)
        except Exception:
            raise RuntimeError("LOCAL_ACCOUNT_CONTRACT_INVALID") from None
        if not accounts or accounts_page.has_more:
            raise ValueError("LOCAL_ACCOUNT_SCOPE_AMBIGUOUS")
        external_account_id = accounts[0].external_account_id
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
        if any(item.external_account_id != external_account_id for item in holdings):
            raise ValueError("LOCAL_ACCOUNT_SCOPE_MISMATCH")
        market_reads = await _read_market_snapshot(
            market,
            codes=_us_holding_codes(holdings),
            request_start_at=request_start_at,
            request_end_at=request_end_at,
            expected_sessions=expected_sessions,
        )
        engine = create_async_engine(settings.database_url, pool_pre_ping=True)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        async with sessions() as session:
            identity = await _load_local_broker_identity(session, holdings=holdings)
        frozen, cash_flow_result = await _read_broker_facts(
            broker,
            account_id=identity.account_id,
            portfolio_id=identity.portfolio_id,
            sync_run_id=identity.sync_run_id,
            external_account_id=external_account_id,
            instrument_ids=identity.instrument_ids,
            cash_flow_end=request_end_at.date(),
            previous_cash_flow_watermark=identity.previous_cash_flow_watermark,
        )
    except BaseException:
        if engine is not None:
            await engine.dispose()
        raise
    finally:
        await broker.aclose()
        await market.aclose()

    if engine is None:
        raise AssertionError("local database engine was not initialized")
    store = S3ProviderEvidenceStore(settings)
    try:
        try:
            async with sessions() as session, session.begin():
                result = await _persist_refresh(
                    session,
                    store,
                    settings=settings,
                    accounts_page=accounts_page,
                    holdings=holdings,
                    identity=identity,
                    frozen=frozen,
                    cash_flow_result=cash_flow_result,
                    market_reads=market_reads,
                    request_start_at=request_start_at,
                    request_end_at=request_end_at,
                    calendar_rows=calendar_rows,
                    authorized_at=datetime.now(UTC),
                )
        except Exception as error:
            if isinstance(error, GatewayClientError):
                raise
            raise RuntimeError("LOCAL_PERSISTENCE_FAILED") from None
        result["diagnostic"] = await _diagnose_local_snapshot(
            sessions,
            frozen=frozen,
            cash_flow_quality=cash_flow_result.quality,
            artifact_reader=store.read_normalized,
        )
        return result
    finally:
        await engine.dispose()


async def _diagnose_local_snapshot(
    sessions: async_sessionmaker[AsyncSession],
    *,
    frozen: BrokerSyncResult,
    cash_flow_quality: str,
    artifact_reader: ArtifactByteReader,
) -> dict[str, object]:
    """Generate from committed local facts; a diagnostic failure must not erase a refresh."""
    if frozen.status != "READY":
        return {"status": "not_run", "code": "RECONCILIATION_PENDING"}
    try:
        async with sessions() as session, session.begin():
            if cash_flow_quality == "PASS":
                report = await diagnose_and_persist(
                    session,
                    portfolio_id=frozen.portfolio_snapshot.portfolio_id,
                    snapshot_id=frozen.portfolio_snapshot.id,
                    artifact_reader=artifact_reader,
                )
            else:
                report = await preview_snapshot_diagnostic(
                    session,
                    portfolio_id=frozen.portfolio_snapshot.portfolio_id,
                    snapshot_id=frozen.portfolio_snapshot.id,
                )
        return {
            "status": "persisted" if cash_flow_quality == "PASS" else "preview",
            "optimization_need": report.optimization_need,
            "items": len(report.items),
        }
    except Exception:
        return {"status": "failed", "code": "LOCAL_DIAGNOSTIC_FAILED"}


def _us_holding_codes(holdings: Sequence[PositionItem]) -> tuple[str, ...]:
    codes = tuple(
        sorted(
            {
                item.instrument_code
                for item in holdings
                if _US_PROVIDER_CODE.fullmatch(item.instrument_code) is not None
            }
        )
    )
    if not codes:
        raise ValueError("LOCAL_US_HOLDING_SCOPE_EMPTY")
    if len(codes) > _MAX_US_HOLDING_SYMBOLS:
        raise ValueError("LOCAL_US_HOLDING_BUDGET_EXCEEDED")
    return codes


async def _freeze_broker_facts(
    broker: GatewayReader,
    *,
    account_id: UUID,
    portfolio_id: UUID,
    sync_run_id: UUID,
    external_account_id: str,
    instrument_ids: dict[str, UUID],
) -> BrokerSyncResult:
    """Freeze one local read-only broker window without automatic retries."""
    provider = FosunBrokerProvider(
        account_id=account_id,
        external_account_id=external_account_id,
        client=_SinglePageBrokerGateway(broker),
        instrument_ids=instrument_ids,
        cash_flow_kinds={},
    )
    return await freeze_broker_snapshot(
        account_id,
        provider,
        portfolio_id=portfolio_id,
        sync_run_id=sync_run_id,
        max_attempts=1,
    )


async def _read_broker_facts(
    broker: GatewayReader,
    *,
    account_id: UUID,
    portfolio_id: UUID,
    sync_run_id: UUID,
    external_account_id: str,
    instrument_ids: dict[str, UUID],
    cash_flow_end: date,
    previous_cash_flow_watermark: date | None,
) -> tuple[BrokerSyncResult, CashFlowSyncResult]:
    """Read the reconciled snapshot, then one bounded cash-flow overlap window."""
    def resolve_order_instrument(code: str) -> UUID:
        if _US_PROVIDER_CODE.fullmatch(code) is None:
            raise ValueError("LOCAL_ORDER_INSTRUMENT_UNSUPPORTED")
        return instrument_ids.setdefault(code, uuid5(_INSTRUMENT_NAMESPACE, code))

    provider = FosunBrokerProvider(
        account_id=account_id,
        external_account_id=external_account_id,
        client=_SinglePageBrokerGateway(broker),
        instrument_ids=instrument_ids,
        cash_flow_kinds={},
        snapshot_time_basis="gateway_observed",
        order_instrument_resolver=resolve_order_instrument,
    )
    frozen = await freeze_broker_snapshot(
        account_id,
        provider,
        portfolio_id=portfolio_id,
        sync_run_id=sync_run_id,
        max_attempts=1,
    )
    if frozen.status != "READY":
        raise RuntimeError("LOCAL_BROKER_RECONCILIATION_REQUIRED")
    cash_flows = await fetch_cash_flows(
        account_id,
        provider,
        sync_run_id=sync_run_id,
        initial_start=cash_flow_end - timedelta(days=6),
        end=cash_flow_end,
        previous_watermark=previous_cash_flow_watermark,
    )
    return frozen, cash_flows


async def _load_local_broker_identity(
    session: AsyncSession,
    *,
    holdings: tuple[PositionItem, ...],
) -> _LocalBrokerIdentity:
    """Resolve stable local IDs without writing before broker reconciliation succeeds."""
    accounts = tuple(
        await session.scalars(
            select(Account).where(Account.provider == "fosun").order_by(Account.id).limit(2)
        )
    )
    if len(accounts) > 1:
        raise ValueError("LOCAL_ACCOUNT_SCOPE_AMBIGUOUS")
    account_id = accounts[0].id if accounts else new_uuid7()
    portfolios = tuple(
        await session.scalars(
            select(Portfolio)
            .where(Portfolio.account_id == account_id)
            .order_by(Portfolio.id)
            .limit(2)
        )
    )
    if len(portfolios) > 1:
        raise ValueError("LOCAL_PORTFOLIO_SCOPE_AMBIGUOUS")
    portfolio_id = portfolios[0].id if portfolios else new_uuid7()
    instrument_ids: dict[str, UUID] = {}
    aliases = await session.execute(
        select(InstrumentAlias.symbol, Instrument.id, Instrument.exchange, Instrument.currency)
        .join(Instrument, Instrument.id == InstrumentAlias.instrument_id)
        .where(
            InstrumentAlias.alias_type == "ticker",
            InstrumentAlias.valid_from <= datetime.now(UTC).date(),
            InstrumentAlias.valid_to.is_(None),
        )
    )
    for symbol, instrument_id, exchange, currency in aliases:
        code = f"us{symbol}"
        if exchange not in {"XNYS", "XNAS"} or currency != "USD":
            continue
        if _US_PROVIDER_CODE.fullmatch(code) is None:
            continue
        if code in instrument_ids and instrument_ids[code] != instrument_id:
            raise ValueError("LOCAL_BROKER_INSTRUMENT_AMBIGUOUS")
        instrument_ids[code] = instrument_id
    for item in holdings:
        symbol, _exchange = _symbol_and_exchange(item.instrument_code)
        alias = await session.scalar(
            select(InstrumentAlias).where(
                InstrumentAlias.symbol == symbol,
                InstrumentAlias.alias_type == "ticker",
            )
        )
        instrument_ids[item.instrument_code] = (
            alias.instrument_id
            if alias is not None
            else uuid5(_INSTRUMENT_NAMESPACE, item.instrument_code)
        )
    return _LocalBrokerIdentity(
        account_id=account_id,
        portfolio_id=portfolio_id,
        sync_run_id=new_uuid7(),
        instrument_ids=instrument_ids,
        previous_cash_flow_watermark=await session.scalar(
            select(BrokerCashFlowWatermark.provider_business_date).where(
                BrokerCashFlowWatermark.account_id == account_id
            )
        ),
    )


async def _persist_ready_broker_snapshot(
    session: AsyncSession,
    *,
    account_id: UUID,
    portfolio_id: UUID,
    frozen: BrokerSyncResult,
    holdings: tuple[PositionItem, ...],
    instrument_ids: dict[str, UUID],
    valid_from: date,
    completed_at: datetime,
    cash_flow_result: CashFlowSyncResult,
) -> PortfolioSnapshotRecord:
    """Persist one already-frozen local broker snapshot in a single transaction."""
    if frozen.status != "READY" or frozen.portfolio_snapshot.portfolio_id != portfolio_id:
        raise ValueError("LOCAL_BROKER_SNAPSHOT_NOT_READY")
    account = await session.get(Account, account_id)
    if account is None:
        account = Account(id=account_id, provider="fosun", status="ACTIVE")
        session.add(account)
        await session.flush()
    elif account.provider != "fosun" or account.status != "ACTIVE":
        raise ValueError("LOCAL_BROKER_ACCOUNT_INVALID")
    portfolio = await session.get(Portfolio, portfolio_id)
    if portfolio is None:
        portfolio = Portfolio(
            id=portfolio_id,
            account_id=account_id,
            name="复星只读账户",
        )
        session.add(portfolio)
        await session.flush()
    elif portfolio.account_id != account_id:
        raise ValueError("LOCAL_BROKER_PORTFOLIO_INVALID")
    for item in holdings:
        expected_id = instrument_ids.get(item.instrument_code)
        if expected_id is None:
            raise ValueError("LOCAL_BROKER_INSTRUMENT_UNMAPPED")
        actual_id = await _ensure_instrument(
            session,
            item.instrument_code,
            currency=item.currency,
            valid_from=valid_from,
            expected_id=expected_id,
        )
        if actual_id != expected_id:
            raise ValueError("LOCAL_BROKER_INSTRUMENT_MISMATCH")
    codes_by_id = {instrument_id: code for code, instrument_id in instrument_ids.items()}
    for order in frozen.portfolio_snapshot.open_orders:
        if await session.get(Instrument, order.instrument_id) is not None:
            continue
        code = codes_by_id.get(order.instrument_id, "")
        if _US_PROVIDER_CODE.fullmatch(code) is None:
            raise ValueError("LOCAL_ORDER_INSTRUMENT_UNSUPPORTED")
        actual_id = await _ensure_instrument(
            session, code, currency="USD", valid_from=valid_from, expected_id=order.instrument_id
        )
        if actual_id != order.instrument_id:
            raise ValueError("LOCAL_BROKER_INSTRUMENT_MISMATCH")
    run = BrokerSyncRun(
        id=frozen.sync_run_id,
        account_id=account_id,
        portfolio_id=portfolio_id,
        trigger_kind="MANUAL",
        actor_id="nexus-mac-local-read-only",
        route="local-fosun-refresh",
        idempotency_key=str(frozen.sync_run_id),
        request_hash=hashlib.sha256(str(frozen.sync_run_id).encode()).hexdigest(),
        status="ACTIVE",
        accepted_at=completed_at,
        completed_at=None,
        attempt_count=0,
        failure_reason=None,
        cash_flow_quality="NOT_RUN",
    )
    session.add(run)
    await session.flush()
    return await finalize_broker_sync(
        session,
        frozen,
        completed_at=completed_at,
        cash_flow_result=cash_flow_result,
    )


async def _read_market_snapshot(
    market: _MarketReader,
    *,
    codes: tuple[str, ...],
    request_start_at: datetime,
    request_end_at: datetime,
    expected_sessions: tuple[date, ...],
) -> _MarketSnapshotReads:
    if codes != tuple(sorted(set(codes))) or not 1 <= len(codes) <= _MAX_US_HOLDING_SYMBOLS:
        raise ValueError("LOCAL_US_HOLDING_SCOPE_INVALID")
    if any(_US_PROVIDER_CODE.fullmatch(code) is None for code in codes):
        raise ValueError("LOCAL_US_HOLDING_SCOPE_INVALID")
    bar_limit = len(expected_sessions)
    if not 1 <= bar_limit <= 500:
        raise ValueError("LOCAL_KLINE_BUDGET_INVALID")
    if 10 + len(codes) > _MAX_PROVIDER_REQUESTS:
        raise ValueError("LOCAL_PROVIDER_REQUEST_BUDGET_EXCEEDED")

    quote_raw = await market.read_quotes_raw(codes)
    try:
        parse_quote_page(quote_raw, expected_codes=codes)
    except Exception:
        raise RuntimeError("LOCAL_QUOTE_CONTRACT_INVALID") from None

    bars_raw: list[tuple[str, RawGatewayResponse]] = []
    for code in codes:
        raw = await market.read_bars_raw(
            code=code,
            ktype="day",
            start_time=request_start_at,
            end_time=request_end_at,
            limit=bar_limit,
        )
        try:
            page = parse_bar_page(
                raw,
                expected_code=code,
                expected_ktype="day",
                start=request_start_at,
                end=request_end_at,
                limit=bar_limit,
            )
        except Exception:
            raise RuntimeError("LOCAL_KLINE_CONTRACT_INVALID") from None
        actual_sessions = tuple(item.timestamp.date() for item in page.items)
        if (
            not actual_sessions
            or actual_sessions[-1] != expected_sessions[-1]
            or any(session not in expected_sessions for session in actual_sessions)
        ):
            raise ValueError("LOCAL_KLINE_WINDOW_INCOMPLETE")
        bars_raw.append((code, raw))

    return _MarketSnapshotReads(
        codes=codes,
        quote_raw=quote_raw,
        bars_raw=tuple(bars_raw),
        request_counts={
            "quotes": 1,
            "bars": len(bars_raw),
            "total": 1 + len(bars_raw),
        },
    )


async def _persist_refresh(
    session: AsyncSession,
    store: S3ProviderEvidenceStore,
    *,
    settings: LocalFosunRefreshSettings,
    accounts_page: GatewayResponsePage,
    holdings: tuple[PositionItem, ...],
    identity: _LocalBrokerIdentity,
    frozen: BrokerSyncResult,
    cash_flow_result: CashFlowSyncResult,
    market_reads: _MarketSnapshotReads,
    request_start_at: datetime,
    request_end_at: datetime,
    calendar_rows: tuple[tuple[date, datetime, datetime], ...],
    authorized_at: datetime,
) -> dict[str, object]:
    quote_raw = market_reads.quote_raw
    bars_raw_by_code = dict(market_reads.bars_raw)
    account_page_as_of = accounts_page.as_of
    instrument_ids = identity.instrument_ids
    snapshot = await _persist_ready_broker_snapshot(
        session,
        account_id=identity.account_id,
        portfolio_id=identity.portfolio_id,
        frozen=frozen,
        holdings=holdings,
        instrument_ids=instrument_ids,
        valid_from=request_start_at.date(),
        completed_at=authorized_at,
        cash_flow_result=cash_flow_result,
    )
    await _ensure_market_universe(
        session,
        instrument_ids=tuple(instrument_ids[code] for code in market_reads.codes),
        published_at=min(
            account_page_as_of,
            quote_raw.received_at,
            *(raw.received_at for raw in bars_raw_by_code.values()),
        ),
        effective_from=request_start_at.date(),
    )
    quote_binding, bars_binding = await _ensure_market_bindings(
        session,
        authorized_at=authorized_at,
        delivery_mode=settings.market_delivery_mode,
    )
    quote_page = parse_quote_page(
        quote_raw,
        expected_codes=market_reads.codes,
    )
    quote_evidence = encode_fosun_snapshot_evidence(((market_reads.codes, quote_raw),))
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
    for quote_item in quote_page.items:
        if quote_item.price is None:
            continue
        quality_flags = ["INTRADAY_BID_ASK_UNAVAILABLE"]
        quality_flags.append("FOSUN_PROVIDER_TIMESTAMP_UNAVAILABLE")
        canonical_quote = canonical_quote_observation(
            instrument_id=instrument_ids[quote_item.instrument_code],
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
                instrument_id=instrument_ids[quote_item.instrument_code],
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
                quality_flags=quality_flags,
                event_kind="snapshot",
                content_hash=hashlib.sha256(canonical_quote).hexdigest(),
                evidence_status="verified",
                data_artifact_id=quote_artifact.id,
                **quote_binding.model_dump(),
            )
        )
    bars_received_at = tuple(raw.received_at for raw in bars_raw_by_code.values())
    calendar_authority = await _insert_calendar(
        session,
        rows=calendar_rows,
        range_start=request_start_at.date(),
        range_end=authorized_at.date(),
        published_at=min(quote_raw.received_at, *bars_received_at),
    )
    bar_provider = FosunMarketDataProvider(
        _ClosedMarketGateway(),
        symbol_resolver=_UnusedSymbolResolver(),
        delivery_mode=settings.market_delivery_mode,
    )
    bar_count = 0
    for code, bars_raw in market_reads.bars_raw:
        archived_bars = await archive_fosun_price_page_raw(
            session,
            store,
            bars_raw,
            commit=False,
        )
        bar_request = BarsRequest(
            instrument_id=InstrumentId(instrument_ids[code]),
            start=request_start_at,
            end=request_end_at,
            interval="1Day",
            adjustment="raw",
        )
        normalized_bars = await bar_provider.normalize_bars(
            bar_request,
            bars_raw,
            symbol=code.removeprefix("us"),
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
        bar_count += len(normalized_bars.items)
    quote_success = sum(item.price is not None for item in quote_page.items)
    broker_request_counts = _broker_request_counts(frozen)
    provider_requests = {
        "accounts": 1,
        "cash": broker_request_counts["cash"],
        "holdings": 1 + broker_request_counts["holdings"],
        "orders": broker_request_counts["orders"],
        "cash_flows": 1,
        "quotes": market_reads.request_counts["quotes"],
        "bars": market_reads.request_counts["bars"],
    }
    provider_requests["total"] = sum(provider_requests.values())
    return {
        "status": "ok",
        "source": "fosun",
        "snapshot_status": snapshot.status,
        "cash_currencies": len(frozen.portfolio_snapshot.cash),
        "positions": len(frozen.portfolio_snapshot.positions),
        "open_orders": len(frozen.portfolio_snapshot.open_orders),
        "cash_flow_quality": cash_flow_result.quality,
        "cash_flows": len(cash_flow_result.facts),
        "market_symbols": len(market_reads.codes),
        "quotes": len(quote_page.items),
        "quote_success": quote_success,
        "quote_missing": len(quote_page.items) - quote_success,
        "bar_series": len(market_reads.bars_raw),
        "bars": bar_count,
        "provider_requests": provider_requests,
        "portfolio_as_of": snapshot.as_of.isoformat() if snapshot.as_of else None,
        "market_as_of": max(quote_raw.received_at, *bars_received_at).isoformat(),
    }


def _broker_request_counts(frozen: BrokerSyncResult) -> dict[str, int]:
    if frozen.attempt_count != 1 or len(frozen.attempt_traces) != 1:
        raise ValueError("LOCAL_BROKER_ATTEMPT_COUNT_INVALID")
    observations = frozen.attempt_traces[0].get("source_observations")
    if not isinstance(observations, tuple | list):
        raise ValueError("LOCAL_BROKER_OBSERVATIONS_INVALID")
    counts = {"cash": 0, "holdings": 0, "orders": 0}
    for observation in observations:
        if not isinstance(observation, dict):
            raise ValueError("LOCAL_BROKER_OBSERVATIONS_INVALID")
        phase = observation.get("phase")
        if phase in {"cash_a", "cash_b"}:
            counts["cash"] += 1
        elif phase in {"positions_a", "positions_b"}:
            counts["holdings"] += 1
        elif phase in {"orders_a", "orders_b"}:
            counts["orders"] += 1
        else:
            raise ValueError("LOCAL_BROKER_OBSERVATIONS_INVALID")
    return counts


async def _ensure_instrument(
    session: AsyncSession,
    code: str,
    *,
    currency: str,
    valid_from: date,
    expected_id: UUID | None = None,
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
    instrument_id = expected_id or uuid5(_INSTRUMENT_NAMESPACE, code)
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
    instrument_ids: tuple[UUID, ...],
    published_at: datetime,
    effective_from: date,
) -> None:
    if not instrument_ids or len(instrument_ids) != len(set(instrument_ids)):
        raise ValueError("LOCAL_MARKET_UNIVERSE_INVALID")
    digest = hashlib.sha256(
        "\n".join(str(instrument_id) for instrument_id in instrument_ids).encode()
    ).hexdigest()
    version = f"holdings-{digest[:16]}"
    universe = await session.scalar(
        select(UniverseSnapshot).where(
            UniverseSnapshot.key == "fosun_local_holdings",
            UniverseSnapshot.version == version,
        )
    )
    if universe is None:
        universe = UniverseSnapshot(
            id=uuid5(_UNIVERSE_ID, digest),
            key="fosun_local_holdings",
            version=version,
            effective_from=effective_from,
            source="fosun_local_read_only",
            content_hash=digest,
            published_at=None,
        )
        session.add(universe)
        await session.flush()
        session.add_all(
            UniverseMember(
                universe_snapshot_id=universe.id,
                instrument_id=instrument_id,
                inclusion_reason="local read-only holding market display",
            )
            for instrument_id in instrument_ids
        )
        await session.flush()
        universe.published_at = published_at
        await session.flush()
        return
    existing_ids = tuple(
        await session.scalars(
            select(UniverseMember.instrument_id)
            .where(UniverseMember.universe_snapshot_id == universe.id)
            .order_by(UniverseMember.instrument_id)
        )
    )
    if existing_ids != tuple(sorted(instrument_ids)) or universe.published_at is None:
        raise ValueError("LOCAL_MARKET_UNIVERSE_INVALID")


async def _ensure_market_bindings(
    session: AsyncSession,
    *,
    authorized_at: datetime,
    delivery_mode: str,
) -> tuple[ProviderMarketBinding, ProviderMarketBinding]:
    created_at = authorized_at - timedelta(seconds=1)
    expires_at = authorized_at + timedelta(days=_LOCAL_CAPABILITY_DAYS)
    status = await session.scalar(
        select(IntegrationStatus).where(IntegrationStatus.provider == "fosun").with_for_update()
    )
    if status is None:
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
    else:
        status.status = "read_only"
        status.last_success_at = authorized_at
        status.version += 1
        status.mode_changed_at = created_at
        status.capability_invalidated_at = None
        status.admitted_contract_hash = _LOCAL_CONTRACT_HASH

    capabilities: list[ProviderCapability] = []
    for operation in ("fosun_get_quotes", "fosun_get_bars"):
        capability = await session.scalar(
            select(ProviderCapability).where(
                ProviderCapability.provider == "fosun",
                ProviderCapability.capability == "READ_ONLY",
                ProviderCapability.contract_hash == _LOCAL_CONTRACT_HASH,
                ProviderCapability.payload["operation"].as_string() == operation,
                ProviderCapability.payload["market_scope"].as_string() == "US",
            )
        )
        if capability is None:
            capability = ProviderCapability(
                provider="fosun",
                capability="READ_ONLY",
                contract_hash=_LOCAL_CONTRACT_HASH,
                expires_at=expires_at,
                schema_version="1",
                payload={
                    "operation": operation,
                    "market_scope": "US",
                    "delivery_mode": delivery_mode,
                },
                created_at=created_at,
            )
            session.add(capability)
        elif (
            capability.schema_version != "1"
            or capability.payload.get("delivery_mode") != delivery_mode
            or capability.created_at > authorized_at
            or capability.expires_at <= authorized_at
        ):
            raise ValueError("LOCAL_MARKET_CAPABILITY_INVALID")
        capabilities.append(capability)
    await session.flush()
    quote_capability, bars_capability = capabilities
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
    digest = hashlib.sha256(
        "\n".join(
            "|".join(
                (
                    session_date.isoformat(),
                    market_open.isoformat(),
                    market_close.isoformat(),
                )
            )
            for session_date, market_open, market_close in rows
        ).encode()
    ).hexdigest()
    version = f"fosun-local-{digest[:16]}"
    existing = await session.scalar(
        select(ExchangeCalendarAuthorityRecord).where(
            ExchangeCalendarAuthorityRecord.exchange == "XNYS",
            ExchangeCalendarAuthorityRecord.version == version,
            ExchangeCalendarAuthorityRecord.range_start == range_start,
            ExchangeCalendarAuthorityRecord.range_end == range_end,
        )
    )
    if existing is not None:
        if (
            existing.source != "exchange_calendars"
            or existing.content_hash != digest
            or existing.completeness_status != "complete"
            or existing.published_at > published_at
        ):
            raise ValueError("LOCAL_CALENDAR_AUTHORITY_INVALID")
        return existing
    authority = ExchangeCalendarAuthorityRecord(
        exchange="XNYS",
        version=version,
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


def _gateway_failure_category(message: str) -> str:
    categories = {
        "gateway rejected request: OPERATION_NOT_ALLOWED": "scope",
        "gateway rejected request: PROVIDER_AUTHENTICATION_FAILED": "auth",
        "gateway rejected request: PROVIDER_PERMISSION_DENIED": "permission",
        "gateway rejected request: PROVIDER_RATE_LIMITED": "rate_limit",
        "gateway rejected request: PROVIDER_APPLICATION_ERROR": "provider",
        "gateway rejected request: PROVIDER_ERROR": "provider",
        "gateway rejected request: PROVIDER_UNAVAILABLE": "provider",
        "gateway rejected request: PROVIDER_MALFORMED_RESPONSE": "contract",
        "gateway rejected request: PROVIDER_WINDOW_MISMATCH": "contract",
        "gateway request failed": "transport",
        "gateway response contract rejected": "contract",
        "gateway response rejected": "contract",
        "gateway pagination rejected": "contract",
    }
    return categories.get(message, "gateway")


def main() -> None:
    try:
        result = asyncio.run(
            refresh_local_fosun(LocalFosunRefreshSettings())  # type: ignore[call-arg]
        )
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as error:
        failure = (
            error.safe_output()
            if isinstance(error, LocalOrderReadError)
            else {"status": "failed", "code": _safe_failure_code(error)}
        )
        sys.stderr.write(json.dumps(failure, sort_keys=True) + "\n")
        raise SystemExit(1) from None
    sys.stdout.write(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    main()
