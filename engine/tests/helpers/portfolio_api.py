from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from helpers.hmac import TEST_HMAC_KEY_ID, TEST_HMAC_SECRET, build_test_hmac_headers
from nexus.main import create_engine_app
from nexus.market.ingestion import MarketObjectStore
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.portfolio import (
    Account,
    BrokerCashFlow,
    BrokerOrderSnapshot,
    BrokerSyncAttempt,
    BrokerSyncRun,
    CashSnapshot,
    Portfolio,
    PortfolioSnapshotRecord,
    PositionSnapshot,
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select
from uuid6 import uuid7

CLOCK = datetime(2026, 8, 24, 4, 0, tzinfo=UTC)
INSTRUMENT_ID = UUID("0198d500-1000-7000-8000-000000000001")
ORDER_INSTRUMENT_ID = UUID("0198d500-1000-7000-8000-000000000003")
ORDER_ID = UUID("0198d500-1000-7000-8000-000000000002")
ORDER_ONLY_INSTRUMENT_ID = UUID("0198d500-1000-7000-8000-000000000004")
ORDER_ONLY_ID = UUID("0198d500-1000-7000-8000-000000000005")
PORTFOLIOS = (
    UUID("0198d500-1000-7000-8000-000000000011"),
    UUID("0198d500-1000-7000-8000-000000000012"),
    UUID("0198d500-1000-7000-8000-000000000013"),
)
ACCOUNTS = (
    UUID("0198d500-1000-7000-8000-000000000021"),
    UUID("0198d500-1000-7000-8000-000000000022"),
    UUID("0198d500-1000-7000-8000-000000000023"),
)


def settings(database_url: str) -> EngineSettings:
    return EngineSettings(
        NEXUS_ENV="test",
        DATABASE_URL=database_url,
        NEXUS_ENGINE_HMAC_CURRENT_KEY_ID=TEST_HMAC_KEY_ID,
        NEXUS_ENGINE_HMAC_CURRENT_SECRET=TEST_HMAC_SECRET,
    )


def app(
    database_url: str,
    *,
    now: datetime = CLOCK,
    scenario_evidence_store: MarketObjectStore | None = None,
):
    engine_app = create_engine_app(
        settings(database_url), scenario_evidence_store=scenario_evidence_store
    )
    engine_app.state.utc_clock = lambda: now
    return engine_app


def headers(
    method: str,
    path: str,
    body: bytes = b"",
    *,
    actor_id: str = "nexus-test-user",
) -> dict[str, str]:
    signed = build_test_hmac_headers(
        method,
        path,
        body,
        nonce=str(uuid7()),
        actor_id=actor_id,
    )
    if body:
        signed["Content-Type"] = "application/json"
    return signed


async def seed_portfolio_list(
    database_url: str,
    *,
    ready_cash_flow_quality: str = "PASS",
    ready_usd_cash: Decimal = Decimal("1000"),
    ready_snapshot_as_of: datetime | None = None,
) -> None:
    factory = session_factory(settings(database_url))
    async with factory() as session, session.begin():
        session.add(
            Instrument(
                id=INSTRUMENT_ID,
                asset_type="equity",
                exchange="XHKG",
                currency="HKD",
                tick_size=Decimal("0.001"),
                risk_bucket="hk_equity",
            )
        )
        session.add(
            InstrumentAlias(
                instrument_id=INSTRUMENT_ID,
                symbol="00656.HK",
                alias_type="ticker",
                valid_from=date(2020, 1, 1),
                valid_to=None,
            )
        )
        session.add(
            Instrument(
                id=ORDER_INSTRUMENT_ID,
                asset_type="equity",
                exchange="XHKG",
                currency="HKD",
                tick_size=Decimal("0.001"),
                risk_bucket="hk_equity",
            )
        )
        session.add(
            InstrumentAlias(
                instrument_id=ORDER_INSTRUMENT_ID,
                symbol="00700.HK",
                alias_type="ticker",
                valid_from=date(2020, 1, 1),
                valid_to=None,
            )
        )
        for account_id, portfolio_id, name in zip(
            ACCOUNTS, PORTFOLIOS, ("Alpha", "Alpha", "Beta"), strict=True
        ):
            session.add(Account(id=account_id, provider="fosun", status="ACTIVE"))
            session.add(Portfolio(id=portfolio_id, account_id=account_id, name=name))
        await session.flush()
        for index, (portfolio_id, account_id, status) in enumerate(
            ((PORTFOLIOS[0], ACCOUNTS[0], "READY"), (PORTFOLIOS[2], ACCOUNTS[2], "PARTIAL"))
        ):
            snapshot_as_of = (
                ready_snapshot_as_of
                if index == 0 and ready_snapshot_as_of is not None
                else CLOCK - timedelta(minutes=1)
            )
            run = BrokerSyncRun(
                id=UUID(f"0198d500-1000-7000-8000-00000000003{index + 1}"),
                account_id=account_id,
                portfolio_id=portfolio_id,
                trigger_kind="AUTOMATIC",
                actor_id=f"synthetic-{index}",
                route="broker-sync",
                idempotency_key=f"synthetic-{index}",
                request_hash=str(index + 1) * 64,
                status=status,
                accepted_at=CLOCK - timedelta(minutes=2),
                completed_at=snapshot_as_of,
                attempt_count=1,
                failure_reason=None,
                cash_flow_quality=ready_cash_flow_quality if status == "READY" else "NOT_RUN",
            )
            snapshot = PortfolioSnapshotRecord(
                id=UUID(f"0198d500-1000-7000-8000-00000000004{index + 1}"),
                portfolio_id=portfolio_id,
                sync_run_id=run.id,
                stale_from_snapshot_id=None,
                status=status,
                as_of=snapshot_as_of,
                position_hash_version="broker_position_hash_v1",
                position_hash="a" * 64,
                order_hash_version="broker_order_hash_v1",
                order_hash="b" * 64,
                source_request_ids=["external-request-canary"],
                source_observations=[{"provider": "secret-canary"}],
            )
            session.add_all((run, snapshot))
            await session.flush()
            session.add(
                CashSnapshot(
                    portfolio_snapshot_id=snapshot.id,
                    currency="USD" if index == 0 else "HKD",
                    cash_balance=ready_usd_cash if index == 0 else Decimal("1000"),
                    available_cash=ready_usd_cash if index == 0 else Decimal("900"),
                    provider_as_of=snapshot.as_of,
                )
            )
            if index == 0:
                session.add_all(
                    (
                        BrokerCashFlow(
                            account_id=account_id,
                            sync_run_id=run.id,
                            provider="fosun",
                            provider_cash_flow_id="synthetic-flow-1",
                            identity_version="provider_cash_flow_id_v1",
                            identity_quality="STABLE",
                            currency="USD",
                            amount=Decimal("10"),
                            occurred_at=snapshot.as_of,
                            provider_business_date=snapshot.as_of.date(),
                            kind="deposit",
                            source_request_id="synthetic-cash-flow-request",
                            response_as_of=snapshot.as_of,
                        ),
                        PositionSnapshot(
                            portfolio_snapshot_id=snapshot.id,
                            instrument_id=INSTRUMENT_ID,
                            currency="HKD",
                            quantity=Decimal("90000"),
                            available_quantity=Decimal("90000"),
                            frozen_quantity=Decimal("0"),
                            avg_cost=Decimal("5.25"),
                            diluted_cost=Decimal("4.95"),
                            current_price=Decimal("6.125"),
                            provider_as_of=snapshot.as_of,
                        ),
                        BrokerOrderSnapshot(
                            id=ORDER_ID,
                            portfolio_snapshot_id=snapshot.id,
                            provider="fosun",
                            provider_order_id="provider-order-secret-canary",
                            instrument_id=ORDER_INSTRUMENT_ID,
                            side="sell",
                            quantity=Decimal("1000"),
                            filled_quantity=Decimal("0"),
                            price=Decimal("6.5"),
                            status="submitted",
                            updated_at=None,
                        ),
                    )
                )


async def add_snapshot_state(
    database_url: str,
    status: str,
    *,
    as_of: datetime | None,
) -> UUID:
    factory = session_factory(settings(database_url))
    snapshot_id = UUID("0198d500-1000-7000-8000-000000000099")
    async with factory() as session, session.begin():
        stale_source_id = None
        if status == "STALE":
            prior_run = BrokerSyncRun(
                id=UUID("0198d500-1000-7000-8000-000000000097"),
                account_id=ACCOUNTS[1],
                portfolio_id=PORTFOLIOS[1],
                trigger_kind="AUTOMATIC",
                actor_id="prior",
                route="broker-sync",
                idempotency_key="prior",
                request_hash="7" * 64,
                status="READY",
                accepted_at=CLOCK,
                completed_at=CLOCK,
                attempt_count=1,
                failure_reason=None,
                cash_flow_quality="NOT_RUN",
            )
            stale_source_id = UUID("0198d500-1000-7000-8000-000000000098")
            session.add(prior_run)
            session.add(snapshot_record(stale_source_id, prior_run.id, "READY", CLOCK, None))
            await session.flush()
        run = BrokerSyncRun(
            id=UUID("0198d500-1000-7000-8000-000000000096"),
            account_id=ACCOUNTS[1],
            portfolio_id=PORTFOLIOS[1],
            trigger_kind="AUTOMATIC",
            actor_id="state",
            route="broker-sync",
            idempotency_key="state",
            request_hash="8" * 64,
            status=status,
            accepted_at=CLOCK,
            completed_at=CLOCK,
            attempt_count=1,
            failure_reason=None,
            cash_flow_quality="NOT_RUN",
        )
        session.add(run)
        session.add(snapshot_record(snapshot_id, run.id, status, as_of, stale_source_id))
    return snapshot_id


async def add_cash_bucket_overflow(database_url: str) -> None:
    factory = session_factory(settings(database_url))
    async with factory() as session, session.begin():
        for index in range(32):
            currency = f"A{chr(65 + index // 26)}{chr(65 + index % 26)}"
            session.add(
                CashSnapshot(
                    portfolio_snapshot_id=UUID("0198d500-1000-7000-8000-000000000041"),
                    currency=currency,
                    cash_balance=Decimal(index),
                    available_cash=Decimal(index),
                    provider_as_of=CLOCK,
                )
            )


async def add_order_only_currency(database_url: str) -> None:
    factory = session_factory(settings(database_url))
    async with factory() as session, session.begin():
        session.add(
            Instrument(
                id=ORDER_ONLY_INSTRUMENT_ID,
                asset_type="equity",
                exchange="XNAS",
                currency="CNY",
                tick_size=Decimal("0.001"),
                risk_bucket="global_equity",
            )
        )
        session.add(
            BrokerOrderSnapshot(
                id=ORDER_ONLY_ID,
                portfolio_snapshot_id=UUID("0198d500-1000-7000-8000-000000000041"),
                provider="fosun",
                provider_order_id="order-only-currency-canary",
                instrument_id=ORDER_ONLY_INSTRUMENT_ID,
                side="buy",
                quantity=Decimal("1"),
                filled_quantity=Decimal("0"),
                price=Decimal("1"),
                status="submitted",
                updated_at=CLOCK,
            )
        )


async def broker_sync_artifact_counts(database_url: str, run_id: UUID) -> tuple[int, int]:
    factory = session_factory(settings(database_url))
    async with factory() as session:
        attempts = await session.scalar(
            select(func.count())
            .select_from(BrokerSyncAttempt)
            .where(BrokerSyncAttempt.sync_run_id == run_id)
        )
        snapshots = await session.scalar(
            select(func.count())
            .select_from(PortfolioSnapshotRecord)
            .where(PortfolioSnapshotRecord.sync_run_id == run_id)
        )
    return int(attempts or 0), int(snapshots or 0)


async def terminalize_broker_sync(
    database_url: str,
    run_id: UUID,
    *,
    status: str,
    completed_at: datetime,
    failure_reason: str | None = None,
) -> None:
    factory = session_factory(settings(database_url))
    async with factory() as session, session.begin():
        run = await session.get(BrokerSyncRun, run_id)
        assert run is not None
        run.status = status
        run.completed_at = completed_at
        run.attempt_count = 1
        run.failure_reason = failure_reason


def snapshot_record(
    snapshot_id: UUID,
    run_id: UUID,
    status: str,
    as_of: datetime | None,
    stale_source_id: UUID | None,
) -> PortfolioSnapshotRecord:
    return PortfolioSnapshotRecord(
        id=snapshot_id,
        portfolio_id=PORTFOLIOS[1],
        sync_run_id=run_id,
        stale_from_snapshot_id=stale_source_id,
        status=status,
        as_of=as_of,
        position_hash_version="broker_position_hash_v1",
        position_hash="c" * 64,
        order_hash_version="broker_order_hash_v1",
        order_hash="d" * 64,
        source_request_ids=[],
        source_observations=[],
    )
