"""Synthetic stored baseline and diagnostic for initial draft tests."""

from datetime import timedelta
from decimal import Decimal
from uuid import UUID

from helpers.portfolio_api import CLOCK, settings
from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID
from nexus.persistence.models.diagnostics import (
    PortfolioDiagnosticEvidenceRefRecord,
    PortfolioDiagnosticItemRecord,
    PortfolioDiagnosticReportRecord,
)
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.portfolio import (
    Account,
    BrokerSyncRun,
    CashSnapshot,
    Portfolio,
    PortfolioSnapshotRecord,
    PositionSnapshot,
)
from nexus.persistence.session import session_factory


async def seed_initial_draft(database_url: str, invalid: str = "") -> tuple[UUID, UUID]:
    at = CLOCK - timedelta(minutes=1)
    async with session_factory(settings(database_url))() as db, db.begin():
        account = Account(provider="fosun", status="ACTIVE")
        db.add(account)
        await db.flush()
        portfolio = Portfolio(account_id=account.id, name="private-draft-canary")
        db.add(portfolio)
        await db.flush()
        sync = BrokerSyncRun(
            account_id=account.id,
            portfolio_id=portfolio.id,
            trigger_kind="MANUAL",
            actor_id="test",
            route="test",
            idempotency_key="seed",
            request_hash="a" * 64,
            status="READY",
            accepted_at=at,
            completed_at=at,
            attempt_count=1,
            cash_flow_quality="NOT_RUN" if invalid == "cash_flow" else "PASS",
        )
        db.add(sync)
        await db.flush()
        snapshot = PortfolioSnapshotRecord(
            portfolio_id=portfolio.id,
            sync_run_id=sync.id,
            status="READY",
            as_of=at,
            position_hash_version="broker_position_hash_v1",
            position_hash="b" * 64,
            order_hash_version="broker_order_hash_v1",
            order_hash="c" * 64,
            source_request_ids=[],
            source_observations=[],
        )
        db.add(snapshot)
        await db.flush()
        db.add(
            CashSnapshot(
                portfolio_snapshot_id=snapshot.id,
                currency="HKD" if invalid == "foreign_cash" else "USD",
                cash_balance=1 if invalid == "foreign_cash" else 0,
                available_cash=0,
                provider_as_of=at,
            )
        )
        instrument_ids = tuple(
            UUID(int=801 + index) for index in range(2 if invalid == "insufficient_names" else 5)
        )
        for index, instrument_id in enumerate(instrument_ids):
            currency = "HKD" if invalid == "mixed_currency" and index == 1 else "USD"
            non_equity = (
                invalid == "non_equity_without_sector" and index == len(instrument_ids) - 1
            )
            if invalid == "zero_quantity" and index == len(instrument_ids) - 1:
                quantity = 0
            elif invalid == "negative_quantity" and index == len(instrument_ids) - 1:
                quantity = -1
            elif index == 0:
                quantity = 60
            elif invalid == "insufficient_names":
                quantity = 40
            else:
                quantity = 10
            db.add(
                Instrument(
                    id=instrument_id,
                    asset_type="exchange_traded_note" if non_equity else "equity",
                    exchange="XNAS",
                    currency=currency,
                    tick_size=Decimal("0.01"),
                    risk_bucket="EXCHANGE_TRADED_NOTE" if non_equity else "us_equity",
                )
            )
            await db.flush()
            db.add(
                InstrumentAlias(
                    instrument_id=instrument_id,
                    symbol=f"TEST{index}",
                    alias_type="ticker",
                    valid_from=at.date(),
                )
            )
            db.add(
                PositionSnapshot(
                    portfolio_snapshot_id=snapshot.id,
                    instrument_id=instrument_id,
                    currency=currency,
                    quantity=quantity,
                    available_quantity=quantity,
                    frozen_quantity=0,
                    current_price=None if invalid == "missing_price" else 1,
                    provider_as_of=at,
                )
            )
        report = PortfolioDiagnosticReportRecord(
            portfolio_id=portfolio.id,
            portfolio_snapshot_id=snapshot.id,
            snapshot_as_of=at,
            trading_day=at.date(),
            policy_id=REAL_ADVISORY_POLICY_ID,
            policy_version="real_advisory_policy_v1",
            optimization_need="BLOCKED" if invalid == "blocked_report" else "RESEARCH_REQUIRED",
            diagnostic_input_hash="d" * 64,
        )
        db.add(report)
        await db.flush()
        metrics = [("CONCENTRATION_TOP1", "0.60", "0.30", "ratio", "critical", "reduce_only")]
        if invalid != "missing_liquidity":
            metrics += [
                (f"LIQUIDITY_DAYS_TO_EXIT:{i}", "1", "2", "trading_days", "info", "monitor")
                for i in instrument_ids
            ]
        for code, value, limit, unit, severity, action in metrics:
            item = PortfolioDiagnosticItemRecord(
                report_id=report.id,
                diagnostic_code=code,
                metric_value=Decimal(value),
                unit=unit,
                reference_limit=Decimal(limit),
                policy_scope="currency:USD",
                severity=severity,
                actionability=action,
            )
            db.add(item)
            await db.flush()
            db.add(
                PortfolioDiagnosticEvidenceRefRecord(
                    item_id=item.id,
                    source="fixture",
                    object_id=code,
                    available_at=CLOCK if invalid == "future_evidence" else at,
                    location=f"fixture://{item.id}",
                    content_hash="e" * 64,
                )
            )
        return snapshot.id, report.id
