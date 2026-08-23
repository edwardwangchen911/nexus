from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from helpers.broker_cashflow import _cash_flow_result, _stable_cash_flow_result
from helpers.broker_seed import ACCOUNT_ID, NOW, PORTFOLIO_ID, _seed_account_and_portfolio
from helpers.broker_snapshot import _snapshot, _sync_result
from nexus.persistence.models.portfolio import (
    BrokerCashFlow,
    BrokerCashFlowWatermark,
    BrokerSyncRun,
)
from nexus.persistence.session import session_factory
from nexus.reconciliation.cash_flow_persistence import persist_cash_flows
from nexus.reconciliation.cash_flows import CashFlowSyncResult
from nexus.reconciliation.persistence import BrokerSyncRepository
from nexus.reconciliation.persistence_errors import BrokerSyncPersistenceError
from nexus.settings import EngineSettings
from sqlalchemy import func, select


async def _finalize(factory, index: int, snapshot_id: UUID, instrument_id: UUID, result) -> UUID:
    async with factory() as session, session.begin():
        repository = BrokerSyncRepository(session)
        admitted = await repository.admit(
            account_id=ACCOUNT_ID,
            portfolio_id=PORTFOLIO_ID,
            trigger_kind="AUTOMATIC",
            actor_id="synthetic-worker",
            route="broker-sync",
            idempotency_key=f"cash-flow-{index}",
            request_hash=str(index) * 64,
            accepted_at=NOW + timedelta(seconds=index),
        )
        await repository.finalize(
            _sync_result(admitted.run.id, _snapshot(instrument_id, snapshot_id, "READY")),
            cash_flow_result=result,
            completed_at=NOW + timedelta(seconds=index),
        )
        return admitted.run.id


@pytest.mark.asyncio
async def test_complete_overlap_advances_watermark_but_interruption_and_conflict_do_not(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    instrument_id = await _seed_account_and_portfolio(factory)
    first = _cash_flow_result(watermark=date(2026, 8, 23))
    await _finalize(factory, 1, UUID("0198d400-6100-7000-8000-000000000001"), instrument_id, first)
    await _finalize(
        factory,
        2,
        UUID("0198d400-6100-7000-8000-000000000002"),
        instrument_id,
        _cash_flow_result(watermark=date(2026, 8, 24)),
    )
    interrupted = CashFlowSyncResult(first.facts, "PARTIAL", False, None, ())
    await _finalize(
        factory,
        3,
        UUID("0198d400-6100-7000-8000-000000000003"),
        instrument_id,
        interrupted,
    )
    conflict = _stable_cash_flow_result(amount=Decimal("11"))
    conflict = replace(conflict, watermark_date=date(2026, 8, 25))
    conflict_run = await _finalize(
        factory,
        4,
        UUID("0198d400-6100-7000-8000-000000000004"),
        instrument_id,
        conflict,
    )

    async with factory() as session:
        watermark = await session.scalar(
            select(BrokerCashFlowWatermark).where(BrokerCashFlowWatermark.account_id == ACCOUNT_ID)
        )
        count = await session.scalar(select(func.count()).select_from(BrokerCashFlow))
        run = await session.get(BrokerSyncRun, conflict_run)
    assert (count, run.cash_flow_quality if run else None) == (2, "PARTIAL")
    assert watermark is not None
    assert (watermark.provider_business_date, watermark.version) == (date(2026, 8, 24), 2)


@pytest.mark.asyncio
async def test_same_day_fallback_downgrades_watermark_and_non_ready_is_rejected(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    instrument_id = await _seed_account_and_portfolio(factory)
    async with factory() as session:
        with pytest.raises(BrokerSyncPersistenceError, match="require a READY"):
            async with session.begin():
                repository = BrokerSyncRepository(session)
                admitted = await repository.admit(
                    account_id=ACCOUNT_ID,
                    portfolio_id=PORTFOLIO_ID,
                    trigger_kind="AUTOMATIC",
                    actor_id="synthetic-worker",
                    route="broker-sync",
                    idempotency_key="cash-flow-partial",
                    request_hash="a" * 64,
                    accepted_at=NOW,
                )
                partial = _snapshot(
                    instrument_id,
                    UUID("0198d400-6100-7000-8000-000000000005"),
                    "PARTIAL",
                )
                await repository.finalize(
                    _sync_result(admitted.run.id, partial, count=2),
                    cash_flow_result=_stable_cash_flow_result(),
                    completed_at=NOW,
                )
    run_id = UUID("0198d400-6100-7000-8000-000000000006")
    async with factory() as session, session.begin():
        session.add(
            BrokerSyncRun(
                id=run_id,
                account_id=ACCOUNT_ID,
                portfolio_id=PORTFOLIO_ID,
                trigger_kind="AUTOMATIC",
                actor_id="synthetic",
                route="broker-sync",
                idempotency_key="watermark",
                request_hash="b" * 64,
                status="ACTIVE",
                accepted_at=NOW,
                attempt_count=0,
                cash_flow_quality="NOT_RUN",
            )
        )
    stable = _stable_cash_flow_result()
    fallback_fact = replace(
        stable.facts[0],
        provider_cash_flow_id="fallback:v1:" + "c" * 64,
        identity_version="broker_cash_flow_fallback_v1",
        identity_quality="FALLBACK_PARTIAL",
    )
    fallback = replace(stable, facts=(fallback_fact,), quality="PARTIAL")
    for result in (stable, fallback):
        async with factory() as session, session.begin():
            run = await session.get(BrokerSyncRun, run_id)
            assert run is not None
            await persist_cash_flows(session, run, "fosun", result)
    async with factory() as session:
        watermark = await session.scalar(select(BrokerCashFlowWatermark))
    assert watermark is not None
    assert (watermark.identity_quality, watermark.version) == ("FALLBACK_PARTIAL", 2)
