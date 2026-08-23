import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from helpers.broker_seed import (
    INSTRUMENT_ID,
    _seed_account_and_portfolio,
    _seed_fosun_capability,
)
from helpers.broker_snapshot import _snapshot, _sync_result
from helpers.broker_storage import _insert_run, _insert_snapshot
from nexus.integrations.service import IntegrationStatusService
from nexus.persistence.models.data import ProviderCapability
from nexus.persistence.models.runtime import IntegrationStatus
from nexus.persistence.session import session_factory
from nexus.reconciliation.persistence import BrokerSyncRepository, SnapshotNotProposalEligible
from nexus.settings import EngineSettings
from sqlalchemy import update

NOW = datetime(2026, 8, 23, 4, 0, tzinfo=UTC)
ACCOUNT_ID = UUID("0198d400-1000-7000-8000-000000000001")
PORTFOLIO_ID = UUID("0198d400-1000-7000-8000-000000000002")
READY_SNAPSHOT_ID = UUID("0198d400-1000-7000-8000-000000000003")
DISABLED_RUN_ID = UUID("0198d400-1000-7000-8000-000000000006")


async def _ready(
    factory,
    *,
    account_status: str = "ACTIVE",
    operations: tuple[str, ...] = ("fosun_sync_portfolio", "fosun_list_orders"),
) -> UUID:
    instrument_id = await _seed_account_and_portfolio(factory, account_status=account_status)
    await _seed_fosun_capability(
        factory,
        expires_at=NOW + timedelta(hours=1),
        operations=operations,
    )
    if account_status == "DISABLED":
        async with factory() as session, session.begin():
            await _insert_run(
                session, DISABLED_RUN_ID, ACCOUNT_ID, PORTFOLIO_ID, "READY", "d"
            )
            await _insert_snapshot(
                session,
                READY_SNAPSHOT_ID,
                PORTFOLIO_ID,
                DISABLED_RUN_ID,
                "READY",
                None,
            )
        return READY_SNAPSHOT_ID
    async with factory() as session, session.begin():
        admitted = await BrokerSyncRepository(session).admit(
            account_id=ACCOUNT_ID,
            portfolio_id=PORTFOLIO_ID,
            trigger_kind="AUTOMATIC",
            actor_id="synthetic-worker",
            route="broker-sync",
            idempotency_key="proposal-ready",
            request_hash="1" * 64,
            accepted_at=NOW,
        )
        await BrokerSyncRepository(session).finalize(
            _sync_result(
                admitted.run.id,
                _snapshot(instrument_id, READY_SNAPSHOT_ID, "READY"),
            ),
            completed_at=NOW,
        )
    return READY_SNAPSHOT_ID


@pytest.mark.asyncio
async def test_only_latest_ready_snapshot_without_active_sync_is_eligible(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _ready(factory)
    async with factory() as session:
        assert (
            await BrokerSyncRepository(session).require_proposal_baseline(
                portfolio_id=PORTFOLIO_ID,
                snapshot_id=READY_SNAPSHOT_ID,
                now=NOW,
            )
            == READY_SNAPSHOT_ID
        )
    async with factory() as session, session.begin():
        active = await BrokerSyncRepository(session).admit(
            account_id=ACCOUNT_ID,
            portfolio_id=PORTFOLIO_ID,
            trigger_kind="AUTOMATIC",
            actor_id="synthetic-worker",
            route="broker-sync",
            idempotency_key="proposal-active",
            request_hash="2" * 64,
            accepted_at=NOW + timedelta(seconds=1),
        )
    async with factory() as session:
        with pytest.raises(SnapshotNotProposalEligible, match="active"):
            await BrokerSyncRepository(session).require_proposal_baseline(
                portfolio_id=PORTFOLIO_ID,
                snapshot_id=READY_SNAPSHOT_ID,
                now=NOW + timedelta(seconds=1),
            )
    partial_snapshot_id = UUID("0198d400-1000-7000-8000-000000000007")
    async with factory() as session, session.begin():
        await BrokerSyncRepository(session).finalize(
            _sync_result(
                active.run.id,
                _snapshot(INSTRUMENT_ID, partial_snapshot_id, "PARTIAL"),
                count=2,
            ),
            completed_at=NOW + timedelta(seconds=2),
        )
    async with factory() as session:
        with pytest.raises(SnapshotNotProposalEligible, match="latest"):
            await BrokerSyncRepository(session).require_proposal_baseline(
                portfolio_id=PORTFOLIO_ID,
                snapshot_id=partial_snapshot_id,
                now=NOW + timedelta(seconds=2),
            )


@pytest.mark.asyncio
async def test_proposal_requires_portfolio_and_order_capabilities(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _ready(factory, operations=("fosun_sync_portfolio",))

    async with factory() as session:
        with pytest.raises(SnapshotNotProposalEligible, match="capability"):
            await BrokerSyncRepository(session).require_proposal_baseline(
                portfolio_id=PORTFOLIO_ID,
                snapshot_id=READY_SNAPSHOT_ID,
                now=NOW,
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("revocation", "message"),
    (
        ("fosun_disabled", "capability"),
        ("latest_hash_mismatch", "capability"),
        ("authority_invalidated", "capability"),
        ("account_disabled", "not active"),
    ),
)
async def test_proposal_rechecks_authority_after_snapshot_commit(
    migrated_database_url: str,
    revocation: str,
    message: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    account_status = "DISABLED" if revocation == "account_disabled" else "ACTIVE"
    await _ready(factory, account_status=account_status)
    if revocation != "account_disabled":
        async with factory() as session, session.begin():
            if revocation == "fosun_disabled":
                await IntegrationStatusService(session, NOW + timedelta(seconds=1)).write_status(
                    "fosun",
                    mode="disabled",
                    expected_version=1,
                    last_success_at=NOW,
                    admitted_contract_hash=None,
                )
            elif revocation == "latest_hash_mismatch":
                session.add(
                    ProviderCapability(
                        provider="fosun",
                        capability="READ_ONLY",
                        contract_hash="b" * 64,
                        expires_at=NOW + timedelta(hours=2),
                        schema_version="1",
                        payload={
                            "operation": "fosun_sync_portfolio",
                            "market_scope": "BROKER",
                        },
                        created_at=NOW + timedelta(seconds=1),
                    )
                )
            else:
                await session.execute(
                    update(IntegrationStatus)
                    .where(IntegrationStatus.provider == "fosun")
                    .values(capability_invalidated_at=NOW)
                )
    async with factory() as session:
        with pytest.raises(SnapshotNotProposalEligible, match=message):
            await BrokerSyncRepository(session).require_proposal_baseline(
                portfolio_id=PORTFOLIO_ID,
                snapshot_id=READY_SNAPSHOT_ID,
                now=NOW + timedelta(seconds=2),
            )


@pytest.mark.asyncio
async def test_proposal_transaction_blocks_sync_disable_and_capability_change(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _ready(factory)
    guarded = asyncio.Event()
    release = asyncio.Event()
    started = asyncio.Event()
    start_count = 0

    async def proposal() -> UUID:
        async with factory() as session, session.begin():
            baseline = await BrokerSyncRepository(session).require_proposal_baseline(
                portfolio_id=PORTFOLIO_ID, snapshot_id=READY_SNAPSHOT_ID, now=NOW
            )
            guarded.set()
            await release.wait()
            return baseline

    async def change(kind: str) -> None:
        nonlocal start_count
        start_count += 1
        if start_count == 3:
            started.set()
        async with factory() as session, session.begin():
            if kind == "sync":
                await BrokerSyncRepository(session).admit(
                    account_id=ACCOUNT_ID,
                    portfolio_id=PORTFOLIO_ID,
                    trigger_kind="AUTOMATIC",
                    actor_id="synthetic-worker",
                    route="broker-sync",
                    idempotency_key="proposal-barrier",
                    request_hash="3" * 64,
                    accepted_at=NOW + timedelta(seconds=1),
                )
            elif kind == "disable":
                await IntegrationStatusService(session, NOW + timedelta(seconds=1)).write_status(
                    "fosun", mode="disabled", expected_version=1,
                    last_success_at=NOW, admitted_contract_hash=None,
                )
            else:
                session.add(
                    ProviderCapability(
                        provider="fosun", capability="READ_ONLY", contract_hash="b" * 64,
                        expires_at=NOW + timedelta(hours=2), schema_version="1",
                        payload={"operation": "fosun_sync_portfolio", "market_scope": "BROKER"},
                        created_at=NOW + timedelta(seconds=1),
                    )
                )
                await session.flush()

    proposal_task = asyncio.create_task(proposal())
    await asyncio.wait_for(guarded.wait(), timeout=2)
    changes = [asyncio.create_task(change(kind)) for kind in ("sync", "disable", "capability")]
    await asyncio.wait_for(started.wait(), timeout=2)
    await asyncio.sleep(0.05)
    assert not any(task.done() for task in changes)
    release.set()
    assert await asyncio.wait_for(proposal_task, timeout=5) == READY_SNAPSHOT_ID
    await asyncio.wait_for(asyncio.gather(*changes), timeout=5)


def test_repository_has_no_callback_controlled_proposal_insert_api() -> None:
    assert not hasattr(BrokerSyncRepository, "insert_proposal_with_baseline")
