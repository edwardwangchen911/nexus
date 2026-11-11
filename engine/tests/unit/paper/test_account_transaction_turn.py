"""Same-host scheduling happens before DB transactions; it grants no send authority."""

import asyncio
from importlib import import_module
from uuid import UUID

import pytest
from nexus.settings import EngineSettings


def settings(*, account=1, database="one"):
    return EngineSettings.for_test().model_copy(
        update={
            "database_url": f"postgresql+asyncpg://fixture:fixture@127.0.0.1:5432/{database}",
            "agent_paper_account_id": UUID(int=account),
        }
    )


@pytest.mark.asyncio
async def test_waiter_cancellation_does_not_release_active_account_turn():
    turn = import_module("nexus.paper.account_transaction_turn").account_transaction_turn
    entered = asyncio.Event()
    release = asyncio.Event()
    order = []

    async def owner():
        async with turn(settings()):
            order.append("scan")
            entered.set()
            await release.wait()
            order.append("scan_closed")

    async def grant(name):
        async with turn(settings()):
            order.append(name)

    task = asyncio.create_task(owner())
    await entered.wait()
    canceled = asyncio.create_task(grant("canceled"))
    await asyncio.sleep(0)
    canceled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await canceled
    successor = asyncio.create_task(grant("grant"))
    await asyncio.sleep(0)
    assert order == ["scan"]
    release.set()
    await asyncio.gather(task, successor)
    assert order == ["scan", "scan_closed", "grant"]


@pytest.mark.asyncio
async def test_accounts_and_databases_do_not_exclude_each_other():
    turn = import_module("nexus.paper.account_transaction_turn").account_transaction_turn
    async with turn(settings()):
        for other in (settings(account=2), settings(database="two")):
            async with turn(other):
                pass


@pytest.mark.asyncio
async def test_failure_releases_turn_for_original_next_operation():
    turn = import_module("nexus.paper.account_transaction_turn").account_transaction_turn
    with pytest.raises(RuntimeError, match="original failure"):
        async with turn(settings()):
            raise RuntimeError("original failure")
    async with turn(settings()):
        pass


@pytest.mark.asyncio
async def test_stopped_origin_is_rechecked_after_wait_without_opening_database():
    from types import SimpleNamespace

    from nexus.agents.research_stages.protocol import ProtocolRejected, StagedProtocol

    turn = import_module("nexus.paper.account_transaction_turn").account_transaction_turn

    def no_database():
        raise AssertionError("stopped origin must not open a DB session")

    protocol = StagedProtocol(no_database, native_settings=settings())
    claim = SimpleNamespace(job_id=UUID(int=7))
    protocol.claims[claim.job_id] = claim
    async with turn(settings()):
        task = asyncio.create_task(
            protocol.authorize(
                claim,
                stage=2,
                now=__import__("datetime").datetime.now(__import__("datetime").UTC),
                wire=b"x",
            )
        )
        await asyncio.sleep(0)
        protocol.local_stopped = True
    with pytest.raises(ProtocolRejected, match="ORIGIN_STOPPED_OR_UNAVAILABLE"):
        await task


@pytest.mark.asyncio
async def test_explicit_request_owner_shares_monitor_turn_without_configured_owner():
    turn = import_module("nexus.paper.account_transaction_turn").account_transaction_turn
    from types import SimpleNamespace

    entered = asyncio.Event()
    request_settings = SimpleNamespace(database_url=settings().database_url)

    async def root():
        async with turn(request_settings, account_id=UUID(int=1)):
            entered.set()

    async with turn(settings()):
        task = asyncio.create_task(root())
        await asyncio.sleep(0)
        assert not entered.is_set()
    await task
    assert entered.is_set()


@pytest.mark.asyncio
async def test_nested_turn_is_owned_by_exact_task_and_child_still_waits():
    turn = import_module("nexus.paper.account_transaction_turn").account_transaction_turn
    entered = asyncio.Event()

    async def child():
        async with turn(settings()):
            entered.set()

    async with turn(settings()):
        async with turn(settings()):
            task = asyncio.create_task(child())
            await asyncio.sleep(0)
            assert not entered.is_set()
        await asyncio.sleep(0)
        assert not entered.is_set()
    await task
    assert entered.is_set()
