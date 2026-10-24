"""Real installer lock ordering with a controlled approved-source boundary.

The normal mixed lifecycle test independently proves source qualification and
consumption. This test isolates PostgreSQL visibility after a pre-lock request.
"""

import asyncio
from datetime import timedelta
from time import monotonic

import pytest
from integration.test_monitor_plan_replacement_pg import synthetic_source
from integration.test_research_monitoring_pg import seed
from nexus.monitoring import installation
from nexus.monitoring.storage import lock_security
from sqlalchemy import func, select, text


@pytest.mark.asyncio
async def test_plan_records_postlock_visibility_instead_of_transaction_start(
    migrated_database_url, monkeypatch
):
    factory, prior = await seed(migrated_database_url)
    source = await synthetic_source(factory, prior, minute=1, threshold="200")
    # Restore the production default before either concurrent transaction starts.
    # The shared dated template otherwise fixes created_at to a synthetic date.
    async with factory() as session, session.begin():
        await session.execute(
            text("ALTER TABLE research_monitor_plans ALTER COLUMN created_at SET DEFAULT now()")
        )
    async with factory() as session:
        current = await session.scalar(select(func.clock_timestamp()))
    source = source.__class__(
        source.run,
        source.output,
        source.proposal.model_copy(
            update={
                "review_due_at": current + timedelta(hours=1),
                "valid_until": current + timedelta(hours=2),
            }
        ),
        source.context,
    )
    selected, release = asyncio.Event(), asyncio.Event()
    state = {}

    async def approved(*args, **kwargs):
        selected.set()
        await release.wait()
        return source

    monkeypatch.setattr(installation, "load_qualified_source", approved)
    monkeypatch.setattr(installation, "load_approved_source", approved)

    async def install():
        async with factory() as session, session.begin():
            state["pid"] = await session.scalar(text("SELECT pg_backend_pid()"))
            state["requested"] = await session.scalar(select(func.clock_timestamp()))
            return await installation.install_plan_from_output(
                session,
                account_id=prior.account_id,
                output_id=source.output.id,
                expected_predecessor_id=prior.id,
                now=state["requested"],
            )

    task = asyncio.create_task(install())
    try:
        await asyncio.wait_for(selected.wait(), timeout=30)
        async with factory() as freezing, freezing.begin():
            await lock_security(
                freezing, account_id=prior.account_id, instrument_id=prior.instrument_id
            )
            holder = await freezing.scalar(text("SELECT pg_backend_pid()"))
            cutoff = await freezing.scalar(select(func.clock_timestamp()))
            assert state["requested"] < cutoff
            release.set()
            deadline = monotonic() + 30
            async with factory() as observer:
                while True:
                    blockers = await observer.scalar(
                        text("SELECT pg_blocking_pids(:pid)"), {"pid": state["pid"]}
                    )
                    if holder in blockers:
                        break
                    assert not task.done(), "installer must reach the held security lock"
                    assert monotonic() < deadline, "database did not expose the expected blocker"
                    await asyncio.sleep(0.05)
        installed = await asyncio.wait_for(task, timeout=30)
        assert installed.effective_at == state["requested"] < cutoff
        assert installed.created_at > cutoff
        async with factory() as session:
            persisted = await session.get(type(prior), installed.id)
            assert persisted.created_at == installed.created_at
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await factory.kw["bind"].dispose()
