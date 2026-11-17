"""Two real PG sessions exercise R3 commit order, waiting readers and downgrade fencing."""

import asyncio
import copy
import importlib.util
import json
from datetime import timedelta
from hashlib import sha256
from pathlib import Path

import pytest
from helpers.durable_r3_foundation import ordinary_discovery
from nexus.domain.ids import new_uuid7
from nexus.monitoring import durable_sec_research as bridge
from nexus.monitoring.durable_sec_refreeze import append_supersession
from nexus.monitoring.durable_sec_storage import _lock, _time
from nexus.persistence.models.monitoring import (
    MonitorCheck,
    MonitorObservationBatch,
    MonitorTrigger,
)
from nexus.persistence.models.sec_research import SecEventResearchBinding
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError


def values(row):
    return {
        column.name: copy.deepcopy(getattr(row, column.name)) for column in row.__table__.columns
    }


async def clone_batch(session, original, at):
    """Adversarial fully audited leaf; never fabricate a Run/output/consumption receipt."""
    bindings = tuple(
        await session.scalars(
            select(SecEventResearchBinding).where(SecEventResearchBinding.batch_id == original.id)
        )
    )
    batch_id = new_uuid7()
    checks = []
    triggers = []
    for old_binding in bindings:
        old_check = await session.get(MonitorCheck, old_binding.check_id)
        old_trigger = await session.get(MonitorTrigger, old_binding.trigger_id)
        binding_id, check_id, trigger_id = new_uuid7(), new_uuid7(), new_uuid7()
        binding = SecEventResearchBinding(
            **(
                values(old_binding)
                | dict(
                    id=binding_id,
                    check_id=check_id,
                    trigger_id=trigger_id,
                    batch_id=batch_id,
                    bound_at=at,
                    created_at=at,
                )
            )
        )
        session.add(binding)
        await session.flush([binding])
        observation = copy.deepcopy(old_check.observation)
        observation["binding_id"] = str(binding_id)
        check = MonitorCheck(
            **(
                values(old_check)
                | dict(
                    id=check_id,
                    created_at=at,
                    scheduled_at=original.scheduled_at - timedelta(minutes=5),
                    checked_at=at,
                    observation=observation,
                )
            )
        )
        session.add(check)
        await session.flush([check])
        trigger = MonitorTrigger(
            **(
                values(old_trigger)
                | dict(
                    id=trigger_id,
                    created_at=at,
                    check_id=check_id,
                    batch_id=batch_id,
                    durable_binding_id=binding_id,
                )
            )
        )
        session.add(trigger)
        await bridge._audit(session, binding)
        checks.append(str(check_id))
        triggers.append(str(trigger_id))
    row = MonitorObservationBatch(
        **(
            values(original)
            | dict(
                id=batch_id,
                created_at=at,
                scheduled_at=original.scheduled_at - timedelta(minutes=5),
                checked_at=at,
                sealed_at=at,
                check_ids=sorted(checks),
                trigger_ids=sorted(triggers),
                observation_hash=sha256(
                    json.dumps(sorted(triggers), separators=(",", ":")).encode()
                ).hexdigest(),
            )
        )
    )
    session.add(row)
    await session.flush()
    return row


async def prepared(foundation):
    async with foundation.factory() as session, session.begin():
        return await bridge.prepare_durable_sec_research_batch(
            session, account_id=foundation.pointer.account_id, rule_id=foundation.rule.id
        )


@pytest.mark.asyncio
async def test_second_leaf_older_business_time_later_commit_is_rejected(
    migrated_database_url, monkeypatch, tmp_path
):
    f = await ordinary_discovery(migrated_database_url, monkeypatch, tmp_path)
    try:
        async with f.factory() as later_committer:
            await later_committer.begin()
            earlier_business = await _time(later_committer)
            async with f.factory() as first, first.begin():
                original = await bridge.prepare_durable_sec_research_batch(
                    first, account_id=f.pointer.account_id, rule_id=f.rule.id
                )
                assert original.checked_at > earlier_business
            fake = await clone_batch(later_committer, original, earlier_business)
            assert fake.checked_at < original.checked_at
            assert (
                await later_committer.scalar(
                    select(func.count()).select_from(SecEventResearchBinding)
                )
                == 2
            )
            with pytest.raises(DBAPIError, match="SEC_REFREEZE_SUPERSESSION_REQUIRED"):
                await later_committer.commit()
            await later_committer.rollback()
        async with f.factory() as check:
            assert (
                await check.scalar(select(func.count()).select_from(SecEventResearchBinding)) == 1
            )
            assert (
                await check.scalar(
                    select(func.count())
                    .select_from(MonitorObservationBatch)
                    .where(MonitorObservationBatch.routing_version == bridge.ROUTING)
                )
                == 1
            )
    finally:
        await f.factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_reader_waiting_for_lock_rejects_superseded_but_history_exact(
    migrated_database_url, monkeypatch, tmp_path
):
    f = await ordinary_discovery(migrated_database_url, monkeypatch, tmp_path)
    original = await prepared(f)
    entered = asyncio.Event()
    reader_pid = []
    original_lock = bridge._lock

    async def reader_lock(session, rule):
        reader_pid.append(await session.scalar(text("SELECT pg_backend_pid()")))
        entered.set()
        await original_lock(session, rule)

    monkeypatch.setattr(bridge, "_lock", reader_lock)
    try:
        async with f.factory() as read:
            old_binding = await read.scalar(
                select(SecEventResearchBinding).where(
                    SecEventResearchBinding.batch_id == original.id
                )
            )
            old_check = await read.get(MonitorCheck, old_binding.check_id)
            expected = copy.deepcopy(old_check.observation)
        async with f.factory() as writer:
            await writer.begin()
            await _lock(writer, f.rule)

            async def attempt_current():
                async with f.factory() as reader, reader.begin():
                    return await bridge.read_binding_observation(
                        reader, check=old_check, plan=f.plan
                    )

            task = asyncio.create_task(attempt_current())
            await asyncio.wait_for(entered.wait(), 5)
            async with f.factory() as observer:
                for _ in range(500):
                    waiting = await observer.scalar(
                        text("SELECT wait_event='advisory' FROM pg_stat_activity WHERE pid=:pid"),
                        {"pid": reader_pid[0]},
                    )
                    if waiting:
                        break
                    await asyncio.sleep(0.01)
                assert waiting is True
            replacement = await clone_batch(writer, original, await _time(writer))
            await append_supersession(
                writer,
                old_batch_id=original.id,
                new_batch_id=replacement.id,
                now=await _time(writer),
            )
            await writer.commit()
            with pytest.raises(ValueError, match="DURABLE_SEC_RESEARCH_BATCH_SUPERSEDED"):
                await asyncio.wait_for(task, 5)
        async with f.factory() as history, history.begin():
            assert (
                await bridge.read_binding_observation(
                    history, check=old_check, plan=f.plan, current_eligibility=False
                )
                == expected
            )
    finally:
        await f.factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_130_downgrade_cannot_cross_inflight_scope_registration(
    migrated_database_url, monkeypatch, tmp_path
):
    from nexus.monitoring.auto_increment_policy import enable_scope

    f = await ordinary_discovery(migrated_database_url, monkeypatch, tmp_path)
    spec = importlib.util.spec_from_file_location(
        "scope130", Path(__file__).parents[2] / "alembic/versions/0130_auto_increment_scope.py"
    )
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    try:
        async with f.factory() as publisher:
            await publisher.begin()
            receipt = await enable_scope(
                publisher,
                account_id=f.pointer.account_id,
                candidate_batch_id=f.settings.agent_paper_candidate_batch_id,
            )
            async with f.factory() as downgrade:
                await downgrade.begin()
                assert (
                    await downgrade.scalar(
                        text(
                            "SELECT count(*) FROM data_snapshots "
                            "WHERE source='nexus_auto_increment_policy'"
                        )
                    )
                    == 0
                )
                with pytest.raises(DBAPIError) as blocked:
                    await downgrade.execute(text(migration.DOWNGRADE_SQL[0]))
                assert getattr(blocked.value.orig, "sqlstate", None) == "55P03"
                await downgrade.rollback()
            await publisher.commit()
        async with f.factory() as downgrade:
            await downgrade.begin()
            await downgrade.execute(text(migration.DOWNGRADE_SQL[0]))
            with pytest.raises(DBAPIError, match="AUTO_INCREMENT_SCOPE_DOWNGRADE_FORBIDDEN"):
                await downgrade.execute(text(migration.DOWNGRADE_SQL[1]))
            await downgrade.rollback()
        async with f.factory() as check:
            assert (
                await check.scalar(
                    text(
                        "SELECT count(*) FROM data_snapshots "
                        "WHERE source='nexus_auto_increment_policy'"
                    )
                )
                == 1
            )
            assert receipt.id is not None
    finally:
        await f.factory.kw["bind"].dispose()
