"""Public lifecycle consumers of one immutable staged proof, isolated PostgreSQL."""

from datetime import timedelta

import pytest
from helpers.staged_public import install_public_readers
from integration.test_staged_worker_pg import ObservedCloseout, exercise
from nexus.monitoring.admission import finish_increment
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.monitoring import MonitorIncrement, MonitorTrigger
from nexus.persistence.models.runtime import Job, JobLease
from sqlalchemy import select, text


@pytest.mark.asyncio
@pytest.mark.parametrize("known", [True, False])
@pytest.mark.parametrize("cleanup", [True, False])
async def test_orphan_public_finish_and_admission_share_frozen_proof(
    migrated_database_url, monkeypatch, tmp_path, known, cleanup, late_cleanup=False
):
    install_public_readers(monkeypatch)

    async def closed(factory, settings, captured):
        from nexus.monitoring.admission import require_monitor_account_available
        from nexus.paper.history_seal import _row_hash

        async with factory() as session:
            member = await session.scalar(select(MonitorIncrement))
            lease = await session.scalar(select(JobLease).where(JobLease.job_id == member.job_id))
            now = lease.expires_at + timedelta(seconds=1)
        assert await captured["protocol"].close(captured["claim"], now=now, orphan=True) is known
        async with factory() as session:
            run = await session.get(AgentResearchRunRecord, member.run_id)
            budget = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == run.id
                )
            )
            before = (_row_hash(run), _row_hash(budget))
            assert budget.state == ("settled" if known else "reserved")
            assert (await session.get(Job, member.job_id)).status == "dead_letter"
        async with factory() as session:
            terminal_before = dict(
                (
                    await session.execute(
                        text("SELECT * FROM stage_proto_terminal WHERE run_id=:run"),
                        {"run": member.run_id},
                    )
                )
                .mappings()
                .one()
            )
            outputs_before = list(
                (
                    await session.execute(
                        text(
                            "SELECT id,content_hash FROM agent_research_outputs "
                            "WHERE run_id=:run ORDER BY id"
                        ),
                        {"run": member.run_id},
                    )
                ).all()
            )
            stages = list(
                await session.scalars(
                    text(
                        "SELECT stage FROM stage_proto_authorization "
                        "WHERE run_id=:run ORDER BY stage"
                    ),
                    {"run": member.run_id},
                )
            )
        sdk_before = captured.get("sdk_run_calls", 0)
        async with factory() as session:
            counts_before = []
            for table in ("job_attempts", "job_leases", "research_monitor_plans"):
                counts_before.append(await session.scalar(text(f"SELECT count(*) FROM {table}")))
        if late_cleanup:
            from dataclasses import replace
            from uuid import uuid4

            from helpers.staged_protocol import ProtocolRejected, Ticket

            for bad in (
                replace(captured["claim"], lease_token=uuid4()),
                replace(captured["claim"], attempt_no=captured["claim"].attempt_no + 1),
            ):
                with pytest.raises(ProtocolRejected, match="CLEANUP_BINDING_INVALID"):
                    await captured["protocol"].cleanup(
                        Ticket(member.run_id, stages[0], bad), now=now, confirmed=True
                    )

            for stage in stages:
                await captured["protocol"].cleanup(
                    Ticket(member.run_id, stage, captured["claim"]),
                    now=now,
                    confirmed=True,
                )
        async with factory.begin() as session:
            result = await finish_increment(
                session, account_id=member.account_id, trigger_id=member.trigger_id, now=now
            )
            assert (
                result.status == ("failed" if known else "unknown")
                if cleanup or late_cleanup
                else result.status == "admitted"
            )
        async with factory.begin() as session:
            triggers = tuple(
                await session.scalars(
                    select(MonitorTrigger).where(MonitorTrigger.research_run_id == member.run_id)
                )
            )
            assert len(triggers) == 3
            assert len({(t.status, t.reason) for t in triggers}) == 1
            if known and (cleanup or late_cleanup):
                await require_monitor_account_available(
                    session, account_id=member.account_id, now=now
                )
            else:
                with pytest.raises(ValueError, match="MONITOR_ACCOUNT_BUSY_OR_UNKNOWN"):
                    await require_monitor_account_available(
                        session, account_id=member.account_id, now=now
                    )
            run = await session.get(AgentResearchRunRecord, member.run_id)
            budget = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == run.id
                )
            )
            assert (_row_hash(run), _row_hash(budget)) == before
            assert (
                dict(
                    (
                        await session.execute(
                            text("SELECT * FROM stage_proto_terminal WHERE run_id=:run"),
                            {"run": member.run_id},
                        )
                    )
                    .mappings()
                    .one()
                )
                == terminal_before
            )
            assert (
                list(
                    (
                        await session.execute(
                            text(
                                "SELECT id,content_hash FROM agent_research_outputs "
                                "WHERE run_id=:run ORDER BY id"
                            ),
                            {"run": member.run_id},
                        )
                    ).all()
                )
                == outputs_before
            )
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM audit_events "
                        "WHERE action='paper.legacy_research.sealed'"
                    )
                )
                == 0
            )
        assert captured.get("sdk_run_calls", 0) == sdk_before
        async with factory() as session:
            counts_after = []
            for table in ("job_attempts", "job_leases", "research_monitor_plans"):
                counts_after.append(await session.scalar(text(f"SELECT count(*) FROM {table}")))
            assert counts_after == counts_before
        raise ObservedCloseout

    controls = {"terminal_commit_fault": False, "post_worker_hook": closed}
    if not known:
        controls["observation_mutation"] = "tokens"
    if not cleanup:
        controls["sdk_cleanup_failure"] = True
    with pytest.raises(ObservedCloseout):
        await exercise(
            migrated_database_url, monkeypatch, tmp_path, "unresolved", controls=controls
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("known", [True, False])
async def test_late_cleanup_changes_lifecycle_not_frozen_accounting(
    migrated_database_url, monkeypatch, tmp_path, known
):
    await test_orphan_public_finish_and_admission_share_frozen_proof(
        migrated_database_url, monkeypatch, tmp_path, known, False, late_cleanup=True
    )
