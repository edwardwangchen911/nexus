"""Synthetic normal admission: a single affordable position is not an affordable round."""

import asyncio
import json
from argparse import Namespace
from decimal import Decimal
from uuid import UUID

import pytest
from integration.test_platform_funding_pg import funding_fixture
from integration.test_platform_producer_pg import setup
from nexus.agents import persistence
from nexus.agents.comparison_manifest import PLATFORM_QUALIFICATION_PURPOSE
from nexus.agents.comparison_producer import admit_comparison_position
from nexus.agents.comparison_registration import register_comparison
from nexus.agents.persistence import BudgetExhausted
from nexus.agents.platform_schema import CURRENT_PLATFORM_UNITS_VERSION
from nexus.agents.platform_usage import CURRENT_PLATFORM_PRICE_VERSION
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text


async def snapshot(factory):
    async with factory() as session:
        return {
            table: list(
                await session.scalars(
                    text(f"SELECT to_jsonb(t) FROM {table} t ORDER BY to_jsonb(t)::text")
                )
            )
            for table in (
                "research_comparison_manifest",
                "research_comparison_chain",
                "research_stage_scope",
                "research_stage_member",
                "agent_research_runs",
                "agent_daily_budget_reservations",
                "jobs",
                "audit_events",
            )
        }


@pytest.mark.asyncio
@pytest.mark.parametrize("stable", [False, True], ids=["legacy", "stable"])
@pytest.mark.parametrize("capacity", ["one_only", "whole_round"])
async def test_normal_admission_checks_whole_unreserved_round_without_reserving_future_positions(
    migrated_database_url, monkeypatch, tmp_path, stable, capacity
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        source, prepared, manifest, model, held, _ = await setup(
            factory,
            monkeypatch,
            price_version=CURRENT_PLATFORM_PRICE_VERSION,
            units_version=CURRENT_PLATFORM_UNITS_VERSION,
            purpose=PLATFORM_QUALIFICATION_PURPOSE if stable else None,
        )
        manifest, funding = await funding_fixture(
            factory, manifest, tmp_path, monkeypatch, explicit_spend=True
        )
        if stable:
            async with factory.begin() as session:
                await register_comparison(session, manifest, allow_new_round=True)
        before = await snapshot(factory)
        old_hold = next(
            r for r in before["agent_daily_budget_reservations"] if r["id"] == str(held)
        )
        limit = Decimal(2) + (
            manifest.reservation_for(1)
            if capacity == "one_only"
            else manifest.total_reservation_usd
        )

        async def admit(ordinal):
            async with factory.begin() as session:
                return await admit_comparison_position(
                    session,
                    source=source,
                    prepared=prepared,
                    manifest=manifest,
                    ordinal=ordinal,
                    model_config_id=model,
                    daily_limit_usd=limit,
                    platform_funding=funding,
                )

        if capacity == "one_only":
            # Before the fix this commits the first Run/Job despite a known shortfall.
            with pytest.raises(BudgetExhausted, match="COMPARISON_ROUND_BUDGET_EXHAUSTED"):
                await admit(1)
            assert await snapshot(factory) == before
        else:
            first = await admit(1)
            after_first = await snapshot(factory)
            added = [r for r in after_first["agent_daily_budget_reservations"] if r != old_hold]
            assert len(added) == 1 and added[0]["id"] == str(first.reservation_id)
            assert Decimal(str(added[0]["reserved_usd"])) == manifest.reservation_for(1)
            # Already admitted positions are counted by the shared ledger, not counted twice.
            await admit(2)
            await admit(3)
            after = await snapshot(factory)
            assert len(after["research_comparison_chain"]) == 3
            assert (
                sum(
                    Decimal(str(r["reserved_usd"]))
                    for r in after["agent_daily_budget_reservations"]
                )
                == limit
            )
            assert (
                next(r for r in after["agent_daily_budget_reservations"] if r["id"] == str(held))
                == old_hold
            )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("audit_failure", [None, "before_write", "after_commit"])
async def test_normal_cli_preserves_budget_refusal_and_can_admit_only_unconsumed_position(
    migrated_database_url, monkeypatch, tmp_path, audit_failure
):
    from nexus.audit import service as audit
    from nexus.ops import openai_local
    from nexus.ops.platform_round_registration import inspect_round
    from nexus.ops.research_comparison import native_command
    from sqlalchemy import event
    from sqlalchemy.exc import SQLAlchemyError
    from sqlalchemy.orm import Session

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    original_append = audit.append_audit_event

    async def append(session, **kwargs):
        if audit_failure == "before_write":
            raise SQLAlchemyError("private audit transport detail")
        result = await original_append(session, **kwargs)
        session.info["refusal_audit_ack_loss"] = audit_failure == "after_commit"
        return result

    def lose_ack(session):
        if session.info.get("refusal_audit_ack_loss"):
            raise SQLAlchemyError("private committed audit acknowledgement")

    if audit_failure:
        monkeypatch.setattr(audit, "append_audit_event", append)
    event.listen(Session, "after_commit", lose_ack)
    try:
        source, prepared, manifest, model, _, _ = await setup(
            factory,
            monkeypatch,
            purpose=PLATFORM_QUALIFICATION_PURPOSE,
            price_version=CURRENT_PLATFORM_PRICE_VERSION,
            units_version=CURRENT_PLATFORM_UNITS_VERSION,
        )
        manifest, funding = await funding_fixture(
            factory, manifest, tmp_path, monkeypatch, explicit_spend=True
        )
        async with factory.begin() as session:
            await register_comparison(session, manifest, allow_new_round=True)
        settings = EngineSettings(
            database_url=migrated_database_url,
            agent_daily_budget_usd=Decimal(2) + manifest.reservation_for(1),
            openai_api_key=funding.credential.api_key,
        )
        monkeypatch.setattr(openai_local, "local_settings", lambda *a, **kw: settings)
        (tmp_path / "source.json").write_bytes(source)
        (tmp_path / "prepared.json").write_text(json.dumps(prepared))
        (tmp_path / "manifest.json").write_text(manifest.model_dump_json())
        args = Namespace(
            command="admit-position",
            repo_root=tmp_path,
            source=tmp_path / "source.json",
            prepared=tmp_path / "prepared.json",
            manifest=tmp_path / "manifest.json",
            funding_proof=tmp_path / "funding.json",
            position=1,
            model_config_id=model,
        )
        before = await snapshot(factory)
        receipt = await native_command(args)
        assert receipt["position_consumed"] is False and receipt["automatic_retry_allowed"] is False
        after = await snapshot(factory)
        for table in before:
            if table != "audit_events":
                assert after[table] == before[table]
        if audit_failure:
            assert receipt["status"] == "unknown"
            assert receipt["reason"] == "COMPARISON_REFUSAL_AUDIT_UNCONFIRMED"
            assert "private" not in json.dumps(receipt)
            assert receipt["native_admission_committed"] is False
            assert len(after["audit_events"]) == len(before["audit_events"]) + int(
                audit_failure == "after_commit"
            )
            readback = await inspect_round(factory, manifest.experiment_id)
            assert readback["position_count"] == 0
            assert (readback["last_admission_refusal"] is not None) == (
                audit_failure == "after_commit"
            )
            return
        assert receipt["status"] == "rejected"
        assert receipt["reason"] == "COMPARISON_ROUND_BUDGET_EXHAUSTED"
        assert len(after["audit_events"]) == len(before["audit_events"]) + 1
        readback = await inspect_round(factory, manifest.experiment_id)
        assert readback["last_admission_refusal"]["audit_event_id"] == receipt["audit_event_id"]
        assert readback["position_count"] == 0 and not readback["stopped"]
        # A deliberate later operator call is distinct from automatic retry. The
        # refusal has no native identity to replay and remains immutable history.
        settings.agent_daily_budget_usd = Decimal(2) + manifest.total_reservation_usd
        admitted = await native_command(args)
        assert admitted["status"] == "position_prepared"
        later = await inspect_round(factory, manifest.experiment_id)
        assert later["position_count"] == 1
        assert later["last_admission_refusal"] == readback["last_admission_refusal"]
        with pytest.raises(ValueError, match="POSITION_CONSUMED"):
            await native_command(args)
    finally:
        event.remove(Session, "after_commit", lose_ack)
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("after_first", [False, True])
async def test_shared_budget_change_is_seen_under_lock_and_later_positions_recheck(
    migrated_database_url, monkeypatch, tmp_path, after_first
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    task = None
    try:
        source, prepared, manifest, model, held, _ = await setup(factory, monkeypatch)
        manifest, funding = await funding_fixture(factory, manifest, tmp_path, monkeypatch)
        limit = Decimal(2) + manifest.total_reservation_usd
        original_run = UUID(json.loads(source)["members"][1]["run_id"])

        async def admit(ordinal):
            async with factory.begin() as session:
                return await admit_comparison_position(
                    session,
                    source=source,
                    prepared=prepared,
                    manifest=manifest,
                    ordinal=ordinal,
                    model_config_id=model,
                    daily_limit_usd=limit,
                    platform_funding=funding,
                )

        if after_first:
            await admit(1)
        before = await snapshot(factory)
        reached_lock = asyncio.Event()
        lock = persistence._lock_budget_day

        async def observed_lock(session, business_date):
            reached_lock.set()
            await lock(session, business_date)

        async with factory.begin() as competing:
            now = await competing.scalar(text("SELECT clock_timestamp()"))
            extra = await persistence.reserve_daily_budget(
                competing,
                run_id=original_run,
                now=now,
                daily_limit_usd=limit,
                worst_case_usd=Decimal("0.01"),
                pricing_version="synthetic-competing-consumer",
                pricing_fresh=True,
            )
            monkeypatch.setattr(persistence, "_lock_budget_day", observed_lock)
            monkeypatch.setattr("nexus.agents.platform_funding._lock_budget_day", observed_lock)
            task = asyncio.create_task(admit(2 if after_first else 1))
            await asyncio.wait_for(reached_lock.wait(), timeout=20)
            assert not task.done()  # The real other transaction owns the shared lock.
        with pytest.raises(BudgetExhausted, match="COMPARISON_ROUND_BUDGET_EXHAUSTED"):
            await task
        after = await snapshot(factory)
        assert after["research_comparison_chain"] == before["research_comparison_chain"]
        assert after["jobs"] == before["jobs"]
        assert after["agent_research_runs"] == before["agent_research_runs"]
        assert [
            r for r in after["agent_daily_budget_reservations"] if r["id"] != str(extra.id)
        ] == before["agent_daily_budget_reservations"]
        assert (
            next(r for r in after["agent_daily_budget_reservations"] if r["id"] == str(held))[
                "state"
            ]
            == "reserved"
        )
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await factory.kw["bind"].dispose()
