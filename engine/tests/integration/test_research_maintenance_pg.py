"""F-prepared identities stay untouched when the maintenance rollback refuses execution."""

import pytest
from sqlalchemy import text


class Checked(BaseException):
    pass


@pytest.mark.asyncio
@pytest.mark.parametrize("selected", [False, True])
@pytest.mark.parametrize("independent", [False, True])
async def test_maintenance_blocks_existing_queued_without_claim_or_downgrade(
    migrated_database_url, monkeypatch, tmp_path, selected, independent
):
    from helpers import staged_routes
    from integration.test_staged_roots_pg import (
        test_normal_three_stock_roots_use_one_staged_protocol as prepare,
    )
    from nexus.agents import research_maintenance
    from nexus.agents.research_stages import routes
    from nexus.paper.manual_request import admit_manual_request
    from nexus.paper.research_request import prepare_research_request as native_prepare
    from nexus.worker import run_agent_worker_once

    async def noop(*args, **kwargs):
        pass

    if not independent:
        from integration import test_candidate_multistock_path as candidates

        native_start = candidates.start_schwab_agent_paper_cycle

        async def start_with_refusal(session, **kwargs):
            tables = (
                "agent_paper_cycles",
                "agent_research_runs",
                "jobs",
                "agent_daily_budget_reservations",
                "research_stage_route",
            )

            async def facts():
                return {
                    table: sorted(
                        await session.scalars(text(f"SELECT to_jsonb(t)::text FROM {table} t"))
                    )
                    for table in tables
                }

            before = await facts()
            monkeypatch.setattr(research_maintenance, "IMAGE_MAINTENANCE", True)
            with pytest.raises(ValueError, match="RESEARCH_MAINTENANCE"):
                await native_start(session, **kwargs)
            assert await facts() == before
            monkeypatch.setattr(research_maintenance, "IMAGE_MAINTENANCE", False)
            return await native_start(session, **kwargs)

        monkeypatch.setattr(candidates, "start_schwab_agent_paper_cycle", start_with_refusal)

    monkeypatch.setattr(staged_routes, "ensure_routes", noop)
    monkeypatch.setattr(staged_routes, "register_prepared_runs", noop)
    if not selected:
        monkeypatch.setattr(routes, "register_new_prepared_runs", noop)

    async def inspect(captured, settings, options):
        factory = captured["fixture"][0]

        async def facts():
            async with factory() as session:
                result = {}
                for table in (
                    "research_stage_scope",
                    "research_stage_route",
                    "research_stage_member",
                    "research_stage_authorization",
                    "research_stage_observation",
                    "research_stage_verification",
                    "research_stage_cleanup",
                    "research_stage_terminal",
                    "research_stage_prepared_job",
                    "agent_daily_budget_reservations",
                    "jobs",
                    "job_attempts",
                    "job_leases",
                    "manual_paper_requests",
                    "agent_research_runs",
                ):
                    result[table] = sorted(
                        await session.scalars(text(f"SELECT to_jsonb(t)::text FROM {table} t"))
                    )
                return result

        before = await facts()
        assert len(before["research_stage_route"]) == (3 if selected else 0)
        assert not before["job_attempts"]
        monkeypatch.setattr(research_maintenance, "IMAGE_MAINTENANCE", True)
        with pytest.raises(ValueError, match="RESEARCH_MAINTENANCE"):
            await run_agent_worker_once(settings, **options)
        # Refusal survives a caller catching the exception and committing.
        async with factory.begin() as session:
            with pytest.raises(ValueError, match="RESEARCH_MAINTENANCE"):
                await admit_manual_request(
                    session,
                    account_id=settings.agent_paper_account_id,
                    batch_id=settings.agent_paper_candidate_batch_id,
                    model_config_id=settings.agent_paper_formal_model_config_id,
                    mode="research_and_monitor",
                    contract_version=3,
                    idempotency_key="rollback-new",
                )
        assert await facts() == before
        monkeypatch.setattr(research_maintenance, "IMAGE_MAINTENANCE", False)
        async with factory() as session:
            ids = await session.scalars(
                text("SELECT id FROM jobs WHERE job_type='agent.paper.research'")
            )
            for job_id in ids:
                assert bool(await routes.selected_route(session, job_id)) == selected
        assert await facts() == before
        if independent:
            from nexus.paper.research_request import finish_research_request
            from nexus.persistence.models.manual_paper import ManualPaperRequest
            from sqlalchemy import select

            monkeypatch.setattr(research_maintenance, "IMAGE_MAINTENANCE", True)
            async with factory.begin() as session:
                row = (await session.scalars(select(ManualPaperRequest))).one()
                # The existing prepared identity remains an exact idempotent readback.
                replay = await native_prepare(session, settings, request_id=row.id)
                assert replay.status == "active"
                closed = await finish_research_request(session, row=row, cancel_unused=True)
                assert closed.status == "unavailable"
            monkeypatch.setattr(research_maintenance, "IMAGE_MAINTENANCE", False)
            async with factory() as session:
                reread = await native_prepare(session, settings, request_id=row.id)
                assert reread.status == "unavailable"
                assert (
                    await session.scalar(text("SELECT count(*) FROM research_stage_authorization"))
                    == 0
                )
                assert await session.scalar(text("SELECT count(*) FROM research_stage_route")) == (
                    3 if selected else 0
                )
        raise Checked

    with pytest.raises(Checked):
        await prepare(
            migrated_database_url, monkeypatch, tmp_path, independent, False, before_worker=inspect
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("known", [False, True])
async def test_maintenance_preserves_closeout_rollback_and_unknown_budget(
    migrated_database_url, monkeypatch, tmp_path, known
):
    from integration.test_research_stages_migration_pg import (
        test_formal_route_birth_legacy_isolation_and_unknown_rollback_protection as scenario,
    )
    from nexus.agents import research_maintenance
    from nexus.agents.research_stages.protocol import StagedProtocol

    original = StagedProtocol.close

    async def close_in_maintenance(self, claim, **kwargs):
        monkeypatch.setattr(research_maintenance, "IMAGE_MAINTENANCE", True)

        class Rollback(Exception):
            pass

        with pytest.raises(Rollback):
            async with self.factory.begin() as session:
                await self.close_in_transaction(session, claim, **kwargs)
                raise Rollback
        async with self.factory() as session:
            assert await session.scalar(text("SELECT count(*) FROM research_stage_terminal")) == 0
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM agent_daily_budget_reservations "
                        "WHERE state='reserved'"
                    )
                )
                == 3
            )
        # No guard on local terminalization; the original proof/lease/budget checks still run.
        return await original(self, claim, **kwargs)

    monkeypatch.setattr(StagedProtocol, "close", close_in_maintenance)
    await scenario(migrated_database_url, monkeypatch, tmp_path, False, True, known)


@pytest.mark.asyncio
@pytest.mark.parametrize("mixed", [False, True])
async def test_maintenance_increment_refusal_has_no_business_residue(
    migrated_database_url, monkeypatch, tmp_path, mixed
):
    from nexus.agents import research_maintenance
    from nexus.monitoring import admission, mixed_admission

    module, method = (mixed_admission, "admit_batch") if mixed else (admission, "admit_trigger")
    native = getattr(module, method)

    async def refuse(session, **kwargs):
        async def facts():
            return {
                table: sorted(
                    await session.scalars(text(f"SELECT to_jsonb(t)::text FROM {table} t"))
                )
                for table in (
                    "jobs",
                    "agent_research_runs",
                    "agent_daily_budget_reservations",
                    "research_stage_route",
                    "research_stage_scope",
                    "research_stage_authorization",
                    "research_monitor_increments",
                    "research_monitor_triggers",
                )
            }

        before = await facts()
        monkeypatch.setattr(research_maintenance, "IMAGE_MAINTENANCE", True)
        with pytest.raises(ValueError, match="RESEARCH_MAINTENANCE"):
            await native(session, **kwargs)
        assert await facts() == before
        raise Checked

    monkeypatch.setattr(module, method, refuse)
    with pytest.raises(Checked):
        if mixed:
            from integration.test_research_stages_runtime_pg import (
                test_formal_increment_scan_settlement_and_next_admission as scenario,
            )

            await scenario(migrated_database_url, monkeypatch, tmp_path, False)
        else:
            # New ordinary requests are rejected before source resolution. Missing
            # or stale source inputs cannot turn maintenance into a write path.
            from datetime import UTC, datetime
            from decimal import Decimal
            from uuid import uuid4

            from nexus.persistence.session import session_factory
            from nexus.settings import EngineSettings

            factory = session_factory(EngineSettings(database_url=migrated_database_url))
            try:
                async with factory.begin() as session:
                    await refuse(
                        session,
                        account_id=uuid4(),
                        trigger_id=uuid4(),
                        candidate_batch_id=uuid4(),
                        calendar_authority_id=uuid4(),
                        model_config_id=uuid4(),
                        daily_limit_usd=Decimal("3"),
                        daily_sdk_call_limit=9,
                        now=datetime.now(UTC),
                    )
            finally:
                await factory.kw["bind"].dispose()
