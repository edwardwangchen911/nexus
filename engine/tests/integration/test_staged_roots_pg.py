"""Normal three-stock root preparation and SDK route in isolated PostgreSQL."""

import pytest
from helpers import disclosure_observations
from helpers.staged_worker import run_staged_worker
from integration import test_candidate_multistock_path as candidates
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.runtime import Job
from sqlalchemy import select, text


class RootPreparationAborted(Exception):
    def __init__(self, factory, run_ids):
        self.factory, self.run_ids = factory, run_ids


@pytest.mark.asyncio
@pytest.mark.parametrize("independent", [False, True])
@pytest.mark.parametrize("legacy_first", [False, True])
async def test_normal_three_stock_roots_use_one_staged_protocol(
    migrated_database_url,
    monkeypatch,
    tmp_path,
    independent,
    legacy_first,
    late_registration=False,
    update_old=False,
    abort_preparation=False,
    overlay_before_prepare=True,
    before_worker=None,
):
    captured = {}
    normal_cycle = disclosure_observations.normal_cycle
    native_start = candidates.start_schwab_agent_paper_cycle
    from nexus.paper import research_request

    native_prepare = research_request.prepare_research_request

    async def verify_uncommitted(run_ids):
        factory = captured["fixture"][0]
        async with factory() as observer:
            assert not (
                await observer.scalars(
                    select(AgentResearchRunRecord.id).where(AgentResearchRunRecord.id.in_(run_ids))
                )
            ).all()
            assert not (
                await observer.scalars(
                    select(Job.id).where(
                        Job.payload["run_id"].astext.in_([str(r) for r in run_ids])
                    )
                )
            ).all()
            assert (
                await observer.scalar(
                    text(
                        "SELECT count(*) FROM agent_daily_budget_reservations "
                        "WHERE run_id=ANY(:ids)"
                    ),
                    {"ids": list(run_ids)},
                )
                == 0
            )
        if abort_preparation:
            raise RootPreparationAborted(factory, tuple(run_ids))

    async def start(session, **kwargs):
        from helpers.staged_routes import ensure_routes, register_prepared_runs

        if overlay_before_prepare:
            await ensure_routes(session)
        result = await native_start(session, **kwargs)
        if not result.replayed and not late_registration:
            await register_prepared_runs(session, result.research_run_ids)
            await verify_uncommitted(result.research_run_ids)
        return result

    async def prepare(session, *args, **kwargs):
        from helpers.staged_routes import ensure_routes, register_prepared_runs

        if overlay_before_prepare:
            await ensure_routes(session)
        result = await native_prepare(session, *args, **kwargs)
        if not late_registration:
            await register_prepared_runs(session, tuple(m.run_id for m in result.research.members))
            await verify_uncommitted(tuple(m.run_id for m in result.research.members))
        return result

    async def verify_protocol(factory):
        if late_registration:
            assert captured.get("sdk_run_calls", 0) == 0
            async with factory() as session:
                for table in ("route", "authorization", "terminal"):
                    assert (
                        await session.scalar(text(f"SELECT count(*) FROM stage_proto_{table}")) == 0
                    )
            assert captured["late_rejected"]
            captured["verified"] = True
            return
        assert captured.get("sdk_run_calls", 0) == 9
        async with factory() as session:
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_terminal")) == 3
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_authorization")) == 9
            assert (
                await session.scalar(
                    text("SELECT count(*) FROM stage_proto_verification WHERE verified")
                )
                == 9
            )
            assert (
                await session.scalar(
                    text("SELECT count(*) FROM stage_proto_route WHERE kind=:kind"),
                    {"kind": "independent" if independent else "formal"},
                )
                == 3
            )
        captured["verified"] = True

    async def remove_overlay_before_native_downgrade(factory):
        # Verify the experimental proof before removing only its disposable DDL.
        # The original native downgrade rejection and data assertions still run.
        await verify_protocol(factory)
        async with factory.begin() as session:
            assert (await session.scalar(text("SELECT current_database()"))).startswith(
                "nexus_test_"
            )
            await session.execute(text("DROP TRIGGER stage_proto_claim ON jobs"))
            await session.execute(text("DROP TRIGGER stage_proto_capture_job ON jobs"))
            await session.execute(text("DROP FUNCTION stage_proto_inflight(uuid)"))
            for name in (
                "prepared_job",
                "route",
                "terminal",
                "cleanup",
                "verification",
                "observation",
                "authorization",
                "member",
                "scope",
            ):
                await session.execute(text(f"DROP TABLE stage_proto_{name}"))
            for name in (
                "birth_guard",
                "root_authorize_guard",
                "capture_job",
                "route_guard",
                "claim_guard",
                "route_member_guard",
                "authorize_guard",
                "member_guard",
                "stop_guard",
                "immutable",
            ):
                await session.execute(text(f"DROP FUNCTION stage_proto_{name}()"))

    async def cycle(*args, **kwargs):
        before = kwargs["before_research"]

        async def capture(settings, factory, authority, manifest, clock):
            await before(settings, factory, authority, manifest, clock)
            captured["clock"] = clock
            captured["fixture"] = (factory, None, None, captured, None, None, None)

        kwargs["before_downgrade"] = remove_overlay_before_native_downgrade
        kwargs["before_research"] = capture
        return await normal_cycle(*args, **kwargs)

    async def staged(settings, **kwargs):
        if before_worker is not None:
            await before_worker(captured, settings, kwargs)
        from helpers.staged_routes import register_prepared_runs

        if late_registration:
            from nexus.worker import run_agent_worker_once as legacy
            from sqlalchemy.exc import DBAPIError

            if not captured.get("late_rejected"):
                with pytest.raises(DBAPIError, match="preparation route binding invalid"):
                    async with captured["fixture"][0].begin() as session:
                        ids = tuple(
                            (
                                await session.execute(text("SELECT id FROM agent_research_runs"))
                            ).scalars()
                        )
                        if update_old:
                            # Preserve native version/transition guards while replacing
                            # an old row version; this is not a Worker claim or dispatch.
                            await session.execute(
                                text(
                                    "UPDATE jobs SET status='running', started_at=created_at, "
                                    "version=version+1 WHERE status='queued'"
                                )
                            )
                            await session.execute(
                                text(
                                    "UPDATE jobs SET status='queued', started_at=NULL, "
                                    "version=version+1 WHERE status='running'"
                                )
                            )
                        await register_prepared_runs(session, ids)
                async with captured["fixture"][0]() as session:
                    assert await session.scalar(text("SELECT count(*) FROM job_attempts")) == 0
                    assert await session.scalar(text("SELECT count(*) FROM stage_proto_route")) == 0
                    assert (
                        await session.scalar(
                            text(
                                "SELECT count(*) FROM agent_daily_budget_reservations "
                                "WHERE state='reserved'"
                            )
                        )
                        == 3
                    )
                captured["late_rejected"] = True
            # Unselected historical protocol remains readable/executable as before;
            # this is a synthetic offline original Job, never a live request replay.
            return await legacy(settings, **kwargs)
        if not captured.get("replay_checked"):
            async with captured["fixture"][0].begin() as session:
                ids = tuple(
                    (await session.execute(text("SELECT run_id FROM stage_proto_route"))).scalars()
                )
                await register_prepared_runs(session, ids)
                assert await session.scalar(text("SELECT count(*) FROM stage_proto_route")) == 3
                assert (
                    await session.scalar(text("SELECT count(*) FROM stage_proto_authorization"))
                    == 0
                )
            captured["replay_checked"] = True
        if legacy_first and not captured.get("legacy_checked"):
            from nexus.worker import run_agent_worker_once as legacy
            from sqlalchemy.exc import DBAPIError

            with pytest.raises(DBAPIError, match="staged worker required"):
                await legacy(settings, **kwargs)
            async with captured["fixture"][0]() as session:
                assert await session.scalar(text("SELECT count(*) FROM job_attempts")) == 0
                assert (
                    await session.scalar(text("SELECT count(*) FROM jobs WHERE status='queued'"))
                    == 3
                )
                assert (
                    await session.scalar(
                        text(
                            "SELECT count(*) FROM agent_daily_budget_reservations "
                            "WHERE state='reserved'"
                        )
                    )
                    == 3
                )
            captured["legacy_checked"] = True
        return await run_staged_worker(
            settings, monkeypatch=monkeypatch, fixture=captured["fixture"], **kwargs
        )

    monkeypatch.setattr(disclosure_observations, "normal_cycle", cycle)
    monkeypatch.setattr(candidates, "start_schwab_agent_paper_cycle", start)
    monkeypatch.setattr(research_request, "prepare_research_request", prepare)
    monkeypatch.setattr(candidates, "run_agent_worker_once", staged)
    factory, _, _, _, _ = await disclosure_observations.reader_source(
        migrated_database_url, monkeypatch, tmp_path, independent=independent
    )
    if independent:
        await verify_protocol(factory)
    assert captured["verified"]


@pytest.mark.asyncio
@pytest.mark.parametrize("independent", [False, True])
@pytest.mark.parametrize("update_old", [False, True])
async def test_root_registration_rejects_committed_job_even_after_update(
    migrated_database_url, monkeypatch, tmp_path, independent, update_old
):
    await test_normal_three_stock_roots_use_one_staged_protocol(
        migrated_database_url,
        monkeypatch,
        tmp_path,
        independent,
        False,
        late_registration=True,
        update_old=update_old,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("independent", [False, True])
async def test_root_preparation_rollback_leaves_no_selected_jobs_or_reservations(
    migrated_database_url, monkeypatch, tmp_path, independent
):
    with pytest.raises(RootPreparationAborted) as stopped:
        await test_normal_three_stock_roots_use_one_staged_protocol(
            migrated_database_url,
            monkeypatch,
            tmp_path,
            independent,
            False,
            abort_preparation=True,
        )
    async with stopped.value.factory() as session:
        ids = stopped.value.run_ids
        assert len(ids) == 3
        assert not (
            await session.scalars(
                select(AgentResearchRunRecord.id).where(AgentResearchRunRecord.id.in_(ids))
            )
        ).all()
        assert not (
            await session.scalars(
                select(Job.id).where(Job.payload["run_id"].astext.in_([str(r) for r in ids]))
            )
        ).all()
        assert (
            await session.scalar(
                text("SELECT count(*) FROM agent_daily_budget_reservations WHERE run_id=ANY(:ids)"),
                {"ids": list(ids)},
            )
            == 0
        )
        assert await session.scalar(text("SELECT to_regclass('stage_proto_route')")) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("independent", [False, True])
@pytest.mark.parametrize("boundary", ["forged_receipt", "member", "preexisting_member"])
async def test_unselected_historical_root_cannot_enter_staged_protocol(
    migrated_database_url, monkeypatch, tmp_path, independent, boundary
):
    from datetime import timedelta
    from uuid import UUID

    from helpers.staged_protocol import StagedProtocol
    from helpers.staged_routes import ensure_routes, owner, register_prepared_runs
    from sqlalchemy.exc import DBAPIError

    class BoundaryObserved(Exception):
        pass

    async def check(captured, settings, kwargs):
        factory = captured["fixture"][0]
        now = captured["clock"][0]
        job_id = kwargs["job_ids"][0]
        async with factory() as session:
            job = await session.get(Job, job_id)
            run = await session.get(AgentResearchRunRecord, UUID(job.payload["run_id"]))
            scope_id, kind = await owner(session, run, job)
            run_id = run.id
        protocol = StagedProtocol(factory)
        if boundary == "forged_receipt":
            async with factory.begin() as session:
                await ensure_routes(session)
            with pytest.raises(DBAPIError, match="birth receipt requires Job insert"):
                async with factory.begin() as session:
                    await session.execute(
                        text("INSERT INTO stage_proto_prepared_job VALUES(:job)"), {"job": job_id}
                    )
                    await register_prepared_runs(session, (run_id,))
        else:
            from nexus.jobs import worker as jobs_worker
            from nexus.worker import run_agent_worker_once as legacy

            class ClaimCaptured(BaseException):
                def __init__(self, claim):
                    self.claim = claim

            async def capture_claim(session, *, claim, **options):
                raise ClaimCaptured(claim)

            with monkeypatch.context() as patch:
                patch.setattr(jobs_worker, "execute_and_finalize_job", capture_claim)
                with pytest.raises(ClaimCaptured) as observed:
                    await legacy(settings, **kwargs)
            claim = observed.value.claim
            install = dict(
                scope_id=scope_id,
                claims=(claim,),
                now=now,
                work_deadline=now + timedelta(seconds=100),
                cleanup_deadline=now + timedelta(seconds=110),
                independent=kind == "independent",
            )
            if boundary == "preexisting_member":
                await protocol.install(**install)
            async with factory.begin() as session:
                await ensure_routes(session)
            with pytest.raises(DBAPIError, match="staged root route required"):
                if boundary == "member":
                    await protocol.install(**install)
                else:
                    await protocol.authorize(
                        claim,
                        stage=1,
                        now=now,
                        wire=b"root-wire",
                    )
        async with factory() as session:
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_route")) == 0
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_authorization")) == 0
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM agent_daily_budget_reservations "
                        "WHERE state='reserved'"
                    )
                )
                == 3
            )
            assert (await session.get(AgentResearchRunRecord, run_id)).status == "prepared"
        assert captured.get("sdk_run_calls", 0) == 0
        raise BoundaryObserved

    with pytest.raises(BoundaryObserved):
        await test_normal_three_stock_roots_use_one_staged_protocol(
            migrated_database_url,
            monkeypatch,
            tmp_path,
            independent,
            False,
            late_registration=True,
            overlay_before_prepare=False,
            before_worker=check,
        )
