"""One explicitly granted root cannot lend priority to another request."""

from datetime import timedelta
from decimal import Decimal

import pytest
from integration.test_independent_history_scope_pg import independent_settings
from integration.test_manual_paper_request_pg import AT
from integration.test_unknown_preparation_ack_pg import (
    test_explicit_unknown_batch_is_atomic_and_does_not_replay as exercise_history,
)
from nexus.paper.manual_entry import (
    check_configured_independent_admission,
    start_configured_manual_request,
)
from nexus.persistence.models.manual_paper import ManualPaperRequest
from sqlalchemy import text


@pytest.mark.asyncio
async def test_one_explicit_priority_is_exact_and_budgeted(migrated_database_url, monkeypatch):
    from nexus.paper.independent_priority import authorize

    async def check(factory, settings, old, clock, original):
        settings = (await independent_settings(settings, clock, monkeypatch)).model_copy(
            update={"agent_daily_budget_usd": Decimal("13")}
        )
        async with factory.begin() as db:
            with pytest.raises(ValueError, match="^INDEPENDENT_NATURAL_BUDGET_PROTECTED$"):
                await check_configured_independent_admission(db, settings)
        async with factory.begin() as db:
            grant = await authorize(
                db, settings, idempotency_key="only-priority", accept_priority=True
            )
            again = await authorize(
                db, settings, idempotency_key="only-priority", accept_priority=True
            )
            assert again.audit_id == grant.audit_id
        async with factory.begin() as db:
            with pytest.raises(ValueError, match="^INDEPENDENT_PRIORITY_INVALID$"):
                await authorize(db, settings, idempotency_key="another-key", accept_priority=True)
        async with factory.begin() as db:
            await db.execute(text("SET TRANSACTION READ ONLY"))
            view = await check_configured_independent_admission(
                db, settings, idempotency_key="only-priority"
            )
            assert view.budget_reserved is False and view.external_requests == 0
        async with factory.begin() as db:
            receipt = await start_configured_manual_request(
                db, settings, mode="research_and_monitor", idempotency_key="only-priority"
            )
            assert receipt.status == "queued"
            row = await db.get(ManualPaperRequest, receipt.request_id)
            assert row.research_profile["budget_priority"]["audit_id"] == str(grant.audit_id)
            again = await start_configured_manual_request(
                db, settings, mode="research_and_monitor", idempotency_key="only-priority"
            )
            assert again == receipt
            assert await db.scalar(text("SELECT count(*) FROM single_sol_budget_credits")) == 0
        async with factory.begin() as db:
            with pytest.raises(ValueError, match="^INDEPENDENT_NATURAL_BUDGET_PROTECTED$"):
                await check_configured_independent_admission(
                    db, settings, idempotency_key="not-the-granted-root"
                )

    await exercise_history(
        migrated_database_url,
        monkeypatch,
        "sol-successor",
        after_successor_setup=check,
        successor_at=(AT + timedelta(days=3)).replace(hour=9, minute=0),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["no_consent", "limit", "expired", "other_model", "late_proof"])
async def test_priority_cannot_change_its_frozen_scope(migrated_database_url, monkeypatch, fault):
    from nexus.paper.independent_history_scope import recheck
    from nexus.paper.independent_priority import authorize
    from nexus.persistence.models.runtime import AuditEvent
    from sqlalchemy import select

    async def check(factory, settings, old, clock, original):
        settings = (await independent_settings(settings, clock, monkeypatch)).model_copy(
            update={"agent_daily_budget_usd": Decimal("13")}
        )
        if fault in {"no_consent", "limit"}:
            async with factory.begin() as db:
                with pytest.raises(ValueError, match="^INDEPENDENT_PRIORITY_INVALID$"):
                    await authorize(
                        db,
                        settings.model_copy(update={"agent_daily_budget_usd": Decimal("14")})
                        if fault == "limit"
                        else settings,
                        idempotency_key="priority",
                        accept_priority=fault != "no_consent",
                    )
            return
        if fault == "expired":
            from nexus.paper import independent_history_scope as scope

            monkeypatch.setattr(
                scope, "execution_boundary", lambda **kw: kw["now"] + timedelta(hours=1)
            )
        async with factory.begin() as db:
            await authorize(db, settings, idempotency_key="priority", accept_priority=True)
        if fault == "expired":
            clock[0] += timedelta(hours=1)
            async with factory.begin() as db:
                with pytest.raises(ValueError, match="^INDEPENDENT_PRIORITY_INVALID$"):
                    await check_configured_independent_admission(
                        db, settings, idempotency_key="priority"
                    )
            return
        if fault == "other_model":
            from uuid import uuid4

            async with factory.begin() as db:
                with pytest.raises(ValueError):
                    await check_configured_independent_admission(
                        db,
                        settings.model_copy(update={"agent_paper_formal_model_config_id": uuid4()}),
                        idempotency_key="priority",
                    )
            return
        async with factory.begin() as db:
            receipt = await start_configured_manual_request(
                db, settings, mode="research_and_monitor", idempotency_key="priority"
            )
        async with factory.begin() as db:
            row = await db.get(ManualPaperRequest, receipt.request_id)
            audit = await db.scalar(
                select(AuditEvent).where(
                    AuditEvent.action == "paper.independent_priority.authorized"
                )
            )
            # Original row is immutable; poison only the loaded reader to exercise validation.
            from sqlalchemy.orm.attributes import set_committed_value

            set_committed_value(audit, "event_hash", "0" * 64)
            with pytest.raises(ValueError, match="^INDEPENDENT_PRIORITY_INVALID$"):
                await recheck(db, row, now=clock[0])

    await exercise_history(
        migrated_database_url,
        monkeypatch,
        "sol-successor",
        after_successor_setup=check,
        successor_at=(AT + timedelta(days=3)).replace(hour=9, minute=0),
    )


@pytest.mark.asyncio
async def test_commit_confirmation_lost_reads_same_grant_and_consumed_root(
    migrated_database_url, monkeypatch
):
    from nexus.ops import manual_paper
    from nexus.paper.independent_priority import read_priority
    from sqlalchemy.ext.asyncio import AsyncSession

    async def check(factory, settings, old, clock, original):
        settings = (await independent_settings(settings, clock, monkeypatch)).model_copy(
            update={"agent_daily_budget_usd": Decimal("13")}
        )
        commit = AsyncSession.commit
        failure = RuntimeError("synthetic commit reply missing")

        async def lost(session):
            await commit(session)
            raise failure

        for argv in [
            [
                "authorize-independent-priority",
                "--idempotency-key",
                "commit-unknown",
                "--accept-priority",
            ],
            ["start", "--mode", "research_and_monitor", "--idempotency-key", "commit-unknown"],
        ]:
            with monkeypatch.context() as patch:
                patch.setattr(AsyncSession, "commit", lost)
                with pytest.raises(RuntimeError) as caught:
                    await manual_paper.run(settings, manual_paper.parser().parse_args(argv))
                assert caught.value is failure
            async with factory.begin() as db:
                await db.execute(text("SET TRANSACTION READ ONLY"))
                receipt = await read_priority(db, settings, idempotency_key="commit-unknown")
                assert receipt.grant_unconsumed == (argv[0] == "authorize-independent-priority")
        async with factory() as db:
            assert (
                await db.scalar(
                    text(
                        "SELECT count(*) FROM audit_events "
                        "WHERE action='paper.independent_priority.authorized'"
                    )
                )
                == 1
            )
            assert (
                await db.scalar(
                    text(
                        "SELECT count(*) FROM audit_events "
                        "WHERE action='paper.independent_priority.consumed'"
                    )
                )
                == 1
            )
        # Neither confirmation loss changes the key or sends another request.

    await exercise_history(
        migrated_database_url,
        monkeypatch,
        "sol-successor",
        after_successor_setup=check,
        successor_at=(AT + timedelta(days=3)).replace(hour=9, minute=0),
    )


@pytest.mark.asyncio
async def test_pending_grant_does_not_prevent_real_guard_but_cannot_override_it(
    migrated_database_url, tmp_path, monkeypatch
):
    from hashlib import sha256

    from integration.test_single_sol_budget_pg import controlled_clock, ledger_fixture
    from nexus.agents.persistence import reserve_daily_budget
    from nexus.audit.service import append_audit_event
    from nexus.paper.candidate_batch import load_agent_paper_candidate_batch
    from nexus.paper.independent_priority import ACTION, ACTOR, SCHEMA, Grant, resolve
    from nexus.paper.single_sol_budget import freeze_single_sol_budget
    from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord

    factory, settings, now, instrument, maximum, ids, calendar = await ledger_fixture(
        migrated_database_url, tmp_path, monkeypatch
    )
    try:
        async with factory.begin() as db:
            controlled_clock(db, monkeypatch, now)
            model = await db.get(
                AgentModelConfigRecord, settings.agent_paper_formal_model_config_id
            )
            batch = await load_agent_paper_candidate_batch(
                db, batch_id=settings.agent_paper_candidate_batch_id
            )
            grant = Grant(
                account_id=settings.agent_paper_account_id,
                key_hash=sha256(b"pending").hexdigest(),
                batch_id=batch.batch_id,
                batch_hash=batch.batch_hash,
                model_id=model.id,
                model_hash=model.config_hash,
                budget_day=now.date(),
                authorized_at=now,
                expires_at=now + timedelta(minutes=25),
                daily_limit=Decimal("13"),
                root_maximum=3 * maximum,
            )
            await append_audit_event(
                db,
                actor_id=ACTOR,
                action=ACTION,
                schema_version=SCHEMA,
                payload=grant.model_dump(mode="json"),
                occurred_at=now,
            )
        async with factory.begin() as db:
            controlled_clock(db, monkeypatch, now)
            source = await db.get(AgentResearchRunRecord, ids[0])
            await reserve_daily_budget(
                db,
                run_id=source.id,
                now=now,
                daily_limit_usd=Decimal("13"),
                worst_case_usd=maximum,
                pricing_version=source.pricing_version,
                pricing_fresh=True,
            )
            await freeze_single_sol_budget(
                db,
                settings,
                run_id=source.id,
                instrument_id=instrument,
                calendar_authority_id=calendar,
            )
        async with factory.begin() as db:
            with pytest.raises(ValueError, match="^INDEPENDENT_PRIORITY_INVALID$"):
                await resolve(
                    db,
                    account_id=grant.account_id,
                    batch_id=grant.batch_id,
                    batch_hash=grant.batch_hash,
                    model_id=grant.model_id,
                    model_hash=grant.model_hash,
                    daily_limit=grant.daily_limit,
                    maximum=maximum,
                    now=now,
                    key_hash=grant.key_hash,
                )
            assert await db.scalar(text("SELECT count(*) FROM single_sol_budget_scopes")) == 1
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_concurrent_same_identity_has_one_root_and_one_consumption(
    migrated_database_url, monkeypatch
):
    import asyncio

    from nexus.paper.independent_priority import authorize

    async def check(factory, settings, old, clock, original):
        settings = (await independent_settings(settings, clock, monkeypatch)).model_copy(
            update={"agent_daily_budget_usd": Decimal("13")}
        )
        async with factory.begin() as db:
            await authorize(db, settings, idempotency_key="one-concurrent", accept_priority=True)

        async def submit():
            async with factory.begin() as db:
                return await start_configured_manual_request(
                    db, settings, mode="research_and_monitor", idempotency_key="one-concurrent"
                )

        first, second = await asyncio.gather(submit(), submit())
        assert first.request_id == second.request_id
        async with factory() as db:
            assert (
                await db.scalar(
                    text(
                        "SELECT count(*) FROM audit_events "
                        "WHERE action='paper.independent_priority.consumed'"
                    )
                )
                == 1
            )

    await exercise_history(
        migrated_database_url,
        monkeypatch,
        "sol-successor",
        after_successor_setup=check,
        successor_at=(AT + timedelta(days=3)).replace(hour=9, minute=0),
    )


@pytest.mark.asyncio
async def test_priority_read_snapshot_survives_concurrent_consumption(
    migrated_database_url, monkeypatch
):
    from nexus.ops import manual_paper
    from nexus.paper.independent_priority import authorize
    from sqlalchemy.ext.asyncio import AsyncSession

    async def check(factory, settings, old, clock, original):
        settings = (await independent_settings(settings, clock, monkeypatch)).model_copy(
            update={"agent_daily_budget_usd": Decimal("13")}
        )
        async with factory.begin() as db:
            await authorize(db, settings, idempotency_key="snapshot", accept_priority=True)
        scalars = AsyncSession.scalars
        crossed = False

        async def interleave(db, statement, *args, **kwargs):
            nonlocal crossed
            result = await scalars(db, statement, *args, **kwargs)
            if not crossed and str(statement).startswith("SELECT manual_paper_requests."):
                crossed = True
                async with factory.begin() as writer:
                    await start_configured_manual_request(
                        writer, settings, mode="research_and_monitor", idempotency_key="snapshot"
                    )
            return result

        with monkeypatch.context() as patch:
            patch.setattr(AsyncSession, "scalars", interleave)
            before = await manual_paper.run(
                settings,
                manual_paper.parser().parse_args(
                    ["read-independent-priority", "--idempotency-key", "snapshot"]
                ),
            )
        assert crossed and before.grant_unconsumed and before.consumed_request_id is None
        after = await manual_paper.run(
            settings,
            manual_paper.parser().parse_args(
                ["read-independent-priority", "--idempotency-key", "snapshot"]
            ),
        )
        assert not after.grant_unconsumed and after.consumed_request_id is not None

    await exercise_history(
        migrated_database_url,
        monkeypatch,
        "sol-successor",
        after_successor_setup=check,
        successor_at=(AT + timedelta(days=3)).replace(hour=9, minute=0),
    )


@pytest.mark.asyncio
async def test_repeat_authorization_rejects_inconsistent_consumption(
    migrated_database_url, monkeypatch
):
    from nexus.paper.independent_priority import authorize, read_priority
    from sqlalchemy.orm.attributes import set_committed_value

    async def check(factory, settings, old, clock, original):
        settings = (await independent_settings(settings, clock, monkeypatch)).model_copy(
            update={"agent_daily_budget_usd": Decimal("13")}
        )
        async with factory.begin() as db:
            await authorize(db, settings, idempotency_key="consistent", accept_priority=True)
            root = await start_configured_manual_request(
                db, settings, mode="research_and_monitor", idempotency_key="consistent"
            )
        async with factory.begin() as db:
            row = await db.get(ManualPaperRequest, root.request_id)
            before = await read_priority(db, settings, idempotency_key="consistent")
            repeated = await authorize(
                db, settings, idempotency_key="consistent", accept_priority=True
            )
            assert repeated == before and repeated.consumed_request_id == row.id
            # Corrupt only the reader identity map; immutable persisted facts stay untouched.
            set_committed_value(
                row,
                "research_profile",
                {k: v for k, v in row.research_profile.items() if k != "budget_priority"},
            )
            for read in (read_priority, authorize):
                with pytest.raises(ValueError, match="^INDEPENDENT_PRIORITY_INVALID$"):
                    await read(
                        db,
                        settings,
                        idempotency_key="consistent",
                        **({"accept_priority": True} if read is authorize else {}),
                    )

    await exercise_history(
        migrated_database_url,
        monkeypatch,
        "sol-successor",
        after_successor_setup=check,
        successor_at=(AT + timedelta(days=3)).replace(hour=9, minute=0),
    )


@pytest.mark.asyncio
async def test_priority_unknown_blocks_normal_single_owner_before_guard(
    migrated_database_url, monkeypatch
):
    from nexus.paper.candidate_batch import load_agent_paper_candidate_batch
    from nexus.paper.independent_priority import authorize
    from nexus.paper.manual_request import claim_manual_request, record_manual_request_event
    from nexus.paper.single_sol_research import single_sol_authority, start_single_sol
    from nexus.settings import AgentRuntimeMode
    from pydantic import SecretStr

    async def check(factory, settings, old, clock, original):
        settings = (await independent_settings(settings, clock, monkeypatch)).model_copy(
            update={
                "agent_daily_budget_usd": Decimal("13"),
                "agent_runtime_mode": AgentRuntimeMode.RESPONSES,
                "openai_api_key": SecretStr("sk-proj-" + "synthetic-only" * 5),
            }
        )
        async with factory.begin() as db:
            await authorize(db, settings, idempotency_key="unknown-priority", accept_priority=True)
            root = await start_configured_manual_request(
                db, settings, mode="research_and_monitor", idempotency_key="unknown-priority"
            )
            await claim_manual_request(db, request_id=root.request_id)
            await record_manual_request_event(
                db,
                await db.get(ManualPaperRequest, root.request_id),
                status="unknown",
                code="MANUAL_RESULT_UNKNOWN",
            )
        async with factory.begin() as db:
            batch = await load_agent_paper_candidate_batch(
                db, batch_id=settings.agent_paper_candidate_batch_id
            )
            offer = await single_sol_authority(db, settings, instrument_id=batch.instrument_ids[0])
            with pytest.raises(ValueError, match="^MANUAL_ACCOUNT_BUSY_OR_UNKNOWN$"):
                await start_single_sol(
                    db,
                    settings,
                    instrument_id=batch.instrument_ids[0],
                    idempotency_key="no-new-single",
                    expected_intent_hash=offer.intent_hash,
                )
            assert await db.scalar(text("SELECT count(*) FROM single_sol_budget_scopes")) == 0
            assert (
                await db.scalar(
                    text(
                        "SELECT count(*) FROM audit_events "
                        "WHERE action='paper.independent_priority.consumed'"
                    )
                )
                == 1
            )

    await exercise_history(
        migrated_database_url,
        monkeypatch,
        "sol-successor",
        after_successor_setup=check,
        successor_at=(AT + timedelta(days=3)).replace(hour=9, minute=0),
    )
