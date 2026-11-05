"""Explicit new policy cannot change an old key or escape a new unknown owner."""

from datetime import timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from helpers.single_sol_maintenance_image import frozen_facts
from integration.test_budgeted_single_sol_pg import completed_predecessor
from nexus.ops import manual_paper
from nexus.paper.manual_request import read_manual_request
from nexus.paper.single_sol_owner import binding_for
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.session import session_factory
from sqlalchemy import select, text


@pytest.mark.asyncio
async def test_same_key_policy_and_frozen_identity_conflicts_leave_no_new_work(
    migrated_database_url, tmp_path, monkeypatch, capsys
):
    settings, _, ids = await completed_predecessor(
        migrated_database_url, tmp_path, monkeypatch, capsys
    )
    factory = session_factory(settings)

    async def call(*args, config=settings):
        return await manual_paper.run(config, manual_paper.parser().parse_args(list(args)))

    try:
        async with factory() as db:
            old = (await db.scalars(select(ManualPaperRequest))).one()
            old_hash = binding_for(old).intent_hash
        offer = await call(
            "inspect-single-sol-intent",
            "--instrument-id",
            str(ids[0]),
            "--admission-policy",
            "shared-budget",
        )

        def args(
            key="new-key", policy="shared-budget", instrument=ids[0], digest=offer.intent_hash
        ):
            return (
                "start-single-sol",
                "--idempotency-key",
                key,
                "--admission-policy",
                policy,
                "--instrument-id",
                str(instrument),
                "--expected-intent-hash",
                digest,
            )

        receipt = await call(*args())
        assert receipt.status == "queued"
        before = await frozen_facts(factory)
        conflicts = [
            (args(key="one-v4", digest=old_hash), settings),
            (args(policy="daily-single"), settings),
            (args(instrument=ids[1]), settings),
            (args(digest="0" * 64), settings),
            (args(), settings.model_copy(update={"agent_paper_formal_model_config_id": uuid4()})),
            (args(), settings.model_copy(update={"agent_paper_candidate_batch_id": uuid4()})),
        ]
        for field in ("budget_day", "deadline"):
            changed = offer.binding.model_copy(
                update={field: getattr(offer.binding, field) + timedelta(days=1)}
            )
            conflicts.append((args(digest=changed.intent_hash), settings))
        for arguments, config in conflicts:
            with pytest.raises(ValueError, match="MANUAL_IDEMPOTENCY_CONFLICT"):
                await call(*arguments, config=config)
            assert await frozen_facts(factory) == before
        assert await call(*args()) == receipt
        assert await frozen_facts(factory) == before
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_new_unknown_rejects_new_keys_and_old_acceptance_entry(
    migrated_database_url, tmp_path, monkeypatch, capsys
):
    from helpers import budgeted_single_readback
    from integration.test_budgeted_single_sol_pg import (
        test_same_day_successor_uses_own_reservation_and_one_day_guard as scenario,
    )

    original = budgeted_single_readback.verify_budgeted_readback

    async def probe(factory, settings, view, *, outcome):
        await original(factory, settings, view, outcome=outcome)
        async with factory() as db:
            row = await db.get(ManualPaperRequest, view.request_id)
            binding = binding_for(row)
            used = await db.scalar(
                text(
                    "SELECT sum(COALESCE(settled_usd,reserved_usd)) "
                    "FROM agent_daily_budget_reservations"
                )
            )
            # Raw ledger headroom alone must not override unresolved ownership.
            assert used + Decimal("2.12192") < Decimal("13")
        before = await frozen_facts(factory)

        async def call(*args):
            return await manual_paper.run(settings, manual_paper.parser().parse_args(list(args)))

        with pytest.raises(ValueError, match="SINGLE_SOL_NATURAL_BUDGET_PROTECTED"):
            await call(
                "start-single-sol",
                "--admission-policy",
                "shared-budget",
                "--instrument-id",
                str(binding.instrument_id),
                "--idempotency-key",
                "new-after-unknown",
                "--expected-intent-hash",
                binding.intent_hash,
            )
        # The other normal producer reaches account admission before monetary preparation.
        with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
            await call(
                "start",
                "--mode",
                "research_and_monitor",
                "--idempotency-key",
                "formal-after-unknown",
            )
        from contextlib import nullcontext

        from nexus.paper import manual_preparation_close

        with monkeypatch.context() as patch:
            patch.setattr(manual_paper, "_require_maintenance_code", lambda *_: None)
            patch.setattr(
                manual_paper, "preparation_service_control_lock", lambda *_: nullcontext()
            )
            patch.setattr(
                manual_preparation_close, "require_preparation_producers_stopped", lambda *_: None
            )
            with pytest.raises(ValueError, match="INDEPENDENT_UNKNOWN_ACCEPTANCE_BLOCKED"):
                await call(
                    "--repo-root",
                    str(tmp_path),
                    "check-independent-acceptance",
                    "--request-id",
                    str(view.request_id),
                )
        assert await call("read", "--request-id", str(view.request_id)) == view
        async with factory() as db:
            assert (
                await read_manual_request(db, await db.get(ManualPaperRequest, view.request_id))
                == view
            )
        assert await frozen_facts(factory) == before

    monkeypatch.setattr(budgeted_single_readback, "verify_budgeted_readback", probe)
    await scenario(migrated_database_url, tmp_path, monkeypatch, capsys, "timeout")


@pytest.mark.asyncio
async def test_valid_old_acceptance_does_not_cover_next_day_v5_unknown(
    migrated_database_url, tmp_path, monkeypatch, capsys
):
    import httpx
    from helpers import single_sol_disposition
    from integration import test_single_sol_request_pg as legacy
    from nexus.jobs.models import job_input_hash
    from nexus.ops.manual_paper_host import dispatch_pure_research_request
    from nexus.paper import manual_preparation_close
    from nexus.paper.independent_unknown_acceptance import (
        independent_unknown_accepted,
        inspect_or_accept_independent_unknown,
    )
    from nexus.paper.independent_unknown_proof import independent_unknown_proof

    async def accepted_predecessor(
        factory, settings, request_id, member, root, patch, ticket, *, race=None, store=None
    ):
        patch.setattr(
            manual_preparation_close, "require_preparation_producers_stopped", lambda *_: None
        )
        async with factory() as db:
            row = await db.get(ManualPaperRequest, request_id)
            now = await db.scalar(text("SELECT clock_timestamp()"))
            digest = job_input_hash(await independent_unknown_proof(db, row=row, now=now))
            calendar = binding_for(row).calendar_id
        pin = root / ".secrets/nexus-agent-paper/single-sol-disposition.sha256"
        pin.parent.mkdir(parents=True, exist_ok=True)
        pin.write_text(digest)
        pin.chmod(0o600)
        async with factory.begin() as db:
            await inspect_or_accept_independent_unknown(
                db,
                settings=settings,
                request_id=request_id,
                repo_root=root,
                env_file=root / "fixture.env",
                accept=True,
                accept_unknown_effects=True,
                expected_proof_hash=digest,
            )
        # Advance only this disposable fixture's DB/Python clocks; old facts stay unchanged.
        shift = timedelta(days=1)
        while (now + shift).weekday() >= 5:
            shift += timedelta(days=1)
        from datetime import UTC, datetime

        from helpers import current_identity
        from integration import test_research_identity_context
        from nexus import worker
        from nexus.agents import snapshots
        from nexus.ops import instrument_identity_local
        from nexus.persistence.models.runtime import WorkerHeartbeat
        from nexus.research import current_identity_archive

        for module in (
            current_identity,
            test_research_identity_context,
            worker,
            snapshots,
            instrument_identity_local,
            current_identity_archive,
        ):

            class NextDayTime(datetime):
                @classmethod
                def now(cls, tz=None):
                    return (datetime.now(UTC) - timedelta(seconds=delta)).astimezone(tz)

            patch.setattr(module, "datetime", NextDayTime)
        async with factory.begin() as db:
            wall = await db.scalar(text("SELECT pg_catalog.clock_timestamp()"))
            delta = int((wall - now - shift).total_seconds())
            for name, source, stability in (
                ("clock_timestamp", "clock_timestamp", "VOLATILE"),
                ("now", "transaction_timestamp", "STABLE"),
            ):
                await db.execute(
                    text(
                        f"CREATE OR REPLACE FUNCTION fixture_clock.{name}() RETURNS timestamptz "
                        f"LANGUAGE sql {stability} AS $$ SELECT pg_catalog.{source}() "
                        f"- interval '{delta} seconds' $$"
                    )
                )
            today = await db.scalar(text("SELECT clock_timestamp()"))
            for heartbeat in await db.scalars(select(WorkerHeartbeat)):
                if ":manual:" in heartbeat.worker_id:
                    heartbeat.seen_at = today
            row = await db.get(ManualPaperRequest, request_id)
            assert await independent_unknown_accepted(db, row, now=today)

        async def call(*args):
            return await manual_paper.run(settings, manual_paper.parser().parse_args(list(args)))

        offer = await call(
            "inspect-single-sol-intent",
            "--instrument-id",
            str(member.instrument_id),
            "--admission-policy",
            "shared-budget",
        )
        fresh = await call(
            "start-single-sol",
            "--instrument-id",
            str(member.instrument_id),
            "--admission-policy",
            "shared-budget",
            "--idempotency-key",
            "next-day-v5",
            "--expected-intent-hash",
            offer.intent_hash,
        )

        async def timeout(_payload):
            raise httpx.ReadTimeout("isolated new response unknown")

        sent, _ = legacy.install_sol_http(patch, store, on_request=timeout)
        assert await dispatch_pure_research_request(
            settings, request_id=fresh.request_id, calendar_authority_id=calendar
        )
        assert sent == ["research"]
        async with factory() as db:
            new_row = await db.get(ManualPaperRequest, fresh.request_id)
            new_view = await read_manual_request(db, new_row)
            assert new_view.status == "unknown" and new_row.contract_version == 5
            old = await db.get(ManualPaperRequest, request_id)
            assert await independent_unknown_accepted(
                db, old, now=await db.scalar(text("SELECT clock_timestamp()"))
            )
            assert not await independent_unknown_accepted(
                db, new_row, now=await db.scalar(text("SELECT clock_timestamp()"))
            )
            assert await db.scalar(
                text(
                    "SELECT sum(reserved_usd) FROM agent_daily_budget_reservations "
                    "WHERE state='reserved'"
                )
            ) == Decimal("4.24384")
        before = await frozen_facts(factory)
        with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
            await call(
                "start", "--mode", "research_and_monitor", "--idempotency-key", "after-new-unknown"
            )
        with pytest.raises(ValueError, match="SINGLE_SOL_NATURAL_BUDGET_PROTECTED"):
            await call(
                "start-single-sol",
                "--instrument-id",
                str(member.instrument_id),
                "--admission-policy",
                "shared-budget",
                "--idempotency-key",
                "another-v5",
                "--expected-intent-hash",
                offer.intent_hash,
            )
        async with factory.begin() as db:
            with pytest.raises(ValueError, match="INDEPENDENT_UNKNOWN_ACCEPTANCE_BLOCKED"):
                await inspect_or_accept_independent_unknown(
                    db,
                    settings=settings,
                    request_id=fresh.request_id,
                    repo_root=root,
                    env_file=root / "fixture.env",
                )
        assert await call("read", "--request-id", str(fresh.request_id)) == new_view
        assert await frozen_facts(factory) == before
        from helpers.budgeted_single_maintenance_image import verify_images

        await verify_images(factory, settings, label="old-and-new-unknown")

    monkeypatch.setattr(single_sol_disposition, "verify_disposition", accepted_predecessor)
    await legacy.test_single_owner(
        migrated_database_url, tmp_path, monkeypatch, capsys, "oversized_critique"
    )
