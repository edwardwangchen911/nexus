"""Normal explicit successor entry; synthetic transports and isolated native ledger."""

import json
from datetime import timedelta
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest
from integration import test_single_sol_request_pg as legacy
from nexus.ops import manual_paper
from nexus.ops.manual_paper_host import dispatch_pure_research_request
from nexus.paper.manual_request import read_manual_request
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.session import session_factory
from sqlalchemy import select, text


async def completed_predecessor(migrated_database_url, tmp_path, monkeypatch, capsys):
    captured = {}
    original = legacy.base.scope
    original_command = manual_paper.run

    async def scope(*args, **kwargs):
        result = await original(*args, **kwargs)
        captured["settings"], captured["store"], captured["ids"] = result[1:]
        return result

    monkeypatch.setattr(legacy.base, "scope", scope)

    async def command(settings, args):
        captured["settings"] = settings
        return await original_command(settings, args)

    monkeypatch.setattr(manual_paper, "run", command)
    await legacy.test_single_owner(migrated_database_url, tmp_path, monkeypatch, capsys, "complete")
    settings, store, ids = (captured[k] for k in ("settings", "store", "ids"))
    return settings, store, ids


@pytest.mark.asyncio
async def test_known_incomplete_v4_allows_explicit_v5_without_changing_failed_evidence(
    migrated_database_url, tmp_path, monkeypatch
):
    """A settled failed predecessor is usable history, never a successful research result."""
    from nexus.agents import platform_native
    from unit.agents.test_http_stage_lifecycle import Body, terminal
    from unit.agents.test_platform_usage import usage

    factory, settings, store, ids, calendar, _ = await legacy.single_scope(
        migrated_database_url, tmp_path, monkeypatch
    )

    async def call(*args):
        return await manual_paper.run(settings, manual_paper.parser().parse_args(list(args)))

    async def submit(key, *policy):
        offer = await call("inspect-single-sol-intent", "--instrument-id", str(ids[0]), *policy)
        return await call(
            "start-single-sol",
            "--instrument-id",
            str(ids[0]),
            "--idempotency-key",
            key,
            "--expected-intent-hash",
            offer.intent_hash,
            *policy,
        )

    sent = []
    clients = []

    def incomplete(request):
        sent.append(request.url.path)
        value = terminal("incomplete")
        value["response"].update(
            model="gpt-6.1-sol",
            service_tier="default",
            usage=usage(),
            incomplete_details={"reason": "max_output_tokens"},
        )
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=Body(("data: " + json.dumps(value) + "\n\n").encode()),
        )

    def client():
        result = httpx.AsyncClient(transport=httpx.MockTransport(incomplete))
        clients.append(result)
        return result

    async def old_facts(db, request_id, run_id):
        return (
            await db.scalar(
                text("SELECT row_to_json(r)::text FROM manual_paper_requests r WHERE id=:id"),
                {"id": request_id},
            ),
            await db.scalar(
                text("SELECT row_to_json(r)::text FROM agent_research_runs r WHERE id=:id"),
                {"id": run_id},
            ),
            await db.scalar(
                text(
                    "SELECT row_to_json(b)::text FROM agent_daily_budget_reservations b "
                    "WHERE run_id=:id"
                ),
                {"id": run_id},
            ),
            await db.scalar(text("SELECT row_to_json(g)::text FROM single_sol_budget_scopes g")),
        )

    try:
        old = await submit("original-v4-incomplete")
        # Install the isolated artifact store as well as the synthetic transport.
        legacy.install_sol_http(monkeypatch, store)
        monkeypatch.setattr(platform_native, "native_http_client", client)
        assert await dispatch_pure_research_request(
            settings, request_id=old.request_id, calendar_authority_id=calendar
        )
        assert len(sent) == 1 and all(c.is_closed for c in clients)
        async with factory() as db:
            row = await db.get(ManualPaperRequest, old.request_id)
            old_view = await read_manual_request(db, row)
            member = old_view.research.members[0]
            assert row.contract_version == 4 and old_view.status == "unavailable"
            assert member.usage_known
            assert await db.scalar(
                text("SELECT settled_usd FROM agent_daily_budget_reservations WHERE run_id=:id"),
                {"id": member.run_id},
            ) == Decimal("0.0299")
            assert await db.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 1
            before = await old_facts(db, old.request_id, member.run_id)
        # The unchanged old policy still refuses; only an explicit new policy gets a new identity.
        with pytest.raises(ValueError, match="SINGLE_SOL_BUDGET_SCOPE_ALREADY_EXISTS"):
            await call("inspect-single-sol-intent", "--instrument-id", str(ids[0]))
        new = await submit("explicit-v5-after-incomplete", "--admission-policy", "shared-budget")
        assert new.request_id != old.request_id
        successor_sent, _ = legacy.install_sol_http(monkeypatch, store)
        assert await dispatch_pure_research_request(
            settings, request_id=new.request_id, calendar_authority_id=calendar
        )
        async with factory() as db:
            row = await db.get(ManualPaperRequest, new.request_id)
            view = await read_manual_request(db, row)
            assert row.contract_version == 5 and view.status == "completed"
            assert view.research.members[0].run_id != member.run_id
            assert await db.scalar(
                text("SELECT settled_usd FROM agent_daily_budget_reservations WHERE run_id=:id"),
                {"id": view.research.members[0].run_id},
            ) == Decimal("0.0897")
            assert await old_facts(db, old.request_id, member.run_id) == before
            assert await db.scalar(text("SELECT count(*) FROM single_sol_budget_credits")) == 0
            assert (
                await db.scalar(text("SELECT count(*) FROM agent_daily_budget_reservations")) == 2
            )
            assert await db.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 4
        assert successor_sent == ["research", "critique", "synthesis"] and len(sent) == 1
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["complete", "output_limit", "timeout"])
async def test_same_day_successor_uses_own_reservation_and_one_day_guard(
    migrated_database_url, tmp_path, monkeypatch, capsys, outcome
):
    settings, store, ids = await completed_predecessor(
        migrated_database_url, tmp_path, monkeypatch, capsys
    )
    factory = session_factory(settings)

    async def call(*args):
        return await manual_paper.run(settings, manual_paper.parser().parse_args(list(args)))

    try:
        async with factory() as db:
            old = (await db.scalars(select(ManualPaperRequest))).one()
            original_request = old.id
            guard = await db.scalar(
                text("SELECT row_to_json(g)::text FROM single_sol_budget_scopes g")
            )
            original_reserves = dict(
                (
                    await db.execute(
                        text(
                            "SELECT run_id,row_to_json(b)::text "
                            "FROM agent_daily_budget_reservations b"
                        )
                    )
                ).all()
            )
            from nexus.paper.single_sol_owner import binding_for

            calendar = binding_for(old).calendar_id
        policy = ("--admission-policy", "shared-budget")
        offer = await call("inspect-single-sol-intent", "--instrument-id", str(ids[0]), *policy)
        receipt = await call(
            "start-single-sol",
            "--instrument-id",
            str(ids[0]),
            "--idempotency-key",
            "new-explicit-v5",
            "--expected-intent-hash",
            offer.intent_hash,
            *policy,
        )
        assert receipt.request_id != original_request and receipt.status == "queued"
        sent, _ = legacy.install_sol_http(monkeypatch, store)
        clients = []
        if outcome != "complete":
            from nexus.agents import platform_native
            from unit.agents.test_http_stage_lifecycle import Body, terminal
            from unit.agents.test_platform_usage import usage

            def handle(request):
                sent.append("research")
                if outcome == "timeout":
                    raise httpx.ReadTimeout("isolated transport timeout")
                value = terminal("incomplete")
                value["response"].update(
                    model="gpt-6.1-sol",
                    service_tier="default",
                    usage=usage(),
                    incomplete_details={"reason": "max_output_tokens"},
                )
                return httpx.Response(
                    200,
                    headers={"content-type": "text/event-stream"},
                    stream=Body(("data: " + json.dumps(value) + "\n\n").encode()),
                )

            def client():
                item = httpx.AsyncClient(transport=httpx.MockTransport(handle))
                clients.append(item)
                return item

            monkeypatch.setattr(platform_native, "native_http_client", client)
        assert await dispatch_pure_research_request(
            settings, request_id=receipt.request_id, calendar_authority_id=calendar
        )
        async with factory() as db:
            row = await db.get(ManualPaperRequest, receipt.request_id)
            assert row.contract_version == 5
            view = await read_manual_request(db, row)
            assert (
                view.status
                == {"complete": "completed", "output_limit": "unavailable", "timeout": "unknown"}[
                    outcome
                ]
            )
            assert len(view.research.members) == 1
            member = view.research.members[0]
            assert member.usage_known is (outcome != "timeout")
            if outcome == "output_limit":
                assert member.status == "failed" and view.code == "MANUAL_INPUTS_UNAVAILABLE"
            hold = (
                await db.execute(
                    text(
                        "SELECT state,settled_usd FROM agent_daily_budget_reservations "
                        "WHERE run_id=:run"
                    ),
                    {"run": member.run_id},
                )
            ).one()
            assert hold == (
                ("reserved", None)
                if outcome == "timeout"
                else ("settled", Decimal("0.0897" if outcome == "complete" else "0.0299"))
            )
            assert (
                await db.scalar(text("SELECT row_to_json(g)::text FROM single_sol_budget_scopes g"))
                == guard
            )
            for run, original_row in original_reserves.items():
                assert (
                    await db.scalar(
                        text(
                            "SELECT row_to_json(b)::text FROM agent_daily_budget_reservations b "
                            "WHERE run_id=:run"
                        ),
                        {"run": run},
                    )
                    == original_row
                )
            assert await db.scalar(text("SELECT count(*) FROM single_sol_budget_credits")) == 0
            assert await db.scalar(
                text("SELECT count(*) FROM research_stage_authorization WHERE run_id=:run"),
                {"run": member.run_id},
            ) == (3 if outcome == "complete" else 1)
            if outcome == "timeout":
                from nexus.paper.independent_unknown_proof import independent_unknown_proof

                with pytest.raises(ValueError, match="INDEPENDENT_UNKNOWN_ACCEPTANCE_BLOCKED"):
                    await independent_unknown_proof(
                        db, row=row, now=await db.scalar(text("SELECT clock_timestamp()"))
                    )
                from nexus.paper.manual_request import require_manual_account_available

                with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                    await require_manual_account_available(
                        db,
                        account_id=row.account_id,
                        now=await db.scalar(text("SELECT clock_timestamp()")),
                    )
        assert sent == (
            ["research", "critique", "synthesis"] if outcome == "complete" else ["research"]
        )
        assert all(item.is_closed for item in clients)
        if outcome == "timeout":
            # The preserved unknown reserve also blocks the shared-budget preview.
            with pytest.raises(ValueError, match="SINGLE_SOL_NATURAL_BUDGET_PROTECTED"):
                await call("inspect-single-sol-intent", "--instrument-id", str(ids[0]), *policy)
        else:
            # Only inspect next admission: no automatic successor submission.
            next_offer = await call(
                "inspect-single-sol-intent", "--instrument-id", str(ids[0]), *policy
            )
            assert next_offer.intent_hash == offer.intent_hash
        from helpers.budgeted_single_readback import verify_budgeted_readback

        await verify_budgeted_readback(factory, settings, view, outcome=outcome)
        from helpers.budgeted_single_maintenance_image import verify_images

        await verify_images(factory, settings, label=outcome)
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("timing", ["on_time", "deadline", "next_day"])
async def test_first_budgeted_owner_respects_original_day_and_deadline(
    migrated_database_url, tmp_path, monkeypatch, timing
):
    from nexus.paper import single_sol_research

    factory, settings, store, ids, calendar, _ = await legacy.single_scope(
        migrated_database_url, tmp_path, monkeypatch
    )
    policy = ("--admission-policy", "shared-budget")

    async def call(*args):
        return await manual_paper.run(settings, manual_paper.parser().parse_args(list(args)))

    try:
        offer = await call("inspect-single-sol-intent", "--instrument-id", str(ids[0]), *policy)
        receipt = await call(
            "start-single-sol",
            "--instrument-id",
            str(ids[0]),
            "--idempotency-key",
            "first-v5",
            "--expected-intent-hash",
            offer.intent_hash,
            *policy,
        )
        if timing != "on_time":
            late = offer.binding.deadline + (
                timedelta(days=1) if timing == "next_day" else timedelta()
            )

            async def clock(_db):
                return late

            monkeypatch.setattr(single_sol_research, "database_now", clock)
            with pytest.raises(ValueError, match="SINGLE_SOL_NATURAL_WINDOW_PROTECTED"):
                async with factory.begin() as db:
                    row = await db.get(ManualPaperRequest, receipt.request_id)
                    await single_sol_research.recheck_single_sol(db, settings, row)
            sent, _ = legacy.install_sol_http(monkeypatch, store)
            assert await dispatch_pure_research_request(
                settings, request_id=receipt.request_id, calendar_authority_id=calendar
            )
            async with factory() as db:
                assert (
                    await db.scalar(text("SELECT count(*) FROM agent_daily_budget_reservations"))
                    == 0
                )
                assert (
                    await db.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 0
                )
                assert await db.scalar(text("SELECT count(*) FROM single_sol_budget_scopes")) == 0
                row = await db.get(ManualPaperRequest, receipt.request_id)
                view = await read_manual_request(db, row)
                assert view.status == "unavailable" and view.code == "MANUAL_INPUTS_UNAVAILABLE"
                assert not sent
            return
        sent, _ = legacy.install_sol_http(monkeypatch, store)
        assert await dispatch_pure_research_request(
            settings, request_id=receipt.request_id, calendar_authority_id=calendar
        )
        async with factory() as db:
            row = await db.get(ManualPaperRequest, receipt.request_id)
            view = await read_manual_request(db, row)
            assert view.status == "completed" and view.research.members[0].usage_known
            assert (
                await db.scalar(text("SELECT source_run_id FROM single_sol_budget_scopes"))
                == view.research.members[0].run_id
            )
            assert await db.scalar(text("SELECT count(*) FROM single_sol_budget_scopes")) == 1
            assert await db.scalar(text("SELECT count(*) FROM single_sol_budget_credits")) == 0
        assert sent == ["research", "critique", "synthesis"]
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_new_keys_cannot_bypass_account_serialization(
    migrated_database_url, tmp_path, monkeypatch, capsys
):
    from nexus.agents.budget import ResearchAccountingBusy
    from nexus.paper.single_sol_research import start_single_sol

    settings, _, ids = await completed_predecessor(
        migrated_database_url, tmp_path, monkeypatch, capsys
    )
    factory = session_factory(settings)

    async def call(*args):
        return await manual_paper.run(settings, manual_paper.parser().parse_args(list(args)))

    policy = ("--admission-policy", "shared-budget")
    try:
        offer = await call("inspect-single-sol-intent", "--instrument-id", str(ids[0]), *policy)

        async def submit(key):
            return await call(
                "start-single-sol",
                "--instrument-id",
                str(ids[0]),
                "--idempotency-key",
                key,
                "--expected-intent-hash",
                offer.intent_hash,
                *policy,
            )

        # Hold the real first admission transaction across the competing CLI call.
        async with factory.begin() as owner:
            first = await start_single_sol(
                owner,
                settings,
                instrument_id=ids[0],
                idempotency_key="serial-first",
                expected_intent_hash=offer.intent_hash,
                shared_budget=True,
            )
            with pytest.raises(ResearchAccountingBusy, match="RESEARCH_ACCOUNTING_BUSY"):
                await submit("serial-contender")
        repeat = await submit("serial-first")
        assert repeat.request_id == first.request_id and repeat.status == "queued"
        with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
            await submit("serial-next")
        async with factory() as db:
            assert (
                await db.scalar(
                    text("SELECT count(*) FROM manual_paper_requests WHERE contract_version=5")
                )
                == 1
            )
            assert await db.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 3
            assert (
                await db.scalar(text("SELECT count(*) FROM agent_daily_budget_reservations")) == 1
            )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_new_request_cannot_spend_remaining_natural_obligations(
    migrated_database_url, tmp_path, monkeypatch, capsys
):
    from nexus.agents.persistence import reserve_daily_budget
    from nexus.persistence.models.agents import AgentResearchRunRecord

    settings, _, ids = await completed_predecessor(
        migrated_database_url, tmp_path, monkeypatch, capsys
    )
    factory = session_factory(settings)
    try:
        async with factory.begin() as db:
            source = (await db.scalars(select(AgentResearchRunRecord))).one()
            baseline = {
                column.name: getattr(source, column.name) for column in source.__table__.columns
            }
            identity = uuid4()
            now = await db.scalar(text("SELECT clock_timestamp()"))
            db.add(
                AgentResearchRunRecord(
                    **(
                        baseline
                        | dict(
                            id=identity,
                            actor_id="ordinary-fixture",
                            idempotency_key=str(identity),
                            status="prepared",
                            decision_time=now,
                            created_at=now,
                            input_hash=identity.hex * 2,
                            reserved_cost_usd=Decimal("0.20"),
                            settled_cost_usd=None,
                            input_tokens=None,
                            output_tokens=None,
                        )
                    )
                )
            )
            await db.flush()
            await reserve_daily_budget(
                db,
                run_id=identity,
                now=now,
                daily_limit_usd=Decimal("13"),
                worst_case_usd=Decimal("0.20"),
                pricing_version=source.pricing_version,
                pricing_fresh=True,
            )
        # 0.0897 known + 0.20 reserved + 10.60960 protected + 2.12192 new > 13.
        with pytest.raises(ValueError, match="SINGLE_SOL_NATURAL_BUDGET_PROTECTED"):
            await manual_paper.run(
                settings,
                manual_paper.parser().parse_args(
                    [
                        "inspect-single-sol-intent",
                        "--instrument-id",
                        str(ids[0]),
                        "--admission-policy",
                        "shared-budget",
                    ]
                ),
            )
        async with factory() as db:
            assert await db.scalar(text("SELECT count(*) FROM manual_paper_requests")) == 1
            assert await db.scalar(text("SELECT count(*) FROM single_sol_budget_credits")) == 0
            assert (
                await db.execute(
                    text(
                        "SELECT state,reserved_usd,settled_usd "
                        "FROM agent_daily_budget_reservations WHERE run_id=:run"
                    ),
                    {"run": identity},
                )
            ).one() == ("reserved", Decimal("0.20"), None)
    finally:
        await factory.kw["bind"].dispose()
