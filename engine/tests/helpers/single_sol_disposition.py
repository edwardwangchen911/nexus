"""Exercise the maintenance entry on a real, terminal synthetic v4 request."""

import json
import os
from contextlib import nullcontext
from datetime import UTC, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from nexus.agents.budget import BudgetExhausted
from nexus.agents.persistence import _shared_daily_budget_used, reserve_daily_budget
from nexus.agents.research_stages.protocol import StagedProtocol
from nexus.jobs.models import job_input_hash
from nexus.ops import manual_paper
from nexus.ops.manual_paper_host import peek_manual_request
from nexus.paper import manual_preparation_close
from nexus.paper.independent_unknown_acceptance import (
    read_independent_disposition,
    require_independent_execution_open,
    require_single_sol_successor_history,
)
from nexus.paper.independent_unknown_proof import independent_unknown_proof
from nexus.paper.manual_request import (
    require_manual_account_available,
    require_manual_request_history,
)
from nexus.persistence.models.manual_paper import ManualPaperRequest
from sqlalchemy import func, select, text


def capture_second_ticket(monkeypatch):
    captured = []
    authorize = StagedProtocol.authorize

    async def issue(protocol, *args, **options):
        ticket = await authorize(protocol, *args, **options)
        if ticket.stage == 2:
            captured.append(ticket)
        return ticket

    monkeypatch.setattr(StagedProtocol, "authorize", issue)
    return captured


async def verify_disposition(factory, settings, request_id, member, root, monkeypatch, ticket):
    monkeypatch.setattr(manual_paper, "_require_maintenance_code", lambda *_: None)
    monkeypatch.setattr(manual_paper, "preparation_service_control_lock", lambda *_: nullcontext())
    monkeypatch.setattr(
        manual_preparation_close, "require_preparation_producers_stopped", lambda *_: None
    )
    base = ["--repo-root", str(root), "--env-file", str(root / "fixture.env")]

    async def verify_workspace(disposition):
        output = os.environ.get("NEXUS_TEST_SINGLE_SOL_BROWSER_DIR")
        if not output:
            return
        from helpers.monitoring_readview_browser import verify_monitoring_browser
        from helpers.paper_api import settings as api_settings
        from nexus.main import create_engine_app

        assert settings.database_url is not None
        api = create_engine_app(
            api_settings(settings.database_url).model_copy(
                update={"agent_paper_account_id": settings.agent_paper_account_id}
            )
        )
        directory = Path(output) / (disposition or "unaccepted")
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "single-sol.json").write_text(
            json.dumps({"request_id": str(request_id), "disposition": disposition})
        )
        await verify_monitoring_browser(
            api, directory=directory, spec_path="tests/e2e/single-sol-disposition-live.spec.ts"
        )

    async def command(action, *options, identity=request_id):
        return await manual_paper.run(
            settings,
            manual_paper.parser().parse_args(
                [*base, action, "--request-id", str(identity), *options]
            ),
        )

    async with factory.begin() as db:
        row = await db.get(ManualPaperRequest, request_id)
        now = await db.scalar(select(func.clock_timestamp()))
        digest = job_input_hash(await independent_unknown_proof(db, row=row, now=now))
        money = text(
            "SELECT state,reserved_usd,settled_usd,pricing_version "
            "FROM agent_daily_budget_reservations "
            "WHERE run_id=:r"
        )
        held = (await db.execute(money, {"r": member.run_id})).one()
        assert held.state == "reserved" and held.settled_usd is None
    check = "check-independent-acceptance"
    accept = "accept-independent-unknown"
    consent = "--accept-unknown-effects", "--expected-proof-hash"
    with pytest.raises(ValueError):
        await command(check)
    pin = root / ".secrets/nexus-agent-paper/single-sol-disposition.sha256"
    pin.parent.mkdir(parents=True, exist_ok=True)
    pin.write_text(digest)
    pin.chmod(0o600)
    checked = await command(check)
    assert checked.status == "eligible_at_check" and checked.proof_hash == digest
    with pytest.raises(ValueError):
        await command(check, identity=uuid4())
    with pytest.raises(ValueError):
        await command(accept, *consent, "0" * 64)
    async with factory.begin() as db:
        with pytest.raises(ValueError):
            await require_manual_request_history(
                db, account_id=settings.agent_paper_account_id, now=now
            )
        assert (
            await peek_manual_request(db, account_id=settings.agent_paper_account_id)
        ).status == "unknown"
        with pytest.raises(ValueError):
            await require_single_sol_successor_history(
                db, account_id=settings.agent_paper_account_id, now=now
            )
    await verify_workspace(None)
    accepted = await command(accept, *consent, digest)
    replay = await command(accept, *consent, digest)
    assert not accepted.replayed and replay.replayed and replay.accepted_at == accepted.accepted_at
    async with factory.begin() as db:
        now = await db.scalar(select(func.clock_timestamp()))
        await require_manual_request_history(
            db, account_id=settings.agent_paper_account_id, now=now
        )
        await require_manual_request_history(
            db, account_id=settings.agent_paper_account_id, now=now + timedelta(days=1)
        )
        assert await peek_manual_request(db, account_id=settings.agent_paper_account_id) is None
        await require_manual_account_available(
            db, account_id=settings.agent_paper_account_id, now=now, read_only=True
        )
        await require_single_sol_successor_history(
            db, account_id=settings.agent_paper_account_id, now=now
        )
        with pytest.raises(ValueError):
            await require_single_sol_successor_history(
                db, account_id=settings.agent_paper_account_id, now=now + timedelta(days=1)
            )
        # Advance the isolated database clock as well as the caller's clock.
        await db.execute(text("CREATE SCHEMA fixture_next_day"))
        await db.execute(
            text("""
            CREATE FUNCTION fixture_next_day.clock_timestamp() RETURNS timestamptz
            LANGUAGE SQL VOLATILE AS $$ SELECT fixture_clock.clock_timestamp() + interval '1 day' $$
        """)
        )
        await db.execute(
            text("SET LOCAL search_path TO fixture_next_day,fixture_clock,public,pg_catalog")
        )
        await require_single_sol_successor_history(
            db, account_id=settings.agent_paper_account_id, now=now + timedelta(days=1)
        )
        next_day = (now + timedelta(days=1)).astimezone(UTC)
        assert await _shared_daily_budget_used(db, business_date=now.date()) == held.reserved_usd
        assert await _shared_daily_budget_used(db, business_date=next_day.date()) == 0
        # The history is acknowledged but a new allocation still has to fit
        # today's capacity, including any explicitly required batch headroom.
        with pytest.raises(BudgetExhausted, match="COMPARISON_ROUND_BUDGET_EXHAUSTED"):
            await reserve_daily_budget(
                db,
                run_id=uuid4(),
                now=next_day,
                daily_limit_usd=settings.agent_daily_budget_usd,
                worst_case_usd=held.reserved_usd,
                pricing_version=held.pricing_version,
                pricing_fresh=True,
                required_capacity_usd=settings.agent_daily_budget_usd + held.reserved_usd,
            )
        assert await _shared_daily_budget_used(db, business_date=next_day.date()) == 0
        with pytest.raises(ValueError):
            await require_independent_execution_open(db, request_id=request_id)
        assert (await db.execute(money, {"r": member.run_id})).one() == held
        assert (
            await db.scalar(
                text("SELECT count(*) FROM research_stage_authorization WHERE run_id=:r"),
                {"r": member.run_id},
            )
            == 2
        )

    await verify_workspace("unknown_acknowledged")
    async with factory.begin() as writer:
        await writer.execute(
            text("SELECT id FROM research_stage_scope WHERE id=:id FOR UPDATE"),
            {"id": request_id},
        )
        async with factory.begin() as competitor:
            with pytest.raises(ValueError):
                await require_single_sol_successor_history(
                    competitor, account_id=settings.agent_paper_account_id, now=now
                )
    # Deliver a late synthetic response through its original ticket. Even at
    # the acceptance timestamp, it changes the proof without rewriting the bill.
    async with factory.begin() as db:
        first = (
            await db.execute(
                text("""
            SELECT platform_response,input_tokens,output_tokens FROM research_stage_observation
            WHERE run_id=:run AND stage=1
        """),
                {"run": member.run_id},
            )
        ).one()
    response = json.loads(first.platform_response)
    response["id"] = "resp_synthetic_late_second"
    payload = json.dumps(response, separators=(",", ":")).encode()
    protocol = StagedProtocol(factory)
    for _ in range(2):
        await protocol.observe(
            ticket,
            now=accepted.accepted_at,
            payload=payload,
            tokens=(first.input_tokens, first.output_tokens),
        )
    async with factory.begin() as db:
        late = (
            await db.execute(
                text("""
            SELECT stage,eligible,observed_at FROM research_stage_observation
            WHERE run_id=:run AND stage=2
        """),
                {"run": member.run_id},
            )
        ).one()
        assert late.stage == 2 and not late.eligible and late.observed_at == accepted.accepted_at
        row = await db.get(ManualPaperRequest, request_id)
        assert await read_independent_disposition(db, row, now=now) == "recheck_required"
        with pytest.raises(ValueError):
            await require_single_sol_successor_history(
                db, account_id=settings.agent_paper_account_id, now=now
            )
        with pytest.raises(ValueError):
            await require_independent_execution_open(db, request_id=request_id)
        assert (await db.execute(money, {"r": member.run_id})).one() == held
    with pytest.raises(ValueError):
        await command(accept, *consent, digest)
    await verify_workspace("recheck_required")
