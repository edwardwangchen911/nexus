"""One selected member in the existing Sol batch, without acquiring any send right."""

from datetime import timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from integration.test_sol_native_preparation_pg import admit, scope
from nexus.audit.service import append_audit_event
from nexus.ops import manual_paper
from nexus.paper import preparation_unknown_ack, single_sol_research
from nexus.paper.research_request import prepare_research_request
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.models.runtime import Job
from sqlalchemy import func, select


@pytest.mark.asyncio
async def test_single_selection_uses_normal_cli_and_never_enqueues(
    migrated_database_url, tmp_path, monkeypatch
):
    factory, settings, store, ids = await scope(migrated_database_url, tmp_path)
    try:

        def args(instrument):
            return manual_paper.parser().parse_args(
                ["inspect-single-sol", "--instrument-id", str(instrument)]
            )

        before = await manual_paper.run(settings, args(ids[0]))
        repeat = await manual_paper.run(settings, args(ids[0]))
        different = await manual_paper.run(settings, args(ids[1]))
        assert before.selection_hash == repeat.selection_hash != different.selection_hash
        assert before.selected_instrument_id == ids[0]
        assert before.batch_member_count == 3
        assert before.submit_authorized is False and before.budget_reserved is False
        assert before.natural_research_maximum_usd == 3 * before.single_maximum_usd
        assert (
            before.synthesis_maximum_usd
            == before.increment_maximum_usd
            == before.single_maximum_usd
        )
        assert before.used_usd == 0
        assert (
            before.full_session_capacity_sufficient is False
        )  # fixture limit is 10, less than six worst-case tasks
        with pytest.raises(ValueError, match="SINGLE_SOL_NOT_BATCH_MEMBER"):
            await manual_paper.run(settings, args(UUID(int=0)))
        async with factory() as db:
            for table in (
                AgentDailyBudgetReservationRecord,
                AgentResearchRunRecord,
                Job,
                ManualPaperRequest,
            ):
                assert await db.scalar(select(func.count()).select_from(table)) == 0
        exact = 6 * before.single_maximum_usd
        funded = settings.model_copy(update={"agent_daily_budget_usd": exact})
        at_limit = await manual_paper.run(funded, args(ids[0]))
        assert at_limit.full_session_capacity_sufficient and not at_limit.budget_reserved
        below = settings.model_copy(update={"agent_daily_budget_usd": exact - Decimal("0.000001")})
        assert not (await manual_paper.run(below, args(ids[0]))).full_session_capacity_sufficient

        # New independent v3 holds are charged to this very same ledger.
        request_id = await admit(factory, settings, store)
        async with factory.begin() as db:
            db.info["sec_exhibit_store"] = store
            await prepare_research_request(db, settings, request_id=request_id)
        occupied = await manual_paper.run(funded, args(ids[0]))
        assert occupied.selection_hash == at_limit.selection_hash
        assert occupied.used_usd == 3 * before.single_maximum_usd
        assert not occupied.full_session_capacity_sufficient and not occupied.submit_authorized

        # A later audit cannot silently enlarge the scope of the old exception.
        async with factory.begin() as db:
            await append_audit_event(
                db,
                actor_id="isolated-test",
                action=preparation_unknown_ack.ACTION,
                schema_version="fixture",
                payload={"account_id": str(settings.agent_paper_account_id)},
                occurred_at=before.checked_at,
            )
        with pytest.raises(ValueError, match="MANUAL_PREPARATION_ACK_BLOCKED"):
            await manual_paper.run(funded, args(ids[0]))

        clock = iter((before.checked_at, before.checked_at + timedelta(days=1)))

        async def crossed_day(session):
            return next(clock)

        monkeypatch.setattr(single_sol_research, "database_now", crossed_day)
        with pytest.raises(ValueError, match="MANUAL_RESULT_UNKNOWN"):
            await manual_paper.run(funded, args(ids[0]))
    finally:
        await factory.kw["bind"].dispose()
