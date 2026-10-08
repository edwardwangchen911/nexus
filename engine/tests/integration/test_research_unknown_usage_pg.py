"""Unknown invocation usage retains dollars; measured fixture errors still settle."""

from decimal import Decimal

import pytest
from nexus.agents.committee import FixtureCommittee
from nexus.agents.execution import execute_research_run
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import _shared_daily_budget_used, admit_research_run
from nexus.agents.responses_budget import UsageSnapshot
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import select
from test_agent_execution_pg import _INSTRUMENT_ID, _MODEL_ID, _NOW, _SNAPSHOT_ID, _seed_authority


@pytest.mark.asyncio
@pytest.mark.parametrize("prior_known,uncertain", [(False, True), (True, True), (True, False)])
async def test_priced_terminal_retains_full_reservation_only_when_usage_unknown(
    migrated_database_url,
    prior_known,
    uncertain,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        prompt_ids = await _seed_authority(factory)
        async with factory.begin() as session:
            admitted = await admit_research_run(
                session,
                actor_id="isolated-usage-case",
                route="/internal/v1/research-runs",
                idempotency_key="unknown-usage",
                request_hash="a" * 64,
                request=ResearchRequest(
                    instrument_id=_INSTRUMENT_ID, data_snapshot_id=_SNAPSHOT_ID, decision_time=_NOW
                ),
                prompt_version_id=prompt_ids["orchestrator"],
                model_config_id=_MODEL_ID,
                daily_limit_usd=Decimal(3),
                now=_NOW,
            )

        class MeasuredFixtureFailure(FixtureCommittee):
            retry_on_error = False
            failure_reason = "model_usage_uncertain" if uncertain else "model_execution_failed"
            usage = UsageSnapshot(
                input_tokens=300 if prior_known else 0, output_tokens=80 if prior_known else 0
            )

            async def analyze(self, role, request, evidence_refs, prompt):
                if not prior_known or role != "orchestrator":
                    raise RuntimeError("synthetic unknown or measured failure")
                return await super().analyze(role, request, evidence_refs, prompt)

        async with factory.begin() as session:
            result = await execute_research_run(
                session, run_id=admitted.run_id, committee=MeasuredFixtureFailure()
            )
            assert result.status == "failed"
        async with factory.begin() as session:
            run = await session.get(AgentResearchRunRecord, admitted.run_id)
            reservation = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == run.id
                )
            )
            assert run.status == "failed" and run.retry_count == 0
            assert reservation.reserved_usd == Decimal("1.02")
            assert (run.input_tokens, run.output_tokens) == (
                (300, 80) if prior_known else (None, None)
            )
            from nexus.ops.research_budget_diagnostic import usage_is_known

            assert usage_is_known(run) is (not uncertain)
            shared = await _shared_daily_budget_used(session, business_date=_NOW.date())
            if uncertain:
                assert shared == Decimal("1.02")
                assert reservation.state == "reserved"
                assert reservation.settled_usd is None and run.settled_cost_usd is None
            else:
                assert shared == Decimal("0.00046")
                assert reservation.state == "settled"
                assert reservation.settled_usd == Decimal("0.00046")
    finally:
        await factory.kw["bind"].dispose()
