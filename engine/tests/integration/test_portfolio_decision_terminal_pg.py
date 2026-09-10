from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from nexus.agents.portfolio_decision_contracts import AgentScenarioProjectionV1
from nexus.agents.portfolio_decision_execution import persist_portfolio_decision_terminal
from nexus.agents.responses_budget import UsageSnapshot
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    PortfolioDecisionBudgetReservationRecord,
    PortfolioDecisionOutputRecord,
    PortfolioDecisionRunRecord,
)
from nexus.persistence.models.diagnostics import DiagnosticPolicy, PortfolioDiagnosticReportRecord
from nexus.persistence.models.portfolio import (
    Account,
    BrokerSyncRun,
    Portfolio,
    PortfolioSnapshotRecord,
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import select, text

NOW = datetime(2026, 9, 10, 8, tzinfo=UTC)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("phase", "error_code", "usage"),
    (
        (
            "first_requested",
            "PORTFOLIO_DECISION_TOOL_REQUIRED",
            UsageSnapshot(
                input_tokens=100,
                output_tokens=50,
                tool_calls=0,
                provider_calls=1,
            ),
        ),
        ("tool_recorded", "PORTFOLIO_DECISION_TOOL_RESULT_MISMATCH", None),
    ),
)
async def test_abstained_terminal_is_atomic_at_the_actual_failure_phase(
    migrated_database_url: str,
    phase: str,
    error_code: str,
    usage: UsageSnapshot | None,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    run_id = await _seed_terminal_run(factory, phase=phase)

    async with factory() as session, session.begin():
        await persist_portfolio_decision_terminal(
            session,
            run_id=run_id,
            status="abstained",
            error_code=error_code,
            intent=None,
            usage=usage,
            usage_known=True,
        )

    async with factory() as session:
        run = await session.get(PortfolioDecisionRunRecord, run_id)
        output = await session.scalar(
            select(PortfolioDecisionOutputRecord).where(
                PortfolioDecisionOutputRecord.run_id == run_id
            )
        )
        reservation = await session.scalar(
            select(PortfolioDecisionBudgetReservationRecord).where(
                PortfolioDecisionBudgetReservationRecord.run_id == run_id
            )
        )
        assert run is not None and run.status == "abstained"
        assert output is not None and output.status == "abstained"
        assert output.details == {"error_code": error_code, "execution_locked": True}
        assert reservation is not None and reservation.state == "settled"
        assert reservation.settled_usd == Decimal("0.000100000000")


async def _seed_terminal_run(factory, *, phase: str) -> UUID:
    run_id = uuid4()
    account_id = uuid4()
    portfolio_id = uuid4()
    sync_id = uuid4()
    snapshot_id = uuid4()
    report_id = uuid4()
    policy_id = uuid4()
    prompt_id = uuid4()
    model_id = uuid4()
    projection = _projection() if phase == "tool_recorded" else None
    async with factory() as session, session.begin():
        session.add_all(
            (
                Account(id=account_id, provider="fosun", status="ACTIVE", created_at=NOW),
                Portfolio(
                    id=portfolio_id,
                    account_id=account_id,
                    name="terminal-fixture",
                    created_at=NOW,
                ),
                AgentPromptVersionRecord(
                    id=prompt_id,
                    role="portfolio_decision",
                    version=f"terminal-{run_id}",
                    content_hash="7" * 64,
                    content="bounded terminal fixture",
                    created_at=NOW,
                ),
                DiagnosticPolicy(
                    id=policy_id,
                    key=f"terminal-{run_id}",
                    version="v1",
                    schema_version="v1",
                    payload={},
                    created_at=NOW,
                ),
                AgentModelConfigRecord(
                id=model_id,
                model_id="fixture-model",
                version=f"terminal-{run_id}",
                config_hash="8" * 64,
                max_input_tokens=1000,
                max_output_tokens=1000,
                max_tool_calls=1,
                input_usd_per_million=Decimal("0.5"),
                output_usd_per_million=Decimal("1"),
                pricing_available_at=NOW,
                pricing_expires_at=datetime(2026, 9, 11, 8, tzinfo=UTC),
                created_at=NOW,
                ),
            )
        )
        await session.flush()
        session.add(
            BrokerSyncRun(
                id=sync_id,
                account_id=account_id,
                portfolio_id=portfolio_id,
                trigger_kind="MANUAL",
                actor_id="fixture",
                route="fixture",
                idempotency_key=f"terminal-{run_id}",
                request_hash="6" * 64,
                status="READY",
                accepted_at=NOW - timedelta(minutes=2),
                completed_at=NOW - timedelta(minutes=1),
                attempt_count=1,
                failure_reason=None,
                cash_flow_quality="PASS",
                created_at=NOW - timedelta(minutes=2),
            )
        )
        await session.flush()
        session.add(
            PortfolioSnapshotRecord(
                id=snapshot_id,
                portfolio_id=portfolio_id,
                sync_run_id=sync_id,
                stale_from_snapshot_id=None,
                status="READY",
                as_of=NOW - timedelta(minutes=1),
                position_hash_version="broker_position_hash_v1",
                position_hash="4" * 64,
                order_hash_version="broker_order_hash_v1",
                order_hash="5" * 64,
                source_request_ids=[],
                source_observations=[],
                created_at=NOW - timedelta(minutes=1),
            )
        )
        await session.flush()
        session.add(
            PortfolioDiagnosticReportRecord(
                id=report_id,
                portfolio_id=portfolio_id,
                portfolio_snapshot_id=snapshot_id,
                snapshot_as_of=NOW - timedelta(minutes=1),
                trading_day=NOW.date(),
                policy_id=policy_id,
                policy_version="real_advisory_v1",
                optimization_need="NO_ACTION",
                diagnostic_input_hash="3" * 64,
                created_at=NOW,
            )
        )
        await session.flush()
        session.add(
            PortfolioDecisionRunRecord(
                id=run_id,
                portfolio_id=portfolio_id,
                portfolio_snapshot_id=snapshot_id,
                diagnostic_report_id=report_id,
                prompt_version_id=prompt_id,
                model_config_id=model_id,
                actor_id="fixture",
                idempotency_key=f"terminal-{run_id}",
                request_hash="a" * 64,
                status="running",
                retry_count=0,
                decision_time=NOW,
                input_hash="a" * 64,
                schema_hash="a" * 64,
                frozen_input={},
                prompt_hash="a" * 64,
                model_hash="8" * 64,
                tool_hash="a" * 64,
                evidence_hash="a" * 64,
                evidence_manifest=[{}],
                tool_result_hash=projection.result_hash if projection is not None else None,
                tool_projection=projection.model_dump(mode="json")
                if projection is not None
                else None,
                provider_phase=phase,
                reserved_cost_usd=Decimal("0.001"),
                settled_cost_usd=None,
                pricing_version=f"terminal-{run_id}",
                input_tokens=100 if projection is not None else None,
                output_tokens=50 if projection is not None else None,
                created_at=NOW,
            )
        )
        await session.flush()
        await session.execute(
            text(
                "INSERT INTO portfolio_decision_budget_reservations ("
                "id,business_date,run_id,pricing_version,reserved_usd,state,created_at) "
                "VALUES (:id,:day,:run,:pricing,:reserved,'reserved',:now)"
            ),
            {
                "id": uuid4(),
                "day": NOW.date(),
                "run": run_id,
                "pricing": f"terminal-{run_id}",
                "reserved": Decimal("0.001"),
                "now": NOW,
            },
        )
    return run_id


def _projection() -> AgentScenarioProjectionV1:
    known = {"status": "known", "value": "0.4", "reason": None}
    unknown = {
        "status": "unknown",
        "value": None,
        "reason": "COST_EVIDENCE_INCOMPLETE",
    }
    metrics = {
        "security_top1": known,
        "security_top3": known,
        "security_hhi": known,
        "cash_weight": known,
        "one_way_turnover": known,
        "estimated_cost": unknown,
    }
    return AgentScenarioProjectionV1.model_validate(
        {
            "schema_version": "agent_scenario_projection_v1",
            "objective": "reduce_security_concentration",
            "objective_status": "improved_not_met",
            "analysis_status": "partial",
            "baseline": metrics,
            "candidate": metrics,
            "unknown_metrics": ["estimated_cost"],
            "hard_constraint_reasons": [],
            "changed_position_count": 1,
            "result_hash": "b" * 64,
            "citation_ids": ["c" * 64],
            "execution_locked": True,
        }
    )
