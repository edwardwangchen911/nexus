"""PostgreSQL proof for atomic portfolio-decision admission."""

import hashlib
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.agents.budget import BudgetExhausted
from nexus.agents.portfolio_decision_contracts import (
    PortfolioDecisionInputV2,
    canonical_portfolio_decision_hash,
)
from nexus.agents.portfolio_decision_persistence import (
    admit_portfolio_decision_run,
    canonical_portfolio_intent_schema_hash,
)
from nexus.agents.portfolio_decision_tools import (
    canonical_portfolio_evidence_manifest_hash,
    canonical_portfolio_tool_hash,
)
from nexus.diagnostics.models import PortfolioDiagnosticReport
from nexus.diagnostics.policy import (
    REAL_ADVISORY_POLICY_ID,
    REAL_ADVISORY_POLICY_VERSION,
    RealAdvisoryPolicy,
)
from nexus.optimizer.scenario_contracts import (
    AdjustmentScenarioConstraintsV1,
    PortfolioScenarioRequestV1,
)
from nexus.optimizer.scenario_spread_cost import ScenarioSpreadEvidenceManifestV1
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    PortfolioDecisionBudgetReservationRecord,
    PortfolioDecisionRunRecord,
)
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.portfolio import (
    Account,
    BrokerSyncRun,
    Portfolio,
    PortfolioSnapshotRecord,
)
from nexus.persistence.models.runtime import IdempotencyRecord, Job
from nexus.persistence.session import session_factory
from nexus.providers.broker.base import PortfolioSnapshot
from nexus.reconciliation.persistence_errors import IdempotencyConflict
from nexus.settings import EngineSettings
from sqlalchemy import func, select

NOW = datetime(2026, 9, 10, 8, tzinfo=UTC)
PORTFOLIO_ID = UUID("0198d500-1000-7000-8000-000000002001")
SNAPSHOT_ID = UUID("0198d500-1000-7000-8000-000000002002")
REPORT_ID = UUID("0198d500-1000-7000-8000-000000002003")
PROMPT_ID = UUID("0198d500-1000-7000-8000-000000002004")
MODEL_ID = UUID("0198d500-1000-7000-8000-000000002005")


@pytest.mark.asyncio
async def test_admission_is_idempotent_and_persists_one_locked_no_retry_job(
    migrated_database_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    frozen_snapshot = PortfolioSnapshot(
        id=SNAPSHOT_ID,
        portfolio_id=PORTFOLIO_ID,
        status="READY",
        cash=(),
        positions=(),
        open_orders=(),
        as_of=NOW - timedelta(minutes=1),
        stale_from_snapshot_id=None,
        source_request_ids=(),
    )
    report = PortfolioDiagnosticReport(
        portfolio_snapshot_id=SNAPSHOT_ID,
        snapshot_as_of=NOW - timedelta(minutes=1),
        trading_day=NOW.date(),
        policy_version="real_advisory_policy_v1",
        optimization_need="NO_ACTION",
        items=(),
    )
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_persistence.load_frozen_snapshot",
        AsyncMock(return_value=frozen_snapshot),
    )
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_persistence.read_report",
        AsyncMock(return_value=report),
    )
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_persistence.load_instrument_identities",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_persistence.load_real_advisory_policy_v1",
        AsyncMock(return_value=RealAdvisoryPolicy.v1()),
    )
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_persistence.evaluate_adjustment_scenario",
        lambda **_: type("Evaluation", (), {"result_hash": "e" * 64})(),
    )
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_persistence._select_scenario_claim",
        AsyncMock(side_effect=_select_scenario_claim),
    )
    request = PortfolioScenarioRequestV1(
        schema_version="portfolio_scenario_request_v1",
        portfolio_snapshot_id=SNAPSHOT_ID,
        diagnostic_report_id=REPORT_ID,
        as_of=NOW - timedelta(minutes=1),
        constraints=AdjustmentScenarioConstraintsV1(
            schema_version="adjustment_scenario_constraints_v1",
            objective="reduce_security_concentration",
            max_turnover=Decimal("0.10"),
            minimum_cash=Decimal("0.10"),
            universe_policy="current_holdings_only",
            allow_new_positions=False,
        ),
    )

    async with factory() as session, session.begin():
        first = await admit_portfolio_decision_run(
            session,
            actor_id="fixture-actor",
            route="/internal/v1/portfolio-decisions",
            idempotency_key="same-key",
            request_hash="a" * 64,
            request=request,
            expected_scenario_result_hash="e" * 64,
            prompt_version_id=PROMPT_ID,
            model_config_id=MODEL_ID,
            daily_limit_usd=Decimal("1"),
            now=NOW,
            scenario_evidence_store=object(),  # type: ignore[arg-type]
        )
    async with factory() as session, session.begin():
        replay = await admit_portfolio_decision_run(
            session,
            actor_id="fixture-actor",
            route="/internal/v1/portfolio-decisions",
            idempotency_key="same-key",
            request_hash="a" * 64,
            request=request,
            expected_scenario_result_hash="e" * 64,
            prompt_version_id=PROMPT_ID,
            model_config_id=MODEL_ID,
            daily_limit_usd=Decimal("1"),
            now=NOW + timedelta(seconds=1),
            scenario_evidence_store=object(),  # type: ignore[arg-type]
        )
    assert replay.run_id == first.run_id
    assert (first.replayed, replay.replayed) == (False, True)

    async with factory() as session, session.begin():
        other_actor = await admit_portfolio_decision_run(
            session,
            actor_id="other-actor",
            route="/internal/v1/portfolio-decisions",
            idempotency_key="other-key",
            request_hash="a" * 64,
            request=request,
            expected_scenario_result_hash="e" * 64,
            prompt_version_id=PROMPT_ID,
            model_config_id=MODEL_ID,
            daily_limit_usd=Decimal("1"),
            now=NOW,
            scenario_evidence_store=object(),  # type: ignore[arg-type]
        )
    assert other_actor.run_id != first.run_id
    assert other_actor.replayed is False

    with pytest.raises(IdempotencyConflict):
        async with factory() as session, session.begin():
            await admit_portfolio_decision_run(
                session,
                actor_id="fixture-actor",
                route="/internal/v1/portfolio-decisions",
                idempotency_key="same-key",
                request_hash="b" * 64,
                request=request,
                expected_scenario_result_hash="e" * 64,
                prompt_version_id=PROMPT_ID,
                model_config_id=MODEL_ID,
                daily_limit_usd=Decimal("1"),
                now=NOW,
                scenario_evidence_store=object(),  # type: ignore[arg-type]
            )

    with pytest.raises(BudgetExhausted, match="daily research budget exhausted"):
        async with factory() as session, session.begin():
            await admit_portfolio_decision_run(
                session,
                actor_id="budget-rejected-actor",
                route="/internal/v1/portfolio-decisions",
                idempotency_key="budget-rejected",
                request_hash="c" * 64,
                request=request,
                expected_scenario_result_hash="e" * 64,
                prompt_version_id=PROMPT_ID,
                model_config_id=MODEL_ID,
                daily_limit_usd=Decimal("0.0001"),
                now=NOW + timedelta(seconds=2),
                scenario_evidence_store=object(),  # type: ignore[arg-type]
            )

    with pytest.raises(ValueError, match="scenario result authority changed"):
        async with factory() as session, session.begin():
            await admit_portfolio_decision_run(
                session,
                actor_id="stale-scenario-actor",
                route="/internal/v1/portfolio-decisions",
                idempotency_key="stale-scenario",
                request_hash="9" * 64,
                request=request,
                expected_scenario_result_hash="8" * 64,
                prompt_version_id=PROMPT_ID,
                model_config_id=MODEL_ID,
                daily_limit_usd=Decimal("1"),
                now=NOW + timedelta(seconds=3),
                scenario_evidence_store=object(),  # type: ignore[arg-type]
            )

    async with factory() as session:
        run = await session.get(PortfolioDecisionRunRecord, first.run_id)
        jobs = list(await session.scalars(select(Job)))
        reservations = list(await session.scalars(select(PortfolioDecisionBudgetReservationRecord)))
        assert await session.scalar(select(func.count()).select_from(IdempotencyRecord)) == 2
        run_count = await session.scalar(
            select(func.count()).select_from(PortfolioDecisionRunRecord)
        )
        assert run_count == 2
        assert len(jobs) == 2
    assert run is not None
    job = next(item for item in jobs if item.payload == {"run_id": str(run.id)})
    reservation = next(item for item in reservations if item.run_id == run.id)
    assert (run.status, run.retry_count, run.provider_phase, run.tool_projection) == (
        "prepared",
        0,
        "none",
        None,
    )
    assert (job.schema_version, job.payload, job.retry_policy, job.max_attempts) == (
        "portfolio_decision_job_v1",
        {"run_id": str(run.id)},
        "never",
        1,
    )
    assert reservation.run_id == run.id and reservation.state == "reserved"
    frozen = PortfolioDecisionInputV2.model_validate(run.frozen_input)
    assert canonical_portfolio_decision_hash(frozen) == run.input_hash
    assert canonical_portfolio_intent_schema_hash() == run.schema_hash
    assert canonical_portfolio_tool_hash() == run.tool_hash
    assert canonical_portfolio_evidence_manifest_hash(run.evidence_manifest) == run.evidence_hash


@pytest.mark.asyncio
async def test_admission_rejects_non_ready_sync_before_loading_authority(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory, sync_status="PARTIAL")
    request = PortfolioScenarioRequestV1(
        schema_version="portfolio_scenario_request_v1",
        portfolio_snapshot_id=SNAPSHOT_ID,
        diagnostic_report_id=REPORT_ID,
        as_of=NOW - timedelta(minutes=1),
        constraints=AdjustmentScenarioConstraintsV1(
            schema_version="adjustment_scenario_constraints_v1",
            objective="reduce_security_concentration",
            max_turnover=Decimal("0.10"),
            minimum_cash=Decimal("0.10"),
            universe_policy="current_holdings_only",
            allow_new_positions=False,
        ),
    )
    with pytest.raises(ValueError, match="point-in-time portfolio authority is invalid"):
        async with factory() as session, session.begin():
            await admit_portfolio_decision_run(
                session,
                actor_id="fixture-actor",
                route="/internal/v1/portfolio-decisions",
                idempotency_key="unready-sync",
                request_hash="7" * 64,
                request=request,
                expected_scenario_result_hash="e" * 64,
                prompt_version_id=PROMPT_ID,
                model_config_id=MODEL_ID,
                daily_limit_usd=Decimal("1"),
                now=NOW,
                scenario_evidence_store=object(),  # type: ignore[arg-type]
            )
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(IdempotencyRecord)) == 0
        run_count = await session.scalar(
            select(func.count()).select_from(PortfolioDecisionRunRecord)
        )
        assert run_count == 0
        assert await session.scalar(select(func.count()).select_from(Job)) == 0


def _scenario_claim_selection():
    manifest = ScenarioSpreadEvidenceManifestV1(
        manifest_schema="scenario_spread_evidence_manifest_v1",
        evidence_selected_at=NOW - timedelta(seconds=1),
        policy_status="unavailable",
        source_policy_id=None,
        source_policy_version=None,
        source_policy_payload_hash=None,
        calendar_authority=None,
        calculation_version="scenario_spread_cost_v1",
        no_cost_candidate_hash="1" * 64,
        scenario_cost_basis_hash="2" * 64,
        regulatory_reference_hash=None,
        selected_quotes=(),
        expected_result_hash="e" * 64,
    )
    return SimpleNamespace(id=UUID("0198d500-1000-7000-8000-000000002099")), manifest


async def _select_scenario_claim(*args, expected_result_hash: str, **kwargs):
    if expected_result_hash != "e" * 64:
        raise ValueError("portfolio scenario result authority changed")
    return _scenario_claim_selection()


async def _seed_authority(factory, *, sync_status: str = "READY") -> None:
    account_id = UUID("0198d500-1000-7000-8000-000000002011")
    sync_id = UUID("0198d500-1000-7000-8000-000000002012")
    prompt = "bounded portfolio decision prompt"
    async with factory() as session, session.begin():
        session.add_all(
            (
                Account(id=account_id, provider="fosun", status="ACTIVE", created_at=NOW),
                Portfolio(id=PORTFOLIO_ID, account_id=account_id, name="fixture", created_at=NOW),
                AgentPromptVersionRecord(
                    id=PROMPT_ID,
                    role="portfolio_decision",
                    version="portfolio-decision-v1",
                    content_hash=hashlib.sha256(prompt.encode()).hexdigest(),
                    content=prompt,
                    created_at=NOW - timedelta(minutes=2),
                ),
                AgentModelConfigRecord(
                    id=MODEL_ID,
                    model_id="fixture-model",
                    version="fixture-portfolio-v1",
                    config_hash="d" * 64,
                    max_input_tokens=100,
                    max_output_tokens=100,
                    max_tool_calls=1,
                    input_usd_per_million=Decimal("1"),
                    output_usd_per_million=Decimal("1"),
                    pricing_available_at=NOW - timedelta(days=1),
                    pricing_expires_at=NOW + timedelta(days=1),
                    created_at=NOW - timedelta(minutes=2),
                ),
            )
        )
        await session.flush()
        session.add(
            BrokerSyncRun(
                id=sync_id,
                account_id=account_id,
                portfolio_id=PORTFOLIO_ID,
                trigger_kind="MANUAL",
                actor_id="fixture",
                route="fixture",
                idempotency_key="fixture",
                request_hash="c" * 64,
                status=sync_status,
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
                id=SNAPSHOT_ID,
                portfolio_id=PORTFOLIO_ID,
                sync_run_id=sync_id,
                stale_from_snapshot_id=None,
                status="READY",
                as_of=NOW - timedelta(minutes=1),
                position_hash_version="broker_position_hash_v1",
                position_hash="a" * 64,
                order_hash_version="broker_order_hash_v1",
                order_hash="b" * 64,
                source_request_ids=[],
                source_observations=[],
                created_at=NOW - timedelta(minutes=1),
            )
        )
        await session.flush()
        session.add(
            PortfolioDiagnosticReportRecord(
                id=REPORT_ID,
                portfolio_id=PORTFOLIO_ID,
                portfolio_snapshot_id=SNAPSHOT_ID,
                snapshot_as_of=NOW - timedelta(minutes=1),
                trading_day=NOW.date(),
                policy_id=REAL_ADVISORY_POLICY_ID,
                policy_version=REAL_ADVISORY_POLICY_VERSION,
                optimization_need="NO_ACTION",
                diagnostic_input_hash="f" * 64,
                created_at=NOW - timedelta(seconds=30),
            )
        )
