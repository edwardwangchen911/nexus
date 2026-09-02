"""Synchronous first-draft construction from stored facts, without formal admission."""

import hashlib
import json
from dataclasses import replace
from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.diagnostics.artifacts import ArtifactByteReader
from nexus.diagnostics.assembler import load_diagnostic_source_policy
from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID, load_real_advisory_policy_v1
from nexus.diagnostics.repository import (
    load_frozen_snapshot,
    load_instrument_identities,
    read_report,
)
from nexus.market.scope import US_EQUITY_EXCHANGES
from nexus.optimizer.compare import TypedMetric
from nexus.optimizer.cost_policy import load_cost_policy_v1
from nexus.optimizer.models import OptimizationPolicy, build_initial_proposal_draft
from nexus.optimizer.persistence import ProposalPersistenceAuthority
from nexus.optimizer.production import (
    PersistedProposalGroup,
    _comparison_inputs,
    _minimum_change_inputs,
    create_proposal_group_idempotently,
)
from nexus.optimizer.scenario_risk import load_scenario_evidence
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.optimization import OptimizationPolicyRecord
from nexus.persistence.models.portfolio import (
    Account,
    BrokerSyncRun,
    Portfolio,
    PortfolioSnapshotRecord,
)
from nexus.research.fx import load_dexhkus_valuation


class InitialDraftUnavailable(ValueError):
    """A stable reason describing a missing local input, without account data."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


async def create_initial_draft(
    session: AsyncSession,
    *,
    portfolio_snapshot_id: UUID,
    diagnostic_report_id: UUID,
    actor_id: str,
    idempotency_key: str,
    request_hash: str,
    now: datetime,
    expected_data_snapshot_id: UUID | None = None,
    artifact_reader: ArtifactByteReader | None = None,
) -> PersistedProposalGroup:
    """Create no Job, authority, research call, order, or synthetic candidate diagnosis."""
    record = await session.scalar(
        select(PortfolioSnapshotRecord)
        .join(Portfolio, Portfolio.id == PortfolioSnapshotRecord.portfolio_id)
        .join(Account, Account.id == Portfolio.account_id)
        .join(BrokerSyncRun, BrokerSyncRun.id == PortfolioSnapshotRecord.sync_run_id)
        .where(
            PortfolioSnapshotRecord.id == portfolio_snapshot_id,
            PortfolioSnapshotRecord.status == "READY",
            PortfolioSnapshotRecord.created_at <= now,
            Account.provider == "fosun",
            Account.status == "ACTIVE",
            BrokerSyncRun.cash_flow_quality == "PASS",
        )
    )
    diagnostic = await session.get(PortfolioDiagnosticReportRecord, diagnostic_report_id)
    policy = await load_real_advisory_policy_v1(session)
    if (
        record is None
        or record.as_of is None
        or record.as_of > now
        or diagnostic is None
        or diagnostic.portfolio_snapshot_id != record.id
        or diagnostic.portfolio_id != record.portfolio_id
        or diagnostic.snapshot_as_of != record.as_of
        or diagnostic.created_at > now
        or diagnostic.policy_id != REAL_ADVISORY_POLICY_ID
        or diagnostic.policy_version != policy.version
        or diagnostic.optimization_need not in {"RESEARCH_REQUIRED", "OPTIMIZATION_CANDIDATE"}
    ):
        raise InitialDraftUnavailable("DRAFT_BASELINE_OR_DIAGNOSTIC_UNAVAILABLE")
    snapshot = await load_frozen_snapshot(
        session,
        portfolio_id=record.portfolio_id,
        snapshot_id=record.id,
    )
    identities = await load_instrument_identities(session, snapshot=snapshot)
    if (
        not snapshot.positions
        or any(
            p.currency != "USD"
            or p.quantity <= 0
            or p.current_price is None
            or p.current_price <= 0
            or (p.provider_as_of is not None and p.provider_as_of > record.as_of)
            or identities[p.instrument_id].restriction != "STANDARD"
            or identities[p.instrument_id].exchange not in US_EQUITY_EXCHANGES
            for p in snapshot.positions
        )
        or any(
            c.cash_balance < 0 or (c.provider_as_of is not None and c.provider_as_of > record.as_of)
            for c in snapshot.cash
        )
    ):
        raise InitialDraftUnavailable("DRAFT_REQUIRES_VALUED_USD_US_BASELINE")
    fx = None
    if any(c.currency != "USD" and c.cash_balance != 0 for c in snapshot.cash):
        if any(c.currency not in {"USD", "HKD"} and c.cash_balance != 0 for c in snapshot.cash):
            raise InitialDraftUnavailable("DRAFT_FX_VALUATION_UNAVAILABLE")
        fx = await load_dexhkus_valuation(session, valuation_time=record.as_of, known_at=now)
        if fx is None:
            raise InitialDraftUnavailable("DRAFT_FX_VALUATION_UNAVAILABLE")
        # Scenario valuation only: retain the immutable broker cash rows and their currency.
        snapshot = snapshot.model_copy(
            update={
                "cash": tuple(
                    cash.model_copy(
                        update={
                            "currency": "USD",
                            "cash_balance": cash.cash_balance / fx.rate_hkd_per_usd,
                            "available_cash": cash.available_cash / fx.rate_hkd_per_usd,
                        }
                    )
                    if cash.currency == "HKD"
                    else cash
                    for cash in snapshot.cash
                )
            }
        )
    report = await read_report(session, diagnostic)
    if any(ref.available_at > record.as_of for item in report.items for ref in item.evidence_refs):
        raise InitialDraftUnavailable("DRAFT_EVIDENCE_AFTER_VALUATION")
    if any(
        item.unit != "trading_days"
        for item in report.items
        if item.diagnostic_code.startswith("LIQUIDITY_DAYS_TO_EXIT:")
    ):
        raise InitialDraftUnavailable("DRAFT_LIQUIDITY_UNIT_INVALID")
    minimum = _minimum_change_inputs(snapshot, report)
    if not minimum.evidence_complete:
        raise InitialDraftUnavailable("DRAFT_REDUCTION_EVIDENCE_INCOMPLETE")
    if not minimum.candidates or minimum.excess_weight <= 0:
        raise InitialDraftUnavailable("DRAFT_NO_EVIDENCED_REDUCTION")
    group = build_initial_proposal_draft(
        snapshot,
        report,
        OptimizationPolicy.v1(),
        minimum_change_candidates=minimum.candidates,
        excess_weight=minimum.excess_weight,
        minimum_change_result=minimum.reduction,
    )
    source_policy = await load_diagnostic_source_policy(session, record.as_of)
    cost_policy = await load_cost_policy_v1(session)
    optimizer_policy = await session.scalar(
        select(OptimizationPolicyRecord).where(
            OptimizationPolicyRecord.key == "optimizer",
            OptimizationPolicyRecord.version == "optimizer_v1",
        )
    )
    if (
        optimizer_policy is None
        or optimizer_policy.created_at > now
        or cost_policy.available_at > record.as_of
    ):
        raise ValueError("initial draft policies are unavailable")
    inputs: dict[str, object] = {
        "schema_version": "optimization_draft_inputs_v1",
        "portfolio_snapshot_id": str(record.id),
        "diagnostic_report_id": str(diagnostic.id),
        "diagnostic_input_hash": diagnostic.diagnostic_input_hash,
        "source_policy_id": str(source_policy.policy.record_id),
        "cost_policy_id": str(cost_policy.id),
        "optimizer_policy_id": str(optimizer_policy.id),
    }
    if fx is not None:
        inputs["fx_valuation"] = fx.input_identity()
    scenario = await load_scenario_evidence(session, snapshot, artifact_reader)
    if scenario.refs:
        inputs["comparison_evidence"] = [ref.model_dump(mode="json") for ref in scenario.refs]
    digest = hashlib.sha256(
        json.dumps(inputs, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    object_id = f"{record.id}:{diagnostic.id}"
    # This is a newly assembled input index, not data pretending to have existed at valuation.
    await session.execute(
        insert(DataSnapshot)
        .values(
            source="nexus_optimizer_draft",
            provider_object_id=object_id,
            revision=digest,
            observed_at=record.as_of,
            available_at=now,
            received_at=now,
            schema_version="optimization_draft_inputs_v1",
            payload=inputs,
        )
        .on_conflict_do_nothing(index_elements=["source", "provider_object_id", "revision"])
    )
    data = await session.scalar(
        select(DataSnapshot).where(
            DataSnapshot.source == "nexus_optimizer_draft",
            DataSnapshot.provider_object_id == object_id,
            DataSnapshot.revision == digest,
        )
    )
    assert data is not None
    if expected_data_snapshot_id is not None and data.id != expected_data_snapshot_id:
        raise InitialDraftUnavailable("DRAFT_INPUT_BINDING_CHANGED")
    authority = ProposalPersistenceAuthority(
        diagnostic_report_id=diagnostic.id,
        data_snapshot_id=data.id,
        source_policy_id=source_policy.policy.record_id,
        optimizer_policy_id=optimizer_policy.id,
        cost_policy_id=cost_policy.id,
        diagnostic_input_hash=diagnostic.diagnostic_input_hash,
        valuation_time=record.as_of,
        baseline_ready=True,
        diagnostic_complete=False,
        source_conflict=False,
        partial=True,
        requires_fx=fx is not None,
        cost_complete=False,
    )
    comparisons = tuple(
        replace(
            comparison,
            requires_fx=fx is not None,
            partial=True,
            constraint_complete=False,
            eligibility_complete=False,
            cost=None,
            cost_complete=False,
            unknown_metrics=tuple(
                sorted(set((*comparison.unknown_metrics, "cost", "initial_draft")))
            ),
            dimensions=comparison.dimensions.model_copy(
                update={
                    "cost": TypedMetric.unknown("BROKER_COSTS_NOT_PROVEN"),
                }
            ),
        )
        for comparison in _comparison_inputs(
            group, report, minimum, snapshot, cost_policy.parameters, scenario
        )
    )
    return await create_proposal_group_idempotently(
        session,
        actor_id=actor_id,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        group=group,
        authority=authority,
        comparisons=(comparisons[0], comparisons[1], comparisons[2]),
    )
