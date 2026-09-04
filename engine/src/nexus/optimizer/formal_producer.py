"""Normal deterministic formal-binding producer; no economic or execution promotion."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID
from nexus.diagnostics.repository import _read_report, load_frozen_snapshot
from nexus.domain.ids import new_uuid7
from nexus.domain.time import require_utc
from nexus.optimizer.admission import admit_queued_job
from nexus.optimizer.cost_policy import COST_POLICY_ID, load_cost_policy_v1
from nexus.optimizer.formal_inputs import (
    FORMAL_SCHEMA,
    FORMAL_SOURCE,
    FormalOptimizationInputs,
    FormalOptimizationJobBinding,
    FormalResearchOutputReference,
    canonical_hash,
    freeze_formal_research,
    load_formal_research,
    read_formal_inputs,
)
from nexus.persistence.models.data import DataSnapshot, QualityReport
from nexus.persistence.models.diagnostics import (
    PortfolioDiagnosticEvidenceRefRecord,
    PortfolioDiagnosticItemRecord,
    PortfolioDiagnosticReportRecord,
)
from nexus.persistence.models.optimization import (
    BaselineSelectionAuthorityRecord,
    OptimizationAdmissionAuthorityRecord,
    OptimizationPolicyRecord,
)
from nexus.persistence.models.portfolio import (
    Account,
    BrokerSyncRun,
    CashSnapshot,
    Portfolio,
    PortfolioSnapshotRecord,
)
from nexus.quality.source_policy import SOURCE_POLICY_ID, load_source_policy_v1
from nexus.reconciliation.admission_claim import claim_or_lock
from nexus.reconciliation.persistence_errors import IdempotencyConflict
from nexus.strategy.baseline import StrategyDecision, evaluate_baseline_v1
from nexus.strategy.snapshot_reader import (
    FrozenStrategyReferences,
    LoadedStrategyFeatures,
    load_unranked_strategy_features,
)

if TYPE_CHECKING:
    from nexus.optimizer.authority import LoadedProposalAuthority


class FormalOptimizationRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    portfolio_snapshot_id: UUID
    diagnostic_report_id: UUID
    decision_cutoff_at: datetime
    strategy_references: FrozenStrategyReferences
    research_outputs: tuple[FormalResearchOutputReference, ...] = Field(
        min_length=1, max_length=100
    )
    cost_policy_id: UUID = COST_POLICY_ID

    @field_validator("decision_cutoff_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return require_utc(value)


@dataclass(frozen=True)
class FormalOptimizationAdmission:
    job_id: UUID
    data_snapshot_id: UUID
    admission_authority_id: UUID
    replayed: bool


async def _baseline(
    session: AsyncSession,
    request: FormalOptimizationRequest,
) -> tuple[PortfolioSnapshotRecord, PortfolioDiagnosticReportRecord]:
    cutoff = request.decision_cutoff_at
    row = (
        await session.execute(
            select(PortfolioSnapshotRecord, Portfolio, Account, BrokerSyncRun)
            .join(Portfolio, Portfolio.id == PortfolioSnapshotRecord.portfolio_id)
            .join(Account, Account.id == Portfolio.account_id)
            .join(BrokerSyncRun, BrokerSyncRun.id == PortfolioSnapshotRecord.sync_run_id)
            .where(PortfolioSnapshotRecord.id == request.portfolio_snapshot_id)
        )
    ).one_or_none()
    if row is None:
        raise ValueError("OPTIMIZATION_FORMAL_BASELINE_NOT_READY")
    record, portfolio, account, sync = row
    if (
        account.provider != "fosun"
        or account.status != "ACTIVE"
        or record.status != "READY"
        or record.as_of is None
        or record.as_of > cutoff
        or sync.status != "READY"
        or sync.cash_flow_quality != "PASS"
        or sync.account_id != account.id
        or sync.portfolio_id != portfolio.id
        or max(record.created_at, portfolio.created_at, account.created_at, sync.created_at)
        > cutoff
        or sync.completed_at is None
        or sync.completed_at > cutoff
    ):
        raise ValueError("OPTIMIZATION_FORMAL_BASELINE_NOT_READY")
    late_cash = await session.scalar(
        select(CashSnapshot.id)
        .where(
            CashSnapshot.portfolio_snapshot_id == record.id,
            CashSnapshot.created_at > cutoff,
        )
        .limit(1)
    )
    if late_cash is not None:
        raise ValueError("OPTIMIZATION_FORMAL_BASELINE_NOT_READY")
    snapshot = await load_frozen_snapshot(session, portfolio_id=portfolio.id, snapshot_id=record.id)
    if snapshot.positions:
        # Broker positions do not establish account/strategy rank continuity.
        raise ValueError("OPTIMIZATION_FORMAL_CONTINUITY_NOT_READY")
    if (
        not snapshot.cash
        or snapshot.open_orders
        or any(c.currency != "USD" or c.cash_balance < 0 for c in snapshot.cash)
        or sum((c.cash_balance for c in snapshot.cash), Decimal(0)) <= 0
        or any(
            c.provider_as_of is not None and c.provider_as_of > record.as_of for c in snapshot.cash
        )
    ):
        raise ValueError("OPTIMIZATION_FORMAL_BASELINE_NOT_READY")
    report = await session.get(PortfolioDiagnosticReportRecord, request.diagnostic_report_id)
    if (
        report is None
        or report.portfolio_snapshot_id != record.id
        or report.portfolio_id != portfolio.id
        or report.snapshot_as_of != record.as_of
        or report.created_at > cutoff
        or report.policy_id != REAL_ADVISORY_POLICY_ID
        or report.policy_version != "real_advisory_policy_v1"
        or report.optimization_need not in {"OPTIMIZATION_CANDIDATE", "RESEARCH_REQUIRED"}
    ):
        raise ValueError("OPTIMIZATION_FORMAL_DIAGNOSTIC_NOT_READY")
    late_item = await session.scalar(
        select(PortfolioDiagnosticItemRecord.id)
        .where(
            PortfolioDiagnosticItemRecord.report_id == report.id,
            PortfolioDiagnosticItemRecord.created_at > cutoff,
        )
        .limit(1)
    )
    late_evidence = await session.scalar(
        select(PortfolioDiagnosticEvidenceRefRecord.id)
        .join(
            PortfolioDiagnosticItemRecord,
            PortfolioDiagnosticItemRecord.id == PortfolioDiagnosticEvidenceRefRecord.item_id,
        )
        .where(
            PortfolioDiagnosticItemRecord.report_id == report.id,
            PortfolioDiagnosticEvidenceRefRecord.created_at > cutoff,
        )
        .limit(1)
    )
    if late_item is not None or late_evidence is not None:
        # Do not silently filter a changed historical report into apparent completeness.
        raise ValueError("OPTIMIZATION_FORMAL_DIAGNOSTIC_NOT_READY")
    domain = await _read_report(session, report)
    if any(
        any(ref.available_at > cutoff for ref in item.evidence_refs) for item in domain.items
    ) or (
        report.optimization_need == "OPTIMIZATION_CANDIDATE"
        and any(item.unknown_reason is not None or not item.evidence_refs for item in domain.items)
    ):
        raise ValueError("OPTIMIZATION_FORMAL_DIAGNOSTIC_NOT_READY")
    return record, report


async def _selection(
    session: AsyncSession,
    references: FrozenStrategyReferences,
) -> tuple[LoadedStrategyFeatures, StrategyDecision]:
    cutoff = references.market.decision_time
    if references.market.source_policy_id != SOURCE_POLICY_ID:
        raise ValueError("OPTIMIZATION_FORMAL_SOURCE_NOT_READY")
    source = await load_source_policy_v1(session, instrument_tick_size=None)
    if source.available_at > cutoff:
        raise ValueError("OPTIMIZATION_FORMAL_SOURCE_NOT_READY")
    quality = await session.get(QualityReport, references.market.quality_report_id)
    if quality is None or quality.status != "PASS" or quality.created_at > cutoff:
        raise ValueError("OPTIMIZATION_FORMAL_QUALITY_NOT_READY")
    quality_data = await session.get(DataSnapshot, quality.data_snapshot_id)
    if (
        quality_data is None
        or max(quality_data.created_at, quality_data.available_at, quality_data.received_at)
        > cutoff
    ):
        raise ValueError("OPTIMIZATION_FORMAL_QUALITY_NOT_READY")
    features = await load_unranked_strategy_features(
        session,
        references=references,
        current_positions=(),
    )
    if (
        quality.payload.get("required_instrument_ids")
        != sorted(str(item.instrument_id) for item in features.snapshot.instruments)
        or quality.payload.get("decision_session") != references.market.decision_session.isoformat()
    ):
        raise ValueError("OPTIMIZATION_FORMAL_QUALITY_NOT_READY")
    decision = evaluate_baseline_v1(
        features.snapshot.with_rank_states(()),
        references.market.decision_session,
    )
    if decision.reason != "WEEKLY_REBALANCE" or not decision.targets:
        raise ValueError("OPTIMIZATION_FORMAL_SELECTION_NOT_READY")
    return features, decision


def _selection_provenance(references: FrozenStrategyReferences) -> list[str]:
    return [
        str(value)
        for value in (
            *references.raw_bar_set_ids,
            *references.adjusted_bar_set_ids,
            *references.sector_exposure_ids,
            references.market.calendar_authority_id,
            references.market.universe_snapshot_id,
            references.market.quality_report_id,
        )
    ]


async def admit_formal_optimization(
    session: AsyncSession,
    *,
    request: FormalOptimizationRequest,
    actor_id: str,
    idempotency_key: str,
) -> FormalOptimizationAdmission:
    """Atomically materialize one exact A, selection, admission and normal native Job.

    The caller commits. A failed validation rolls back this admission's savepoint,
    even if the caller catches the fixed error and commits unrelated work.
    """
    async with session.begin_nested():
        digest = canonical_hash(request.model_dump(mode="json"))
        claim, owner = await claim_or_lock(
            session,
            actor_id,
            "optimization-formal-producer",
            idempotency_key,
            digest,
            FORMAL_SCHEMA,
        )
        if not owner:
            if claim.request_hash != digest:
                raise IdempotencyConflict("formal optimization idempotency conflict")
            return FormalOptimizationAdmission(
                UUID(str(claim.response_payload["job_id"])),
                UUID(str(claim.response_payload["data_snapshot_id"])),
                UUID(str(claim.response_payload["admission_authority_id"])),
                True,
            )
        # The existing Job key is global to the frozen input, not to the caller's
        # request key. Reuse its whole A/admission binding before materializing.
        canonical, canonical_owner = await claim_or_lock(
            session,
            "optimizer-formal-producer",
            "optimization-formal-inputs",
            digest,
            digest,
            FORMAL_SCHEMA,
        )
        if not canonical_owner:
            claim.response_payload = dict(canonical.response_payload)
            await session.flush()
            return FormalOptimizationAdmission(
                UUID(str(canonical.response_payload["job_id"])),
                UUID(str(canonical.response_payload["data_snapshot_id"])),
                UUID(str(canonical.response_payload["admission_authority_id"])),
                True,
            )
        now = await session.scalar(select(func.clock_timestamp()))
        if now is None or request.decision_cutoff_at > now:
            raise ValueError("OPTIMIZATION_FORMAL_CUTOFF_UNAVAILABLE")
        baseline, report = await _baseline(session, request)
        assert baseline.as_of is not None
        if request.strategy_references.market.decision_time != request.decision_cutoff_at:
            raise ValueError("OPTIMIZATION_FORMAL_BINDING_INVALID")
        cost = await load_cost_policy_v1(session)
        if request.cost_policy_id != cost.id or cost.available_at > request.decision_cutoff_at:
            raise ValueError("OPTIMIZATION_FORMAL_COST_POLICY_UNAVAILABLE")
        policy = await session.scalar(
            select(OptimizationPolicyRecord).where(
                OptimizationPolicyRecord.key == "optimizer",
                OptimizationPolicyRecord.version == "optimizer_v1",
            )
        )
        if policy is None or policy.created_at > request.decision_cutoff_at:
            raise ValueError("OPTIMIZATION_FORMAL_POLICY_UNAVAILABLE")
        bindings = []
        for reference in request.research_outputs:
            binding, _ = await freeze_formal_research(
                session,
                reference,
                portfolio_snapshot_id=baseline.id,
                diagnostic_report_id=report.id,
                diagnostic_input_hash=report.diagnostic_input_hash,
                valuation_time=baseline.as_of,
                decision_cutoff_at=request.decision_cutoff_at,
            )
            bindings.append(binding)
        features, decision = await _selection(session, request.strategy_references)
        selected = tuple(target.instrument_id for target in decision.targets)
        if not set(selected) <= {binding.instrument_id for binding in bindings}:
            raise ValueError("OPTIMIZATION_FORMAL_RESEARCH_COVERAGE_MISSING")
        generated = await session.scalar(select(func.clock_timestamp()))
        assert generated is not None
        materialized = await session.scalar(select(func.clock_timestamp()))
        assert materialized is not None
        inputs = FormalOptimizationInputs(
            portfolio_snapshot_id=baseline.id,
            valuation_time=baseline.as_of,
            decision_cutoff_at=request.decision_cutoff_at,
            materialized_at=materialized,
            diagnostic_report_id=report.id,
            diagnostic_input_hash=report.diagnostic_input_hash,
            research_bindings=tuple(sorted(bindings, key=lambda row: row.instrument_id.int)),
            strategy_references=request.strategy_references,
            strategy_references_hash=request.strategy_references.content_hash,
        )
        payload = inputs.model_dump(mode="json")
        data = DataSnapshot(
            id=new_uuid7(),
            source=FORMAL_SOURCE,
            schema_version=FORMAL_SCHEMA,
            provider_object_id=str(baseline.id),
            revision=canonical_hash(payload),
            observed_at=baseline.as_of,
            received_at=materialized,
            available_at=materialized,
            payload=payload,
        )
        session.add(data)
        await session.flush()
        provenance = _selection_provenance(request.strategy_references)
        selection_hash = canonical_hash(
            {
                "data_snapshot_hash": data.revision,
                "strategy_references_hash": inputs.strategy_references_hash,
                "decision": decision.model_dump(mode="json"),
            }
        )
        selection = BaselineSelectionAuthorityRecord(
            id=new_uuid7(),
            policy_version="baseline_v1",
            data_snapshot_id=data.id,
            available_at=generated,
            decision_at=generated,
            evidence_refs=provenance,
            candidates=[
                {"instrument_id": str(item.instrument_id), "provenance_refs": provenance}
                for item in features.snapshot.instruments
            ],
            selected_instrument_ids=[str(item) for item in selected],
            evidence_hash=selection_hash,
        )
        session.add(selection)
        await session.flush()
        authority = OptimizationAdmissionAuthorityRecord(
            id=new_uuid7(),
            portfolio_snapshot_id=baseline.id,
            diagnostic_report_id=report.id,
            data_snapshot_id=data.id,
            source_policy_id=SOURCE_POLICY_ID,
            optimizer_policy_id=policy.id,
            cost_policy_id=cost.id,
            diagnostic_input_hash=report.diagnostic_input_hash,
            valuation_time=baseline.as_of,
            decision_cutoff_at=request.decision_cutoff_at,
            baseline_ready=True,
            diagnostic_complete=report.optimization_need == "OPTIMIZATION_CANDIDATE",
            source_conflict=False,
            partial=report.optimization_need == "RESEARCH_REQUIRED",
            requires_fx=False,
            cost_complete=False,
            authority_stale=False,
            portfolio_scope="real_read_only",
            selection_authority_id=selection.id,
            selection_evidence_hash=selection.evidence_hash,
        )
        session.add(authority)
        await session.flush()
        job_binding = FormalOptimizationJobBinding(
            admission_authority_id=authority.id,
            data_snapshot_id=data.id,
            data_snapshot_hash=data.revision,
            decision_cutoff_at=request.decision_cutoff_at,
        )
        queued = await admit_queued_job(
            session,
            actor_id=actor_id,
            route="optimization-formal-job",
            idempotency_key=idempotency_key,
            request_hash=digest,
            portfolio_id=baseline.portfolio_id,
            effective_at=baseline.as_of,
            payload={
                "portfolio_snapshot_id": str(baseline.id),
                "data_snapshot_id": str(data.id),
                "cost_policy_id": str(cost.id),
                "formal_binding": job_binding.model_dump(mode="json"),
            },
        )
        claim.response_payload = {
            "job_id": str(queued.job_id),
            "data_snapshot_id": str(data.id),
            "admission_authority_id": str(authority.id),
        }
        canonical.response_payload = dict(claim.response_payload)
        await session.flush()
        return FormalOptimizationAdmission(queued.job_id, data.id, authority.id, False)


async def load_formal_admission_authority(
    session: AsyncSession,
    authority: OptimizationAdmissionAuthorityRecord,
    data: DataSnapshot,
) -> "LoadedProposalAuthority":
    """Revalidate exact persisted facts, never trust admission readiness booleans alone."""
    from nexus.optimizer.authority import LoadedProposalAuthority
    from nexus.optimizer.persistence import ProposalPersistenceAuthority

    inputs = read_formal_inputs(data)
    if (
        authority.decision_cutoff_at != inputs.decision_cutoff_at
        or authority.portfolio_snapshot_id != inputs.portfolio_snapshot_id
        or authority.diagnostic_report_id != inputs.diagnostic_report_id
        or authority.diagnostic_input_hash != inputs.diagnostic_input_hash
        or authority.valuation_time != inputs.valuation_time
        or authority.source_policy_id != SOURCE_POLICY_ID
        or authority.portfolio_scope != "real_read_only"
        or not authority.baseline_ready
        or authority.source_conflict
        or authority.requires_fx
        or authority.cost_complete
        or authority.authority_stale
    ):
        raise ValueError("OPTIMIZATION_FORMAL_AUTHORITY_INVALID")
    request = FormalOptimizationRequest(
        portfolio_snapshot_id=inputs.portfolio_snapshot_id,
        diagnostic_report_id=inputs.diagnostic_report_id,
        decision_cutoff_at=inputs.decision_cutoff_at,
        strategy_references=inputs.strategy_references,
        research_outputs=tuple(
            FormalResearchOutputReference(run_id=r.run_id, output_id=r.output_id)
            for r in inputs.research_bindings
        ),
        cost_policy_id=authority.cost_policy_id,
    )
    baseline, report = await _baseline(session, request)
    complete = report.optimization_need == "OPTIMIZATION_CANDIDATE"
    if authority.diagnostic_complete != complete or authority.partial == complete:
        raise ValueError("OPTIMIZATION_FORMAL_DIAGNOSTIC_NOT_READY")
    policy = await session.get(OptimizationPolicyRecord, authority.optimizer_policy_id)
    cost = await load_cost_policy_v1(session)
    if (
        policy is None
        or policy.key != "optimizer"
        or policy.version != "optimizer_v1"
        or policy.created_at > inputs.decision_cutoff_at
        or cost.id != authority.cost_policy_id
        or cost.available_at > inputs.decision_cutoff_at
    ):
        raise ValueError("OPTIMIZATION_FORMAL_POLICY_UNAVAILABLE")
    await load_formal_research(session, inputs)
    features, decision = await _selection(session, inputs.strategy_references)
    selection = await session.get(
        BaselineSelectionAuthorityRecord, authority.selection_authority_id
    )
    expected_hash = canonical_hash(
        {
            "data_snapshot_hash": data.revision,
            "strategy_references_hash": inputs.strategy_references_hash,
            "decision": decision.model_dump(mode="json"),
        }
    )
    provenance = _selection_provenance(inputs.strategy_references)
    if (
        selection is None
        or selection.data_snapshot_id != data.id
        or selection.policy_version != "baseline_v1"
        or not inputs.decision_cutoff_at
        <= selection.available_at
        <= selection.decision_at
        <= inputs.materialized_at
        or selection.evidence_hash != expected_hash
        or authority.selection_evidence_hash != expected_hash
        or selection.selected_instrument_ids != [str(t.instrument_id) for t in decision.targets]
        or {str(t.instrument_id) for t in decision.targets}
        - {str(r.instrument_id) for r in inputs.research_bindings}
        or selection.evidence_refs != provenance
        or selection.candidates
        != [
            {"instrument_id": str(item.instrument_id), "provenance_refs": provenance}
            for item in features.snapshot.instruments
        ]
    ):
        raise ValueError("OPTIMIZATION_FORMAL_SELECTION_INVALID")
    return LoadedProposalAuthority(
        admission_authority_id=authority.id,
        portfolio_id=baseline.portfolio_id,
        persistence=ProposalPersistenceAuthority(
            diagnostic_report_id=authority.diagnostic_report_id,
            data_snapshot_id=data.id,
            source_policy_id=authority.source_policy_id,
            optimizer_policy_id=authority.optimizer_policy_id,
            cost_policy_id=authority.cost_policy_id,
            diagnostic_input_hash=authority.diagnostic_input_hash,
            valuation_time=inputs.valuation_time,
            decision_cutoff_at=inputs.decision_cutoff_at,
            baseline_ready=True,
            diagnostic_complete=complete,
            source_conflict=False,
            partial=not complete,
            requires_fx=False,
            cost_complete=False,
            portfolio_scope="real_read_only",
            selection_authority_id=selection.id,
            selection_evidence_hash=selection.evidence_hash,
        ),
    )
