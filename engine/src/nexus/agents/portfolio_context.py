"""Minimal, redacted portfolio evidence stored inline in the research snapshot."""

import hashlib
import json
from datetime import datetime
from decimal import Decimal, localcontext
from typing import Literal, cast
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    ValidationError,
    model_serializer,
    model_validator,
)
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.concentration import FrozenConcentrationFacts
from nexus.agents.proposal_context import FrozenProposalComparison, read_comparison_for_freeze
from nexus.diagnostics.assembler import _snapshot_bindings
from nexus.diagnostics.models import DiagnosticEvidence, OptimizationNeed
from nexus.diagnostics.policy import (
    REAL_ADVISORY_POLICY_ID,
    REAL_ADVISORY_POLICY_VERSION,
    RealAdvisoryPolicy,
    load_real_advisory_policy_v1,
)
from nexus.diagnostics.repository import _stored_decimal, load_frozen_snapshot
from nexus.domain.decimal import DecimalString, decimal_to_string
from nexus.domain.evidence import EvidenceRef
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.diagnostics import (
    PortfolioDiagnosticEvidenceRefRecord,
    PortfolioDiagnosticItemRecord,
    PortfolioDiagnosticReportRecord,
)
from nexus.persistence.models.market import Instrument
from nexus.persistence.models.portfolio import (
    Account,
    BrokerSyncRun,
    CashSnapshot,
    Portfolio,
    PortfolioSnapshotRecord,
    PositionSnapshot,
)
from nexus.persistence.models.research import ResearchEvidenceRef

SOURCE = "nexus_portfolio_context"


class ContextDiagnostic(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    code: str = Field(max_length=200)
    severity: Literal["info", "warning", "critical"]
    actionability: Literal["monitor", "research", "reduce_only", "blocked"]
    unknown_reason: str | None = Field(max_length=200)


class ConcentrationResearchFocus(BaseModel):
    """A research question tied to the holding that actually triggered the warning."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    question: Literal["该持仓触发单一标的集中度预警：现有证据支持维持，还是需要研究减仓？"]
    diagnostic_code: Literal["CONCENTRATION_TOP1"] = "CONCENTRATION_TOP1"
    policy_scope: str
    policy_version: str
    observed_weight: DecimalString
    reference_limit: DecimalString
    weight_basis: Literal["same_currency_securities_excluding_cash"] = (
        "same_currency_securities_excluding_cash"
    )


class PortfolioResearchContext(BaseModel):
    """No account identities, cash amounts, quantities, order IDs or other holdings."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    instrument_id: UUID
    portfolio_snapshot_id: UUID
    snapshot_as_of: datetime
    research_as_of: datetime
    source: Literal["fosun"] = "fosun"
    currency: str = Field(pattern="^[A-Z]{3}$")
    position_weight: DecimalString | None
    cash_weight: DecimalString | None
    valuation_reason: Literal["INCOMPLETE_CURRENCY_VALUATION"] | None
    snapshot_decision_ready: bool
    execution_locked: Literal[True] = True
    diagnostic_run_id: UUID | None
    optimization_need: (
        Literal["BLOCKED", "RESEARCH_REQUIRED", "OPTIMIZATION_CANDIDATE", "NO_ACTION"] | None
    )
    diagnostic_input_hash: str | None = Field(pattern="^[0-9a-f]{64}$")
    diagnostics: tuple[ContextDiagnostic, ...] = Field(max_length=100)
    diagnostics_truncated: bool
    research_focus: ConcentrationResearchFocus | None = None
    concentration_facts: FrozenConcentrationFacts | None = None
    proposal_comparison: FrozenProposalComparison | None = None
    evidence_ref_id: UUID | None = None
    content_hash: str | None = Field(default=None, pattern="^[0-9a-f]{64}$")

    @model_serializer(mode="wrap")
    def _legacy_payload(self, handler: SerializerFunctionWrapHandler):  # type: ignore[no-untyped-def]
        payload = handler(self)
        if self.concentration_facts is None:
            payload.pop("concentration_facts", None)
        return payload

    @model_validator(mode="after")
    def _concentration_binding(self) -> "PortfolioResearchContext":
        if self.concentration_facts is not None and (
            self.concentration_facts.policy_scope != f"currency:{self.currency}"
            or self.diagnostic_run_id is None or self.diagnostic_input_hash is None
            or any(ref.available_at > self.snapshot_as_of
                   for ref in self.concentration_facts.policy_evidence_refs)
        ):
            raise ValueError("invalid concentration context binding")
        return self


def _bytes(context: PortfolioResearchContext) -> bytes:
    excluded = {"evidence_ref_id", "content_hash"}
    # Preserve the exact content identity of older frozen contexts.
    if context.research_focus is None:
        excluded.add("research_focus")
    if context.proposal_comparison is None:
        excluded.add("proposal_comparison")
    if context.concentration_facts is None:
        excluded.add("concentration_facts")
    return json.dumps(
        context.model_dump(mode="json", exclude=excluded),
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def _concentration_metric(
    held: PositionSnapshot,
    positions: list[PositionSnapshot],
    items: list[PortfolioDiagnosticItemRecord],
    report: PortfolioDiagnosticReportRecord | None,
) -> tuple[Decimal, PortfolioDiagnosticItemRecord] | None:
    if (
        report is None or report.policy_id != REAL_ADVISORY_POLICY_ID
        or report.policy_version != REAL_ADVISORY_POLICY_VERSION
        or not positions or len({p.instrument_id for p in positions}) != len(positions)
    ) or any(
        p.currency != held.currency or p.current_price is None
        or not p.current_price.is_finite() or p.current_price <= 0
        or not p.quantity.is_finite() or p.quantity <= 0 for p in positions
    ):
        return None
    candidates = [item for item in items if (
        item.diagnostic_code == "CONCENTRATION_TOP1"
        and item.policy_scope == f"currency:{held.currency}"
    )]
    if len(candidates) != 1:
        return None
    with localcontext() as context:
        # Match the diagnostic Decimal calculation, before NUMERIC(38, 12) storage.
        context.prec = 28
        values = {p.instrument_id: p.quantity * cast(Decimal, p.current_price) for p in positions}
        held_value = values.get(held.instrument_id)
        if held_value is None or held_value != max(values.values()):
            return None
        weight = held_value / sum(values.values(), Decimal(0))
        policy = RealAdvisoryPolicy.v1()
        if weight > policy.single_name_critical:
            severity, limit = "critical", policy.single_name_critical
        elif weight > policy.single_name_warning:
            severity, limit = "warning", policy.single_name_warning
        else:
            severity, limit = "info", policy.single_name_warning
        matches = [item for item in candidates if (
            item.unknown_reason is None
            and item.unit == "ratio" and item.severity == severity
            and item.metric_value is not None and item.reference_limit is not None
            and item.metric_value.is_finite() and item.reference_limit == limit
            # Database ratios use NUMERIC(38, 12).
            and item.metric_value == _stored_decimal(str(weight))
        )]
    if len(matches) != 1:
        return None
    trigger = matches[0]
    return weight, trigger


def _concentration_focus(
    held: PositionSnapshot,
    positions: list[PositionSnapshot],
    items: list[PortfolioDiagnosticItemRecord],
    report: PortfolioDiagnosticReportRecord | None,
) -> ConcentrationResearchFocus | None:
    metric = _concentration_metric(held, positions, items, report)
    if metric is None or metric[1].severity == "info" or report is None:
        return None
    trigger = metric[1]
    assert trigger.metric_value is not None and trigger.reference_limit is not None
    return ConcentrationResearchFocus(
        question="该持仓触发单一标的集中度预警：现有证据支持维持，还是需要研究减仓？",
        policy_scope=trigger.policy_scope,
        policy_version=report.policy_version,
        observed_weight=decimal_to_string(trigger.metric_value),
        reference_limit=decimal_to_string(trigger.reference_limit),
    )


async def _verified_concentration_facts(
    session: AsyncSession,
    snapshot: PortfolioSnapshotRecord,
    sync: BrokerSyncRun,
    held: PositionSnapshot,
    positions: list[PositionSnapshot],
    items: list[PortfolioDiagnosticItemRecord],
    report: PortfolioDiagnosticReportRecord | None,
    as_of: datetime,
) -> FrozenConcentrationFacts | None:
    """Verify only a new question; historical contexts retain their original bytes."""
    metric = _concentration_metric(held, positions, items, report)
    if metric is None or report is None or (
        snapshot.status != "READY" or sync.status != "READY" or sync.cash_flow_quality != "PASS"
        or sync.portfolio_id != snapshot.portfolio_id or sync.id != snapshot.sync_run_id
        or report.portfolio_id != snapshot.portfolio_id
        or report.portfolio_snapshot_id != snapshot.id or report.snapshot_as_of != snapshot.as_of
        or report.created_at > as_of or report.optimization_need == "BLOCKED"
        or len(items) > 100
        or any(p.portfolio_snapshot_id != snapshot.id or p.created_at > as_of
               or (p.provider_as_of is not None and p.provider_as_of > snapshot.as_of)
               for p in positions)
    ):
        return None
    try:
        policy = await load_real_advisory_policy_v1(session)
        if not policy.authority_refs or any(
            ref.available_at > snapshot.as_of for ref in policy.authority_refs
        ):
            return None
        frozen = await load_frozen_snapshot(
            session, portfolio_id=snapshot.portfolio_id, snapshot_id=snapshot.id,
        )
        if {p.instrument_id for p in frozen.positions if p.currency == held.currency} != {
            p.instrument_id for p in positions
        }:
            return None
        bindings, _ = await _snapshot_bindings(session, frozen)
        evidence = DiagnosticEvidence.model_validate(bindings)
        assert evidence.snapshot is not None
        expected = set(evidence.snapshot.refs)
        for position in positions:
            expected.update(evidence.position_prices[position.instrument_id].refs)
        trigger = metric[1]
        refs = list(await session.scalars(
            select(PortfolioDiagnosticEvidenceRefRecord).where(
                PortfolioDiagnosticEvidenceRefRecord.item_id == trigger.id,
            )
        ))
        if any(ref.created_at > as_of or ref.available_at > snapshot.as_of for ref in refs):
            return None
        # TOP1 also carries instrument identity refs. Check the complete original
        # valuation denominator, not unrelated market/issuer evidence or the full D hash.
        actual = {
            EvidenceRef.model_validate(ref, from_attributes=True) for ref in refs
            if ref.object_id.startswith(("portfolio-snapshot:", "position-snapshot:"))
        }
        if actual != expected:
            return None
    except (ValueError, KeyError):
        # Missing/invalid optional authority must not prevent generic research.
        return None
    return FrozenConcentrationFacts(
        observed_ratio=decimal_to_string(metric[0]),
        policy_id=report.policy_id,
        policy_evidence_refs=policy.authority_refs,
        policy_scope=trigger.policy_scope,
    )


async def _verified_concentration_focus(
    session: AsyncSession,
    snapshot: PortfolioSnapshotRecord,
    sync: BrokerSyncRun,
    held: PositionSnapshot,
    positions: list[PositionSnapshot],
    items: list[PortfolioDiagnosticItemRecord],
    report: PortfolioDiagnosticReportRecord | None,
    as_of: datetime,
    *,
    facts: FrozenConcentrationFacts | None,
) -> ConcentrationResearchFocus | None:
    """Retain the original question only when the newly verified facts support it."""
    return _concentration_focus(held, positions, items, report) if facts is not None else None


async def freeze_portfolio_context(
    session: AsyncSession,
    instrument_id: UUID,
    as_of: datetime,
    *,
    portfolio_snapshot_id: UUID | None = None,
) -> tuple[PortfolioResearchContext, ResearchEvidenceRef] | None:
    """Use the explicit snapshot, or the sole account's latest visible one.

    Neither path falls back to a different READY snapshot when facts are missing.
    """
    candidates = list(
        await session.execute(
            select(PortfolioSnapshotRecord, BrokerSyncRun)
            .join(BrokerSyncRun, BrokerSyncRun.id == PortfolioSnapshotRecord.sync_run_id)
            .join(Portfolio, Portfolio.id == PortfolioSnapshotRecord.portfolio_id)
            .join(Account, Account.id == Portfolio.account_id)
            .where(
                Account.provider == "fosun",
                Account.status == "ACTIVE",
                PortfolioSnapshotRecord.created_at <= as_of,
                PortfolioSnapshotRecord.as_of <= as_of,
                BrokerSyncRun.created_at <= as_of,
                *(() if portfolio_snapshot_id is None else (
                    PortfolioSnapshotRecord.id == portfolio_snapshot_id,
                )),
            )
            .distinct(PortfolioSnapshotRecord.portfolio_id)
            .order_by(
                PortfolioSnapshotRecord.portfolio_id,
                PortfolioSnapshotRecord.created_at.desc(),
                PortfolioSnapshotRecord.id.desc(),
            )
            .limit(2)
        )
    )
    if len(candidates) != 1:
        return None
    snapshot, sync = candidates[0]
    positions = list(
        await session.scalars(
            select(PositionSnapshot).where(
                PositionSnapshot.portfolio_snapshot_id == snapshot.id,
            )
        )
    )
    held = next((p for p in positions if p.instrument_id == instrument_id), None)
    if held is not None and held.created_at > as_of:
        return None
    if held is None:
        # Only an explicitly selected, reconciled, actually empty portfolio can
        # establish absence. Do not attach arbitrary accounts to watchlist research.
        if (
            portfolio_snapshot_id is None or positions or snapshot.status != "READY"
            or sync.status != "READY" or sync.cash_flow_quality != "PASS"
        ):
            return None
        instrument = await session.get(Instrument, instrument_id)
        if instrument is None or instrument.created_at > as_of:
            return None
        currency = instrument.currency
    else:
        currency = held.currency
    currency_positions = [p for p in positions if p.currency == currency]
    cash = await session.scalar(
        select(CashSnapshot).where(
            CashSnapshot.portfolio_snapshot_id == snapshot.id,
            CashSnapshot.currency == currency,
        )
    )
    if held is None and (cash is None or cash.created_at > as_of):
        return None
    valid = (
        cash is not None
        and cash.created_at <= as_of
        and cash.cash_balance.is_finite()
        and cash.cash_balance >= 0
        and (held is not None or cash.cash_balance > 0)
        and (cash.provider_as_of is None or cash.provider_as_of <= snapshot.as_of)
        and all(
            p.created_at <= as_of
            and p.current_price is not None
            and p.current_price.is_finite()
            and p.current_price > 0
            and p.quantity.is_finite()
            and p.quantity > 0
            and (p.provider_as_of is None or p.provider_as_of <= snapshot.as_of)
            for p in currency_positions
        )
    )
    position_weight = cash_weight = None
    if valid:
        assert cash is not None
        with localcontext() as decimal_context:
            decimal_context.prec = 28
            total = cash.cash_balance + sum(
                (p.quantity * cast(Decimal, p.current_price) for p in currency_positions),
                Decimal(0),
            )
            if held is None:
                position_weight = "0"
            else:
                assert held.current_price is not None
                position_weight = decimal_to_string(held.quantity * held.current_price / total)
            cash_weight = decimal_to_string(cash.cash_balance / total)
    ready = snapshot.status == "READY" and sync.cash_flow_quality == "PASS"
    report = (
        await session.scalar(
            select(PortfolioDiagnosticReportRecord)
            .where(
                PortfolioDiagnosticReportRecord.portfolio_snapshot_id == snapshot.id,
                PortfolioDiagnosticReportRecord.snapshot_as_of == snapshot.as_of,
                PortfolioDiagnosticReportRecord.created_at <= as_of,
            )
            .order_by(
                PortfolioDiagnosticReportRecord.created_at.desc(),
                PortfolioDiagnosticReportRecord.id.desc(),
            )
            .limit(1)
        )
        if ready
        else None
    )
    items = (
        list(
            await session.scalars(
                select(PortfolioDiagnosticItemRecord)
                .where(
                    PortfolioDiagnosticItemRecord.report_id == report.id,
                    PortfolioDiagnosticItemRecord.created_at <= as_of,
                )
                .order_by(
                    PortfolioDiagnosticItemRecord.diagnostic_code, PortfolioDiagnosticItemRecord.id
                )
                .limit(101)
            )
        )
        if report
        else []
    )
    facts = (
        await _verified_concentration_facts(
            session, snapshot, sync, held, currency_positions, items, report, as_of,
        ) if ready and valid and held is not None else None
    )
    context = PortfolioResearchContext(
        instrument_id=instrument_id,
        portfolio_snapshot_id=snapshot.id,
        snapshot_as_of=snapshot.as_of,
        research_as_of=as_of,
        currency=currency,
        position_weight=position_weight,
        cash_weight=cash_weight,
        valuation_reason=None if valid else "INCOMPLETE_CURRENCY_VALUATION",
        snapshot_decision_ready=ready,
        diagnostic_run_id=report.id if report else None,
        optimization_need=cast(OptimizationNeed, report.optimization_need) if report else None,
        diagnostic_input_hash=report.diagnostic_input_hash if report else None,
        diagnostics=tuple(
            ContextDiagnostic(
                code=item.diagnostic_code,
                severity=cast(Literal["info", "warning", "critical"], item.severity),
                actionability=cast(
                    Literal["monitor", "research", "reduce_only", "blocked"], item.actionability
                ),
                unknown_reason=item.unknown_reason,
            )
            for item in items[:100]
        ),
        diagnostics_truncated=len(items) > 100,
        concentration_facts=facts,
        research_focus=(
            await _verified_concentration_focus(
                session, snapshot, sync, held, currency_positions, items, report, as_of,
                facts=facts,
            )
            if ready and valid and held is not None else None
        ),
        proposal_comparison=(
            await read_comparison_for_freeze(
                session, portfolio_snapshot_id=snapshot.id,
                diagnostic_report_id=report.id,
                diagnostic_input_hash=report.diagnostic_input_hash,
                valuation_time=snapshot.as_of, as_of=as_of,
            ) if ready and report is not None else None
        ),
    )
    content = _bytes(context)
    digest = hashlib.sha256(content).hexdigest()
    object_id = f"{snapshot.id}:{instrument_id}"
    # The canonical bytes are retained inline in DataSnapshot.payload, not in a new service.
    await session.execute(
        insert(ResearchEvidenceRef)
        .values(
            source=SOURCE,
            provider_object_id=object_id,
            revision=digest,
            available_at=as_of,
            content_hash=digest,
            byte_length=len(content),
            location=f"memory://sha256/{digest}",
        )
        .on_conflict_do_nothing(index_elements=["source", "provider_object_id", "revision"])
    )
    evidence = await session.scalar(
        select(ResearchEvidenceRef).where(
            ResearchEvidenceRef.source == SOURCE,
            ResearchEvidenceRef.provider_object_id == object_id,
            ResearchEvidenceRef.revision == digest,
        )
    )
    assert evidence is not None
    return context.model_copy(
        update={"evidence_ref_id": evidence.id, "content_hash": digest}
    ), evidence


async def read_portfolio_context(
    session: AsyncSession,
    snapshot_id: UUID,
    instrument_id: UUID,
    as_of: datetime,
) -> PortfolioResearchContext | None:
    """Only the exact inline content bound into this snapshot is readable, not current holdings."""
    snapshot = await session.get(DataSnapshot, snapshot_id)
    if (
        snapshot is None
        or snapshot.source != "nexus_research"
        or "agent_paper_context" in snapshot.payload
        or snapshot.provider_object_id != str(instrument_id)
        or snapshot.available_at > as_of
        or snapshot.received_at > as_of
    ):
        return None
    if (
        hashlib.sha256(json.dumps(snapshot.payload, sort_keys=True).encode()).hexdigest()
        != snapshot.revision
    ):
        return None
    try:
        context = PortfolioResearchContext.model_validate(snapshot.payload.get("portfolio_context"))
        if (
            context.instrument_id != instrument_id
            or context.research_as_of > as_of
            or context.snapshot_as_of > context.research_as_of
        ):
            return None
    except (ValidationError, TypeError):
        return None
    content = _bytes(context)
    digest = hashlib.sha256(content).hexdigest()
    if context.content_hash != digest:
        return None
    evidence = await session.scalar(
        select(ResearchEvidenceRef).where(
            ResearchEvidenceRef.id == context.evidence_ref_id,
            ResearchEvidenceRef.source == SOURCE,
            ResearchEvidenceRef.content_hash == digest,
            ResearchEvidenceRef.revision == digest,
            ResearchEvidenceRef.provider_object_id
            == f"{context.portfolio_snapshot_id}:{instrument_id}",
            ResearchEvidenceRef.available_at == context.research_as_of,
            ResearchEvidenceRef.location == f"memory://sha256/{digest}",
            ResearchEvidenceRef.byte_length == len(content),
        )
    )
    return context if evidence else None
