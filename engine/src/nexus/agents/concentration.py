"""Two fixed concentration observations; never direction or execution authority."""

from decimal import Decimal
from typing import TYPE_CHECKING, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID, REAL_ADVISORY_POLICY_VERSION
from nexus.domain.decimal import DecimalString
from nexus.domain.evidence import EvidenceRef
from nexus.strategy.intents import ResearchCondition, ResearchConditionEvaluation, ResearchIntent

if TYPE_CHECKING:
    from nexus.agents.portfolio_context import PortfolioResearchContext


class FrozenConcentrationFacts(BaseModel):
    """The original Decimal(precision=28) ratio, before NUMERIC(38,12) rounding.

    Identity, P/D hashes, currency and clocks are bound by the enclosing immutable
    portfolio context; this record contains no peer identities or position amounts.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")
    schema_version: Literal["concentration_facts_v1"] = "concentration_facts_v1"
    observed_ratio: DecimalString
    policy_id: UUID
    policy_version: Literal["real_advisory_policy_v1"] = "real_advisory_policy_v1"
    policy_evidence_refs: tuple[EvidenceRef, ...] = Field(min_length=1, max_length=1)
    policy_scope: str = Field(pattern=r"^currency:[A-Z]{3}$")
    weight_basis: Literal["same_currency_securities_excluding_cash"] = (
        "same_currency_securities_excluding_cash"
    )
    warning_limit: Literal["0.15"] = "0.15"
    critical_limit: Literal["0.30"] = "0.30"

    @model_validator(mode="after")
    def _canonical(self) -> "FrozenConcentrationFacts":
        ref = self.policy_evidence_refs[0]
        if (
            not Decimal(0) < Decimal(self.observed_ratio) <= Decimal(1)
            or self.policy_id != REAL_ADVISORY_POLICY_ID
            or ref.source != "nexus_db"
            or ref.object_id
            != f"diagnostic-policy:{REAL_ADVISORY_POLICY_ID}:{REAL_ADVISORY_POLICY_VERSION}"
            or ref.location != f"db://diagnostic-policies/{REAL_ADVISORY_POLICY_ID}"
        ):
            raise ValueError("invalid frozen concentration authority")
        return self


def evaluate_frozen_concentration_conditions(
    context: "PortfolioResearchContext | None",
) -> tuple[ResearchConditionEvaluation, ...] | None:
    """Evaluate only the two fixed signals in an already verified exact context."""
    if context is None or context.evidence_ref_id is None or context.content_hash is None:
        return None
    ref = EvidenceRef(
        source="nexus_portfolio_context",
        object_id=f"{context.portfolio_snapshot_id}:{context.instrument_id}",
        available_at=context.research_as_of,
        location=f"memory://sha256/{context.content_hash}",
        content_hash=context.content_hash,
    )
    facts = context.concentration_facts
    return tuple(
        ResearchConditionEvaluation(
            condition=ResearchCondition(
                signal=signal,
                operator="is_true",
                threshold=True,
                unit="boolean",
                evidence_refs=(ref,),
            ),
            result=(
                "unknown"
                if facts is None
                else "true"
                if Decimal(facts.observed_ratio) > Decimal(limit)
                else "false"
            ),
            reason="frozen_concentration_unavailable" if facts is None else None,
            observed_at=context.snapshot_as_of,
        )
        for signal, limit in (
            ("portfolio.top1_warning_exceeded", "0.15"),
            ("portfolio.top1_critical_exceeded", "0.30"),
        )
    )


def condition_evaluations_match(
    intent: ResearchIntent,
    context: "PortfolioResearchContext | None",
) -> bool:
    """Legacy absent fields remain readable; new frozen facts cannot lose their result."""
    if intent.condition_evaluations is None and (
        context is None or context.concentration_facts is None
    ):
        return True
    return intent.condition_evaluations == evaluate_frozen_concentration_conditions(context)
