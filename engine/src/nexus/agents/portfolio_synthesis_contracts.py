"""Frozen evidence-bound portfolio choices; allocation remains deterministic."""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Final, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StrictInt, field_validator, model_validator

from nexus.agents.portfolio_decision_contracts import canonical_portfolio_decision_hash
from nexus.audit.canonical import Hash64
from nexus.domain.time import require_utc
from nexus.risk.canonical import canonical_paper_projection_hash
from nexus.risk.models import PaperPortfolioProjection
from nexus.strategy.intents import ResearchIntent

SYNTHESIS_PROMPT_VERSION: Final = "portfolio-management-synthesis-v1"
SYNTHESIS_TOOL_HASH = canonical_portfolio_decision_hash(
    {"schema_version": "portfolio_synthesis_tools_v1", "tools": []}
)
_UNSAFE = re.compile(
    r"(?:\b(?:weights?|percent(?:age)?|amounts?|quantit(?:y|ies)|orders?|prices?|"
    r"shares?|risk[_ -]?override)\b|\d\s*[%％]|\$\s*\d|"
    r"(?:目标权重|仓位比例|金额|股数|下单|限价|市价|绕过风控|修改风控))",
    re.IGNORECASE,
)


def _reject_float(value: object) -> None:
    if isinstance(value, float):
        raise ValueError("portfolio synthesis rejects binary floats")
    if isinstance(value, Mapping):
        for item in value.values():
            _reject_float(item)
    elif isinstance(value, tuple | list):
        for item in value:
            _reject_float(item)


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    @model_validator(mode="before")
    @classmethod
    def _no_float(cls, value: object) -> object:
        _reject_float(value)
        return value


class PortfolioEvidenceReferenceV1(_Frozen):
    research_output_id: UUID
    evidence_ref_id: UUID


class PortfolioEvidenceStatementV1(_Frozen):
    text: str = Field(min_length=1, max_length=2000)
    references: tuple[PortfolioEvidenceReferenceV1, ...] = Field(min_length=1, max_length=100)

    @field_validator("text")
    @classmethod
    def _safe_text(cls, value: str) -> str:
        if not value.strip() or _UNSAFE.search(value):
            raise ValueError("portfolio choices cannot contain sizing or trading instructions")
        return value.strip()

    @model_validator(mode="after")
    def _unique_refs(self) -> PortfolioEvidenceStatementV1:
        if len(set(self.references)) != len(self.references):
            raise ValueError("duplicate evidence reference")
        return self


class PortfolioChoiceV1(_Frozen):
    instrument_id: UUID
    action: Literal["include", "increase", "reduce", "exit", "maintain", "defer"]
    role: Literal["core", "tactical", "none"]
    rank: Annotated[StrictInt, Field(ge=1, le=5)] | None
    replaces_instrument_id: UUID | None
    depends_on_instrument_ids: tuple[UUID, ...] = Field(max_length=4)
    thesis: PortfolioEvidenceStatementV1
    counter_thesis: PortfolioEvidenceStatementV1
    invalidation: PortfolioEvidenceStatementV1

    @model_validator(mode="after")
    def _choice_semantics(self) -> PortfolioChoiceV1:
        increase = self.action in {"include", "increase"}
        if increase != (self.rank is not None) or (increase and self.role == "none"):
            raise ValueError("only increases have a positive rank and require a portfolio role")
        if self.replaces_instrument_id is not None and (
            not increase or self.replaces_instrument_id == self.instrument_id
        ):
            raise ValueError("only an increase may replace a different instrument")
        if self.instrument_id in self.depends_on_instrument_ids or len(
            set(self.depends_on_instrument_ids)
        ) != len(self.depends_on_instrument_ids):
            raise ValueError("dependencies must be unique other instruments")
        return self


class PortfolioSynthesisIntentV1(_Frozen):
    schema_version: Literal["portfolio_synthesis_intent_v1"]
    input_hash: Hash64
    compared_instrument_ids: tuple[UUID, ...] = Field(min_length=1, max_length=5)
    choices: tuple[PortfolioChoiceV1, ...] = Field(min_length=1, max_length=5)
    cash_rationale: PortfolioEvidenceStatementV1

    @model_validator(mode="after")
    def _complete_comparison(self) -> PortfolioSynthesisIntentV1:
        ids = self.compared_instrument_ids
        choices = {choice.instrument_id: choice for choice in self.choices}
        if (
            len(set(ids)) != len(ids)
            or len(choices) != len(self.choices)
            or set(choices) != set(ids)
        ):
            raise ValueError("each compared instrument must have exactly one choice")
        ranks = sorted(choice.rank for choice in self.choices if choice.rank is not None)
        if ranks != list(range(1, len(ranks) + 1)):
            raise ValueError("increase priorities must be unique and contiguous")
        for choice in self.choices:
            if not set(choice.depends_on_instrument_ids) <= set(ids):
                raise ValueError("unbound dependency")
            replaced = choice.replaces_instrument_id
            if replaced is not None and (
                replaced not in choices or choices[replaced].action not in {"reduce", "exit"}
            ):
                raise ValueError("replacement requires an explicit reduction or exit")
        return self


class PortfolioSecurityOutputV1(_Frozen):
    instrument_id: UUID
    research_output_id: UUID
    content_hash: Hash64
    intent_hash: Hash64
    evidence_ref_ids: tuple[UUID, ...] = Field(min_length=1, max_length=100)
    intent: ResearchIntent

    @model_validator(mode="after")
    def _bound_security(self) -> PortfolioSecurityOutputV1:
        if (
            self.intent.instrument_id != self.instrument_id
            or len(set(self.evidence_ref_ids)) != len(self.evidence_ref_ids)
            or len(self.evidence_ref_ids) != len(self.intent.evidence_refs)
        ):
            raise ValueError("security evidence binding is incomplete")
        return self


class PortfolioTargetWeightV1(_Frozen):
    instrument_id: UUID
    target_weight: Decimal = Field(ge=0, le=1)


class PortfolioSynthesisInputV1(_Frozen):
    schema_version: Literal["portfolio_synthesis_input_v1"]
    cycle_id: UUID
    account_id: UUID
    portfolio_id: UUID
    candidate_batch_id: UUID
    candidate_batch_hash: Hash64
    candidate_instrument_ids: tuple[UUID, ...] = Field(min_length=1, max_length=5)
    security_outputs: tuple[PortfolioSecurityOutputV1, ...] = Field(min_length=1, max_length=5)
    active_target_version_id: UUID | None
    active_target_content_hash: Hash64 | None
    active_target_instrument_ids: tuple[UUID, ...]
    active_target_weights: tuple[PortfolioTargetWeightV1, ...]
    active_target_cash_weight: Decimal | None = Field(ge=0, le=1)
    projection: PaperPortfolioProjection
    projection_hash: Hash64
    policy_version: Literal["portfolio_allocation_v1"]
    policy_hash: Hash64
    prompt_version_id: UUID
    prompt_version: Literal["portfolio-management-synthesis-v1"]
    prompt_hash: Hash64
    model_config_id: UUID
    model_hash: Hash64
    pricing_version: str = Field(min_length=1, max_length=100)
    tool_hash: Hash64
    intent_schema_hash: Hash64
    calendar_authority_id: UUID
    decision_time: datetime
    admitted_at: datetime
    expires_at: datetime

    @field_validator("projection", mode="before")
    @classmethod
    def _read_projection(cls, value: object) -> object:
        if not isinstance(value, dict):
            return value
        parsed = dict(value)
        for name in ("available_cash", "reserved_cash", "nav", "peak_nav"):
            if isinstance(parsed.get(name), str):
                parsed[name] = Decimal(parsed[name])
        for collection, fields in (
            ("positions", ("quantity", "reserved_quantity", "cost_basis", "reference_close")),
            ("open_orders", ("quantity", "worst_price", "reserved_cash", "reserved_quantity")),
        ):
            rows = parsed.get(collection)
            if isinstance(rows, list | tuple):
                parsed[collection] = [
                    {
                        name: Decimal(item) if name in fields and isinstance(item, str) else item
                        for name, item in row.items()
                    }
                    if isinstance(row, dict)
                    else row
                    for row in rows
                ]
        return parsed

    @field_validator("decision_time", "admitted_at", "expires_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="after")
    def _authority(self) -> PortfolioSynthesisInputV1:
        ids = self.candidate_instrument_ids
        if (
            tuple(sorted(set(ids), key=lambda item: item.int)) != ids
            or tuple(item.instrument_id for item in self.security_outputs) != ids
            or len({item.research_output_id for item in self.security_outputs}) != len(ids)
            or (self.active_target_version_id is None) != (self.active_target_content_hash is None)
            or (self.active_target_version_id is None and self.active_target_instrument_ids)
            or (self.active_target_version_id is None) != (self.active_target_cash_weight is None)
            or tuple(row.instrument_id for row in self.active_target_weights)
            != self.active_target_instrument_ids
            or (
                self.active_target_cash_weight is not None
                and sum((row.target_weight for row in self.active_target_weights), Decimal("0"))
                + self.active_target_cash_weight
                != Decimal("1")
            )
            or self.projection.account_id != self.account_id
            or self.projection.portfolio_id != self.portfolio_id
            or self.projection.projection_hash != self.projection_hash
            or canonical_paper_projection_hash(self.projection) != self.projection_hash
            or self.projection.as_of != self.decision_time
            or not self.decision_time <= self.admitted_at < self.expires_at
            or any(
                item.intent.decision_time != self.decision_time for item in self.security_outputs
            )
        ):
            raise ValueError("portfolio synthesis authority is inconsistent")
        return self


def canonical_portfolio_synthesis_hash(value: BaseModel | Mapping[str, object]) -> str:
    return canonical_portfolio_decision_hash(value)


def validate_portfolio_synthesis_intent(
    intent: PortfolioSynthesisIntentV1, frozen: PortfolioSynthesisInputV1
) -> None:
    """Bind every output reference and requested action to admitted security research."""
    if intent.input_hash != canonical_portfolio_synthesis_hash(frozen) or set(
        intent.compared_instrument_ids
    ) != set(frozen.candidate_instrument_ids):
        raise ValueError("portfolio synthesis input binding mismatch")
    allowed = {
        (item.research_output_id, evidence_id)
        for item in frozen.security_outputs
        for evidence_id in item.evidence_ref_ids
    }
    by_instrument = {item.instrument_id: item for item in frozen.security_outputs}
    held_targets = {
        item.instrument_id for item in frozen.active_target_weights if item.target_weight > 0
    }
    statements = [intent.cash_rationale]
    for choice in intent.choices:
        source = by_instrument[choice.instrument_id]
        existing = choice.instrument_id in held_targets
        if (
            choice.action in {"include", "increase"}
            and source.intent.stance != "buy_candidate"
            or choice.action in {"reduce", "exit"}
            and (not existing or source.intent.stance not in {"reduce_candidate", "hold"})
            or choice.action == "maintain"
            and not existing
            or choice.action == "include"
            and existing
            or choice.action == "increase"
            and not existing
            or (source.intent.quality_status != "complete" or source.intent.stance == "watch")
            and choice.action != "defer"
        ):
            raise ValueError("portfolio choice exceeds admitted security research")
        for statement in (choice.thesis, choice.counter_thesis, choice.invalidation):
            if not any(
                ref.research_output_id == source.research_output_id for ref in statement.references
            ):
                raise ValueError("choice lacks its own security evidence")
            statements.append(statement)
    if any(
        (ref.research_output_id, ref.evidence_ref_id) not in allowed
        for statement in statements
        for ref in statement.references
    ):
        raise ValueError("portfolio synthesis evidence reference is unbound")
