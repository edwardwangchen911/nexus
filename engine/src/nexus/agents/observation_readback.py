"""Read-only v10/v11 observation exception; never an optimization admission rule."""

from collections.abc import Mapping, Sequence
from datetime import datetime
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.actionable_research import (
    _all_nested_evidence_bound,
    _evidence_identity,
    safe_no_trade_intent_shape_valid,
)
from nexus.agents.actionable_research import (
    _hash as canonical_hash,
)
from nexus.agents.codex_versions import (
    BOUNDED_STAGE_BUDGET_CONFIG_VERSION,
    CANDIDATE_BUDGET_CONFIG_VERSION,
    FLEXIBLE_STAGE_BUDGET_CONFIG_VERSION,
    FORMAL_STAGE_CAPACITY_CONFIG_VERSION,
    FULL_INPUT_CAPACITY_CONFIG_VERSION,
    SDK_CONFIG_VERSIONS,
)
from nexus.agents.committee import AgentRole
from nexus.agents.concentration import condition_evaluations_match
from nexus.agents.execution_authority import load_frozen_evidence, load_frozen_prompts
from nexus.agents.portfolio_context import read_portfolio_context
from nexus.agents.research_only import require_observation_snapshot
from nexus.agents.synthesis import domain_evidence, staged_intent_semantics_valid
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    AgentResearchOutputEvidenceRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.strategy.intents import ResearchIntent

_OBSERVATION_VERSIONS = {FLEXIBLE_STAGE_BUDGET_CONFIG_VERSION, BOUNDED_STAGE_BUDGET_CONFIG_VERSION}
_KNOWN_VERSIONS = (
    SDK_CONFIG_VERSIONS
    | _OBSERVATION_VERSIONS
    | {
        CANDIDATE_BUDGET_CONFIG_VERSION,
        FORMAL_STAGE_CAPACITY_CONFIG_VERSION,
        FULL_INPUT_CAPACITY_CONFIG_VERSION,
    }
)


def _sufficient_research_observation(intent: ResearchIntent) -> bool:
    """Display shape only; the bound pure-research snapshot must also be verified."""
    assessment = intent.synthesis_assessment
    votes = {vote.role: vote.vote for vote in intent.role_votes}
    if (
        assessment is None
        or intent.stance != "watch"
        or assessment.stance != "watch"
        or intent.exposure_action != "none"
        or intent.quality_status != "partial"
        or intent.evidence_audit != "complete"
        or assessment.review_conclusion != "sufficient"
        or len(votes) != 8
        or votes.get("evidence_audit") != "support"
        or not intent.theses
        or not intent.counter_theses
    ):
        return False
    reviews_by_role: dict[str, set[int]] = {"bear": set(), "evidence_audit": set()}
    for review in assessment.reviews:
        if not review.resolved:
            return False
        role, raw_index = review.claim_id.split(":", 1)
        reviews_by_role[role].add(int(raw_index))
    return all(
        bool(indexes) and indexes == set(range(max(indexes) + 1))
        for indexes in reviews_by_role.values()
    )


def readable_semantics(intent: ResearchIntent, version: str, *, observation_verified: bool) -> bool:
    if version.startswith("codex-sdk-") and version not in _KNOWN_VERSIONS:
        return False
    return staged_intent_semantics_valid(intent, version) or (
        version in _OBSERVATION_VERSIONS
        and observation_verified
        and (safe_no_trade_intent_shape_valid(intent) or _sufficient_research_observation(intent))
    )


class ObservationReadback:
    """Request-local cache; callers can batch the exact output bindings they already read."""

    def __init__(self, session: AsyncSession, *, read_at: datetime) -> None:
        self.session = session
        self.read_at = read_at
        self._terminals: dict[UUID, tuple[UUID, ...]] = {}
        self._models: dict[UUID, AgentModelConfigRecord | None] = {}
        self._prompts: dict[
            tuple[str, UUID], Mapping[AgentRole, AgentPromptVersionRecord] | None
        ] = {}
        self._evidence: dict[tuple[str, UUID, datetime], tuple[ResearchEvidenceRef, ...]] = {}

    async def prime(self, runs: Sequence[AgentResearchRunRecord]) -> None:
        """Batch metadata for this read request; no new execution authority is computed."""
        selected = [run for run in runs if run.pricing_version in _OBSERVATION_VERSIONS]
        if not selected:
            return
        run_ids = [run.id for run in selected]
        terminals: dict[UUID, list[UUID]] = {run_id: [] for run_id in run_ids}
        for run_id, output_id in await self.session.execute(
            select(AgentResearchOutputRecord.run_id, AgentResearchOutputRecord.id).where(
                AgentResearchOutputRecord.run_id.in_(run_ids),
                AgentResearchOutputRecord.role.is_(None),
                AgentResearchOutputRecord.status.in_(("completed", "failed", "abstained")),
            )
        ):
            terminals[run_id].append(output_id)
        self._terminals.update({key: tuple(value) for key, value in terminals.items()})
        model_ids = {run.model_config_id for run in selected}
        self._models.update({key: None for key in model_ids})
        for model in await self.session.scalars(
            select(AgentModelConfigRecord).where(AgentModelConfigRecord.id.in_(model_ids))
        ):
            self._models[model.id] = model

    async def verified(
        self,
        run: AgentResearchRunRecord,
        output: AgentResearchOutputRecord | None,
        bindings: Sequence[AgentResearchOutputEvidenceRecord],
    ) -> bool:
        if output is None or run.pricing_version not in _OBSERVATION_VERSIONS:
            return False
        return await self._verify(run, output, bindings)

    async def _verify(
        self,
        run: AgentResearchRunRecord,
        output: AgentResearchOutputRecord,
        bindings: Sequence[AgentResearchOutputEvidenceRecord],
    ) -> bool:
        try:
            intent = ResearchIntent.model_validate(output.intent)
        except ValidationError:
            return False
        # Do not widen the old complete/legacy path or perform extra work for it.
        sufficient_observation = _sufficient_research_observation(intent)
        if not (safe_no_trade_intent_shape_valid(intent) or sufficient_observation):
            return False
        if (
            run.status != "completed"
            or output.status != "completed"
            or output.role is not None
            or output.run_id != run.id
            or max(run.created_at, output.created_at, run.decision_time) > self.read_at
            or intent.instrument_id != run.instrument_id
            or intent.data_snapshot_id != run.data_snapshot_id
            or intent.decision_time != run.decision_time
            or output.payload.get("quality_status") != intent.quality_status
            or canonical_hash(
                {
                    "status": output.status,
                    "role": output.role,
                    "payload": output.payload,
                    "intent": output.intent,
                }
            )
            != output.content_hash
        ):
            return False
        if run.id not in self._terminals:
            await self.prime([run])
        terminals = self._terminals[run.id]
        model = self._models[run.model_config_id]
        snapshot = await self.session.get(DataSnapshot, run.data_snapshot_id)
        if sufficient_observation:
            try:
                require_observation_snapshot(snapshot)
            except ValueError:
                return False
            if (
                snapshot is None
                or snapshot.provider_object_id != str(run.instrument_id)
                or snapshot.payload.get("instrument_id") != str(run.instrument_id)
            ):
                return False
        if canonical_hash(run.prompt_manifest) != run.prompt_hash:
            return False
        prompt_key = (run.prompt_hash, run.prompt_version_id)
        if prompt_key not in self._prompts:
            self._prompts[prompt_key] = await load_frozen_prompts(self.session, run)
        prompts = self._prompts[prompt_key]
        if (
            terminals != (output.id,)
            or model is None
            or snapshot is None
            or prompts is None
            or model.version != run.pricing_version
            or model.config_hash != run.model_hash
            or intent.model_version != model.model_id
            or intent.prompt_version != prompts["orchestrator"].version
            or intent.data_version != snapshot.revision
            or max(snapshot.available_at, snapshot.received_at) > run.decision_time
        ):
            return False
        if canonical_hash(run.evidence_manifest) != run.evidence_hash:
            return False
        evidence_key = (run.evidence_hash, run.instrument_id, run.decision_time)
        if evidence_key not in self._evidence:
            self._evidence[evidence_key] = await load_frozen_evidence(
                self.session, run, allow_fixture_evidence=False
            )
        evidence = self._evidence[evidence_key]
        by_id = {item.id: item for item in evidence}
        if (
            not evidence
            or len(bindings) != len(by_id)
            or {item.evidence_ref_id for item in bindings} != set(by_id)
            or any(
                item.output_id != output.id or item.created_at > self.read_at for item in bindings
            )
            or any(item.created_at > self.read_at for item in evidence)
        ):
            return False
        fields = (
            "source",
            "provider_object_id",
            "revision",
            "available_at",
            "location",
            "content_hash",
        )
        if any(
            any(
                getattr(item, field) != getattr(by_id[item.evidence_ref_id], field)
                for field in fields
            )
            for item in bindings
        ):
            return False
        expected = {_evidence_identity(domain_evidence(item)) for item in evidence}
        if (
            len(intent.evidence_refs) != len(expected)
            or {_evidence_identity(item) for item in intent.evidence_refs} != expected
            or not _all_nested_evidence_bound(intent, expected)
        ):
            return False
        context = await read_portfolio_context(
            self.session, run.data_snapshot_id, run.instrument_id, run.decision_time
        )
        # valid_until is an execution cutoff, not permission to erase historical reading.
        return condition_evaluations_match(intent, context)
