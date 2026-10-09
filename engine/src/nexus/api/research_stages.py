"""Read saved experimental stages without replaying model calls or promoting intent."""

import hashlib
import json
from collections.abc import Sequence

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_staged import STAGES
from nexus.agents.codex_versions import (
    CANDIDATE_BUDGET_CONFIG_VERSION,
    STAGED_CONFIG_VERSIONS,
    STRUCTURED_CONFIG_VERSIONS,
)
from nexus.agents.committee import CommitteeRoleOutput
from nexus.agents.execution_authority import load_frozen_evidence, load_frozen_prompts
from nexus.agents.synthesis import citations_bound, domain_evidence
from nexus.api.schemas.agents import ResearchStageRole, ResearchStageSummary, ResearchStageTrace
from nexus.domain.evidence import EvidenceRef
from nexus.persistence.models.agents import AgentResearchOutputRecord, AgentResearchRunRecord
from nexus.strategy.intents import ResearchClaim


async def read_stage_trace(
    session: AsyncSession, run: AgentResearchRunRecord
) -> ResearchStageTrace | None:
    if run.pricing_version not in (STAGED_CONFIG_VERSIONS | {CANDIDATE_BUDGET_CONFIG_VERSION}):
        return None
    records = tuple(
        await session.scalars(
            select(AgentResearchOutputRecord)
            .where(
                AgentResearchOutputRecord.run_id == run.id,
                AgentResearchOutputRecord.role.is_not(None),
            )
            .order_by(AgentResearchOutputRecord.sequence)
            .limit(9)
        )
    )
    evidence = await load_frozen_evidence(session, run, allow_fixture_evidence=False)
    prompts = await load_frozen_prompts(session, run)
    return validate_stage_trace(
        run,
        records,
        tuple(domain_evidence(item) for item in evidence),
        {} if prompts is None else {role: prompt.content_hash for role, prompt in prompts.items()},
    )


def validate_stage_trace(
    run: AgentResearchRunRecord,
    records: Sequence[AgentResearchOutputRecord],
    evidence: tuple[EvidenceRef, ...],
    prompt_hashes: dict[str, str],
) -> ResearchStageTrace:
    """Suppress a corrupted trace; never display arbitrary stored JSON as checked research."""
    invalid = ResearchStageTrace(evidence_hash=run.evidence_hash, status="invalid", stages=())
    expected = [(stage, role) for stage, roles in STAGES for role in roles]
    if not evidence or len(records) > len(expected) or len(prompt_hashes) != 8:
        return invalid
    outputs = []
    prior_ids: set[str] = set()
    try:
        for record, (stage, role) in zip(records, expected, strict=False):
            content = {
                "status": record.status,
                "role": record.role,
                "payload": record.payload,
                "intent": record.intent,
            }
            digest = hashlib.sha256(
                json.dumps(content, sort_keys=True, separators=(",", ":"), default=str).encode()
            ).hexdigest()
            output = CommitteeRoleOutput.model_validate(record.payload)
            reviewed = set(output.reviewed_claim_ids)
            # Only earlier stages are review targets, matching the actual model input.
            allowed_ids = {
                item
                for item in prior_ids
                if not item.startswith(
                    tuple(f"{peer}:" for name, peers in STAGES if name == stage for peer in peers)
                )
            }
            if (
                record.content_hash != digest
                or record.run_id != run.id
                or record.role != role
                or output.role != role
                or output.stage != stage
                or record.prompt_hash != output.prompt_hash
                or output.prompt_hash != prompt_hashes.get(role)
                or not citations_bound(output.claims, evidence)
                or not reviewed.issubset(allowed_ids)
                or (stage == "research" and reviewed)
                or (stage != "research" and not reviewed)
                or (
                    stage == "synthesis"
                    and any(
                        not any(item.startswith(f"{reviewer}:") for item in reviewed)
                        for reviewer in ("bear", "evidence_audit")
                    )
                )
            ):
                return invalid
            assessment = output.synthesis_assessment
            if (
                run.pricing_version
                in (STRUCTURED_CONFIG_VERSIONS | {CANDIDATE_BUDGET_CONFIG_VERSION})
                and stage == "synthesis"
            ):
                required = {
                    item for item in prior_ids if item.startswith(("bear:", "evidence_audit:"))
                }
                if (
                    assessment is None
                    or {item.claim_id for item in assessment.reviews} != required
                    or reviewed != required
                    or output.claims != assessment.claims
                    or not citations_bound(
                        assessment.claims
                        + tuple(
                            ResearchClaim(text=item.rationale, evidence_refs=item.evidence_refs)
                            for item in assessment.reviews
                        ),
                        evidence,
                    )
                ):
                    return invalid
            elif assessment is not None:
                return invalid
            ResearchStageRole.model_validate(
                output.model_dump(
                    include={"role", "claims", "reviewed_claim_ids", "synthesis_assessment"}
                )
            )
            outputs.append(output)
            prior_ids.update(f"{role}:{index}" for index in range(len(output.claims)))
    except (ValidationError, TypeError, ValueError):
        return invalid
    if any(o.stage_input_hash or o.frozen_inputs_hash for o in outputs) and (
        any(not o.stage_input_hash or not o.frozen_inputs_hash for o in outputs)
        or len({o.frozen_inputs_hash for o in outputs}) != 1
        or any(
            len({o.stage_input_hash for o in outputs if o.stage == stage}) > 1
            for stage, _ in STAGES
        )
    ):
        return invalid
    return ResearchStageTrace(
        evidence_hash=run.evidence_hash,
        status="verified",
        stages=tuple(
            ResearchStageSummary(
                stage=stage,
                status="complete"
                if len(saved := [o for o in outputs if o.stage == stage]) == len(roles)
                else "partial"
                if saved
                else "unavailable",
                roles=tuple(
                    ResearchStageRole(
                        role=o.role,
                        claims=o.claims,
                        reviewed_claim_ids=o.reviewed_claim_ids,
                        synthesis_assessment=o.synthesis_assessment,
                    )
                    for o in saved
                ),
                input_tokens=sum(o.input_tokens for o in saved),
                output_tokens=sum(o.output_tokens for o in saved),
                input_hash=saved[0].stage_input_hash if saved else None,
                frozen_inputs_hash=saved[0].frozen_inputs_hash if saved else None,
            )
            for stage, roles in STAGES
        ),
    )
