"""Eight bounded research roles, versioned prompts, and the deterministic CI committee."""

import hashlib
from collections.abc import Mapping
from pathlib import Path
from typing import Final, Literal, Protocol, cast

from pydantic import BaseModel, ConfigDict, Field, SerializerFunctionWrapHandler, model_serializer

from nexus.agents.models import ResearchRequest
from nexus.domain.evidence import EvidenceRef
from nexus.strategy.intents import ResearchClaim, SynthesisAssessment

type AgentRole = Literal[
    "orchestrator",
    "fundamental",
    "market_state",
    "macro_sector",
    "bull",
    "bear",
    "evidence_audit",
    "portfolio",
]

ROLE_ORDER: Final[tuple[AgentRole, ...]] = (
    "orchestrator",
    "fundamental",
    "market_state",
    "macro_sector",
    "bull",
    "bear",
    "evidence_audit",
    "portfolio",
)


class PromptAsset(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    role: AgentRole
    version: str
    content: str
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class StageInputIdentity(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    stage: Literal["research", "critique", "synthesis"]
    request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    frozen_inputs_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class CommitteeRoleOutput(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    role: AgentRole
    vote: Literal["support", "oppose", "abstain"]
    claims: tuple[ResearchClaim, ...]
    prompt_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)
    conflicting_facts: bool = False
    stage: Literal["research", "critique", "synthesis"] | None = None
    reviewed_claim_ids: tuple[str, ...] = ()
    stage_input_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    frozen_inputs_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    synthesis_assessment: SynthesisAssessment | None = None

    @model_serializer(mode="wrap")
    # A return annotation would replace the closed model serialization schema.
    def _legacy_payload(self, handler: SerializerFunctionWrapHandler):  # type: ignore[no-untyped-def]
        payload = cast(dict[str, object], handler(self))
        if self.synthesis_assessment is None:
            payload.pop("synthesis_assessment", None)
        return payload


class CommitteeModel(Protocol):
    async def analyze(
        self,
        role: AgentRole,
        request: ResearchRequest,
        evidence_refs: tuple[EvidenceRef, ...],
        prompt: PromptAsset,
    ) -> CommitteeRoleOutput: ...


class FixtureCommittee:
    """Deterministic local committee used by CI; it performs no transport or I/O."""

    def __init__(self, *, conflicting_facts: bool = False) -> None:
        self._conflicting_facts = conflicting_facts

    async def analyze(
        self,
        role: AgentRole,
        request: ResearchRequest,
        evidence_refs: tuple[EvidenceRef, ...],
        prompt: PromptAsset,
    ) -> CommitteeRoleOutput:
        del request
        votes: Mapping[AgentRole, Literal["support", "oppose", "abstain"]] = {
            "orchestrator": "support",
            "fundamental": "support",
            "market_state": "support",
            "macro_sector": "abstain",
            "bull": "support",
            "bear": "oppose",
            "evidence_audit": "support",
            "portfolio": "abstain",
        }
        return CommitteeRoleOutput(
            role=role,
            vote=votes[role],
            claims=(
                ResearchClaim(
                    text=f"The {role} fixture review supports continued monitoring.",
                    evidence_refs=evidence_refs,
                ),
            ),
            prompt_hash=prompt.content_hash,
            input_tokens=10,
            output_tokens=5,
            conflicting_facts=self._conflicting_facts and role in {"bull", "bear"},
        )


def load_prompt_assets() -> Mapping[AgentRole, PromptAsset]:
    """Load all versioned prompt content and derive the hashes persisted by the worker."""
    directory = Path(__file__).with_name("prompts")
    prompts: dict[AgentRole, PromptAsset] = {}
    for role in ROLE_ORDER:
        content = (directory / f"{role}-v1.md").read_text(encoding="utf-8").strip()
        prompts[role] = PromptAsset(
            role=role,
            version="fixture-v1",
            content=content,
            content_hash=hashlib.sha256(content.encode()).hexdigest(),
        )
    return prompts
