"""Eight bounded research roles, versioned prompts, and the deterministic CI committee."""

import hashlib
from collections.abc import Mapping
from pathlib import Path
from typing import Annotated, Final, Literal, Protocol, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    model_serializer,
    model_validator,
)

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


type InvocationPhase = Literal[
    "preflight",
    "client_init",
    "account",
    "config_read",
    "thread_start",
    "turn_run",
    "client_close",
    "response_check",
    "aggregate_wait",
]
type InvocationKind = Literal[
    "input_too_large",
    "login_required",
    "response_incomplete",
    "unexpected_tool",
    "output_too_large",
    "usage_missing",
    "usage_invalid",
    "timeout",
    "unexpected_exception",
]
type TimeoutSource = Literal[
    "stage_deadline", "invocation_deadline", "committee_deadline", "unknown"
]


class SdkStreamProgress(BaseModel):
    """Delivered notification metadata only; terminal receipt is not success."""

    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    available: bool = Field(strict=True)
    notifications: int = Field(default=0, strict=True, ge=0, le=1000000)
    progress_notifications: int = Field(default=0, strict=True, ge=0, le=1000000)
    first_notification_ms: int | None = Field(default=None, strict=True, ge=0)
    last_notification_ms: int | None = Field(default=None, strict=True, ge=0)
    terminal_notification_seen: bool = Field(default=False, strict=True)
    # Absent in old receipts: unknown, never inferred from the combined count.
    reasoning_notifications: int | None = Field(default=None, strict=True, ge=0, le=1000000)
    answer_notifications: int | None = Field(default=None, strict=True, ge=0, le=1000000)

    @model_serializer(mode="wrap")
    def _serialize_counts(self, handler: SerializerFunctionWrapHandler) -> dict[str, object]:
        serialized = cast(dict[str, object], handler(self))
        for field in ("reasoning_notifications", "answer_notifications"):
            if getattr(self, field) is None:
                serialized.pop(field, None)
        return serialized


class StageInvocationTiming(BaseModel):
    """Local monotonic durations only, with no model text or account facts."""

    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    allocated_timeout_ms: int | None = Field(default=None, strict=True, ge=0, le=295000)
    flexible_critique_profile: Literal["codex-sdk-chatgpt-v10-flexible-stage-budget"] | None = None
    bounded_stage_profile: Literal["codex-sdk-chatgpt-v11-bounded-stage-budget"] | None = None
    diagnostic_profile: Literal["full-input-timing-diagnostic-v1"] | None = None
    elapsed_ms: int = Field(strict=True, ge=0)
    phase_elapsed_ms: dict[InvocationPhase, Annotated[int, Field(strict=True, ge=0)]]
    stream_progress: SdkStreamProgress | None = None
    # Sampled together at allocation, before SDK invocation; not new allowances.
    committee_remaining_ms: int | None = Field(default=None, strict=True, ge=0)
    stage_preparation_ms: int | None = Field(default=None, strict=True, ge=0)

    @model_validator(mode="after")
    def _profile_allocation(self) -> "StageInvocationTiming":
        if (
            sum(
                profile is not None
                for profile in (
                    self.diagnostic_profile,
                    self.flexible_critique_profile,
                    self.bounded_stage_profile,
                )
            )
            > 1
        ):
            raise ValueError("STAGE_TIMING_PROFILE_INVALID")
        limit = (
            295000
            if self.diagnostic_profile is not None
            else 288000
            if self.bounded_stage_profile is not None
            else 116000
            if self.flexible_critique_profile is not None
            else 108000
        )
        if (self.allocated_timeout_ms or 0) > limit:
            raise ValueError("STAGE_TIMING_PROFILE_INVALID")
        return self

    @model_serializer(mode="wrap")
    def _serialize_profile(self, handler: SerializerFunctionWrapHandler) -> dict[str, object]:
        serialized = cast(dict[str, object], handler(self))
        if self.bounded_stage_profile is None:
            serialized.pop("bounded_stage_profile", None)
        if self.flexible_critique_profile is None:
            serialized.pop("flexible_critique_profile", None)
        if self.diagnostic_profile is None:
            serialized.pop("diagnostic_profile", None)
        for field in ("committee_remaining_ms", "stage_preparation_ms"):
            if getattr(self, field) is None:
                serialized.pop(field, None)
        return serialized


type SdkExceptionKind = Literal[
    "transport_closed", "rpc_error", "runtime_error", "timeout", "validation_error", "other"
]


class SdkFailureFacts(BaseModel):
    """Allowlisted metadata only; no exception messages or provider response bodies."""

    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    exception_kind: SdkExceptionKind = "other"
    cleanup_kind: SdkExceptionKind | None = None
    terminal_status: Literal["completed", "failed", "interrupted", "inProgress"] | None = None
    provider_reason: (
        Literal[
            "contextWindowExceeded",
            "sessionBudgetExceeded",
            "usageLimitExceeded",
            "serverOverloaded",
            "cyberPolicy",
            "internalServerError",
            "unauthorized",
            "badRequest",
            "threadRollbackFailed",
            "sandboxError",
            "other",
            "httpConnectionFailed",
            "responseStreamConnectionFailed",
            "responseStreamDisconnected",
            "responseTooManyFailedAttempts",
            "activeTurnNotSteerable",
        ]
        | None
    ) = None


class StageFailureDiagnostic(BaseModel):
    """Safe local terminal metadata, never response text or exception context."""

    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    stage: Literal["research", "critique", "synthesis"]
    boundary: Literal["invocation", "wire_validation", "role_binding", "assessment_binding"]
    role: AgentRole | None = None
    response_hash: str | None = Field(pattern=r"^[0-9a-f]{64}$")
    invocation_phase: InvocationPhase | None = None
    invocation_kind: InvocationKind | None = None
    timeout_source: TimeoutSource | None = None
    timing: StageInvocationTiming | None = None
    failure_facts: SdkFailureFacts | None = None
    validation_kind: (
        Literal[
            "schema_validation",
            "unsafe_text",
            "domain_validation",
            "review_rationale_blank",
            "review_resolution_inconsistent",
            "review_evidence_incomplete",
            "review_sufficiency_inconsistent",
        ]
        | None
    ) = None

    @model_validator(mode="after")
    def _boundary_fields(self) -> "StageFailureDiagnostic":
        if self.boundary == "invocation":
            if (
                self.response_hash is not None
                or self.role is not None
                or self.validation_kind is not None
                or self.invocation_phase is None
                or self.invocation_kind is None
            ):
                raise ValueError("INVOCATION_DIAGNOSTIC_INVALID")
            if self.timeout_source is not None and self.invocation_kind != "timeout":
                raise ValueError("INVOCATION_DIAGNOSTIC_INVALID")
        elif (
            self.response_hash is None
            or self.invocation_phase is not None
            or self.invocation_kind is not None
            or self.timing is not None
            or self.timeout_source is not None
            or self.failure_facts is not None
        ):
            raise ValueError("VALIDATION_DIAGNOSTIC_INVALID")
        return self

    @model_serializer(mode="wrap")
    def _legacy_payload(self, handler: SerializerFunctionWrapHandler):  # type: ignore[no-untyped-def]
        payload = cast(dict[str, object], handler(self))
        if self.failure_facts is None:
            payload.pop("failure_facts", None)
        if self.timeout_source is None:
            payload.pop("timeout_source", None)
        if self.timing is None:
            payload.pop("timing", None)
        if self.boundary != "invocation":
            payload.pop("invocation_phase", None)
            payload.pop("invocation_kind", None)
        return payload


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


SECURITY_RESEARCH_SCOPE_PROMPT_VERSION = "local-portfolio-synthesis-v5-paper-scope"


CLAIM_REVIEW_SCOPE_PROMPT_VERSION = "local-portfolio-synthesis-v6-claim-review"


INFERENCE_REVIEW_SCOPE_PROMPT_VERSION = "local-portfolio-synthesis-v7-inference-review"


def load_portfolio_synthesis_prompt(
    *,
    security_research_scope: bool = False,
    claim_review_scope: bool = False,
    inference_review_scope: bool = False,
) -> PromptAsset:
    """Load the immutable cross-source final-role prompt used by normal local setup."""
    if type(security_research_scope) is not bool:
        raise ValueError("SECURITY_RESEARCH_SCOPE_INVALID")
    if type(claim_review_scope) is not bool or (claim_review_scope and not security_research_scope):
        raise ValueError("CLAIM_REVIEW_SCOPE_INVALID")
    if type(inference_review_scope) is not bool or (
        inference_review_scope and not (security_research_scope and claim_review_scope)
    ):
        raise ValueError("INFERENCE_REVIEW_SCOPE_INVALID")
    version = (
        INFERENCE_REVIEW_SCOPE_PROMPT_VERSION
        if inference_review_scope
        else CLAIM_REVIEW_SCOPE_PROMPT_VERSION
        if claim_review_scope
        else SECURITY_RESEARCH_SCOPE_PROMPT_VERSION
        if security_research_scope
        else "local-portfolio-synthesis-v4"
    )
    content = (
        Path(__file__)
        .with_name("prompts")
        .joinpath(f"{version.removeprefix('local-')}.md")
        .read_text(encoding="utf-8")
        .strip()
    )
    return PromptAsset(
        role="portfolio",
        version=version,
        content=content,
        content_hash=hashlib.sha256(content.encode()).hexdigest(),
    )
