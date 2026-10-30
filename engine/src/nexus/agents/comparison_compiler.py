"""The same normal v11 stage compiler for both experimental transports."""

import copy
from time import monotonic
from typing import Literal

from pydantic import BaseModel

from nexus.agents.codex_sdk import codex_output_schema
from nexus.agents.codex_staged import StructuredStagedCodexCommittee
from nexus.agents.codex_versions import BOUNDED_STAGE_BUDGET_CONFIG_VERSION
from nexus.agents.committee import ROLE_ORDER, PromptAsset
from nexus.agents.comparison_inputs import PreparedComparisonInput, assert_outbound_safe
from nexus.agents.models import ResearchRequest
from nexus.agents.tools import ReadOnlyTools
from nexus.domain.evidence import EvidenceRef


def compile_comparison_stage(
    prepared: PreparedComparisonInput,
    *,
    expected_input_hash: str,
    stage: Literal["research", "critique", "synthesis"],
    previous: list[dict[str, object]],
) -> tuple[dict[str, object], type[BaseModel]]:
    """Compile only. No tools, provider, Run, reservation or dispatch is invoked.

    The runner must supply this chain's validated prior outputs and the input
    hash from its registered position. It cannot silently substitute new inputs.
    """
    if (
        prepared.input_hash != expected_input_hash
        or prepared.proof["input_hash"] != expected_input_hash
    ):
        raise ValueError("COMPARISON_INPUT_CHANGED")
    assert_outbound_safe(previous, prepared.forbidden)
    member = copy.deepcopy(prepared.payload)
    compiler = StructuredStagedCodexCommittee(
        prompts={role: PromptAsset.model_validate(member["prompts"][role]) for role in ROLE_ORDER},
        tools=ReadOnlyTools(executors={}),
        # Required by the v11 compiler constructor. No execution occurs here;
        # the native claim/deadline remains the runner's separate authority.
        deadline=monotonic() + 300,
        aggregate_input_tokens=96000,
        max_wire_bytes=100479,
        inline_citations=True,
        paper_stage_scope=True,
        stage_time_budget=True,
        stage_budget_version=BOUNDED_STAGE_BUDGET_CONFIG_VERSION,
    )
    payload, output_model = compiler.build_stage_request(
        stage=stage,
        request=ResearchRequest.model_validate(member["request"]),
        evidence_refs=tuple(EvidenceRef.model_validate(row) for row in member["evidence"]),
        frozen_inputs=member["inputs"],
        previous=copy.deepcopy(previous),
    )
    assert_outbound_safe(
        {"payload": payload, "output_schema": codex_output_schema(output_model)}, prepared.forbidden
    )
    return payload, output_model
