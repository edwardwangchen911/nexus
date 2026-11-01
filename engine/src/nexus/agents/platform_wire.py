"""Opt-in Platform compilation only; no credential, admission or dispatch authority.

Freeze all stages before a future producer reserves anything. The native caller
must bind the allocation hash and verify same-chain prior observations. Authorize
and send the returned bytes unchanged; never rebuild JSON in the HTTP lifecycle.
Existing SDK/HTTP routes and their limits remain unchanged.
"""

import copy
import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field, fields
from typing import Any, Literal

from pydantic import BaseModel

from nexus.agents.codex_capacity import _monitoring_capacity_view, candidate_capacity_envelope
from nexus.agents.codex_sdk import (
    BASE_INSTRUCTIONS,
    DEVELOPER_INSTRUCTIONS,
    codex_output_schema,
    codex_request_wire,
)
from nexus.agents.codex_staged import STAGES
from nexus.agents.committee import INDEPENDENT_MONITOR_COMPACT_PROMPT_VERSION, ROLE_ORDER
from nexus.agents.comparison_compiler import _compiler, compile_comparison_stage
from nexus.agents.comparison_inputs import PreparedComparisonInput, assert_outbound_safe
from nexus.agents.models import ResearchRequest
from nexus.agents.platform_usage import (
    PLATFORM_PRICE_VERSION,
    PLATFORM_PRICE_VERSIONS,
    price_platform_usage,
)
from nexus.domain.evidence import EvidenceRef

Stage = Literal["research", "critique", "synthesis"]
_STAGE_ORDER = ("research", "critique", "synthesis")
_OUTPUT_LIMIT = 8192
_FRAMING_UNITS = 8192
_MAX_STAGE_UNITS = 272000


def _hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()


@dataclass(frozen=True)
class PlatformProfile:
    """Credential kind is a declaration, never proof of Key or funding eligibility."""

    credential_kind: str = "platform_project_key"
    endpoint: str = "https://api.openai.com/v1/responses"
    model: str = "gpt-5.6-terra"
    effort: str = "low"
    service_tier: str = "default"
    cache_mode: str = "implicit"
    cache_ttl: str = "30m"
    stream: bool = True
    store: bool = False
    price_version: str = PLATFORM_PRICE_VERSION
    base_instructions_hash: str = hashlib.sha256(BASE_INSTRUCTIONS.encode()).hexdigest()
    developer_instructions_hash: str = hashlib.sha256(DEVELOPER_INSTRUCTIONS.encode()).hexdigest()


def _require_profile(profile: PlatformProfile) -> None:
    fixed = PlatformProfile(price_version=profile.price_version)
    if profile.price_version not in PLATFORM_PRICE_VERSIONS or any(
        type(getattr(profile, f.name)) is not type(getattr(fixed, f.name))
        or getattr(profile, f.name) != getattr(fixed, f.name)
        for f in fields(fixed)
    ):
        raise ValueError("PLATFORM_PROFILE_INVALID")


@dataclass(frozen=True)
class PlatformAllocation:
    """Freeze/persist this exact allocation and hash; it grants no execution right."""

    input_hash: str
    stage_input_upper_bounds: tuple[int, ...]
    stage_schema_envelope_hashes: tuple[str, ...]
    profile: PlatformProfile
    stage_order: tuple[str, ...] = _STAGE_ORDER
    output_limit: int = _OUTPUT_LIMIT
    framing_units: int = _FRAMING_UNITS
    units_version: str = "platform-full-http-utf8-plus8192-v1"

    @property
    def allocation_hash(self) -> str:
        return _hash(asdict(self))


@dataclass(frozen=True)
class PriorPlatformUsage:
    """Same-chain native verification supplied by the caller, not created here.

    Stage numbers are the existing native protocol's 1/2/3. Keep the original
    response usage and actual response model/tier, before SDK field coercion.
    A caller must also verify its Run/attempt/observation identities and outputs.
    """

    stage: int
    input_hash: str
    verified: bool
    response: Mapping[str, object] = field(repr=False)


@dataclass(frozen=True)
class PlatformStageWire:
    wire: bytes = field(repr=False)
    body_sha256: str
    binding_hash: str
    allocation_hash: str
    stage: str
    input_units: int
    input_upper_bound: int
    endpoint: str
    credential_kind: str

    def verify_wire(self, wire: bytes) -> None:
        """Future lifecycle must verify then send these bytes without serialization."""
        if (
            type(wire) is not bytes
            or hashlib.sha256(wire).hexdigest() != self.body_sha256
            or len(wire) + _FRAMING_UNITS > self.input_upper_bound
        ):
            raise ValueError("PLATFORM_WIRE_CHANGED")


def _wire(
    payload: Mapping[str, object],
    output_model: type[BaseModel],
    *,
    output_remaining: int,
    profile: PlatformProfile,
    forbidden: tuple[str, ...],
) -> bytes:
    body = {
        "model": profile.model,
        "instructions": BASE_INSTRUCTIONS,
        "input": [
            {"role": "developer", "content": DEVELOPER_INSTRUCTIONS},
            {"role": "user", "content": codex_request_wire(payload)},
        ],
        "reasoning": {"effort": profile.effort},
        "tools": [],
        "stream": profile.stream,
        "store": profile.store,
        "text": {
            "format": {
                "type": "json_schema",
                "name": "research_stage",
                "strict": True,
                "schema": codex_output_schema(output_model),
            }
        },
        "max_output_tokens": output_remaining,
        "service_tier": profile.service_tier,
        "prompt_cache_options": {"mode": profile.cache_mode, "ttl": profile.cache_ttl},
    }
    assert_outbound_safe(body, forbidden)
    wire = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode()
    assert_outbound_safe(wire.decode(), forbidden)
    return wire


def freeze_platform_allocation(
    prepared: PreparedComparisonInput, *, profile: PlatformProfile = PlatformProfile()
) -> PlatformAllocation:
    """Compile the existing maximal schema envelope, including outer JSON escaping.

    These local UTF-8-plus-framing units are conservative, not official tokens.
    Prior shapes cover all accepted schema outputs, independent of a sample or
    the old experiment's outputs. No old common-wire 100479 admission is used.
    """
    _require_profile(profile)
    if prepared.input_hash != prepared.proof.get("input_hash"):
        raise ValueError("COMPARISON_INPUT_CHANGED")
    member = copy.deepcopy(prepared.payload)
    compiler = _compiler(member)
    request = ResearchRequest.model_validate(member["request"])
    refs = tuple(EvidenceRef.model_validate(row) for row in member["evidence"])
    inputs = member["inputs"]
    envelope = candidate_capacity_envelope(
        compiler, request=request, evidence_refs=refs, inputs=inputs
    )
    view = _monitoring_capacity_view(compiler, refs, inputs)
    widest = (
        "𠀀"
        if compiler._prompts["portfolio"].version == INDEPENDENT_MONITOR_COMPACT_PROMPT_VERSION
        else "\x00"
    )
    bounds, schemas = [], []
    envelope_stages: Any = envelope["stages"]
    for (stage, roles), bound in zip(STAGES, envelope_stages, strict=True):
        previous: list[dict[str, object]] = [
            dict(
                claim_id=f"{role}:{i}",
                role=role,
                vote="abstain",
                text=widest * 400,
                citations=[len(refs) - 1] * 3,
            )
            for role in ROLE_ORDER[: ROLE_ORDER.index(roles[0])]
            for i in range(2)
        ]
        payload, schema = view.build_stage_request(
            stage=stage,
            request=request,
            evidence_refs=refs,
            frozen_inputs=inputs,
            previous=previous,
            financial_table_candidate=True,
        )
        if len(codex_request_wire(payload).encode()) != bound["wire_upper_bytes"]:
            raise ValueError("PLATFORM_CAPACITY_ENVELOPE_CHANGED")
        units = (
            len(
                _wire(
                    payload,
                    schema,
                    output_remaining=_OUTPUT_LIMIT,
                    profile=profile,
                    forbidden=prepared.forbidden,
                )
            )
            + _FRAMING_UNITS
        )
        if units > _MAX_STAGE_UNITS:
            raise ValueError("PLATFORM_INPUT_TOO_LARGE")
        bounds.append(units)
        schemas.append(_hash(codex_output_schema(schema)))
    return PlatformAllocation(prepared.input_hash, tuple(bounds), tuple(schemas), profile)


def _remaining(
    prior: Sequence[PriorPlatformUsage],
    *,
    index: int,
    input_hash: str,
    profile: PlatformProfile,
    input_bounds: tuple[int, ...],
) -> int:
    if len(prior) != index:
        raise ValueError("PLATFORM_PRIOR_USAGE_INVALID")
    spent = 0
    for stage, row in enumerate(prior, start=1):
        if (
            type(row.stage) is not int
            or row.stage != stage
            or row.input_hash != input_hash
            or row.verified is not True
            or row.response.get("status") != "completed"
        ):
            raise ValueError("PLATFORM_PRIOR_USAGE_INVALID")
        usage = price_platform_usage(
            row.response.get("usage"),
            model=row.response.get("model"),
            service_tier=row.response.get("service_tier"),
            cache_mode=profile.cache_mode,
            cache_ttl=profile.cache_ttl,
        )
        if (
            not usage.totals_verified
            or usage.exact_cost_usd is None
            or usage.input_tokens is None
            or usage.input_tokens > input_bounds[stage - 1]
        ):
            raise ValueError("PLATFORM_PRIOR_USAGE_INVALID")
        assert usage.output_tokens is not None
        spent += usage.output_tokens
    remaining = _OUTPUT_LIMIT - spent
    if remaining < 16:
        raise ValueError("PLATFORM_OUTPUT_EXHAUSTED")
    return remaining


def compile_platform_stage(
    prepared: PreparedComparisonInput,
    *,
    allocation: PlatformAllocation,
    expected_allocation_hash: str,
    stage: Stage,
    previous: list[dict[str, object]],
    prior_usage: Sequence[PriorPlatformUsage],
    compiled_request: tuple[Mapping[str, object], type[BaseModel]] | None = None,
) -> PlatformStageWire:
    """The single future producer/claim/HTTP handoff: frozen bytes plus their binding.

    Authorization must retain binding_hash; HTTP sends wire unchanged to endpoint
    using separately qualified project-key auth. This helper never loads a Key,
    creates a Grant/JWT, verifies funds, grants a ticket, or settles a ledger.
    """
    _require_profile(allocation.profile)
    if (
        allocation.allocation_hash != expected_allocation_hash
        or allocation.input_hash != prepared.input_hash
        or allocation.stage_order != _STAGE_ORDER
        or type(allocation.output_limit) is not int
        or allocation.output_limit != _OUTPUT_LIMIT
        or type(allocation.framing_units) is not int
        or allocation.framing_units != _FRAMING_UNITS
        or allocation.units_version != "platform-full-http-utf8-plus8192-v1"
        or len(allocation.stage_schema_envelope_hashes) != 3
        or len(allocation.stage_input_upper_bounds) != 3
        or any(
            type(n) is not int or not _FRAMING_UNITS < n <= _MAX_STAGE_UNITS
            for n in allocation.stage_input_upper_bounds
        )
    ):
        raise ValueError("PLATFORM_ALLOCATION_CHANGED")
    if stage not in _STAGE_ORDER:
        raise ValueError("PLATFORM_STAGE_INVALID")
    index = _STAGE_ORDER.index(stage)
    remaining = _remaining(
        prior_usage,
        index=index,
        input_hash=prepared.input_hash,
        profile=allocation.profile,
        input_bounds=allocation.stage_input_upper_bounds,
    )
    if compiled_request is None:
        payload, schema = compile_comparison_stage(
            prepared,
            expected_input_hash=allocation.input_hash,
            stage=stage,
            previous=previous,
        )
    else:
        # The native committee includes prior monitoring proposals/reviews as
        # well as claims. Preserve its actual schema and complete payload.
        actual_payload, schema = compiled_request
        payload = dict(actual_payload)
        if payload.get("stage") != stage:
            raise ValueError("PLATFORM_STAGE_INVALID")
    # Normal schemas bind actual prior claim IDs; their enums are subsets of
    # the maximal envelope. The exact current schema is bound by body_sha256.
    wire = _wire(
        payload,
        schema,
        output_remaining=remaining,
        profile=allocation.profile,
        forbidden=prepared.forbidden,
    )
    units = len(wire) + _FRAMING_UNITS
    upper_bound = allocation.stage_input_upper_bounds[index]
    if units > upper_bound:
        raise ValueError("PLATFORM_INPUT_TOO_LARGE")
    body_hash = hashlib.sha256(wire).hexdigest()
    return PlatformStageWire(
        wire=wire,
        body_sha256=body_hash,
        binding_hash=_hash(
            dict(allocation_hash=expected_allocation_hash, stage=stage, body_sha256=body_hash)
        ),
        allocation_hash=expected_allocation_hash,
        stage=stage,
        input_units=units,
        input_upper_bound=upper_bound,
        endpoint=allocation.profile.endpoint,
        credential_kind=allocation.profile.credential_kind,
    )
