"""Local v9 full-content capacity qualification, without admission or registration."""

import hashlib
import json
from collections.abc import Sequence
from copy import copy
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_sdk import (
    BASE_INSTRUCTIONS,
    DEVELOPER_INSTRUCTIONS,
    MAX_INPUT_TOKENS,
    _Claim,
    _Role,
    codex_input_allowance,
    codex_output_schema,
    codex_request_hash,
    codex_request_wire,
)
from nexus.agents.codex_staged import (
    STAGES,
    StagedCodexCommittee,
    _DailyPendingItem,
    _DailyPendingProposal,
    _DailyPendingReview,
    _MonitoringProposal,
    _MonitoringReview,
)
from nexus.agents.codex_token_budget import INPUT_RESERVE
from nexus.agents.codex_versions import (
    BOUNDED_STAGE_BUDGET_CONFIG_VERSION,
    FLEXIBLE_STAGE_BUDGET_CONFIG_VERSION,
    FULL_INPUT_CAPACITY_CONFIG_VERSION,
    FULL_INPUT_CAPACITY_MAX_WIRE_BYTES,
    ISOLATED_CAPACITY_CONFIG_VERSIONS,
)
from nexus.agents.committee import (
    DAILY_PENDING_SCOPE_PROMPT_VERSIONS,
    DISCLOSURE_SCOPE_PROMPT_VERSIONS,
    INDEPENDENT_MONITOR_COMPACT_PROMPT_VERSION,
    MONITORING_SCOPE_PROMPT_VERSIONS,
    ROLE_ORDER,
    AgentRole,
    CommitteeRoleOutput,
)
from nexus.agents.models import ResearchRequest
from nexus.domain.evidence import EvidenceRef
from nexus.monitoring.contracts import (
    PriceMonitoringProposal,
    PriceMonitoringReview,
    PriceReviewCondition,
)
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.models.runtime import Job
from nexus.strategy.intents import ResearchClaim


def capacity_config() -> dict[str, str | int | bool]:
    """New frozen identity only; constructing it does not register a model."""
    from nexus.agents.codex_formal_stage import formal_stage_config

    config: dict[str, str | int | bool] = dict(formal_stage_config())
    config.update(
        version=FULL_INPUT_CAPACITY_CONFIG_VERSION,
        max_wire_bytes=FULL_INPUT_CAPACITY_MAX_WIRE_BYTES,
        financial_table_projection="lossless_if_smaller",
        stage_prompt_projection="current_stage_content",
        complete_pool_preflight=True,
    )
    return config


def capacity_config_hash() -> str:
    return hashlib.sha256(json.dumps(capacity_config(), sort_keys=True).encode()).hexdigest()


def capacity_model_matches(
    model: AgentModelConfigRecord,
    *,
    flexible_stage_budget: bool = False,
    bounded_stage_budget: bool = False,
) -> bool:
    if (
        type(flexible_stage_budget) is not bool
        or type(bounded_stage_budget) is not bool
        or (flexible_stage_budget and bounded_stage_budget)
    ):
        return False
    from nexus.agents.codex_bounded_budget import bounded_stage_config, bounded_stage_config_hash
    from nexus.agents.codex_flexible_budget import flexible_stage_config, flexible_stage_config_hash

    config = (
        bounded_stage_config()
        if bounded_stage_budget
        else flexible_stage_config()
        if flexible_stage_budget
        else capacity_config()
    )
    config_hash = (
        bounded_stage_config_hash()
        if bounded_stage_budget
        else flexible_stage_config_hash()
        if flexible_stage_budget
        else capacity_config_hash()
    )
    return (
        model.version == config["version"]
        and model.config_hash == config_hash
        and model.model_id == config["model_id"]
        and model.max_input_tokens == 96000
        and model.max_output_tokens == 8192
        and model.max_tool_calls == 0
        and model.input_usd_per_million == 0
        and model.output_usd_per_million == 0
        and model.pricing_available_at
        == datetime.fromisoformat(str(config["pricing_available_at"]))
        and model.pricing_expires_at == datetime.fromisoformat(str(config["pricing_expires_at"]))
    )


def isolated_capacity_model_matches(model: AgentModelConfigRecord) -> bool:
    return capacity_model_matches(
        model,
        flexible_stage_budget=model.version == FLEXIBLE_STAGE_BUDGET_CONFIG_VERSION,
        bounded_stage_budget=model.version == BOUNDED_STAGE_BUDGET_CONFIG_VERSION,
    )


async def capacity_job_qualified(session: AsyncSession, job: Job, *, claimed: bool = False) -> bool:
    """Exact isolated first-claim authority; pool binding is still required by analyze."""
    from nexus.agents.codex_candidate import _candidate_job_hash, require_candidate_database
    from nexus.jobs.models import JobType, QueueName, RetryPolicy

    await require_candidate_database(session)
    if (
        type(claimed) is not bool
        or job.job_type != JobType.AGENT_PAPER_RESEARCH.value
        or job.queue != QueueName.GENERAL.value
        or job.schema_version != "agent_research_job_v1"
        or job.retry_policy != RetryPolicy.AUTOMATIC_SAFE.value
        or job.max_attempts != 1
        or job.status != ("running" if claimed else "queued")
        or job.attempt_count != (1 if claimed else 0)
        or job.manual_retry_count != 0
        or set(job.payload) != {"run_id"}
        or type(job.payload.get("run_id")) is not str
    ):
        return False
    try:
        run_id = UUID(cast(str, job.payload["run_id"]))
    except ValueError:
        return False
    run = await session.get(
        AgentResearchRunRecord, run_id, with_for_update=True, populate_existing=True
    )
    if run is None or run.status != "prepared" or run.retry_count != 0:
        return False
    model = await session.get(AgentModelConfigRecord, run.model_config_id, populate_existing=True)
    now = await session.scalar(select(func.clock_timestamp()))
    return (
        model is not None
        and isinstance(now, datetime)
        and isolated_capacity_model_matches(model)
        and model.created_at <= now
        and model.pricing_available_at <= now < model.pricing_expires_at
        and run.model_hash == model.config_hash
        and run.pricing_version == model.version
        and job.scope_kind == "instrument"
        and job.scope_id == str(run.instrument_id)
        and job.effective_at == run.decision_time
        and job.input_hash == _candidate_job_hash(run)
    )


def _monitoring_capacity_view(
    committee: StagedCodexCommittee, evidence: tuple[EvidenceRef, ...], inputs: dict[str, object]
) -> StagedCodexCommittee:
    if committee._prompts["portfolio"].version not in MONITORING_SCOPE_PROMPT_VERSIONS:
        return committee
    compact = committee._prompts["portfolio"].version == INDEPENDENT_MONITOR_COMPACT_PROMPT_VERSION
    # Only the v2 input codec has a four-byte bound for every accepted prose scalar.
    # Static deduplication never observes these variable prior/model strings.
    widest_text = "𠀀" if compact else "\x00"
    for model in (_MonitoringProposal, _MonitoringReview):
        properties = model.model_json_schema()["properties"]
        if (
            properties["rationale"].get("maxLength") != 500
            or properties["citations"].get("maxItems") != 4
        ):
            raise ValueError("WORKLOAD_RUN_AUTHORITY_INVALID")
    if PriceReviewCondition.model_json_schema()["properties"]["threshold"].get("maxLength") != 40:
        raise ValueError("WORKLOAD_RUN_AUTHORITY_INVALID")
    # Bound normalized domain values, not raw model datetime spellings. Price
    # allows 38 digits plus a decimal point; Unicode decimal digits can occupy
    # four UTF-8 bytes each. All rationale characters need at most six JSON bytes.
    widest_reference = max(evidence, key=evidence.index)
    proposal = PriceMonitoringProposal(
        condition=PriceReviewCondition(
            signal="schwab.l1.bid_ask_midpoint",
            operator="gte",
            threshold="9" + "𝟏" * 25 + "." + "𝟏" * 12,
            unit="currency",
            currency="USD",
            purpose="review",
            source="schwab",
            feed="l1",
            price_basis="unadjusted_bid_ask_midpoint",
            max_age_seconds=60,
        ),
        rationale=widest_text * 500,
        evidence_refs=(widest_reference,) * 4,
        review_due_at=datetime.max.replace(tzinfo=UTC),
        valid_until=datetime.max.replace(tzinfo=UTC),
    )
    review = PriceMonitoringReview(
        proposal_hash=proposal.content_hash,
        decision="accepted",
        rationale=widest_text * 500,
        evidence_refs=proposal.evidence_refs,
    )
    view = copy(committee)
    monitor_roles: tuple[AgentRole, ...] = ("market_state", "bear", "evidence_audit")
    view._outputs = {
        role: CommitteeRoleOutput(
            role=role,
            vote="support",
            claims=(ResearchClaim(text="capacity boundary", evidence_refs=(evidence[-1],)),),
            prompt_hash=committee._prompts[role].content_hash,
            input_tokens=0,
            output_tokens=0,
            monitoring_proposal=proposal if role == "market_state" else None,
            monitoring_review=review if role != "market_state" else None,
        )
        for role in monitor_roles
    }
    if committee._prompts["portfolio"].version in DAILY_PENDING_SCOPE_PROMPT_VERSIONS:
        from nexus.monitoring.daily_contracts import (
            DailyPendingItem,
            DailyPendingProposal,
            DailyPendingReview,
            daily_context_from_inputs,
            daily_context_identity,
        )

        context = daily_context_from_inputs(inputs)
        if context is not None:
            context_hash, trigger_ids = daily_context_identity(context)
            props = _DailyPendingItem.model_json_schema()["properties"]
            if (
                props["rationale"].get("maxLength") != 160
                or props["citations"].get("maxItems") != 3
                or _DailyPendingProposal.model_json_schema()["properties"]["items"].get("maxItems")
                != 32
                or _DailyPendingReview.model_json_schema()["properties"]["rationale"].get(
                    "maxLength"
                )
                != 500
                or _DailyPendingReview.model_json_schema()["properties"]["citations"].get(
                    "maxItems"
                )
                != 4
            ):
                raise ValueError("WORKLOAD_RUN_AUTHORITY_INVALID")
            daily = DailyPendingProposal(
                context_hash=context_hash,
                items=tuple(
                    DailyPendingItem(
                        trigger_id=trigger_id,
                        status="unresolved",
                        rationale=widest_text * 160,
                        evidence_refs=(widest_reference,) * 3,
                    )
                    for trigger_id in trigger_ids
                ),
            )
            view._outputs["orchestrator"] = CommitteeRoleOutput(
                role="orchestrator",
                vote="support",
                claims=(ResearchClaim(text="capacity boundary", evidence_refs=(evidence[-1],)),),
                prompt_hash=committee._prompts["orchestrator"].content_hash,
                input_tokens=0,
                output_tokens=0,
                daily_pending_proposal=daily,
            )
            daily_review = DailyPendingReview(
                proposal_hash=daily.content_hash,
                decision="accepted",
                rationale=widest_text * 500,
                evidence_refs=(widest_reference,) * 4,
            )
            for reviewer in ("bear", "evidence_audit"):
                view._outputs[reviewer] = view._outputs[reviewer].model_copy(
                    update={"daily_pending_review": daily_review}
                )
    if committee._prompts["portfolio"].version in DISCLOSURE_SCOPE_PROMPT_VERSIONS:
        from nexus.monitoring.contracts import DisclosureReviewCondition
        from nexus.monitoring.disclosure_contracts import (
            DisclosureMonitoringProposal,
            DisclosureMonitoringReview,
        )

        # A schema superset only, never research evidence or a usable baseline.
        maximal = DisclosureMonitoringProposal(
            condition=DisclosureReviewCondition(
                signal="sec.issuer_disclosure_added",
                operator="exists_new",
                threshold=True,
                unit="filing",
                purpose="review",
                source="sec",
                forms=("8-K", "8-K/A"),
                max_age_seconds=300,
                time_authority="frozen_index_vs_local_receipt",
            ),
            baseline_snapshot_id=UUID(int=(1 << 128) - 1),
            baseline_hash="f" * 64,
            issuer_cik="9" * 10,
            baseline_as_of=datetime.max.replace(tzinfo=UTC),
            rationale=widest_text * 160,
            evidence_refs=(widest_reference,) * 3,
            review_due_at=datetime.max.replace(tzinfo=UTC),
            valid_until=datetime.max.replace(tzinfo=UTC),
        )
        disclosure_review = DisclosureMonitoringReview(
            proposal_hash=maximal.content_hash,
            decision="accepted",
            rationale=widest_text * 160,
            evidence_refs=maximal.evidence_refs,
        )
        proposal_possible = True
        if compact:
            from nexus.monitoring.disclosure_contracts import (
                baseline_fields,
                disclosure_from_inputs,
            )

            try:
                baseline_fields(disclosure_from_inputs(inputs))
            except ValueError:
                # The real research output receiver rejects non-null proposals
                # without this frozen baseline before a later stage can see them.
                proposal_possible = False
        view._outputs["market_state"] = view._outputs["market_state"].model_copy(
            update={"disclosure_proposal": maximal if proposal_possible else None}
        )
        for role in ("bear", "evidence_audit"):
            view._outputs[role] = view._outputs[role].model_copy(
                update={"disclosure_review": disclosure_review}
            )
    from nexus.monitoring.mixed_contracts import (
        MixedEventItem,
        MixedEventProposal,
        MixedEventReview,
        mixed_context_from_inputs,
    )

    mixed_context = mixed_context_from_inputs(inputs)
    if mixed_context is not None:
        from nexus.agents.codex_staged import (
            _MixedEventItem,
            _MixedEventProposal,
            _MixedEventReview,
        )

        for domain, wire in (
            (MixedEventItem, _MixedEventItem),
            (MixedEventReview, _MixedEventReview),
        ):
            domain_props, wire_props = (
                domain.model_json_schema()["properties"],
                wire.model_json_schema()["properties"],
            )
            if (
                domain_props["rationale"].get("maxLength") != 160
                or wire_props["rationale"].get("maxLength") != 160
                or wire_props["citations"].get("maxItems") != 3
                or domain_props["evidence_refs"].get("maxItems") != 3
            ):
                raise ValueError("WORKLOAD_RUN_AUTHORITY_INVALID")
        if _MixedEventProposal.model_json_schema()["properties"]["items"].get("maxItems") != 32:
            raise ValueError("WORKLOAD_RUN_AUTHORITY_INVALID")
        context_ref = next(ref for ref in evidence if ref.source == "nexus_monitor_increment_v2")
        # Exact immutable identity values; only bounded model prose/citations vary.
        context_checks = mixed_context.get("trigger_checks")
        if not isinstance(context_checks, list):
            raise ValueError("WORKLOAD_RUN_AUTHORITY_INVALID")
        materials = mixed_context.get("materials")
        if not isinstance(materials, dict):
            raise ValueError("WORKLOAD_RUN_AUTHORITY_INVALID")
        bodies = {
            f"sec:{row['archive']['issuer_cik']}:"
            f"{row['archive']['entry']['accession_number']}": UUID(row["snapshot_id"])
            for row in materials["contents"]
        }
        mixed_refs = (context_ref, widest_reference, widest_reference)
        # Mixed-event prose is not encoded by input v2: retain six-byte JSON bounds.
        mixed = MixedEventProposal(
            context_hash=context_ref.content_hash,
            items=tuple(
                MixedEventItem(
                    trigger_id=row["trigger_id"],
                    kind=row["kind"],
                    event_key=row["event_key"],
                    status="unresolved",
                    rationale="\x00" * 160,
                    content_snapshot_id=bodies[row["event_key"]]
                    if row["kind"] == "disclosure"
                    else None,
                    evidence_refs=mixed_refs,
                )
                for row in context_checks
            ),
        )
        mixed_review = MixedEventReview(
            proposal_hash=mixed.content_hash,
            decision="accepted",
            rationale="\x00" * 160,
            evidence_refs=mixed_refs,
        )
        view._outputs["market_state"] = view._outputs["market_state"].model_copy(
            update={"mixed_event_proposal": mixed}
        )
        mixed_roles: tuple[AgentRole, ...] = ("bear", "evidence_audit")
        for role in mixed_roles:
            view._outputs[role] = view._outputs[role].model_copy(
                update={"mixed_event_review": mixed_review}
            )
    return view


def candidate_capacity_envelope(
    committee: StagedCodexCommittee,
    *,
    request: ResearchRequest,
    evidence_refs: tuple[EvidenceRef, ...],
    inputs: dict[str, object],
) -> dict[str, object]:
    """Bound wire shapes for these frozen inputs, not model success or SDK usage.

    Valid Unicode characters need at most six UTF-8 JSON bytes. Fixed role/ID
    fields, maximal rows, seven-character votes and widest valid citations form
    a schema superset. Frozen-input projection choices are independent of prior
    text. Source rows are frozen and prior rows have fixed keys and field order.
    Column savings therefore depend on row count, not text or citation values.
    The maximal rows also bound every smaller row-count branch, including raw
    fallback. Bound the selected lossless wire, with its decoding instructions,
    rather than an expanded representation that will not be sent.
    No output-token reachability assumption is needed for this conservative set.
    """
    if not evidence_refs:
        raise ValueError("WORKLOAD_RUN_AUTHORITY_INVALID")
    committee = _monitoring_capacity_view(committee, evidence_refs, inputs)
    # This proof is for the declared current SDK schema. Schema evolution must
    # explicitly update the envelope rather than silently keeping old maxima.
    claim_properties = _Claim.model_json_schema()["properties"]
    role_properties = _Role.model_json_schema()["properties"]
    if (
        claim_properties["text"].get("maxLength") != 400
        or claim_properties["citations"].get("maxItems") != 3
        or role_properties["claims"].get("maxItems") != 2
        or max(map(len, role_properties["vote"]["enum"])) != 7
        or set(role_properties["vote"]["enum"]) != {"support", "oppose", "abstain"}
        or max(
            len(json.dumps(vote, ensure_ascii=False).encode())
            for vote in role_properties["vote"]["enum"]
        )
        != 9
    ):
        raise ValueError("WORKLOAD_RUN_AUTHORITY_INVALID")
    stages = []
    widest_text = (
        "𠀀"
        if committee._prompts["portfolio"].version == INDEPENDENT_MONITOR_COMPACT_PROMPT_VERSION
        else "\x00"
    )
    for stage, roles in STAGES:
        prior_roles = ROLE_ORDER[: ROLE_ORDER.index(roles[0])]
        previous: list[dict[str, object]] = [
            {
                "claim_id": f"{role}:{index}",
                "role": role,
                "vote": "abstain",
                "text": widest_text * 400,
                "citations": [len(evidence_refs) - 1] * 3,
            }
            for role in prior_roles
            for index in range(2)
        ]
        payload, output_model = committee.build_stage_request(
            stage=stage,
            request=request,
            evidence_refs=evidence_refs,
            frozen_inputs=inputs,
            previous=previous,
            financial_table_candidate=True,
        )
        claims_seen = 0

        def check_stage_schema(value: object) -> None:
            nonlocal claims_seen
            if isinstance(value, dict):
                properties = value.get("properties")
                if isinstance(properties, dict) and "claims" in properties:
                    claims_seen += 1
                    if properties["claims"].get("maxItems") != 2:
                        raise ValueError("WORKLOAD_RUN_AUTHORITY_INVALID")
                if (
                    isinstance(properties, dict)
                    and "citations" in properties
                    and "text" in properties
                ):
                    if (
                        properties["citations"].get("maxItems") != 3
                        or properties.get("text", {}).get("maxLength") != 400
                    ):
                        raise ValueError("WORKLOAD_RUN_AUTHORITY_INVALID")
                for child in value.values():
                    check_stage_schema(child)
            elif isinstance(value, list):
                for child in value:
                    check_stage_schema(child)

        check_stage_schema(output_model.model_json_schema())
        if not claims_seen:
            raise ValueError("WORKLOAD_RUN_AUTHORITY_INVALID")
        wire_upper = len(codex_request_wire(payload).encode())
        sample = {
            "wire_bytes": len(codex_request_wire(payload).encode()),
            "input_allowance_units": codex_input_allowance(
                codex_request_wire(payload), codex_output_schema(output_model)
            ),
        }
        if wire_upper < cast(int, sample["wire_bytes"]):
            raise ValueError("WORKLOAD_RUN_AUTHORITY_INVALID")
        schema_bytes = len(
            json.dumps(
                codex_output_schema(output_model), ensure_ascii=False, separators=(",", ":")
            ).encode()
        )
        # Byte fallback covers token counting, including shapes not represented
        # by the control-character sample. Do not call that sample a token upper.
        allowance_upper = (
            wire_upper
            + schema_bytes
            + len(BASE_INSTRUCTIONS.encode())
            + len(DEVELOPER_INSTRUCTIONS.encode())
            + INPUT_RESERVE
        )
        stages.append(
            {
                "stage": stage,
                "prior_claims_max": len(previous),
                "wire_upper_bytes": wire_upper,
                "wire_limit_bytes": committee._max_wire_bytes,
                "wire_envelope_status": "within_limit"
                if wire_upper <= committee._max_wire_bytes
                else "exceeded",
                "schema_superset_sample_wire_bytes": sample["wire_bytes"],
                "schema_superset_sample_allowance_units": sample["input_allowance_units"],
                "conservative_allowance_upper_units": allowance_upper,
                "single_call_preflight_limit_units": MAX_INPUT_TOKENS,
                "conservative_preflight_status": "within_limit"
                if allowance_upper <= MAX_INPUT_TOKENS
                else "not_proven_within_limit",
                "previous_actual_input_threshold_for_sample": (
                    committee._aggregate_input_tokens - cast(int, sample["input_allowance_units"])
                    if cast(int, sample["input_allowance_units"])
                    <= committee._aggregate_input_tokens
                    else None
                ),
                "previous_actual_input_threshold_for_conservative_envelope": (
                    committee._aggregate_input_tokens - allowance_upper
                    if allowance_upper <= committee._aggregate_input_tokens
                    else None
                ),
            }
        )
    return {
        "scope": "current_frozen_inputs",
        "activated": False,
        "model_calls": 0,
        "joint_output_admissibility_proven": False,
        "aggregate_sdk_input_completion_proven": False,
        "wire_upper_bytes": max(cast(int, row["wire_upper_bytes"]) for row in stages),
        "stages": stages,
    }


@dataclass(frozen=True, slots=True)
class CapacityPoolMember:
    committee: StagedCodexCommittee
    request: ResearchRequest
    evidence_refs: tuple[EvidenceRef, ...]


def qualify_capacity_input(
    committee: StagedCodexCommittee,
    *,
    request: ResearchRequest,
    evidence_refs: tuple[EvidenceRef, ...],
    inputs: dict[str, object],
) -> dict[str, object]:
    """Check one frozen input with the runner's gates; grant no pool/send authority."""
    envelope = candidate_capacity_envelope(
        committee, request=request, evidence_refs=evidence_refs, inputs=inputs
    )
    if cast(int, envelope["wire_upper_bytes"]) > committee._max_wire_bytes:
        raise ValueError("CODEX_CAPACITY_POOL_WIRE_EXCEEDED")
    payload, model = committee.build_stage_request(
        stage="research",
        request=request,
        evidence_refs=evidence_refs,
        frozen_inputs=inputs,
        previous=[],
    )
    allowance = codex_input_allowance(codex_request_wire(payload), codex_output_schema(model))
    if allowance > MAX_INPUT_TOKENS or allowance > committee._aggregate_input_tokens:
        raise ValueError("CODEX_CAPACITY_POOL_INPUT_EXCEEDED")
    return {
        "instrument_id": str(request.instrument_id),
        "snapshot_id": str(request.data_snapshot_id),
        "request_hash": codex_request_hash(payload, model),
        "wire_upper_bytes": envelope["wire_upper_bytes"],
        "research_allowance_units": allowance,
    }


async def preflight_capacity_pool(
    members: Sequence[CapacityPoolMember], *, expected_instrument_ids: tuple[UUID, ...]
) -> dict[str, object]:
    """The caller supplies the complete frozen pool authority, never a filtered subset.

    This local qualification entry does not admit Jobs. None of its committees
    can invoke v9 until every member has passed; source/context drift is checked
    again against the exact compiled research request before the SDK boundary.
    Token completion remains conditional on actual usage at each stage.
    """
    members = tuple(members)
    if (
        not 1 <= len(expected_instrument_ids) <= 5
        or len(set(expected_instrument_ids)) != len(expected_instrument_ids)
        or tuple(member.request.instrument_id for member in members) != expected_instrument_ids
        or len({id(member.committee) for member in members}) != len(members)
        or len({member.request.decision_time for member in members}) != 1
        or any(
            member.committee._stage_budget_version not in ISOLATED_CAPACITY_CONFIG_VERSIONS
            or member.committee._capacity_pool_manifest is not None
            or member.committee._attempted
            for member in members
        )
    ):
        raise ValueError("CODEX_CAPACITY_POOL_BINDING_INVALID")
    profiles = {member.committee._stage_budget_version for member in members}
    if len(profiles) != 1:
        raise ValueError("CODEX_CAPACITY_POOL_BINDING_INVALID")
    profile = next(iter(profiles))
    pending = []
    rows = []
    for member in members:
        committee, request, refs = member.committee, member.request, member.evidence_refs
        inputs = await committee._inputs(request)
        row = qualify_capacity_input(committee, request=request, evidence_refs=refs, inputs=inputs)
        request_hash = cast(str, row["request_hash"])
        pending.append((member, request_hash))
        rows.append(row)
    manifest = hashlib.sha256(
        json.dumps(
            {
                "profile": profile,
                "members": rows,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    # Async input reads let another qualification finish or start research.
    # Recheck all members, then bind synchronously with no intervening await.
    if any(
        member.committee._capacity_pool_manifest is not None
        or member.committee._attempted
        or member.committee._stage_budget_version != profile
        or member.committee._max_wire_bytes != FULL_INPUT_CAPACITY_MAX_WIRE_BYTES
        or member.committee._aggregate_input_tokens != 96000
        or member.committee._inline_citations is not True
        or member.committee._paper_stage_scope is not True
        or member.committee._stage_time_budget is not True
        or member.committee.structured_synthesis is not True
        for member, _ in pending
    ):
        raise ValueError("CODEX_CAPACITY_POOL_BINDING_INVALID")
    for member, request_hash in pending:
        member.committee._capacity_pool_manifest = manifest
        member.committee._capacity_pool_request = member.request
        member.committee._capacity_pool_evidence = member.evidence_refs
        member.committee._capacity_research_hash = request_hash
    return {
        "candidate_count": len(members),
        "manifest_hash": manifest,
        "model_calls": 0,
        "aggregate_completion_proven": False,
        "registered": False,
        "members": rows,
    }
