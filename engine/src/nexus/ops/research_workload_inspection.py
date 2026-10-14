"""Read one existing Run's frozen input workload, without a provider, Job or model call."""

import argparse
import asyncio
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Literal, cast
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_capacity import candidate_capacity_envelope as candidate_capacity_envelope
from nexus.agents.codex_sdk import (
    codex_input_allowance,
    codex_output_schema,
    codex_request_hash,
    codex_request_wire,
)
from nexus.agents.codex_staged import StagedCodexCommittee
from nexus.agents.codex_versions import (
    FLEXIBLE_STAGE_BUDGET_CONFIG_VERSION,
    FORMAL_RESEARCH_HANDLER_TIMEOUT_SECONDS,
    FORMAL_STAGE_CAPACITY_CONFIG_VERSION,
    FULL_INPUT_CAPACITY_CONFIG_VERSION,
)
from nexus.agents.committee import ROLE_ORDER, CommitteeRoleOutput
from nexus.agents.execution import CommitteeModel
from nexus.agents.execution_authority import load_frozen_evidence
from nexus.agents.models import ResearchRequest
from nexus.agents.synthesis import domain_evidence
from nexus.api.research_stages import validate_stage_trace
from nexus.domain.evidence import EvidenceRef
from nexus.ops.openai_local import local_settings
from nexus.ops.research_budget_diagnostic import read_database_url
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from nexus.worker import _committee


def summarize_stage_request(
    payload: dict[str, object], output_model: type[BaseModel], *, max_wire_bytes: int | None = None
) -> dict[str, object]:
    wire = codex_request_wire(payload)
    schema = codex_output_schema(output_model)
    return {
        "wire_bytes": len(wire.encode()),
        "schema_bytes": len(
            json.dumps(schema, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
        ),
        "schema_encoding": "canonical_json_utf8",
        "input_allowance_units": codex_input_allowance(wire, schema),
        "server_token_count": None,
        "request_hash": codex_request_hash(payload, output_model),
        **(
            {
                "wire_limit_bytes": max_wire_bytes,
                "wire_limit_status": "exceeded"
                if len(wire.encode()) > max_wire_bytes
                else "within_limit",
            }
            if max_wire_bytes is not None
            else {}
        ),
    }


def summarize_inputs(inputs: dict[str, object]) -> dict[str, object]:
    """Only sizes, digest and context presence; never publish input values."""
    wire = json.dumps(inputs, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    security = inputs.get("security_snapshot")
    items = security.get("items") if isinstance(security, dict) else None
    present: bool | None = None
    if isinstance(items, list) and items and all(isinstance(item, dict) for item in items):
        contexts = [item.get("agent_paper_context") for item in items]
        if all(context is None or isinstance(context, dict) for context in contexts):
            present = any(isinstance(context, dict) for context in contexts)
    return {
        "evidence_kind": "current_code_frozen_input_readback",
        "paper_context_present": present,
        "frozen_inputs_bytes": len(wire),
        "component_bytes": {
            name: len(
                json.dumps(
                    inputs[name], sort_keys=True, ensure_ascii=False, separators=(",", ":")
                ).encode()
            )
            for name in (
                "security_snapshot",
                "filing_disclosures",
                "financial_facts",
                "trailing_valuation",
                "price_close",
                "price_features",
                "macro",
            )
            if name in inputs
        },
        "frozen_inputs_sha256": hashlib.sha256(wire).hexdigest(),
        "model_calls": 0,
        "business_writes": 0,
        "formal_acceptance": False,
        "original_invocation_reconstructed": False,
        "execution_locked": True,
    }


def inspect_critique_request(
    committee: StagedCodexCommittee,
    *,
    run: AgentResearchRunRecord,
    request: ResearchRequest,
    evidence_refs: tuple[EvidenceRef, ...],
    inputs: dict[str, object],
    records: tuple[AgentResearchOutputRecord, ...],
    stage: Literal["research", "critique", "synthesis"] = "critique",
    financial_table_candidate: bool = False,
) -> dict[str, object]:
    unavailable: dict[str, object] = {"status": "unavailable", "code": "PRIOR_RESEARCH_UNVERIFIED"}
    try:
        for record in records:
            digest = hashlib.sha256(
                json.dumps(
                    {
                        "status": record.status,
                        "role": record.role,
                        "payload": record.payload,
                        "intent": record.intent,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                ).encode()
            ).hexdigest()
            if record.run_id != run.id or record.content_hash != digest:
                return unavailable
        # Reconstruct critique from its five preceding research roles. A later
        # synthesis assessment is not an input to that original request.
        prior_roles = (
            () if stage == "research" else ROLE_ORDER[:5] if stage == "critique" else ROLE_ORDER[:7]
        )
        role_records = tuple(record for record in records if record.role in prior_roles)
        if any(
            record.status != "running" or record.intent is not None for record in role_records
        ) or (
            stage != "research"
            and validate_stage_trace(
                run,
                role_records,
                evidence_refs,
                {role: prompt.content_hash for role, prompt in committee._prompts.items()},
            ).status
            != "verified"
        ):
            return unavailable
        research = tuple(record for record in records if record.role in prior_roles)
        outputs = tuple(CommitteeRoleOutput.model_validate(record.payload) for record in research)
        frozen_hash = summarize_inputs(inputs)["frozen_inputs_sha256"]
        if (
            tuple(output.role for output in outputs) != prior_roles
            or any(record.role != output.role for record, output in zip(research, outputs))
            or any(
                output.stage != ("research" if output.role in ROLE_ORDER[:5] else "critique")
                or output.frozen_inputs_hash != frozen_hash
                for output in outputs
            )
            or any(
                output.prompt_hash != committee._prompts[output.role].content_hash
                for output in outputs
            )
            or not evidence_refs
        ):
            return unavailable
        previous: list[dict[str, object]] = [
            {
                "claim_id": f"{output.role}:{index}",
                "role": output.role,
                "vote": output.vote,
                "text": claim.text,
                "citations": [evidence_refs.index(ref) for ref in claim.evidence_refs],
            }
            for output in outputs
            for index, claim in enumerate(output.claims)
        ]
        payload, output_model = committee.build_stage_request(
            stage=stage,
            request=request,
            evidence_refs=evidence_refs,
            frozen_inputs=inputs,
            previous=previous,
            financial_table_candidate=financial_table_candidate,
        )
        recorded_hashes = set()
        for record in records:
            value = record.payload
            if value.get("stage") == stage and value.get("stage_input_hash"):
                recorded_hashes.add(value["stage_input_hash"])
            identity = value.get("stage_input")
            if isinstance(identity, dict) and identity.get("stage") == stage:
                if identity.get("frozen_inputs_hash") != frozen_hash:
                    return unavailable
                recorded_hashes.add(cast(str, identity.get("request_hash")))
        summary = summarize_stage_request(
            payload, output_model, max_wire_bytes=committee._max_wire_bytes
        )
        matches = not financial_table_candidate and recorded_hashes == {summary["request_hash"]}
        return {
            "status": "request_hash_verified" if matches else "current_code_readback_only",
            "prior_roles": len(outputs),
            "prior_claims": len(previous),
            "recorded_request_hash_present": bool(recorded_hashes),
            "original_request_hash_matched": matches,
            "provenance": "persisted_request_hash_match"
            if matches
            else "readonly_candidate_presentation"
            if financial_table_candidate
            else "current_code_reconstruction",
            **summary,
        }
    except (ValueError, TypeError, KeyError, AttributeError):
        return unavailable


async def _inspection_committee(
    session: AsyncSession,
    settings: EngineSettings,
    run: AgentResearchRunRecord,
    *,
    model: AgentModelConfigRecord,
) -> CommitteeModel:
    flexible = model.version == FLEXIBLE_STAGE_BUDGET_CONFIG_VERSION
    formal = model.version in {
        FORMAL_STAGE_CAPACITY_CONFIG_VERSION,
        FULL_INPUT_CAPACITY_CONFIG_VERSION,
    }
    if flexible:
        from nexus.agents.capacity_cycle import capacity_output_mode_qualified

        formal = await capacity_output_mode_qualified(session, run, formal=True)
    # The factory needs the exact frozen profile and a construction deadline.
    # This observation-only deadline is never used to invoke or resume a model.
    return await _committee(
        session,
        settings,
        run,
        responses_client=None,
        tools=None,
        formal_stage_capacity=formal,
        full_input_capacity=flexible and not formal,
        deadline=asyncio.get_running_loop().time() + FORMAL_RESEARCH_HANDLER_TIMEOUT_SECONDS
        if formal or flexible
        else None,
    )


async def inspect_run(
    settings: EngineSettings, *, run_id: UUID, financial_table_candidate: bool = False
) -> dict[str, object]:
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            now = await session.scalar(select(func.clock_timestamp()))
            if not isinstance(now, datetime):
                raise ValueError("WORKLOAD_RUN_AUTHORITY_INVALID")
            run = await session.get(AgentResearchRunRecord, run_id)
            model = (
                None
                if run is None
                else await session.get(AgentModelConfigRecord, run.model_config_id)
            )
            if run is None or model is None or model.config_hash != run.model_hash:
                raise ValueError("WORKLOAD_RUN_AUTHORITY_INVALID")
            committee = await _inspection_committee(session, settings, run, model=model)
            if not isinstance(committee, StagedCodexCommittee):
                raise ValueError("WORKLOAD_RUN_AUTHORITY_INVALID")
            # Reuse the normal input builder only. Never call analyze/invoke/Worker.
            inputs = await committee._inputs(
                ResearchRequest(
                    instrument_id=run.instrument_id,
                    data_snapshot_id=run.data_snapshot_id,
                    decision_time=run.decision_time,
                )
            )
            records = tuple(
                await session.scalars(
                    select(AgentResearchOutputRecord)
                    .where(AgentResearchOutputRecord.run_id == run.id)
                    .order_by(AgentResearchOutputRecord.sequence)
                )
            )
            evidence = await load_frozen_evidence(session, run, allow_fixture_evidence=False)
            refs = tuple(domain_evidence(record) for record in evidence)
            input_summary = summarize_inputs(inputs)
            payload, output_model = committee.build_stage_request(
                stage="research",
                request=ResearchRequest(
                    instrument_id=run.instrument_id,
                    data_snapshot_id=run.data_snapshot_id,
                    decision_time=run.decision_time,
                ),
                evidence_refs=refs,
                frozen_inputs=inputs,
                previous=[],
            )
            research = summarize_stage_request(
                payload, output_model, max_wire_bytes=committee._max_wire_bytes
            )
            recorded_hashes = {
                record.payload.get("stage_input_hash")
                for record in records
                if record.payload.get("stage") == "research"
                and record.payload.get("stage_input_hash")
            }
            prior = tuple(record for record in records if record.role in ROLE_ORDER[:5])
            original_research_matched = (
                len(prior) == len(ROLE_ORDER[:5])
                and validate_stage_trace(
                    run,
                    prior,
                    refs,
                    {role: prompt.content_hash for role, prompt in committee._prompts.items()},
                ).status
                == "verified"
                and all(
                    record.payload.get("frozen_inputs_hash")
                    == input_summary["frozen_inputs_sha256"]
                    for record in prior
                )
                and recorded_hashes == {research["request_hash"]}
            )
            research.update(
                {
                    "original_request_hash_matched": original_research_matched,
                    "recorded_request_hash_present": bool(recorded_hashes),
                    "provenance": "persisted_request_hash_match"
                    if original_research_matched
                    else "current_code_reconstruction",
                }
            )
            critique = inspect_critique_request(
                committee,
                run=run,
                request=ResearchRequest(
                    instrument_id=run.instrument_id,
                    data_snapshot_id=run.data_snapshot_id,
                    decision_time=run.decision_time,
                ),
                evidence_refs=refs,
                inputs=inputs,
                records=records,
            )
            candidate: dict[str, object] = {}
            if financial_table_candidate:
                request = ResearchRequest(
                    instrument_id=run.instrument_id,
                    data_snapshot_id=run.data_snapshot_id,
                    decision_time=run.decision_time,
                )
                candidate_payload, candidate_model = committee.build_stage_request(
                    stage="research",
                    request=request,
                    evidence_refs=refs,
                    frozen_inputs=inputs,
                    previous=[],
                    financial_table_candidate=True,
                )
                candidate = {
                    "candidate_presentation": {
                        "activated": False,
                        "provenance": "readonly_candidate_presentation",
                        "future_claims_capacity_proven": False,
                        "capacity_envelope": candidate_capacity_envelope(
                            committee, request=request, evidence_refs=refs, inputs=inputs
                        ),
                        "research": summarize_stage_request(
                            candidate_payload,
                            candidate_model,
                            max_wire_bytes=committee._max_wire_bytes,
                        ),
                        **{
                            stage: inspect_critique_request(
                                committee,
                                run=run,
                                request=request,
                                evidence_refs=refs,
                                inputs=inputs,
                                records=records,
                                stage=cast(Literal["critique", "synthesis"], stage),
                                financial_table_candidate=True,
                            )
                            for stage in ("critique", "synthesis")
                        },
                    }
                }
            return {
                "status": "workload_observed",
                "observed_at": now.isoformat(),
                "model_version": model.version,
                "run_status": run.status,
                **input_summary,
                "research": research,
                "critique": critique,
                **candidate,
            }
    finally:
        await factory.kw["bind"].dispose()


async def inspect_capacity_cycle(settings: EngineSettings, *, cycle_id: UUID) -> dict[str, object]:
    from nexus.ops.research_capacity_pool import inspect_cycle

    return await inspect_cycle(settings, cycle_id=cycle_id)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, required=True)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--run-id", type=UUID)
    target.add_argument("--capacity-cycle-id", type=UUID)
    parser.add_argument("--database-url-file", type=Path)
    parser.add_argument(
        "--financial-table-candidate",
        action="store_true",
        help="Compare a readonly presentation candidate; never invoke or activate it.",
    )
    args = parser.parse_args()
    if args.capacity_cycle_id is not None and args.financial_table_candidate:
        parser.error("cycle qualification already selects the fixed v9 profile")
    try:
        settings = local_settings(args.repo_root.resolve(), backend="codex_sdk")
        if args.database_url_file is not None:
            settings = settings.model_copy(
                update={"database_url": read_database_url(args.database_url_file)}
            )
        result = asyncio.run(
            inspect_capacity_cycle(settings, cycle_id=args.capacity_cycle_id)
            if args.capacity_cycle_id is not None
            else inspect_run(
                settings,
                run_id=args.run_id,
                financial_table_candidate=args.financial_table_candidate,
            )
        )
    except Exception as error:
        code = str(error)
        if code not in {
            "WORKLOAD_RUN_AUTHORITY_INVALID",
            "CODEX_CAPACITY_POOL_BINDING_INVALID",
            "CODEX_CAPACITY_POOL_WIRE_EXCEEDED",
            "CODEX_CAPACITY_POOL_INPUT_EXCEEDED",
        }:
            code = "WORKLOAD_READBACK_UNAVAILABLE"
        print(json.dumps({"status": "unavailable", "code": code}))
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
