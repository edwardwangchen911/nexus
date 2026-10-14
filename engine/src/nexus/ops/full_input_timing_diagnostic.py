"""One private, read-only-source timing observation; never a business Run or retry."""

import argparse
import asyncio
import json
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID, uuid4

from sqlalchemy import text

from nexus.agents.capacity_cycle import capacity_output_mode_qualified, derive_cycle_members
from nexus.agents.codex_capacity import CapacityPoolMember, preflight_capacity_pool
from nexus.agents.codex_staged import StructuredStagedCodexCommittee
from nexus.agents.codex_versions import FULL_INPUT_CAPACITY_CONFIG_VERSION
from nexus.agents.committee import ROLE_ORDER, AgentRole, PromptAsset
from nexus.agents.execution_authority import load_frozen_evidence
from nexus.agents.models import ResearchRequest
from nexus.agents.paper_context import (
    FrozenAgentPaperContextStatus,
    read_agent_paper_research_context,
    revalidate_frozen_agent_paper_context,
)
from nexus.agents.synthesis import domain_evidence
from nexus.domain.evidence import EvidenceRef
from nexus.ops.agent_paper_once import _validate_local_database
from nexus.ops.openai_local import local_settings
from nexus.ops.research_budget_diagnostic import _private_directory, _receipt
from nexus.ops.research_workload_inspection import _inspection_committee
from nexus.paper.agent_cycle import _events, _fact_uuids
from nexus.paper.agent_cycle_models import CandidateManifestEntry
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.models.paper import AgentPaperCycleEventRecord, AgentPaperCycleRecord
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings

PROFILE = "full-input-timing-diagnostic-v1"


@dataclass(frozen=True)
class TimingCase:
    committee: StructuredStagedCodexCommittee
    request: ResearchRequest
    evidence: tuple[EvidenceRef, ...]
    prompts: dict[AgentRole, PromptAsset]
    summary: dict[str, object]


def validate_source_pool(
    cycle: AgentPaperCycleRecord,
    events: tuple[AgentPaperCycleEventRecord, ...],
    runs: Sequence[AgentResearchRunRecord],
    source: AgentResearchRunRecord,
) -> tuple[tuple[CandidateManifestEntry, AgentResearchRunRecord], ...]:
    paired = derive_cycle_members(cycle, events, runs)
    if source.id not in {run.id for _, run in paired} or any(
        run.status not in {"failed", "completed", "abstained"}
        or run.model_config_id != source.model_config_id
        or run.model_hash != source.model_hash
        or run.pricing_version != FULL_INPUT_CAPACITY_CONFIG_VERSION
        for _, run in paired
    ):
        raise ValueError("TIMING_DIAGNOSTIC_SOURCE_INVALID")
    return paired


async def load_case(settings: EngineSettings, run_id: UUID, *, deadline: float) -> TimingCase:
    """Compile the complete historical pool from exact cycle events in a read-only transaction."""
    _validate_local_database(settings.database_url)
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            source = await session.get(AgentResearchRunRecord, run_id)
            if source is None or source.status not in {"failed", "completed", "abstained"}:
                raise ValueError("TIMING_DIAGNOSTIC_SOURCE_INVALID")
            context = await read_agent_paper_research_context(
                session,
                snapshot_id=source.data_snapshot_id,
                instrument_id=source.instrument_id,
                as_of=source.decision_time,
            )
            if context is None or not await capacity_output_mode_qualified(
                session, source, formal=True
            ):
                raise ValueError("TIMING_DIAGNOSTIC_SOURCE_INVALID")
            cycle = await session.get(AgentPaperCycleRecord, context[0].cycle_id)
            if cycle is None:
                raise ValueError("TIMING_DIAGNOSTIC_SOURCE_INVALID")
            events = await _events(session, cycle.id)
            runs = []
            for member_id in _fact_uuids(events, "research_run_ids"):
                run = await session.get(AgentResearchRunRecord, member_id)
                if run is None:
                    raise ValueError("TIMING_DIAGNOSTIC_SOURCE_INVALID")
                runs.append(run)
            paired = validate_source_pool(cycle, events, runs, source)
            members = []
            selected = None
            for row, run in paired:
                member_id = run.id
                model = (
                    None
                    if run is None
                    else await session.get(AgentModelConfigRecord, run.model_config_id)
                )
                if (
                    run is None
                    or model is None
                    or model.config_hash != run.model_hash
                    or model.version != FULL_INPUT_CAPACITY_CONFIG_VERSION
                ):
                    raise ValueError("TIMING_DIAGNOSTIC_SOURCE_INVALID")
                original = await _inspection_committee(session, settings, run, model=model)
                if not isinstance(original, StructuredStagedCodexCommittee):
                    raise ValueError("TIMING_DIAGNOSTIC_SOURCE_INVALID")
                committee = StructuredStagedCodexCommittee(
                    prompts=original._prompts,
                    tools=original._tools,
                    deadline=deadline,
                    aggregate_input_tokens=96000,
                    max_wire_bytes=100479,
                    inline_citations=True,
                    paper_stage_scope=True,
                    stage_time_budget=True,
                    stage_budget_version=FULL_INPUT_CAPACITY_CONFIG_VERSION,
                    diagnostic_profile=PROFILE,
                )
                evidence = await load_frozen_evidence(session, run, allow_fixture_evidence=False)
                bound = await read_agent_paper_research_context(
                    session,
                    snapshot_id=run.data_snapshot_id,
                    instrument_id=run.instrument_id,
                    as_of=run.decision_time,
                )
                if (
                    bound is None
                    or bound[0].cycle_id != cycle.id
                    or (
                        await revalidate_frozen_agent_paper_context(
                            session, run=run, evidence_records=evidence
                        )
                        is not FrozenAgentPaperContextStatus.VALID
                    )
                ):
                    raise ValueError("TIMING_DIAGNOSTIC_SOURCE_INVALID")
                refs = tuple(domain_evidence(record) for record in evidence)
                request = ResearchRequest(
                    instrument_id=run.instrument_id,
                    data_snapshot_id=run.data_snapshot_id,
                    decision_time=run.decision_time,
                )
                members.append(CapacityPoolMember(committee, request, refs))
                if member_id == run_id:
                    selected = (committee, request, refs)
            await preflight_capacity_pool(
                members, expected_instrument_ids=tuple(row.instrument_id for row, _ in paired)
            )
            if selected is None:
                raise ValueError("TIMING_DIAGNOSTIC_SOURCE_INVALID")
            committee, request, refs = selected
            # Inputs are read only again during analyze; this session must stay alive.
            inputs = await committee._inputs(request)

            async def frozen_inputs(request: ResearchRequest) -> dict[str, object]:
                return inputs

            committee._inputs = frozen_inputs  # type: ignore[method-assign]
            return TimingCase(
                committee,
                request,
                refs,
                dict(committee._prompts),
                {
                    "candidate_count": len(members),
                    "source_profile": FULL_INPUT_CAPACITY_CONFIG_VERSION,
                    "provenance": "current_code_frozen_input_reconstruction",
                    "original_invocation_reconstructed": False,
                },
            )
    finally:
        await factory.kw["bind"].dispose()


async def execute_timing_diagnostic(
    settings: EngineSettings, *, run_id: UUID, output_dir: Path
) -> dict[str, object]:
    _private_directory(output_dir.parent)
    try:
        output_dir.mkdir(mode=0o700)
    except FileExistsError:
        raise ValueError("TIMING_DIAGNOSTIC_ALREADY_ATTEMPTED") from None
    identity = str(uuid4())
    _receipt(
        output_dir / "attempt.json",
        {
            "diagnostic_id": identity,
            "profile": PROFILE,
            "source_run_id": str(run_id),
            "retry_allowed": False,
        },
    )
    deadline = asyncio.get_running_loop().time() + 300
    case = None
    result: dict[str, object] = {
        "diagnostic_id": identity,
        "profile": PROFILE,
        "status": "failed",
        "business_writes": 0,
        "formal_acceptance": False,
        "execution_locked": True,
        "retry_allowed": False,
        "observation_limit_seconds": 300,
    }
    try:
        async with asyncio.timeout_at(deadline):
            case = await load_case(settings, run_id, deadline=deadline)
            result.update(case.summary)
            for role in ROLE_ORDER:
                await case.committee.analyze(role, case.request, case.evidence, case.prompts[role])
            result["status"] = "observed"
    except Exception:
        # A failed or unknown attempt is terminal; no raw exception or content leaves this entry.
        result["code"] = "TIMING_DIAGNOSTIC_NOT_COMPLETED"
    if case is not None:
        result.update(
            model_calls=case.committee.usage.provider_calls,
            known_input_tokens=case.committee.usage.input_tokens,
            known_output_tokens=case.committee.usage.output_tokens,
            usage_complete=result["status"] == "observed",
            stage_timings={
                stage: value.model_dump(mode="json")
                for stage, value in case.committee.stage_timings.items()
            },
            failure_diagnostic=None
            if case.committee.failure_diagnostic is None
            else case.committee.failure_diagnostic.model_dump(
                mode="json", exclude={"response_hash"}
            ),
        )
    _receipt(output_dir / "result.json", result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--source-run-id", type=UUID, required=True)
    args = parser.parse_args()
    try:
        root = args.repo_root.resolve()
        _private_directory(root / ".secrets")
        parent = root / ".secrets/nexus-full-input-timing"
        if not parent.exists():
            parent.mkdir(mode=0o700)
        _private_directory(parent)
        result = asyncio.run(
            execute_timing_diagnostic(
                local_settings(root, backend="codex_sdk"),
                run_id=args.source_run_id,
                output_dir=parent / str(args.source_run_id),
            )
        )
        # Native identities remain only in private receipts.
        print(
            json.dumps(
                {
                    key: result[key]
                    for key in (
                        "status",
                        "profile",
                        "business_writes",
                        "formal_acceptance",
                        "retry_allowed",
                    )
                }
            )
        )
        return 0 if result["status"] == "observed" else 1
    except Exception:
        print(json.dumps({"status": "blocked", "code": "TIMING_DIAGNOSTIC_ENTRY_INVALID"}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
