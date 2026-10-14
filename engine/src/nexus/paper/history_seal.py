"""Explicit legacy failure disposition; usage and budget facts remain unknown."""

import hashlib
import json
import os
import re
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Literal, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_formal_stage import formal_preparation_model_matches
from nexus.agents.codex_sdk import MODEL_ID
from nexus.agents.codex_versions import ACTIONABLE_CONFIG_VERSIONS
from nexus.agents.committee import CommitteeRoleOutput, StageFailureDiagnostic, StageInputIdentity
from nexus.audit.canonical import AuditChainInput, audit_event_hash, canonical_audit_event
from nexus.audit.service import append_audit_event
from nexus.jobs.models import job_input_hash
from nexus.paper.agent_cycle_models import CycleStatus, canonical_cycle_event_hash
from nexus.paper.projection import load_paper_projection
from nexus.paper.target_service import (
    lock_portfolio_account,
    require_activation_projection_unoccupied,
)
from nexus.persistence.base import Base
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentModelConfigRecord,
    AgentPortfolioSynthesisRunRecord,
    AgentResearchEventRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.models.paper import AgentPaperCycleEventRecord, AgentPaperCycleRecord
from nexus.persistence.models.runtime import AuditEvent, Job, JobAttempt, JobLease

_ACTION = "paper.legacy_research.sealed"
_SCHEMA = "paper_legacy_seal_v1"
_SAME_DAY_SCHEMA = "paper_same_day_close_v1"
_ACTOR = "local-manual-history-seal"
_BLOCKED = "MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"
_DESKTOP_CHILD = (
    "/Applications/ChatGPT.app/Contents/Resources/codex-cli/CodexCLI.app/Contents/MacOS/codex"
)
_DESKTOP_PARENT = "/Applications/ChatGPT.app/Contents/MacOS/ChatGPT"
_DESKTOP_REPL = "/Applications/ChatGPT.app/Contents/Resources/cua_node/bin/node_repl"
_DESKTOP_NODE = "/Applications/ChatGPT.app/Contents/Resources/cua_node/bin/node"


class HistorySealReceipt(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    status: str = "legacy_research_sealed_usage_unknown"
    sealed_research: int
    replayed: bool
    execution_locked: bool = True


def require_research_processes_quiescent() -> None:
    """Inspect process identity and executable mappings; never argv or credentials."""

    def output(command: list[str]) -> str:
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=5, check=False)
        except (OSError, subprocess.SubprocessError):
            raise ValueError(_BLOCKED) from None
        if result.returncode != 0:
            raise ValueError(_BLOCKED)
        return result.stdout

    def snapshot() -> dict[int, tuple[int, int, str, str]]:
        rows: dict[int, tuple[int, int, str, str]] = {}
        raw = output(["/bin/ps", "-axo", "pid=,ppid=,uid=,lstart=,comm="])
        for line in raw.splitlines():
            parts = line.strip().split(None, 8)
            if len(parts) != 9 or not all(value.isdigit() for value in parts[:3]):
                raise ValueError(_BLOCKED)
            pid = int(parts[0])
            if pid in rows:
                raise ValueError(_BLOCKED)
            rows[pid] = (int(parts[1]), int(parts[2]), " ".join(parts[3:8]), parts[8])
        if not rows:
            raise ValueError(_BLOCKED)
        return rows

    def candidates(rows: dict[int, tuple[int, int, str, str]]) -> set[int]:
        return {
            pid
            for pid, (_, _, _, executable) in rows.items()
            if Path(executable).name == "codex" or "codex_cli_bin" in executable
        }

    def mappings(pid: int) -> dict[str, list[str]]:
        raw = output(["/usr/sbin/lsof", "-nP", "-a", "-p", str(pid), "-d", "txt,cwd", "-Fpfn"])
        fields: dict[str, list[str]] = {"txt": [], "cwd": []}
        descriptor = ""
        identities = []
        for line in raw.splitlines():
            if line.startswith("p"):
                identities.append(line[1:])
            elif line.startswith("f"):
                descriptor = line[1:]
            elif line.startswith("n") and descriptor in fields:
                fields[descriptor].append(line[1:])
        if identities != [str(pid)]:
            raise ValueError(_BLOCKED)
        return fields

    host = snapshot()
    desktop = candidates(host)
    verified: set[int] = set()
    for pid in desktop:
        parent = host.get(host[pid][0])
        chain = (
            (_DESKTOP_CHILD, _DESKTOP_PARENT)
            if parent is not None and parent[3] == _DESKTOP_PARENT
            else (_DESKTOP_CHILD, _DESKTOP_REPL, _DESKTOP_NODE, _DESKTOP_CHILD, _DESKTOP_PARENT)
        )
        ancestors: set[int] = set()
        for executable in chain:
            row = host.get(pid)
            if row is None or pid in ancestors or row[1] != os.getuid() or row[3] != executable:
                raise ValueError(_BLOCKED)
            ancestors.add(pid)
            if pid not in verified:
                files = mappings(pid)
                if (
                    executable not in files["txt"]
                    or len(files["cwd"]) != 1
                    or not Path(files["cwd"][0]).is_absolute()
                    or any(part.startswith("nexus-codex-") for part in Path(files["cwd"][0]).parts)
                ):
                    raise ValueError(_BLOCKED)
                verified.add(pid)
            pid = row[0]
    if desktop:
        after = snapshot()
        if candidates(after) != desktop or any(after.get(pid) != host[pid] for pid in verified):
            raise ValueError(_BLOCKED)
    containers = output(
        [
            "docker",
            "ps",
            "--filter",
            "label=com.docker.compose.project=nexus-fosun-live",
            "--format",
            "{{.ID}}",
        ]
    ).splitlines()
    if not containers or any(re.fullmatch(r"[0-9a-f]{6,64}", row) is None for row in containers):
        raise ValueError(_BLOCKED)
    for identity in containers:
        names = output(["docker", "top", identity, "-eo", "pid,comm"]).splitlines()
        if len(names) < 2 or names[0].split() != ["PID", "COMMAND"]:
            raise ValueError(_BLOCKED)
        for line in names[1:]:
            parts = line.strip().split(None, 1)
            if (
                len(parts) != 2
                or not parts[0].isdigit()
                or Path(parts[1]).name == "codex"
                or "codex_cli_bin" in parts[1]
            ):
                raise ValueError(_BLOCKED)


def _row_hash(row: Base) -> str:
    values = {item.key: getattr(row, item.key) for item in row.__mapper__.column_attrs}
    return job_input_hash(json.loads(json.dumps(values, default=str)))


async def _require_partial_usage_proof(
    session: AsyncSession,
    run: AgentResearchRunRecord,
    outputs: tuple[AgentResearchOutputRecord, ...],
    model: AgentModelConfigRecord,
) -> None:
    await _require_stage_usage_proof(session, run, outputs, model, initial=False)


async def _require_initial_usage_proof(
    session: AsyncSession,
    run: AgentResearchRunRecord,
    outputs: tuple[AgentResearchOutputRecord, ...],
    model: AgentModelConfigRecord,
) -> None:
    try:
        await _require_stage_usage_proof(session, run, outputs, model, initial=True)
    except ValueError:
        raise ValueError(_BLOCKED) from None


async def _require_stage_usage_proof(
    session: AsyncSession,
    run: AgentResearchRunRecord,
    outputs: tuple[AgentResearchOutputRecord, ...],
    model: AgentModelConfigRecord,
    *,
    initial: bool,
) -> None:
    """Verify known earlier stages without settling the unknown later invocation."""
    from nexus.api.research_stages import read_stage_trace

    roles = tuple(output for output in outputs if output.role is not None)
    terminal = tuple(output for output in outputs if output.role is None)
    if (
        run.status != "failed"
        or len(roles) not in ({0} if initial else {5, 7})
        or len(terminal) != 1
        or any(output.intent is not None for output in outputs)
        or any(output.status != "running" for output in roles)
        or terminal[0].status != "failed"
        or [output.sequence for output in outputs] != list(range(1, len(outputs) + 1))
        or outputs[-1] is not terminal[0]
        or terminal[0].payload.get("reason") != "model_usage_uncertain"
    ):
        raise ValueError(_BLOCKED)
    expected_stage: Literal["research", "critique", "synthesis"] = (
        "research" if initial else "critique" if len(roles) == 5 else "synthesis"
    )
    # Reuse the existing observation-only compiler. A current-code rendering
    # without an exact persisted request-hash match is never seal authority.
    from nexus.agents.codex_staged import StagedCodexCommittee
    from nexus.agents.execution_authority import load_frozen_evidence
    from nexus.agents.models import ResearchRequest
    from nexus.agents.synthesis import domain_evidence
    from nexus.ops.research_workload_inspection import (
        _inspection_committee,
        inspect_critique_request,
    )
    from nexus.settings import AgentRuntimeMode, EngineSettings

    committee = await _inspection_committee(
        session, EngineSettings(agent_runtime_mode=AgentRuntimeMode.CODEX_SDK), run, model=model
    )
    if not isinstance(committee, StagedCodexCommittee):
        raise ValueError(_BLOCKED)
    request = ResearchRequest(
        instrument_id=run.instrument_id,
        data_snapshot_id=run.data_snapshot_id,
        decision_time=run.decision_time,
    )
    inputs = await committee._inputs(request)
    evidence = await load_frozen_evidence(session, run, allow_fixture_evidence=False)
    identity = inspect_critique_request(
        committee,
        run=run,
        request=request,
        evidence_refs=tuple(domain_evidence(record) for record in evidence),
        inputs=inputs,
        records=outputs,
        stage=expected_stage,
    )
    if identity.get("status") != "request_hash_verified":
        raise ValueError(_BLOCKED)
    content = {
        "status": terminal[0].status,
        "role": None,
        "payload": terminal[0].payload,
        "intent": None,
    }
    digest = hashlib.sha256(
        json.dumps(content, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()
    trace = await read_stage_trace(session, run)
    if terminal[0].content_hash != digest or trace is None or trace.status != "verified":
        raise ValueError(_BLOCKED)
    expected_complete = (
        set() if initial else {"research"} if len(roles) == 5 else {"research", "critique"}
    )
    if {stage.stage for stage in trace.stages if stage.status == "complete"} != expected_complete:
        raise ValueError(_BLOCKED)
    try:
        parsed = tuple(CommitteeRoleOutput.model_validate(output.payload) for output in roles)
        diagnostic = StageFailureDiagnostic.model_validate(terminal[0].payload.get("diagnostic"))
        stage_input = StageInputIdentity.model_validate(terminal[0].payload.get("stage_input"))
    except ValidationError:
        raise ValueError(_BLOCKED) from None
    known_input = sum(output.input_tokens for output in parsed)
    known_output = sum(output.output_tokens for output in parsed)
    stored_usage = (run.input_tokens, run.output_tokens)
    if (
        stored_usage != (known_input, known_output)
        and not (stored_usage == (None, None) and (known_input, known_output) == (0, 0))
    ) or (
        diagnostic.stage != expected_stage
        or diagnostic.boundary != "invocation"
        or stage_input.stage != expected_stage
        or (
            bool(parsed)
            and (
                not parsed[0].frozen_inputs_hash
                or stage_input.frozen_inputs_hash != parsed[0].frozen_inputs_hash
            )
        )
    ):
        raise ValueError(_BLOCKED)


async def _seal_manual_scope(
    session: AsyncSession, *, account_id: UUID, cycle_id: UUID
) -> ManualPaperRequest | None:
    from nexus.paper.manual_request import manual_request_for_cycle, read_manual_request

    target = await manual_request_for_cycle(session, cycle_id=cycle_id)
    rows = tuple(
        await session.scalars(
            select(ManualPaperRequest).where(ManualPaperRequest.account_id == account_id)
        )
    )
    for row in rows:
        view = await read_manual_request(session, row)
        if target is not None and row.id == target.id:
            if view.status not in {"active", "unavailable"} or view.cycle_id != cycle_id:
                raise ValueError(_BLOCKED)
        elif view.status in {"preparing", "active", "unknown"}:
            raise ValueError(_BLOCKED)
    if target is not None and target.account_id != account_id:
        raise ValueError(_BLOCKED)
    return target


async def _proof(
    session: AsyncSession,
    *,
    cycle_id: UUID,
    run_id: UUID,
    now: datetime,
    same_day: bool = False,
) -> dict[str, object]:
    cycle = await session.get(AgentPaperCycleRecord, cycle_id)
    if (
        cycle is None
        or not cycle.execution_locked
        or cycle.decision_time > now
        or (same_day and cycle.decision_time.date() != now.date())
        or (not same_day and cycle.decision_time.date() >= now.date())
    ):
        raise ValueError(_BLOCKED)
    events = tuple(
        await session.scalars(
            select(AgentPaperCycleEventRecord)
            .where(AgentPaperCycleEventRecord.cycle_id == cycle_id)
            .order_by(AgentPaperCycleEventRecord.sequence)
        )
    )
    prior = None
    run_ids: set[UUID] = set()
    for sequence, event in enumerate(events, 1):
        if (
            event.sequence != sequence
            or event.from_status != prior
            or event.occurred_at > now
            or event.to_status not in {"prepared", "researching", "failed", "unavailable"}
            or event.event_hash
            != canonical_cycle_event_hash(
                cycle_id=cycle_id,
                sequence=sequence,
                from_status=cast(CycleStatus | None, prior),
                to_status=cast(CycleStatus, event.to_status),
                result_code=event.result_code,
                fact_ids=event.fact_ids,
                occurred_at=event.occurred_at,
            )
        ):
            raise ValueError(_BLOCKED)
        raw = event.fact_ids.get("research_run_ids", [])
        if not isinstance(raw, list):
            raise ValueError(_BLOCKED)
        try:
            run_ids.update(UUID(str(item)) for item in raw)
        except (ValueError, TypeError):
            raise ValueError(_BLOCKED) from None
        prior = event.to_status
    if (
        prior not in {"failed", "unavailable"}
        or not 1 <= len(run_ids) <= 5
        or run_id not in run_ids
    ):
        raise ValueError(_BLOCKED)
    if await session.scalar(
        select(AgentPortfolioSynthesisRunRecord.id)
        .where(AgentPortfolioSynthesisRunRecord.cycle_id == cycle_id)
        .limit(1)
    ):
        raise ValueError(_BLOCKED)
    run = await session.get(AgentResearchRunRecord, run_id, with_for_update=True)
    if run is None or (
        run.status not in {"prepared", "failed"}
        or (same_day and run.status != "failed")
        or run.settled_cost_usd is not None
        or run.retry_count != 0
        or run.actor_id != "local-agent-paper-cycle"
        or run.idempotency_key != f"agent-paper:{cycle_id}:{run.instrument_id}"
        or run.decision_time != cycle.decision_time
        or run.reserved_cost_usd != 0
    ):
        raise ValueError(_BLOCKED)
    model = await session.get(AgentModelConfigRecord, run.model_config_id)
    reservation = await session.scalar(
        select(AgentDailyBudgetReservationRecord)
        .where(AgentDailyBudgetReservationRecord.run_id == run_id)
        .with_for_update()
    )
    if (
        model is None
        or reservation is None
        or (
            (
                run.pricing_version not in ACTIONABLE_CONFIG_VERSIONS
                and not formal_preparation_model_matches(model)
            )
            or model.model_id != MODEL_ID
            or model.config_hash != run.model_hash
            or model.version != run.pricing_version
            or model.max_tool_calls != 0
            or model.input_usd_per_million != 0
            or model.output_usd_per_million != 0
            or reservation.reserved_usd != 0
            or reservation.settled_usd is not None
            or reservation.state not in {"reserved", "released"}
            or reservation.pricing_version != run.pricing_version
        )
    ):
        raise ValueError(_BLOCKED)
    if run.pricing_version not in ACTIONABLE_CONFIG_VERSIONS:
        from nexus.paper.agent_cycle import _require_cycle_candidate_profile

        try:
            await _require_cycle_candidate_profile(
                session,
                cycle=cycle,
                candidate_stage_capacity=False,
                formal_stage_capacity=True,
                expected_formal_model_id=run.model_config_id,
                now=cycle.decision_time,
            )
        except ValueError:
            raise ValueError(_BLOCKED) from None
    jobs = tuple(
        await session.scalars(
            select(Job)
            .where(
                Job.job_type == "agent.paper.research",
                Job.payload["run_id"].as_string() == str(run_id),
            )
            .with_for_update()
        )
    )
    if len(jobs) != 1:
        raise ValueError(_BLOCKED)
    job = jobs[0]
    expected = job_input_hash(
        {
            "instrument_id": str(run.instrument_id),
            "data_snapshot_id": str(run.data_snapshot_id),
            "decision_time": run.decision_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "request_hash": run.input_hash,
            "schema_hash": run.schema_hash,
            "model_hash": run.model_hash,
            "prompt_hash": run.prompt_hash,
            "evidence_hash": run.evidence_hash,
            "tool_hash": run.tool_hash,
            "pricing_version": run.pricing_version,
        }
    )
    if (
        job.status not in {"failed", "dead_letter"}
        or job.finished_at is None
        or job.finished_at > now
        or job.payload != {"run_id": str(run_id)}
        or job.input_hash != expected
        or job.scope_id != str(run.instrument_id)
        or job.effective_at != run.decision_time
        or job.retry_policy != "automatic_safe"
        or job.max_attempts != 1
        or job.attempt_count != 1
        or job.manual_retry_count != 0
    ):
        raise ValueError(_BLOCKED)
    attempts = tuple(await session.scalars(select(JobAttempt).where(JobAttempt.job_id == job.id)))
    leases = tuple(await session.scalars(select(JobLease).where(JobLease.job_id == job.id)))
    outputs = tuple(
        await session.scalars(
            select(AgentResearchOutputRecord)
            .where(AgentResearchOutputRecord.run_id == run_id)
            .order_by(AgentResearchOutputRecord.sequence)
        )
    )
    if (
        len(attempts) != 1
        or len(leases) != 1
        or attempts[0].outcome not in {"failed", "lost"}
        or attempts[0].finished_at is None
        or attempts[0].finished_at > now
        or leases[0].expires_at > now
        or attempts[0].lease_token != leases[0].lease_token
        or attempts[0].fence_token != leases[0].fence_token
        or attempts[0].attempt_no != job.attempt_count
    ):
        raise ValueError(_BLOCKED)
    if any(output.role is not None for output in outputs):
        await _require_partial_usage_proof(session, run, outputs, model)
    elif same_day:
        await _require_initial_usage_proof(session, run, outputs, model)
    elif (
        run.input_tokens is not None
        or run.output_tokens is not None
        or any(output.status != "failed" or output.intent is not None for output in outputs)
    ):
        raise ValueError(_BLOCKED)
    research_events = tuple(
        await session.scalars(
            select(AgentResearchEventRecord)
            .where(AgentResearchEventRecord.run_id == run_id)
            .order_by(AgentResearchEventRecord.sequence)
        )
    )
    return {
        **({"closure_policy": "same_day_terminal_failure_v1"} if same_day else {}),
        "account_id": str(cycle.account_id),
        "cycle_id": str(cycle_id),
        "run_id": str(run_id),
        "usage": "unknown",
        "disposition": "execution_closed_without_usage_settlement",
        "fact_hash": job_input_hash(
            {
                "rows": [
                    _row_hash(row)
                    for row in (
                        cycle,
                        run,
                        model,
                        reservation,
                        job,
                        *attempts,
                        *leases,
                        *outputs,
                        *research_events,
                    )
                ],
                "cycle_events": [event.event_hash for event in events],
            }
        ),
    }


async def legacy_research_sealed(
    session: AsyncSession, *, cycle_id: UUID, run_id: UUID, now: datetime
) -> bool:
    rows = tuple(
        await session.scalars(
            select(AuditEvent).where(
                AuditEvent.action == _ACTION,
                AuditEvent.payload["run_id"].as_string() == str(run_id),
            )
        )
    )
    if not rows:
        return False
    if len(rows) != 1:
        raise ValueError(_BLOCKED)
    row = rows[0]
    if row.schema_version not in {_SCHEMA, _SAME_DAY_SCHEMA} or row.occurred_at > now:
        raise ValueError(_BLOCKED)
    same_day = row.schema_version == _SAME_DAY_SCHEMA
    proof = await _proof(
        session,
        cycle_id=cycle_id,
        run_id=run_id,
        now=row.occurred_at if same_day else now,
        same_day=same_day,
    )
    canonical = canonical_audit_event(
        AuditChainInput(
            event_seq=row.event_seq,
            occurred_at=row.occurred_at,
            actor_id=row.actor_id,
            action=row.action,
            schema_version=row.schema_version,
            payload=row.payload,
        )
    )
    previous = await session.scalar(
        select(AuditEvent.event_hash).where(AuditEvent.event_seq == row.event_seq - 1)
    )
    if (
        row.actor_id != _ACTOR
        or row.payload != proof
        or row.occurred_at > now
        or row.chain_version != "audit_jcs_sha256_v1"
        or row.canonical_event != canonical
        or row.event_hash != audit_event_hash(row.previous_hash, canonical)
        or row.previous_hash != ("0" * 64 if row.event_seq == 1 else previous)
    ):
        raise ValueError(_BLOCKED)
    return True


async def seal_failed_research(
    session: AsyncSession,
    *,
    account_id: UUID,
    cycle_id: UUID,
    run_ids: tuple[UUID, ...],
    now: datetime,
    accept_unknown_usage: bool,
    same_day: bool = False,
) -> HistorySealReceipt:
    if (
        accept_unknown_usage is not True
        or type(same_day) is not bool
        or not 1 <= len(run_ids) <= 5
        or len(set(run_ids)) != len(run_ids)
    ):
        raise ValueError("MANUAL_REQUEST_INVALID")
    await lock_portfolio_account(session, account_id)
    cycle = await session.get(AgentPaperCycleRecord, cycle_id, with_for_update=True)
    if cycle is None or cycle.account_id != account_id:
        raise ValueError(_BLOCKED)
    manual = await _seal_manual_scope(session, account_id=account_id, cycle_id=cycle_id)
    projection = await load_paper_projection(session, account_id=account_id, as_of=now)
    await require_activation_projection_unoccupied(
        session, account_id=account_id, projection=projection
    )
    require_research_processes_quiescent()
    new = 0
    for run_id in run_ids:
        if await legacy_research_sealed(session, cycle_id=cycle_id, run_id=run_id, now=now):
            continue
        proof = await _proof(session, cycle_id=cycle_id, run_id=run_id, now=now, same_day=same_day)
        await append_audit_event(
            session,
            actor_id=_ACTOR,
            action=_ACTION,
            schema_version=_SAME_DAY_SCHEMA if same_day else _SCHEMA,
            payload=proof,
            occurred_at=now,
        )
        new += 1
    if manual is not None:
        from nexus.paper.manual_request import close_failed_manual_request

        # Native cancellation proves zero attempts/outputs/leases before writing
        # zero usage for untouched siblings. Any failure rolls back the seal too.
        await close_failed_manual_request(session, request_id=manual.id, cancel_unused=True)
    if same_day:
        # Recheck after proof/audit work, before returning to the CLI's commit fence.
        require_research_processes_quiescent()
    return HistorySealReceipt(sealed_research=len(run_ids), replayed=new == 0)
