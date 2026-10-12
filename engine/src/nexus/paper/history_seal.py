"""Explicit legacy failure disposition; usage and budget facts remain unknown."""

import json
import re
import subprocess
from datetime import datetime
from pathlib import Path
from typing import cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_sdk import MODEL_ID
from nexus.agents.codex_versions import ACTIONABLE_CONFIG_VERSIONS
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
from nexus.persistence.models.paper import AgentPaperCycleEventRecord, AgentPaperCycleRecord
from nexus.persistence.models.runtime import AuditEvent, Job, JobAttempt, JobLease

_ACTION = "paper.legacy_research.sealed"
_SCHEMA = "paper_legacy_seal_v1"
_ACTOR = "local-manual-history-seal"
_BLOCKED = "MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"
_DESKTOP_CHILD = (
    "/Applications/ChatGPT.app/Contents/Resources/codex-cli/CodexCLI.app/Contents/MacOS/codex"
)
_DESKTOP_PARENT = "/Applications/ChatGPT.app/Contents/MacOS/ChatGPT"


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

    def snapshot() -> dict[int, tuple[int, str, str]]:
        rows: dict[int, tuple[int, str, str]] = {}
        raw = output(["/bin/ps", "-axo", "pid=,ppid=,lstart=,comm="])
        for line in raw.splitlines():
            parts = line.strip().split(None, 7)
            if len(parts) != 8 or not parts[0].isdigit() or not parts[1].isdigit():
                raise ValueError(_BLOCKED)
            pid = int(parts[0])
            if pid in rows:
                raise ValueError(_BLOCKED)
            rows[pid] = (int(parts[1]), " ".join(parts[2:7]), parts[7])
        if not rows:
            raise ValueError(_BLOCKED)
        return rows

    def candidates(rows: dict[int, tuple[int, str, str]]) -> set[int]:
        return {
            pid
            for pid, (_, _, executable) in rows.items()
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
    for pid in desktop:
        parent_id, _, executable = host[pid]
        parent = host.get(parent_id)
        if executable != _DESKTOP_CHILD or parent is None or parent[2] != _DESKTOP_PARENT:
            raise ValueError(_BLOCKED)
        child_files, parent_files = mappings(pid), mappings(parent_id)
        if (
            executable not in child_files["txt"]
            or _DESKTOP_PARENT not in parent_files["txt"]
            or len(child_files["cwd"]) != 1
            or not Path(child_files["cwd"][0]).is_absolute()
            or any(part.startswith("nexus-codex-") for part in Path(child_files["cwd"][0]).parts)
        ):
            raise ValueError(_BLOCKED)
    if desktop:
        after = snapshot()
        if candidates(after) != desktop or any(
            after.get(pid) != host[pid] or after.get(host[pid][0]) != host[host[pid][0]]
            for pid in desktop
        ):
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


async def _proof(
    session: AsyncSession, *, cycle_id: UUID, run_id: UUID, now: datetime
) -> dict[str, object]:
    cycle = await session.get(AgentPaperCycleRecord, cycle_id)
    if cycle is None or not cycle.execution_locked or cycle.decision_time.date() >= now.date():
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
        or run.settled_cost_usd is not None
        or run.input_tokens is not None
        or run.output_tokens is not None
        or run.retry_count != 0
        or run.actor_id != "local-agent-paper-cycle"
        or run.idempotency_key != f"agent-paper:{cycle_id}:{run.instrument_id}"
        or run.decision_time != cycle.decision_time
        or run.reserved_cost_usd != 0
        or run.pricing_version not in ACTIONABLE_CONFIG_VERSIONS
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
            model.model_id != MODEL_ID
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
    proof = await _proof(session, cycle_id=cycle_id, run_id=run_id, now=now)
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
        or row.schema_version != _SCHEMA
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
) -> HistorySealReceipt:
    if (
        accept_unknown_usage is not True
        or not 1 <= len(run_ids) <= 5
        or len(set(run_ids)) != len(run_ids)
    ):
        raise ValueError("MANUAL_REQUEST_INVALID")
    await lock_portfolio_account(session, account_id)
    cycle = await session.get(AgentPaperCycleRecord, cycle_id, with_for_update=True)
    if cycle is None or cycle.account_id != account_id:
        raise ValueError(_BLOCKED)
    from nexus.paper.manual_request import require_no_running_manual_request

    await require_no_running_manual_request(session, account_id=account_id)
    projection = await load_paper_projection(session, account_id=account_id, as_of=now)
    await require_activation_projection_unoccupied(
        session, account_id=account_id, projection=projection
    )
    require_research_processes_quiescent()
    new = 0
    for run_id in run_ids:
        if await legacy_research_sealed(session, cycle_id=cycle_id, run_id=run_id, now=now):
            continue
        proof = await _proof(session, cycle_id=cycle_id, run_id=run_id, now=now)
        await append_audit_event(
            session,
            actor_id=_ACTOR,
            action=_ACTION,
            schema_version=_SCHEMA,
            payload=proof,
            occurred_at=now,
        )
        new += 1
    return HistorySealReceipt(sealed_research=len(run_ids), replayed=new == 0)
