"""Explicit no-native-trigger disposition; the original unknown receipt never changes."""

import json
import runpy
import subprocess
import sys
from contextlib import AbstractContextManager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.canonical import AuditChainInput, audit_event_hash, canonical_audit_event
from nexus.audit.service import append_audit_event
from nexus.jobs.models import job_input_hash
from nexus.paper.history_seal import _row_hash, require_research_processes_quiescent
from nexus.paper.manual_entry import manual_host_scope
from nexus.paper.target_service import lock_portfolio_account
from nexus.persistence.models.manual_paper import ManualPaperRequest, ManualPaperRequestEvent
from nexus.persistence.models.paper import AgentPaperCycleEventRecord
from nexus.persistence.models.runtime import AuditEvent, IdempotencyRecord, WorkerHeartbeat
from nexus.settings import AgentRuntimeMode, EngineSettings

ACTION = "paper.manual_preparation.closed"
SCHEMA = "paper_preparation_close_v1"
ACTOR = "local-user:manual-preparation-maintenance"
BLOCKED = "MANUAL_PREPARATION_CLOSE_BLOCKED"
QUALIFIED_ORIGINAL_REVISION = "7617eded5b450c1487289a65d52ba735fbd80739"
# Only reviewed producers whose entire manual dispatch path commits the native
# reservation before source collection/refresh/Model/Order work. Unknown builds
# are not qualified by the absence of a claim. No caller-supplied revision.
QUALIFIED_PRODUCERS = {
    QUALIFIED_ORIGINAL_REVISION: "0c25671e11e107fdddc985ec20444c263e9e83c181a53d6254d6251658c0a642",
    "92276fa3192a2af9e89676cd6bf52a6e6679a3e4": (
        "4f0cb8437a6a86ff8d83a491d8e70a3c4f0014e608f4548d84fb3c895dedbc0b"
    ),
}


class PreparationCloseDiagnosticError(ValueError):
    """Fixed failure classification; never carries external command text."""

    _SAFE_CODES = {
        "service_control_source": {
            "SOURCE_IDENTITY_INVALID",
            "INSPECTION_TIMEOUT",
            "INSPECTION_FAILED",
        },
        "service_control_lock": {"LOCK_BUSY", "LOCK_UNSAFE", "LOCK_UNAVAILABLE"},
        "host_status": {
            "INSPECTION_TIMEOUT",
            "INSPECTION_FAILED",
            "STATUS_INVALID",
            "HOST_NOT_INACTIVE",
        },
        "research_processes": {"QUIESCENCE_UNCONFIRMED"},
    }

    def __init__(self, stage: str, code: str) -> None:
        super().__init__(BLOCKED)
        self.stage = stage
        self.code = code

    def safe_diagnostics(self) -> dict[str, str]:
        if (
            isinstance(self.stage, str)
            and isinstance(self.code, str)
            and self.code in self._SAFE_CODES.get(self.stage, set())
        ):
            return {"diagnostic_stage": self.stage, "diagnostic_code": self.code}
        return {}


class PreparationCloseReceipt(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    request_id: UUID
    status: Literal["unknown_preparation_closed"] = "unknown_preparation_closed"
    original_result: Literal["unknown"] = "unknown"
    replayed: bool
    execution_locked: Literal[True] = True


def preparation_service_control_lock(
    repo_root: Path, revision: str | None
) -> AbstractContextManager[None]:
    """Share the normal launchd lifecycle fence through the database commit."""
    script = repo_root / "scripts/macos/agent-paper-loop-service.py"
    try:
        if revision is None or script.is_symlink():
            raise PreparationCloseDiagnosticError(
                "service_control_source", "SOURCE_IDENTITY_INVALID"
            )
        source = subprocess.run(
            [
                "git",
                "-C",
                str(repo_root),
                "show",
                revision + ":scripts/macos/agent-paper-loop-service.py",
            ],
            capture_output=True,
            timeout=5,
            check=False,
        )
        if source.returncode != 0:
            raise PreparationCloseDiagnosticError("service_control_source", "INSPECTION_FAILED")
        if source.stdout != script.read_bytes():
            raise PreparationCloseDiagnosticError(
                "service_control_source", "SOURCE_IDENTITY_INVALID"
            )
        namespace = runpy.run_path(str(script))
        return cast(AbstractContextManager[None], namespace["_service_control_lock"]())
    except PreparationCloseDiagnosticError:
        raise
    except subprocess.TimeoutExpired:
        raise PreparationCloseDiagnosticError(
            "service_control_source", "INSPECTION_TIMEOUT"
        ) from None
    except (OSError, subprocess.SubprocessError, ValueError):
        raise PreparationCloseDiagnosticError(
            "service_control_source", "INSPECTION_FAILED"
        ) from None


def require_preparation_producers_stopped(repo_root: Path, env_file: Path) -> None:
    """Use the normal owner-aware status, never equate a stale heartbeat with exit."""
    try:
        result = subprocess.run(
            [
                sys.executable,
                str(repo_root / "scripts/macos/agent-paper-loop-service.py"),
                "status",
                "--repo-root",
                str(repo_root),
                "--env-file",
                str(env_file),
                "--project-name",
                "nexus-fosun-live",
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )
    except subprocess.TimeoutExpired:
        raise PreparationCloseDiagnosticError("host_status", "INSPECTION_TIMEOUT") from None
    except (OSError, subprocess.SubprocessError, ValueError):
        raise PreparationCloseDiagnosticError("host_status", "INSPECTION_FAILED") from None
    if result.returncode != 0:
        raise PreparationCloseDiagnosticError("host_status", "INSPECTION_FAILED")
    try:
        status = json.loads(result.stdout)
    except ValueError:
        raise PreparationCloseDiagnosticError("host_status", "STATUS_INVALID") from None
    if not isinstance(status, dict):
        raise PreparationCloseDiagnosticError("host_status", "STATUS_INVALID")
    if status.get("status") != "inactive":
        raise PreparationCloseDiagnosticError("host_status", "HOST_NOT_INACTIVE")
    try:
        require_research_processes_quiescent()
    except (OSError, subprocess.SubprocessError, ValueError):
        raise PreparationCloseDiagnosticError(
            "research_processes", "QUIESCENCE_UNCONFIRMED"
        ) from None


async def _proof(
    session: AsyncSession, row: ManualPaperRequest, *, at: datetime
) -> dict[str, object]:
    from nexus.paper.manual_request import read_manual_request

    view = await read_manual_request(session, row)
    events = tuple(
        await session.scalars(
            select(ManualPaperRequestEvent)
            .where(ManualPaperRequestEvent.request_id == row.id)
            .order_by(ManualPaperRequestEvent.sequence)
        )
    )
    if (
        at.utcoffset() is None
        or view.status != "unknown"
        or view.cycle_id is not None
        or [e.status for e in events] != ["queued", "preparing", "unknown"]
        or any(e.cycle_id is not None or e.occurred_at > at for e in events)
    ):
        raise ValueError(BLOCKED)
    actor = "local-agent-paper-schwab:" + str(row.account_id)
    claim = await session.scalar(
        select(IdempotencyRecord.id).where(
            IdempotencyRecord.actor_id == actor,
            IdempotencyRecord.idempotency_key.in_([str(row.id), "manual-entry:" + str(row.id)]),
        )
    )
    bound = await session.scalar(
        select(AgentPaperCycleEventRecord.cycle_id).where(
            AgentPaperCycleEventRecord.fact_ids["manual_request_id"].astext == str(row.id)
        )
    )
    if claim is not None or bound is not None:
        raise ValueError(BLOCKED)
    scope = manual_host_scope(
        EngineSettings(
            agent_runtime_mode=AgentRuntimeMode.CODEX_SDK,
            agent_paper_account_id=row.account_id,
            agent_paper_candidate_batch_id=row.candidate_batch_id,
            agent_paper_formal_model_config_id=row.model_config_id,
            agent_paper_manual_enabled=True,
            formal_budget_protection_enabled=False,
        )
    )
    producers = tuple(
        await session.scalars(
            select(WorkerHeartbeat).where(
                WorkerHeartbeat.queue == "agent_paper",
                WorkerHeartbeat.worker_id.endswith(":manual:" + scope),
                WorkerHeartbeat.started_at <= events[-1].occurred_at,
                WorkerHeartbeat.seen_at >= events[1].occurred_at,
            )
        )
    )
    if len(producers) != 1:
        raise ValueError(BLOCKED)
    producer = producers[0]
    revision = producer.process_version.removeprefix("nexus-manual-paper-v1+")
    if (
        producer.process_version != "nexus-manual-paper-v1+" + revision
        or producer.started_at > row.requested_at
        or producer.seen_at > events[-1].occurred_at
        or revision not in QUALIFIED_PRODUCERS
        or producer.migration_head != "0078_formal_v10_output_binding"
        or producer.seen_at >= at - timedelta(seconds=30)
    ):
        raise ValueError(BLOCKED)
    return {
        "request_id": str(row.id),
        "account_id": str(row.account_id),
        "original_result": "unknown",
        "disposition": "closed_without_native_trigger",
        "producer_revision": revision,
        "trigger_source_hash": QUALIFIED_PRODUCERS[revision],
        "fact_hash": job_input_hash(
            {
                "request": _row_hash(row),
                "events": [e.event_hash for e in events],
                "producer": _row_hash(producer),
            }
        ),
    }


async def preparation_closed(
    session: AsyncSession, row: ManualPaperRequest, *, now: datetime
) -> bool:
    from nexus.paper.current_preparation_acceptance import current_unknown_accepted

    if await current_unknown_accepted(session, row):
        return True
    audits = tuple(
        await session.scalars(
            select(AuditEvent).where(
                AuditEvent.action == ACTION, AuditEvent.payload["request_id"].astext == str(row.id)
            )
        )
    )
    if not audits:
        from nexus.paper.preparation_unknown_ack import acknowledged_request

        return await acknowledged_request(session, row, now=now)
    if len(audits) != 1:
        raise ValueError(BLOCKED)
    audit = audits[0]
    proof = await _proof(session, row, at=audit.occurred_at)
    canonical = canonical_audit_event(
        AuditChainInput(
            event_seq=audit.event_seq,
            occurred_at=audit.occurred_at,
            actor_id=audit.actor_id,
            action=audit.action,
            schema_version=audit.schema_version,
            payload=audit.payload,
        )
    )
    previous = await session.scalar(
        select(AuditEvent.event_hash).where(AuditEvent.event_seq == audit.event_seq - 1)
    )
    if (
        audit.actor_id != ACTOR
        or audit.schema_version != SCHEMA
        or audit.payload != proof
        or audit.occurred_at > now
        or audit.chain_version != "audit_jcs_sha256_v1"
        or audit.canonical_event != canonical
        or audit.event_hash != audit_event_hash(audit.previous_hash, canonical)
        or audit.previous_hash != ("0" * 64 if audit.event_seq == 1 else previous)
    ):
        raise ValueError(BLOCKED)
    return True


async def close_unstarted_preparation(
    session: AsyncSession,
    *,
    account_id: UUID,
    request_id: UUID,
    now: datetime,
    repo_root: Path,
    env_file: Path,
) -> PreparationCloseReceipt:
    from nexus.paper.manual_request import require_manual_account_available

    await lock_portfolio_account(session, account_id)
    row = await session.get(ManualPaperRequest, request_id, with_for_update=True)
    if row is None or row.account_id != account_id:
        raise ValueError(BLOCKED)
    if await preparation_closed(session, row, now=now):
        return PreparationCloseReceipt(request_id=request_id, replayed=True)
    require_preparation_producers_stopped(repo_root, env_file)
    proof = await _proof(session, row, at=now)
    # Other in-flight native source work may exist before a cycle is committed.
    claims = tuple(
        await session.scalars(
            select(IdempotencyRecord).where(
                IdempotencyRecord.actor_id == "local-agent-paper-schwab:" + str(account_id)
            )
        )
    )
    if any(
        not isinstance(claim.response_payload, dict)
        or claim.response_payload.get("outcome") not in {"completed", "failed"}
        for claim in claims
    ):
        raise ValueError(BLOCKED)
    await require_manual_account_available(
        session, account_id=account_id, now=now, excluding_request_id=request_id
    )
    await append_audit_event(
        session,
        actor_id=ACTOR,
        action=ACTION,
        schema_version=SCHEMA,
        payload=proof,
        occurred_at=now,
    )
    require_preparation_producers_stopped(repo_root, env_file)
    return PreparationCloseReceipt(request_id=request_id, replayed=False)
