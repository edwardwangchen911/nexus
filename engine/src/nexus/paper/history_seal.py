"""Explicit legacy failure disposition; usage and budget facts remain unknown."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, ValidationError
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_formal_stage import formal_preparation_model_matches
from nexus.agents.codex_sdk import MODEL_ID
from nexus.agents.codex_versions import ACTIONABLE_CONFIG_VERSIONS
from nexus.agents.committee import CommitteeRoleOutput, StageFailureDiagnostic, StageInputIdentity
from nexus.agents.research_job_identity import research_run_job_input_hash
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

if TYPE_CHECKING:
    from nexus.agents.research_stages.proof import TerminalProof

_ACTION = "paper.legacy_research.sealed"
_SCHEMA = "paper_legacy_seal_v1"
_SAME_DAY_SCHEMA = "paper_same_day_close_v1"
_EXPIRED_SCHEMA = "paper_expired_attempt_close_v1"
_ACTOR = "local-manual-history-seal"
_BLOCKED = "MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"
_REVIEWED_HISTORY_ACTION = "paper.legacy_research.reviewed_history_bound"
_REVIEWED_HISTORY_SCHEMA = "paper_reviewed_history_bind_v1"
# NEXUS-SCHEDULED-SEAL-HISTORY-20261119-R1. These digests pin one maintenance
# operation without putting the live account or original request IDs in code.
_REVIEWED_SEAL_IDENTITY = "bc14cefcfa06eaf51d38a0773b9194fb226bc355f2294c58f724e0857305f7c2"
_REVIEWED_SEAL_PROOF = "2e558cc92a3d66bf7509e0278aa18c69fa378e0b92365c31ecb91ca021faa78a"
_REVIEWED_INDEPENDENT_PROOFS = frozenset(
    {
        "48e510a8345b00495bf5dbe283ff92dbdbeb363e1ed280980f5493ad3a5b4e7f",
        "c8bd6784d341e8aea0681a28a98441edc937f2b07d65889e1cf013251eeb4c7c",
        "e72bd2a98a12ef6ad86947ff2bc3ff8234fd85eef13885c8b6693185b1092a28",
    }
)
_DESKTOP_CHILD = (
    "/Applications/ChatGPT.app/Contents/Resources/codex-cli/CodexCLI.app/Contents/MacOS/codex"
)
_DESKTOP_PARENT = "/Applications/ChatGPT.app/Contents/MacOS/ChatGPT"
_DESKTOP_REPL = "/Applications/ChatGPT.app/Contents/Resources/cua_node/bin/node_repl"
_DESKTOP_NODE = "/Applications/ChatGPT.app/Contents/Resources/cua_node/bin/node"
_DESKTOP_APERITIF = (
    "/Applications/ChatGPT.app/Contents/Frameworks/Codex Framework.framework/"
    "Versions/158.0.8127.37/Helpers/Codex (Aperitif).app/Contents/MacOS/Codex (Aperitif)"
)
# NEXUS-APERITIF-RECOVERY-20261121-R1 pins the observed arm64 code objects.
# An application update requires a new review, even if the installation paths match.
_APERITIF_CODE_IDENTITIES = {
    _DESKTOP_CHILD: ("codex", "9aca7e9e8b34af80dec29d17468073b262c26cbb"),
    _DESKTOP_APERITIF: ("com.openai.codex.helper", "bb8eeee22cd0fa7d12768e3817290e4598fd4ef3"),
    _DESKTOP_PARENT: ("com.openai.codex", "b5215002d934d1b51fa2dba60375f702e4789f28"),
}
_APERITIF_TEAM = "2DC432GLL2"
_FileIdentity = tuple[int, int, int, int, int]
_ProcessMapping = tuple[str, _FileIdentity, _FileIdentity]
_MACOS_DOCKER_CLI = Path("/Applications/Docker.app/Contents/Resources/bin/docker")
terminal_proof_read_session: ContextVar[AsyncSession | None] = ContextVar(
    "terminal_proof_read_session", default=None
)


@contextmanager
def terminal_proof_read_scope(session: AsyncSession) -> Iterator[None]:
    """Reuse the already validated terminal proof for this read-only session."""
    token = terminal_proof_read_session.set(session)
    try:
        yield
    finally:
        terminal_proof_read_session.reset(token)


class ResearchProcessDiagnosticError(ValueError):
    """Fixed process boundary only; no PID, executable path or command output."""

    SAFE_CODES = frozenset(
        {
            "HOST_SNAPSHOT_UNCONFIRMED",
            "HOST_MAPPING_UNCONFIRMED",
            "HOST_SIGNATURE_UNCONFIRMED",
            "HOST_IDENTITY_UNCONFIRMED",
            "HOST_CHANGED_DURING_CHECK",
            "CONTAINER_LIST_UNCONFIRMED",
            "CONTAINER_PROCESSES_UNCONFIRMED",
        }
    )

    IDENTITY_REASONS = frozenset(
        {"ANCESTOR_MISSING", "ANCESTOR_CYCLE", "OWNER_MISMATCH", "EXECUTABLE_MISMATCH"}
    )

    def __init__(
        self,
        code: str,
        *,
        reason: str | None = None,
        chain: str | None = None,
        hop: str | None = None,
        observed: str | None = None,
    ) -> None:
        super().__init__(_BLOCKED)
        self.code = code
        self.reason = reason
        self.chain, self.hop, self.observed = chain, hop, observed

    def safe_diagnostics(self) -> dict[str, str]:
        if isinstance(self.code, str) and self.code in self.SAFE_CODES:
            result = {"diagnostic_stage": "research_processes", "diagnostic_code": self.code}
            if (
                self.code == "HOST_IDENTITY_UNCONFIRMED"
                and isinstance(self.reason, str)
                and self.reason in self.IDENTITY_REASONS
            ):
                result["diagnostic_reason"] = self.reason
                for key, value, allowed in (
                    ("chain", self.chain, {"direct", "nested"}),
                    (
                        "hop",
                        self.hop,
                        {"candidate", "repl", "node", "ancestor_codex", "desktop", "aperitif"},
                    ),
                    (
                        "observed",
                        self.observed,
                        {"codex", "chatgpt", "repl", "node", "aperitif", "other", "missing"},
                    ),
                ):
                    if isinstance(value, str) and value in allowed:
                        result["diagnostic_" + key] = value
            return result
        return {}


class HistorySealReceipt(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    status: str = "legacy_research_sealed_usage_unknown"
    sealed_research: int
    replayed: bool
    execution_locked: bool = True


def require_research_processes_quiescent() -> None:
    _require_research_processes_quiescent(formal_recovery=False)


def require_formal_recovery_processes_quiescent() -> None:
    """Use the desktop bridge topology reviewed for managed host recovery."""
    _require_research_processes_quiescent(formal_recovery=True)


def _aperitif_file_identity(path: Path) -> _FileIdentity:
    if not path.is_absolute() or path.resolve(strict=True) != path:
        raise ValueError("noncanonical process mapping")
    info = path.stat(follow_symlinks=False)
    return info.st_dev, info.st_ino, info.st_ctime_ns, info.st_mtime_ns, info.st_size


def _aperitif_process_mapping(raw: str, pid: int, executable: str) -> _ProcessMapping:
    header, *records = raw.split("\0\n")
    if header != f"p{pid}" or not records or records[-1] != "":
        raise ResearchProcessDiagnosticError("HOST_MAPPING_UNCONFIRMED")

    mapped = []
    directories = []
    try:
        for record in records[:-1]:
            kind, *fields = record.split("\0")
            names = [field[1:] for field in fields if field.startswith("n")]
            if kind != "fcwd" and (kind != "ftxt" or executable not in names):
                continue
            (name,) = names
            (device,) = [int(field[1:], 16) for field in fields if field.startswith("D")]
            (inode,) = [int(field[1:]) for field in fields if field.startswith("i")]
            path = Path(name)
            identity = _aperitif_file_identity(path)
            if str(path) != name or identity[:2] != (device, inode):
                raise ValueError("process mapping changed")
            if kind == "fcwd":
                if any(part.startswith("nexus-codex-") for part in path.parts):
                    raise ValueError("research working directory")
                directories.append((name, identity))
            else:
                mapped.append(identity)
        (executable_identity,) = mapped
        ((cwd, directory_identity),) = directories
    except (OSError, RuntimeError, ValueError):
        raise ResearchProcessDiagnosticError("HOST_MAPPING_UNCONFIRMED") from None
    return cwd, executable_identity, directory_identity


def _verify_aperitif_process(
    output: Callable[[list[str]], str], pid: int, executable: str
) -> _ProcessMapping:
    command = ["/usr/sbin/lsof", "-nP", "-a", "-p", str(pid), "-d", "txt,cwd", "-F0pfDin"]
    mapping = _aperitif_process_mapping(output(command), pid, executable)
    identifier, cdhash = _APERITIF_CODE_IDENTITIES[executable]
    requirement = (
        f'-R=anchor apple generic and identifier "{identifier}" and '
        f'certificate leaf[subject.OU] = "{_APERITIF_TEAM}" and cdhash H"{cdhash}"'
    )
    for subject in (str(pid), executable):
        output(["/usr/bin/codesign", "--verify", "--strict", requirement, subject])
    if _aperitif_process_mapping(output(command), pid, executable) != mapping:
        raise ResearchProcessDiagnosticError("HOST_CHANGED_DURING_CHECK")
    return mapping


def _require_research_processes_quiescent(*, formal_recovery: bool) -> None:
    """Inspect process identity and executable mappings; never argv or credentials."""

    def output(command: list[str]) -> str:
        code = (
            "HOST_SNAPSHOT_UNCONFIRMED"
            if command[0] == "/bin/ps"
            else "HOST_MAPPING_UNCONFIRMED"
            if command[0] == "/usr/sbin/lsof"
            else "HOST_SIGNATURE_UNCONFIRMED"
            if command[0] == "/usr/bin/codesign"
            else "CONTAINER_LIST_UNCONFIRMED"
            if command[1] == "ps"
            else "CONTAINER_PROCESSES_UNCONFIRMED"
        )
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=5, check=False)
        except (OSError, subprocess.SubprocessError, UnicodeError):
            raise ResearchProcessDiagnosticError(code) from None
        if result.returncode != 0:
            raise ResearchProcessDiagnosticError(code)
        return result.stdout

    def snapshot() -> dict[int, tuple[int, int, str, str]]:
        rows: dict[int, tuple[int, int, str, str]] = {}
        raw = output(["/bin/ps", "-axo", "pid=,ppid=,uid=,lstart=,comm="])
        for line in raw.splitlines():
            parts = line.strip().split(None, 8)
            if len(parts) != 9 or not all(value.isdigit() for value in parts[:3]):
                raise ResearchProcessDiagnosticError("HOST_SNAPSHOT_UNCONFIRMED")
            pid = int(parts[0])
            if pid in rows:
                raise ResearchProcessDiagnosticError("HOST_SNAPSHOT_UNCONFIRMED")
            rows[pid] = (int(parts[1]), int(parts[2]), " ".join(parts[3:8]), parts[8])
        if not rows:
            raise ResearchProcessDiagnosticError("HOST_SNAPSHOT_UNCONFIRMED")
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
            raise ResearchProcessDiagnosticError("HOST_MAPPING_UNCONFIRMED")
        return fields

    host = snapshot()
    desktop = candidates(host)
    verified: set[int] = set()
    aperitif: dict[int, tuple[str, _ProcessMapping]] = {}
    for pid in desktop:
        parent = host.get(host[pid][0])
        ancestor = None if parent is None else host.get(parent[0])
        aperitif_chain = formal_recovery and parent is not None and parent[3] == _DESKTOP_APERITIF
        chain: tuple[str, ...]
        roles: tuple[str, ...]
        if parent is not None and parent[3] == _DESKTOP_PARENT:
            chain = (_DESKTOP_CHILD, _DESKTOP_PARENT)
            roles = ("candidate", "desktop")
        elif aperitif_chain:
            chain = (_DESKTOP_CHILD, _DESKTOP_APERITIF, _DESKTOP_PARENT)
            roles = ("candidate", "aperitif", "desktop")
        elif (
            formal_recovery
            and parent is not None
            and parent[3] == _DESKTOP_REPL
            and ancestor is not None
            and ancestor[3] == _DESKTOP_CHILD
        ):
            chain = (_DESKTOP_CHILD, _DESKTOP_REPL, _DESKTOP_CHILD, _DESKTOP_PARENT)
            roles = ("candidate", "repl", "ancestor_codex", "desktop")
        else:
            chain = (_DESKTOP_CHILD, _DESKTOP_REPL, _DESKTOP_NODE, _DESKTOP_CHILD, _DESKTOP_PARENT)
            roles = ("candidate", "repl", "node", "ancestor_codex", "desktop")
        ancestors: set[int] = set()
        for role, executable in zip(roles, chain, strict=True):
            row = host.get(pid)
            observed = (
                "missing"
                if row is None
                else {
                    _DESKTOP_CHILD: "codex",
                    _DESKTOP_PARENT: "chatgpt",
                    _DESKTOP_REPL: "repl",
                    _DESKTOP_NODE: "node",
                    _DESKTOP_APERITIF: "aperitif",
                }.get(row[3], "other")
            )

            def identity_error(reason: str) -> ResearchProcessDiagnosticError:
                return ResearchProcessDiagnosticError(
                    "HOST_IDENTITY_UNCONFIRMED",
                    reason=reason,
                    chain="direct" if len(chain) == 2 else "nested",
                    hop=role,
                    observed=observed,
                )

            if row is None:
                raise identity_error("ANCESTOR_MISSING")
            if pid in ancestors:
                raise identity_error("ANCESTOR_CYCLE")
            if row[1] != os.getuid():
                raise identity_error("OWNER_MISMATCH")
            if row[3] != executable:
                raise identity_error("EXECUTABLE_MISMATCH")
            ancestors.add(pid)
            if aperitif_chain:
                if pid not in aperitif:
                    aperitif[pid] = (executable, _verify_aperitif_process(output, pid, executable))
                verified.add(pid)
            elif pid not in verified:
                files = mappings(pid)
                if (
                    executable not in files["txt"]
                    or len(files["cwd"]) != 1
                    or not Path(files["cwd"][0]).is_absolute()
                    or any(part.startswith("nexus-codex-") for part in Path(files["cwd"][0]).parts)
                ):
                    raise ResearchProcessDiagnosticError("HOST_MAPPING_UNCONFIRMED")
                verified.add(pid)
            pid = row[0]
    if desktop and not aperitif:
        after = snapshot()
        if candidates(after) != desktop or any(after.get(pid) != host[pid] for pid in verified):
            raise ResearchProcessDiagnosticError("HOST_CHANGED_DURING_CHECK")
    docker = "docker"
    # launchd's default PATH excludes Docker Desktop's installed CLI. Resolve
    # this existing dependency without inheriting a shell environment or
    # skipping any container inspection when it is unavailable.
    if (
        shutil.which(docker) is None
        and sys.platform == "darwin"
        and _MACOS_DOCKER_CLI.is_file()
        and os.access(_MACOS_DOCKER_CLI, os.X_OK)
    ):
        docker = str(_MACOS_DOCKER_CLI)
    containers = output(
        [
            docker,
            "ps",
            "--filter",
            "label=com.docker.compose.project=nexus-fosun-live",
            "--format",
            "{{.ID}}",
        ]
    ).splitlines()
    if not containers or any(re.fullmatch(r"[0-9a-f]{6,64}", row) is None for row in containers):
        raise ResearchProcessDiagnosticError("CONTAINER_LIST_UNCONFIRMED")
    for identity in containers:
        names = output([docker, "top", identity, "-eo", "pid,comm"]).splitlines()
        if len(names) < 2 or names[0].split() != ["PID", "COMMAND"]:
            raise ResearchProcessDiagnosticError("CONTAINER_PROCESSES_UNCONFIRMED")
        for line in names[1:]:
            parts = line.strip().split(None, 1)
            if (
                len(parts) != 2
                or not parts[0].isdigit()
                or Path(parts[1]).name == "codex"
                or "codex_cli_bin" in parts[1]
            ):
                raise ResearchProcessDiagnosticError("CONTAINER_PROCESSES_UNCONFIRMED")
    if aperitif:
        for pid, (executable, before) in aperitif.items():
            if _verify_aperitif_process(output, pid, executable) != before:
                raise ResearchProcessDiagnosticError("HOST_CHANGED_DURING_CHECK")
        after = snapshot()
        if candidates(after) != desktop or any(after.get(pid) != host[pid] for pid in verified):
            raise ResearchProcessDiagnosticError("HOST_CHANGED_DURING_CHECK")


def _row_hash(row: Base) -> str:
    keys: tuple[str, ...]
    if isinstance(row, ManualPaperRequest) and row.contract_version == 1:
        # v1 audit proofs predate 0084. Keep their exact original column set;
        # new pure-research metadata cannot revoke or broaden historical acceptance.
        if row.research_profile is not None:
            raise ValueError("MANUAL_RESULT_UNKNOWN")
        keys = (
            "id",
            "account_id",
            "candidate_batch_id",
            "model_config_id",
            "mode",
            "key_hash",
            "request_hash",
            "batch_hash",
            "model_hash",
            "requested_at",
            "created_at",
        )
    else:
        keys = tuple(item.key for item in row.__mapper__.column_attrs)
    values = {key: getattr(row, key) for key in keys}
    # The nullable 0113 extension was not part of historical SDK audit rows.
    # A real native profile must participate in the hash in its entirety.
    if isinstance(row, AgentModelConfigRecord) and values.get("platform_profile") is None:
        values.pop("platform_profile", None)
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
    session: AsyncSession,
    *,
    account_id: UUID,
    cycle_id: UUID,
    now: datetime,
    reviewed_history: dict[str, dict[str, object]] | None = None,
) -> ManualPaperRequest | None:
    from nexus.paper.manual_preparation_close import preparation_closed
    from nexus.paper.manual_request import manual_request_for_cycle, read_manual_request

    target = await manual_request_for_cycle(session, cycle_id=cycle_id)
    rows = tuple(
        await session.scalars(
            select(ManualPaperRequest)
            .where(ManualPaperRequest.account_id == account_id)
            .order_by(ManualPaperRequest.id)
        )
    )
    for row in rows:
        view = await read_manual_request(session, row)
        if target is not None and row.id == target.id:
            if view.status not in {"active", "unavailable"} or view.cycle_id != cycle_id:
                raise ValueError(_BLOCKED)
        elif view.status in {"preparing", "active", "unknown"}:
            if view.status == "unknown":
                if await preparation_closed(session, row, now=now):
                    continue
                if (
                    target is None
                    and reviewed_history is not None
                    and row.contract_version in {3, 4}
                ):
                    digest, binding = await _verified_reviewed_history_binding(
                        session, row, now=now
                    )
                    if digest in reviewed_history:
                        raise ValueError(_BLOCKED)
                    reviewed_history[digest] = binding
                    continue
            raise ValueError(_BLOCKED)
    if target is not None and target.account_id != account_id:
        raise ValueError(_BLOCKED)
    if reviewed_history is not None and (
        target is not None or reviewed_history.keys() != _REVIEWED_INDEPENDENT_PROOFS
    ):
        raise ValueError(_BLOCKED)
    return target


async def _verified_reviewed_history_binding(
    session: AsyncSession, row: ManualPaperRequest, *, now: datetime
) -> tuple[str, dict[str, object]]:
    from nexus.paper.independent_unknown_acceptance import _audits, _validate

    audits = await _audits(session, row.id)
    if len(audits) != 1:
        raise ValueError(_BLOCKED)
    audit = audits[0]
    binding = {
        "request_id": str(row.id),
        "contract": row.contract_version,
        "audit_id": str(audit.id),
        "event_hash": audit.event_hash,
        "proof_hash": audit.payload.get("proof_hash"),
    }
    digest = job_input_hash(binding)
    if digest not in _REVIEWED_INDEPENDENT_PROOFS:
        raise ValueError(_BLOCKED)
    # The same validator as account recovery holds the original scope lock
    # through this maintenance transaction and refuses late or changed facts.
    if await _validate(session, row=row, audit=audit, now=now) != binding["proof_hash"]:
        raise ValueError(_BLOCKED)
    return digest, binding


def _reviewed_history_payload(
    seal: AuditEvent, reviewed: dict[str, dict[str, object]]
) -> dict[str, object]:
    return {
        "review": "NEXUS-SCHEDULED-SEAL-HISTORY-20261119-R1",
        "seal_event_id": str(seal.id),
        "seal_event_hash": seal.event_hash,
        "run_id": seal.payload["run_id"],
        "cycle_id": seal.payload["cycle_id"],
        "account_id": seal.payload["account_id"],
        "source_acceptances": [reviewed[digest] for digest in sorted(reviewed)],
    }


async def _require_reviewed_history_receipt(
    session: AsyncSession, run_id: UUID, reviewed: dict[str, dict[str, object]]
) -> None:
    rows = tuple(
        await session.scalars(
            select(AuditEvent).where(
                AuditEvent.action.in_((_ACTION, _REVIEWED_HISTORY_ACTION)),
                AuditEvent.payload["run_id"].as_string() == str(run_id),
            )
        )
    )
    seals = [row for row in rows if row.action == _ACTION]
    bindings = [row for row in rows if row.action == _REVIEWED_HISTORY_ACTION]
    if len(seals) != 1 or len(bindings) != 1:
        raise ValueError(_BLOCKED)
    seal, binding = seals[0], bindings[0]
    canonical = canonical_audit_event(
        AuditChainInput(
            event_seq=binding.event_seq,
            occurred_at=binding.occurred_at,
            actor_id=binding.actor_id,
            action=binding.action,
            schema_version=binding.schema_version,
            payload=binding.payload,
        )
    )
    if (
        job_input_hash(seal.payload) != _REVIEWED_SEAL_PROOF
        or binding.actor_id != _ACTOR
        or binding.schema_version != _REVIEWED_HISTORY_SCHEMA
        or binding.chain_version != seal.chain_version
        or binding.event_seq != seal.event_seq + 1
        or binding.occurred_at != seal.occurred_at
        or binding.previous_hash != seal.event_hash
        or binding.payload != _reviewed_history_payload(seal, reviewed)
        or binding.canonical_event != canonical
        or binding.event_hash != audit_event_hash(seal.event_hash, canonical)
    ):
        raise ValueError(_BLOCKED)


async def _legacy_proof(
    session: AsyncSession,
    *,
    cycle_id: UUID,
    run_id: UUID,
    now: datetime,
    same_day: bool = False,
    expired_attempt: bool = False,
    read_only: bool = False,
) -> tuple[dict[str, object], TerminalProof | None]:
    cycle = await session.get(AgentPaperCycleRecord, cycle_id)
    if (
        cycle is None
        or (expired_attempt and not same_day)
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
    run = await session.get(AgentResearchRunRecord, run_id, with_for_update=not read_only)
    if run is None or (
        run.status not in {"prepared", "failed"}
        or (same_day and run.status != "failed")
        or run.settled_cost_usd is not None
        or run.retry_count != 0
        or run.actor_id != "local-agent-paper-cycle"
        or run.idempotency_key != f"agent-paper:{cycle_id}:{run.instrument_id}"
        or run.decision_time != cycle.decision_time
    ):
        raise ValueError(_BLOCKED)
    model = await session.get(AgentModelConfigRecord, run.model_config_id)
    reservation_query = select(AgentDailyBudgetReservationRecord).where(
        AgentDailyBudgetReservationRecord.run_id == run_id
    )
    reservation = await session.scalar(
        reservation_query if read_only else reservation_query.with_for_update()
    )
    if (
        model is None
        or reservation is None
        or model.config_hash != run.model_hash
        or model.version != run.pricing_version
        or model.max_tool_calls != 0
        or reservation.settled_usd is not None
        or reservation.pricing_version != run.pricing_version
        or reservation.reserved_usd != run.reserved_cost_usd
    ):
        raise ValueError(_BLOCKED)
    from nexus.agents.platform_native import native_platform_semantics

    try:
        platform = await native_platform_semantics(session, run, model)
    except ValueError:
        raise ValueError(_BLOCKED) from None
    if platform:
        # The original allocation and immutable price profile are verified above.
        # Disposition never turns unknown usage into zero or releases a paid hold.
        if reservation.state != "reserved" or run.reserved_cost_usd != model.worst_case_cost_usd:
            raise ValueError(_BLOCKED)
    elif (
        getattr(model, "platform_profile", None) is not None
        or (
            run.pricing_version not in ACTIONABLE_CONFIG_VERSIONS
            and not formal_preparation_model_matches(model)
        )
        or model.model_id != MODEL_ID
        or model.input_usd_per_million != 0
        or model.output_usd_per_million != 0
        or run.reserved_cost_usd != 0
        or reservation.state not in {"reserved", "released"}
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
    jobs_query = select(Job).where(
        Job.job_type == "agent.paper.research",
        Job.payload["run_id"].as_string() == str(run_id),
    )
    jobs = tuple(await session.scalars(jobs_query if read_only else jobs_query.with_for_update()))
    if len(jobs) != 1:
        raise ValueError(_BLOCKED)
    job = jobs[0]
    expected = research_run_job_input_hash(run)
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
    research_events = tuple(
        await session.scalars(
            select(AgentResearchEventRecord)
            .where(AgentResearchEventRecord.run_id == run_id)
            .order_by(AgentResearchEventRecord.sequence)
        )
    )
    from nexus.agents.research_stages.proof import read_terminal_proof
    from nexus.agents.research_stages.protocol import ProtocolRejected

    try:
        stage_proof = await read_terminal_proof(session, run_id=run.id, now=now)
    except ProtocolRejected:
        raise ValueError(_BLOCKED) from None
    if platform and stage_proof is None:
        raise ValueError(_BLOCKED)
    if stage_proof is not None:
        # Selected identities prove their frozen lower bound from stage facts;
        # stopped publication need not have emitted legacy per-role outputs.
        if stage_proof.known or not stage_proof.cleanup_confirmed:
            raise ValueError(_BLOCKED)
        if (expired_attempt and not stage_proof.orphan) or (
            same_day and not expired_attempt and stage_proof.orphan
        ):
            raise ValueError(_BLOCKED)
        if any(output.role is not None for output in outputs):
            await _require_partial_usage_proof(session, run, outputs, model)
        elif any(output.status != "failed" or output.intent is not None for output in outputs):
            raise ValueError(_BLOCKED)
    elif expired_attempt:
        from nexus.paper.research_orphan_proof import require_expired_research_proof

        if not formal_preparation_model_matches(model):
            raise ValueError(_BLOCKED)
        await require_expired_research_proof(
            session,
            run=run,
            reservation=reservation,
            job=job,
            attempt=attempts[0],
            lease=leases[0],
            outputs=outputs,
            events=research_events,
            now=now,
        )
    elif any(output.role is not None for output in outputs):
        await _require_partial_usage_proof(session, run, outputs, model)
    elif same_day:
        await _require_initial_usage_proof(session, run, outputs, model)
    elif (
        run.input_tokens is not None
        or run.output_tokens is not None
        or any(output.status != "failed" or output.intent is not None for output in outputs)
    ):
        raise ValueError(_BLOCKED)
    facts: dict[str, object] = {
        **(
            {"closure_policy": "same_day_expired_attempt_v1"}
            if expired_attempt
            else {"closure_policy": "same_day_terminal_failure_v1"}
            if same_day
            else {}
        ),
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
    return facts, stage_proof


async def legacy_research_sealed(
    session: AsyncSession, *, cycle_id: UUID, run_id: UUID, now: datetime, read_only: bool = False
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
    if (
        row.schema_version not in {_SCHEMA, _SAME_DAY_SCHEMA, _EXPIRED_SCHEMA}
        or row.occurred_at > now
    ):
        raise ValueError(_BLOCKED)
    same_day = row.schema_version in {_SAME_DAY_SCHEMA, _EXPIRED_SCHEMA}
    proof = await _proof(
        session,
        cycle_id=cycle_id,
        run_id=run_id,
        now=row.occurred_at if same_day else now,
        same_day=same_day,
        expired_attempt=row.schema_version == _EXPIRED_SCHEMA,
        read_only=read_only,
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
    expired_attempt: bool = False,
    with_reviewed_independent_history: bool = False,
) -> HistorySealReceipt:
    if (
        accept_unknown_usage is not True
        or type(same_day) is not bool
        or type(expired_attempt) is not bool
        or type(with_reviewed_independent_history) is not bool
        or (expired_attempt and not same_day)
        or not 1 <= len(run_ids) <= 5
        or len(set(run_ids)) != len(run_ids)
    ):
        raise ValueError("MANUAL_REQUEST_INVALID")
    await lock_portfolio_account(session, account_id)
    cycle = await session.get(AgentPaperCycleRecord, cycle_id, with_for_update=True)
    if cycle is None or cycle.account_id != account_id:
        raise ValueError(_BLOCKED)
    reviewed: dict[str, dict[str, object]] | None = None
    if with_reviewed_independent_history:
        if (
            not same_day
            or expired_attempt
            or len(run_ids) != 1
            or job_input_hash(
                {
                    "account_id": str(account_id),
                    "cycle_id": str(cycle_id),
                    "run_id": str(run_ids[0]),
                }
            )
            != _REVIEWED_SEAL_IDENTITY
            or await session.scalar(text("SHOW transaction_read_only")) != "off"
            or await session.scalar(text("SHOW transaction_isolation")) != "read committed"
        ):
            raise ValueError(_BLOCKED)
        reviewed = {}
    if expired_attempt:
        from nexus.paper.manual_request import manual_request_for_cycle, read_manual_request

        sealed = [
            await legacy_research_sealed(session, cycle_id=cycle_id, run_id=run_id, now=now)
            for run_id in run_ids
        ]
        if all(sealed):
            target = await manual_request_for_cycle(session, cycle_id=cycle_id)
            if target is None or (
                target.account_id == account_id
                and (await read_manual_request(session, target)).status == "unavailable"
            ):
                # A verified historical receipt does not require today's legitimate
                # producer to stop. No closing, cancellation or new audit is done.
                return HistorySealReceipt(sealed_research=len(run_ids), replayed=True)
    manual = await _seal_manual_scope(
        session, account_id=account_id, cycle_id=cycle_id, now=now, reviewed_history=reviewed
    )
    projection = await load_paper_projection(session, account_id=account_id, as_of=now)
    await require_activation_projection_unoccupied(
        session, account_id=account_id, projection=projection
    )
    require_research_processes_quiescent()
    if reviewed is not None:
        recorded_at = await session.scalar(text("SELECT clock_timestamp()"))
        if not isinstance(recorded_at, datetime) or recorded_at < now:
            raise ValueError(_BLOCKED)
        now = recorded_at
    new = 0
    for run_id in run_ids:
        if await legacy_research_sealed(session, cycle_id=cycle_id, run_id=run_id, now=now):
            if reviewed is not None:
                await _require_reviewed_history_receipt(session, run_id, reviewed)
            continue
        proof = await _proof(
            session,
            cycle_id=cycle_id,
            run_id=run_id,
            now=now,
            same_day=same_day,
            expired_attempt=expired_attempt,
        )
        if reviewed is not None and job_input_hash(proof) != _REVIEWED_SEAL_PROOF:
            raise ValueError(_BLOCKED)
        seal = await append_audit_event(
            session,
            actor_id=_ACTOR,
            action=_ACTION,
            schema_version=_EXPIRED_SCHEMA
            if expired_attempt
            else _SAME_DAY_SCHEMA
            if same_day
            else _SCHEMA,
            payload=proof,
            occurred_at=now,
        )
        if reviewed is not None:
            await append_audit_event(
                session,
                actor_id=_ACTOR,
                action=_REVIEWED_HISTORY_ACTION,
                schema_version=_REVIEWED_HISTORY_SCHEMA,
                payload=_reviewed_history_payload(seal, reviewed),
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


async def _proof(
    session: AsyncSession,
    *,
    cycle_id: UUID,
    run_id: UUID,
    now: datetime,
    same_day: bool = False,
    expired_attempt: bool = False,
    read_only: bool = False,
) -> dict[str, object]:
    from typing import cast

    from nexus.agents.research_stages.public import _history_proof

    return cast(
        dict[str, object],
        await _history_proof(
            session,
            cycle_id=cycle_id,
            run_id=run_id,
            now=now,
            same_day=same_day,
            expired_attempt=expired_attempt,
            read_only=read_only,
        ),
    )
