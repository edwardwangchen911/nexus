import os
import subprocess
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from nexus.paper.history_seal import require_research_processes_quiescent

_DESKTOP = (
    "/Applications/ChatGPT.app/Contents/Resources/codex-cli/CodexCLI.app/Contents/MacOS/codex"
)
_APP = "/Applications/ChatGPT.app/Contents/MacOS/ChatGPT"
_BIRTH = "Mon Oct 12 10:00:00 2026"


@pytest.mark.parametrize(
    "fault",
    [
        None,
        "unknown-node",
        "foreign-user",
        "ancestor-restarted",
        "ancestor-reparented",
        "ancestor-user-changed",
        "ancestor-map",
        "ancestor-research-cwd",
        "relative-cwd",
        "missing-cwd",
        "cycle",
        "new-sdk",
    ],
)
def test_exact_desktop_bridge_requires_complete_verified_stable_chain(monkeypatch, fault):
    repl = "/Applications/ChatGPT.app/Contents/Resources/cua_node/bin/node_repl"
    node = "/Applications/ChatGPT.app/Contents/Resources/cua_node/bin/node"
    identities = {
        200: (300, _DESKTOP),
        300: (400, repl),
        400: (500, node),
        500: (100, _DESKTOP),
        100: (1, _APP),
    }
    scans = 0

    def execute(command, **kwargs):
        nonlocal scans
        assert not any(flag in command for flag in ("args", "command=", "-e"))
        if command[0] == "/bin/ps":
            scans += 1
            lines = []
            for pid, (parent, executable) in identities.items():
                if fault == "unknown-node" and pid == 400:
                    executable = "/usr/local/bin/node"
                if fault == "cycle" and pid == 500:
                    parent = 200
                if fault == "ancestor-reparented" and scans > 1 and pid == 400:
                    parent = 100
                uid = os.getuid() + 1 if fault == "foreign-user" and pid == 400 else os.getuid()
                if fault == "ancestor-user-changed" and scans > 1 and pid == 400:
                    uid += 1
                birth = (
                    "Mon Oct 12 10:01:00 2026"
                    if fault == "ancestor-restarted" and scans > 1 and pid == 400
                    else _BIRTH
                )
                uid_field = f"{uid} " if "uid=" in command[-1] else ""
                lines.append(f"{pid} {parent} {uid_field}{birth} {executable}")
            if fault == "new-sdk" and scans > 1:
                uid_field = f"{os.getuid()} " if "uid=" in command[-1] else ""
                lines.append(f"999 1 {uid_field}{_BIRTH} /test/codex_cli_bin/codex")
            raw = "\n".join(lines)
        elif command[0] == "/usr/sbin/lsof":
            pid = int(command[command.index("-p") + 1])
            executable = identities[pid][1]
            if fault == "ancestor-map" and pid == 400:
                executable = "/other/node"
            cwd = "/"
            if fault == "ancestor-research-cwd" and pid == 400:
                cwd = "/private/tmp/nexus-codex-controlled"
            if fault == "relative-cwd" and pid == 400:
                cwd = "relative"
            raw = f"p{pid}\nftxt\nn{executable}\n"
            if fault != "missing-cwd" or pid != 400:
                raw += f"fcwd\nn{cwd}\n"
        elif command[1] == "ps":
            raw = "abc123\n"
        else:
            raw = "PID COMMAND\n1 python\n"
        return subprocess.CompletedProcess(command, 0, raw)

    monkeypatch.setattr("nexus.paper.history_seal.subprocess.run", execute)
    if fault is None:
        require_research_processes_quiescent()
        assert scans == 2
    else:
        with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
            require_research_processes_quiescent()


@pytest.mark.asyncio
@pytest.mark.parametrize("other_status", ["active", "preparing", "unknown"])
async def test_exact_terminal_manual_seal_does_not_exempt_another_request(
    monkeypatch, other_status
):
    from nexus.paper.history_seal import _seal_manual_scope

    account_id, cycle_id = UUID(int=100), UUID(int=101)
    target = SimpleNamespace(id=UUID(int=102), account_id=account_id)
    other = SimpleNamespace(id=UUID(int=103), account_id=account_id)
    monkeypatch.setattr(
        "nexus.paper.manual_request.manual_request_for_cycle", AsyncMock(return_value=target)
    )
    monkeypatch.setattr(
        "nexus.paper.manual_request.read_manual_request",
        AsyncMock(
            side_effect=[
                SimpleNamespace(status="active", cycle_id=cycle_id),
                SimpleNamespace(status=other_status, cycle_id=UUID(int=999)),
            ]
        ),
    )
    session = SimpleNamespace(scalars=AsyncMock(return_value=(target, other)))
    with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
        await _seal_manual_scope(session, account_id=account_id, cycle_id=cycle_id)


def test_docker_top_uses_pid_column_required_by_docker_daemon(monkeypatch):
    def execute(command, **kwargs):
        if command[0] == "/bin/ps":
            return subprocess.CompletedProcess(
                command, 0, f"1 0 {os.getuid()} {_BIRTH} /usr/bin/python\n"
            )
        if command[1] == "ps":
            return subprocess.CompletedProcess(command, 0, "abc123\n")
        if command[-1] == "comm":
            return subprocess.CompletedProcess(command, 1, "")
        assert command[-1] == "pid,comm"
        return subprocess.CompletedProcess(command, 0, "PID COMMAND\n1 python\n")

    monkeypatch.setattr("nexus.paper.history_seal.subprocess.run", execute)
    require_research_processes_quiescent()


@pytest.mark.parametrize(
    "fault",
    [
        None,
        "child-map",
        "parent-map",
        "missing-cwd",
        "research-cwd",
        "parent-path",
        "child-path",
        "restarted",
        "reparented",
        "new-sdk",
        "lsof-failed",
    ],
)
def test_desktop_exclusion_requires_independent_stable_process_identity(monkeypatch, fault):
    scans = 0

    def execute(command, **kwargs):
        nonlocal scans
        if command[0] == "/bin/ps":
            scans += 1
            child = "/opt/homebrew/bin/codex" if fault == "child-path" else _DESKTOP
            parent = "/other/ChatGPT" if fault == "parent-path" else _APP
            birth = "Mon Oct 12 10:01:00 2026" if fault == "restarted" and scans > 1 else _BIRTH
            ppid = 1 if fault == "reparented" and scans > 1 else 100
            output = (
                f"100 1 {os.getuid()} {_BIRTH} {parent}\n200 {ppid} {os.getuid()} {birth} {child}\n"
            )
            if fault == "new-sdk" and scans > 1:
                output += f"300 1 {os.getuid()} {_BIRTH} /test/codex_cli_bin/codex\n"
        elif command[0] == "/usr/sbin/lsof":
            if fault == "lsof-failed":
                return subprocess.CompletedProcess(command, 1, "")
            pid = command[command.index("-p") + 1]
            executable = _DESKTOP if pid == "200" else _APP
            if fault == ("child-map" if pid == "200" else "parent-map"):
                executable = "/other/executable"
            output = f"p{pid}\nftxt\nn{executable}\n"
            if fault != "missing-cwd":
                cwd = "/private/tmp/nexus-codex-controlled" if fault == "research-cwd" else "/"
                output += f"fcwd\nn{cwd}\n"
        elif command[1] == "ps":
            output = "abc123\n"
        else:
            output = "PID COMMAND\n1 python\n"
        return subprocess.CompletedProcess(command, 0, output)

    monkeypatch.setattr("nexus.paper.history_seal.subprocess.run", execute)
    if fault is None:
        require_research_processes_quiescent()
        assert scans == 2
    else:
        with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
            require_research_processes_quiescent()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "priced",
        "tools",
        "reserved",
        "budget-reserved",
        "unknown-price",
        "new-version",
        "running-attempt",
        "retryable",
        "bad-event-hash",
    ],
)
async def test_frozen_failure_proof_rejects_unsafe_or_unknown_facts(fault):
    from nexus.agents.codex_sdk import MODEL_ID
    from nexus.agents.codex_versions import LEGACY_ACTIONABLE_CONFIG_VERSION
    from nexus.jobs.models import job_input_hash
    from nexus.paper.agent_cycle_models import canonical_cycle_event_hash
    from nexus.paper.history_seal import _proof

    at = datetime(2026, 10, 7, 14, tzinfo=UTC)
    cycle_id, run_id, instrument_id = (UUID(int=i) for i in (1, 2, 3))
    cycle = SimpleNamespace(execution_locked=True, decision_time=at)
    events = []
    prior = None
    for sequence, status in enumerate(("prepared", "researching", "unavailable"), 1):
        facts = {"research_run_ids": [str(run_id)]} if status == "researching" else {}
        event = SimpleNamespace(
            sequence=sequence,
            from_status=prior,
            to_status=status,
            result_code="CONTROLLED_FAILURE",
            fact_ids=facts,
            occurred_at=at,
            event_hash=canonical_cycle_event_hash(
                cycle_id=cycle_id,
                sequence=sequence,
                from_status=prior,
                to_status=status,
                result_code="CONTROLLED_FAILURE",
                fact_ids=facts,
                occurred_at=at,
            ),
        )
        events.append(event)
        prior = status
    run = SimpleNamespace(
        status="prepared",
        settled_cost_usd=None,
        input_tokens=None,
        output_tokens=None,
        retry_count=0,
        actor_id="local-agent-paper-cycle",
        instrument_id=instrument_id,
        idempotency_key=f"agent-paper:{cycle_id}:{instrument_id}",
        decision_time=at,
        reserved_cost_usd=0,
        pricing_version=LEGACY_ACTIONABLE_CONFIG_VERSION,
        model_hash="a" * 64,
        model_config_id=UUID(int=4),
        data_snapshot_id=UUID(int=5),
        input_hash="b" * 64,
        schema_hash="c" * 64,
        prompt_hash="d" * 64,
        evidence_hash="e" * 64,
        tool_hash="f" * 64,
    )
    model = SimpleNamespace(
        model_id=MODEL_ID,
        config_hash=run.model_hash,
        version=run.pricing_version,
        max_tool_calls=0,
        input_usd_per_million=0,
        output_usd_per_million=0,
    )
    budget = SimpleNamespace(
        reserved_usd=0, settled_usd=None, state="reserved", pricing_version=run.pricing_version
    )
    job = SimpleNamespace(
        id=UUID(int=6),
        status="dead_letter",
        finished_at=at,
        payload={"run_id": str(run_id)},
        scope_id=str(instrument_id),
        effective_at=at,
        retry_policy="automatic_safe",
        max_attempts=1,
        attempt_count=1,
        manual_retry_count=0,
        input_hash=job_input_hash(
            {
                "instrument_id": str(instrument_id),
                "data_snapshot_id": str(run.data_snapshot_id),
                "decision_time": at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "request_hash": run.input_hash,
                "schema_hash": run.schema_hash,
                "model_hash": run.model_hash,
                "prompt_hash": run.prompt_hash,
                "evidence_hash": run.evidence_hash,
                "tool_hash": run.tool_hash,
                "pricing_version": run.pricing_version,
            }
        ),
    )
    attempt = SimpleNamespace(
        outcome="lost", finished_at=at, lease_token=UUID(int=7), fence_token=1, attempt_no=1
    )
    lease = SimpleNamespace(
        expires_at=at + timedelta(seconds=30), lease_token=attempt.lease_token, fence_token=1
    )
    if fault == "priced":
        model.input_usd_per_million = 1
    elif fault == "tools":
        model.max_tool_calls = 1
    elif fault == "reserved":
        run.reserved_cost_usd = 1
    elif fault == "budget-reserved":
        budget.reserved_usd = 1
    elif fault == "unknown-price":
        model.output_usd_per_million = None
    elif fault == "new-version":
        run.pricing_version = "codex-sdk-chatgpt-v8-stage-capacity"
    elif fault == "running-attempt":
        attempt.outcome, attempt.finished_at = "running", None
    elif fault == "retryable":
        job.retry_policy = "manual_safe"
    elif fault == "bad-event-hash":
        events[-1].event_hash = "0" * 64
    session = SimpleNamespace(
        get=AsyncMock(side_effect=[cycle, run, model]),
        scalar=AsyncMock(side_effect=[None, budget]),
        scalars=AsyncMock(side_effect=[events, [job], [attempt], [lease], []]),
    )
    with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
        await _proof(session, cycle_id=cycle_id, run_id=run_id, now=at + timedelta(days=5))


@pytest.mark.parametrize(
    "boundary", ["host-live", "host-full-path", "container-live", "scan-failed", "no-container"]
)
def test_process_boundary_is_fail_closed(monkeypatch, boundary):
    def execute(command, **kwargs):
        if command[0] == "/bin/ps":
            if boundary == "scan-failed":
                raise subprocess.TimeoutExpired(command, 5)
            output = f"1 0 {os.getuid()} {_BIRTH} /usr/bin/python\n"
            if boundary == "host-live":
                output += f"2 1 {os.getuid()} {_BIRTH} /test/.venv/lib/codex_cli_bin/vendor/codex\n"
            if boundary == "host-full-path":
                output += f"2 1 {os.getuid()} {_BIRTH} /opt/homebrew/bin/codex\n"
        elif command[1] == "ps":
            output = "" if boundary == "no-container" else "abc123\n"
        else:
            output = "PID COMMAND\n1 python\n" + (
                "2 codex\n" if boundary == "container-live" else ""
            )
        return subprocess.CompletedProcess(command, 0, output)

    monkeypatch.setattr("nexus.paper.history_seal.subprocess.run", execute)
    with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
        require_research_processes_quiescent()


def test_quiescence_checks_host_and_existing_project_containers_without_arguments(monkeypatch):
    execute = Mock(
        side_effect=[
            subprocess.CompletedProcess([], 0, f"1 0 {os.getuid()} {_BIRTH} /usr/bin/python\n"),
            subprocess.CompletedProcess([], 0, "abc123\ndef456\n"),
            subprocess.CompletedProcess([], 0, "PID COMMAND\n1 python\n"),
            subprocess.CompletedProcess([], 0, "PID COMMAND\n1 uv\n"),
        ]
    )
    monkeypatch.setattr("nexus.paper.history_seal.subprocess.run", execute)
    require_research_processes_quiescent()
    commands = [call.args[0] for call in execute.call_args_list]
    assert commands[0] == ["/bin/ps", "-axo", "pid=,ppid=,uid=,lstart=,comm="]
    assert "label=com.docker.compose.project=nexus-fosun-live" in commands[1]
    assert commands[2:] == [
        ["docker", "top", identity, "-eo", "pid,comm"] for identity in ("abc123", "def456")
    ]


@pytest.mark.parametrize(
    "boundary,code",
    [
        ("ps", "HOST_SNAPSHOT_UNCONFIRMED"),
        ("lsof", "HOST_MAPPING_UNCONFIRMED"),
        ("identity", "HOST_IDENTITY_UNCONFIRMED"),
        ("changed", "HOST_CHANGED_DURING_CHECK"),
        ("docker-list", "CONTAINER_LIST_UNCONFIRMED"),
        ("docker-top", "CONTAINER_PROCESSES_UNCONFIRMED"),
    ],
)
def test_process_inspection_preserves_safe_failure_boundary(monkeypatch, boundary, code):
    from pathlib import Path

    from nexus.paper import history_seal
    from nexus.paper import manual_preparation_close as close

    scans = 0

    def execute(command, **kwargs):
        nonlocal scans
        if "status" in command:
            return subprocess.CompletedProcess(command, 0, '{"status":"inactive"}')
        if command[0] == "/bin/ps":
            scans += 1
            if boundary == "ps":
                raise OSError("private host details")
            executable = "/foreign/codex" if boundary == "identity" else _DESKTOP
            raw = (
                f"100 1 {os.getuid()} {_BIRTH} {_APP}\n"
                f"200 100 {os.getuid()} {_BIRTH} {executable}\n"
            )
            if boundary == "changed" and scans > 1:
                raw += f"300 1 {os.getuid()} {_BIRTH} /foreign/codex\n"
        elif command[0] == "/usr/sbin/lsof":
            if boundary == "lsof":
                return subprocess.CompletedProcess(command, 1, "private mapping details")
            pid = command[command.index("-p") + 1]
            executable = _DESKTOP if pid == "200" else _APP
            raw = f"p{pid}\nftxt\nn{executable}\nfcwd\nn/\n"
        elif command[1] == "ps":
            raw = "" if boundary == "docker-list" else "abcdef123456\n"
        else:
            raw = (
                "PID COMMAND\n1 codex\n" if boundary == "docker-top" else "PID COMMAND\n1 python\n"
            )
        return subprocess.CompletedProcess(command, 0, raw)

    monkeypatch.setattr(
        history_seal,
        "subprocess",
        SimpleNamespace(
            run=execute,
            SubprocessError=subprocess.SubprocessError,
            TimeoutExpired=subprocess.TimeoutExpired,
        ),
    )
    monkeypatch.setattr(
        close,
        "subprocess",
        SimpleNamespace(
            run=execute,
            SubprocessError=subprocess.SubprocessError,
            TimeoutExpired=subprocess.TimeoutExpired,
        ),
    )
    with pytest.raises(close.PreparationCloseDiagnosticError) as failure:
        close.require_preparation_producers_stopped(Path("/fixture"), Path("/fixture/env"))
    assert str(failure.value) == close.BLOCKED
    assert failure.value.safe_diagnostics() == {
        "diagnostic_stage": "research_processes",
        "diagnostic_code": code,
    }
