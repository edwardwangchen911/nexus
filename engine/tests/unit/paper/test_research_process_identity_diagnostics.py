"""Identity rejection keeps its cause through the normal safe CLI envelope."""

import json
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
from nexus.ops import manual_paper
from nexus.paper import history_seal
from nexus.paper import manual_preparation_close as close

_DESKTOP = (
    "/Applications/ChatGPT.app/Contents/Resources/codex-cli/CodexCLI.app/Contents/MacOS/codex"
)
_BIRTH = "Mon Oct 12 10:00:00 2026"
_APERITIF = (
    "/Applications/ChatGPT.app/Contents/Frameworks/Codex Framework.framework/"
    "Versions/158.0.8127.37/Helpers/Codex (Aperitif).app/Contents/MacOS/Codex (Aperitif)"
)


@pytest.mark.parametrize(
    "scenario",
    [
        "valid",
        "maintenance",
        "wrong_installation",
        "wrong_version",
        "inserted_parent",
        "wrong_uid",
        "bad_live_signature",
        "bad_disk_signature",
        "signature_timeout",
        "wrong_inode",
        "wrong_device",
        "duplicate_mapping",
        "changed_inode",
        "second_signature_failure",
        "changed_cwd",
        "research_cwd",
        "unconfirmed_cwd",
        "replaced_process",
        "changed_parent",
        "additional_codex",
        "codex_after_container_check",
        "missing_containers",
        "container_codex",
    ],
)
def test_aperitif_is_verified_only_by_normal_formal_process_gate(monkeypatch, scenario):
    from nexus.ops.agent_paper_recovery import require_recovery_process_quiescence

    paths = {
        201: _DESKTOP,
        202: _APERITIF,
        203: history_seal._DESKTOP_PARENT,
    }
    if scenario == "wrong_installation":
        paths[202] = _APERITIF.replace("/Applications/", "/tmp/Applications/")
    if scenario == "wrong_version":
        paths[202] = _APERITIF.replace("158.0.8127.37", "158.0.8127.38")
    pids = {201: 202, 202: 203, 203: 1}
    if scenario == "inserted_parent":
        pids[202] = 204
        pids[204] = 203
        paths[204] = "/usr/bin/sh"
    if scenario == "additional_codex":
        pids[205] = 1
        paths[205] = "/tmp/extra/codex"

    inode_by_path = {path: 1000 + pid for pid, path in paths.items()}
    inode_by_path.update(
        {
            "/Users/test/workbench": 4001,
            "/Users/test/other": 4002,
            "/private/tmp/nexus-codex-job": 4003,
        }
    )
    identifiers = {
        _DESKTOP: ("codex", "9aca7e9e8b34af80dec29d17468073b262c26cbb"),
        _APERITIF: ("com.openai.codex.helper", "bb8eeee22cd0fa7d12768e3817290e4598fd4ef3"),
        history_seal._DESKTOP_PARENT: (
            "com.openai.codex",
            "b5215002d934d1b51fa2dba60375f702e4789f28",
        ),
    }
    snapshots = 0
    container_checks = 0
    mapping_reads = {}
    signatures = []

    def execute(command, **kwargs):
        nonlocal snapshots, container_checks
        if command[0] == "/bin/ps":
            snapshots += 1
            lines = []
            for pid, path in paths.items():
                parent = (
                    401
                    if scenario == "changed_parent" and snapshots > 1 and pid == 202
                    else pids[pid]
                )
                birth = (
                    "Mon Oct 12 10:00:01 2026"
                    if scenario == "replaced_process" and snapshots > 1 and pid == 201
                    else _BIRTH
                )
                owner = os.getuid() + (scenario == "wrong_uid")
                lines.append(f"{pid} {parent} {owner} {birth} {path}")
            if scenario == "codex_after_container_check" and snapshots == 2:
                assert container_checks == 1
                lines.append(f"205 1 {os.getuid()} {_BIRTH} /tmp/late/codex")
            result = "\n".join(lines)
        elif command[0] == "/usr/sbin/lsof":
            pid = int(command[command.index("-p") + 1])
            if command[-1] == "-Fpfn":
                result = f"p{pid}\nftxt\nn{paths[pid]}\nfcwd\nn/\n"
            else:
                assert command[-1] == "-F0pfDin"
                mapping_reads[pid] = mapping_reads.get(pid, 0) + 1
                inode = inode_by_path[paths[pid]]
                if pid == 202 and (
                    scenario == "wrong_inode"
                    or scenario == "changed_inode"
                    and mapping_reads[pid] > 1
                ):
                    inode += 1
                cwd = "/Users/test/workbench"
                if scenario == "changed_cwd" and pid == 202 and mapping_reads[pid] > 1:
                    cwd = "/Users/test/other"
                if scenario == "research_cwd" and pid == 202:
                    cwd = "/private/tmp/nexus-codex-job"
                cwd_record = f"fcwd\0D0x11\0i{inode_by_path[cwd]}\0n{cwd}\0\n"
                if scenario == "unconfirmed_cwd" and pid == 202:
                    cwd_record = ""
                device = "0x12" if pid == 202 and scenario == "wrong_device" else "0x11"
                executable_record = f"ftxt\0D{device}\0i{inode}\0n{paths[pid]}\0\n"
                if scenario == "duplicate_mapping" and pid == 202:
                    executable_record *= 2
                result = f"p{pid}\0\n{executable_record}{cwd_record}"
        elif command[0] == "/usr/bin/codesign":
            assert command[:3] == ["/usr/bin/codesign", "--verify", "--strict"]
            subject = command[-1]
            path = paths[int(subject)] if subject.isdecimal() else subject
            identifier, digest = identifiers[path]
            requirement = command[-2]
            assert "anchor apple generic" in requirement
            assert f'identifier "{identifier}"' in requirement
            assert 'certificate leaf[subject.OU] = "2DC432GLL2"' in requirement
            assert f'cdhash H"{digest}"' in requirement
            signatures.append(subject)
            if path == _APERITIF:
                if scenario == "signature_timeout":
                    raise subprocess.TimeoutExpired(command, timeout=kwargs["timeout"])
                if scenario == "second_signature_failure" and signatures.count(subject) == 2:
                    return subprocess.CompletedProcess(command, 1, "", "/private/canary")
                if (scenario == "bad_live_signature" and subject.isdecimal()) or (
                    scenario == "bad_disk_signature" and not subject.isdecimal()
                ):
                    return subprocess.CompletedProcess(command, 1, "", "/private/canary")
            result = ""
        elif command[1] == "ps":
            result = "" if scenario == "missing_containers" else "abc123\n"
        else:
            container_checks += 1
            result = "PID COMMAND\n1 python\n"
            if scenario == "container_codex":
                result += "2 codex\n"
        return subprocess.CompletedProcess(command, 0, result, "")

    def identity(path):
        return (17, inode_by_path[str(path)], 100, 200, 300)

    monkeypatch.setattr(history_seal.subprocess, "run", execute)
    monkeypatch.setattr(history_seal, "_aperitif_file_identity", identity, raising=False)
    if scenario == "valid":
        require_recovery_process_quiescence()
        assert snapshots == 2 and container_checks == 1
        assert mapping_reads == {201: 4, 202: 4, 203: 4}
        assert set(signatures) == {"201", "202", "203", *paths.values()}
        assert all(signatures.count(subject) == 2 for subject in signatures)
    else:
        check = require_recovery_process_quiescence
        if scenario == "maintenance":
            check = history_seal.require_research_processes_quiescent
        with pytest.raises(history_seal.ResearchProcessDiagnosticError) as error:
            check()
        assert "canary" not in json.dumps(error.value.safe_diagnostics())
        if scenario in {"wrong_inode", "wrong_device", "duplicate_mapping", "unconfirmed_cwd"}:
            assert error.value.code == "HOST_MAPPING_UNCONFIRMED"
        if scenario in {
            "bad_live_signature",
            "bad_disk_signature",
            "signature_timeout",
            "second_signature_failure",
        }:
            assert error.value.code == "HOST_SIGNATURE_UNCONFIRMED"
        if scenario == "missing_containers":
            assert error.value.code == "CONTAINER_LIST_UNCONFIRMED"
        if scenario == "container_codex":
            assert error.value.code == "CONTAINER_PROCESSES_UNCONFIRMED"
        if scenario == "codex_after_container_check":
            assert error.value.code == "HOST_CHANGED_DURING_CHECK"
        if scenario == "maintenance":
            assert not signatures


def test_aperitif_mapping_rejects_relative_paths_and_symlinks(tmp_path):
    target = tmp_path / "target"
    target.write_bytes(b"file")
    alias = tmp_path / "alias"
    alias.symlink_to(target)
    assert history_seal._aperitif_file_identity(target.resolve())[:2] == (
        target.stat().st_dev,
        target.stat().st_ino,
    )
    for path in (Path("target"), alias.resolve().parent / alias.name):
        with pytest.raises(ValueError, match="noncanonical process mapping"):
            history_seal._aperitif_file_identity(path)


@pytest.mark.parametrize(
    "scenario",
    [
        "valid",
        "historical",
        "extra_node",
        "wrong_path",
        "wrong_owner",
        "wrong_mapping",
        "research_cwd",
        "ambiguous_cwd",
        "changed_birth",
        "changed_parent",
        "other_candidate",
        "running_container_codex",
    ],
)
def test_formal_recovery_classifies_only_exact_repl_ancestry(monkeypatch, scenario):
    from nexus.ops.agent_paper_recovery import require_recovery_process_quiescence

    uid = os.getuid()
    paths = [
        _DESKTOP,
        history_seal._DESKTOP_REPL,
        _DESKTOP,
        history_seal._DESKTOP_PARENT,
    ]
    if scenario == "extra_node":
        paths.insert(2, "/usr/bin/sh")
    if scenario == "wrong_path":
        paths[1] = "/tmp/node_repl"
    snapshots = 0
    container_checks = []

    def execute(command, **kwargs):
        nonlocal snapshots
        if command[0] == "/bin/ps":
            snapshots += 1
            rows = []
            for index, executable in enumerate(paths):
                pid, parent = index + 200, index + 201
                owner = uid + 1 if scenario == "wrong_owner" and index == 1 else uid
                born = _BIRTH
                if snapshots == 2 and index == 1:
                    if scenario == "changed_birth":
                        born = "Mon Oct 12 10:00:01 2026"
                    if scenario == "changed_parent":
                        parent = 400
                rows.append(f"{pid} {parent} {owner} {born} {executable}")
            if scenario == "other_candidate":
                rows.append(f"400 0 {uid} {_BIRTH} /opt/unowned/codex")
            output = "\n".join(rows)
        elif command[0] == "/usr/sbin/lsof":
            pid = int(command[command.index("-p") + 1])
            mapped = paths[pid - 200]
            cwd = "/Users/test/workbench"
            if pid == 201:
                if scenario == "wrong_mapping":
                    mapped = "/tmp/node_repl"
                if scenario == "research_cwd":
                    cwd = "/tmp/nexus-codex-unknown/run"
            output = f"p{pid}\nftxt\nn{mapped}\nfcwd\nn{cwd}\n"
            if pid == 201 and scenario == "ambiguous_cwd":
                output += "fcwd\nn/tmp/other\n"
        elif command[1] == "ps":
            output = "abc123\n"
        else:
            container_checks.append(command)
            output = "PID COMMAND\n1 python\n"
            if scenario == "running_container_codex":
                output += "2 codex\n"
        return subprocess.CompletedProcess(command, 0, output)

    monkeypatch.setattr(history_seal.subprocess, "run", execute)
    if scenario == "valid":
        require_recovery_process_quiescence()
        assert snapshots == 2 and len(container_checks) == 1
    else:
        check = (
            history_seal.require_research_processes_quiescent
            if scenario == "historical"
            else require_recovery_process_quiescence
        )
        with pytest.raises(history_seal.ResearchProcessDiagnosticError):
            check()


@pytest.mark.parametrize("wrapped", [False, True])
@pytest.mark.parametrize(
    "fault,reason",
    [
        ("missing", "ANCESTOR_MISSING"),
        ("cycle", "ANCESTOR_CYCLE"),
        ("owner", "OWNER_MISMATCH"),
        ("executable", "EXECUTABLE_MISMATCH"),
    ],
)
def test_identity_cause_survives_guard_and_cli_without_host_details(
    monkeypatch, tmp_path, capsys, wrapped, fault, reason
):
    def execute(command, **kwargs):
        if "status" in command:
            return subprocess.CompletedProcess(command, 0, '{"status":"inactive"}')
        if command[0] == "/bin/ps":
            parent = 200 if fault == "cycle" else 999
            uid = os.getuid() + 1 if fault == "owner" else os.getuid()
            executable = "/private/credential-marker/codex" if fault == "executable" else _DESKTOP
            raw = f"200 {parent} {uid} {_BIRTH} {executable}\n"
        elif command[0] == "/usr/sbin/lsof":
            raw = f"p200\nftxt\nn{_DESKTOP}\nfcwd\nn/\n"
        else:
            raise AssertionError("Identity rejection must precede container inspection")
        return subprocess.CompletedProcess(command, 0, raw)

    fake = SimpleNamespace(
        run=execute,
        SubprocessError=subprocess.SubprocessError,
        TimeoutExpired=subprocess.TimeoutExpired,
    )
    monkeypatch.setattr(history_seal, "subprocess", fake)
    monkeypatch.setattr(close, "subprocess", fake)

    async def run(*_):
        if wrapped:
            close.require_preparation_producers_stopped(tmp_path, tmp_path / "env")
        else:
            history_seal.require_research_processes_quiescent()

    monkeypatch.setattr(manual_paper, "run", run)
    monkeypatch.setattr(manual_paper, "_validate_local_env", lambda root, path: path)
    monkeypatch.setattr(manual_paper, "load_agent_paper_service_environment", lambda *a, **k: {})
    monkeypatch.setattr(manual_paper, "_checkout_revision", lambda root: "a" * 40)
    args = ["--repo-root", str(tmp_path)]
    if wrapped:
        args += [
            "check-preparation-acceptance",
            "--request-id",
            "00000000-0000-0000-0000-000000000001",
        ]
    else:
        args += [
            "seal-failed-research",
            "--cycle-id",
            "00000000-0000-0000-0000-000000000001",
            "--run-id",
            "00000000-0000-0000-0000-000000000002",
            "--accept-unknown-usage",
        ]
    assert manual_paper.main(args) == 1
    captured = capsys.readouterr()
    assert captured.err == ""
    assert json.loads(captured.out) == {
        "status": "blocked",
        "error_code": "MANUAL_PREPARATION_CLOSE_BLOCKED"
        if wrapped
        else "MANUAL_ACCOUNT_BUSY_OR_UNKNOWN",
        "execution_locked": True,
        "diagnostic_stage": "research_processes",
        "diagnostic_code": "HOST_IDENTITY_UNCONFIRMED",
        "diagnostic_reason": reason,
        "diagnostic_chain": "nested",
        "diagnostic_hop": "repl" if fault in {"missing", "cycle"} else "candidate",
        "diagnostic_observed": {
            "missing": "missing",
            "cycle": "codex",
            "owner": "codex",
            "executable": "other",
        }[fault],
    }
    assert "credential-marker" not in captured.out and _DESKTOP not in captured.out


@pytest.mark.parametrize("reason", [None, "private secret", ["OWNER_MISMATCH"]])
@pytest.mark.parametrize("wrapped", [False, True])
def test_untrusted_identity_reason_is_omitted(monkeypatch, reason, wrapped):
    error = history_seal.ResearchProcessDiagnosticError("HOST_IDENTITY_UNCONFIRMED", reason=reason)
    if wrapped:
        error = close.PreparationCloseDiagnosticError(
            "research_processes", "HOST_IDENTITY_UNCONFIRMED", reason=reason
        )
    assert error.safe_diagnostics() == {
        "diagnostic_stage": "research_processes",
        "diagnostic_code": "HOST_IDENTITY_UNCONFIRMED",
    }


@pytest.mark.parametrize("wrapped", [False, True])
def test_identity_reason_cannot_be_attached_to_another_boundary(wrapped):
    if wrapped:
        error = close.PreparationCloseDiagnosticError(
            "host_status", "INSPECTION_FAILED", reason="OWNER_MISMATCH"
        )
    else:
        error = history_seal.ResearchProcessDiagnosticError(
            "HOST_MAPPING_UNCONFIRMED", reason="OWNER_MISMATCH"
        )
    assert "diagnostic_reason" not in error.safe_diagnostics()


@pytest.mark.parametrize("hop", [0, 1, 2, 3, 4])
def test_nested_identity_reports_exact_failed_hop(monkeypatch, hop):
    paths = [
        _DESKTOP,
        history_seal._DESKTOP_REPL,
        history_seal._DESKTOP_NODE,
        _DESKTOP,
        history_seal._DESKTOP_PARENT,
    ]
    roles = ["candidate", "repl", "node", "ancestor_codex", "desktop"]

    def execute(command, **kwargs):
        assert kwargs["timeout"] == 5
        if command[0] == "/bin/ps":
            # Only the first codex candidate is mismatched in place; the parent
            # codex is omitted from candidate discovery when it is the bad hop.
            lines = [
                f"{200 + i} {201 + i} {os.getuid()} {_BIRTH} "
                + (
                    path
                    if i != hop
                    else "/private/secret/codex"
                    if i == 0
                    else "/private/secret/other"
                )
                for i, path in enumerate(paths)
            ]
            raw = "\n".join(lines) + "\n"
        elif command[0] == "/usr/sbin/lsof":
            pid = int(command[command.index("-p") + 1])
            raw = f"p{pid}\nftxt\nn{paths[pid - 200]}\nfcwd\nn/\n"
        else:
            raise AssertionError("must reject before container check")
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
    with pytest.raises(history_seal.ResearchProcessDiagnosticError) as error:
        history_seal.require_research_processes_quiescent()
    d = error.value.safe_diagnostics()
    assert d["diagnostic_reason"] == "EXECUTABLE_MISMATCH"
    assert d["diagnostic_chain"] == "nested"
    assert d["diagnostic_hop"] == roles[hop]
    assert d["diagnostic_observed"] == "other"
    assert "private" not in json.dumps(d) and "secret" not in json.dumps(d)


@pytest.mark.parametrize("value", ["private-secret", ["nested"], None])
def test_identity_context_rejects_untrusted_values(value):
    error = history_seal.ResearchProcessDiagnosticError(
        "HOST_IDENTITY_UNCONFIRMED",
        reason="EXECUTABLE_MISMATCH",
        chain=value,
        hop=value,
        observed=value,
    )
    assert set(error.safe_diagnostics()) == {
        "diagnostic_stage",
        "diagnostic_code",
        "diagnostic_reason",
    }
