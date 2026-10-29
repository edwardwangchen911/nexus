"""The approved temporary acceptance must never hide another audit failure."""

from __future__ import annotations

import copy
import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from scripts.ci import npm_audit_gate as gate
from scripts.ci.repository_policy import check_workflow


@pytest.fixture
def report() -> dict:
    return json.loads((Path(__file__).parent / "fixtures/npm-audit-braces.json").read_text())


@pytest.fixture
def root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    subprocess.run(["git", "init", "--quiet", str(tmp_path)], check=True)
    (tmp_path / "package-lock.json").write_bytes(
        (Path(__file__).parent / "fixtures/npm-audit-braces-package-lock.json").read_bytes()
    )
    (tmp_path / "package.json").write_text('{"private": true}')
    (tmp_path / "next.config.ts").write_text("export default {};\n")
    (tmp_path / "app").mkdir()
    (tmp_path / "app/page.tsx").write_text("export default () => null;\n")
    monkeypatch.setattr(gate, "LOCK_SHA256", gate.file_digest(tmp_path / "package-lock.json"))
    monkeypatch.setattr(gate, "SURFACE_SHA256", gate.surface_digest(tmp_path))
    return tmp_path


NOW = datetime(2026, 10, 15, tzinfo=UTC)


def test_exact_observed_report_is_accepted_but_not_called_clean(root, report):
    assert gate.evaluate(report, 1, root, NOW) == "risk_accepted_until_2026-10-24T00:00:00Z"


def test_user_approved_extension_remains_bounded(root, report):
    assert gate.evaluate(report, 1, root, datetime(2026, 10, 23, tzinfo=UTC)) == (
        "risk_accepted_until_2026-10-24T00:00:00Z"
    )
    with pytest.raises(gate.AuditRejected, match="AUDIT_ACCEPTANCE_EXPIRED"):
        gate.evaluate(report, 1, root, datetime(2026, 10, 24, tzinfo=UTC))


@pytest.mark.parametrize("offset", [timedelta(0), timedelta(microseconds=1)])
def test_expiration_is_exclusive(root, report, offset):
    with pytest.raises(gate.AuditRejected):
        gate.evaluate(report, 1, root, gate.EXPIRES_AT + offset)


@pytest.mark.parametrize(
    "path",
    [
        "package-lock.json",
        "package.json",
        "next.config.ts",
        "app/page.tsx",
        "app/new.ts",
        "eslint.config.mjs",
    ],
)
def test_dependency_or_source_change_revokes_acceptance(root, report, path):
    (root / path).write_text("changed")
    with pytest.raises(gate.AuditRejected):
        gate.evaluate(report, 1, root, NOW)


def test_symlink_surface_is_not_accepted(root, report):
    (root / "app/new.ts").symlink_to(root / "next.config.ts")
    with pytest.raises(gate.AuditRejected):
        gate.evaluate(report, 1, root, NOW)


@pytest.mark.parametrize(
    "mutation", ["extra", "url", "severity", "node", "via", "count", "error", "missing", "version"]
)
def test_any_unreviewed_or_malformed_report_fails(root, report, mutation):
    if mutation == "extra":
        report["vulnerabilities"]["other"] = copy.deepcopy(report["vulnerabilities"]["braces"])
    elif mutation == "url":
        report["vulnerabilities"]["braces"]["via"][0]["url"] += "-other"
    elif mutation == "severity":
        report["vulnerabilities"]["braces"]["via"][0]["severity"] = "critical"
    elif mutation == "node":
        report["vulnerabilities"]["braces"]["nodes"].append("node_modules/another/braces")
    elif mutation == "via":
        report["vulnerabilities"]["micromatch"]["via"] = []
    elif mutation == "count":
        report["metadata"]["vulnerabilities"]["low"] = 1
    elif mutation == "error":
        report["error"] = {}
    elif mutation == "missing":
        del report["metadata"]
    else:
        report["auditReportVersion"] = 3
    with pytest.raises(gate.AuditRejected):
        gate.evaluate(report, 1, root, NOW)


@pytest.mark.parametrize("exit_code", [0, 2, -1])
def test_inconsistent_npm_exit_is_not_accepted(root, report, exit_code):
    with pytest.raises(gate.AuditRejected):
        gate.evaluate(report, exit_code, root, NOW)


def test_clean_report_needs_no_expired_exception(root, report):
    report["vulnerabilities"] = {}
    report["metadata"]["vulnerabilities"] = dict.fromkeys(report["metadata"]["vulnerabilities"], 0)
    assert gate.evaluate(report, 0, root, gate.EXPIRES_AT) == "clean"
    with pytest.raises(gate.AuditRejected):
        gate.evaluate(report, 1, root, NOW)


def test_duplicate_json_fields_are_rejected():
    with pytest.raises(gate.AuditRejected):
        gate.parse_report('{"vulnerabilities":{},"vulnerabilities":{}}')


def test_normal_cli_keeps_complete_report_and_has_no_omit_flag(root, report, monkeypatch):
    calls = []

    def audit(command, **kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, 1, json.dumps(report), "")

    monkeypatch.setattr(gate, "run_audit", audit)
    target = root / "audit-report.txt"
    assert gate.run(root, target, NOW) == "risk_accepted_until_2026-10-24T00:00:00Z"
    assert json.loads(target.read_text()) == report
    assert calls == [
        [
            "npm",
            "audit",
            "--audit-level=high",
            "--json",
            "--include=dev",
            "--include=optional",
            "--include=peer",
        ]
    ]


def test_historical_report_keeps_its_exact_approved_lock():
    historical_lock = Path(__file__).parent / "fixtures/npm-audit-braces-package-lock.json"
    assert gate.file_digest(historical_lock) == gate.LOCK_SHA256


def test_current_checkout_rejects_historical_acceptance(report):
    # The dependency fix changes the inventory; the old report cannot certify it.
    with pytest.raises(gate.AuditRejected, match="^AUDIT_DEPENDENCY_SCAN_INVALID$"):
        gate.evaluate(report, 1, Path(__file__).resolve().parents[2], NOW)


def test_current_checkout_clean_scan_needs_no_risk_extension():
    report = json.loads(
        (Path(__file__).parent / "fixtures/npm-audit-clean-20261029.json").read_text()
    )
    assert gate.evaluate(report, 0, Path(__file__).resolve().parents[2], gate.EXPIRES_AT) == "clean"


@pytest.mark.parametrize("raw", ["not-json", "[]", '{"error":{}}'])
def test_invalid_upstream_payload_never_passes(root, raw):
    with pytest.raises(gate.AuditRejected):
        gate.evaluate(gate.parse_report(raw), 1, root, NOW)


def test_cli_transport_failure_is_safe_and_not_retried(tmp_path, monkeypatch, capsys):
    calls = []

    def unavailable(command, **kwargs):
        calls.append(command)
        raise subprocess.TimeoutExpired(command, 120, stderr="synthetic-sensitive-upstream")

    monkeypatch.setattr(gate, "run_audit", unavailable)
    monkeypatch.setattr(sys, "argv", ["audit", "--report", str(tmp_path / "report.json")])
    assert gate.main() == 1
    assert len(calls) == 1
    output = capsys.readouterr().out
    assert "AUDIT_EXECUTION_FAILED" in output
    assert "synthetic-sensitive-upstream" not in output


def test_default_workflow_cannot_silently_drop_the_audit_gate(tmp_path):
    source = Path(__file__).resolve().parents[2] / ".github/workflows/ci.yml"
    target = tmp_path / ".github/workflows/ci.yml"
    target.parent.mkdir(parents=True)
    target.write_text(source.read_text())
    assert check_workflow(tmp_path) == []
    target.write_text(target.read_text().replace("scripts/ci/npm_audit_gate.py", "missing_gate.py"))
    assert "ci_gate_missing" in {finding.rule for finding in check_workflow(tmp_path)}


@pytest.mark.parametrize("mutation", ["missing", "zero", "partial", "omit_dev"])
def test_clean_requires_a_complete_dependency_scan(root, report, mutation):
    report["vulnerabilities"] = {}
    report["metadata"]["vulnerabilities"] = dict.fromkeys(report["metadata"]["vulnerabilities"], 0)
    if mutation == "missing":
        del report["metadata"]["dependencies"]
    elif mutation == "zero":
        report["metadata"]["dependencies"] = dict.fromkeys(report["metadata"]["dependencies"], 0)
    elif mutation == "partial":
        report["metadata"]["dependencies"]["total"] -= 1
    else:
        report["metadata"]["dependencies"]["dev"] = 0
    with pytest.raises(gate.AuditRejected):
        gate.evaluate(report, 0, root, NOW)


def test_audit_crossing_expiry_is_rejected(root, report, monkeypatch):
    current = gate.EXPIRES_AT - timedelta(seconds=1)

    def audit(command, **kwargs):
        nonlocal current
        current = gate.EXPIRES_AT
        return subprocess.CompletedProcess(command, 1, json.dumps(report), "")

    monkeypatch.setattr(gate, "run_audit", audit)
    with pytest.raises(gate.AuditRejected, match="AUDIT_ACCEPTANCE_EXPIRED"):
        gate.run(root, root / "report.txt", lambda: current)


def test_identity_check_crossing_expiry_is_rejected(root, report, monkeypatch):
    current = gate.EXPIRES_AT - timedelta(seconds=1)
    original = gate.surface_digest

    def digest(path):
        nonlocal current
        current = gate.EXPIRES_AT
        return original(path)

    monkeypatch.setattr(gate, "surface_digest", digest)
    with pytest.raises(gate.AuditRejected, match="AUDIT_ACCEPTANCE_EXPIRED"):
        gate.evaluate(report, 1, root, lambda: current)
