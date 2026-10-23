"""Full npm audit with one explicitly approved, expiring braces risk acceptance."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

EXPIRES_AT = datetime(2026, 10, 24, tzinfo=UTC)
APPROVED_AT = datetime(2026, 10, 15, tzinfo=UTC)
ADVISORY = "https://github.com/advisories/GHSA-vfj7-8cjw-p6xm"
LOCK_SHA256 = "1d5954e6a769aa6818e39758cdf3930ddf54fa369da8ce7b345d916cca971b25"
SURFACE_SHA256 = "66f1bbd0f769a0d62fec0e0b502320ba2b276e68115a724152d79d8c9b8ffa1e"
# Node paths are part of the acceptance, including the second fast-glob installation.
CHAIN = {
    "@next/eslint-plugin-next": ("fast-glob",),
    "braces": (),
    "fast-glob": ("micromatch",),
    "micromatch": ("braces",),
    "vinext": ("vite-plugin-commonjs",),
    "vite-plugin-commonjs": ("vite-plugin-dynamic-import",),
    "vite-plugin-dynamic-import": ("fast-glob",),
}
SOURCE_SUFFIXES = {".js", ".jsx", ".ts", ".tsx", ".mjs", ".cjs", ".mts", ".cts", ".json", ".jsonc"}
run_audit = subprocess.run


class AuditRejected(ValueError):
    """Fixed safe failure code; upstream stderr is never relayed."""


def file_digest(path: Path) -> str:
    if path.is_symlink() or not path.is_file():
        raise AuditRejected("AUDIT_IDENTITY_INVALID")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def surface_digest(root: Path) -> str:
    tracked = (
        subprocess.run(
            ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
            cwd=root,
            capture_output=True,
            check=True,
            timeout=15,
        )
        .stdout.decode()
        .split("\0")
    )
    paths = {p for p in tracked if p and Path(p).suffix in SOURCE_SUFFIXES}
    # Include ignored source/config files that a local build might still consume.
    paths.update(p.name for p in root.iterdir() if p.suffix in SOURCE_SUFFIXES)
    for directory in ("app", "worker"):
        base = root / directory
        if base.is_symlink():
            raise AuditRejected("AUDIT_IDENTITY_INVALID")
        if base.exists():
            for path in base.rglob("*"):
                if path.is_symlink():
                    raise AuditRejected("AUDIT_IDENTITY_INVALID")
                if path.is_file():
                    paths.add(path.relative_to(root).as_posix())
    digest = hashlib.sha256()
    for relative in sorted(paths):
        path = root / relative
        if any(parent.is_symlink() for parent in path.parents if parent != root.parent):
            raise AuditRejected("AUDIT_IDENTITY_INVALID")
        digest.update(relative.encode() + b"\0" + file_digest(path).encode() + b"\n")
    return digest.hexdigest()


def parse_report(raw: str) -> dict:
    def unique(pairs: list) -> dict:
        result = {}
        for key, value in pairs:
            if key in result:
                raise AuditRejected("AUDIT_REPORT_INVALID")
            result[key] = value
        return result

    try:
        report = json.loads(raw, object_pairs_hook=unique)
    except (ValueError, TypeError) as exc:
        raise AuditRejected("AUDIT_REPORT_INVALID") from exc
    if not isinstance(report, dict):
        raise AuditRejected("AUDIT_REPORT_INVALID")
    return report


def evaluate(
    report: dict, exit_code: int, root: Path, now: datetime | Callable[[], datetime]
) -> str:
    try:
        if report.get("auditReportVersion") != 2 or "error" in report:
            raise AuditRejected("AUDIT_REPORT_INVALID")
        vulnerabilities = report["vulnerabilities"]
        counts = report["metadata"]["vulnerabilities"]
        dependencies = report["metadata"]["dependencies"]
        lock = parse_report((root / "package-lock.json").read_text())
        packages = lock["packages"]
        if not isinstance(packages, dict) or "" not in packages:
            raise AuditRejected("AUDIT_DEPENDENCY_SCAN_INVALID")
        entries = [entry for path, entry in packages.items() if path]
        if any(not isinstance(entry, dict) for entry in entries):
            raise AuditRejected("AUDIT_DEPENDENCY_SCAN_INVALID")
        expected_dependencies = {
            "prod": 1 + sum(not entry.get("dev", False) for entry in entries),
            "dev": sum(bool(entry.get("dev", False)) for entry in entries),
            "optional": sum(bool(entry.get("optional", False)) for entry in entries),
            "peer": sum(bool(entry.get("peer", False)) for entry in entries),
            "peerOptional": sum(bool(entry.get("peerOptional", False)) for entry in entries),
            "total": len(entries),
        }
        if (
            not isinstance(dependencies, dict)
            or set(dependencies) != {"prod", "dev", "optional", "peer", "peerOptional", "total"}
            or any(type(value) is not int or value < 0 for value in dependencies.values())
            or dependencies != expected_dependencies
        ):
            raise AuditRejected("AUDIT_DEPENDENCY_SCAN_INVALID")
        empty = dict.fromkeys(("info", "low", "moderate", "high", "critical", "total"), 0)
        if not isinstance(vulnerabilities, dict) or not isinstance(counts, dict):
            raise AuditRejected("AUDIT_REPORT_INVALID")
        if any(type(value) is not int for value in counts.values()):
            raise AuditRejected("AUDIT_REPORT_INVALID")
        if not vulnerabilities and counts == empty and exit_code == 0:
            return "clean"
        if exit_code != 1 or set(vulnerabilities) != set(CHAIN):
            raise AuditRejected("AUDIT_UNACCEPTED_FINDING")
        if counts != {**empty, "high": 7, "total": 7}:
            raise AuditRejected("AUDIT_UNACCEPTED_FINDING")
        for name, expected_via in CHAIN.items():
            item = vulnerabilities[name]
            nodes = [f"node_modules/{name}"]
            if name == "fast-glob":
                nodes.append("node_modules/vite-plugin-dynamic-import/node_modules/fast-glob")
            if (
                item["name"] != name
                or item["severity"] != "high"
                or sorted(item["nodes"]) != sorted(nodes)
            ):
                raise AuditRejected("AUDIT_UNACCEPTED_FINDING")
            via = item["via"]
            if name != "braces":
                if sorted(via) != sorted(expected_via):
                    raise AuditRejected("AUDIT_UNACCEPTED_FINDING")
            else:
                if len(via) != 1 or not isinstance(via[0], dict):
                    raise AuditRejected("AUDIT_UNACCEPTED_FINDING")
                advisory = via[0]
                expected = {
                    "url": ADVISORY,
                    "name": "braces",
                    "dependency": "braces",
                    "severity": "high",
                    "range": "<=3.0.3",
                }
                if any(advisory.get(key) != value for key, value in expected.items()):
                    raise AuditRejected("AUDIT_UNACCEPTED_FINDING")
        if (
            file_digest(root / "package-lock.json") != LOCK_SHA256
            or surface_digest(root) != SURFACE_SHA256
        ):
            raise AuditRejected("AUDIT_ACCEPTANCE_IDENTITY_CHANGED")
        # Sample after both the external audit and potentially slow identity reads.
        checked_at = now() if callable(now) else now
        if not APPROVED_AT <= checked_at < EXPIRES_AT:
            raise AuditRejected("AUDIT_ACCEPTANCE_EXPIRED")
    except (KeyError, TypeError, AttributeError, OSError, subprocess.SubprocessError) as exc:
        raise AuditRejected("AUDIT_REPORT_OR_IDENTITY_INVALID") from exc
    return "risk_accepted_until_2026-10-24T00:00:00Z"


def run(root: Path, report_path: Path, now: datetime | Callable[[], datetime]) -> str:
    result = run_audit(
        [
            "npm",
            "audit",
            "--audit-level=high",
            "--json",
            "--include=dev",
            "--include=optional",
            "--include=peer",
        ],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    report_path.write_text(result.stdout)
    return evaluate(parse_report(result.stdout), result.returncode, root, now)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--report", type=Path, required=True, help="Full audit JSON outside the source tree"
    )
    args = parser.parse_args()
    try:
        status = run(Path(__file__).resolve().parents[2], args.report, lambda: datetime.now(UTC))
    except AuditRejected as exc:
        print(json.dumps({"status": "blocked", "code": str(exc)}))
        return 1
    except (OSError, subprocess.SubprocessError):
        print(json.dumps({"status": "blocked", "code": "AUDIT_EXECUTION_FAILED"}))
        return 1
    print(json.dumps({"status": status, "advisory": ADVISORY if status != "clean" else None}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
