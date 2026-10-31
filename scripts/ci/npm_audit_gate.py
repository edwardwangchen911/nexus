"""Full npm audit with exact, separately approved and expiring exceptions."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

EXPIRES_AT = datetime(2026, 10, 24, tzinfo=UTC)
APPROVED_AT = datetime(2026, 10, 15, tzinfo=UTC)
ADVISORY = "https://github.com/advisories/GHSA-vfj7-8cjw-p6xm"
LOCK_SHA256 = "e4e329ece76151c267e88db423ea7e7973f21d37c3c96a5cb78e549dc676aa1c"
SURFACE_SHA256 = "c605a2566dca905b93142dad3cd16962238c33d815d2ade0fcb931cb4caa67a0"
# User accepted this exact residual scope for candidate 2a33aaa6 on 2026-10-31.
# Effective after the recorded approval; never extend with a later build.
MINIMATCH_ACTIVE_FROM = datetime(2026, 10, 31, 3, 34, 37, tzinfo=UTC)
MINIMATCH_EXPIRES_AT = datetime(2026, 11, 1, tzinfo=UTC)
MINIMATCH_ADVISORY = "https://github.com/advisories/GHSA-g3hv-75wg-m249"
MINIMATCH_GRAPH_SHA256 = "0b0a5266fdf557f0297f1028da05736ea41a4f8b6d8b3de6dcd9a94140fadd80"
MINIMATCH_LOCK_SHA256 = "2f7aec4834ba4069f52a20e7e312f5a49f6e3c857ff6bb2964c152dab3ae54cc"
MINIMATCH_SURFACE_SHA256 = "ae52e4dc9e6fcbb59a454ac781193b630e6aa0fa907fd8e29234a64c9f8e2d43"
MINIMATCH_TREE_SHA256 = {
    "node_modules/minimatch": "c8690806d606597f04633171624e8266a579e5ed8da5b6a89016a621fa74e240",
    "node_modules/@redocly/openapi-core/node_modules/minimatch": (
        "6f63f3a46ea889d895892f1d073f47d57bbe82930fd0b58562282ad867c0c08a"
    ),
    # A patched lock alone must not certify an older, still-installed parser.
    "node_modules/@typescript-eslint/typescript-estree/node_modules/minimatch": (
        "ca5e627f2ea48f57b07deff39ffaa92af2f3293a04f769f9d852529db76b91d4"
    ),
    "node_modules/@isaacs/nonrecursive-glob-match": (
        "456fbb3d95737f98d07a75d60877a64ee34132923cd889e87c126a0a88173714"
    ),
}
MINIMATCH_GENERATOR_INPUTS = (
    "engine/src/nexus/main.py",
    "engine/src/nexus/settings.py",
    "pyproject.toml",
    "engine/pyproject.toml",
    "uv.lock",
)
MINIMATCH_RESOLUTION_EDGES = (
    ("node_modules/@eslint/config-array/dist/cjs/index.cjs", "minimatch", "node_modules/minimatch"),
    ("node_modules/@eslint/config-array/dist/esm/index.js", "minimatch", "node_modules/minimatch"),
    ("node_modules/@eslint/eslintrc/dist/eslintrc.cjs", "minimatch", "node_modules/minimatch"),
    (
        "node_modules/@eslint/eslintrc/lib/config-array/override-tester.js",
        "minimatch",
        "node_modules/minimatch",
    ),
    (
        "node_modules/eslint/lib/cli-engine/file-enumerator.js",
        "minimatch",
        "node_modules/minimatch",
    ),
    ("node_modules/eslint/lib/eslint/eslint-helpers.js", "minimatch", "node_modules/minimatch"),
    (
        "node_modules/eslint-plugin-jsx-a11y/lib/util/mayContainChildComponent.js",
        "minimatch",
        "node_modules/minimatch",
    ),
    (
        "node_modules/eslint-plugin-jsx-a11y/lib/util/mayHaveAccessibleLabel.js",
        "minimatch",
        "node_modules/minimatch",
    ),
    (
        "node_modules/eslint-plugin-react/lib/rules/forbid-component-props.js",
        "minimatch",
        "node_modules/minimatch",
    ),
    (
        "node_modules/eslint-plugin-react/lib/rules/jsx-handler-names.js",
        "minimatch",
        "node_modules/minimatch",
    ),
    (
        "node_modules/eslint-plugin-react/lib/rules/jsx-pascal-case.js",
        "minimatch",
        "node_modules/minimatch",
    ),
    (
        "node_modules/eslint-plugin-react/lib/rules/no-danger.js",
        "minimatch",
        "node_modules/minimatch",
    ),
    (
        "node_modules/eslint-plugin-react/lib/rules/no-unstable-nested-components.js",
        "minimatch",
        "node_modules/minimatch",
    ),
    (
        "node_modules/@redocly/openapi-core/lib/utils.js",
        "minimatch",
        "node_modules/@redocly/openapi-core/node_modules/minimatch",
    ),
    (
        "node_modules/@typescript-eslint/typescript-estree/dist/useProgramFromProjectService.js",
        "minimatch",
        "node_modules/@typescript-eslint/typescript-estree/node_modules/minimatch",
    ),
    (
        "node_modules/@typescript-eslint/typescript-estree/node_modules/minimatch/dist/commonjs/index.js",
        "@isaacs/nonrecursive-glob-match",
        "node_modules/@isaacs/nonrecursive-glob-match",
    ),
    (
        "node_modules/@typescript-eslint/typescript-estree/node_modules/minimatch/dist/esm/index.js",
        "@isaacs/nonrecursive-glob-match",
        "node_modules/@isaacs/nonrecursive-glob-match",
    ),
)
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


def installed_tree_digest(root: Path, relative: str) -> str:
    """Bind the inspected installed implementation, not just its package name."""
    base = root / relative
    if not base.is_dir() or base.is_symlink() or base.resolve() != base.absolute():
        raise AuditRejected("AUDIT_ACCEPTANCE_IDENTITY_CHANGED")
    digest = hashlib.sha256()
    files = 0
    for path in sorted(base.rglob("*")):
        name = path.relative_to(base)
        if "node_modules" in name.parts:
            continue
        if path.is_symlink():
            raise AuditRejected("AUDIT_ACCEPTANCE_IDENTITY_CHANGED")
        if path.is_file():
            digest.update(name.as_posix().encode() + b"\0" + file_digest(path).encode() + b"\n")
            files += 1
    if not files:
        raise AuditRejected("AUDIT_ACCEPTANCE_IDENTITY_CHANGED")
    return digest.hexdigest()


def verify_minimatch_resolution(root: Path) -> None:
    """Check only the accepted packages, including ignored nested installations."""
    base = root / "node_modules"
    if base.is_symlink() or any(os.environ.get(key) for key in ("NODE_PATH", "NODE_OPTIONS")):
        raise AuditRejected("AUDIT_ACCEPTANCE_IDENTITY_CHANGED")
    found = set()

    def refuse_unreadable_tree(_error: OSError) -> None:
        raise AuditRejected("AUDIT_ACCEPTANCE_IDENTITY_CHANGED")

    for directory, directories, _ in os.walk(
        base, followlinks=False, onerror=refuse_unreadable_tree
    ):
        for name in directories:
            path = Path(directory) / name
            if name == "node_modules" and path.is_symlink():
                raise AuditRejected("AUDIT_ACCEPTANCE_IDENTITY_CHANGED")
            selected = (name == "minimatch" and path.parent.name == "node_modules") or (
                name == "nonrecursive-glob-match"
                and path.parent.name == "@isaacs"
                and path.parent.parent.name == "node_modules"
            )
            if selected:
                if path.is_symlink():
                    raise AuditRejected("AUDIT_ACCEPTANCE_IDENTITY_CHANGED")
                found.add(path.relative_to(root).as_posix())
    if found != set(MINIMATCH_TREE_SHA256):
        raise AuditRejected("AUDIT_ACCEPTANCE_IDENTITY_CHANGED")
    # require.resolve checks real Node search precedence without executing packages.
    script = """
const fs = require('node:fs'), path = require('node:path');
const edges = JSON.parse(process.argv[1]);
console.log(JSON.stringify(edges.map(([caller, spec]) => {
  if (fs.realpathSync(caller) !== path.resolve(caller)) throw new Error('identity');
  return fs.realpathSync(require.resolve(spec, {paths:[path.dirname(caller)]}));
})));
"""
    try:
        result = subprocess.run(
            [
                "node",
                "--input-type=commonjs",
                "-e",
                script,
                json.dumps(
                    [[str(root / caller), spec] for caller, spec, _ in MINIMATCH_RESOLUTION_EDGES]
                ),
            ],
            capture_output=True,
            text=True,
            timeout=5,
            check=True,
        )
        resolved = json.loads(result.stdout)
        if len(resolved) != len(MINIMATCH_RESOLUTION_EDGES) or any(
            not Path(path).is_relative_to(root / expected)
            for path, (_, _, expected) in zip(resolved, MINIMATCH_RESOLUTION_EDGES, strict=True)
        ):
            raise AuditRejected("AUDIT_ACCEPTANCE_IDENTITY_CHANGED")
    except (OSError, ValueError, TypeError, subprocess.SubprocessError) as exc:
        raise AuditRejected("AUDIT_ACCEPTANCE_IDENTITY_CHANGED") from exc


def minimatch_surface_digest(root: Path) -> str:
    # Normal generation uses a local for_test schema and no user config.
    # New Redocly configuration is a new pattern/options source outside approval.
    if any(
        (root / name).exists() or (root / name).is_symlink()
        for name in (
            "redocly.yaml",
            "redocly.yml",
            ".redocly.yaml",
            ".redocly.yml",
        )
    ):
        raise AuditRejected("AUDIT_ACCEPTANCE_IDENTITY_CHANGED")
    digest = hashlib.sha256(surface_digest(root).encode())
    for name in (".github/workflows/ci.yml", *MINIMATCH_GENERATOR_INPUTS):
        digest.update(name.encode() + b"\0" + file_digest(root / name).encode() + b"\n")
    return digest.hexdigest()


def minimatch_graph_digest(vulnerabilities: dict) -> str:
    # Suggestions and ordering are not authority; every actual edge, path and
    # advisory identity is. A second advisory always changes this projection.
    graph = {}
    keys = ("url", "name", "dependency", "severity", "range")
    for name, item in vulnerabilities.items():
        via = [
            {"package": entry} if isinstance(entry, str) else {key: entry[key] for key in keys}
            for entry in item["via"]
        ]
        graph[name] = {
            "name": item["name"],
            "severity": item["severity"],
            "nodes": sorted(item["nodes"]),
            "via": sorted(via, key=lambda entry: json.dumps(entry, sort_keys=True)),
        }
    return hashlib.sha256(
        json.dumps(graph, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def evaluate_minimatch(vulnerabilities, counts, exit_code, root, now):
    expected = {"info": 0, "low": 0, "moderate": 14, "high": 0, "critical": 0, "total": 14}
    # The unchanged normal command uses --audit-level=high: moderate-only is 0.
    if (
        exit_code != 0
        or counts != expected
        or minimatch_graph_digest(vulnerabilities) != MINIMATCH_GRAPH_SHA256
    ):
        raise AuditRejected("AUDIT_UNACCEPTED_FINDING")
    if (
        file_digest(root / "package-lock.json") != MINIMATCH_LOCK_SHA256
        or minimatch_surface_digest(root) != MINIMATCH_SURFACE_SHA256
        or any(
            installed_tree_digest(root, path) != digest
            for path, digest in MINIMATCH_TREE_SHA256.items()
        )
    ):
        raise AuditRejected("AUDIT_ACCEPTANCE_IDENTITY_CHANGED")
    verify_minimatch_resolution(root)
    checked_at = now() if callable(now) else now
    if not MINIMATCH_ACTIVE_FROM <= checked_at < MINIMATCH_EXPIRES_AT:
        raise AuditRejected("AUDIT_ACCEPTANCE_EXPIRED")
    return "risk_accepted_minimatch_until_2026-11-01T00:00:00Z"


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
        if "minimatch" in vulnerabilities:
            return evaluate_minimatch(vulnerabilities, counts, exit_code, root, now)
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
    advisory = MINIMATCH_ADVISORY if status.startswith("risk_accepted_minimatch_") else ADVISORY
    print(json.dumps({"status": status, "advisory": advisory if status != "clean" else None}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
