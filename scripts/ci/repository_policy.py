"""Deterministic repository, dependency, and CI policy gates for NEXUS."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import asdict, dataclass
from importlib.metadata import distributions
from pathlib import Path
from typing import Any

_SDK_PACKAGE = re.compile(r'(?im)^name\s*=\s*["\']fsopenapi["\']|openapi-python-sdk')
_SHA256_IMAGE = re.compile(r"^[^\s@]+:[^\s@]+@sha256:[0-9a-f]{64}$")
_FORBIDDEN_GATEWAY = re.compile(
    r"OrderCreate|OrderModify|OrderCancel|/webhooks?(?:/|[\"'])|"
    r"(?:place|create|modify|cancel|submit)_order",
    re.IGNORECASE,
)
_SECRET_PATTERNS = {
    "fosun_api_key": re.compile(r"\bak_[A-Za-z0-9]{20,}=*"),
    "private_key": re.compile(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----"),
    "aws_access_key": re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    "github_token": re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,}\b"),
    "npm_token": re.compile(r"\bnpm_[A-Za-z0-9]{20,}\b"),
    "openai_token": re.compile(r"\bsk-[A-Za-z0-9_-]{24,}\b"),
}
_REQUIRED_SDK_EVIDENCE = (
    "written_permission",
    "provider_test_vector",
    "security_review",
    "tls_verification_remediation",
)
_LOCAL_PYTHON_PACKAGES = {"nexus-engine", "nexus-fosun-gateway", "nexus-workspace"}


@dataclass(frozen=True, order=True)
class PolicyFinding:
    rule: str
    path: str
    message: str


@dataclass(frozen=True, order=True)
class DependencyRecord:
    ecosystem: str
    name: str
    version: str
    license: str


def scan_secret_material(files: dict[str, str]) -> list[PolicyFinding]:
    findings: list[PolicyFinding] = []
    for path, content in sorted(files.items()):
        for rule, pattern in _SECRET_PATTERNS.items():
            if pattern.search(content):
                findings.append(
                    PolicyFinding(rule, path, "tracked material matches a secret signature")
                )
    return sorted(findings)


def check_forbidden_gateway_surface(root: Path) -> list[PolicyFinding]:
    source = root / "integrations/fosun_gateway/src"
    if not source.is_dir():
        return [
            PolicyFinding(
                "gateway_source_missing",
                _relative(root, source),
                "gateway source directory is unavailable",
            )
        ]
    findings: list[PolicyFinding] = []
    for path in sorted(source.rglob("*.py")):
        if _FORBIDDEN_GATEWAY.search(_read_text(path)):
            findings.append(
                PolicyFinding(
                    "forbidden_gateway_surface",
                    _relative(root, path),
                    "gateway production source contains a write or webhook surface token",
                )
            )
    return findings


def check_sdk_lock_admission(root: Path) -> list[PolicyFinding]:
    lock_path = root / "uv.lock"
    manifest_path = root / "contracts/providers/fosun/manifest.json"
    if not lock_path.is_file() or not manifest_path.is_file():
        return [
            PolicyFinding(
                "sdk_admission_inputs_missing",
                "uv.lock|contracts/providers/fosun/manifest.json",
                "SDK admission requires both lock and provider manifest",
            )
        ]
    lock_text = _read_text(lock_path)
    try:
        manifest = json.loads(_read_text(manifest_path))
    except json.JSONDecodeError:
        return [
            PolicyFinding(
                "sdk_manifest_invalid",
                _relative(root, manifest_path),
                "provider manifest is not valid JSON",
            )
        ]
    if not isinstance(manifest, dict):
        return [
            PolicyFinding(
                "sdk_manifest_invalid",
                _relative(root, manifest_path),
                "provider manifest must be an object",
            )
        ]
    admission = manifest.get("admission")
    sdk = manifest.get("sdk")
    status = admission.get("status") if isinstance(admission, dict) else None
    sdk_locked = _SDK_PACKAGE.search(lock_text) is not None
    if not sdk_locked:
        if status == "admitted":
            return [
                PolicyFinding(
                    "admitted_sdk_missing_from_lock",
                    _relative(root, lock_path),
                    "manifest admission and dependency lock disagree",
                )
            ]
        return []
    if not _complete_sdk_admission(admission, sdk):
        return [
            PolicyFinding(
                "sdk_not_admitted",
                _relative(root, lock_path),
                "provider SDK is locked without complete admission evidence",
            )
        ]
    commit = sdk.get("commit")
    if not isinstance(commit, str) or commit not in lock_text:
        return [
            PolicyFinding(
                "sdk_lock_not_exact",
                _relative(root, lock_path),
                "provider SDK lock does not contain the admitted commit",
            )
        ]
    return []


def _complete_sdk_admission(admission: object, sdk: object) -> bool:
    if not isinstance(admission, dict) or not isinstance(sdk, dict):
        return False
    if admission.get("status") != "admitted":
        return False
    commit = sdk.get("commit")
    if not isinstance(commit, str) or re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        return False
    if admission.get("sdk_commit") != commit:
        return False
    license_name = sdk.get("license")
    if not isinstance(license_name, str) or license_name.upper() in {"", "UNRESOLVED"}:
        return False
    evidence = admission.get("evidence")
    return isinstance(evidence, dict) and all(
        isinstance(evidence.get(name), str) and bool(evidence[name].strip())
        for name in _REQUIRED_SDK_EVIDENCE
    )


def license_findings(records: list[DependencyRecord]) -> list[PolicyFinding]:
    findings: list[PolicyFinding] = []
    for record in sorted(records):
        identifier = f"{record.ecosystem}:{record.name}@{record.version}"
        normalized = record.license.strip().upper()
        if not normalized:
            findings.append(
                PolicyFinding(
                    "missing_license_metadata",
                    identifier,
                    "dependency has no declared license metadata",
                )
            )
        elif (
            "AGPL" in normalized
            or "SSPL" in normalized
            or re.search(r"(?:^|[^A-Z])GPL(?:-|$)", normalized) is not None
        ):
            findings.append(
                PolicyFinding(
                    "strong_copyleft_license",
                    identifier,
                    "dependency license requires an explicit written admission",
                )
            )
    return findings


def dependency_inventory(root: Path) -> list[DependencyRecord]:
    records = _python_inventory()
    records.extend(_npm_inventory(root / "package-lock.json"))
    return sorted(set(records))


def _python_inventory() -> list[DependencyRecord]:
    records: list[DependencyRecord] = []
    for distribution in distributions():
        name = distribution.metadata.get("Name", "").strip()
        if not name or name.lower() in _LOCAL_PYTHON_PACKAGES:
            continue
        classifiers = distribution.metadata.get_all("Classifier") or []
        classified_license = "; ".join(
            value.removeprefix("License :: ")
            for value in classifiers
            if value.startswith("License :: ")
        )
        license_name = (
            distribution.metadata.get("License-Expression")
            or classified_license
            or distribution.metadata.get("License")
            or ""
        ).strip()
        records.append(
            DependencyRecord("python", name, distribution.version, license_name.strip())
        )
    return records


def _npm_inventory(lock_path: Path) -> list[DependencyRecord]:
    try:
        document = json.loads(_read_text(lock_path))
    except (OSError, json.JSONDecodeError):
        return []
    packages = document.get("packages") if isinstance(document, dict) else None
    if not isinstance(packages, dict):
        return []
    records: list[DependencyRecord] = []
    for package_path, metadata in packages.items():
        if not isinstance(package_path, str) or "node_modules/" not in package_path:
            continue
        if not isinstance(metadata, dict):
            continue
        name = package_path.rsplit("node_modules/", 1)[-1]
        version = metadata.get("version")
        license_name = metadata.get("license")
        records.append(
            DependencyRecord(
                "npm",
                name,
                version if isinstance(version, str) else "",
                license_name if isinstance(license_name, str) else "",
            )
        )
    return records


def check_container_pins(root: Path) -> list[PolicyFinding]:
    candidates = [root / "infra/compose.yaml"]
    candidates.extend(sorted((root / "infra/docker").glob("*.Dockerfile")))
    candidates.extend(sorted((root / ".github/workflows").glob("*.yml")))
    findings: list[PolicyFinding] = []
    for path in candidates:
        if not path.is_file():
            continue
        for line_number, reference in _container_references(path):
            if not _SHA256_IMAGE.fullmatch(reference):
                findings.append(
                    PolicyFinding(
                        "unpinned_container_image",
                        f"{_relative(root, path)}:{line_number}",
                        "container image must include a tag and immutable sha256 digest",
                    )
                )
    drill_path = root / "engine/src/nexus/ops/backup_restore.py"
    if drill_path.is_file():
        drill_content = _read_text(drill_path)
        assignment_pattern = re.compile(
            r'^_\w+_IMAGE\s*=\s*(?:["\']([^"\']+)["\']|\((.*?)\))',
            re.MULTILINE | re.DOTALL,
        )
        for match in assignment_pattern.finditer(drill_content):
            reference = match.group(1) or "".join(
                re.findall(r'["\']([^"\']+)["\']', match.group(2) or "")
            )
            if not _SHA256_IMAGE.fullmatch(reference):
                line_number = drill_content.count("\n", 0, match.start()) + 1
                findings.append(
                    PolicyFinding(
                        "unpinned_container_image",
                        f"{_relative(root, drill_path)}:{line_number}",
                        "drill image must include a tag and immutable sha256 digest",
                    )
                )
    return sorted(findings)


def check_container_hardening(root: Path) -> list[PolicyFinding]:
    findings: list[PolicyFinding] = []
    for path in sorted((root / "infra/docker").glob("*.Dockerfile")):
        users = re.findall(r"(?im)^USER\s+([^\s#]+)", _read_text(path))
        if not users or users[-1].lower() in {"0", "root"}:
            findings.append(
                PolicyFinding(
                    "container_runs_as_root",
                    _relative(root, path),
                    "final runtime stage must declare a non-root user",
                )
            )
    ignore_path = root / ".dockerignore"
    required_ignores = {".git", ".secrets", ".env", "node_modules"}
    if not ignore_path.is_file():
        missing_ignores = required_ignores
    else:
        configured = {
            line.strip()
            for line in _read_text(ignore_path).splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        }
        missing_ignores = required_ignores - configured
    if missing_ignores:
        findings.append(
            PolicyFinding(
                "docker_context_not_restricted",
                _relative(root, ignore_path),
                "Docker context must exclude VCS, secret, env, and dependency domains",
            )
        )
    return sorted(findings)


def _container_references(path: Path) -> list[tuple[int, str]]:
    references: list[tuple[int, str]] = []
    for line_number, line in enumerate(_read_text(path).splitlines(), 1):
        stripped = line.strip()
        if stripped.upper().startswith("FROM "):
            reference = stripped.split()[1]
            references.append((line_number, reference))
        elif re.fullmatch(r"image:\s*[^\s#]+", stripped):
            references.append((line_number, stripped.removeprefix("image:").strip()))
    return references


def check_workflow(root: Path) -> list[PolicyFinding]:
    path = root / ".github/workflows/ci.yml"
    if not path.is_file():
        return [
            PolicyFinding(
                "ci_workflow_missing",
                _relative(root, path),
                "required CI workflow is unavailable",
            )
        ]
    content = _read_text(path)
    findings: list[PolicyFinding] = []
    required_jobs = {"web", "gateway-offline", "engine-postgres", "e2e", "policy"}
    jobs = set(re.findall(r"(?m)^  ([a-z0-9-]+):\n", content))
    if not required_jobs.issubset(jobs):
        findings.append(
            PolicyFinding(
                "ci_jobs_missing",
                _relative(root, path),
                "workflow must isolate web, gateway, engine, e2e, and policy jobs",
            )
        )
    if re.search(r"(?m)^permissions:\n  contents: read\s*$", content) is None:
        findings.append(
            PolicyFinding(
                "ci_permissions_not_minimal",
                _relative(root, path),
                "workflow token permissions must be contents read only",
            )
        )
    if "${{ secrets." in content:
        findings.append(
            PolicyFinding(
                "ci_secret_reference_forbidden",
                _relative(root, path),
                "offline CI must not load repository or environment secrets",
            )
        )
    for reference in re.findall(r"(?m)^\s*-?\s*uses:\s*([^\s#]+)", content):
        if reference.startswith("./"):
            continue
        action_ref = reference.rsplit("@", 1)[-1]
        if re.fullmatch(r"[0-9a-f]{40}", action_ref) is None:
            findings.append(
                PolicyFinding(
                    "ci_action_not_immutable",
                    _relative(root, path),
                    "third-party actions must be pinned to a full commit SHA",
                )
            )
            break
    required_markers = (
        "NEXUS_OFFLINE_SOCKET_GUARD: \"1\"",
        "scripts/offline_guard/selftest.py",
        "scripts/ci/repository_policy.py",
        "npm audit --audit-level=high",
        "pip-audit",
    )
    if any(marker not in content for marker in required_markers):
        findings.append(
            PolicyFinding(
                "ci_gate_missing",
                _relative(root, path),
                "workflow omits an offline, policy, or dependency gate",
            )
        )
    if (
        "if: failure()" not in content
        or "path: test-results/**" not in content
        or "retention-days: 3" not in content
        or "include-hidden-files: false" not in content
    ):
        findings.append(
            PolicyFinding(
                "ci_artifact_policy_invalid",
                _relative(root, path),
                "failure artifacts must be narrow, short-lived, and exclude hidden files",
            )
        )
    return sorted(findings)


def repository_texts(root: Path) -> dict[str, str]:
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=root,
        check=True,
        capture_output=True,
    )
    files: dict[str, str] = {}
    for raw_path in result.stdout.split(b"\0"):
        if not raw_path:
            continue
        relative = raw_path.decode("utf-8")
        path = root / relative
        try:
            if not path.is_file() or path.stat().st_size > 2 * 1024 * 1024:
                continue
            content = path.read_text(encoding="utf-8")
        except (OSError, UnicodeError):
            continue
        files[relative] = content
    return files


def run_all(root: Path) -> tuple[list[PolicyFinding], list[DependencyRecord]]:
    inventory = dependency_inventory(root)
    findings = scan_secret_material(repository_texts(root))
    findings.extend(check_forbidden_gateway_surface(root))
    findings.extend(check_sdk_lock_admission(root))
    findings.extend(check_container_pins(root))
    findings.extend(check_container_hardening(root))
    findings.extend(check_workflow(root))
    findings.extend(license_findings(inventory))
    return sorted(set(findings)), inventory


def _write_inventory(path: Path, records: list[DependencyRecord]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {
        "schema_version": "nexus_dependency_inventory_v1",
        "dependencies": [asdict(record) for record in records],
        "summary": {
            "total": len(records),
            "npm": sum(record.ecosystem == "npm" for record in records),
            "python": sum(record.ecosystem == "python" for record in records),
        },
    }
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n")


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _relative(root: Path, path: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--inventory-output", type=Path, required=True)
    arguments = parser.parse_args(argv)
    root = arguments.root.resolve()
    findings, inventory = run_all(root)
    _write_inventory(arguments.inventory_output, inventory)
    payload = {
        "status": "fail" if findings else "pass",
        "finding_count": len(findings),
        "dependency_count": len(inventory),
        "findings": [asdict(finding) for finding in findings],
    }
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
