"""Repository policy gate contracts."""

from __future__ import annotations

import json
from pathlib import Path

from scripts.ci.repository_policy import (
    DependencyRecord,
    check_container_hardening,
    check_container_pins,
    check_forbidden_gateway_surface,
    check_sdk_lock_admission,
    check_workflow,
    license_findings,
    scan_secret_material,
)


def test_secret_scan_reports_only_pattern_and_path_not_material() -> None:
    findings = scan_secret_material(
        {
            "infra/env.example": "FSOPENAPI_API_KEY=\n",
            "leak.txt": "ak_" + "a" * 30 + "=\n",
        }
    )

    assert [(finding.rule, finding.path) for finding in findings] == [
        ("fosun_api_key", "leak.txt")
    ]
    assert all("a" * 20 not in finding.message for finding in findings)


def test_gateway_source_forbids_write_and_webhook_surface(tmp_path: Path) -> None:
    source = tmp_path / "integrations/fosun_gateway/src"
    source.mkdir(parents=True)
    (source / "safe.py").write_text('ROUTE = "/internal/v1/read-only/fosun_get_quotes"')

    assert check_forbidden_gateway_surface(tmp_path) == []

    (source / "unsafe.py").write_text('ROUTE = "/internal/v1/OrderCreate"')
    findings = check_forbidden_gateway_surface(tmp_path)
    assert [(finding.rule, finding.path) for finding in findings] == [
        ("forbidden_gateway_surface", "integrations/fosun_gateway/src/unsafe.py")
    ]


def test_sdk_lock_requires_complete_machine_readable_admission(tmp_path: Path) -> None:
    manifest_path = tmp_path / "contracts/providers/fosun/manifest.json"
    manifest_path.parent.mkdir(parents=True)
    manifest = {
        "sdk": {"commit": "8" * 40, "license": "UNRESOLVED"},
        "admission": {"status": "blocked", "sdk_commit": "8" * 40, "evidence": {}},
    }
    manifest_path.write_text(json.dumps(manifest))
    (tmp_path / "uv.lock").write_text('version = 1\n')

    assert check_sdk_lock_admission(tmp_path) == []

    (tmp_path / "uv.lock").write_text('name = "fsopenapi"\n')
    assert {finding.rule for finding in check_sdk_lock_admission(tmp_path)} == {
        "sdk_not_admitted"
    }

    (tmp_path / "uv.lock").write_text('version = 1\n')
    manifest["admission"]["status"] = "admitted"
    manifest_path.write_text(json.dumps(manifest))
    assert {finding.rule for finding in check_sdk_lock_admission(tmp_path)} == {
        "admitted_sdk_missing_from_lock"
    }


def test_license_policy_blocks_strong_copyleft_but_inventories_lgpl() -> None:
    records = [
        DependencyRecord("npm", "safe", "1.0.0", "MIT"),
        DependencyRecord("npm", "dynamic-lib", "1.0.0", "LGPL-3.0-or-later"),
        DependencyRecord("python", "unsafe", "1.0.0", "AGPL-3.0-only"),
    ]

    findings = license_findings(records)

    assert [(finding.rule, finding.path) for finding in findings] == [
        ("strong_copyleft_license", "python:unsafe@1.0.0")
    ]


def test_container_references_require_tag_and_sha256_digest(tmp_path: Path) -> None:
    (tmp_path / "infra/docker").mkdir(parents=True)
    (tmp_path / "engine/src/nexus/ops").mkdir(parents=True)
    (tmp_path / "infra/compose.yaml").write_text(
        "services:\n  db:\n    image: postgres:17@sha256:" + "a" * 64 + "\n"
    )
    (tmp_path / "infra/docker/service.Dockerfile").write_text(
        "FROM python:3.12-slim@sha256:" + "b" * 64 + "\n"
    )
    drill = tmp_path / "engine/src/nexus/ops/backup_restore.py"
    drill.write_text(
        '_POSTGRES_IMAGE = (\n    "postgres:17@"\n    "sha256:' + "c" * 64 + '"\n)\n'
    )

    assert check_container_pins(tmp_path) == []

    drill.write_text('_POSTGRES_IMAGE = (\n    "postgres:17"\n)\n')
    assert {finding.rule for finding in check_container_pins(tmp_path)} == {
        "unpinned_container_image"
    }

    drill.write_text(
        '_POSTGRES_IMAGE = (\n    "postgres:17@"\n    "sha256:' + "c" * 64 + '"\n)\n'
    )
    (tmp_path / "infra/docker/service.Dockerfile").write_text("FROM python:3.12-slim\n")
    assert {finding.rule for finding in check_container_pins(tmp_path)} == {
        "unpinned_container_image"
    }


def test_container_runtime_is_non_root_and_context_excludes_secret_domains(
    tmp_path: Path,
) -> None:
    docker = tmp_path / "infra/docker"
    docker.mkdir(parents=True)
    (docker / "service.Dockerfile").write_text("FROM synthetic@sha256:" + "a" * 64)
    (tmp_path / ".dockerignore").write_text(".git\n.secrets\n.env\nnode_modules\n")

    assert {finding.rule for finding in check_container_hardening(tmp_path)} == {
        "container_runs_as_root"
    }

    (docker / "service.Dockerfile").write_text(
        "FROM synthetic@sha256:" + "a" * 64 + "\nUSER nexus\n"
    )
    assert check_container_hardening(tmp_path) == []


def test_committed_workflow_has_minimal_permissions_and_offline_jobs() -> None:
    root = Path(__file__).resolve().parents[2]

    assert check_workflow(root) == []
