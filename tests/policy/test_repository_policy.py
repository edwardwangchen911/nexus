"""Repository policy gate contracts."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from scripts.ci.repository_policy import (
    DependencyRecord,
    check_container_hardening,
    check_container_pins,
    check_forbidden_gateway_surface,
    check_gateway_entrypoint_isolation,
    check_gateway_read_allowlist,
    check_probe_runtime_isolation,
    check_provider_tls_remediation,
    check_sdk_lock_admission,
    check_workflow,
    license_findings,
    repository_texts,
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


def test_secret_scan_cannot_be_bypassed_by_binary_or_large_tracked_file(
    tmp_path: Path,
) -> None:
    subprocess.run(["git", "init", "--quiet"], cwd=tmp_path, check=True)
    leak = tmp_path / "large-binary.dat"
    leak.write_bytes(b"\xff\xfe" + b"ak_" + b"z" * 30 + b"=" + b"x" * (2 * 1024 * 1024))
    subprocess.run(["git", "add", leak.name], cwd=tmp_path, check=True)

    findings = scan_secret_material(repository_texts(tmp_path))

    assert [(finding.rule, finding.path) for finding in findings] == [
        ("fosun_api_key", leak.name)
    ]


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


@pytest.mark.parametrize(
    "forbidden_token",
    [
        "SimAccountCreate",
        "SimAccountReset",
        "createOrder",
        "cancelOrder",
        "createSubscription",
        "subscription_cancel",
        "order_modify",
        "REPLACE_ORDER",
        "replaceOrderPayload",
        "OrderCreateRequest",
        "SimAccountCreateRequest",
        "wealth.redeem_fund",
        "fund_purchase",
        "withdraw_cash",
        "transfer_assets",
    ],
)
def test_gateway_policy_covers_provider_write_token_variants(
    forbidden_token: str,
    tmp_path: Path,
) -> None:
    source = tmp_path / "integrations/fosun_gateway/src"
    source.mkdir(parents=True)
    (source / "unsafe.py").write_text(f'OPERATION = "{forbidden_token}"\n')

    findings = check_forbidden_gateway_surface(tmp_path)

    assert [(finding.rule, finding.path) for finding in findings] == [
        ("forbidden_gateway_surface", "integrations/fosun_gateway/src/unsafe.py")
    ]


def test_gateway_policy_exempts_only_the_reviewed_offline_simulation_files(
    tmp_path: Path,
) -> None:
    package = tmp_path / "integrations/fosun_gateway/src/nexus_fosun_gateway"
    reviewed = (
        package / "adapter/simulation_fixture.py",
        package / "policy/simulation_operations.py",
        package / "routes/simulation.py",
    )
    for path in reviewed:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("def create_order(): pass\n")

    assert check_forbidden_gateway_surface(tmp_path) == []

    near_miss = package / "routes/simulation_external.py"
    near_miss.write_text("def create_order(): pass\n")
    findings = check_forbidden_gateway_surface(tmp_path)
    assert [(finding.rule, finding.path) for finding in findings] == [
        (
            "forbidden_gateway_surface",
            "integrations/fosun_gateway/src/nexus_fosun_gateway/routes/simulation_external.py",
        )
    ]


def test_gateway_requires_both_frozen_sdk_tls_remediation_surfaces(tmp_path: Path) -> None:
    source = tmp_path / "integrations/fosun_gateway/src/nexus_fosun_gateway"
    security = source / "security"
    security.mkdir(parents=True)
    (source / "main.py").write_text("enforce_sdk_tls_verification(client, auth_module)")
    remediation = security / "provider_tls.py"
    remediation.write_text(
        'kwargs["verify"] = True\nkwargs["allow_redirects"] = False\n'
        'kwargs["verify"] = True\nkwargs["allow_redirects"] = False\n'
    )

    assert check_provider_tls_remediation(tmp_path) == []

    remediation.write_text('kwargs["verify"] = True\n')
    assert {finding.rule for finding in check_provider_tls_remediation(tmp_path)} == {
        "provider_tls_remediation_missing"
    }


def test_long_running_gateway_cannot_import_probe_only_construction(tmp_path: Path) -> None:
    main_path = tmp_path / "integrations/fosun_gateway/src/nexus_fosun_gateway/main.py"
    main_path.parent.mkdir(parents=True)
    main_path.write_text(
        "from nexus_fosun_gateway.adapter.sdk import SdkFosunTransport\n"
    )

    assert check_probe_runtime_isolation(tmp_path) == []

    main_path.write_text(
        "from nexus_fosun_gateway.adapter.probe_sdk import build_probe_transport\n"
    )
    findings = check_probe_runtime_isolation(tmp_path)
    assert [(finding.rule, finding.path) for finding in findings] == [
        (
            "probe_runtime_isolation_missing",
            "integrations/fosun_gateway/src/nexus_fosun_gateway/main.py",
        )
    ]

    main_path.write_text("from .adapter import probe_sdk\n")
    findings = check_probe_runtime_isolation(tmp_path)
    assert [(finding.rule, finding.path) for finding in findings] == [
        (
            "probe_runtime_isolation_missing",
            "integrations/fosun_gateway/src/nexus_fosun_gateway/main.py",
        )
    ]

    main_path.write_text("from . import probe_capture\n")
    findings = check_probe_runtime_isolation(tmp_path)
    assert [(finding.rule, finding.path) for finding in findings] == [
        (
            "probe_runtime_isolation_missing",
            "integrations/fosun_gateway/src/nexus_fosun_gateway/main.py",
        )
    ]

    main_path.write_text("from .publication import load_probe_signing_key\n")
    findings = check_probe_runtime_isolation(tmp_path)
    assert [(finding.rule, finding.path) for finding in findings] == [
        (
            "probe_runtime_isolation_missing",
            "integrations/fosun_gateway/src/nexus_fosun_gateway/main.py",
        )
    ]

    bridge_path = main_path.with_name("bridge.py")
    bridge_path.write_text("from . import probe_settings\n")
    main_path.write_text("from . import bridge\n")
    findings = check_probe_runtime_isolation(tmp_path)
    assert [(finding.rule, finding.path) for finding in findings] == [
        (
            "probe_runtime_isolation_missing",
            "integrations/fosun_gateway/src/nexus_fosun_gateway/main.py",
        )
    ]

    bridge_path.write_text("from . import probe_receipt\n")
    findings = check_probe_runtime_isolation(tmp_path)
    assert {finding.rule for finding in findings} == {
        "probe_runtime_isolation_missing"
    }

    bridge_path.write_text(
        'import importlib\nimportlib.import_module("nexus_fosun_gateway.probe_receipt")\n'
    )
    findings = check_probe_runtime_isolation(tmp_path)
    assert {finding.rule for finding in findings} == {
        "probe_runtime_isolation_missing"
    }


def test_gateway_read_surface_is_an_exact_operation_route_and_sdk_allowlist(
    tmp_path: Path,
) -> None:
    package = tmp_path / "integrations/fosun_gateway/src/nexus_fosun_gateway"
    adapter = package / "adapter"
    policy = package / "policy"
    routes = package / "routes"
    adapter.mkdir(parents=True)
    policy.mkdir()
    routes.mkdir()
    operations = (
        "fosun_capabilities",
        "fosun_sync_accounts",
        "fosun_sync_portfolio",
        "fosun_list_orders",
        "fosun_list_cash_flows",
        "fosun_get_quotes",
        "fosun_get_bars",
    )
    (policy / "operations.py").write_text(
        "READ_ONLY_OPERATIONS = frozenset(" + repr(set(operations)) + ")\n"
    )
    route_lines = ['router = APIRouter(prefix="/internal/v1/read-only")']
    for operation in operations:
        route_lines.append(f"def {operation}(): pass")
        route_lines.append(
            f'router.add_api_route("/{operation}", {operation}, methods=["POST"])'
        )
    route_source = "\n".join(route_lines) + "\n"
    (routes / "read_only.py").write_text(route_source)
    sdk_pairs = (
        ("account", "list_accounts"),
        ("portfolio", "get_assets_summary"),
        ("portfolio", "get_holdings"),
        ("trade", "list_orders"),
        ("trade", "get_cash_flows"),
        ("market", "quote"),
        ("market", "kline"),
    )
    (adapter / "sdk.py").write_text(
        "class Adapter:\n"
        "    def bind(self):\n"
        + "".join(
            f'        self._method("{section}", "{method}")\n'
            for section, method in sdk_pairs
        )
    )

    assert check_gateway_read_allowlist(tmp_path) == []

    (routes / "read_only.py").write_text(
        route_source.replace(
            'router = APIRouter(prefix="/internal/v1/read-only")',
            'router = object()\ndecoy = APIRouter(prefix="/internal/v1/read-only")',
        )
    )
    assert {
        finding.rule for finding in check_gateway_read_allowlist(tmp_path)
    } == {"gateway_read_allowlist_mismatch"}

    (routes / "read_only.py").write_text(route_source)
    with (adapter / "sdk.py").open("a") as stream:
        stream.write('        self._method("wealth", "redeem_fund")\n')
    assert {
        finding.rule for finding in check_gateway_read_allowlist(tmp_path)
    } == {"gateway_read_allowlist_mismatch"}


def test_gateway_entrypoints_are_frozen_away_from_probe_runtime(tmp_path: Path) -> None:
    pyproject = tmp_path / "integrations/fosun_gateway/pyproject.toml"
    compose = tmp_path / "infra/compose.yaml"
    dockerfile = tmp_path / "infra/docker/fosun-gateway.Dockerfile"
    pyproject.parent.mkdir(parents=True)
    dockerfile.parent.mkdir(parents=True)
    pyproject.write_text(
        '[project]\nname="synthetic"\n[project.scripts]\n'
        'nexus-fosun-gateway="nexus_fosun_gateway.main:run"\n'
    )
    compose.write_text(
        'services:\n  fosun-gateway:\n    command: ["nexus-fosun-gateway"]\n'
    )
    dockerfile.write_text('FROM synthetic\nCMD ["nexus-fosun-gateway"]\n')

    assert check_gateway_entrypoint_isolation(tmp_path) == []

    compose.write_text(
        'services:\n  fosun-gateway:\n'
        '    command: ["nexus-fosun-read-only-probe"]\n'
    )
    assert {
        finding.rule for finding in check_gateway_entrypoint_isolation(tmp_path)
    } == {"gateway_entrypoint_isolation_missing"}

    compose.write_text(
        'services:\n  fosun-gateway:\n'
        '    command: ["nexus-fosun-read-only-probe"]\n'
        '  decoy:\n    # command: ["nexus-fosun-gateway"]\n'
    )
    assert {
        finding.rule for finding in check_gateway_entrypoint_isolation(tmp_path)
    } == {"gateway_entrypoint_isolation_missing"}

    compose.write_text(
        'services:\n  fosun-gateway:\n    command: ["nexus-fosun-gateway"]\n'
    )
    dockerfile.write_text(
        'FROM synthetic\nCMD ["nexus-fosun-gateway"]\n'
        'CMD ["nexus-fosun-read-only-probe"]\n'
    )
    assert {
        finding.rule for finding in check_gateway_entrypoint_isolation(tmp_path)
    } == {"gateway_entrypoint_isolation_missing"}

    dockerfile.write_text(
        'FROM synthetic\ncmd ["nexus-fosun-read-only-probe"]\n'
    )
    assert {
        finding.rule for finding in check_gateway_entrypoint_isolation(tmp_path)
    } == {"gateway_entrypoint_isolation_missing"}

    dockerfile.write_text(
        'FROM synthetic\nENTRYPOINT ["nexus-fosun-read-only-probe"]\n'
        'CMD ["nexus-fosun-gateway"]\n'
    )
    assert {
        finding.rule for finding in check_gateway_entrypoint_isolation(tmp_path)
    } == {"gateway_entrypoint_isolation_missing"}

    dockerfile.write_text('FROM synthetic\nCMD ["nexus-fosun-gateway"]\n')
    compose.write_text(
        'services:\n  fosun-gateway:\n'
        '    entrypoint: ["nexus-fosun-read-only-probe"]\n'
        '    command: ["nexus-fosun-gateway"]\n'
    )
    assert {
        finding.rule for finding in check_gateway_entrypoint_isolation(tmp_path)
    } == {"gateway_entrypoint_isolation_missing"}

    compose.write_text(
        'services:\n  fosun-gateway:\n    command: ["nexus-fosun-gateway"]\n'
        '    environment:\n      - SSLKEYLOGFILE=/tmp/keylog\n'
    )
    assert {
        finding.rule for finding in check_gateway_entrypoint_isolation(tmp_path)
    } == {"gateway_entrypoint_isolation_missing"}

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
