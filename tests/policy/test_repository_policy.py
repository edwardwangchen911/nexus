"""Repository policy gate contracts."""

from __future__ import annotations

import hashlib
import json
import subprocess
import tomllib
from pathlib import Path

import pytest

from scripts.ci import repository_policy
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


def test_long_running_gateway_cannot_import_probe_only_construction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    main_path = tmp_path / "integrations/fosun_gateway/src/nexus_fosun_gateway/main.py"
    main_path.parent.mkdir(parents=True)
    main_path.write_text(
        "from nexus_fosun_gateway.adapter.sdk import SdkFosunTransport\n"
    )

    def approve_current_source_for_semantic_check() -> None:
        monkeypatch.setattr(
            repository_policy,
            "_EXPECTED_GATEWAY_INSTALLABLE_SRC_SHA256",
            repository_policy._installable_source_tree_sha256(
                main_path.parent.parent
            ),
        )

    approve_current_source_for_semantic_check()

    assert check_probe_runtime_isolation(tmp_path) == []

    package_init = main_path.with_name("__init__.py")
    package_init.write_text("from . import probe_cli\n")
    approve_current_source_for_semantic_check()
    assert {finding.rule for finding in check_probe_runtime_isolation(tmp_path)} == {
        "probe_runtime_isolation_missing"
    }
    package_init.unlink()

    adapter_dir = main_path.parent / "adapter"
    adapter_dir.mkdir()
    adapter_init = adapter_dir / "__init__.py"
    (adapter_dir / "sdk.py").write_text("class SdkFosunTransport: pass\n")
    adapter_init.write_text("from .. import probe_capture\n")
    approve_current_source_for_semantic_check()
    assert {finding.rule for finding in check_probe_runtime_isolation(tmp_path)} == {
        "probe_runtime_isolation_missing"
    }
    adapter_init.unlink()

    readiness_path = main_path.with_name("readiness.py")
    readiness_path.write_text("from . import probe_receipt\n")
    approve_current_source_for_semantic_check()
    assert {finding.rule for finding in check_probe_runtime_isolation(tmp_path)} == {
        "probe_runtime_isolation_missing"
    }
    readiness_path.unlink()

    main_path.write_text(
        "from nexus_fosun_gateway.adapter.probe_sdk import build_probe_transport\n"
    )
    approve_current_source_for_semantic_check()
    findings = check_probe_runtime_isolation(tmp_path)
    assert [(finding.rule, finding.path) for finding in findings] == [
        (
            "probe_runtime_isolation_missing",
            "integrations/fosun_gateway/src/nexus_fosun_gateway/main.py",
        )
    ]

    main_path.write_text("from .adapter import probe_sdk\n")
    approve_current_source_for_semantic_check()
    findings = check_probe_runtime_isolation(tmp_path)
    assert [(finding.rule, finding.path) for finding in findings] == [
        (
            "probe_runtime_isolation_missing",
            "integrations/fosun_gateway/src/nexus_fosun_gateway/main.py",
        )
    ]

    main_path.write_text("from . import probe_capture\n")
    approve_current_source_for_semantic_check()
    findings = check_probe_runtime_isolation(tmp_path)
    assert [(finding.rule, finding.path) for finding in findings] == [
        (
            "probe_runtime_isolation_missing",
            "integrations/fosun_gateway/src/nexus_fosun_gateway/main.py",
        )
    ]

    main_path.write_text("from .publication import load_probe_signing_key\n")
    approve_current_source_for_semantic_check()
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
    approve_current_source_for_semantic_check()
    findings = check_probe_runtime_isolation(tmp_path)
    assert [(finding.rule, finding.path) for finding in findings] == [
        (
            "probe_runtime_isolation_missing",
            "integrations/fosun_gateway/src/nexus_fosun_gateway/main.py",
        )
    ]

    bridge_path.write_text("from . import probe_receipt\n")
    approve_current_source_for_semantic_check()
    findings = check_probe_runtime_isolation(tmp_path)
    assert {finding.rule for finding in findings} == {
        "probe_runtime_isolation_missing"
    }

    bridge_path.write_text(
        'import importlib\nimportlib.import_module("nexus_fosun_gateway.probe_receipt")\n'
    )
    approve_current_source_for_semantic_check()
    findings = check_probe_runtime_isolation(tmp_path)
    assert {finding.rule for finding in findings} == {
        "probe_runtime_isolation_missing"
    }

    bridge_path.write_text(
        '__import__("nexus_fosun_gateway.probe_receipt")\n'
    )
    approve_current_source_for_semantic_check()
    assert {finding.rule for finding in check_probe_runtime_isolation(tmp_path)} == {
        "probe_runtime_isolation_missing"
    }

    bridge_path.write_text("VALUE = 1\n")
    approve_current_source_for_semantic_check()
    bridge_path.write_text(
        "import importlib\n"
        "loader = getattr(importlib, ''.join(('import', '_module')))\n"
        "loader('.'.join(('nexus_fosun_gateway', 'probe_cli')))\n"
    )
    assert {finding.rule for finding in check_probe_runtime_isolation(tmp_path)} == {
        "probe_runtime_isolation_missing"
    }

    bridge_path.write_text(
        'import importlib\nname = "nexus_fosun_gateway.probe_" + "cli"\n'
        "importlib.import_module(name)\n"
    )
    approve_current_source_for_semantic_check()
    assert {finding.rule for finding in check_probe_runtime_isolation(tmp_path)} == {
        "probe_runtime_isolation_missing"
    }

    bridge_path.write_text(
        'import importlib\nloader = importlib.import_module\nname = "safe"\nloader(name)\n'
    )
    approve_current_source_for_semantic_check()
    assert {finding.rule for finding in check_probe_runtime_isolation(tmp_path)} == {
        "probe_runtime_isolation_missing"
    }

    bridge_path.write_text(
        "import importlib\n"
        "loader: object = importlib.import_module\n"
        'name = "nexus_fosun_gateway.probe_" + "cli"\n'
        "loader(name)\n"
    )
    approve_current_source_for_semantic_check()
    assert {finding.rule for finding in check_probe_runtime_isolation(tmp_path)} == {
        "probe_runtime_isolation_missing"
    }

    bridge_path.write_text(
        "import importlib\n"
        'loader = getattr(importlib, "import_" + "module")\n'
        'name = "nexus_fosun_gateway.probe_" + "cli"\n'
        "loader(name)\n"
    )
    approve_current_source_for_semantic_check()
    assert {finding.rule for finding in check_probe_runtime_isolation(tmp_path)} == {
        "probe_runtime_isolation_missing"
    }


def test_gateway_read_surface_is_an_exact_operation_route_and_sdk_allowlist(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package = tmp_path / "integrations/fosun_gateway/src/nexus_fosun_gateway"
    adapter = package / "adapter"
    policy = package / "policy"
    routes = package / "routes"
    adapter.mkdir(parents=True)
    policy.mkdir()
    routes.mkdir()
    (package / "main.py").write_text("app.include_router(read_only_router)\n")
    operations = (
        "fosun_capabilities",
        "fosun_sync_accounts",
        "fosun_sync_portfolio",
        "fosun_list_orders",
        "fosun_list_cash_flows",
        "fosun_get_quotes",
        "fosun_get_bars",
    )
    broker_operations = {
        "fosun_sync_portfolio",
        "fosun_list_orders",
        "fosun_list_cash_flows",
    }
    market_operations = {"fosun_get_quotes", "fosun_get_bars"}
    read_line = "READ_ONLY_OPERATIONS = frozenset(" + repr(set(operations)) + ")"
    operation_source = (
        "GatewayOperation = Literal["
        + ",".join(repr(operation) for operation in operations)
        + "]\n"
        + read_line
        + "\nBROKER_OPERATIONS = frozenset("
        + repr(broker_operations)
        + ")\nMARKET_OPERATIONS = frozenset("
        + repr(market_operations)
        + ")\n"
    )
    (policy / "operations.py").write_text(operation_source)
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
    adapter_source = (
        "class Adapter:\n"
        "    def __init__(self, client):\n"
        "        self._client = client\n"
        "    def bind(self):\n"
        + "".join(
            f'        self._method("{section}", "{method}")\n'
            for section, method in sdk_pairs
        )
        + "    def _method(self, section_name, method_name):\n"
        "        section = getattr(self._client, section_name, None)\n"
        "        method = getattr(section, method_name, None)\n"
        "        return method\n"
    )
    (adapter / "sdk.py").write_text(adapter_source)
    reviewed_sources = {
        "policy/operations.py": policy / "operations.py",
        "routes/read_only.py": routes / "read_only.py",
        "adapter/sdk.py": adapter / "sdk.py",
        "main.py": package / "main.py",
    }

    def approve_current_source_for_semantic_check() -> None:
        monkeypatch.setattr(
            repository_policy,
            "_EXPECTED_GATEWAY_SOURCE_SHA256",
            {
                relative: hashlib.sha256(path.read_bytes()).hexdigest()
                for relative, path in reviewed_sources.items()
            },
        )
        monkeypatch.setattr(
            repository_policy,
            "_EXPECTED_GATEWAY_INSTALLABLE_SRC_SHA256",
            repository_policy._installable_source_tree_sha256(package.parent),
        )

    approve_current_source_for_semantic_check()

    assert check_gateway_read_allowlist(tmp_path) == []

    (routes / "read_only.py").write_text(
        route_source.replace(
            'router = APIRouter(prefix="/internal/v1/read-only")',
            'router = object()\ndecoy = APIRouter(prefix="/internal/v1/read-only")',
        )
    )
    approve_current_source_for_semantic_check()
    assert {
        finding.rule for finding in check_gateway_read_allowlist(tmp_path)
    } == {"gateway_read_allowlist_mismatch"}

    (routes / "read_only.py").write_text(route_source)
    (package / "main.py").write_text(
        "app.include_router(read_only_router)\napp.include_router(extra_router)\n"
    )
    approve_current_source_for_semantic_check()
    assert {
        finding.rule for finding in check_gateway_read_allowlist(tmp_path)
    } == {"gateway_read_allowlist_mismatch"}

    (package / "main.py").write_text("app.include_router(read_only_router)\n")
    (package / "main.py").write_text(
        "app.include_router(read_only_router)\n"
        '@app.post("/internal/v1/read-only/unreviewed")\n'
        "def unreviewed(): pass\n"
    )
    approve_current_source_for_semantic_check()
    assert {
        finding.rule for finding in check_gateway_read_allowlist(tmp_path)
    } == {"gateway_read_allowlist_mismatch"}

    (package / "main.py").write_text("app.include_router(read_only_router)\n")
    (routes / "read_only.py").write_text(
        route_source
        + "def fosun_get_order_book(): pass\n"
        + "extra_registrar = router.add_api_route\n"
        + 'extra_registrar("/fosun_get_order_book", fosun_get_order_book, '
        'methods=["POST"])\n'
    )
    approve_current_source_for_semantic_check()
    assert {
        finding.rule for finding in check_gateway_read_allowlist(tmp_path)
    } == {"gateway_read_allowlist_mismatch"}

    (routes / "read_only.py").write_text(
        route_source + '@router.get("/unreviewed")\ndef unreviewed(): pass\n'
    )
    approve_current_source_for_semantic_check()
    assert {
        finding.rule for finding in check_gateway_read_allowlist(tmp_path)
    } == {"gateway_read_allowlist_mismatch"}

    (routes / "read_only.py").write_text(route_source)
    (policy / "operations.py").write_text(
        operation_source.replace(
            read_line,
            "EXTRA_OPERATIONS = frozenset({'fosun_get_order_book'})\n"
            + read_line
            + " | EXTRA_OPERATIONS",
        )
    )
    approve_current_source_for_semantic_check()
    assert {
        finding.rule for finding in check_gateway_read_allowlist(tmp_path)
    } == {"gateway_read_allowlist_mismatch"}

    (policy / "operations.py").write_text(operation_source)
    (policy / "operations.py").write_text(
        operation_source
        + "if True:\n"
        + "    READ_ONLY_OPERATIONS = READ_ONLY_OPERATIONS | "
        + "frozenset({'fosun_get_order_book'})\n"
    )
    approve_current_source_for_semantic_check()
    assert {
        finding.rule for finding in check_gateway_read_allowlist(tmp_path)
    } == {"gateway_read_allowlist_mismatch"}

    (policy / "operations.py").write_text(operation_source)
    (adapter / "sdk.py").write_text(
        adapter_source.replace(
            '        self._method("market", "kline")\n',
            '        self._method("market", "kline")\n'
            "        resolver = self._method\n"
            '        resolver("market", "order_book")\n',
        )
    )
    approve_current_source_for_semantic_check()
    assert {
        finding.rule for finding in check_gateway_read_allowlist(tmp_path)
    } == {"gateway_read_allowlist_mismatch"}

    (adapter / "sdk.py").write_text(adapter_source)
    approve_current_source_for_semantic_check()
    (adapter / "sdk.py").write_text(
        adapter_source
        + "def hidden_write(self):\n"
        + '    client = vars(self)["_" + "client"]\n'
        + '    section = getattr(client, "wea" + "lth")\n'
        + '    return getattr(section, "redeem_" + "fund")()\n'
    )
    assert {
        finding.rule for finding in check_gateway_read_allowlist(tmp_path)
    } == {"gateway_read_allowlist_mismatch"}

    (adapter / "sdk.py").write_text(adapter_source)
    approve_current_source_for_semantic_check()
    bridge = package / "security/dependency_logging.py"
    bridge.parent.mkdir()
    bridge.write_text(
        "import importlib\n"
        "loader = getattr(importlib, ''.join(('import', '_module')))\n"
        "loader('.'.join(('nexus_fosun_gateway', 'publication')))\n"
    )
    assert {
        finding.rule for finding in check_gateway_read_allowlist(tmp_path)
    } == {"gateway_read_allowlist_mismatch"}
    bridge.unlink()

    approve_current_source_for_semantic_check()
    sitecustomize = package.parent / "sitecustomize.py"
    sitecustomize.write_text("raise RuntimeError('must never be installable')\n")
    assert {
        finding.rule for finding in check_gateway_read_allowlist(tmp_path)
    } == {"gateway_read_allowlist_mismatch"}
    sitecustomize.unlink()

    approve_current_source_for_semantic_check()
    extension_override = package / "main.so"
    extension_override.write_bytes(b"synthetic-extension-override")
    assert {
        finding.rule for finding in check_gateway_read_allowlist(tmp_path)
    } == {"gateway_read_allowlist_mismatch"}
    extension_override.unlink()

    approve_current_source_for_semantic_check()
    with (adapter / "sdk.py").open("a") as stream:
        stream.write('        self._method("wealth", "redeem_fund")\n')
    approve_current_source_for_semantic_check()
    assert {
        finding.rule for finding in check_gateway_read_allowlist(tmp_path)
    } == {"gateway_read_allowlist_mismatch"}


def test_gateway_entrypoints_are_frozen_away_from_probe_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pyproject = tmp_path / "integrations/fosun_gateway/pyproject.toml"
    compose = tmp_path / "infra/compose.yaml"
    dockerfile = tmp_path / "infra/docker/fosun-gateway.Dockerfile"
    pyproject.parent.mkdir(parents=True)
    dockerfile.parent.mkdir(parents=True)
    safe_pyproject = (
        '[project]\nname="synthetic"\n[project.scripts]\n'
        'nexus-fosun-gateway="nexus_fosun_gateway.main:run"\n'
        '[build-system]\nrequires=["hatchling"]\n'
        'build-backend="hatchling.build"\n'
        '[tool.hatch.build.targets.wheel]\n'
        'packages=["src/nexus_fosun_gateway"]\n'
    )
    safe_packaging = tomllib.loads(safe_pyproject)
    monkeypatch.setattr(
        repository_policy,
        "_EXPECTED_FOSUN_GATEWAY_PROJECT",
        safe_packaging["project"],
    )
    monkeypatch.setattr(
        repository_policy,
        "_EXPECTED_FOSUN_GATEWAY_BUILD_SYSTEM",
        safe_packaging["build-system"],
    )
    monkeypatch.setattr(
        repository_policy,
        "_EXPECTED_FOSUN_GATEWAY_TOOL",
        safe_packaging["tool"],
    )
    pyproject.write_text(safe_pyproject)
    safe_compose = 'services:\n  fosun-gateway:\n    command: ["nexus-fosun-gateway"]\n'
    safe_dockerfile = (
        "FROM synthetic:test@sha256:" + "a" * 64 + "\n"
        'ENTRYPOINT []\nCMD ["nexus-fosun-gateway"]\n'
    )
    compose.write_text(safe_compose)
    dockerfile.write_text(safe_dockerfile)

    def approve_current_composition_for_semantic_check() -> None:
        pyproject_content = pyproject.read_text()
        compose_content = compose.read_text()
        dockerfile_content = dockerfile.read_text()
        monkeypatch.setattr(
            repository_policy,
            "_EXPECTED_FOSUN_GATEWAY_PYPROJECT_SHA256",
            hashlib.sha256(pyproject_content.encode()).hexdigest(),
        )
        monkeypatch.setattr(
            repository_policy,
            "_EXPECTED_COMPOSE_SHA256",
            hashlib.sha256(compose_content.encode()).hexdigest(),
        )
        monkeypatch.setattr(
            repository_policy,
            "_EXPECTED_FOSUN_GATEWAY_SERVICE_SHA256",
            hashlib.sha256(
                repository_policy._compose_service_block(
                    compose_content,
                    "fosun-gateway",
                ).encode()
            ).hexdigest(),
        )
        monkeypatch.setattr(
            repository_policy,
            "_EXPECTED_FOSUN_GATEWAY_DOCKERFILE_SHA256",
            hashlib.sha256(dockerfile_content.encode()).hexdigest(),
        )

    approve_current_composition_for_semantic_check()

    assert check_gateway_entrypoint_isolation(tmp_path) == []

    with pyproject.open("a") as stream:
        stream.write(
            '[tool.hatch.build.targets.wheel.force-include]\n'
            '"src/nexus_fosun_gateway/probe_cli.py"="sitecustomize.py"\n'
        )
    approve_current_composition_for_semantic_check()
    assert {
        finding.rule for finding in check_gateway_entrypoint_isolation(tmp_path)
    } == {"gateway_entrypoint_isolation_missing"}
    pyproject.write_text(safe_pyproject)
    approve_current_composition_for_semantic_check()

    with pyproject.open("a") as stream:
        stream.write(
            '[project.gui-scripts]\n'
            'nexus-fosun-gateway="nexus_fosun_gateway.probe_cli:run"\n'
        )
    approve_current_composition_for_semantic_check()
    assert {
        finding.rule for finding in check_gateway_entrypoint_isolation(tmp_path)
    } == {"gateway_entrypoint_isolation_missing"}
    pyproject.write_text(safe_pyproject)
    approve_current_composition_for_semantic_check()

    with pyproject.open("a") as stream:
        stream.write("# unreviewed packaging metadata change\n")
    assert {
        finding.rule for finding in check_gateway_entrypoint_isolation(tmp_path)
    } == {"gateway_entrypoint_isolation_missing"}
    pyproject.write_text(safe_pyproject)
    approve_current_composition_for_semantic_check()

    compose.write_text(
        safe_compose
        + "configs:\n"
        + "  fosun-activation-publisher-key:\n"
        + "    file: /tmp/attacker-controlled-key\n"
    )
    assert {
        finding.rule for finding in check_gateway_entrypoint_isolation(tmp_path)
    } == {"gateway_entrypoint_isolation_missing"}
    compose.write_text(safe_compose)
    approve_current_composition_for_semantic_check()

    compose.write_text(
        'services:\n  fosun-gateway:\n'
        '    command: ["nexus-fosun-read-only-probe"]\n'
    )
    approve_current_composition_for_semantic_check()
    assert {
        finding.rule for finding in check_gateway_entrypoint_isolation(tmp_path)
    } == {"gateway_entrypoint_isolation_missing"}

    compose.write_text(
        'services:\n  fosun-gateway:\n'
        '    command: ["nexus-fosun-read-only-probe"]\n'
        '  decoy:\n    # command: ["nexus-fosun-gateway"]\n'
    )
    approve_current_composition_for_semantic_check()
    assert {
        finding.rule for finding in check_gateway_entrypoint_isolation(tmp_path)
    } == {"gateway_entrypoint_isolation_missing"}

    compose.write_text(safe_compose)
    dockerfile.write_text(
        "FROM synthetic:test@sha256:" + "a" * 64 + "\n"
        'ENTRYPOINT []\nCMD ["nexus-fosun-gateway"]\n'
        'CMD ["nexus-fosun-read-only-probe"]\n'
    )
    approve_current_composition_for_semantic_check()
    assert {
        finding.rule for finding in check_gateway_entrypoint_isolation(tmp_path)
    } == {"gateway_entrypoint_isolation_missing"}

    dockerfile.write_text(
        "FROM synthetic:test@sha256:" + "a" * 64 + "\n"
        'ENTRYPOINT []\ncmd ["nexus-fosun-read-only-probe"]\n'
    )
    approve_current_composition_for_semantic_check()
    assert {
        finding.rule for finding in check_gateway_entrypoint_isolation(tmp_path)
    } == {"gateway_entrypoint_isolation_missing"}

    dockerfile.write_text(
        "FROM synthetic:test@sha256:" + "a" * 64 + "\n"
        'ENTRYPOINT ["nexus-fosun-read-only-probe"]\n'
        'CMD ["nexus-fosun-gateway"]\n'
    )
    approve_current_composition_for_semantic_check()
    assert {
        finding.rule for finding in check_gateway_entrypoint_isolation(tmp_path)
    } == {"gateway_entrypoint_isolation_missing"}

    dockerfile.write_text(safe_dockerfile)
    compose.write_text(
        'services:\n  fosun-gateway:\n'
        '    entrypoint: ["nexus-fosun-read-only-probe"]\n'
        '    command: ["nexus-fosun-gateway"]\n'
    )
    approve_current_composition_for_semantic_check()
    assert {
        finding.rule for finding in check_gateway_entrypoint_isolation(tmp_path)
    } == {"gateway_entrypoint_isolation_missing"}

    compose.write_text(
        'services:\n  fosun-gateway:\n    command: ["nexus-fosun-gateway"]\n'
        '    entrypoint : ["nexus-fosun-read-only-probe"]\n'
    )
    approve_current_composition_for_semantic_check()
    assert {
        finding.rule for finding in check_gateway_entrypoint_isolation(tmp_path)
    } == {"gateway_entrypoint_isolation_missing"}

    compose.write_text(
        'services:\n  fosun-gateway:\n    "entrypoint": '
        '["nexus-fosun-read-only-probe"]\n'
        '    command: ["nexus-fosun-gateway"]\n'
    )
    approve_current_composition_for_semantic_check()
    assert {
        finding.rule for finding in check_gateway_entrypoint_isolation(tmp_path)
    } == {"gateway_entrypoint_isolation_missing"}

    compose.write_text(
        'x-probe: &probe\n  entrypoint: ["nexus-fosun-read-only-probe"]\n'
        "  environment:\n    - PYTHONPATH=/unsafe\n"
        "services:\n  fosun-gateway:\n    <<: *probe\n"
        '    command: ["nexus-fosun-gateway"]\n'
    )
    approve_current_composition_for_semantic_check()
    assert {
        finding.rule for finding in check_gateway_entrypoint_isolation(tmp_path)
    } == {"gateway_entrypoint_isolation_missing"}

    compose.write_text(
        "services:\n  fosun-gateway:\n    extends: probe\n"
        '    command: ["nexus-fosun-gateway"]\n'
    )
    approve_current_composition_for_semantic_check()
    assert {
        finding.rule for finding in check_gateway_entrypoint_isolation(tmp_path)
    } == {"gateway_entrypoint_isolation_missing"}

    compose.write_text(safe_compose)
    dockerfile.write_text(
        "FROM synthetic:test@sha256:" + "a" * 64 + " AS unsafe\n"
        'ENTRYPOINT ["nexus-fosun-read-only-probe"]\n'
        'FROM unsafe\nENTRYPOINT []\nCMD ["nexus-fosun-gateway"]\n'
    )
    approve_current_composition_for_semantic_check()
    assert {
        finding.rule for finding in check_gateway_entrypoint_isolation(tmp_path)
    } == {"gateway_entrypoint_isolation_missing"}

    dockerfile.write_text(safe_dockerfile)
    compose.write_text(
        'services:\n  fosun-gateway:\n    command: ["nexus-fosun-gateway"]\n'
        '    healthcheck:\n      test: ["CMD", "nexus-fosun-read-only-probe"]\n'
    )
    approve_current_composition_for_semantic_check()
    assert {
        finding.rule for finding in check_gateway_entrypoint_isolation(tmp_path)
    } == {"gateway_entrypoint_isolation_missing"}

    compose.write_text(safe_compose)
    dockerfile.write_text(
        "ARG FINAL_BASE=unsafe\n"
        "FROM ${FINAL_BASE}\n"
        'ENTRYPOINT []\nCMD ["nexus-fosun-gateway"]\n'
    )
    approve_current_composition_for_semantic_check()
    assert {
        finding.rule for finding in check_gateway_entrypoint_isolation(tmp_path)
    } == {"gateway_entrypoint_isolation_missing"}

    dockerfile.write_text(
        "FROM synthetic:test@sha256:" + "a" * 64 + "\n"
        'CMD ["nexus-fosun-gateway"]\n'
    )
    approve_current_composition_for_semantic_check()
    assert {
        finding.rule for finding in check_gateway_entrypoint_isolation(tmp_path)
    } == {"gateway_entrypoint_isolation_missing"}

    dockerfile.write_text(safe_dockerfile)
    compose.write_text(
        'services:\n  fosun-gateway:\n    command: ["nexus-fosun-gateway"]\n'
        '    environment:\n      - SSLKEYLOGFILE=/tmp/keylog\n'
    )
    approve_current_composition_for_semantic_check()
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
