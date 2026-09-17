"""Deterministic repository, dependency, and CI policy gates for NEXUS."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import re
import subprocess
import sys
import tomllib
from dataclasses import asdict, dataclass
from enum import Enum
from importlib.metadata import distributions
from pathlib import Path
from typing import Any

_SDK_PACKAGE_NAME = "fsopenapi"
_SDK_PACKAGE_VERSION = "0.1.0"
_SDK_GIT_REPOSITORY = "https://github.com/fosunwealth/openapi-python-sdk.git"
_SDK_TAG = "v1.2.0"
_SDK_COMMIT = "891d95fb4967efaa682387c1b35966ceea38fb21"
_SDK_LOCK_SOURCE = f"{_SDK_GIT_REPOSITORY}?rev={_SDK_COMMIT}#{_SDK_COMMIT}"
_SDK_PRIVATE_USE_BASIS = "PROVIDER-AUTHORIZED-PRIVATE-USE"
_FOSUN_REST_BASE_URL = "https://openapi.fosunxcz.com"
_FOSUN_READ_ONLY_OPENAPI = {
    "market": (
        "https://openapi-docs.fosunxcz.com/specs/public/market/market/openapi.yaml",
        "f35cfaa3484d8507723729b68a2b8d60fa85d4d28d06991b4a434995cbfac22a",
    ),
    "trade": (
        "https://openapi-docs.fosunxcz.com/specs/public/trade/openapi.yaml",
        "87e4e6601b8da122a504239a1922d8742ecd4072e2d10c30c0d6a97f896fb808",
    ),
}
_EXPECTED_SDK_EVIDENCE = {
    "written_permission": "docs/runbooks/fosun-sdk-admission.md#private-use-basis",
    "provider_test_vector": "integrations/fosun_gateway/tests/fixtures/fosun",
    "security_review": (
        "docs/superpowers/reviews/"
        "2026-08-27-task-21-final-operational-gates-review.md"
    ),
    "tls_verification_remediation": (
        "integrations/fosun_gateway/tests/security/test_provider_tls.py"
    ),
}
_SHA256_IMAGE = re.compile(r"^[^\s@]+:[^\s@]+@sha256:[0-9a-f]{64}$")
_FORBIDDEN_GATEWAY = re.compile(
    r"(?:\bSimAccount(?:Create|Reset)|"
    r"\b(?:place|create|modify|cancel|submit|replace|delete|update)Order|"
    r"\b(?:Place|Create|Modify|Cancel|Submit|Replace|Delete|Update)Order|"
    r"\bOrder(?:Create|Modify|Cancel|Submit|Replace|Delete|Update)|"
    r"\b(?:create|modify|cancel|submit|replace|delete|update)Subscription|"
    r"\bSubscription(?:Create|Modify|Cancel|Submit|Replace|Delete|Update)|"
    r"(?i:\b(?:place|create|modify|cancel|submit|replace|delete|update)_order)|"
    r"(?i:\border_(?:place|create|modify|cancel|submit|replace|delete|update))|"
    r"(?i:\b(?:create|modify|cancel|submit|replace|delete|update)_subscription)|"
    r"(?i:\bsubscription_(?:create|modify|cancel|submit|replace|delete|update))|"
    r"\b(?:subscribe|unsubscribe)(?:Market|Quote|Order|Trade|Topic)|"
    r"(?i:\bwealth\s*\.)|"
    r"(?i:\b(?:redeem[_]?fund|fund[_]?purchase|withdraw[_]?cash|transfer[_]?assets))|"
    r"(?i:/webhooks?(?:/|[\"'])))"
)
_PROBE_ONLY_RUNTIME = re.compile(
    r"\bnexus_fosun_gateway\.(?:adapter\.probe_sdk|probe_capture|probe_cli|"
    r"probe_receipt|probe_settings|publication|probe)\b|"
    r"\b(?:build_probe_transport|capture_probe_result_frame|"
    r"enforce_probe_tls_controls|load_probe_signing_key|publish_probe_artifact|"
    r"publisher_key_preflight_main|"
    r"run_sanitized_probe)\b"
)
_PROBE_ONLY_MODULES = frozenset(
    {
        "nexus_fosun_gateway.adapter.probe_sdk",
        "nexus_fosun_gateway.probe",
        "nexus_fosun_gateway.probe_capture",
        "nexus_fosun_gateway.probe_cli",
        "nexus_fosun_gateway.probe_receipt",
        "nexus_fosun_gateway.probe_settings",
        "nexus_fosun_gateway.publication",
    }
)
_PROBE_ONLY_NAMES = frozenset(
    {
        "build_probe_transport",
        "capture_probe_result_frame",
        "enforce_probe_tls_controls",
        "load_probe_signing_key",
        "probe",
        "probe_capture",
        "probe_cli",
        "probe_receipt",
        "probe_sdk",
        "probe_settings",
        "publish_probe_artifact",
        "publication",
        "publisher_key_preflight_main",
        "run_sanitized_probe",
    }
)
_REVIEWED_OFFLINE_SIMULATION_PATHS = frozenset(
    {
        "integrations/fosun_gateway/src/nexus_fosun_gateway/adapter/simulation_fixture.py",
        "integrations/fosun_gateway/src/nexus_fosun_gateway/policy/simulation_operations.py",
        "integrations/fosun_gateway/src/nexus_fosun_gateway/routes/simulation.py",
    }
)
_SECRET_PATTERNS = {
    "fosun_api_key": re.compile(r"\bak_[A-Za-z0-9]{20,}=*"),
    "private_key": re.compile(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----"),
    "aws_access_key": re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    "github_token": re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,}\b"),
    "npm_token": re.compile(r"\bnpm_[A-Za-z0-9]{20,}\b"),
    "openai_token": re.compile(r"\bsk-[A-Za-z0-9_-]{24,}\b"),
}
_LOCAL_PYTHON_PACKAGES = {"nexus-engine", "nexus-fosun-gateway", "nexus-workspace"}
_EXPECTED_GATEWAY_OPERATIONS = frozenset(
    {
        "fosun_capabilities",
        "fosun_sync_accounts",
        "fosun_sync_portfolio",
        "fosun_list_orders",
        "fosun_list_cash_flows",
        "fosun_get_quotes",
        "fosun_get_bars",
    }
)
_EXPECTED_BROKER_OPERATIONS = frozenset(
    {
        "fosun_capabilities",
        "fosun_sync_accounts",
        "fosun_sync_portfolio",
        "fosun_sync_portfolio:cash",
        "fosun_sync_portfolio:holdings",
        "fosun_list_orders",
        "fosun_list_cash_flows",
    }
)
_EXPECTED_MARKET_OPERATIONS = frozenset(
    {"fosun_capabilities", "fosun_get_quotes", "fosun_get_bars"}
)
_EXPECTED_GATEWAY_ROUTES = frozenset(
    (f"/{operation}", operation, ("POST",))
    for operation in _EXPECTED_GATEWAY_OPERATIONS
)
_EXPECTED_GATEWAY_ROUTE_PREFIX = "/internal/v1/read-only"
_EXPECTED_SDK_READ_METHODS = frozenset(
    {
        ("account", "list_accounts"),
        ("portfolio", "get_assets_summary"),
        ("portfolio", "get_holdings"),
        ("trade", "list_orders"),
        ("trade", "get_cash_flows"),
        ("market", "quote"),
        ("market", "kline"),
    }
)
# These hashes are a deliberate review tripwire, not a substitute for the
# semantic checks below.  The four files jointly define the production HTTP
# and Provider-call surface.  Any byte-level change must update this reviewed
# contract in the same diff instead of relying on an incomplete AST blacklist.
_EXPECTED_GATEWAY_SOURCE_SHA256 = {
    "policy/operations.py": "0c91bb7741d51103e6c109f06ae11433b0f80f8ff4eb8e8ed49099b1325e77be",
    "routes/read_only.py": "b989b806f6fcb29cf7cb6cc0f686f2aef89eb933759af3b9fda1bce0d471e995",
    "adapter/sdk.py": "c55898547c29e4e0a4489c17b87b4f10d9c3584d23f99c2660b221c147fa05b7",
    "main.py": "dcab8e8167b7a866b1fe5b6bd1d36c4c69f75723c338bec68d5052cbc6c8ae65",
}
_EXPECTED_GATEWAY_INSTALLABLE_SRC_SHA256 = (
    "00c8c68a02d9865eb81cfcc872b2243c4188de802bfce4ee2c5febb5485ecfda"
)
_EXPECTED_FOSUN_GATEWAY_PYPROJECT_SHA256 = (
    "af648cc96932885d58dca6acb6b5d18f5f0da21ad35e9e37db3eda9b9177c6ea"
)
_EXPECTED_FOSUN_GATEWAY_PROJECT = {
    "name": "nexus-fosun-gateway",
    "version": "0.1.0",
    "description": "NEXUS isolated Fosun read-only gateway",
    "requires-python": ">=3.12,<3.13",
    "dependencies": [
        "cryptography>=50,<51",
        "fastapi>=0.116,<0.141",
        "fsopenapi",
        "httpx>=0.28,<0.29",
        "limits>=5.4,<6",
        "pydantic-settings>=2.10,<3",
        "uvicorn>=0.35,<1",
        "websockets==17.1",
    ],
    "scripts": {
        "nexus-fosun-gateway": "nexus_fosun_gateway.main:run",
        "nexus-fosun-gateway-ready": "nexus_fosun_gateway.readiness:main",
        "nexus-fosun-activation-gate": "nexus_fosun_gateway.activation:main",
        "nexus-fosun-read-only-probe": "nexus_fosun_gateway.probe_cli:run",
        "nexus-fosun-capture-read-only-probe": (
            "nexus_fosun_gateway.probe_capture:run"
        ),
        "nexus-fosun-push-probe": "nexus_fosun_gateway.push_probe_cli:run",
        "nexus-fosun-probe-publisher-key-preflight": (
            "nexus_fosun_gateway.probe_capture:key_preflight_run"
        ),
        "nexus-fosun-publish-probe-receipt": (
            "nexus_fosun_gateway.probe_receipt:run"
        ),
        "nexus-fosun-simulation-fixture": (
            "nexus_fosun_gateway.simulation_main:run_fixture"
        ),
    },
}
_EXPECTED_FOSUN_GATEWAY_BUILD_SYSTEM = {
    "requires": ["hatchling"],
    "build-backend": "hatchling.build",
}
_EXPECTED_FOSUN_GATEWAY_TOOL = {
    "hatch": {
        "build": {
            "targets": {"wheel": {"packages": ["src/nexus_fosun_gateway"]}}
        }
    },
    "uv": {
        "sources": {
            "fsopenapi": {
                "git": "https://github.com/fosunwealth/openapi-python-sdk.git",
                "rev": "891d95fb4967efaa682387c1b35966ceea38fb21",
            }
        }
    },
}
_EXPECTED_COMPOSE_SHA256 = (
    "02870b2529070646f9f95ae27fe3847439aabed502e5adb3bb4b8ec5bed59fcf"
)
_EXPECTED_FOSUN_GATEWAY_SERVICE_SHA256 = (
    "f22c88598929e7cab2282fab240b0d19791e12a7af4c6ca051df99d7eda9d6f1"
)
_EXPECTED_FOSUN_GATEWAY_DOCKERFILE_SHA256 = (
    "0f04089c2c5285b8e46af2d76a8ff0f9a5991fca94a7cec96b2506dc06efaf1f"
)
_REAL_E2E_CONFIG_PATH = "playwright.real.config.ts"
_REAL_E2E_TEST_DIRECTORY = "tests/e2e-real"
_REAL_E2E_TEST_PATH = f"{_REAL_E2E_TEST_DIRECTORY}/integrations-real.spec.ts"
_REAL_E2E_PACKAGE_SCRIPT = "test:e2e:real"
_REAL_E2E_PACKAGE_COMMAND = "node scripts/e2e/run-real-fixture.mjs"
_REAL_E2E_WORKFLOW_PATH = ".github/workflows/ci.yml"
_REAL_E2E_JOBS_SECTION = re.compile(r"^jobs\s*:\s*(?:#.*)?$")
_REAL_E2E_WORKFLOW_JOB = re.compile(r"^(?:real-e2e|['\"]real-e2e['\"])\s*:")
_REAL_E2E_YAML_INDIRECTION = re.compile(
    r"(?m)(?:^\s*<<\s*:|(?:^|[\s:\[,])[&*][A-Za-z_][A-Za-z0-9_-]*)"
)
_REAL_E2E_WORKFLOW_RUN_SHELL = re.compile(
    r"(?ms)^defaults:\s*$.*?^  run:\s*$.*?^    shell\s*:"
)
_REAL_E2E_NETWORK_MOCK = re.compile(
    r"\b(?:page|context|browserContext)\s*(?:\.\s*|\[\s*['\"`])(?:route|routeFromHAR|addInitScript)(?:['\"`]\s*\])?\s*\(|"
    r"\broute\s*(?:\.\s*|\[\s*['\"`])fulfill(?:['\"`]\s*\])?\s*\(|"
    r"\bnavigator\s*(?:\.\s*serviceWorker|\[\s*['\"`]serviceWorker['\"`]\s*\])\s*(?:\.\s*|\[\s*['\"`])register(?:['\"`]\s*\])?\s*\(|"
    r"\b(?:window|globalThis)\s*(?:\.\s*fetch|\[\s*['\"`]fetch['\"`]\s*\])\s*="
)
_REAL_E2E_IMPORT = re.compile(
    r"(?:\bfrom\s*|\bimport\s*(?:/\*.*?\*/\s*)?(?:\(\s*(?:/\*.*?\*/\s*)?)?)[\"'`]([^\"'`]+)[\"'`]",
    re.DOTALL,
)
_REAL_E2E_CONFIG_REQUIRED = (
    'const engineUrl = "http://127.0.0.1:8100";',
    'const webUrl = "http://127.0.0.1:4174";',
    'testDir: "./tests/e2e-real"',
    'testMatch: "integrations-real.spec.ts"',
    "fullyParallel: false",
    "retries: 0",
    "workers: 1",
    'reporter: [["json", { outputFile: "test-results/real-e2e-results.json" }]]',
    'serviceWorkers: "block"',
    'CLOUDFLARE_LOAD_DEV_VARS_FROM_DOT_ENV: "false"',
    'CLOUDFLARE_INCLUDE_PROCESS_ENV: "false"',
)
# These hashes freeze the approved, reviewed real-fixture execution boundary.
# The workflow digest covers only the real-e2e job so unrelated CI maintenance
# does not require re-approving the real-fixture boundary.
_EXPECTED_REAL_E2E_CONFIG_SHA256 = (
    "78dbbb17a0bcfa2689a349bd8a98254e9e5797a37ea3a2f2a945cfd7519fffb2"
)
_EXPECTED_REAL_E2E_WORKFLOW_JOB_SHA256 = (
    "a9f765040dd2916c8950991854944b5a78501c57db87047bcb6e02a3768a2d66"
)
_EXPECTED_REAL_E2E_TEST_SHA256 = (
    "4814a5d221a2dec347afd2b544df97620b4ff20b18b00d6c1c0a160889c2e001"
)
_REAL_E2E_POSTGRES_IMAGE = (
    "postgres:17@sha256:"
    "e38411452a464af89e5adadb8d223bf53b898d47d6ef918b2d58c08707350449"
)
_REAL_E2E_DATABASE_URL = (
    "postgresql+asyncpg://nexus_e2e_ci:nexus_e2e_synthetic_ci_only@"
    "127.0.0.1:5432/nexus_e2e_ci"
)


@dataclass(frozen=True, order=True)
class PolicyFinding:
    rule: str
    path: str
    message: str


class GitRepositoryState(str, Enum):
    VALID = "valid"
    NON_REPOSITORY = "non_repository"
    ERROR = "error"


@dataclass(frozen=True)
class GitArtifactIdentity:
    repository_state: GitRepositoryState
    mode: str | None
    object_id: str | None


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
        if _relative(root, path) in _REVIEWED_OFFLINE_SIMULATION_PATHS:
            continue
        if _FORBIDDEN_GATEWAY.search(_read_text(path)):
            findings.append(
                PolicyFinding(
                    "forbidden_gateway_surface",
                    _relative(root, path),
                    "gateway production source contains a write or webhook surface token",
                )
            )
    return findings


def check_provider_tls_remediation(root: Path) -> list[PolicyFinding]:
    """Pin the two reviewed v1.2.0 verify=False call sites to gateway-owned verification."""
    main_path = root / "integrations/fosun_gateway/src/nexus_fosun_gateway/main.py"
    remediation_path = (
        root
        / "integrations/fosun_gateway/src/nexus_fosun_gateway/security/provider_tls.py"
    )
    if not main_path.is_file() or not remediation_path.is_file():
        return [
            PolicyFinding(
                "provider_tls_remediation_missing",
                "integrations/fosun_gateway/src/nexus_fosun_gateway",
                "provider TLS remediation source is unavailable",
            )
        ]
    main_content = _read_text(main_path)
    remediation = _read_text(remediation_path)
    gateway_source = "\n".join(
        _read_text(path)
        for path in sorted((root / "integrations/fosun_gateway/src").rglob("*.py"))
    )
    if (
        "enforce_sdk_tls_verification(client, auth_module)" not in main_content
        or remediation.count('kwargs["verify"] = True') < 2
        or remediation.count('kwargs["allow_redirects"] = False') < 2
        or re.search(r"verify\s*=\s*False", gateway_source) is not None
    ):
        return [
            PolicyFinding(
                "provider_tls_remediation_missing",
                _relative(root, remediation_path),
                "both reviewed SDK HTTP surfaces must force certificate verification",
            )
        ]
    return []


def check_probe_runtime_isolation(root: Path) -> list[PolicyFinding]:
    """Keep the one-shot no-activation constructor out of the long-running Gateway."""
    main_path = root / "integrations/fosun_gateway/src/nexus_fosun_gateway/main.py"
    if not main_path.is_file():
        return [_probe_runtime_isolation_finding(root, main_path)]
    try:
        installable_source_hash = _installable_source_tree_sha256(main_path.parent.parent)
    except OSError:
        return [_probe_runtime_isolation_finding(root, main_path)]
    main_source = _read_text(main_path)
    if (
        installable_source_hash != _EXPECTED_GATEWAY_INSTALLABLE_SRC_SHA256
        or _PROBE_ONLY_RUNTIME.search(main_source) is not None
        or _runtime_import_graph_reaches_probe_only(main_path)
    ):
        return [_probe_runtime_isolation_finding(root, main_path)]
    return []


def _probe_runtime_isolation_finding(root: Path, main_path: Path) -> PolicyFinding:
    return PolicyFinding(
        "probe_runtime_isolation_missing",
        _relative(root, main_path),
        "long-running gateway source or import graph reaches an unreviewed runtime surface",
    )


def check_gateway_read_allowlist(root: Path) -> list[PolicyFinding]:
    """Require the exact seven operations, routes, and Provider SDK read methods."""
    package = root / "integrations/fosun_gateway/src/nexus_fosun_gateway"
    operations_path = package / "policy/operations.py"
    routes_path = package / "routes/read_only.py"
    adapter_path = package / "adapter/sdk.py"
    main_path = package / "main.py"
    reviewed_sources = {
        "policy/operations.py": operations_path,
        "routes/read_only.py": routes_path,
        "adapter/sdk.py": adapter_path,
        "main.py": main_path,
    }
    try:
        installable_source_hash = _installable_source_tree_sha256(package.parent)
        source_hashes = {
            relative: hashlib.sha256(path.read_bytes()).hexdigest()
            for relative, path in reviewed_sources.items()
        }
        operations_tree = ast.parse(_read_text(operations_path))
        routes_tree = ast.parse(_read_text(routes_path))
        adapter_tree = ast.parse(_read_text(adapter_path))
        main_tree = ast.parse(_read_text(main_path))
    except (OSError, SyntaxError):
        return [_gateway_read_allowlist_finding(root, package)]
    if (
        installable_source_hash != _EXPECTED_GATEWAY_INSTALLABLE_SRC_SHA256
        or source_hashes != _EXPECTED_GATEWAY_SOURCE_SHA256
    ):
        return [_gateway_read_allowlist_finding(root, package)]

    operation_literal = _literal_type_alias(operations_tree, "GatewayOperation")
    operations = _exact_frozen_string_assignment(
        operations_tree,
        "READ_ONLY_OPERATIONS",
    )
    broker_operations = _exact_frozen_string_assignment(
        operations_tree,
        "BROKER_OPERATIONS",
    )
    market_operations = _exact_frozen_string_assignment(
        operations_tree,
        "MARKET_OPERATIONS",
    )
    operation_bindings_are_exact = all(
        sum(
            isinstance(node, ast.Name)
            and node.id == name
            and isinstance(node.ctx, ast.Store)
            for node in ast.walk(operations_tree)
        )
        == 1
        for name in (
            "GatewayOperation",
            "READ_ONLY_OPERATIONS",
            "BROKER_OPERATIONS",
            "MARKET_OPERATIONS",
        )
    )
    routes: set[tuple[str, str, tuple[str, ...]]] = set()
    route_calls: list[ast.Call] = []
    malformed_route = False
    route_prefix: str | None = None
    for node in routes_tree.body:
        if not (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and any(
                isinstance(target, ast.Name) and target.id == "router"
                for target in node.targets
            )
            and isinstance(node.value, ast.Call)
            and isinstance(node.value.func, ast.Name)
            and node.value.func.id == "APIRouter"
            and not node.value.args
            and len(node.value.keywords) == 1
        ):
            continue
        prefix_keyword = next(
            (keyword.value for keyword in node.value.keywords if keyword.arg == "prefix"),
            None,
        )
        if (
            isinstance(prefix_keyword, ast.Constant)
            and isinstance(prefix_keyword.value, str)
        ):
            route_prefix = prefix_keyword.value
        else:
            malformed_route = True
    for node in ast.walk(routes_tree):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "add_api_route"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "router"
        ):
            continue
        route_calls.append(node)
        methods_keyword = next(
            (keyword.value for keyword in node.keywords if keyword.arg == "methods"),
            None,
        )
        if (
            len(node.args) != 2
            or not isinstance(node.args[0], ast.Constant)
            or not isinstance(node.args[0].value, str)
            or not isinstance(node.args[1], ast.Name)
            or not isinstance(methods_keyword, ast.List | ast.Tuple)
            or len(node.keywords) != 1
            or any(
                not isinstance(item, ast.Constant) or not isinstance(item.value, str)
                for item in methods_keyword.elts
            )
        ):
            malformed_route = True
            continue
        routes.add(
            (
                node.args[0].value,
                node.args[1].id,
                tuple(item.value for item in methods_keyword.elts),  # type: ignore[misc]
            )
        )

    route_function_attributes = [
        node
        for node in ast.walk(routes_tree)
        if isinstance(node, ast.Attribute) and node.attr == "add_api_route"
    ]
    router_loads = [
        node
        for node in ast.walk(routes_tree)
        if isinstance(node, ast.Name)
        and node.id == "router"
        and isinstance(node.ctx, ast.Load)
    ]
    router_stores = [
        node
        for node in ast.walk(routes_tree)
        if isinstance(node, ast.Name)
        and node.id == "router"
        and isinstance(node.ctx, ast.Store)
    ]
    direct_route_functions = {id(node.func) for node in route_calls}
    direct_router_loads = {
        id(node.func.value)
        for node in route_calls
        if isinstance(node.func, ast.Attribute)
    }
    top_level_route_calls = {
        id(node.value)
        for node in routes_tree.body
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call)
    }
    if (
        {id(node) for node in route_function_attributes} != direct_route_functions
        or {id(node) for node in router_loads} != direct_router_loads
        or len(router_stores) != 1
        or top_level_route_calls != {id(node) for node in route_calls}
        or any(
            isinstance(node, ast.Constant)
            and node.value in {"add_api_route", "include_router"}
            for node in ast.walk(routes_tree)
        )
    ):
        malformed_route = True

    sdk_methods: set[tuple[str, str]] = set()
    malformed_sdk_method = False
    sdk_method_calls: list[ast.Call] = []
    for node in ast.walk(adapter_tree):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "_method"
        ):
            continue
        sdk_method_calls.append(node)
        if (
            len(node.args) != 2
            or node.keywords
            or any(
                not isinstance(argument, ast.Constant)
                or not isinstance(argument.value, str)
                for argument in node.args
            )
        ):
            malformed_sdk_method = True
            continue
        sdk_methods.add((node.args[0].value, node.args[1].value))  # type: ignore[arg-type]

    sdk_method_attributes = [
        node
        for node in ast.walk(adapter_tree)
        if isinstance(node, ast.Attribute) and node.attr == "_method"
    ]
    sdk_client_attributes = [
        node
        for node in ast.walk(adapter_tree)
        if isinstance(node, ast.Attribute) and node.attr == "_client"
    ]
    if (
        {id(node) for node in sdk_method_attributes}
        != {id(node.func) for node in sdk_method_calls}
        or len(sdk_client_attributes) != 2
        or sum(isinstance(node.ctx, ast.Store) for node in sdk_client_attributes) != 1
        or sum(isinstance(node.ctx, ast.Load) for node in sdk_client_attributes) != 1
        or any(
            isinstance(node, ast.Constant) and node.value in {"_client", "_method"}
            for node in ast.walk(adapter_tree)
        )
    ):
        malformed_sdk_method = True

    main_include_calls = [
        node
        for node in ast.walk(main_tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "include_router"
    ]
    main_include_attributes = [
        node
        for node in ast.walk(main_tree)
        if isinstance(node, ast.Attribute) and node.attr == "include_router"
    ]
    main_router_graph_is_exact = (
        len(main_include_calls) == 1
        and len(main_include_attributes) == 1
        and main_include_calls[0].func is main_include_attributes[0]
        and isinstance(main_include_calls[0].func.value, ast.Name)
        and main_include_calls[0].func.value.id == "app"
        and len(main_include_calls[0].args) == 1
        and isinstance(main_include_calls[0].args[0], ast.Name)
        and main_include_calls[0].args[0].id == "read_only_router"
        and not main_include_calls[0].keywords
        and not any(
            isinstance(node, ast.Constant) and node.value == "include_router"
            for node in ast.walk(main_tree)
        )
    )
    allowed_app_registration_calls = {
        id(node)
        for node in ast.walk(main_tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "app"
        and (
            (
                node.func.attr == "include_router"
                and len(node.args) == 1
                and isinstance(node.args[0], ast.Name)
                and node.args[0].id == "read_only_router"
                and not node.keywords
            )
            or (
                node.func.attr == "get"
                and len(node.args) == 1
                and isinstance(node.args[0], ast.Constant)
                and node.args[0].value in {"/health/live", "/health/ready"}
                and not node.keywords
            )
            or (
                node.func.attr == "exception_handler"
                and len(node.args) == 1
                and isinstance(node.args[0], ast.Name)
                and node.args[0].id in {"GatewayError", "StarletteHTTPException"}
                and not node.keywords
            )
        )
    }
    app_registration_calls = {
        id(node)
        for node in ast.walk(main_tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "app"
        and node.func.attr
        in {
            "add_api_route",
            "add_route",
            "api_route",
            "delete",
            "exception_handler",
            "get",
            "head",
            "include_router",
            "mount",
            "options",
            "patch",
            "post",
            "put",
            "route",
            "websocket",
            "websocket_route",
        }
    }
    main_registration_surface_is_exact = (
        app_registration_calls == allowed_app_registration_calls
    )

    if (
        operation_literal != set(_EXPECTED_GATEWAY_OPERATIONS)
        or operations != set(_EXPECTED_GATEWAY_OPERATIONS)
        or broker_operations != set(_EXPECTED_BROKER_OPERATIONS)
        or market_operations != set(_EXPECTED_MARKET_OPERATIONS)
        or not operation_bindings_are_exact
        or malformed_route
        or route_prefix != _EXPECTED_GATEWAY_ROUTE_PREFIX
        or routes != set(_EXPECTED_GATEWAY_ROUTES)
        or malformed_sdk_method
        or sdk_methods != set(_EXPECTED_SDK_READ_METHODS)
        or not main_router_graph_is_exact
        or not main_registration_surface_is_exact
    ):
        return [_gateway_read_allowlist_finding(root, package)]
    return []


def _top_level_assignment_values(tree: ast.Module, name: str) -> list[ast.AST]:
    values: list[ast.AST] = []
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == name for target in node.targets
        ):
            values.append(node.value)
        elif (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == name
            and node.value is not None
        ):
            values.append(node.value)
    return values


def _installable_source_tree_sha256(source_root: Path) -> str:
    digest = hashlib.sha256()
    source_count = 0
    for path in sorted(source_root.rglob("*")):
        relative_path = path.relative_to(source_root)
        if "__pycache__" in relative_path.parts:
            continue
        if path.is_dir() and not path.is_symlink():
            continue
        if (
            path.is_symlink()
            or not path.is_file()
            or path.suffix != ".py"
            or not relative_path.parts
            or relative_path.parts[0] != "nexus_fosun_gateway"
        ):
            raise OSError("gateway installable source tree is not closed")
        relative = relative_path.as_posix().encode()
        content = path.read_bytes()
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
        source_count += 1
    if source_count == 0:
        raise OSError("gateway installable source tree is empty")
    return digest.hexdigest()


def _exact_frozen_string_assignment(tree: ast.Module, name: str) -> set[str] | None:
    values = _top_level_assignment_values(tree, name)
    if len(values) != 1:
        return None
    value = values[0]
    if not (
        isinstance(value, ast.Call)
        and isinstance(value.func, ast.Name)
        and value.func.id == "frozenset"
        and len(value.args) == 1
        and not value.keywords
        and isinstance(value.args[0], ast.Set)
        and all(
            isinstance(item, ast.Constant) and isinstance(item.value, str)
            for item in value.args[0].elts
        )
    ):
        return None
    return {item.value for item in value.args[0].elts}  # type: ignore[misc]


def _literal_type_alias(tree: ast.Module, name: str) -> set[str] | None:
    values = _top_level_assignment_values(tree, name)
    if len(values) != 1:
        return None
    value = values[0]
    if not (
        isinstance(value, ast.Subscript)
        and isinstance(value.value, ast.Name)
        and value.value.id == "Literal"
    ):
        return None
    elements = value.slice.elts if isinstance(value.slice, ast.Tuple) else [value.slice]
    if any(
        not isinstance(item, ast.Constant) or not isinstance(item.value, str)
        for item in elements
    ):
        return None
    return {item.value for item in elements}  # type: ignore[misc]


def _gateway_read_allowlist_finding(root: Path, package: Path) -> PolicyFinding:
    return PolicyFinding(
        "gateway_read_allowlist_mismatch",
        _relative(root, package),
        "production operation, route, and SDK method surfaces must match the exact read allowlist",
    )


def check_gateway_entrypoint_isolation(root: Path) -> list[PolicyFinding]:
    """Freeze both packaged and Compose production entry points to Gateway main."""
    pyproject_path = root / "integrations/fosun_gateway/pyproject.toml"
    compose_path = root / "infra/compose.yaml"
    dockerfile_path = root / "infra/docker/fosun-gateway.Dockerfile"
    try:
        pyproject_text = _read_text(pyproject_path)
        pyproject = tomllib.loads(pyproject_text)
        project_config = pyproject["project"]
        scripts = project_config["scripts"]
        build_system = pyproject["build-system"]
        tool_config = pyproject["tool"]
    except (KeyError, OSError, tomllib.TOMLDecodeError, TypeError):
        pyproject_text = ""
        project_config = None
        scripts = None
        build_system = None
        tool_config = None
    packaged_entrypoint = (
        scripts.get("nexus-fosun-gateway") if isinstance(scripts, dict) else None
    )
    try:
        compose = _read_text(compose_path)
        dockerfile = _read_text(dockerfile_path)
    except OSError:
        compose = ""
        dockerfile = ""
    gateway_service = _compose_service_block(compose, "fosun-gateway")
    compose_commands = re.findall(r"(?m)^    command:\s*(.*?)\s*$", gateway_service)
    compose_entrypoints = re.findall(
        r"(?m)^    [\"']?entrypoint[\"']?\s*:\s*(.*?)\s*$",
        gateway_service,
    )
    docker_instructions = _docker_final_stage_process_instructions(dockerfile)
    if (
        packaged_entrypoint != "nexus_fosun_gateway.main:run"
        or hashlib.sha256(pyproject_text.encode()).hexdigest()
        != _EXPECTED_FOSUN_GATEWAY_PYPROJECT_SHA256
        or project_config != _EXPECTED_FOSUN_GATEWAY_PROJECT
        or build_system != _EXPECTED_FOSUN_GATEWAY_BUILD_SYSTEM
        or tool_config != _EXPECTED_FOSUN_GATEWAY_TOOL
        or hashlib.sha256(compose.encode()).hexdigest() != _EXPECTED_COMPOSE_SHA256
        or hashlib.sha256(gateway_service.encode()).hexdigest()
        != _EXPECTED_FOSUN_GATEWAY_SERVICE_SHA256
        or hashlib.sha256(dockerfile.encode()).hexdigest()
        != _EXPECTED_FOSUN_GATEWAY_DOCKERFILE_SHA256
        or compose_commands != ['["nexus-fosun-gateway"]']
        or compose_entrypoints
        or _compose_uses_yaml_aliases(compose)
        or re.search(
            r"(?m)^    [\"']?(?:extends|env_file)[\"']?\s*:",
            gateway_service,
        )
        is not None
        or docker_instructions
        != [("ENTRYPOINT", "[]"), ("CMD", '["nexus-fosun-gateway"]')]
        or any(
            marker in gateway_service or marker in dockerfile
            for marker in (
                "PYTHONHOME",
                "PYTHONPATH",
                "SSLKEYLOGFILE",
                "nexus-fosun-read-only-probe",
                "nexus-fosun-capture-read-only-probe",
            )
        )
    ):
        return [
            PolicyFinding(
                "gateway_entrypoint_isolation_missing",
                "integrations/fosun_gateway/pyproject.toml|infra/compose.yaml|"
                "infra/docker/fosun-gateway.Dockerfile",
                "production packaging and runtime must use the fixed Gateway entry "
                "point without probe or interpreter-injection surfaces",
            )
        ]
    return []


def _compose_service_block(compose: str, service: str) -> str:
    lines = compose.splitlines()
    marker = f"  {service}:"
    try:
        start = next(index for index, line in enumerate(lines) if line == marker)
    except StopIteration:
        return ""
    end = len(lines)
    for index in range(start + 1, len(lines)):
        if re.fullmatch(r"  [A-Za-z0-9_.-]+:\s*", lines[index]):
            end = index
            break
        if lines[index] and not lines[index].startswith(" "):
            end = index
            break
    return "\n".join(lines[start:end])


def _compose_uses_yaml_aliases(compose: str) -> bool:
    """Reject anchors, aliases, and merge keys that obscure the effective service."""
    for raw_line in compose.splitlines():
        line = raw_line.split("#", 1)[0]
        if re.search(r"<<\s*:", line) is not None or re.search(
            r"(?:^|[\s:\[,])[&*][A-Za-z0-9_][A-Za-z0-9_.-]*",
            line,
        ) is not None:
            return True
    return False


def _docker_final_stage_process_instructions(
    dockerfile: str,
) -> list[tuple[str, str]]:
    """Return effective process directives from only the final Docker stage."""
    lines = dockerfile.splitlines()
    stages: list[tuple[int, str, str | None]] = []
    for index, line in enumerate(lines):
        match = re.match(
            r"(?i)^\s*FROM\s+(?:--platform=\S+\s+)?(\S+)"
            r"(?:\s+AS\s+([A-Za-z0-9_.-]+))?\s*$",
            line,
        )
        if match is not None and not line.lstrip().startswith("#"):
            stages.append((index, match.group(1), match.group(2)))
    if not stages:
        return []
    prior_aliases = {
        alias.casefold() for _index, _base, alias in stages[:-1] if alias is not None
    }
    final_index, final_base, _final_alias = stages[-1]
    if (
        final_base.casefold() in prior_aliases
        or "$" in final_base
        or re.fullmatch(r"[^\s@]+:[^\s@]+@sha256:[0-9a-f]{64}", final_base)
        is None
    ):
        return [("INHERITED_STAGE", final_base)]
    instructions: list[tuple[str, str]] = []
    for line in lines[final_index + 1 :]:
        match = re.match(r"(?i)^\s*(CMD|ENTRYPOINT)\s+(.+?)\s*$", line)
        if match is not None and not line.lstrip().startswith("#"):
            instructions.append((match.group(1).upper(), match.group(2)))
    return instructions


def _runtime_import_graph_reaches_probe_only(main_path: Path) -> bool:
    package_root = main_path.parent
    module_paths: dict[str, Path] = {}
    for path in sorted(package_root.rglob("*.py")):
        relative = path.relative_to(package_root)
        parts = list(relative.with_suffix("").parts)
        if parts[-1] == "__init__":
            parts.pop()
        module = ".".join(("nexus_fosun_gateway", *parts))
        module_paths[module] = path

    pending: list[str] = []
    visited: set[str] = set()

    def schedule_with_package_initializers(module: str) -> None:
        parts = module.split(".")
        for length in range(1, len(parts)):
            package = ".".join(parts[:length])
            package_path = module_paths.get(package)
            if (
                package_path is not None
                and package_path.name == "__init__.py"
                and package not in visited
                and package not in pending
            ):
                pending.append(package)
        if module in module_paths and module not in visited and module not in pending:
            pending.append(module)

    schedule_with_package_initializers("nexus_fosun_gateway.main")
    schedule_with_package_initializers("nexus_fosun_gateway.readiness")
    while pending:
        module = pending.pop()
        if module in visited:
            continue
        visited.add(module)
        path = module_paths.get(module)
        if path is None:
            continue
        try:
            tree = ast.parse(_read_text(path))
        except SyntaxError:
            return True
        for imported_module, imported_names in _local_imports(
            module,
            tree,
            is_package=path.name == "__init__.py",
        ):
            if _is_probe_only_module(imported_module):
                return True
            if imported_names & _PROBE_ONLY_NAMES:
                return True
            for candidate in (imported_module, *(
                f"{imported_module}.{name}" for name in imported_names
            )):
                if _is_probe_only_module(candidate):
                    return True
                schedule_with_package_initializers(candidate)
    return False


def _local_imports(
    current_module: str,
    tree: ast.AST,
    *,
    is_package: bool,
) -> list[tuple[str, set[str]]]:
    imports: list[tuple[str, set[str]]] = []
    current_package = (
        current_module if is_package else current_module.rpartition(".")[0]
    )
    allowed_dynamic_calls = {
        id(node)
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and _is_allowed_sdk_dynamic_import(current_module, node)
    }
    allowed_dynamic_attributes = {
        id(node.func)
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and id(node) in allowed_dynamic_calls
    }
    if _has_forbidden_dynamic_import_surface(
        tree,
        allowed_dynamic_attributes=allowed_dynamic_attributes,
    ):
        imports.append(("nexus_fosun_gateway.probe_cli", set()))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend((alias.name, set()) for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if node.level:
                package_parts = current_package.split(".")
                keep = len(package_parts) - node.level + 1
                if keep <= 0:
                    imports.append(("", {alias.name for alias in node.names}))
                    continue
                prefix = ".".join(package_parts[:keep])
                module = f"{prefix}.{module}" if module else prefix
            imports.append((module, {alias.name for alias in node.names}))
        elif isinstance(node, ast.Call) and id(node) in allowed_dynamic_calls:
            module = _static_string(node.args[0])
            assert module is not None
            imports.append((_resolve_dynamic_module(current_package, module), set()))
    return imports


_DYNAMIC_IMPORT_MARKERS = frozenset(
    {
        "__import__",
        "BuiltinImporter",
        "ExtensionFileLoader",
        "FrozenImporter",
        "SourceFileLoader",
        "SourcelessFileLoader",
        "exec_module",
        "find_spec",
        "import_module",
        "load_module",
        "module_from_spec",
        "run_module",
        "run_path",
        "spec_from_file_location",
    }
)


def _is_allowed_sdk_dynamic_import(current_module: str, node: ast.Call) -> bool:
    return (
        current_module == "nexus_fosun_gateway.main"
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "importlib"
        and node.func.attr == "import_module"
        and len(node.args) == 1
        and not node.keywords
        and isinstance(node.args[0], ast.Constant)
        and node.args[0].value in {"fsopenapi", "fsopenapi.auth"}
    )


def _has_forbidden_dynamic_import_surface(
    tree: ast.AST,
    *,
    allowed_dynamic_attributes: set[int],
) -> bool:
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Attribute)
            and node.attr in _DYNAMIC_IMPORT_MARKERS
            and id(node) not in allowed_dynamic_attributes
        ):
            return True
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load) and (
            node.id in _DYNAMIC_IMPORT_MARKERS or node.id in {"compile", "eval", "exec"}
        ):
            return True
        if isinstance(node, ast.ImportFrom) and any(
            alias.name in _DYNAMIC_IMPORT_MARKERS for alias in node.names
        ):
            return True
        if isinstance(node, ast.Constant | ast.BinOp) and _static_string(
            node
        ) in _DYNAMIC_IMPORT_MARKERS:
            return True
    return False


def _static_string(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _static_string(node.left)
        right = _static_string(node.right)
        if left is not None and right is not None:
            return left + right
    return None


def _resolve_dynamic_module(current_package: str, module: str) -> str:
    if not module.startswith("."):
        return module
    level = len(module) - len(module.lstrip("."))
    package_parts = current_package.split(".")
    keep = len(package_parts) - level + 1
    if keep <= 0:
        return ""
    suffix = module[level:]
    prefix = ".".join(package_parts[:keep])
    return f"{prefix}.{suffix}" if suffix else prefix


def _is_probe_only_module(module: str) -> bool:
    return module in _PROBE_ONLY_MODULES or any(
        module.startswith(f"{name}.") for name in _PROBE_ONLY_MODULES
    )


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
    try:
        lock_document = tomllib.loads(_read_text(lock_path))
    except (OSError, UnicodeError, tomllib.TOMLDecodeError):
        return [
            PolicyFinding(
                "sdk_lock_invalid",
                _relative(root, lock_path),
                "provider SDK lock must be valid TOML",
            )
        ]
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
    status = admission.get("status") if isinstance(admission, dict) else None
    sdk_packages = _sdk_lock_packages(lock_document)
    if not sdk_packages:
        if status in {"probe_ready", "admitted"}:
            return [
                PolicyFinding(
                    "admitted_sdk_missing_from_lock",
                    _relative(root, lock_path),
                    "manifest admission and dependency lock disagree",
                )
            ]
        return []
    if not _complete_sdk_admission(manifest):
        return [
            PolicyFinding(
                "sdk_not_admitted",
                _relative(root, lock_path),
                "provider SDK is locked without complete admission evidence",
            )
        ]
    if len(sdk_packages) != 1 or not _sdk_lock_package_is_exact(sdk_packages[0]):
        return [
            PolicyFinding(
                "sdk_lock_not_exact",
                _relative(root, lock_path),
                "provider SDK lock does not contain the admitted commit",
            )
        ]
    return []


def _complete_sdk_admission(document: object) -> bool:
    if not isinstance(document, dict) or document.get("provider") != "fosun":
        return False
    admission = document.get("admission")
    sdk = document.get("sdk")
    openapi = document.get("openapi")
    if (
        not isinstance(admission, dict)
        or not isinstance(sdk, dict)
        or not isinstance(openapi, dict)
    ):
        return False
    if admission.get("status") not in {"probe_ready", "admitted"}:
        return False
    if sdk.get("tag") != _SDK_TAG or sdk.get("commit") != _SDK_COMMIT:
        return False
    if admission.get("sdk_commit") != _SDK_COMMIT:
        return False
    if not _valid_provider_base_url(admission.get("provider_base_url")):
        return False
    if sdk.get("license") != _SDK_PRIVATE_USE_BASIS:
        return False
    admission_hashes = admission.get("openapi_sha256")
    if not isinstance(admission_hashes, dict):
        return False
    for name, (url, digest) in _FOSUN_READ_ONLY_OPENAPI.items():
        contract = openapi.get(name)
        if (
            not isinstance(contract, dict)
            or contract.get("url") != url
            or contract.get("sha256") != digest
            or admission_hashes.get(name) != digest
        ):
            return False
    evidence = admission.get("evidence")
    return evidence == _EXPECTED_SDK_EVIDENCE


def _valid_provider_base_url(value: object) -> bool:
    return value == _FOSUN_REST_BASE_URL


def _sdk_lock_packages(document: object) -> list[dict[str, object]]:
    if not isinstance(document, dict):
        return []
    packages = document.get("package")
    if not isinstance(packages, list):
        return []
    return [
        package
        for package in packages
        if isinstance(package, dict) and package.get("name") == _SDK_PACKAGE_NAME
    ]


def _sdk_lock_package_is_exact(package: dict[str, object]) -> bool:
    source = package.get("source")
    return (
        package.get("name") == _SDK_PACKAGE_NAME
        and package.get("version") == _SDK_PACKAGE_VERSION
        and isinstance(source, dict)
        and source == {"git": _SDK_LOCK_SOURCE}
    )


def _private_sdk_subject_is_exact(root: Path) -> bool:
    try:
        lock_document = tomllib.loads(_read_text(root / "uv.lock"))
        manifest = json.loads(
            _read_text(root / "contracts/providers/fosun/manifest.json")
        )
    except (OSError, UnicodeError, json.JSONDecodeError, tomllib.TOMLDecodeError):
        return False
    sdk_packages = _sdk_lock_packages(lock_document)
    if len(sdk_packages) != 1 or not _sdk_lock_package_is_exact(sdk_packages[0]):
        return False
    if not isinstance(manifest, dict):
        return False
    return _complete_sdk_admission(manifest)


def license_findings(
    records: list[DependencyRecord],
    *,
    root: Path | None = None,
) -> list[PolicyFinding]:
    exact_private_sdk = root is not None and _private_sdk_subject_is_exact(root)
    findings: list[PolicyFinding] = []
    for record in sorted(records):
        identifier = f"{record.ecosystem}:{record.name}@{record.version}"
        normalized = record.license.strip().upper()
        if not normalized:
            if exact_private_sdk and (
                record.ecosystem,
                record.name.lower(),
                record.version,
            ) == ("python", _SDK_PACKAGE_NAME, _SDK_PACKAGE_VERSION):
                # The provider repository publishes no standard license metadata.
                # Its exact Git commit and private-use basis are separately bound by
                # the Fosun admission manifest and SDK lock policy.
                continue
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


def check_real_e2e_boundary(root: Path) -> list[PolicyFinding]:
    """Fail closed once the dedicated real-fixture E2E suite is adopted."""
    config_path = root / _REAL_E2E_CONFIG_PATH
    test_directory = root / _REAL_E2E_TEST_DIRECTORY
    package_path = root / "package.json"
    workflow_path = root / _REAL_E2E_WORKFLOW_PATH
    package_script_declared, package_script = _real_e2e_package_script(package_path)
    workflow_present = workflow_path.exists() or workflow_path.is_symlink()
    workflow_artifact_safe = workflow_present and _tracked_regular_artifact_is_safe(
        root, workflow_path, _REAL_E2E_WORKFLOW_PATH
    )
    workflow_job = (
        _real_e2e_workflow_job_block(workflow_path)
        if workflow_artifact_safe
        else None
    )
    workflow_job_present = workflow_job is not None
    adopted = (
        config_path.exists()
        or test_directory.exists()
        or package_script_declared
        or workflow_present
    )
    if not adopted:
        return []

    findings: list[PolicyFinding] = []
    if not config_path.is_file():
        findings.append(
            PolicyFinding(
                "real_e2e_config_missing",
                _REAL_E2E_CONFIG_PATH,
                "real fixture E2E requires its dedicated Playwright configuration",
            )
        )
    elif not _real_e2e_config_is_safe(root, config_path):
        findings.append(
            PolicyFinding(
                "real_e2e_config_invalid",
                _REAL_E2E_CONFIG_PATH,
                "real fixture E2E Playwright safety semantics must remain fixed",
            )
        )
    if not test_directory.is_dir():
        findings.append(
            PolicyFinding(
                "real_e2e_test_directory_missing",
                _REAL_E2E_TEST_DIRECTORY,
                "real fixture E2E requires its dedicated test directory",
            )
        )
    elif not _real_e2e_test_directory_is_safe(test_directory):
        findings.append(
            PolicyFinding(
                "real_e2e_test_boundary_invalid",
                _REAL_E2E_TEST_DIRECTORY,
                "real fixture E2E must contain only its approved integration journey",
            )
        )
    elif not _real_e2e_test_artifact_is_safe(root):
        findings.append(
            PolicyFinding(
                "real_e2e_test_artifact_invalid",
                _REAL_E2E_TEST_PATH,
                "real fixture E2E journey must be a tracked regular file",
            )
        )
    else:
        test_path = root / _REAL_E2E_TEST_PATH
        if not _path_matches_sha256(test_path, _EXPECTED_REAL_E2E_TEST_SHA256):
            findings.append(
                PolicyFinding(
                    "real_e2e_test_artifact_invalid",
                    _REAL_E2E_TEST_PATH,
                    "real fixture E2E journey must match its reviewed artifact digest",
                )
            )
    if not package_script_declared:
        findings.append(
            PolicyFinding(
                "real_e2e_package_script_missing",
                "package.json",
                "real fixture E2E requires the test:e2e:real package script",
            )
        )
    elif package_script != _REAL_E2E_PACKAGE_COMMAND:
        findings.append(
            PolicyFinding(
                "real_e2e_package_script_invalid",
                "package.json",
                "real fixture E2E package script must invoke only its fixed runner",
            )
        )
    elif _real_e2e_has_package_lifecycle_script(package_path):
        findings.append(
            PolicyFinding(
                "real_e2e_package_lifecycle_forbidden",
                "package.json",
                "real fixture E2E package script must not have npm lifecycle wrappers",
            )
        )
    if not workflow_present:
        findings.append(
            PolicyFinding(
                "real_e2e_ci_job_missing",
                _REAL_E2E_WORKFLOW_PATH,
                "real fixture E2E requires the real-e2e CI job",
            )
        )
    elif not workflow_artifact_safe:
        findings.append(
            PolicyFinding(
                "real_e2e_ci_job_invalid",
                _REAL_E2E_WORKFLOW_PATH,
                "real fixture E2E workflow must be a tracked regular file",
            )
        )
    elif not workflow_job_present:
        findings.append(
            PolicyFinding(
                "real_e2e_ci_job_missing",
                _REAL_E2E_WORKFLOW_PATH,
                "real fixture E2E requires the real-e2e CI job",
            )
        )
    elif not _real_e2e_workflow_is_safe(root, workflow_path, workflow_job):
        findings.append(
            PolicyFinding(
                "real_e2e_ci_job_invalid",
                _REAL_E2E_WORKFLOW_PATH,
                "real fixture E2E CI job must preserve its fixed local-only execution",
            )
        )
    can_scan_real_e2e_source = _frozen_artifact_path_is_safe(
        root, root / _REAL_E2E_TEST_PATH
    )
    if test_directory.is_dir() and can_scan_real_e2e_source:
        for path in sorted(test_directory.rglob("*")):
            if not path.is_file():
                continue
            content = _read_text(path)
            if _REAL_E2E_NETWORK_MOCK.search(content):
                findings.append(
                    PolicyFinding(
                        "real_e2e_network_mock_forbidden",
                        _relative(root, path),
                        "real fixture E2E must not replace network responses",
                    )
                )
            if _real_e2e_has_external_import(path, test_directory, content):
                findings.append(
                    PolicyFinding(
                        "real_e2e_external_import_forbidden",
                        _relative(root, path),
                        "real fixture E2E must not import helpers from outside its boundary",
                    )
                )
    return findings


def _real_e2e_package_script(package_path: Path) -> tuple[bool, str | None]:
    try:
        package = json.loads(_read_text(package_path))
    except (OSError, json.JSONDecodeError):
        return False, None
    scripts = package.get("scripts") if isinstance(package, dict) else None
    if not isinstance(scripts, dict) or _REAL_E2E_PACKAGE_SCRIPT not in scripts:
        return False, None
    script = scripts.get(_REAL_E2E_PACKAGE_SCRIPT)
    return True, script if isinstance(script, str) else None


def _real_e2e_has_package_lifecycle_script(package_path: Path) -> bool:
    try:
        package = json.loads(_read_text(package_path))
    except (OSError, json.JSONDecodeError):
        return False
    scripts = package.get("scripts") if isinstance(package, dict) else None
    return isinstance(scripts, dict) and any(
        lifecycle in scripts
        for lifecycle in (
            f"pre{_REAL_E2E_PACKAGE_SCRIPT}",
            f"post{_REAL_E2E_PACKAGE_SCRIPT}",
        )
    )


def _real_e2e_config_is_safe(root: Path, config_path: Path) -> bool:
    if not _frozen_artifact_is_safe(
        root,
        config_path,
        _REAL_E2E_CONFIG_PATH,
        _EXPECTED_REAL_E2E_CONFIG_SHA256,
    ):
        return False
    try:
        content = _read_text(config_path)
    except OSError:
        return False
    return (
        all(required in content for required in _REAL_E2E_CONFIG_REQUIRED)
        and content.count("reuseExistingServer: false") == 2
        and "reuseExistingServer: true" not in content
    )


def _real_e2e_workflow_is_safe(root: Path, workflow_path: Path, job: str) -> bool:
    if not _tracked_regular_artifact_is_safe(root, workflow_path, _REAL_E2E_WORKFLOW_PATH):
        return False
    try:
        workflow = _read_text(workflow_path)
    except OSError:
        return False
    run_commands = re.findall(r"(?m)^\s*-\s+run:\s*(.+?)\s*$", workflow)
    real_e2e_commands = [
        command for command in run_commands if "npm run test:e2e:real" in command
    ]
    return (
        _normalized_text_sha256(job.encode("utf-8"))
        == _EXPECTED_REAL_E2E_WORKFLOW_JOB_SHA256
        and _REAL_E2E_YAML_INDIRECTION.search(workflow) is None
        and _REAL_E2E_WORKFLOW_RUN_SHELL.search(workflow) is None
        and real_e2e_commands == ["npm run test:e2e:real"]
        and _real_e2e_workflow_job_is_safe(job)
    )


def _real_e2e_workflow_job_is_safe(job: str) -> bool:
    required = (
        "runs-on: ubuntu-24.04",
        "timeout-minutes: 25",
        f"image: {_REAL_E2E_POSTGRES_IMAGE}",
        "POSTGRES_DB: nexus_e2e_ci",
        "POSTGRES_USER: nexus_e2e_ci",
        "POSTGRES_PASSWORD: nexus_e2e_synthetic_ci_only",
        f"NEXUS_REAL_E2E_DATABASE_URL: {_REAL_E2E_DATABASE_URL}",
    )
    exact_run = re.findall(r"(?m)^\s*-\s+run:\s*(.+?)\s*$", job)
    return (
        all(value in job for value in required)
        and exact_run.count("npm run test:e2e:real") == 1
        and not any(
            "test:e2e:real" in command and command != "npm run test:e2e:real"
            for command in exact_run
        )
        and re.search(r"(?m)^ {4}if\s*:", job) is None
        and re.search(r"(?m)^\s*-\s+if\s*:", job) is None
        and "continue-on-error:" not in job
        and "secrets." not in job
        and "NEXUS_ENV: production" not in job
    )


def _real_e2e_has_external_import(path: Path, test_directory: Path, content: str) -> bool:
    boundary = test_directory.resolve()
    for match in _REAL_E2E_IMPORT.finditer(content):
        specifier = match.group(1)
        if specifier == "@playwright/test":
            continue
        if not specifier.startswith("."):
            return True
        try:
            (path.parent / specifier).resolve().relative_to(boundary)
        except ValueError:
            return True
    return False


def _real_e2e_workflow_job_present(workflow_path: Path) -> bool:
    return _real_e2e_workflow_job_block(workflow_path) is not None


def _real_e2e_workflow_job_block(workflow_path: Path) -> str | None:
    try:
        lines = _read_text(workflow_path).splitlines()
    except OSError:
        return None
    jobs_section_index: int | None = None
    for index, line in enumerate(lines):
        if line == line.lstrip(" ") and _REAL_E2E_JOBS_SECTION.fullmatch(line):
            jobs_section_index = index
            break
    if jobs_section_index is None:
        return None

    job_indent: int | None = None
    job_indices: list[int] = []
    for index in range(jobs_section_index + 1, len(lines)):
        line = lines[index]
        stripped = line.lstrip(" ")
        if not stripped or stripped.startswith("#"):
            continue
        indent = len(line) - len(stripped)
        if indent == 0:
            break
        if job_indent is None:
            job_indent = indent
        if indent < job_indent:
            break
        if indent == job_indent and _REAL_E2E_WORKFLOW_JOB.match(stripped):
            job_indices.append(index)
    if not job_indices:
        return None
    if len(job_indices) != 1 or job_indent is None:
        return ""

    job_index = job_indices[0]
    if not _workflow_job_has_body(lines, job_index, job_indent):
        return ""
    end = len(lines)
    for candidate_index in range(job_index + 1, len(lines)):
        candidate = lines[candidate_index]
        candidate_stripped = candidate.lstrip(" ")
        if not candidate_stripped or candidate_stripped.startswith("#"):
            continue
        candidate_indent = len(candidate) - len(candidate_stripped)
        if candidate_indent <= job_indent:
            end = candidate_index
            break
    job = "\n".join(lines[job_index:end])
    return "" if _REAL_E2E_YAML_INDIRECTION.search(job) is not None else job


def _path_matches_sha256(path: Path, expected: str) -> bool:
    try:
        digest = _normalized_text_sha256(path.read_bytes())
    except OSError:
        return False
    return digest == expected


def _normalized_text_sha256(content: bytes) -> str | None:
    try:
        decoded = content.decode("utf-8")
    except UnicodeDecodeError:
        return None
    normalized = decoded.replace("\r\n", "\n").replace("\r", "\n")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _git_blob_sha256(root: Path, object_id: str) -> str | None:
    try:
        blob = subprocess.run(
            ["git", "cat-file", "blob", object_id],
            cwd=root,
            check=False,
            capture_output=True,
        )
    except OSError:
        return None
    if blob.returncode != 0:
        return None
    return _normalized_text_sha256(blob.stdout)


def _real_e2e_test_directory_is_safe(test_directory: Path) -> bool:
    return all(
        path.relative_to(test_directory).as_posix() == "integrations-real.spec.ts"
        for path in sorted(test_directory.rglob("*"))
        if path.is_file()
    )


def _real_e2e_test_artifact_is_safe(root: Path) -> bool:
    test_path = root / _REAL_E2E_TEST_PATH
    return _frozen_artifact_is_safe(
        root,
        test_path,
        _REAL_E2E_TEST_PATH,
        _EXPECTED_REAL_E2E_TEST_SHA256,
    )


def _frozen_artifact_is_safe(
    root: Path, path: Path, relative_path: str, expected_digest: str
) -> bool:
    if not _frozen_artifact_path_is_safe(root, path):
        return False
    identity = _git_artifact_identity(root, relative_path)
    try:
        working_digest = _normalized_text_sha256(path.read_bytes())
    except OSError:
        return False
    if working_digest != expected_digest:
        return False
    if identity.repository_state is GitRepositoryState.NON_REPOSITORY:
        return True
    if (
        identity.repository_state is not GitRepositoryState.VALID
        or identity.mode not in {"100644", "100755"}
        or identity.object_id is None
    ):
        return False
    blob_digest = _git_blob_sha256(root, identity.object_id)
    return blob_digest == working_digest == expected_digest


def _frozen_artifact_path_is_safe(root: Path, path: Path) -> bool:
    try:
        relative_path = path.relative_to(root)
        resolved_root = root.resolve(strict=True)
    except (OSError, RuntimeError, ValueError):
        return False
    current = root
    for component in relative_path.parts:
        current = current / component
        if current.is_symlink():
            return False
    try:
        path.resolve(strict=True).relative_to(resolved_root)
    except (OSError, RuntimeError, ValueError):
        return False
    return path.is_file()


def _tracked_regular_artifact_is_safe(
    root: Path, path: Path, relative_path: str
) -> bool:
    if not _frozen_artifact_path_is_safe(root, path):
        return False
    identity = _git_artifact_identity(root, relative_path)
    if identity.repository_state is GitRepositoryState.NON_REPOSITORY:
        return True
    if (
        identity.repository_state is not GitRepositoryState.VALID
        or identity.mode not in {"100644", "100755"}
        or identity.object_id is None
    ):
        return False
    try:
        working_digest = _normalized_text_sha256(path.read_bytes())
    except OSError:
        return False
    return _git_blob_sha256(root, identity.object_id) == working_digest


def _git_artifact_identity(root: Path, relative_path: str) -> GitArtifactIdentity:
    repository_state = _git_repository_state(root)
    if repository_state is not GitRepositoryState.VALID:
        return GitArtifactIdentity(repository_state, None, None)
    try:
        indexed = subprocess.run(
            ["git", "ls-files", "--stage", "--", relative_path],
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return GitArtifactIdentity(GitRepositoryState.ERROR, None, None)
    lines = indexed.stdout.splitlines()
    if indexed.returncode != 0:
        return GitArtifactIdentity(GitRepositoryState.ERROR, None, None)
    if len(lines) != 1:
        return GitArtifactIdentity(GitRepositoryState.VALID, "", None)
    fields = lines[0].split(maxsplit=3)
    if (
        len(fields) != 4
        or fields[2] != "0"
        or re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", fields[1]) is None
    ):
        return GitArtifactIdentity(GitRepositoryState.VALID, "", None)
    return GitArtifactIdentity(GitRepositoryState.VALID, fields[0], fields[1])


def _git_repository_state(root: Path) -> GitRepositoryState:
    try:
        repository = subprocess.run(
            ["git", "rev-parse", "--is-inside-work-tree"],
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return GitRepositoryState.ERROR
    if repository.returncode == 0 and repository.stdout.strip() == "true":
        return GitRepositoryState.VALID
    return (
        GitRepositoryState.ERROR
        if _git_metadata_exists_on_ancestor_chain(root) or repository.returncode == 0
        else GitRepositoryState.NON_REPOSITORY
    )


def _git_metadata_exists_on_ancestor_chain(root: Path) -> bool:
    try:
        current = root.resolve(strict=True)
    except (OSError, RuntimeError):
        return True
    for candidate in (current, *current.parents):
        metadata = candidate / ".git"
        if metadata.exists() or metadata.is_symlink():
            return True
    return False


def _workflow_job_has_body(lines: list[str], job_index: int, job_indent: int) -> bool:
    job_line = lines[job_index].lstrip(" ")
    value = job_line.split(":", 1)[1].strip()
    if value and not value.startswith("#"):
        return value.startswith("{")
    for line in lines[job_index + 1 :]:
        stripped = line.lstrip(" ")
        if not stripped or stripped.startswith("#"):
            continue
        return len(line) - len(stripped) > job_indent
    return False


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
        "nexus-engine-backup-restore-drill",
        "npm audit --audit-level=high",
        "pip-audit",
        "docker compose -f infra/compose.yaml --profile app config --quiet",
        "docker compose -f infra/compose.yaml --profile broker config --quiet",
        "docker compose -f infra/compose.yaml --profile provider config --quiet",
        "docker compose -f infra/compose.yaml --profile provider-alpaca config --quiet",
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
            if not path.is_file():
                continue
            content = path.read_bytes().decode("utf-8", errors="ignore")
        except OSError:
            continue
        files[relative] = content
    return files


def run_all(root: Path) -> tuple[list[PolicyFinding], list[DependencyRecord]]:
    inventory = dependency_inventory(root)
    findings = scan_secret_material(repository_texts(root))
    findings.extend(check_forbidden_gateway_surface(root))
    findings.extend(check_provider_tls_remediation(root))
    findings.extend(check_probe_runtime_isolation(root))
    findings.extend(check_gateway_read_allowlist(root))
    findings.extend(check_gateway_entrypoint_isolation(root))
    findings.extend(check_sdk_lock_admission(root))
    findings.extend(check_container_pins(root))
    findings.extend(check_container_hardening(root))
    findings.extend(check_real_e2e_boundary(root))
    findings.extend(check_workflow(root))
    findings.extend(license_findings(inventory, root=root))
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
