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
from importlib.metadata import distributions
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

_SDK_PACKAGE = re.compile(r'(?im)^name\s*=\s*["\']fsopenapi["\']|openapi-python-sdk')
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
_REQUIRED_SDK_EVIDENCE = (
    "written_permission",
    "provider_test_vector",
    "security_review",
    "tls_verification_remediation",
)
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
        "fosun_sync_portfolio",
        "fosun_list_orders",
        "fosun_list_cash_flows",
    }
)
_EXPECTED_MARKET_OPERATIONS = frozenset({"fosun_get_quotes", "fosun_get_bars"})
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
    "policy/operations.py": "35c9786b7dfd7a53b5eb7d2f690e4149cc5a1a6925c5b24b9a727cbb69b0b85e",
    "routes/read_only.py": "587e96dc12a06a8f393f071f4761868719130e167feb70d7fd35f0b2d69d5b44",
    "adapter/sdk.py": "1fa0171e306318d0a5c6264fcfedced7e6b0e524cc59ecc26c5cd1704b2a4f74",
    "main.py": "afa5c173c7f3f6de27e36634725cf3ea65b6b22372156ba1bbc2c1e37ed277b0",
}
_EXPECTED_GATEWAY_INSTALLABLE_SRC_SHA256 = (
    "84a5ef06e98d362371ef9ce2afcd209cdf0bae8216fbe62f234729fc946455ad"
)
_EXPECTED_FOSUN_GATEWAY_PYPROJECT_SHA256 = (
    "ac872ba669920971eca0b0c98b7c5cb9b8058d29756c15b044d1518db5221c38"
)
_EXPECTED_COMPOSE_SHA256 = (
    "9a4a462f8eb0588c399427908f88144bc9213023f18764c5a808b00ce698420c"
)
_EXPECTED_FOSUN_GATEWAY_SERVICE_SHA256 = (
    "f22c88598929e7cab2282fab240b0d19791e12a7af4c6ca051df99d7eda9d6f1"
)
_EXPECTED_FOSUN_GATEWAY_DOCKERFILE_SHA256 = (
    "a072e4e4898ea191b807be3f8fdcd7fa4c6c08b4b4e9e4c3ded61897dd37174b"
)


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
        scripts = pyproject["project"]["scripts"]
        build_system = pyproject["build-system"]
        hatch_build = pyproject["tool"]["hatch"]["build"]
    except (KeyError, OSError, tomllib.TOMLDecodeError, TypeError):
        pyproject_text = ""
        scripts = None
        build_system = None
        hatch_build = None
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
        or build_system
        != {"requires": ["hatchling"], "build-backend": "hatchling.build"}
        or hatch_build
        != {"targets": {"wheel": {"packages": ["src/nexus_fosun_gateway"]}}}
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

    pending = ["nexus_fosun_gateway.main"]
    visited: set[str] = set()
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
                if candidate in module_paths and candidate not in visited:
                    pending.append(candidate)
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
    if not _valid_provider_base_url(admission.get("provider_base_url")):
        return False
    license_name = sdk.get("license")
    if not isinstance(license_name, str) or license_name.upper() in {"", "UNRESOLVED"}:
        return False
    evidence = admission.get("evidence")
    return isinstance(evidence, dict) and all(
        isinstance(evidence.get(name), str) and bool(evidence[name].strip())
        for name in _REQUIRED_SDK_EVIDENCE
    )


def _valid_provider_base_url(value: object) -> bool:
    if (
        not isinstance(value, str)
        or value != value.strip()
        or value != value.rstrip("/")
    ):
        return False
    parsed = urlsplit(value)
    try:
        parsed.port
    except ValueError:
        return False
    return (
        parsed.scheme.lower() == "https"
        and bool(parsed.hostname)
        and parsed.username is None
        and parsed.password is None
        and not parsed.query
        and not parsed.fragment
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
