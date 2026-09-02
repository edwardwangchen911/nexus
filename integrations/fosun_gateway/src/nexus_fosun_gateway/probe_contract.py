"""Code-owned, value-free contract digests for the closed Fosun probe."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal

ProbeScope = Literal["core", "extended"]
RuntimeOperation = Literal[
    "fosun_sync_accounts",
    "fosun_sync_portfolio:cash",
    "fosun_sync_portfolio:holdings",
    "fosun_list_orders",
    "fosun_list_cash_flows",
    "fosun_get_quotes",
    "fosun_get_bars",
]
ProbeLabel = Literal[
    "Session",
    "Accounts",
    "CashSummary",
    "Holdings",
    "OrderList",
    "CashFlows",
    "quote",
    "kline_minimal",
]


@dataclass(frozen=True)
class ScopeDescriptor:
    scope: ProbeScope
    scope_contract_version: str
    physical_operation_set: tuple[ProbeLabel, ...]
    prerequisite_operation_set: tuple[ProbeLabel, ...]
    authorized_operation_set: tuple[RuntimeOperation, ...]


SCOPE_DESCRIPTORS: Mapping[ProbeScope, ScopeDescriptor] = MappingProxyType(
    {
        "core": ScopeDescriptor(
            "core",
            "nexus.fosun.read-only-scope.v1.core",
            ("Session", "Accounts", "CashSummary", "Holdings", "quote", "kline_minimal"),
            ("Session", "Accounts"),
            (
                "fosun_sync_accounts",
                "fosun_sync_portfolio:cash",
                "fosun_sync_portfolio:holdings",
                "fosun_get_quotes",
                "fosun_get_bars",
            ),
        ),
        "extended": ScopeDescriptor(
            "extended",
            "nexus.fosun.read-only-scope.v1.extended",
            ("Session", "Accounts", "OrderList", "CashFlows"),
            ("Session", "Accounts"),
            ("fosun_list_orders", "fosun_list_cash_flows"),
        ),
    }
)


def scope_descriptor(scope: ProbeScope) -> ScopeDescriptor:
    return SCOPE_DESCRIPTORS[scope]


REQUIRED_PROBE_OPERATIONS = (
    "Session",
    "Accounts",
    "CashSummary",
    "Holdings",
    "OrderList",
    "CashFlows",
    "quote",
    "kline_minimal",
)
BROKER_PROBE_OPERATIONS = REQUIRED_PROBE_OPERATIONS[:6]
MAX_BROKER_REQUESTS = 6
PROBE_CLI_ARGUMENT_NAMES = (
    "--manifest",
    "--run-id",
    "--scope",
    "--symbol",
    "--session-date",
    "--limit",
    "--capability-ttl-seconds",
    "--output-basename",
)
PROBE_RESULT_FIELD_NAMES = (
    "status",
    "code",
    "failure_stage",
    "failure_operation",
    "failure_category",
    "physical_request_count",
    "run_id",
    "scope",
    "scope_contract_version",
    "artifact_sha256",
    "byte_length",
    "capability_expires_at",
    "activation_contract_hash",
    "publication_receipt",
)
PROBE_FAILURE_OPERATIONS = ("unknown", *REQUIRED_PROBE_OPERATIONS)
PROBE_FAILURE_CATEGORIES = (
    "auth",
    "permission",
    "provider",
    "transport",
    "rate_limit",
    "budget",
    "contract",
    "unknown",
)

_CONTRACT_SCHEMA_VERSION = "nexus.fosun.probe-response-contract.v1"
_PAGE_FIELDS: tuple[tuple[str, str], ...] = (
    ("items", "tuple[expected-item]"),
    ("source_request_id", "nonempty-string-discarded-after-validation"),
    ("has_more", "boolean"),
    ("next_cursor", "null-or-nonempty-string"),
    ("as_of", "aware-utc-datetime"),
    ("quality", "PASS-or-PARTIAL"),
)
_ITEM_FIELDS: Mapping[str, tuple[tuple[str, str], ...]] = MappingProxyType(
    {
        "Session": (
            ("http_status", "integer-2xx"),
            ("response_body", "bounded-and-discarded"),
        ),
        "Accounts": (
            ("kind", "literal-account"),
            ("external_account_id", "nonempty-string-in-memory-only"),
            ("status", "string"),
            ("base_currency", "null-or-string"),
        ),
        "CashSummary": (
            ("kind", "literal-cash"),
            ("external_account_id", "nonempty-string-in-memory-only"),
            ("currency", "string"),
            ("cash_balance", "finite-decimal"),
            ("available_cash", "finite-decimal"),
            ("provider_as_of", "null-or-aware-utc-datetime"),
        ),
        "Holdings": (
            ("kind", "literal-position"),
            ("external_account_id", "nonempty-string-in-memory-only"),
            ("instrument_code", "string"),
            ("currency", "string"),
            ("quantity", "finite-decimal"),
            ("available_quantity", "finite-decimal"),
            ("frozen_quantity", "finite-decimal"),
            ("average_cost", "null-or-finite-decimal"),
            ("diluted_cost", "null-or-finite-decimal"),
            ("current_price", "null-or-finite-decimal"),
            ("provider_as_of", "null-or-aware-utc-datetime"),
        ),
        "OrderList": (
            ("kind", "literal-order"),
            ("external_order_id", "nonempty-string"),
            ("instrument_code", "string"),
            ("side", "buy-or-sell"),
            ("quantity", "finite-decimal"),
            ("filled_quantity", "finite-decimal"),
            ("price", "null-or-finite-decimal"),
            ("status", "string"),
            ("submitted_at", "aware-utc-datetime"),
            ("updated_at", "null-or-aware-utc-datetime"),
        ),
        "CashFlows": (
            ("kind", "literal-cash-flow"),
            ("external_cash_flow_id", "nonempty-string"),
            ("currency", "string"),
            ("amount", "finite-decimal"),
            ("occurred_at", "null-or-aware-utc-datetime"),
            ("provider_business_date", "null-or-iso-date"),
            ("direction", "null-or-inflow-1-or-outflow-2"),
            ("flow_type", "string"),
        ),
        "quote": (
            ("kind", "literal-quote"),
            ("instrument_code", "requested-symbol"),
            ("price", "null-or-finite-decimal"),
            ("as_of", "aware-utc-datetime"),
        ),
        "kline_minimal": (
            ("kind", "literal-bar"),
            ("instrument_code", "requested-symbol"),
            ("interval", "literal-day"),
            ("timestamp", "requested-provider-day-utc"),
            ("open", "positive-finite-decimal"),
            ("high", "positive-finite-decimal"),
            ("low", "positive-finite-decimal"),
            ("close", "positive-finite-decimal"),
            ("volume", "nonnegative-finite-decimal"),
        ),
    }
)


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _contract_material(label: str) -> dict[str, object]:
    item_fields = _ITEM_FIELDS[label]
    return {
        "contract_schema_version": _CONTRACT_SCHEMA_VERSION,
        "item_fields": item_fields,
        "logical_label": label,
        "page_fields": (
            (("authentication", "bounded-physical-http-result"),)
            if label == "Session"
            else _PAGE_FIELDS
        ),
    }


_RESPONSE_CONTRACT_SHA256: Mapping[str, str] = MappingProxyType(
    {
        label: hashlib.sha256(_canonical_json(_contract_material(label))).hexdigest()
        for label in REQUIRED_PROBE_OPERATIONS
    }
)


def probe_response_contract_sha256(label: str) -> str | None:
    """Return the static digest for a closed operation, or ``None`` if unknown."""
    return _RESPONSE_CONTRACT_SHA256.get(label)
