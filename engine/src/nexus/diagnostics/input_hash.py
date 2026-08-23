"""Canonical binding for one authoritative diagnostic computation."""

import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from decimal import Decimal
from hashlib import sha256
from uuid import UUID

from nexus.diagnostics.assembler import DiagnosticAssembly
from nexus.diagnostics.models import DiagnosticHistory, PortfolioDiagnosticReport
from nexus.diagnostics.policy import (
    REAL_ADVISORY_POLICY_ID,
    REAL_ADVISORY_POLICY_PAYLOAD,
)
from nexus.providers.broker.base import PortfolioSnapshot


def diagnostic_input_hash(
    *,
    snapshot: PortfolioSnapshot,
    assembly: DiagnosticAssembly,
    history: DiagnosticHistory,
    policy_version: str,
) -> str:
    """Hash canonical facts without transport locations or credentials."""
    value = {
        "snapshot_id": str(snapshot.id),
        "snapshot_as_of": _canonical(snapshot.as_of),
        "snapshot_content": assembly.canonical_inputs["snapshot"],
        "evidence": assembly.canonical_inputs,
        "policy": {
            "id": str(REAL_ADVISORY_POLICY_ID),
            "version": policy_version,
            "payload": REAL_ADVISORY_POLICY_PAYLOAD,
        },
        "history": tuple(_report_input(report) for report in history.prior_reports),
        "history_authority": tuple(
            {
                "source": ref.source,
                "object_id": ref.object_id,
                "available_at": _canonical(ref.available_at),
                "content_hash": ref.content_hash,
            }
            for ref in history.authority_refs
        ),
        "comparison": (),
    }
    encoded = json.dumps(
        _canonical(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode()
    return sha256(encoded).hexdigest()


def _report_input(report: PortfolioDiagnosticReport) -> dict[str, object]:
    return {
        "snapshot_id": str(report.portfolio_snapshot_id),
        "snapshot_as_of": _canonical(report.snapshot_as_of),
        "trading_day": _canonical(report.trading_day),
        "policy_version": report.policy_version,
        "optimization_need": report.optimization_need,
        "items": tuple(
            {
                "code": item.diagnostic_code,
                "value": item.metric_value,
                "limit": item.reference_limit,
                "unit": item.unit,
                "scope": item.policy_scope,
                "severity": item.severity,
                "actionability": item.actionability,
                "unknown": item.unknown_reason,
                "refs": tuple(
                    {
                        "source": ref.source,
                        "object_id": ref.object_id,
                        "available_at": _canonical(ref.available_at),
                        "content_hash": ref.content_hash,
                    }
                    for ref in item.evidence_refs
                ),
            }
            for item in report.items
        ),
    }


def _canonical(value: object) -> object:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal | UUID):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _canonical(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return tuple(_canonical(item) for item in value)
    if value is None or isinstance(value, str | int | bool):
        return value
    raise TypeError(f"unsupported canonical diagnostic value: {type(value).__name__}")
