"""Typed diagnostic item constructors with explicit evidence attachment."""

from decimal import Decimal

from nexus.diagnostics.models import (
    DiagnosticActionability,
    DiagnosticItem,
    DiagnosticSeverity,
    MarketEvidence,
)
from nexus.domain.decimal import decimal_to_string
from nexus.domain.evidence import EvidenceRef


def upper_threshold_item(
    code: str,
    value: Decimal,
    warning_limit: Decimal,
    critical_limit: Decimal,
    scope: str,
    market: MarketEvidence,
    *,
    unit: str = "ratio",
    inclusive: bool = False,
    evidence_refs: tuple[EvidenceRef, ...] | None = None,
) -> DiagnosticItem:
    if value >= critical_limit if inclusive else value > critical_limit:
        return item(
            code,
            value,
            unit,
            critical_limit,
            scope,
            "critical",
            "reduce_only",
            None,
            market,
            evidence_refs=evidence_refs,
        )
    if value >= warning_limit if inclusive else value > warning_limit:
        return item(
            code,
            value,
            unit,
            warning_limit,
            scope,
            "warning",
            "research",
            None,
            market,
            evidence_refs=evidence_refs,
        )
    return item(
        code,
        value,
        unit,
        warning_limit,
        scope,
        "info",
        "monitor",
        None,
        market,
        evidence_refs=evidence_refs,
    )


def threshold_item(
    code: str,
    value: Decimal,
    unit: str,
    reference_limit: Decimal,
    scope: str,
    market: MarketEvidence,
    *,
    warning: bool,
    evidence_refs: tuple[EvidenceRef, ...] | None = None,
) -> DiagnosticItem:
    severity: DiagnosticSeverity = "warning" if warning else "info"
    actionability: DiagnosticActionability = "research" if warning else "monitor"
    return item(
        code,
        value,
        unit,
        reference_limit,
        scope,
        severity,
        actionability,
        None,
        market,
        evidence_refs=evidence_refs,
    )


def unknown_item(
    code: str, unit: str, reason: str, scope: str, market: MarketEvidence
) -> DiagnosticItem:
    return DiagnosticItem(
        diagnostic_code=code,
        metric_value=None,
        unit=unit,
        reference_limit=None,
        policy_scope=scope,
        severity="info",
        actionability="research",
        unknown_reason=reason,
        evidence_refs=(),
    )


def item(
    code: str,
    value: Decimal,
    unit: str,
    reference_limit: Decimal | None,
    scope: str,
    severity: DiagnosticSeverity,
    actionability: DiagnosticActionability,
    reason: str | None,
    market: MarketEvidence,
    *,
    evidence_refs: tuple[EvidenceRef, ...] | None = None,
) -> DiagnosticItem:
    return DiagnosticItem(
        diagnostic_code=code,
        metric_value=decimal_to_string(value),
        unit=unit,
        reference_limit=None if reference_limit is None else decimal_to_string(reference_limit),
        policy_scope=scope,
        severity=severity,
        actionability=actionability,
        unknown_reason=reason,
        evidence_refs=market.evidence_refs if evidence_refs is None else evidence_refs,
    )
