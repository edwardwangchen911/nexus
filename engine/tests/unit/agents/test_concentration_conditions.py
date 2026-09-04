"""Concentration conditions are exact frozen observations, not trading permissions."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.agents.portfolio_context import _concentration_metric
from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID, REAL_ADVISORY_POLICY_VERSION
from nexus.diagnostics.repository import _stored_decimal
from nexus.persistence.models.diagnostics import (
    PortfolioDiagnosticItemRecord,
    PortfolioDiagnosticReportRecord,
)
from nexus.persistence.models.portfolio import PositionSnapshot


@pytest.mark.parametrize(
    "ratio,severity,limit",
    [
        ("0.15", "info", "0.15"),
        ("0.1500000000001", "warning", "0.15"),
        ("0.30", "warning", "0.15"),
        ("0.3000000000001", "critical", "0.30"),
    ],
)
def test_metric_retains_unrounded_ratio_including_verified_info(ratio, severity, limit):
    held = PositionSnapshot(
        instrument_id=UUID(int=1), currency="USD", quantity=Decimal(1), current_price=Decimal(ratio)
    )
    peers = [
        PositionSnapshot(
            instrument_id=UUID(int=i),
            currency="USD",
            quantity=Decimal(1),
            current_price=(1 - Decimal(ratio)) / 10,
        )
        for i in range(2, 12)
    ]
    item = PortfolioDiagnosticItemRecord(
        diagnostic_code="CONCENTRATION_TOP1",
        policy_scope="currency:USD",
        metric_value=_stored_decimal(ratio),
        reference_limit=Decimal(limit),
        unit="ratio",
        severity=severity,
        unknown_reason=None,
    )
    report = PortfolioDiagnosticReportRecord(
        policy_id=REAL_ADVISORY_POLICY_ID,
        policy_version=REAL_ADVISORY_POLICY_VERSION,
    )
    result = _concentration_metric(held, [held, *peers], [item], report)
    assert result is not None
    assert result[0] == Decimal(ratio)
    assert result[1] is item


def test_frozen_concentration_fields_are_optional_and_absent_from_legacy_bytes():
    from nexus.agents.portfolio_context import PortfolioResearchContext, _bytes

    context = PortfolioResearchContext(
        instrument_id=UUID(int=1),
        portfolio_snapshot_id=UUID(int=2),
        snapshot_as_of=datetime(2026, 9, 1, tzinfo=UTC),
        research_as_of=datetime(2026, 9, 2, tzinfo=UTC),
        currency="USD",
        position_weight=None,
        cash_weight=None,
        valuation_reason="INCOMPLETE_CURRENCY_VALUATION",
        snapshot_decision_ready=False,
        diagnostic_run_id=None,
        optimization_need=None,
        diagnostic_input_hash=None,
        diagnostics=(),
        diagnostics_truncated=False,
    )
    assert context.concentration_facts is None
    assert b"concentration_facts" not in _bytes(context)


def frozen_context(ratio="0.3000000000001"):
    from nexus.agents.concentration import FrozenConcentrationFacts
    from nexus.agents.portfolio_context import PortfolioResearchContext
    from nexus.domain.evidence import EvidenceRef

    at = datetime(2026, 8, 24, tzinfo=UTC)
    return PortfolioResearchContext(
        instrument_id=UUID("0198d500-1000-7000-8000-000000000112"),
        portfolio_snapshot_id=UUID(int=2),
        snapshot_as_of=at,
        research_as_of=at,
        currency="USD",
        position_weight="0.1",
        cash_weight="0.9",
        valuation_reason=None,
        snapshot_decision_ready=True,
        diagnostic_run_id=UUID(int=3),
        optimization_need="RESEARCH_REQUIRED",
        diagnostic_input_hash="a" * 64,
        diagnostics=(),
        diagnostics_truncated=False,
        evidence_ref_id=UUID(int=4),
        content_hash="b" * 64,
        concentration_facts=FrozenConcentrationFacts(
            observed_ratio=ratio,
            policy_id=REAL_ADVISORY_POLICY_ID,
            policy_scope="currency:USD",
            policy_evidence_refs=(
                EvidenceRef(
                    source="nexus_db",
                    object_id=f"diagnostic-policy:{REAL_ADVISORY_POLICY_ID}:{REAL_ADVISORY_POLICY_VERSION}",
                    location=f"db://diagnostic-policies/{REAL_ADVISORY_POLICY_ID}",
                    available_at=at,
                    content_hash="c" * 64,
                ),
            ),
        ),
    )


@pytest.mark.parametrize(
    "ratio,expected",
    [
        ("0.15", ("false", "false")),
        ("0.1500000000001", ("true", "false")),
        ("0.30", ("true", "false")),
        ("0.3000000000001", ("true", "true")),
    ],
)
def test_only_two_frozen_signals_are_evaluated_at_unrounded_boundaries(ratio, expected):
    from nexus.agents.concentration import evaluate_frozen_concentration_conditions

    context = frozen_context(ratio)
    results = evaluate_frozen_concentration_conditions(context)
    assert tuple(row.result for row in results) == expected
    assert tuple(row.condition.signal for row in results) == (
        "portfolio.top1_warning_exceeded",
        "portfolio.top1_critical_exceeded",
    )
    assert all(row.purpose == "research_observation" and row.reason is None for row in results)
    assert all(row.observed_at == context.snapshot_as_of for row in results)
    assert all(
        row.condition.evidence_refs[0].content_hash == context.content_hash for row in results
    )


def test_missing_facts_are_unknown_and_missing_context_has_no_invented_citation():
    from nexus.agents.concentration import evaluate_frozen_concentration_conditions

    old = frozen_context().model_copy(update={"concentration_facts": None})
    assert all(row.result == "unknown" for row in evaluate_frozen_concentration_conditions(old))
    assert evaluate_frozen_concentration_conditions(None) is None


def test_synthesis_uses_exact_context_without_changing_stance_or_legacy_conditions():
    from .test_execution import _synthesize

    legacy = _synthesize(("support",) * 8, structured=True)
    assert "condition_evaluations" not in legacy.model_dump(mode="json")
    result = _synthesize(("support",) * 8, structured=True, portfolio_context=frozen_context())
    assert [row.result for row in result.condition_evaluations] == ["true", "true"]
    assert result.quality_status == "partial" and result.evidence_audit == "partial"
    assert result.stance == legacy.stance and result.exposure_action == legacy.exposure_action
    assert result.invalidation_conditions == legacy.invalidation_conditions
    assert result.entry_conditions == legacy.entry_conditions
    assert result.exit_conditions == legacy.exit_conditions


@pytest.mark.parametrize("defect", ["deleted", "result", "refs", "clock"])
def test_existing_frozen_facts_require_exact_output_evaluations(defect):
    from nexus.agents.concentration import condition_evaluations_match

    from .test_execution import _synthesize

    context = frozen_context()
    intent = _synthesize(("support",) * 8, structured=True, portfolio_context=context)
    assert condition_evaluations_match(intent, context)
    first, second = intent.condition_evaluations
    if defect == "deleted":
        broken = None
    else:
        update = {
            "result": {"result": "false"},
            "refs": {
                "condition": first.condition.model_copy(
                    update={
                        "evidence_refs": (intent.evidence_refs[0],),
                    }
                )
            },
            "clock": {"observed_at": datetime(2026, 8, 23, tzinfo=UTC)},
        }[defect]
        broken = (first.model_copy(update=update), second)
    assert not condition_evaluations_match(
        intent.model_copy(update={"condition_evaluations": broken}), context
    )
