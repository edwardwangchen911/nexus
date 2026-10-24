"""Coverage never treats a new risk or evidence revision as unchanged old support."""

from datetime import UTC, datetime, timedelta

import pytest
from nexus.domain.evidence import EvidenceRef

AT = datetime(2026, 10, 9, 14, tzinfo=UTC)


def evidence(revision="a"):
    return EvidenceRef(
        source="sec",
        object_id="synthetic-filing",
        available_at=AT,
        location="memory://synthetic",
        content_hash=revision * 64,
    )


@pytest.mark.parametrize("case", ["same", "new", "missing", "expired", "audit_failed"])
def test_retention_requires_same_evidence_and_validity(case):
    from nexus.monitoring import reconciliation

    current = (
        (evidence(), evidence("b")) if case == "new" else () if case == "missing" else (evidence(),)
    )
    assert reconciliation.can_retain_plan(
        prior_evidence=(evidence(),),
        current_evidence=current,
        valid_until=AT if case == "expired" else AT + timedelta(hours=1),
        now=AT,
        vetoed=case == "audit_failed",
    ) is (case == "same")


def test_normal_cli_exposes_reconciliation_without_new_request_parameters():
    from nexus.ops.research_monitor import parser

    args = parser().parse_args(["reconcile"])
    assert args.action == "reconcile"
    assert not hasattr(args, "request_id") and not hasattr(args, "trigger_id")


def test_a_withdrawn_plan_cannot_be_retained_by_later_unknown_or_failed_research():
    from nexus.monitoring import reconciliation

    assert not reconciliation.can_retain_plan(
        prior_evidence=(evidence(),),
        current_evidence=(evidence(),),
        valid_until=AT + timedelta(hours=1),
        now=AT,
        vetoed=False,
        previously_active=False,
    )
