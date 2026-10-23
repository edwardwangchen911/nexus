"""Sanitized 2026-10-23 frozen outcome shapes, without account facts or identifiers."""

import pytest
from nexus.workbench.research_summary import _limitations

from engine.tests.unit.agents.test_execution import _synthesize


@pytest.mark.parametrize("symbol,unresolved", [("NVDA", 0), ("AVGO", 0), ("AAPL", 2)])
def test_frozen_audit_veto_is_distinct_from_unresolved_reviews(symbol, unresolved):
    intent = _synthesize(
        ("support",) * 6 + ("oppose", "abstain"), actionable=True, assessment_stance="watch"
    )
    assessment = intent.synthesis_assessment
    reviews = tuple(
        r.model_copy(update={"resolved": i >= unresolved}) for i, r in enumerate(assessment.reviews)
    )
    intent = intent.model_copy(
        update={
            "synthesis_assessment": assessment.model_copy(
                update={"reviews": reviews, "review_conclusion": "limited"}
            )
        }
    )
    before = intent.model_dump_json()
    items, _ = _limitations(intent)
    status = next(item for item in items if item.kind == "review_status")
    assert status.source == "system"
    assert "审计票：反对" in status.text
    assert "综合结论：受限" in status.text
    assert f"未解决审查项：{unresolved}" in status.text
    assert not any(
        i.kind == "uncertainties" and i.text == "Evidence review has unresolved objections."
        for i in items
    )
    assert intent.model_dump_json() == before
    assert intent.quality_status == "partial"
    assert intent.exposure_action == "none"
