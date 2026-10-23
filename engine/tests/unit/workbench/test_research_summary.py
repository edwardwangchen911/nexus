"""Bounded saved limitations preserve provenance and never invent completion."""

import pytest
from nexus.strategy.intents import ResearchIntent
from nexus.workbench.research_summary import _limitations

from engine.tests.unit.agents.test_execution import _synthesize


def limited_intent():
    intent = _synthesize(
        ("support",) * 6 + ("oppose", "abstain"), actionable=True, assessment_stance="watch"
    )
    doc = intent.model_dump(mode="json")
    review = doc["synthesis_assessment"]["reviews"][0]
    review.update(
        status="unresolved",
        resolved=False,
        missing_evidence=["历史 EPS 口径未确认"],
        prevented_conclusion="不能确定该估值分母",
    )
    doc["synthesis_assessment"]["review_conclusion"] = "limited"
    doc["synthesis_assessment"]["uncertainties"] = ["未来业绩存在不确定性"]
    return ResearchIntent.model_validate(doc)


def test_model_gaps_and_uncertainty_have_distinct_provenance():
    intent = limited_intent()
    items, more = _limitations(intent)
    assert items
    assert not more
    assert all(item.source == "model" for item in items)
    assert any(item.kind == "missing_evidence" and item.impact for item in items)
    assert any(item.kind == "uncertainties" and item.impact is None for item in items)
    assert not any("已解决" in item.text for item in items)


def test_saved_text_is_bounded_and_explicitly_truncated():
    intent = _synthesize(
        ("support",) * 6 + ("oppose", "abstain"), actionable=True, assessment_stance="watch"
    )
    review = intent.synthesis_assessment.reviews[0].model_copy(
        update={"missing_evidence": ("x" * 1100,)}
    )
    intent = intent.model_copy(
        update={
            "synthesis_assessment": intent.synthesis_assessment.model_copy(
                update={"reviews": (review,)}
            )
        }
    )
    items, _ = _limitations(intent)
    assert items[0].text == "x" * 1000
    assert items[0].truncated is True


def test_limitation_count_is_bounded_without_claiming_a_total():
    intent = limited_intent()
    reviews = tuple(
        intent.synthesis_assessment.reviews[0].model_copy(
            update={
                "claim_id": f"bear:{index}",
                "missing_evidence": tuple(f"gap {index}:{j}" for j in range(4)),
            }
        )
        for index in range(4)
    )
    intent = intent.model_copy(
        update={
            "synthesis_assessment": intent.synthesis_assessment.model_copy(
                update={
                    "reviews": reviews,
                    "uncertainties": tuple(f"uncertain {index}" for index in range(5)),
                }
            )
        }
    )
    items, more = _limitations(intent)
    assert len(items) == 20
    assert more is True
    assert items[0].text == "gap 0:0"


@pytest.mark.asyncio
async def test_missing_run_never_looks_up_instrument_latest():
    from datetime import UTC, datetime
    from uuid import UUID

    from nexus.workbench.research_summary import _project

    result = await _project(
        None,
        instrument_id=UUID(int=1),
        symbol="AAPL",
        run=None,
        jobs=(),
        reader=None,
        read_at=datetime.now(UTC),
    )
    assert result.run_id is None
    assert result.href is None
    assert result.reason_code == "RESEARCH_RUN_UNAVAILABLE"
    assert result.limitations == ()


@pytest.mark.asyncio
async def test_cycle_without_a_valid_event_chain_is_not_verified():
    from datetime import UTC, datetime
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from uuid import UUID

    from nexus.paper.agent_cycle_models import candidate_manifest_hash
    from nexus.persistence.models.paper import PAPER_DECISION_SOURCE_POLICY_ID
    from nexus.workbench.research_summary import cycle_candidates

    at = datetime.now(UTC)
    cycle = SimpleNamespace(
        id=UUID(int=1),
        candidate_manifest=[],
        candidate_hash=candidate_manifest_hash(()),
        created_at=at,
        decision_price_policy_id=PAPER_DECISION_SOURCE_POLICY_ID,
    )
    session = SimpleNamespace(scalars=AsyncMock(return_value=[]))
    with pytest.raises(ValueError, match="WORKBENCH_CANDIDATE_BINDING_INVALID"):
        await cycle_candidates(session, cycle, read_at=at)


def test_impact_excerpt_and_standalone_conclusion_preserve_model_provenance():
    intent = limited_intent()
    review = intent.synthesis_assessment.reviews[0].model_copy(
        update={"prevented_conclusion": "y" * 1100}
    )
    intent = intent.model_copy(
        update={
            "synthesis_assessment": intent.synthesis_assessment.model_copy(
                update={"reviews": (review,)}
            )
        }
    )
    items, _ = _limitations(intent)
    assert items[0].impact == "y" * 1000
    assert items[0].truncated
    review = review.model_copy(update={"missing_evidence": ()})
    intent = intent.model_copy(
        update={
            "synthesis_assessment": intent.synthesis_assessment.model_copy(
                update={"reviews": (review,)}
            )
        }
    )
    items, _ = _limitations(intent)
    assert items[0].kind == "prevented_conclusion"
    assert items[0].source == "model"
    assert items[0].text == "y" * 1000
    assert items[0].truncated


@pytest.mark.asyncio
@pytest.mark.parametrize("cancelled", [False, True])
async def test_verified_job_running_or_cancelled_is_projected(cancelled):
    from datetime import UTC, datetime
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from uuid import UUID

    from nexus.workbench.research_summary import _project, canonical_hash

    at = datetime.now(UTC)
    run = SimpleNamespace(id=UUID(int=1), instrument_id=UUID(int=2),
                          data_snapshot_id=UUID(int=3), decision_time=at,
                          status="abstained" if cancelled else "prepared")
    job = SimpleNamespace(payload={"run_id": str(run.id)}, scope_id=str(run.instrument_id),
                          effective_at=at, job_type="agent.paper.research", created_at=at,
                          started_at=at, finished_at=at if cancelled else None,
                          status="skipped" if cancelled else "running")
    content = {"status": "abstained", "role": None,
               "payload": {"reason": "manual_unused_research_cancelled"}, "intent": None}
    output = SimpleNamespace(**content, created_at=at, content_hash=canonical_hash(content))
    session = SimpleNamespace(scalars=AsyncMock(return_value=[output]))
    result = await _project(session, instrument_id=run.instrument_id, symbol="AAPL",
                            run=run, jobs=(job,), reader=None, read_at=at)
    assert result.read_status == "verified"
    assert result.job_status == ("skipped" if cancelled else "running")
    assert result.run_status == ("abstained" if cancelled else "prepared")
    if cancelled:
        assert result.reason_code == "manual_unused_research_cancelled"
        assert len(result.limitations) == 1
        assert result.limitations[0].source == "system"
    else:
        session.scalars.assert_not_awaited()
