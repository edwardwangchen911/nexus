import hashlib
import json
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID

import pytest
from nexus.agents import paper_context
from nexus.agents.portfolio_context import PortfolioResearchContext, _bytes, read_portfolio_context
from pydantic import ValidationError


def _context(projection_hash: str) -> paper_context.AgentPaperResearchContext:
    return paper_context.AgentPaperResearchContext.model_validate(
        {
            "cycle_id": "63000000-0000-0000-0000-000000000001",
            "instrument_id": "63000000-0000-0000-0000-000000000002",
            "decision_time": "2026-09-15T14:10:00Z",
            "decision_quote": {
                "bid": "100",
                "ask": "101",
                "observed_at": "2026-09-15T14:09:59Z",
                "available_at": "2026-09-15T14:10:00Z",
                "received_at": "2026-09-15T14:10:00Z",
            },
            "prior_daily": {"session": "2026-09-14", "close": "99"},
            "paper_portfolio": {
                "candidate_position_weight": "0",
                "cash_weight": "1",
                "projection_hash": projection_hash,
            },
        }
    )


def test_paper_context_must_match_the_projection_used_for_sizing() -> None:
    matches = getattr(
        paper_context,
        "paper_contexts_match_projection",
        lambda _contexts, _projection: True,
    )

    assert not matches((_context("a" * 64),), SimpleNamespace(projection_hash="b" * 64))
    assert matches((_context("a" * 64),), SimpleNamespace(projection_hash="a" * 64))


def test_daily_pending_gap_does_not_change_legacy_bytes() -> None:
    original = _context("a" * 64)
    # Captured from the previous reader, before the optional gap field existed.
    assert hashlib.sha256(paper_context._content(original)).hexdigest() == (
        "dc5ebaee2fda49c94aa24553ca4ce8c00cd5f030496c0f415efc16b4c8c4fe94"
    )
    payload = original.model_dump(mode="json")
    assert "pending_monitoring_gap" not in payload
    restored = paper_context.AgentPaperResearchContext.model_validate(
        {**payload, "pending_monitoring_gap": None}
    )
    assert paper_context._content(restored) == paper_context._content(original)


@pytest.mark.parametrize("fault", [None, "under_capacity", "future", "unknown_reason"])
def test_daily_pending_gap_is_closed_and_bound_to_frozen_cutoff(fault) -> None:
    original = _context("a" * 64)
    gap = {
        "purpose": "daily_monitor_gap_v1",
        "reason": "MONITOR_PENDING_OVERFLOW",
        "observed_at": original.decision_time.isoformat(),
        "pending_count": 33,
        "max_pending": 32,
    }
    if fault == "under_capacity":
        gap["pending_count"] = 32
    elif fault == "future":
        gap["observed_at"] = "2026-09-15T14:10:01Z"
    elif fault == "unknown_reason":
        gap["reason"] = "arbitrary upstream text"
    payload = {**original.model_dump(mode="json"), "pending_monitoring_gap": gap}
    if fault is not None:
        with pytest.raises(ValidationError):
            paper_context.AgentPaperResearchContext.model_validate(payload)
    else:
        restored = paper_context.AgentPaperResearchContext.model_validate(payload)
        assert restored.pending_monitoring is None
        assert restored.pending_monitoring_gap.pending_count == 33


@pytest.mark.asyncio
async def test_paper_snapshot_never_exposes_inherited_real_portfolio_context() -> None:
    at = datetime(2026, 9, 15, 14, 10, tzinfo=UTC)
    instrument_id = UUID("63000000-0000-0000-0000-000000000002")
    draft = PortfolioResearchContext(
        instrument_id=instrument_id,
        portfolio_snapshot_id=UUID(int=3),
        snapshot_as_of=at,
        research_as_of=at,
        currency="USD",
        position_weight="0.25",
        cash_weight="0.75",
        valuation_reason=None,
        snapshot_decision_ready=True,
        diagnostic_run_id=None,
        optimization_need=None,
        diagnostic_input_hash=None,
        diagnostics=(),
        diagnostics_truncated=False,
        evidence_ref_id=UUID(int=4),
        content_hash="0" * 64,
    )
    context = draft.model_copy(update={"content_hash": hashlib.sha256(_bytes(draft)).hexdigest()})
    payload = {
        "portfolio_context": context.model_dump(mode="json"),
        "agent_paper_context": _context("a" * 64).model_dump(mode="json"),
    }
    snapshot = SimpleNamespace(
        source="nexus_research",
        provider_object_id=str(instrument_id),
        available_at=at,
        received_at=at,
        payload=payload,
        revision=hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest(),
    )

    class Session:
        async def get(self, *_args):
            return snapshot

        async def scalar(self, *_args):
            return SimpleNamespace()

    assert await read_portfolio_context(Session(), UUID(int=5), instrument_id, at) is None
