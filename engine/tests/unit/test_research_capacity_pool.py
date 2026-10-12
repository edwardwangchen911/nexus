"""Cycle-derived capacity qualification cannot accept a hand-picked candidate subset."""

from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from nexus.paper.agent_cycle_models import (
    CandidateManifestEntry,
    candidate_manifest_hash,
    canonical_cycle_event_hash,
)


def case():
    cycle_id = uuid4()
    now = datetime(2026, 10, 12, 14, tzinfo=UTC)
    candidates = []
    runs = []
    for symbol in ("AAA", "BBB"):
        candidate = CandidateManifestEntry(
            instrument_id=uuid4(),
            symbol=symbol,
            inclusion_reason="synthetic",
            research_snapshot_id=uuid4(),
            prior_daily_price_authority_id=uuid4(),
            decision_quote_id=uuid4(),
            decision_trade_id=None,
            decision_artifact_id=uuid4(),
            decision_evidence_hash="a" * 64,
            risk_query_authority_ids=(uuid4(), uuid4(), uuid4()),
        )
        candidates.append(candidate)
        runs.append(
            SimpleNamespace(
                id=uuid4(),
                instrument_id=candidate.instrument_id,
                data_snapshot_id=uuid4(),
                decision_time=now,
            )
        )
    cycle = SimpleNamespace(
        id=cycle_id,
        execution_locked=True,
        decision_time=now,
        candidate_manifest=[r.model_dump(mode="json") for r in candidates],
        candidate_hash=candidate_manifest_hash(tuple(candidates)),
    )
    event = SimpleNamespace(
        cycle_id=cycle_id,
        sequence=1,
        from_status=None,
        to_status="prepared",
        result_code="SYNTHETIC",
        fact_ids={"research_run_ids": [str(r.id) for r in runs]},
        occurred_at=now,
    )
    event.event_hash = canonical_cycle_event_hash(**vars(event))
    return cycle, (event,), tuple(runs)


@pytest.mark.parametrize(
    "damage", [None, "candidate_hash", "event_hash", "omitted_run", "duplicate_run", "wrong_time"]
)
def test_cycle_members_require_complete_bound_pool_and_preserve_augmented_snapshots(damage):
    from nexus.ops.research_capacity_pool import derive_cycle_members

    cycle, events, runs = case()
    if damage == "candidate_hash":
        cycle.candidate_hash = "f" * 64
    elif damage == "event_hash":
        events[0].event_hash = "f" * 64
    elif damage == "omitted_run":
        runs = runs[:1]
    elif damage == "duplicate_run":
        runs = (runs[0], runs[0])
    elif damage == "wrong_time":
        runs[0].decision_time = datetime(2026, 10, 13, tzinfo=UTC)
    if damage:
        with pytest.raises(ValueError, match="^WORKLOAD_RUN_AUTHORITY_INVALID$"):
            derive_cycle_members(cycle, events, runs)
    else:
        paired = derive_cycle_members(cycle, events, runs)
        assert len(paired) == 2
        assert paired[0][1].data_snapshot_id != paired[0][0].research_snapshot_id


def test_normal_cli_selects_cycle_qualification_without_inspecting_single_run(monkeypatch, capsys):
    import sys
    from unittest.mock import AsyncMock

    from nexus.ops import research_workload_inspection as inspection

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "inspection",
            "--repo-root",
            ".",
            "--capacity-cycle-id",
            "00000000-0000-0000-0000-000000000001",
        ],
    )
    monkeypatch.setattr(inspection, "local_settings", lambda *a, **k: object())
    read = AsyncMock(
        return_value={"status": "capacity_observed", "registered": False, "model_calls": 0}
    )
    monkeypatch.setattr(inspection, "inspect_capacity_cycle", read, raising=False)
    assert inspection.main() == 0
    read.assert_awaited_once()
    assert not __import__("json").loads(capsys.readouterr().out)["registered"]


@pytest.mark.asyncio
async def test_cycle_entry_rejects_valid_context_bound_to_another_cycle(monkeypatch):
    from unittest.mock import AsyncMock

    from nexus.ops import research_capacity_pool as pool
    from nexus.ops import research_workload_inspection as inspection
    from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
    from nexus.persistence.models.paper import AgentPaperCycleRecord

    cycle, events, runs = case()
    model_id = uuid4()
    for run in runs:
        run.model_config_id = model_id
        run.model_hash = "b" * 64
        run.pricing_version = "codex-sdk-chatgpt-v8-stage-capacity"

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        def begin(self):
            return self

        async def execute(self, command):
            assert str(command) == "SET TRANSACTION READ ONLY"

        async def get(self, kind, key):
            if kind is AgentPaperCycleRecord:
                return cycle
            if kind is AgentResearchRunRecord:
                return next(r for r in runs if r.id == key)
            if kind is AgentModelConfigRecord:
                return SimpleNamespace(config_hash="b" * 64, version=runs[0].pricing_version)
            raise AssertionError("UNEXPECTED_READ")

    class Factory:
        kw = {"bind": SimpleNamespace(dispose=AsyncMock())}

        def __call__(self):
            return Session()

    monkeypatch.setattr(pool, "session_factory", lambda settings: Factory())
    monkeypatch.setattr(pool, "_events", AsyncMock(return_value=events))
    monkeypatch.setattr(pool, "load_frozen_evidence", AsyncMock(return_value=()))
    monkeypatch.setattr(
        pool,
        "revalidate_frozen_agent_paper_context",
        AsyncMock(return_value=pool.FrozenAgentPaperContextStatus.VALID),
    )
    monkeypatch.setattr(
        pool,
        "read_agent_paper_research_context",
        AsyncMock(return_value=(SimpleNamespace(cycle_id=uuid4()), None)),
        raising=False,
    )
    compiler = AsyncMock(side_effect=AssertionError("UNEXPECTED_COMPILER_ACCESS"))
    monkeypatch.setattr(inspection, "_inspection_committee", compiler)
    with pytest.raises(ValueError, match="^WORKLOAD_RUN_AUTHORITY_INVALID$"):
        await pool.inspect_cycle(object(), cycle_id=cycle.id)
    compiler.assert_not_awaited()
