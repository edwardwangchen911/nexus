"""Durable fixture execution must append the full Agent lifecycle."""

import asyncio
from datetime import UTC, datetime
from uuid import UUID

from nexus.agents.committee import ROLE_ORDER, FixtureCommittee, load_prompt_assets
from nexus.agents.execution import LIFECYCLE
from nexus.agents.models import ResearchRequest
from nexus.domain.evidence import EvidenceRef


def test_execution_lifecycle_is_exact_and_append_only() -> None:
    """Skipping an audit phase would make completed research unauditable."""
    assert LIFECYCLE == (
        "prepared",
        "running",
        "validated",
        "audited",
        "synthesized",
        "completed",
    )


def test_versioned_prompt_assets_drive_all_eight_fixture_role_outputs() -> None:
    """A missing role prompt or ignored prompt hash would make the committee unauditable."""
    prompts = load_prompt_assets()
    assert (
        tuple(prompts)
        == ROLE_ORDER
        == (
            "orchestrator",
            "fundamental",
            "market_state",
            "macro_sector",
            "bull",
            "bear",
            "evidence_audit",
            "portfolio",
        )
    )
    request = ResearchRequest(
        instrument_id=UUID("0198d500-1000-7000-8000-000000000112"),
        data_snapshot_id=UUID("0198d500-1000-7000-8000-000000000113"),
        decision_time=datetime(2026, 8, 24, tzinfo=UTC),
    )
    evidence = (
        EvidenceRef(
            source="fixture",
            object_id="evidence-1",
            available_at=datetime(2026, 8, 23, tzinfo=UTC),
            location="memory:fixture-evidence-1",
            content_hash="a" * 64,
        ),
    )

    async def execute_twice():
        committee = FixtureCommittee()
        results = []
        for _ in range(2):
            results.append(
                [
                    await committee.analyze(role, request, evidence, prompts[role])
                    for role in ROLE_ORDER
                ]
            )
        return tuple(results)

    first, second = asyncio.run(execute_twice())
    assert tuple(output.role for output in first) == ROLE_ORDER
    assert all(output.prompt_hash == prompts[output.role].content_hash for output in first)
    assert [output.model_dump(mode="json") for output in first] == [
        output.model_dump(mode="json") for output in second
    ]
