# ruff: noqa: E501
"""Real PostgreSQL lifecycle API and CAS tests for immutable proposal groups."""

import asyncio
import hashlib
import json
from datetime import UTC, date, datetime
from uuid import UUID

import httpx
import pytest
from helpers.portfolio_api import PORTFOLIOS, app, headers, seed_portfolio_list, settings
from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID
from nexus.optimizer.lifecycle import LifecycleTransition, transition_group_idempotently
from nexus.optimizer.repository import transition_group
from nexus.persistence.models.data import DataSnapshot, SourcePolicy
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.optimization import (
    OptimizationProposalGroupRecord,
    ProposalComparisonRecord,
)
from nexus.persistence.models.portfolio import BrokerOrderSnapshot
from nexus.persistence.models.runtime import IdempotencyRecord, Job
from nexus.persistence.session import session_factory
from sqlalchemy import func, select, text

_BASELINE_ID = UUID("0198d500-1000-7000-8000-000000000041")
_POLICY_ID = UUID("e690b91d-1e24-5de3-b2b2-566a463fca25")
_CLOCK = datetime(2026, 8, 24, 3, 59, tzinfo=UTC)


@pytest.mark.asyncio
async def test_incomplete_strategy_cannot_enter_shadowing_and_stale_version_conflicts(
    migrated_database_url: str,
) -> None:
    """Skipping evidence eligibility or CAS would make unsafe/stale lifecycle writes succeed."""
    group_id = UUID("0198d500-1000-7000-8000-000000000801")
    baseline_id = UUID("0198d500-1000-7000-8000-000000000041")
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        await session.execute(
            text(
                "INSERT INTO optimization_proposal_groups (id, baseline_portfolio_snapshot_id, status, version, policy_id) VALUES (:id, :baseline, 'compared', 1, 'e690b91d-1e24-5de3-b2b2-566a463fca25')"
            ),
            {"id": group_id, "baseline": baseline_id},
        )
        for index, variant in enumerate(("hold_current", "minimum_change", "strategy_target"), 1):
            await session.execute(
                text(
                    "INSERT INTO optimization_proposals (id, proposal_group_id, variant, baseline_portfolio_snapshot_id, analysis_only, strategy_evidence_complete) VALUES (:id, :group, :variant, :baseline, true, false)"
                ),
                {
                    "id": UUID(f"0198d500-1000-7000-8000-00000000080{index + 1}"),
                    "group": group_id,
                    "variant": variant,
                    "baseline": baseline_id,
                },
            )
        session.add_all(
            ProposalComparisonRecord(
                proposal_id=UUID(f"0198d500-1000-7000-8000-00000000080{index + 1}"),
                metrics={},
                tax_impact="unknown",
                requires_fx=False,
                non_executable=True,
                cost_complete=False,
            )
            for index in range(1, 4)
        )
    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="PROPOSAL_NOT_EXECUTABLE"):
            await transition_group(session, group_id, "shadowing", expected_version=1)
        with pytest.raises(ValueError, match="VERSION_CONFLICT"):
            await transition_group(session, group_id, "rejected", expected_version=0)


@pytest.mark.asyncio
async def test_shadow_and_reject_api_require_idempotency_and_replay_stably(
    migrated_database_url: str,
) -> None:
    """A lifecycle key binds both target and version, and never writes execution objects."""
    group_id = UUID("0198d500-1000-7000-8000-000000000821")
    await _seed_complete_group(migrated_database_url, group_id)
    body = b'{"version":1}'
    shadow_path = f"/internal/v1/optimization-proposals/{group_id}/shadow"
    reject_path = f"/internal/v1/optimization-proposals/{group_id}/reject"
    engine_app = app(migrated_database_url)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=engine_app), base_url="http://testserver"
    ) as client:
        missing = await client.post(
            shadow_path, content=body, headers=headers("POST", shadow_path, body)
        )
        first = await client.post(
            shadow_path,
            content=body,
            headers={**headers("POST", shadow_path, body), "Idempotency-Key": "shadow-key"},
        )
        replay = await client.post(
            shadow_path,
            content=body,
            headers={**headers("POST", shadow_path, body), "Idempotency-Key": "shadow-key"},
        )
        changed_body = await client.post(
            shadow_path,
            content=b'{"version":2}',
            headers={
                **headers("POST", shadow_path, b'{"version":2}'),
                "Idempotency-Key": "shadow-key",
            },
        )
        changed_target = await client.post(
            reject_path,
            content=body,
            headers={**headers("POST", reject_path, body), "Idempotency-Key": "shadow-key"},
        )
    assert missing.status_code == 422
    assert missing.json()["error"]["code"] == "VALIDATION_ERROR"
    assert first.status_code == replay.status_code == 200
    assert (
        first.json()["data"]
        == replay.json()["data"]
        == {
            "proposal_group_id": str(group_id),
            "status": "shadowing",
            "version": 2,
        }
    )
    assert changed_body.status_code == changed_target.status_code == 409
    assert changed_body.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
    assert changed_target.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
    assert await _artifact_counts(migrated_database_url) == (1, 0, 1)


@pytest.mark.asyncio
async def test_concurrent_shadow_reject_has_one_cas_winner_and_terminal_is_immutable(
    migrated_database_url: str,
) -> None:
    """Two independent PostgreSQL transactions cannot both advance one version."""
    group_id = UUID("0198d500-1000-7000-8000-000000000831")
    await _seed_complete_group(migrated_database_url, group_id)
    factory = session_factory(settings(migrated_database_url))
    barrier = asyncio.Barrier(2)

    async def transition(target: str, key: str) -> LifecycleTransition:
        async with factory() as session, session.begin():
            await barrier.wait()
            return await transition_group_idempotently(
                session,
                actor_id="lifecycle-race",
                idempotency_key=key,
                request_hash=hashlib.sha256(f"{target}:1".encode()).hexdigest(),
                group_id=group_id,
                target=target,  # type: ignore[arg-type]
                expected_version=1,
            )

    results = await asyncio.wait_for(
        asyncio.gather(
            transition("shadowing", "race-shadow"),
            transition("rejected", "race-reject"),
            return_exceptions=True,
        ),
        timeout=5,
    )
    assert sum(isinstance(result, LifecycleTransition) for result in results) == 1
    assert (
        sum(
            isinstance(result, ValueError) and str(result) == "VERSION_CONFLICT"
            for result in results
        )
        == 1
    )
    async with factory() as session:
        group = await session.get(OptimizationProposalGroupRecord, group_id)
    assert group is not None
    assert (group.status, group.version) in {("shadowing", 2), ("rejected", 2)}
    if group.status == "shadowing":
        async with factory() as session, session.begin():
            validated = await transition_group_idempotently(
                session,
                actor_id="lifecycle-race",
                idempotency_key="validate-key",
                request_hash=hashlib.sha256(b"validated:2").hexdigest(),
                group_id=group_id,
                target="validated",
                expected_version=2,
            )
            with pytest.raises(ValueError, match="VALIDATION_ERROR"):
                await transition_group(session, group_id, "rejected", expected_version=3)
        assert validated.status == "validated"
    else:
        async with factory() as session, session.begin():
            with pytest.raises(ValueError, match="VALIDATION_ERROR"):
                await transition_group(session, group_id, "expired", expected_version=2)
    claims, jobs, order_snapshots = await _artifact_counts(migrated_database_url)
    assert claims in {1, 2}
    assert (jobs, order_snapshots) == (0, 1)


@pytest.mark.asyncio
async def test_shadow_incomplete_persisted_authority_rolls_back_its_claim(
    migrated_database_url: str,
) -> None:
    """A rejected owner must leave neither a lifecycle row nor a half-advanced group."""
    group_id = UUID("0198d500-1000-7000-8000-000000000841")
    await _seed_complete_group(migrated_database_url, group_id, source_conflict=True)
    body = json.dumps({"version": 1}, separators=(",", ":")).encode()
    path = f"/internal/v1/optimization-proposals/{group_id}/shadow"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)), base_url="http://testserver"
    ) as client:
        response = await client.post(
            path,
            content=body,
            headers={**headers("POST", path, body), "Idempotency-Key": "bad-authority"},
        )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "PROPOSAL_NOT_EXECUTABLE"
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session:
        group = await session.get(OptimizationProposalGroupRecord, group_id)
        claims = await session.scalar(select(func.count()).select_from(IdempotencyRecord))
    assert group is not None and (group.status, group.version) == ("compared", 1)
    assert claims == 0
    assert await _artifact_counts(migrated_database_url) == (0, 0, 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", ("validated", "rejected", "expired"))
async def test_every_terminal_lifecycle_state_is_immutable(
    migrated_database_url: str, terminal: str
) -> None:
    """Validated, rejected, and expired groups have no legal follow-up transition."""
    group_id = UUID(
        f"0198d500-1000-7000-8000-000000000{850 + (('validated', 'rejected', 'expired').index(terminal)):03d}"
    )
    await _seed_complete_group(migrated_database_url, group_id)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        version = 1
        if terminal == "validated":
            await transition_group(session, group_id, "shadowing", expected_version=version)
            version += 1
        await transition_group(session, group_id, terminal, expected_version=version)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="VALIDATION_ERROR"):
            await transition_group(session, group_id, "rejected", expected_version=version + 1)
    assert await _artifact_counts(migrated_database_url) == (0, 0, 1)


async def _seed_complete_group(
    database_url: str, group_id: UUID, *, source_conflict: bool = False
) -> None:
    await seed_portfolio_list(database_url)
    factory = session_factory(settings(database_url))
    suffix = str(group_id)[-3:]
    data_id = UUID(f"0198d500-1000-7000-8000-000000000{suffix}")
    source_id = UUID(f"0198d500-1000-7000-8000-000000000{int(suffix) + 10:03d}")
    report_id = UUID(f"0198d500-1000-7000-8000-000000000{int(suffix) + 20:03d}")
    async with factory() as session, session.begin():
        session.add_all(
            (
                DataSnapshot(
                    id=data_id,
                    source="fixture",
                    provider_object_id=f"lifecycle-{suffix}",
                    revision="1",
                    observed_at=_CLOCK,
                    available_at=_CLOCK,
                    received_at=_CLOCK,
                    schema_version="1",
                    payload={},
                ),
                SourcePolicy(
                    id=source_id,
                    key=f"source-{suffix}",
                    version="v1",
                    schema_version="1",
                    payload={},
                ),
                PortfolioDiagnosticReportRecord(
                    id=report_id,
                    portfolio_id=PORTFOLIOS[0],
                    portfolio_snapshot_id=_BASELINE_ID,
                    snapshot_as_of=_CLOCK,
                    trading_day=date(2026, 8, 24),
                    policy_id=REAL_ADVISORY_POLICY_ID,
                    policy_version="real_advisory_policy_v1",
                    optimization_need="OPTIMIZATION_CANDIDATE",
                    diagnostic_input_hash="e" * 64,
                ),
            )
        )
        await session.flush()
        await session.execute(
            text(
                "INSERT INTO optimization_proposal_groups (id, baseline_portfolio_snapshot_id, status, version, policy_id, diagnostic_report_id, data_snapshot_id, source_policy_id, cost_model_version, diagnostic_input_hash, valuation_time, baseline_ready, diagnostic_complete, source_conflict, partial, requires_fx, cost_complete) VALUES (:id, :baseline, 'compared', 1, :policy, :report, :data, :source, 'cost_model_v1', :hash, :valuation, true, true, :source_conflict, false, false, true)"
            ),
            {
                "id": group_id,
                "baseline": _BASELINE_ID,
                "policy": _POLICY_ID,
                "report": report_id,
                "data": data_id,
                "source": source_id,
                "hash": "e" * 64,
                "valuation": _CLOCK,
                "source_conflict": source_conflict,
            },
        )
        for index, variant in enumerate(("hold_current", "minimum_change", "strategy_target"), 1):
            await session.execute(
                text(
                    "INSERT INTO optimization_proposals (id, proposal_group_id, variant, baseline_portfolio_snapshot_id, analysis_only, strategy_evidence_complete) VALUES (:id, :group, :variant, :baseline, false, true)"
                ),
                {
                    "id": UUID(f"0198d500-1000-7000-8000-000000000{int(suffix) + 30 + index:03d}"),
                    "group": group_id,
                    "variant": variant,
                    "baseline": _BASELINE_ID,
                },
            )
        session.add_all(
            ProposalComparisonRecord(
                proposal_id=UUID(
                    f"0198d500-1000-7000-8000-000000000{int(suffix) + 30 + index:03d}"
                ),
                metrics={},
                tax_impact="unknown",
                requires_fx=False,
                non_executable=True,
                cost_complete=True,
            )
            for index in range(1, 4)
        )


async def _artifact_counts(database_url: str) -> tuple[int, int, int]:
    factory = session_factory(settings(database_url))
    async with factory() as session:
        return (
            await session.scalar(select(func.count()).select_from(IdempotencyRecord)) or 0,
            await session.scalar(select(func.count()).select_from(Job)) or 0,
            await session.scalar(select(func.count()).select_from(BrokerOrderSnapshot)) or 0,
        )
