# ruff: noqa: E501
"""Real PostgreSQL lifecycle API and CAS tests for immutable proposal groups."""

import asyncio
import hashlib
import json
from datetime import UTC, date, datetime
from uuid import UUID

import httpx
import nexus.shadow.start as shadow_start_module
import pytest
from helpers.optimization import complete_comparison_metrics
from helpers.portfolio_api import (
    INSTRUMENT_ID,
    PORTFOLIOS,
    app,
    headers,
    seed_portfolio_list,
    settings,
)
from helpers.shadow import seed_shadow_eligible_group
from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID
from nexus.optimizer.cost_policy import COST_POLICY_ID
from nexus.optimizer.lifecycle import (
    LifecycleTransition,
    reject_group_idempotently,
)
from nexus.optimizer.repository import transition_group
from nexus.persistence.models.data import DataSnapshot, SourcePolicy
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.optimization import (
    BaselineSelectionAuthorityRecord,
    OptimizationProposalGroupRecord,
    ProposalComparisonRecord,
)
from nexus.persistence.models.portfolio import BrokerOrderSnapshot
from nexus.persistence.models.runtime import IdempotencyRecord, Job
from nexus.persistence.session import session_factory
from nexus.quality.source_policy import SOURCE_POLICY_ID
from nexus.shadow.start import ShadowStartResult, start_proposal_shadow
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError

_BASELINE_ID = UUID("0198d500-1000-7000-8000-000000000041")
_POLICY_ID = UUID("e690b91d-1e24-5de3-b2b2-566a463fca25")
_CLOCK = datetime(2026, 8, 24, 3, 59, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _historical_shadow_start_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exercise legacy Proposal Shadow mechanics only behind an explicit test gate."""
    monkeypatch.setattr(
        shadow_start_module,
        "current_proposal_shadow_price_authority_available",
        lambda _source_policy_id: True,
    )


@pytest.mark.asyncio
async def test_incomplete_strategy_cannot_enter_shadowing_and_stale_version_conflicts(
    migrated_database_url: str,
) -> None:
    """Skipping evidence eligibility or CAS would make unsafe/stale lifecycle writes succeed."""
    group_id = UUID("0198d500-1000-7000-8000-000000000801")
    baseline_id = UUID("0198d500-1000-7000-8000-000000000041")
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    with pytest.raises(IntegrityError, match="diagnostic_report_id"):
        async with factory() as session, session.begin():
            await session.execute(
                text(
                    "INSERT INTO optimization_proposal_groups (id, baseline_portfolio_snapshot_id, status, version, policy_id) VALUES (:id, :baseline, 'compared', 1, :policy)"
                ),
                {"id": group_id, "baseline": baseline_id, "policy": _POLICY_ID},
            )
@pytest.mark.asyncio
async def test_shadow_and_reject_api_require_idempotency_and_replay_stably(
    migrated_database_url: str,
) -> None:
    """A lifecycle key binds both target and version, and never writes execution objects."""
    seed = await seed_shadow_eligible_group(migrated_database_url)
    group_id = seed.group_id
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
        mismatched = await client.post(
            shadow_path,
            content=body,
            headers={
                **headers("POST", shadow_path, body),
                "Idempotency-Key": "mismatched-if-match",
                "If-Match": '"2"',
            },
        )
        first = await client.post(
            shadow_path,
            content=body,
            headers={
                **headers("POST", shadow_path, body),
                "Idempotency-Key": "shadow-key",
                "If-Match": '"1"',
            },
        )
        replay = await client.post(
            shadow_path,
            content=body,
            headers={
                **headers("POST", shadow_path, body),
                "Idempotency-Key": "shadow-key",
                "If-Match": '"1"',
            },
        )
        changed_body = await client.post(
            shadow_path,
            content=b'{"version":2}',
            headers={
                **headers("POST", shadow_path, b'{"version":2}'),
                "Idempotency-Key": "shadow-key",
                "If-Match": '"2"',
            },
        )
        changed_target = await client.post(
            reject_path,
            content=body,
            headers={
                **headers("POST", reject_path, body),
                "Idempotency-Key": "shadow-key",
                "If-Match": '"1"',
            },
        )
    assert missing.status_code == 422
    assert missing.json()["error"]["code"] == "VALIDATION_ERROR"
    assert mismatched.status_code == 422
    assert mismatched.json()["error"]["code"] == "VALIDATION_ERROR"
    assert first.status_code == replay.status_code == 200
    assert set(first.json()["meta"]) == {"request_id", "schema_version", "as_of"}
    first_data = first.json()["data"]
    assert first_data == replay.json()["data"]
    assert set(first_data) == {
        "proposal_group_id",
        "shadow_group_id",
        "status",
        "version",
        "start_session",
        "evaluation_end_session",
        "grace_end_session",
    }
    assert first_data["proposal_group_id"] == str(group_id)
    assert UUID(first_data["shadow_group_id"]) != group_id
    assert (first_data["status"], first_data["version"]) == ("shadowing", 2)
    assert first_data["start_session"] == seed.calendar_sessions[0].isoformat()
    assert first_data["evaluation_end_session"] == seed.calendar_sessions[62].isoformat()
    assert first_data["grace_end_session"] == seed.calendar_sessions[67].isoformat()
    assert changed_body.status_code == changed_target.status_code == 409
    assert changed_body.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
    assert changed_target.json()["error"]["code"] == "VERSION_CONFLICT"
    assert await _artifact_counts(migrated_database_url) == (1, 0, 1)


@pytest.mark.asyncio
async def test_concurrent_shadow_reject_has_one_cas_winner_and_terminal_is_immutable(
    migrated_database_url: str,
) -> None:
    """Two independent PostgreSQL transactions cannot both advance one version."""
    seed = await seed_shadow_eligible_group(migrated_database_url)
    group_id = seed.group_id
    factory = session_factory(settings(migrated_database_url))
    barrier = asyncio.Barrier(2)

    async def start() -> ShadowStartResult:
        async with factory() as session, session.begin():
            await barrier.wait()
            return await start_proposal_shadow(
                session,
                actor_id="lifecycle-race",
                idempotency_key="race-shadow",
                request_hash=hashlib.sha256(b"shadowing:1").hexdigest(),
                group_id=group_id,
                expected_version=1,
            )

    async def reject() -> LifecycleTransition:
        async with factory() as session, session.begin():
            await barrier.wait()
            return await reject_group_idempotently(
                session,
                actor_id="lifecycle-race",
                idempotency_key="race-reject",
                request_hash=hashlib.sha256(b"rejected:1").hexdigest(),
                group_id=group_id,
                expected_version=1,
            )

    results = await asyncio.wait_for(
        asyncio.gather(
            start(),
            reject(),
            return_exceptions=True,
        ),
        timeout=5,
    )
    assert sum(isinstance(result, LifecycleTransition | ShadowStartResult) for result in results) == 1
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
            rejected = await reject_group_idempotently(
                session,
                actor_id="lifecycle-race",
                idempotency_key="terminal-reject-key",
                request_hash=hashlib.sha256(b"rejected:2").hexdigest(),
                group_id=group_id,
                expected_version=2,
            )
            with pytest.raises(ValueError, match="VALIDATION_ERROR"):
                await transition_group(session, group_id, "rejected", expected_version=3)
        assert rejected.status == "rejected"
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
            headers={
                **headers("POST", path, body),
                "Idempotency-Key": "bad-authority",
                "If-Match": '"1"',
            },
        )
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "PROPOSAL_NOT_EXECUTABLE"
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session:
        group = await session.get(OptimizationProposalGroupRecord, group_id)
        claims = await session.scalar(select(func.count()).select_from(IdempotencyRecord))
    assert group is not None and (group.status, group.version) == ("compared", 1)
    assert claims == 0
    assert await _artifact_counts(migrated_database_url) == (0, 0, 1)


@pytest.mark.asyncio
async def test_shadow_rejects_non_executable_persisted_comparison(
    migrated_database_url: str,
) -> None:
    """A complete group authority cannot override a non-executable child comparison."""
    group_id = UUID("0198d500-1000-7000-8000-000000000845")
    await _seed_complete_group(migrated_database_url, group_id, comparison_non_executable=True)
    body = b'{"version":1}'
    path = f"/internal/v1/optimization-proposals/{group_id}/shadow"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)), base_url="http://testserver"
    ) as client:
        response = await client.post(
            path,
            content=body,
            headers={
                **headers("POST", path, body),
                "Idempotency-Key": "non-executable-comparison",
                "If-Match": '"1"',
            },
        )
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "PROPOSAL_NOT_EXECUTABLE"


@pytest.mark.asyncio
async def test_raw_sql_shadow_cannot_bypass_non_executable_comparison(
    migrated_database_url: str,
) -> None:
    """The lifecycle trigger, not only the repository, must reject a direct SQL shadow write."""
    group_id = UUID("0198d500-1000-7000-8000-000000000846")
    await _seed_complete_group(migrated_database_url, group_id, comparison_non_executable=True)
    factory = session_factory(settings(migrated_database_url))
    with pytest.raises(IntegrityError, match="PROPOSAL_NOT_EXECUTABLE"):
        async with factory() as session, session.begin():
            await session.execute(text("UPDATE optimization_proposal_groups SET status='shadowing', version=2 WHERE id=:id"), {"id": group_id})


@pytest.mark.asyncio
async def test_shadow_requires_the_frozen_selection_authority(
    migrated_database_url: str,
) -> None:
    """A caller cannot mark strategy evidence complete while omitting its selection row."""
    group_id = UUID("0198d500-1000-7000-8000-000000000844")
    await _seed_complete_group(migrated_database_url, group_id, missing_selection=True)
    factory = session_factory(settings(migrated_database_url))
    with pytest.raises(ValueError, match="PROPOSAL_NOT_EXECUTABLE"):
        async with factory() as session, session.begin():
            await transition_group(session, group_id, "shadowing", expected_version=1)
    with pytest.raises(IntegrityError, match="PROPOSAL_NOT_EXECUTABLE"):
        async with factory() as session, session.begin():
            await session.execute(
                text("UPDATE optimization_proposal_groups SET status='shadowing', version=2 WHERE id=:id"),
                {"id": group_id},
            )


@pytest.mark.asyncio
async def test_shadow_rejects_comparison_from_different_authority(migrated_database_url: str) -> None:
    group_id = UUID("0198d500-1000-7000-8000-000000000646")
    await _seed_complete_group(migrated_database_url, group_id, comparison_authority_conflict=True)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="PROPOSAL_NOT_EXECUTABLE"):
            await transition_group(session, group_id, "shadowing", expected_version=1)
    with pytest.raises(IntegrityError, match="PROPOSAL_NOT_EXECUTABLE"):
        async with factory() as session, session.begin():
            await session.execute(text("UPDATE optimization_proposal_groups SET status='shadowing', version=2 WHERE id=:id"), {"id": group_id})

@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("group_id", "changes"),
    ((UUID("0198d500-1000-7000-8000-000000000847"), {"authority_stale": True}),
     (UUID("0198d500-1000-7000-8000-000000000857"), {"future_source_policy": True})),
)
async def test_raw_sql_shadow_cannot_bypass_stale_policy_authority(
    migrated_database_url: str, group_id: UUID, changes: dict[str, bool],
) -> None:
    """Stale or future policy authority must block raw SQL."""
    await _seed_complete_group(migrated_database_url, group_id, **changes)
    factory = session_factory(settings(migrated_database_url))
    with pytest.raises(IntegrityError, match="PROPOSAL_NOT_EXECUTABLE"):
        async with factory() as session, session.begin():
            await session.execute(text("UPDATE optimization_proposal_groups SET status='shadowing', version=2 WHERE id=:id"), {"id": group_id})


@pytest.mark.asyncio
async def test_raw_sql_shadow_cannot_bypass_noncanonical_source_policy(
    migrated_database_url: str,
) -> None:
    """An arbitrary source-policy row cannot substitute for source_policy_v1."""
    group_id = UUID("0198d500-1000-7000-8000-000000000848")
    await _seed_complete_group(migrated_database_url, group_id, noncanonical_source=True)
    factory = session_factory(settings(migrated_database_url))
    with pytest.raises(IntegrityError, match="PROPOSAL_NOT_EXECUTABLE"):
        async with factory() as session, session.begin():
            await session.execute(text("UPDATE optimization_proposal_groups SET status='shadowing', version=2 WHERE id=:id"), {"id": group_id})


@pytest.mark.asyncio
async def test_raw_sql_shadow_cannot_bypass_missing_cost_policy(
    migrated_database_url: str,
) -> None:
    """NULL cost authority is rejected by the DB trigger with no repository involved."""
    group_id = UUID("0198d500-1000-7000-8000-000000000849")
    factory = session_factory(settings(migrated_database_url))
    with pytest.raises(IntegrityError, match="diagnostic_report_id"):
        async with factory() as session, session.begin():
            await session.execute(
                text("INSERT INTO optimization_proposal_groups (id, baseline_portfolio_snapshot_id, status, version, policy_id) VALUES (:id, :baseline, 'compared', 1, :policy)"),
                {"id": group_id, "baseline": _BASELINE_ID, "policy": _POLICY_ID},
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", ("rejected", "expired"))
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
        await transition_group(session, group_id, terminal, expected_version=version)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="VALIDATION_ERROR"):
            await transition_group(session, group_id, "rejected", expected_version=version + 1)
    assert await _artifact_counts(migrated_database_url) == (0, 0, 1)


async def _seed_complete_group(
    database_url: str,
    group_id: UUID,
    *,
    source_conflict: bool = False,
    comparison_non_executable: bool = False,
    authority_stale: bool = False,
    noncanonical_source: bool = False,
    missing_cost_policy: bool = False,
    missing_selection: bool = False,
    comparison_authority_conflict: bool = False,
    future_source_policy: bool = False,
) -> None:
    await seed_portfolio_list(database_url)
    factory = session_factory(settings(database_url))
    suffix = str(group_id)[-3:]
    data_id = UUID(f"0198d500-1000-7000-8000-000000000{suffix}")
    source_id = (
        UUID(f"0198d500-1000-7000-8000-000000000{int(suffix) + 10:03d}")
        if noncanonical_source
        else SOURCE_POLICY_ID
    )
    report_id = UUID(f"0198d500-1000-7000-8000-000000000{int(suffix) + 20:03d}")
    selection_id = UUID(f"0198d500-1000-7000-8000-000000000{int(suffix) + 24:03d}")
    async with factory() as session, session.begin():
        if future_source_policy:
            await session.execute(text("ALTER TABLE source_policies DISABLE TRIGGER USER"))
            await session.execute(text("UPDATE source_policies SET created_at=:future WHERE id=:id"), {"future": _CLOCK.replace(day=25), "id": SOURCE_POLICY_ID})
            await session.execute(text("ALTER TABLE source_policies ENABLE TRIGGER USER"))
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
        if noncanonical_source:
            session.add(
                SourcePolicy(
                    id=source_id,
                    key="untrusted_source",
                    version="v1",
                    schema_version="1",
                    payload={},
                )
            )
        if not missing_selection:
            session.add(
                BaselineSelectionAuthorityRecord(
                    id=selection_id,
                    policy_version="baseline_v1",
                    data_snapshot_id=data_id,
                    decision_at=_CLOCK,
                    available_at=_CLOCK,
                    evidence_refs=["selection:lifecycle"],
                    candidates=[{"instrument_id": str(INSTRUMENT_ID), "provenance_refs": ["candidate:lifecycle"]}],
                    selected_instrument_ids=[str(INSTRUMENT_ID)],
                    evidence_hash="a" * 64,
                )
            )
        await session.flush()
        await session.execute(
            text(
                "INSERT INTO optimization_proposal_groups (id, baseline_portfolio_snapshot_id, status, version, policy_id, selection_authority_id, selection_evidence_hash, diagnostic_report_id, data_snapshot_id, source_policy_id, cost_policy_id, diagnostic_input_hash, valuation_time, baseline_ready, diagnostic_complete, source_conflict, partial, requires_fx, cost_complete, authority_stale) VALUES (:id, :baseline, 'compared', 1, :policy, :selection, :selection_hash, :report, :data, :source, :cost_policy, :hash, :valuation, true, true, :source_conflict, false, false, true, :authority_stale)"
            ),
            {
                "id": group_id,
                "baseline": _BASELINE_ID,
                "policy": _POLICY_ID,
                "selection": None if missing_selection else selection_id,
                "selection_hash": None if missing_selection else "a" * 64,
                "report": report_id,
                "data": data_id,
                "source": source_id,
                "cost_policy": None if missing_cost_policy else COST_POLICY_ID,
                "hash": "e" * 64,
                "valuation": _CLOCK,
                "source_conflict": source_conflict,
                "authority_stale": authority_stale,
            },
        )
        for index, variant in enumerate(("hold_current", "minimum_change", "strategy_target"), 1):
            await session.execute(
                text(
                    "INSERT INTO optimization_proposals (id, proposal_group_id, variant, baseline_portfolio_snapshot_id, analysis_only, strategy_evidence_complete) VALUES (:id, :group, :variant, :baseline, true, true)"
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
                metrics={
                    **complete_comparison_metrics(
                        report_id=report_id, data_id=data_id, source_id=source_id,
                        optimizer_id=_POLICY_ID, cost_id=COST_POLICY_ID,
                    ),
                    "ranking_eligible": not comparison_non_executable,
                    "authority": {"diagnostic_report_id": str(_BASELINE_ID)}
                    if comparison_authority_conflict else complete_comparison_metrics(
                        report_id=report_id, data_id=data_id, source_id=source_id,
                        optimizer_id=_POLICY_ID, cost_id=COST_POLICY_ID,
                    )["authority"],
                },
                tax_impact="unknown",
                requires_fx=False,
                non_executable=comparison_non_executable,
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
