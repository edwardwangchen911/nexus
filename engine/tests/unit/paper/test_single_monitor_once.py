"""The once-authorized monitor root cannot masquerade as old research-only owners."""

from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from nexus.ops.manual_paper import parser
from nexus.paper.manual_request import manual_request_hash
from nexus.paper.single_sol_owner import binding_for, research_identity


def row(version=6, mode="research_and_monitor"):
    return SimpleNamespace(
        contract_version=version,
        mode=mode,
        research_profile={
            "single_sol": {
                "instrument_id": str(uuid4()),
                "selection_hash": "a" * 64,
                "budget_day": "2026-11-16",
                "calendar_id": str(uuid4()),
                "deadline": datetime(2026, 11, 16, 14, 30, tzinfo=UTC).isoformat(),
                "history_policy": "single_monitor_original_acceptances_once_v1",
                "preparation_proof": "b" * 64,
                "scheduled_proof": "c" * 64,
                "admission_policy": "shared_budget_v1",
                "protection_context_hash": "d" * 64,
                "authorization_scope": "single_monitor_root_20261116_once_v1",
            }
        },
    )


def test_v6_exact_monitor_binding_and_identity():
    r = row()
    b = binding_for(r)
    assert b.history_policy == "single_monitor_original_acceptances_once_v1"
    assert research_identity(6) == ("local-single-monitor-sol-v1", "single-monitor-sol")
    assert len(b.intent_hash) == 64


@pytest.mark.parametrize(
    "version,mode", [(5, "research_only"), (6, "research_only"), (3, "research_and_monitor")]
)
def test_new_policy_cannot_be_read_as_old_or_wrong_mode(version, mode):
    with pytest.raises(ValueError):
        binding_for(row(version, mode))


@pytest.mark.parametrize(
    "field,value",
    [
        ("authorization_scope", "other"),
        ("history_policy", "single_sol_original_acceptances_v1"),
        ("protection_context_hash", "bad"),
    ],
)
def test_monitor_binding_rejects_changed_scope(field, value):
    r = row()
    r.research_profile["single_sol"][field] = value
    with pytest.raises(ValueError):
        binding_for(r)


def test_explicit_normal_monitor_cli():
    instrument = str(uuid4())
    assert (
        parser()
        .parse_args(["inspect-single-monitor-sol-intent", "--instrument-id", instrument])
        .action
        == "inspect-single-monitor-sol-intent"
    )
    a = parser().parse_args(
        [
            "start-single-monitor-sol",
            "--instrument-id",
            instrument,
            "--idempotency-key",
            "new-root",
            "--expected-intent-hash",
            "a" * 64,
        ]
    )
    assert a.action == "start-single-monitor-sol"


def test_monitor_request_hash_is_distinct_from_three_stock():
    kwargs = dict(
        account_id=uuid4(),
        batch_id=uuid4(),
        mode="research_and_monitor",
        research_profile=row().research_profile,
    )
    assert manual_request_hash(**kwargs, contract_version=6) != manual_request_hash(
        **kwargs, contract_version=3
    )


@pytest.mark.asyncio
async def test_v6_workbench_before_manifest(monkeypatch):
    from unittest.mock import AsyncMock

    from nexus.workbench import research_summary

    identity = uuid4()
    at = datetime(2026, 11, 16, 12, tzinfo=UTC)
    r = SimpleNamespace(
        id=identity, mode="research_and_monitor", contract_version=6, requested_at=at
    )
    view = SimpleNamespace(request_id=identity, research=None)
    monkeypatch.setattr(research_summary, "read_research_manifest", AsyncMock(return_value=None))
    assert await research_summary.research_candidates(None, r, view, read_at=at) == ()
