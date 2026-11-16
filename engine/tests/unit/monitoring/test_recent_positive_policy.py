"""The one-plan positive route never proves absence or replaces C1."""

from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.monitoring import recent_positive_policy as policy
from nexus.ops.research_monitor import parser
from unit.research.test_sec_forward_comparison import indexes


def test_current_recent_uses_all_c1_accessions_and_rejects_visible_changes():
    _, _, old, first = indexes()
    original = first.entries[-1]
    current = old.model_copy(update={"entries": (original,)})
    assert policy.seen_additions(first, current) == ()
    new = old.entries[0].model_copy(update={"accession_number": "0000000042-26-000999"})
    current = current.model_copy(update={"entries": (new,), "history_complete": False})
    assert policy.seen_additions(first, current) == (new,)
    changed = original.model_copy(update={"form": "8-K/A" if original.form == "8-K" else "8-K"})
    with pytest.raises(ValueError, match="SEC_INDEX_EVENT_CONFLICT"):
        policy.seen_additions(first, current.model_copy(update={"entries": (new, changed)}))
    with pytest.raises(ValueError, match="SEC_INDEX_IDENTITY_INVALID"):
        policy.seen_additions(first, current.model_copy(update={"issuer_cik": "0000000099"}))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode,expected",
    [
        ("new", "true"),
        ("empty", "unknown"),
        ("stale", "unknown"),
        ("future", "unknown"),
        ("conflict", "unknown"),
    ],
)
async def test_observation_only_emits_true_partial_or_unknown(monkeypatch, mode, expected):
    _, _, old, first = indexes()
    now = old.received_at
    plan = SimpleNamespace(id=UUID(int=8), instrument_id=first.instrument_id)
    selected = SimpleNamespace(id=UUID(int=71), revision="a" * 64)
    opt = SimpleNamespace(issuer_cik=first.issuer_cik, effective_at=now)
    basis = SimpleNamespace(
        baseline_snapshot_id=UUID(int=72),
        baseline_hash="b" * 64,
        baseline_as_of=now - timedelta(days=1),
        capture_id=UUID(int=73),
        capture_hash="c" * 64,
        effective_at=now - timedelta(hours=1),
    )
    new = old.entries[0].model_copy(update={"accession_number": "0000000042-26-000999"})
    entries = (new,) if mode != "empty" else ()
    if mode == "conflict":
        known = first.entries[0]
        entries += (known.model_copy(update={"form": "8-K/A" if known.form == "8-K" else "8-K"}),)
    age = 301 if mode == "stale" else -1 if mode == "future" else 0
    current = old.model_copy(
        update={"entries": entries, "received_at": now - timedelta(seconds=age)}
    )
    row = SimpleNamespace(id=UUID(int=74), revision="d" * 64, available_at=now)
    monkeypatch.setattr(policy, "read_policy", AsyncMock(return_value=(selected, opt)))
    monkeypatch.setattr(
        policy,
        "read_origin",
        AsyncMock(return_value=(SimpleNamespace(id=UUID(int=75)), basis, old, first)),
    )
    monkeypatch.setattr(policy, "_capture", AsyncMock(return_value=(row, current)))
    monkeypatch.setattr(policy, "all_c1_accessions", AsyncMock(return_value=set()))
    monkeypatch.setattr(policy, "configured_exhibit_store", lambda _: object())
    result = await policy.read_recent_observation(
        SimpleNamespace(scalar=AsyncMock(return_value=row)), plan=plan, now=now
    )
    assert result.result == expected and result.history_coverage == "partial"
    assert result.result != "false"
    assert bool(result.added) == (expected == "true")


def test_normal_cli_is_explicit_plan_only():
    parsed = parser().parse_args(["enable-recent-positive-once", "--plan-id", str(UUID(int=8))])
    assert parsed.plan_id == UUID(int=8)


@pytest.mark.asyncio
async def test_non_v6_original_root_is_not_authorized():
    with pytest.raises(ValueError, match="PLAN_NOT_AUTHORIZED"):
        await policy._root_request(
            SimpleNamespace(scalar=AsyncMock(return_value=None)),
            SimpleNamespace(
                source_run_id=UUID(int=1), instrument_id=UUID(int=2), account_id=UUID(int=3)
            ),
        )


def policy_fixture(now):
    return policy.RecentPositivePolicy(
        plan_id=UUID(int=1),
        root_request_id=UUID(int=2),
        own_origin_id=UUID(int=3),
        own_origin_hash="a" * 64,
        account_id=UUID(int=4),
        instrument_id=UUID(int=5),
        source_run_id=UUID(int=6),
        source_hash="b" * 64,
        condition_hash="c" * 64,
        root_origin_id=UUID(int=7),
        root_origin_hash="d" * 64,
        comparison_snapshot_id=UUID(int=8),
        comparison_hash="e" * 64,
        issuer_cik="0000000042",
        effective_at=now,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode", ["valid", "own_changed", "root_changed", "missing_audit", "time_changed"]
)
async def test_policy_read_requires_own_root_capture_and_original_audit(monkeypatch, mode):
    from hashlib import sha256

    now = indexes()[2].received_at
    opt = policy_fixture(now)
    plan = SimpleNamespace(
        id=opt.plan_id,
        account_id=opt.account_id,
        instrument_id=opt.instrument_id,
        source_run_id=opt.source_run_id,
        source_hash=opt.source_hash,
        condition_hash=opt.condition_hash,
        effective_at=now - timedelta(hours=1),
        valid_until=now + timedelta(days=1),
    )
    row = SimpleNamespace(
        payload=opt.model_dump(mode="json"),
        schema_version=policy.POLICY,
        revision=sha256(opt.canonical()).hexdigest(),
        id=UUID(int=10),
        created_at=now,
        observed_at=now,
        available_at=now,
        received_at=now,
    )
    own = SimpleNamespace(id=opt.own_origin_id, revision=opt.own_origin_hash)
    root = SimpleNamespace(id=opt.root_origin_id, revision=opt.root_origin_hash)
    first = SimpleNamespace(capture_id=opt.comparison_snapshot_id, capture_hash=opt.comparison_hash)
    if mode == "own_changed":
        own.revision = "f" * 64
    if mode == "root_changed":
        root.id = UUID(int=20)
    if mode == "time_changed":
        row.available_at = now + timedelta(seconds=1)
    monkeypatch.setattr(policy, "read_binding", AsyncMock(return_value=(own, root, plan, first)))
    monkeypatch.setattr(policy, "_root_request", AsyncMock(return_value=opt.root_request_id))
    monkeypatch.setattr(policy, "_issuer", AsyncMock(return_value=opt.issuer_cik))
    session = SimpleNamespace(
        scalars=AsyncMock(return_value=[row]),
        scalar=AsyncMock(return_value=None if mode == "missing_audit" else UUID(int=11)),
    )
    if mode == "valid":
        assert await policy.read_policy(session, plan, now) == (row, opt)
    else:
        with pytest.raises(ValueError, match=policy.INVALID):
            await policy.read_policy(session, plan, now)


def test_policy_fields_and_positive_observation_fail_closed():
    from pydantic import ValidationError

    opt = policy_fixture(indexes()[2].received_at)
    for field, value in [
        ("source_hash", "bad"),
        ("issuer_cik", "42"),
        ("effective_at", opt.effective_at.replace(tzinfo=None)),
    ]:
        document = {**opt.model_dump(), field: value}
        with pytest.raises(ValidationError):
            policy.RecentPositivePolicy.model_validate(document)
