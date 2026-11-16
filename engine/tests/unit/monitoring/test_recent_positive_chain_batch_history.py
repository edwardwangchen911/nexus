"""The real typed-batch consumer preserves each chain key at the saved check time."""

from datetime import timedelta
from hashlib import sha256
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from unit.monitoring.test_filing_content import source

from nexus.monitoring import (
    batch_inputs,
    disclosure_observations,
    source_qualification,
)
from nexus.monitoring.filing_archive import FilingArchive, VerifiedFilingContent
from nexus.monitoring.recent_positive_chain import ChainPositiveObservation


async def saved_chain_batch(monkeypatch):
    entry, _, _ = source()
    checked = entry.accepted_at
    prepared = checked + timedelta(seconds=3)

    def uid(n):
        return UUID(int=n)

    observation = ChainPositiveObservation(
        instrument_id=uid(1),
        plan_id=uid(2),
        baseline_snapshot_id=uid(3),
        baseline_hash="a" * 64,
        baseline_as_of=checked - timedelta(days=1),
        issuer_cik="0000123456",
        current_snapshot_id=uid(4),
        current_hash="b" * 64,
        received_at=checked,
        available_at=checked,
        origin_id=uid(5),
        comparison_snapshot_id=uid(6),
        comparison_hash="c" * 64,
        comparison_from=checked - timedelta(hours=1),
        policy_snapshot_id=uid(7),
        policy_hash="d" * 64,
        scope_snapshot_id=uid(8),
        scope_hash="e" * 64,
        result="true",
        reason=None,
        added=(entry,),
    )
    document = observation.model_dump(mode="json", exclude={"added"})
    document["entry"] = entry.model_dump(mode="json")
    plan = SimpleNamespace(
        id=uid(2),
        kind="disclosure",
        generation=2,
        source_run_id=uid(10),
        source_output_id=uid(11),
        source_hash="f" * 64,
        effective_at=checked - timedelta(seconds=1),
        valid_until=checked + timedelta(days=8),
    )
    check = SimpleNamespace(
        id=uid(12),
        plan_id=plan.id,
        result="true",
        created_at=checked,
        checked_at=checked,
        observation=document,
    )
    batch = SimpleNamespace(
        id=uid(13),
        account_id=uid(14),
        instrument_id=uid(1),
        created_at=checked,
        sealed_at=checked,
        checked_at=checked,
        trigger_ids=[str(uid(15))],
    )
    trigger = SimpleNamespace(
        id=uid(15),
        batch_id=batch.id,
        check_id=check.id,
        account_id=batch.account_id,
        instrument_id=batch.instrument_id,
        kind="disclosure",
        event_key=f"sec:0000123456:{entry.accession_number}",
        cause="disclosure_added",
        created_at=checked,
    )
    archive = FilingArchive(
        instrument_id=batch.instrument_id,
        issuer_cik="0000123456",
        index_snapshot_id=uid(4),
        index_hash="b" * 64,
        entry=entry,
        originals={},
        documents=(),
    )
    content = VerifiedFilingContent(
        snapshot_id=uid(16), evidence_ref_id=uid(17), canonical_hash="1" * 64, archive=archive
    )
    qualified = SimpleNamespace(
        context=object(),
        run=SimpleNamespace(id=plan.source_run_id),
        output=SimpleNamespace(id=plan.source_output_id, content_hash=plan.source_hash),
        intent=SimpleNamespace(model_dump=lambda **_: {"stance": "watch"}),
        role_outputs=(),
    )
    monkeypatch.setattr(
        source_qualification, "load_qualified_source", AsyncMock(return_value=qualified)
    )
    monkeypatch.setattr(batch_inputs, "read_filing_content", AsyncMock(return_value=content))

    async def read_saved_check(session, *, plan, now):
        # Passing the current revoked/expired time here would invalidate the saved
        # observation. The normal batch consumer must use its original check time.
        assert now == checked
        return observation

    monkeypatch.setattr(disclosure_observations, "read_disclosure_observation", read_saved_check)
    objects = {plan.id: plan, check.id: check, batch.id: batch, trigger.id: trigger}

    async def get(model, identifier):
        return objects.get(identifier)

    session = SimpleNamespace(
        get=get,
        scalar=AsyncMock(return_value="on"),
        execute=AsyncMock(),
        scalars=AsyncMock(return_value=(SimpleNamespace(id=content.snapshot_id),)),
    )
    saved = await batch_inputs._inputs(session, batch, prepared, current_eligibility=False)
    row = SimpleNamespace(
        source=batch_inputs.SOURCE,
        schema_version=batch_inputs.SCHEMA,
        created_at=prepared,
        observed_at=prepared,
        available_at=prepared,
        received_at=prepared,
        revision=sha256(saved.canonical()).hexdigest(),
        provider_object_id=str(batch.id),
        payload=saved.model_dump(mode="json"),
    )
    objects[uid(18)] = row
    return session, uid(18), saved, check, checked


@pytest.mark.asyncio
@pytest.mark.parametrize("later", [timedelta(seconds=4), timedelta(days=7, microseconds=1)])
async def test_typed_chain_batch_rereads_original_check_across_revoke_and_expiry(
    monkeypatch, later
):
    session, snapshot_id, saved, _, checked = await saved_chain_batch(monkeypatch)
    reread = await batch_inputs.read_batch_inputs(
        session, snapshot_id=snapshot_id, as_of=checked + later
    )
    assert reread == saved
    observation = reread.members[0]["observation"]
    assert observation["scope_snapshot_id"] == str(UUID(int=8))
    assert observation["scope_hash"] == "e" * 64
    assert observation["policy_snapshot_id"] == str(UUID(int=7))
    assert observation["policy_hash"] == "d" * 64


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field", ["scope_snapshot_id", "scope_hash", "policy_snapshot_id", "policy_hash"]
)
async def test_typed_chain_batch_rejects_any_changed_scope_or_member_key(monkeypatch, field):
    session, snapshot_id, _, check, checked = await saved_chain_batch(monkeypatch)
    check.observation[field] = str(UUID(int=99)) if field.endswith("id") else "9" * 64
    assert (
        await batch_inputs.read_batch_inputs(
            session, snapshot_id=snapshot_id, as_of=checked + timedelta(days=7, microseconds=1)
        )
        is None
    )
