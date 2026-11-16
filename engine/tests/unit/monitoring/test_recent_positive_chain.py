"""Seven-day scope keeps exact ancestry, historical time and true-only semantics."""

from datetime import UTC, datetime, timedelta
from hashlib import sha256
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.monitoring import recent_positive_chain as chain
from nexus.monitoring import recent_positive_policy as old
from nexus.ops.research_monitor import parser
from pydantic import ValidationError
from unit.monitoring.test_recent_positive_policy import policy_fixture

NOW = datetime(2026, 11, 16, 10, tzinfo=UTC)


def scope_value():
    previous = policy_fixture(NOW - timedelta(hours=1))
    return chain.PositiveChainScope(
        **previous.model_dump(exclude={"policy", "authorization_scope", "effective_at"}),
        effective_at=NOW,
        expires_at=NOW + timedelta(days=7),
        anchor_policy_snapshot_id=UUID(int=90),
        anchor_policy_hash="e" * 64,
    )


def plan_value(**changes):
    scope = scope_value()
    values = dict(
        id=scope.plan_id,
        account_id=scope.account_id,
        instrument_id=scope.instrument_id,
        kind="disclosure",
        condition_hash=scope.condition_hash,
        created_at=NOW - timedelta(hours=2),
        effective_at=NOW - timedelta(hours=2),
        generation=1,
        valid_until=NOW + timedelta(days=1),
        predecessor_id=None,
    )
    return SimpleNamespace(**{**values, **changes})


@pytest.mark.parametrize("schema", [chain.POLICY, chain.MEMBER_SCHEMA, chain.REVOKE_SCHEMA])
def test_new_schema_fits_database_limit(schema):
    assert len(schema) <= 32


def test_scope_is_new_immutable_wire_and_old_wire_is_unchanged():
    value = scope_value()
    before = policy_fixture(NOW)
    assert value.expires_at - value.effective_at == timedelta(days=7)
    assert value.policy != before.policy
    assert old.RecentPositivePolicy.model_validate_json(before.canonical()) == before
    with pytest.raises(ValidationError):
        value.expires_at = NOW + timedelta(days=8)
    with pytest.raises(ValidationError):
        chain.PositiveChainScope.model_validate({**value.model_dump(), "extra": True})


@pytest.mark.parametrize(
    "action",
    ["enable-positive-chain-scope", "ensure-positive-chain-member", "revoke-positive-chain-scope"],
)
def test_cli_requires_explicit_plan_anchor(action):
    assert parser().parse_args([action, "--plan-id", str(UUID(int=1))]).plan_id == UUID(int=1)
    with pytest.raises(SystemExit):
        parser().parse_args([action])


@pytest.mark.asyncio
async def test_exact_anchor_ancestry_not_merely_same_c1():
    anchor = plan_value()
    unrelated = plan_value(id=UUID(int=222))
    successor = plan_value(id=UUID(int=223), predecessor_id=anchor.id, generation=2)
    session = SimpleNamespace(get=AsyncMock(return_value=anchor))
    assert await chain._descends(session, successor, scope_value(), NOW)
    assert not await chain._descends(session, unrelated, scope_value(), NOW)
    assert session.get.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["cycle", "gap", "generation", "future"])
async def test_ancestry_fails_closed(mode):
    anchor = plan_value()
    plan = plan_value(id=UUID(int=223), predecessor_id=anchor.id, generation=2)
    prior = anchor
    if mode == "cycle":
        plan.predecessor_id = plan.id
        prior = plan
    if mode == "gap":
        prior = None
    if mode == "generation":
        prior.generation = 8
    if mode == "future":
        plan.created_at = NOW + timedelta(microseconds=1)
    with pytest.raises(ValueError, match=chain.INVALID):
        await chain._descends(
            SimpleNamespace(get=AsyncMock(return_value=prior)), plan, scope_value(), NOW
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "offset,expected", [(-1, False), (0, True), (604799, True), (604800, False)]
)
async def test_scope_fixed_window_uses_checked_at(monkeypatch, offset, expected):
    monkeypatch.setattr(chain, "_visible", AsyncMock(return_value=()))
    assert (
        await chain._active(
            object(),
            SimpleNamespace(id=UUID(int=81)),
            scope_value(),
            NOW + timedelta(seconds=offset),
        )
        == expected
    )


@pytest.mark.asyncio
async def test_revocation_is_historical_and_hash_bound(monkeypatch):
    scope = scope_value()
    row = SimpleNamespace(id=UUID(int=81), revision="a" * 64)
    revoke = chain.ChainRevocation(
        scope_snapshot_id=row.id,
        scope_hash=row.revision,
        account_id=scope.account_id,
        instrument_id=scope.instrument_id,
        effective_at=NOW + timedelta(seconds=1),
    )
    rev_row = SimpleNamespace(payload=revoke.model_dump(mode="json"))
    monkeypatch.setattr(chain, "_visible", AsyncMock(return_value=()))
    assert await chain._active(object(), row, scope, NOW)
    monkeypatch.setattr(chain, "_verify", AsyncMock())
    monkeypatch.setattr(chain, "_visible", AsyncMock(return_value=(rev_row,)))
    assert not await chain._active(object(), row, scope, NOW + timedelta(seconds=2))
    rev_row.payload["scope_hash"] = "b" * 64
    with pytest.raises(ValueError, match=chain.INVALID):
        await chain._active(object(), row, scope, NOW + timedelta(seconds=2))


@pytest.mark.asyncio
async def test_duplicate_revocation_is_unknown(monkeypatch):
    monkeypatch.setattr(chain, "_visible", AsyncMock(return_value=(object(), object())))
    with pytest.raises(ValueError, match=chain.INVALID):
        await chain._active(object(), SimpleNamespace(id=UUID(int=81)), scope_value(), NOW)


@pytest.mark.asyncio
async def test_missing_active_member_is_unknown_and_read_never_writes(monkeypatch):
    monkeypatch.setattr(chain, "_visible", AsyncMock(return_value=()))
    monkeypatch.setattr(chain, "_scope", AsyncMock(return_value=scope_value()))
    monkeypatch.setattr(chain, "_active", AsyncMock(return_value=True))
    session = SimpleNamespace(
        scalars=AsyncMock(return_value=(object(),)),
        get=AsyncMock(),
        scalar=AsyncMock(return_value=None),
    )
    with pytest.raises(ValueError, match=chain.INVALID):
        await chain.read_member(session, plan_value(), NOW)
    monkeypatch.setattr(chain, "_active", AsyncMock(return_value=False))
    assert await chain.read_member(session, plan_value(), NOW) is None
    assert not hasattr(session, "add")


@pytest.mark.asyncio
async def test_old_authorization_has_priority_without_new_scope_reads(monkeypatch):
    monkeypatch.setattr(
        chain, "read_member", AsyncMock(side_effect=AssertionError("unexpected scope read"))
    )
    # A selected old policy reaches its original bound-origin read, proving the
    # new scope lookup is not consulted or allowed to rewrite old policy facts.
    monkeypatch.setattr(old, "read_policy", AsyncMock(return_value=(object(), object())))
    monkeypatch.setattr(old, "read_origin", AsyncMock(return_value=None))
    with pytest.raises(ValueError, match=old.INVALID):
        await old.read_recent_observation(object(), plan=plan_value(), now=NOW)
    chain.read_member.assert_not_awaited()


@pytest.mark.asyncio
async def test_effective_time_cannot_reinterpret_an_existing_check():
    session = SimpleNamespace(scalar=AsyncMock(side_effect=[NOW, NOW]))
    with pytest.raises(ValueError, match=chain.INVALID):
        await chain._clock(session, plan=plan_value())


def test_chain_observation_cannot_claim_false_or_complete():
    fields = chain.ChainPositiveObservation.model_fields
    assert (
        fields["result"].annotation
        == old.RecentPositiveObservation.model_fields["result"].annotation
    )
    assert (
        fields["history_coverage"].annotation
        == old.RecentPositiveObservation.model_fields["history_coverage"].annotation
    )
    assert "scope_snapshot_id" in fields and "scope_hash" in fields
    assert len(sha256(scope_value().canonical()).hexdigest()) == 64


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["new", "empty", "stale", "conflict"])
async def test_chain_true_partial_normal_reader_reuses_existing_raw_checks(monkeypatch, mode):
    from unit.research.test_sec_forward_comparison import indexes

    _, _, current, first = indexes()
    now = current.received_at
    scope = scope_value()
    new = current.entries[0].model_copy(update={"accession_number": "0000000042-26-000999"})
    entries = (new,) if mode != "empty" else ()
    if mode == "conflict":
        prior = first.entries[0]
        entries += (prior.model_copy(update={"form": "8-K/A" if prior.form == "8-K" else "8-K"}),)
    current = current.model_copy(
        update={
            "entries": entries,
            "received_at": now - timedelta(seconds=301 if mode == "stale" else 0),
        }
    )
    member = SimpleNamespace(
        issuer_cik=first.issuer_cik,
        effective_at=now,
        scope_snapshot_id=UUID(int=80),
        scope_hash="a" * 64,
    )
    selected = SimpleNamespace(id=UUID(int=81), revision="b" * 64)
    basis = SimpleNamespace(
        baseline_snapshot_id=UUID(int=82),
        baseline_hash="c" * 64,
        baseline_as_of=now - timedelta(hours=2),
        capture_id=UUID(int=83),
        capture_hash="d" * 64,
        effective_at=now - timedelta(hours=1),
    )
    monkeypatch.setattr(old, "read_policy", AsyncMock(return_value=None))
    monkeypatch.setattr(chain, "read_member", AsyncMock(return_value=(selected, member)))
    monkeypatch.setattr(
        old,
        "read_origin",
        AsyncMock(return_value=(SimpleNamespace(id=UUID(int=84)), basis, current, first)),
    )
    row = SimpleNamespace(id=UUID(int=85), revision="e" * 64, available_at=now)
    monkeypatch.setattr(old, "_capture", AsyncMock(return_value=(row, current)))
    monkeypatch.setattr(old, "all_c1_accessions", AsyncMock(return_value=set()))
    monkeypatch.setattr(old, "configured_exhibit_store", lambda _: object())
    result = await old.read_recent_observation(
        SimpleNamespace(scalar=AsyncMock(return_value=row)),
        plan=SimpleNamespace(id=scope.plan_id, instrument_id=first.instrument_id),
        now=now,
    )
    assert isinstance(result, chain.ChainPositiveObservation)
    assert result.policy == chain.POLICY
    assert (result.policy_snapshot_id, result.policy_hash) == (selected.id, selected.revision)
    assert (result.scope_snapshot_id, result.scope_hash) == (
        member.scope_snapshot_id,
        member.scope_hash,
    )
    assert result.result == ("true" if mode == "new" else "unknown")
    assert result.history_coverage == "partial"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [None, "source", "scopehash", "expiry", "plan_expired", "plan_replaced", "historical"],
)
async def test_member_hash_custody_and_expiry_readback(monkeypatch, mutation):
    scope = scope_value()
    bindings = scope.model_dump(
        exclude={
            "policy",
            "authorization_scope",
            "root_request_id",
            "effective_at",
            "anchor_policy_snapshot_id",
            "anchor_policy_hash",
            "expires_at",
        }
    )
    member = chain.PositiveChainMember(
        **bindings, scope_snapshot_id=UUID(int=80), scope_hash="a" * 64, effective_at=NOW
    )
    scope_row = SimpleNamespace(id=member.scope_snapshot_id, revision=member.scope_hash)
    member_row = SimpleNamespace(payload=member.model_dump(mode="json"))
    monkeypatch.setattr(chain, "_visible", AsyncMock(return_value=(member_row,)))
    monkeypatch.setattr(chain, "_verify", AsyncMock())
    monkeypatch.setattr(chain, "_scope", AsyncMock(return_value=scope))
    monkeypatch.setattr(chain, "_binding_values", AsyncMock(return_value=bindings))
    monkeypatch.setattr(chain, "_active", AsyncMock(return_value=mutation != "expiry"))
    session = SimpleNamespace(
        get=AsyncMock(return_value=scope_row),
        scalar=AsyncMock(return_value=UUID(int=501) if mutation == "plan_replaced" else None),
    )
    plan = plan_value(valid_until=NOW + timedelta(days=1))
    read_at = plan.valid_until if mutation == "plan_expired" else NOW
    if mutation == "historical":
        session.scalar = AsyncMock(side_effect=[UUID(int=501), None])
        assert await chain.read_member(session, plan, NOW + timedelta(seconds=3)) is None
        read_at = NOW + timedelta(seconds=1)
    if mutation == "source":
        bindings["source_hash"] = "f" * 64
    if mutation == "scopehash":
        scope_row.revision = "f" * 64
    if mutation in {"source", "scopehash"}:
        with pytest.raises(ValueError, match=chain.INVALID):
            await chain.read_member(session, plan, NOW)
    else:
        result = await chain.read_member(session, plan, read_at)
        assert (result is None) == (mutation in {"expiry", "plan_expired", "plan_replaced"})
        if mutation == "historical":
            query = session.scalar.await_args.args[0]
            params = query.compile().params
            assert read_at in params.values()
            assert "created_at" in str(query) and "effective_at" in str(query)


@pytest.mark.asyncio
async def test_grant_uses_current_normal_descendant_even_when_anchor_ttl_expired(monkeypatch):
    anchor = plan_value(valid_until=NOW - timedelta(minutes=1))
    current = plan_value(
        id=UUID(int=201),
        predecessor_id=anchor.id,
        generation=2,
        valid_until=NOW + timedelta(days=1),
    )
    original = policy_fixture(NOW - timedelta(hours=1))
    original_row = SimpleNamespace(id=UUID(int=90), revision="e" * 64)
    clock = NOW + timedelta(seconds=1)
    monkeypatch.setattr(chain, "lock_security", AsyncMock())
    monkeypatch.setattr(chain, "_clock", AsyncMock(return_value=clock))
    monkeypatch.setattr(chain, "_visible", AsyncMock(return_value=()))
    monkeypatch.setattr(chain, "read_policy", AsyncMock(return_value=(original_row, original)))
    monkeypatch.setattr(chain, "_current", AsyncMock())
    monkeypatch.setattr(
        chain, "_binding_values", AsyncMock(return_value=scope_value().model_dump())
    )
    monkeypatch.setattr(chain, "_append", AsyncMock(return_value=SimpleNamespace(id=UUID(int=300))))
    monkeypatch.setattr(chain, "ensure_member", AsyncMock())

    async def get_plan(model, identity):
        return anchor

    session = SimpleNamespace(get=get_plan, scalar=AsyncMock(return_value=current))
    await chain.enable_scope(session, account_id=anchor.account_id, plan_id=anchor.id, now=NOW)
    scope = chain._append.await_args.args[1]
    assert scope.plan_id == anchor.id
    assert scope.effective_at == clock and scope.expires_at == clock + timedelta(days=7)
    assert chain._current.await_args.args[1] == current
    assert chain.ensure_member.await_args.kwargs["plan"] == current


@pytest.mark.asyncio
async def test_original_v1_scope_is_never_renewed(monkeypatch):
    anchor = plan_value()
    row = SimpleNamespace(id=UUID(int=301))
    monkeypatch.setattr(chain, "lock_security", AsyncMock())
    monkeypatch.setattr(chain, "_clock", AsyncMock(return_value=NOW + timedelta(days=8)))
    monkeypatch.setattr(chain, "_visible", AsyncMock(return_value=(row,)))
    monkeypatch.setattr(chain, "_scope", AsyncMock(return_value=scope_value()))
    monkeypatch.setattr(chain, "_append", AsyncMock(side_effect=AssertionError("must not renew")))
    assert (
        await chain.enable_scope(
            SimpleNamespace(get=AsyncMock(return_value=anchor)),
            account_id=anchor.account_id,
            plan_id=anchor.id,
            now=NOW,
        )
        is row
    )
    chain._append.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode", ["valid", "hash", "time", "audit_payload", "audit_future", "audit_duplicate"]
)
async def test_scope_exact_hash_clock_and_audit_are_independent_proofs(mode):
    value = scope_value()
    row = SimpleNamespace(
        id=UUID(int=301),
        source=chain.SCOPE_SOURCE,
        schema_version=value.policy,
        revision=sha256(value.canonical()).hexdigest(),
        created_at=NOW,
        available_at=NOW,
        observed_at=NOW,
        received_at=NOW,
    )
    audit = SimpleNamespace(
        actor_id="research-monitor",
        schema_version=value.policy,
        occurred_at=NOW,
        created_at=NOW,
        payload={
            "snapshot_id": str(row.id),
            "policy_hash": row.revision,
            **value.model_dump(mode="json"),
        },
    )
    if mode == "hash":
        row.revision = "f" * 64
    if mode == "time":
        row.available_at = NOW - timedelta(microseconds=1)
    if mode == "audit_payload":
        audit.payload = {**audit.payload, "scope_hash": "f" * 64}
    if mode == "audit_future":
        audit.created_at = NOW + timedelta(microseconds=1)
    session = SimpleNamespace(
        scalars=AsyncMock(return_value=(audit, audit) if mode == "audit_duplicate" else (audit,))
    )
    if mode == "valid":
        await chain._verify(session, row, value, chain.SCOPE_SOURCE, chain.SCOPE_ACTION, NOW)
    else:
        with pytest.raises(ValueError, match=chain.INVALID):
            await chain._verify(session, row, value, chain.SCOPE_SOURCE, chain.SCOPE_ACTION, NOW)


@pytest.mark.asyncio
@pytest.mark.parametrize("corrupt", [False, True])
async def test_api_explicit_chain_policy_validates_saved_member_at_historical_check(
    monkeypatch, corrupt
):
    from nexus.api.research_disclosure_monitoring import _comparison_boundary
    from nexus.monitoring import disclosure_inheritance

    scope = scope_value()
    origin = SimpleNamespace(
        capture_id=scope.comparison_snapshot_id,
        capture_hash=scope.comparison_hash,
        effective_at=NOW - timedelta(hours=1),
        baseline_snapshot_id=UUID(int=302),
        baseline_hash="b" * 64,
        baseline_as_of=NOW - timedelta(hours=2),
    )
    observation = chain.ChainPositiveObservation(
        instrument_id=scope.instrument_id,
        plan_id=scope.plan_id,
        baseline_snapshot_id=origin.baseline_snapshot_id,
        baseline_hash=origin.baseline_hash,
        baseline_as_of=origin.baseline_as_of,
        issuer_cik=scope.issuer_cik,
        current_snapshot_id=None,
        current_hash=None,
        received_at=None,
        available_at=None,
        origin_id=scope.own_origin_id,
        comparison_snapshot_id=origin.capture_id,
        comparison_hash=origin.capture_hash,
        comparison_from=origin.effective_at,
        policy_snapshot_id=UUID(int=303),
        policy_hash="c" * 64,
        scope_snapshot_id=UUID(int=304),
        scope_hash="d" * 64,
        result="unknown",
        reason="SEC_RECENT_NO_NEW_TARGET_CONFIRMED",
        added=(),
    )
    check = SimpleNamespace(
        observation=observation.model_dump(mode="json"), checked_at=NOW, result="unknown"
    )
    member = SimpleNamespace(
        scope_snapshot_id=observation.scope_snapshot_id,
        scope_hash="f" * 64 if corrupt else observation.scope_hash,
    )
    monkeypatch.setattr(
        chain,
        "read_member",
        AsyncMock(
            return_value=(
                SimpleNamespace(
                    id=observation.policy_snapshot_id, revision=observation.policy_hash
                ),
                member,
            )
        ),
    )
    monkeypatch.setattr(
        disclosure_inheritance,
        "read_binding",
        AsyncMock(
            return_value=(SimpleNamespace(id=scope.own_origin_id), object(), object(), origin)
        ),
    )
    if corrupt:
        with pytest.raises(ValueError, match=chain.INVALID):
            await _comparison_boundary(object(), check, plan_value())
    else:
        assert await _comparison_boundary(object(), check, plan_value()) == (
            origin.effective_at,
            origin.baseline_as_of,
        )
    assert chain.read_member.await_args.args[2] == check.checked_at


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["valid", "expiry", "anchor_hash", "source_binding"])
async def test_scope_reader_revalidates_original_one_plan_authorization(monkeypatch, mode):
    scope = scope_value()
    previous = policy_fixture(NOW - timedelta(hours=1))
    row = SimpleNamespace(
        payload=scope.model_dump(mode="json"), provider_object_id=str(scope.plan_id)
    )
    old_row = SimpleNamespace(id=scope.anchor_policy_snapshot_id, revision=scope.anchor_policy_hash)
    if mode == "expiry":
        row.payload["expires_at"] = (scope.expires_at + timedelta(microseconds=1)).isoformat()
    if mode == "anchor_hash":
        old_row.revision = "f" * 64
    if mode == "source_binding":
        row.payload["source_hash"] = "f" * 64
    monkeypatch.setattr(chain, "_verify", AsyncMock())
    monkeypatch.setattr(chain, "read_policy", AsyncMock(return_value=(old_row, previous)))
    session = SimpleNamespace(get=AsyncMock(return_value=plan_value()))
    if mode == "valid":
        assert await chain._scope(session, row, NOW) == scope
    else:
        with pytest.raises(ValueError, match=chain.INVALID):
            await chain._scope(session, row, NOW)


@pytest.mark.asyncio
async def test_normal_install_without_scope_has_no_new_clock_or_registration_gate(monkeypatch):
    monkeypatch.setattr(chain, "lock_security", AsyncMock())
    monkeypatch.setattr(chain, "_clock", AsyncMock(side_effect=AssertionError("must remain no-op")))
    monkeypatch.setattr(
        chain, "_append", AsyncMock(side_effect=AssertionError("must remain no-op"))
    )
    assert (
        await chain.ensure_member(
            SimpleNamespace(scalars=AsyncMock(return_value=())), plan=plan_value(), now=NOW
        )
        is None
    )
    chain._clock.assert_not_awaited()
    chain._append.assert_not_awaited()
