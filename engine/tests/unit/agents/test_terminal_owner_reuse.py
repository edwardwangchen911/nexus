"""The terminal reader validates one owner without repeating frozen context reads."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from nexus.agents import platform_stage_accounting
from nexus.agents.research_stages import proof, routes
from nexus.agents.research_stages.protocol import ProtocolRejected
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.runtime import Job


class ReachedAccounting(Exception):
    pass


class Rows:
    def __init__(self, value):
        self.value = value

    def mappings(self):
        return self

    def one_or_none(self):
        return self.value

    def __iter__(self):
        return iter(self.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["formal", "independent", "comparison", "increment"])
@pytest.mark.parametrize("wrong_expected_job", [False, True])
async def test_terminal_reader_resolves_owner_once(monkeypatch, kind, wrong_expected_job):
    run_id, job_id, scope_id = uuid4(), uuid4(), uuid4()
    now = datetime(2026, 11, 16, tzinfo=UTC)
    run = SimpleNamespace(id=run_id, model_config_id=uuid4())
    job = SimpleNamespace(id=job_id, payload={"run_id": str(run_id)})
    row = {
        "run_id": run_id,
        "job_id": job_id,
        "scope_id": scope_id,
        "kind": kind,
        "version": routes.VERSION,
        "platform_binding": None,
    }

    class Session:
        scalar = AsyncMock(return_value=True)

        async def execute(self, statement, *args):
            sql = str(statement)
            if "JOIN research_stage_scope" in sql:
                return Rows({"job_id": job_id, "scope_id": scope_id})
            if "FROM research_stage_terminal" in sql:
                return Rows({"closed_at": now})
            if "FROM research_stage_route" in sql:
                assert kind != "increment", "increment retains its original no-route branch"
                return Rows(row)
            raise AssertionError(sql)

        async def get(self, model, identity):
            assert model in (AgentResearchRunRecord, Job)
            return run if model is AgentResearchRunRecord else job

        async def scalars(self, statement):
            return Rows([job])

    owner = AsyncMock(return_value=(scope_id, kind))
    monkeypatch.setattr(proof, "owner", owner)
    monkeypatch.setattr(routes, "owner", owner)
    monkeypatch.setattr(proof.admission, "require_increment_binding", AsyncMock())
    monkeypatch.setattr(
        platform_stage_accounting,
        "read_stage_accounting_model",
        AsyncMock(side_effect=ReachedAccounting),
    )
    with pytest.raises(ProtocolRejected if wrong_expected_job else ReachedAccounting):
        await proof.read_terminal_proof(
            Session(),
            run_id=run_id,
            now=now,
            expected_job_id=uuid4() if wrong_expected_job else None,
        )
    assert owner.await_count == (0 if wrong_expected_job else 1)


@pytest.mark.asyncio
async def test_unselected_legacy_route_does_not_resolve_owner(monkeypatch):
    session = SimpleNamespace(
        scalar=AsyncMock(return_value=True), execute=AsyncMock(return_value=Rows(None))
    )
    owner = AsyncMock(side_effect=AssertionError("legacy route has no staged owner"))
    monkeypatch.setattr(routes, "owner", owner)
    assert await routes.selected_route(session, uuid4()) is None
    owner.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["run_id", "job_id", "scope_id", "kind", "version"])
async def test_public_route_still_rejects_each_owner_binding_mismatch(monkeypatch, field):
    from nexus.agents.research_stages.protocol import ProtocolRejected

    run_id, job_id, scope_id = uuid4(), uuid4(), uuid4()
    run = SimpleNamespace(id=run_id)
    job = SimpleNamespace(id=job_id, payload={"run_id": str(run_id)})
    row = {
        "run_id": run_id,
        "job_id": job_id,
        "scope_id": scope_id,
        "kind": "formal",
        "version": routes.VERSION,
        "platform_binding": None,
    }
    row[field] = uuid4() if field.endswith("_id") else "invalid"

    async def get(model, identity):
        return run if model is AgentResearchRunRecord else job

    session = SimpleNamespace(
        scalar=AsyncMock(return_value=True), execute=AsyncMock(return_value=Rows(row)), get=get
    )
    monkeypatch.setattr(routes, "owner", AsyncMock(return_value=(scope_id, "formal")))
    with pytest.raises(ProtocolRejected, match="STAGED_ROUTE_BINDING_INVALID"):
        await routes.selected_route(session, job_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["formal", "independent"])
@pytest.mark.parametrize("owner_fault", [None, "scope", "kind", "run", "job", "version", "native"])
async def test_public_member_runs_canonical_owner_validation_once(monkeypatch, kind, owner_fault):
    from nexus.agents.research_stages import public

    run_id, job_id, scope_id = uuid4(), uuid4(), uuid4()
    now = datetime(2026, 11, 16, tzinfo=UTC)
    run = SimpleNamespace(id=run_id, model_config_id=uuid4())
    job = SimpleNamespace(id=job_id, payload={"run_id": str(run_id)})
    row = {
        "run_id": run_id,
        "job_id": job_id,
        "scope_id": scope_id,
        "kind": kind,
        "version": routes.VERSION,
        "platform_binding": None,
    }

    if owner_fault in {"run", "job"}:
        row[owner_fault + "_id"] = uuid4()
    if owner_fault == "version":
        row["version"] = "invalid"
    if owner_fault == "native":
        row["platform_binding"] = {}

    class Session:
        async def scalar(self, statement, *args):
            return (
                False
                if owner_fault == "native" and "nexus_sol_native_route_valid" in str(statement)
                else True
            )

        async def execute(self, statement, *args):
            sql = str(statement)
            if "JOIN research_stage_scope" in sql:
                return Rows({"job_id": job_id, "scope_id": scope_id})
            if "FROM research_stage_terminal" in sql:
                return Rows({"closed_at": now})
            if "FROM research_stage_route" in sql:
                return Rows(row)
            raise AssertionError(sql)

        async def get(self, model, identity):
            return run if model is AgentResearchRunRecord else job

        async def scalars(self, statement):
            return Rows([job])

    owner = AsyncMock(
        return_value=(
            uuid4() if owner_fault == "scope" else scope_id,
            "increment" if owner_fault == "kind" else kind,
        )
    )
    monkeypatch.setattr(proof, "owner", owner)
    monkeypatch.setattr(routes, "owner", owner)
    monkeypatch.setattr(
        platform_stage_accounting,
        "read_stage_accounting_model",
        AsyncMock(side_effect=ReachedAccounting),
    )
    with pytest.raises(
        ReachedAccounting if owner_fault is None else (ValueError, ProtocolRejected)
    ):
        await public._selected_member_settled(
            Session(),
            owner_id=scope_id,
            kind=kind,
            member=SimpleNamespace(run_id=run_id, job_id=job_id),
            now=now,
            allow_unknown_usage=False,
        )
    assert owner.await_count == (0 if owner_fault in {"run", "job", "version", "native"} else 1)


@pytest.mark.asyncio
async def test_unstaged_public_member_keeps_full_owner_and_unused_guard(monkeypatch):
    from nexus.agents.research_stages import public
    from nexus.paper import research_failure

    run_id, job_id, scope_id = uuid4(), uuid4(), uuid4()
    now = datetime(2026, 11, 16, tzinfo=UTC)
    run = SimpleNamespace(id=run_id)
    job = SimpleNamespace(
        id=job_id, payload={"run_id": str(run_id)}, result_code="MANUAL_UNUSED_RESEARCH_CANCELLED"
    )
    row = {
        "run_id": run_id,
        "job_id": job_id,
        "scope_id": scope_id,
        "kind": "formal",
        "version": routes.VERSION,
        "platform_binding": None,
    }

    class Session:
        async def scalar(self, statement, *args):
            return None if "SELECT 1 FROM research_stage_member" in str(statement) else True

        async def execute(self, *args):
            return Rows(row)

        async def get(self, model, identity):
            return run if model is AgentResearchRunRecord else job

    owner = AsyncMock(return_value=(scope_id, "formal"))
    unused = AsyncMock()
    monkeypatch.setattr(routes, "owner", owner)
    monkeypatch.setattr(research_failure, "require_unused_research_member_closed", unused)
    member = SimpleNamespace(run_id=run_id, job_id=job_id)
    await public._selected_member_settled(
        Session(),
        owner_id=scope_id,
        kind="formal",
        member=member,
        now=now,
        allow_unknown_usage=False,
    )
    owner.assert_awaited_once()
    unused.assert_awaited_once()
    assert unused.await_args.kwargs == {"owner_id": scope_id, "member": member, "now": now}
