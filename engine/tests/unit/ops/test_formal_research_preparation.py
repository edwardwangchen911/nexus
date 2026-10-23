"""The formal trigger prepares once under its committed native execution."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.ops import formal_research_preparation as preparation


@pytest.mark.asyncio
async def test_prepare_orders_sources_and_requires_exact_final_snapshot(monkeypatch, tmp_path):
    events = []
    candidate = SimpleNamespace(instrument_id=UUID(int=1), symbol="TEST")
    scope = SimpleNamespace(
        now=datetime(2026, 10, 22, 15, tzinfo=UTC), calendar_id=UUID(int=2), candidates=(candidate,)
    )
    monkeypatch.setattr(preparation, "read_preparation_scope", AsyncMock(return_value=scope))

    async def step(name, **kwargs):
        events.append(name)
        return "prepared"

    from nexus.ops import agent_paper_loop as loop

    monkeypatch.setattr(loop, "prepare_prior_daily_once", lambda *a, **kw: step("daily", **kw))
    monkeypatch.setattr(loop, "prepare_macro_once", lambda *a, **kw: step("macro", **kw))
    monkeypatch.setattr(loop, "prepare_current_trends_once", lambda *a, **kw: step("trends", **kw))
    monkeypatch.setattr(
        preparation, "prepare_agent_paper_identity_once", lambda *a, **kw: step("identity", **kw)
    )

    async def freeze(*a, **kw):
        events.append("freeze_readback")

    monkeypatch.setattr(preparation, "freeze_prepared_snapshots", freeze)
    context = SimpleNamespace(
        account_id=UUID(int=3), reservation=SimpleNamespace(candidate_batch_id=UUID(int=4))
    )
    await preparation.prepare_formal_trigger_inputs(
        SimpleNamespace(), context=context, repo_root=tmp_path, process_environment={}
    )
    assert events == ["daily", "identity", "macro", "trends", "freeze_readback"]


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["daily", "identity", "trends"])
async def test_required_preparation_failure_never_freezes_or_retries(monkeypatch, tmp_path, phase):
    candidate = SimpleNamespace(instrument_id=UUID(int=1), symbol="TEST")
    scope = SimpleNamespace(
        now=datetime(2026, 10, 22, 15, tzinfo=UTC), calendar_id=UUID(int=2), candidates=(candidate,)
    )
    monkeypatch.setattr(preparation, "read_preparation_scope", AsyncMock(return_value=scope))
    calls = []

    async def step(name):
        calls.append(name)
        return "unavailable" if name == phase else "ready"

    from nexus.ops import agent_paper_loop as loop

    for name, attribute in [
        ("daily", "prepare_prior_daily_once"),
        ("macro", "prepare_macro_once"),
        ("trends", "prepare_current_trends_once"),
    ]:
        monkeypatch.setattr(loop, attribute, lambda *a, _name=name, **kw: step(_name))
    monkeypatch.setattr(
        preparation, "prepare_agent_paper_identity_once", lambda *a, **kw: step("identity")
    )
    freeze = AsyncMock()
    monkeypatch.setattr(preparation, "freeze_prepared_snapshots", freeze)
    context = SimpleNamespace(
        account_id=UUID(int=3), reservation=SimpleNamespace(candidate_batch_id=UUID(int=4))
    )
    with pytest.raises(ValueError, match="AGENT_PAPER_TRIGGER_FAILED"):
        await preparation.prepare_formal_trigger_inputs(
            SimpleNamespace(), context=context, repo_root=tmp_path, process_environment={}
        )
    assert calls.count(phase) == 1
    freeze.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_at", [None, "prepare", "identity_scope", "trigger", "secondary"])
@pytest.mark.parametrize("trigger_mode", ["scheduled", "manual_recovery", "synthesis_recovery"])
async def test_native_trigger_prepares_after_claim_once_and_preserves_failure(
    monkeypatch, tmp_path, fail_at, trigger_mode, capsys
):
    from contextlib import asynccontextmanager

    from nexus.ops import agent_paper_once as once
    from nexus.paper.agent_cycle_models import AgentPaperTriggerReservation

    if fail_at in {"prepare", "identity_scope"} and trigger_mode != "scheduled":
        pytest.skip("Recovery bypasses collection by contract")
    events = []
    original = RuntimeError("private-source-response")
    reservation = AgentPaperTriggerReservation(
        claim_id=UUID(int=8),
        candidate_batch_id=UUID(int=2),
        decision_session=datetime(2026, 10, 22).date(),
        decision_slot_at=datetime(2026, 10, 22, 15, tzinfo=UTC),
        formal_stage_capacity=trigger_mode == "scheduled",
        formal_model_config_id=UUID(int=9) if trigger_mode == "scheduled" else None,
        formal_model_hash="a" * 64,
        trigger_mode=trigger_mode,
    )

    @asynccontextmanager
    async def transaction():
        yield
        events.append("commit")

    class Session:
        async def scalar(self, _):
            return datetime(2026, 10, 22, 15, tzinfo=UTC)

        def begin(self):
            return transaction()

    @asynccontextmanager
    async def factory():
        yield Session()

    @asynccontextmanager
    async def client(**kwargs):
        yield object()

    async def reserve(*a, **kw):
        events.append("claim")
        return reservation

    original_prepare = preparation.prepare_formal_trigger_inputs
    if fail_at == "identity_scope":
        from nexus.ops import agent_paper_identity as identity

        monkeypatch.setattr(
            preparation,
            "read_preparation_scope",
            AsyncMock(
                return_value=SimpleNamespace(
                    now=datetime(2026, 10, 22, 15, tzinfo=UTC),
                    calendar_id=UUID(int=10),
                    candidates=(),
                )
            ),
        )
        monkeypatch.setattr(identity, "_read_scope", AsyncMock(return_value=None))

    async def prepare(*a, **kw):
        events.append("prepare")
        if fail_at == "identity_scope":
            return await original_prepare(*a, **kw)
        if fail_at == "prepare":
            raise original

    async def trigger(*a, **kw):
        events.append("trigger")
        if fail_at in {"trigger", "secondary"}:
            raise original
        return "success"

    async def finish(*a, **kw):
        events.append(kw.get("error_code", "finish"))
        if fail_at == "secondary":
            raise RuntimeError("private-secondary-failure")

    monkeypatch.setattr(once, "_validate_trigger_dependencies", lambda *_: None)
    monkeypatch.setattr(
        once, "create_async_engine", lambda *_: SimpleNamespace(dispose=AsyncMock())
    )
    monkeypatch.setattr(once, "async_sessionmaker", lambda *a, **kw: factory)
    monkeypatch.setattr(once, "reserve_schwab_agent_paper_trigger", reserve)
    monkeypatch.setattr(preparation, "prepare_formal_trigger_inputs", prepare)
    monkeypatch.setattr(once, "trigger_in_session", trigger)
    monkeypatch.setattr(once, "finish_schwab_agent_paper_trigger", finish)
    monkeypatch.setattr(once, "S3ProviderEvidenceStore", lambda *a, **kw: object())
    monkeypatch.setattr(once, "schwab_http_client", client)
    settings = SimpleNamespace(database_url="postgresql+asyncpg://localhost/test")
    request = once.AgentPaperTriggerRequest(
        action="trigger",
        account_id=UUID(int=1),
        candidate_batch_id=UUID(int=2),
        idempotency_key="test",
        formal_stage_capacity=trigger_mode == "scheduled",
        formal_model_config_id=UUID(int=9) if trigger_mode == "scheduled" else None,
        trigger_mode=trigger_mode,
    )
    if fail_at:
        with pytest.raises(ValueError if fail_at == "identity_scope" else RuntimeError) as caught:
            await once.trigger_once(
                settings,
                request,
                repo_root=tmp_path,
                process_environment={"SEC_CONTACT": "synthetic@example.invalid"},
            )
        if fail_at == "identity_scope":
            import json

            from nexus.ops import agent_paper_loop as loop

            assert str(caught.value) == "AGENT_PAPER_TRIGGER_FAILED"
            loop._emit_agent_paper_unknown_failure(
                "execute",
                datetime(2026, 10, 22, 15, tzinfo=UTC),
                action=loop.AgentPaperLoopAction.TRIGGER,
                error=caught.value,
            )
            rows = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
            assert rows[0]["failure_phase"] == "research_identity"
            assert rows[0]["failure_code"] == "SCOPE_UNAVAILABLE"
            assert rows[-1]["failure_code"] == "AGENT_PAPER_TRIGGER_FAILED"
            assert rows[-1]["failure_kind"] == "validation"
            assert rows[-1]["failure_phase"] == "trigger_provider"
            assert rows[-1]["failure_action"] == "trigger"
        else:
            assert caught.value is original
        if fail_at != "secondary":
            assert events[-2:] == ["AGENT_PAPER_TRIGGER_FAILED", "commit"]
        else:
            assert "AGENT_PAPER_TRIGGER_FAILED" in events
            err = capsys.readouterr().err
            assert "trigger_failure_receipt" in err and "UNEXPECTED_EXCEPTION" in err
            assert "private" not in err
    else:
        assert (
            await once.trigger_once(settings, request, repo_root=tmp_path, process_environment={})
            == "success"
        )
    assert events[:2] == ["claim", "commit"]
    assert events.count("prepare") == (1 if trigger_mode == "scheduled" else 0)
    assert events.count("trigger") == (0 if fail_at in {"prepare", "identity_scope"} else 1)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "tamper", [None, "actor", "hash", "response", "request_key", "execution", "closed"]
)
async def test_manual_scope_requires_exact_native_claim_and_execution(monkeypatch, tamper):
    from datetime import timedelta
    from hashlib import sha256

    from nexus.agents import codex_formal_stage
    from nexus.jobs.models import job_input_hash
    from nexus.paper import manual_request
    from nexus.paper.agent_cycle_models import AgentPaperTriggerReservation
    from nexus.persistence.models.paper import PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID

    now = datetime(2026, 10, 22, 15, tzinfo=UTC)
    model = SimpleNamespace(id=UUID(int=7), config_hash="a" * 64)
    batch = SimpleNamespace(batch_id=UUID(int=8), batch_hash="b" * 64)
    manual = SimpleNamespace(id=UUID(int=9), mode="research_and_paper")
    reservation = AgentPaperTriggerReservation(
        claim_id=UUID(int=10),
        decision_session=now.date(),
        decision_slot_at=now,
        candidate_batch_id=batch.batch_id,
        candidate_batch_hash=batch.batch_hash,
        formal_stage_capacity=True,
        formal_model_config_id=model.id,
        formal_model_hash=model.config_hash,
        trigger_mode="manual",
        manual_request_id=manual.id,
        manual_execution_id=UUID(int=11),
    )
    key = f"manual-entry:{manual.id}"
    context = preparation.FormalPreparationContext(UUID(int=12), reservation, key, UUID(int=13))
    payload = {
        "schema": "agent-paper-explicit-manual-v1",
        "account_id": str(context.account_id),
        "decision_session": now.date().isoformat(),
        "decision_slot_at": now.isoformat(),
        "source_policy_id": str(PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID),
        "client_idempotency_hash": sha256(key.encode()).hexdigest(),
        "candidate_batch_id": str(batch.batch_id),
        "candidate_batch_hash": batch.batch_hash,
        "formal_stage_capacity": True,
        "formal_model_config_id": str(model.id),
        "formal_model_hash": model.config_hash,
        "manual_execution_id": str(reservation.manual_execution_id),
        "trigger_mode": "manual",
        "manual_request_id": str(manual.id),
        "manual_mode": manual.mode,
    }
    claim = SimpleNamespace(
        actor_id=f"local-agent-paper-schwab:{context.account_id}",
        route=payload["schema"],
        schema_version="agent_paper_explicit_manual_v1",
        idempotency_key=str(manual.id),
        request_hash=job_input_hash(payload),
        response_payload={},
    )
    if tamper == "actor":
        claim.actor_id = "another-account"
    if tamper == "hash":
        claim.request_hash = "c" * 64
    if tamper == "response":
        claim.response_payload = {"status": "failed"}
    if tamper == "request_key":
        context = preparation.FormalPreparationContext(
            context.account_id, reservation, "wrong", context.calendar_id
        )
    calendar = SimpleNamespace(
        session=now.date(),
        local_date=now.date(),
        market_open_at=now - timedelta(hours=2),
        market_close_at=now + timedelta(hours=4),
    )
    if tamper == "closed":
        calendar.market_close_at = now
    checked = []

    async def require_execution(session, **kwargs):
        checked.append(kwargs)
        assert kwargs["execution_id"] == reservation.manual_execution_id
        assert kwargs["request_id"] == manual.id
        assert kwargs["account_id"] == context.account_id
        if tamper == "execution":
            raise ValueError("MANUAL_REQUEST_TERMINAL_OR_UNKNOWN")
        return manual

    monkeypatch.setattr(
        codex_formal_stage, "require_formal_stage_model_profile", AsyncMock(return_value=model)
    )
    monkeypatch.setattr(manual_request, "require_preparing_manual_request", require_execution)
    monkeypatch.setattr(
        preparation, "validate_agent_paper_candidate_batch", AsyncMock(return_value=(batch, ()))
    )
    monkeypatch.setattr(
        preparation, "load_agent_paper_service_calendar_authority", AsyncMock(return_value=calendar)
    )
    session = SimpleNamespace(get=AsyncMock(return_value=claim))
    if tamper:
        with pytest.raises(ValueError):
            await preparation.require_preparation_scope(session, context, now=now)
    else:
        assert await preparation.require_preparation_scope(session, context, now=now) is calendar
        assert len(checked) == 1
