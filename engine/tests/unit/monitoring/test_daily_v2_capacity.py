"""Daily material preview is local and never grants a complete-pool admission."""

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from nexus.agents.codex_bounded_budget import bounded_stage_config, bounded_stage_config_hash
from nexus.persistence.models.data import DataSnapshot
from unit.agents.test_codex_sdk import _authority, _tools


def setup_preview(monkeypatch, profile="bounded"):
    from nexus.agents.codex_capacity import capacity_config, capacity_config_hash
    from nexus.agents.codex_flexible_budget import flexible_stage_config, flexible_stage_config_hash
    from nexus.agents.codex_formal_stage import formal_stage_config, formal_stage_config_hash
    from nexus.monitoring import daily_v2_capacity as module

    request, refs, prompts = _authority()
    config_factory, hash_factory = {
        "bounded": (bounded_stage_config, bounded_stage_config_hash),
        "flexible": (flexible_stage_config, flexible_stage_config_hash),
        "full": (capacity_config, capacity_config_hash),
        "formal": (formal_stage_config, formal_stage_config_hash),
    }[profile]
    config = config_factory()
    model = SimpleNamespace(
        **{
            key: config[key]
            for key in (
                "version",
                "model_id",
                "max_input_tokens",
                "max_output_tokens",
                "max_tool_calls",
            )
        }
    )
    model.config_hash = hash_factory()
    model.input_usd_per_million = model.output_usd_per_million = 0
    for key in ("pricing_available_at", "pricing_expires_at"):
        setattr(model, key, datetime.fromisoformat(config[key]))
    run = SimpleNamespace(
        **request.model_dump(),
        model_config_id=uuid4(),
        pricing_version=model.version,
        model_hash=model.config_hash,
    )
    snapshot = SimpleNamespace(payload={"agent_paper_context": {"pending_monitoring_v2": {}}})
    session = SimpleNamespace(
        get=AsyncMock(side_effect=lambda kind, *a, **k: snapshot if kind is DataSnapshot else model)
    )
    records = tuple(
        SimpleNamespace(
            source=e.source,
            provider_object_id=e.object_id,
            available_at=e.available_at,
            location=e.location,
            content_hash=e.content_hash,
        )
        for e in refs
    )
    monkeypatch.setattr(module, "load_frozen_evidence", AsyncMock(return_value=records))
    monkeypatch.setattr(module, "load_frozen_prompts", AsyncMock(return_value=prompts))
    monkeypatch.setattr(module, "database_read_only_tools", AsyncMock(return_value=_tools()))
    monkeypatch.setattr(
        module,
        "read_agent_paper_research_context",
        AsyncMock(
            return_value=(
                SimpleNamespace(
                    pending_monitoring_v2=SimpleNamespace(trigger_checks=(1, 2, 3)),
                    pending_monitoring_v2_gap=None,
                ),
                object(),
            )
        ),
    )
    return module, session, run, snapshot, model, prompts, records


@pytest.mark.asyncio
@pytest.mark.parametrize("overflow", ["wire", "token", None])
async def test_exact_capacity_gates_and_zero_provider_calls(monkeypatch, overflow):
    module, session, run, *_ = setup_preview(monkeypatch)
    seen = []
    original = module.candidate_capacity_envelope

    def envelope(committee, **kwargs):
        seen.append(committee)
        result = original(committee, **kwargs)
        if overflow == "wire":
            result["wire_upper_bytes"] = committee._max_wire_bytes + 1
        return result

    monkeypatch.setattr(module, "candidate_capacity_envelope", envelope)
    if overflow == "token":
        monkeypatch.setattr(module, "codex_input_allowance", lambda *a: 96001)
    if overflow:
        with pytest.raises(module.DailyEventCapacityExceeded) as caught:
            await module.preview_daily_event_capacity(session, run)
        assert caught.value.instrument_id == run.instrument_id
        assert caught.value.pending_count == 3
        assert caught.value.reason == str(caught.value) == "MONITOR_CONTENT_CAPACITY"
    else:
        assert await module.preview_daily_event_capacity(session, run) is None
    assert len(seen) == 1
    assert seen[0].usage.provider_calls == 0
    assert seen[0]._capacity_pool_manifest is None
    assert not seen[0]._attempted
    module.database_read_only_tools.assert_awaited_once_with(
        session, run, allow_fixture_evidence=False, allow_fixture_price_evidence=False
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("skip", ["no_paper", "no_v2", "legacy_model"])
async def test_unrelated_runs_do_not_preview(monkeypatch, skip):
    module, session, run, snapshot, model, *_ = setup_preview(monkeypatch)
    if skip == "no_paper":
        snapshot.payload = {}
    elif skip == "no_v2":
        snapshot.payload["agent_paper_context"] = {"pending_monitoring_gap": {}}
    else:
        model.version = "legacy"
    await module.preview_daily_event_capacity(session, run)
    module.load_frozen_evidence.assert_not_awaited()
    module.database_read_only_tools.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", ["wire", "token", None])
async def test_gap_base_is_previewed_but_never_converted_to_another_gap(monkeypatch, limit):
    module, session, run, snapshot, *_ = setup_preview(monkeypatch)
    snapshot.payload["agent_paper_context"] = {"pending_monitoring_v2_gap": {}}
    module.read_agent_paper_research_context.return_value = (
        SimpleNamespace(
            pending_monitoring_v2=None,
            pending_monitoring_v2_gap=SimpleNamespace(reason="MONITOR_CONTENT_CAPACITY"),
        ),
        object(),
    )
    if limit == "wire":
        monkeypatch.setattr(
            module, "candidate_capacity_envelope", lambda *a, **k: {"wire_upper_bytes": 100480}
        )
    elif limit == "token":
        monkeypatch.setattr(module, "codex_input_allowance", lambda *a: 96001)
    if limit:
        with pytest.raises(
            ValueError,
            match=f"^CODEX_CAPACITY_POOL_{'WIRE' if limit == 'wire' else 'INPUT'}_EXCEEDED$",
        ) as caught:
            await module.preview_daily_event_capacity(session, run)
        assert not isinstance(caught.value, module.DailyEventCapacityExceeded)
    else:
        await module.preview_daily_event_capacity(session, run)
    module.database_read_only_tools.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", ["model", "run_hash", "prompts", "evidence", "context", "tools", "envelope"]
)
async def test_authority_errors_are_not_capacity_gaps(monkeypatch, fault):
    module, session, run, _, model, *_ = setup_preview(monkeypatch)
    if fault == "model":
        model.config_hash = "wrong"
    elif fault == "run_hash":
        run.model_hash = "wrong"
    elif fault in {"prompts", "evidence", "context"}:
        target = {
            "prompts": "load_frozen_prompts",
            "evidence": "load_frozen_evidence",
            "context": "read_agent_paper_research_context",
        }[fault]
        monkeypatch.setattr(
            module, target, AsyncMock(return_value=None if fault != "evidence" else ())
        )
    elif fault == "tools":
        monkeypatch.setattr(
            module,
            "database_read_only_tools",
            AsyncMock(side_effect=ValueError("ORIGINAL_AUTHORITY")),
        )
    else:
        monkeypatch.setattr(
            module, "candidate_capacity_envelope", Mock(side_effect=ValueError("ORIGINAL_SCHEMA"))
        )
    with pytest.raises(ValueError) as caught:
        await module.preview_daily_event_capacity(session, run)
    assert not isinstance(caught.value, module.DailyEventCapacityExceeded)
    if fault in {"tools", "envelope"}:
        assert str(caught.value).startswith("ORIGINAL_")


@pytest.mark.asyncio
@pytest.mark.parametrize("profile", ["formal", "full", "flexible", "bounded"])
async def test_preview_profile_matches_actual_formal_worker_composition(monkeypatch, profile):
    from nexus import worker
    from nexus.agents.price_authority import FrozenPriceAuthorityStatus
    from nexus.settings import EngineSettings

    module, session, run, _, _, prompts, records = setup_preview(monkeypatch, profile)
    monkeypatch.setattr(worker, "load_frozen_evidence", AsyncMock(return_value=records))
    monkeypatch.setattr(worker, "load_frozen_prompts", AsyncMock(return_value=prompts))
    monkeypatch.setattr(
        worker,
        "revalidate_frozen_price_authority",
        AsyncMock(return_value=FrozenPriceAuthorityStatus.MISSING),
    )
    actual = await worker._committee(
        session,
        EngineSettings(agent_runtime_mode="codex_sdk"),
        run,
        responses_client=None,
        tools=_tools(),
        deadline=1000,
        formal_stage_capacity=True,
    )
    monkeypatch.setattr(
        module.asyncio, "get_running_loop", lambda: SimpleNamespace(time=lambda: 805)
    )
    captured = []
    original = module.candidate_capacity_envelope

    def capture(committee, **kwargs):
        captured.append(committee)
        return original(committee, **kwargs)

    monkeypatch.setattr(module, "candidate_capacity_envelope", capture)
    await module.preview_daily_event_capacity(session, run)
    for field in (
        "_stage_time_budget",
        "_stage_budget_version",
        "_paper_stage_scope",
        "_inline_citations",
        "_deadline",
        "_max_wire_bytes",
        "_aggregate_input_tokens",
        "structured_synthesis",
        "_prompts",
    ):
        assert getattr(captured[0], field) == getattr(actual, field), field


@pytest.mark.asyncio
@pytest.mark.parametrize("allowance", [64000, 64001])
async def test_exact_research_gate_is_64k_even_with_96k_aggregate(monkeypatch, allowance):
    module, session, run, *_ = setup_preview(monkeypatch)
    monkeypatch.setattr(
        module, "candidate_capacity_envelope", lambda *a, **k: {"wire_upper_bytes": 100479}
    )
    monkeypatch.setattr(module, "codex_input_allowance", lambda *a: allowance)
    if allowance == 64001:
        with pytest.raises(module.DailyEventCapacityExceeded):
            await module.preview_daily_event_capacity(session, run)
    else:
        await module.preview_daily_event_capacity(session, run)


@pytest.mark.asyncio
async def test_real_daily_envelope_rejects_complete_material_bytes_without_sdk(monkeypatch):
    from unit.agents.test_daily_pending_producer import context, prompts_for
    from unit.agents.test_monitoring_capacity import capacity_fixture

    module, session, run, *_ = setup_preview(monkeypatch)
    _, args = capacity_fixture()
    frozen = {
        **context(3),
        "purpose": "daily_monitor_context_v2",
        "materials": [{"payload": {"exact_archived_body": "x" * 120000}}],
    }
    args["inputs"]["security_snapshot"]["items"][0]["agent_paper_context"][
        "pending_monitoring_v2"
    ] = frozen
    module.load_frozen_prompts.return_value = prompts_for()
    monkeypatch.setattr(
        module.StructuredStagedCodexCommittee,
        "_inputs",
        AsyncMock(return_value=args["inputs"]),
    )
    with pytest.raises(module.DailyEventCapacityExceeded, match="^MONITOR_CONTENT_CAPACITY$"):
        await module.preview_daily_event_capacity(session, run)
