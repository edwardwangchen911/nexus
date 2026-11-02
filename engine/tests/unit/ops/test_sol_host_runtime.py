"""Static native host readiness never probes a provider or invokes SDK login."""

from uuid import UUID

import pytest
from nexus.ops import agent_paper_loop as loop
from nexus.settings import AgentRuntimeMode
from unit.ops.test_agent_paper_loop import NOW, service_ready_environment


def environment():
    return service_ready_environment() | {
        "AGENT_RUNTIME_MODE": "responses",
        "OPENAI_API_KEY": "sk-proj-" + "synthetic-only" * 5,
        "OPENAI_EXTRACTION_MODEL": "gpt-6.1-sol",
        "OPENAI_REASONING_MODEL": "gpt-6.1-sol",
        "FRED_API_KEY": "synthetic",
        "NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID": str(UUID(int=3)),
        "NEXUS_AGENT_PAPER_MANUAL_ENABLED": "true",
        "NEXUS_RELEASE_REVISION": "a" * 40,
    }


def test_exact_sol_host_readiness_and_identity_without_sdk_or_network(monkeypatch, tmp_path):
    path = tmp_path / ".secrets/nexus-agent-paper"
    path.mkdir(parents=True, mode=0o700)
    monkeypatch.setattr(loop, "load_schwab_app_config", lambda *_: object())
    monkeypatch.setattr(loop, "load_schwab_token", lambda *_: object())
    monkeypatch.setattr(loop, "require_live_schwab_access_token", lambda *a, **kw: "synthetic")
    monkeypatch.setattr(loop, "_schwab_token_store_writable", lambda *_: True)

    def forbidden():
        raise AssertionError("SDK login must not gate explicit native Sol")

    monkeypatch.setattr(loop, "_codex_sdk_preflight_blocker", forbidden)
    env = environment()
    for result in (
        loop.agent_paper_service_startup_preflight(env, repo_root=tmp_path),
        loop.agent_paper_service_preflight(env, repo_root=tmp_path, now=NOW),
    ):
        assert result["status"] == "ready" and result["execution_locked"]
        assert result["external_requests"] == 0
    settings = loop._FileEngineSettings.model_validate(env)
    assert settings.agent_runtime_mode is AgentRuntimeMode.RESPONSES
    worker = f"agent-paper-loop-{UUID(int=4)}"
    ident = loop.agent_paper_host_identity(settings, worker_id=worker, account_id=UUID(int=1))
    assert ":manual:" in ident[0]
    assert ident[1] == "nexus-manual-paper-v1+" + "a" * 40
    legacy = settings.model_copy(update={"agent_runtime_mode": AgentRuntimeMode.CODEX_SDK})
    assert (
        loop.agent_paper_host_identity(legacy, worker_id=worker, account_id=UUID(int=1))[0]
        != ident[0]
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("OPENAI_REASONING_MODEL", "other"),
        ("OPENAI_EXTRACTION_MODEL", "other"),
        ("OPENAI_API_KEY", ""),
    ],
)
def test_native_static_readiness_rejects_incomplete_route(tmp_path, field, value):
    path = tmp_path / ".secrets/nexus-agent-paper"
    path.mkdir(parents=True, mode=0o700)
    result = loop.agent_paper_service_startup_preflight(
        environment() | {field: value}, repo_root=tmp_path
    )
    assert result["status"] == "blocked" and result["external_requests"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("explicit_file", [False, True])
async def test_formal_loop_dispatch_preserves_explicit_native_key_without_sdk_probe(
    monkeypatch, tmp_path, explicit_file
):
    from unit.ops.test_agent_paper_loop import snapshot

    env = environment()
    settings = loop._FileEngineSettings.model_validate(env)
    calls = []

    async def resolve(*a, **kw):
        return True

    async def forbidden():
        raise AssertionError("native research must not probe SDK login")

    async def execute(loaded, **kwargs):
        assert loaded.agent_runtime_mode is AgentRuntimeMode.RESPONSES
        assert loaded.openai_api_key == settings.openai_api_key
        assert kwargs["formal_stage_capacity"] is True
        calls.append(kwargs["job_id"])
        return True

    monkeypatch.setattr(loop, "resolve_formal_host_cycle_profile", resolve)
    monkeypatch.setattr(loop, "_codex_sdk_preflight_blocker_async", forbidden)
    monkeypatch.setattr(loop, "run_cycle_job_once", execute)
    monkeypatch.setattr(loop, "load_agent_paper_service_environment", lambda *a, **kw: env)
    result = await loop.execute_agent_paper_loop_action(
        settings,
        account_id=UUID(int=1),
        repo_root=tmp_path,
        env_file=tmp_path / "local.env" if explicit_file else None,
        action=loop.AgentPaperLoopAction.RUN_RESEARCH_JOB,
        snapshot=snapshot(
            cycle_id=UUID(int=31),
            cycle_status="researching",
            research_job_id=UUID(int=41),
            research_job_status="queued",
        ),
    )
    assert result is None and calls == [UUID(int=41)]


@pytest.mark.asyncio
async def test_untyped_error_matching_native_refusal_is_not_zero_call_authority(
    monkeypatch, tmp_path
):
    from unit.ops.test_agent_paper_loop import snapshot

    env = environment()
    settings = loop._FileEngineSettings.model_validate(env)

    async def resolve(*args, **kwargs):
        return True

    async def unclassified(*args, **kwargs):
        raise ValueError("PLATFORM_HTTP_CREDENTIAL_CHANGED")

    async def forbidden(*args, **kwargs):
        raise AssertionError("unclassified error closed the pool")

    monkeypatch.setattr(loop, "resolve_formal_host_cycle_profile", resolve)
    monkeypatch.setattr(loop, "run_cycle_job_once", unclassified)
    monkeypatch.setattr(loop, "close_capacity_rejected_cycle_once", forbidden)
    with pytest.raises(ValueError, match="PLATFORM_HTTP_CREDENTIAL_CHANGED"):
        await loop.execute_agent_paper_loop_action(
            settings,
            account_id=UUID(int=1),
            repo_root=tmp_path,
            action=loop.AgentPaperLoopAction.RUN_RESEARCH_JOB,
            snapshot=snapshot(
                cycle_id=UUID(int=31),
                cycle_status="researching",
                research_job_id=UUID(int=41),
                research_job_status="queued",
            ),
        )
