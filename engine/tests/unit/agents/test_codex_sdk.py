"""Offline SDK transport, citation, cleanup and no-retry boundary tests."""

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from nexus.agents import codex_sdk
from nexus.agents.committee import ROLE_ORDER, load_prompt_assets
from nexus.agents.execution import _analyze_with_one_retry
from nexus.agents.models import ResearchRequest
from nexus.agents.responses_budget import UsageSnapshot
from nexus.agents.tools import ReadOnlyTools
from nexus.domain.evidence import EvidenceRef
from nexus.settings import EngineSettings
from openai_codex import ApprovalMode, Sandbox
from openai_codex.generated.v2_all import ChatgptAccount, TurnStatus


def _batch(citation=0):
    return {
        "roles": [
            {
                "role": role,
                "vote": "abstain",
                "claims": [
                    {"text": "财报证据已冻结，行情不足，保持观察。", "citations": [citation]}
                ],
                "conflicting_facts": False,
            }
            for role in ROLE_ORDER
        ]
    }


def _staged_batch(stage, roles):
    reviewed = (
        []
        if stage == "research"
        else ["bull:0"]
        if stage == "critique"
        else ["bear:0", "evidence_audit:0"]
    )
    return {
        "roles": [
            {**item, "reviewed_claim_ids": reviewed}
            for item in _batch()["roles"]
            if item["role"] in roles
        ]
    }


@pytest.mark.asyncio
async def test_staged_committee_passes_validated_claims_forward_and_shares_usage(monkeypatch):
    from nexus.agents import codex_staged

    request, evidence, prompts = _authority()
    submitted = []

    async def invoke(payload, *, output_model):
        submitted.append(payload)
        batch = _staged_batch(payload["stage"], [p["role"] for p in payload["prompts"]])
        assert output_model.model_validate(batch)
        return json.dumps(batch), UsageSnapshot(input_tokens=300, output_tokens=80)

    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    model = codex_staged.StagedCodexCommittee(prompts=prompts, tools=_tools())
    outputs = [await model.analyze(role, request, evidence, prompts[role]) for role in ROLE_ORDER]
    assert [p["stage"] for p in submitted] == ["research", "critique", "synthesis"]
    assert [len(p["previous_claims"]) for p in submitted] == [0, 5, 7]
    assert {p["claim_id"] for p in submitted[1]["previous_claims"]} == {
        f"{role}:0" for role in ROLE_ORDER[:5]
    }
    assert all(p["frozen_inputs"] == submitted[0]["frozen_inputs"] for p in submitted)
    assert model.usage == UsageSnapshot(input_tokens=900, output_tokens=240, provider_calls=3)
    assert sum(o.input_tokens for o in outputs) == 900
    assert sum(o.output_tokens for o in outputs) == 240
    assert outputs[-1].reviewed_claim_ids == ("bear:0", "evidence_audit:0")
    assert outputs[-1].stage == "synthesis"
    assert all(o.claims[0].evidence_refs == evidence for o in outputs)
    assert await model.analyze("portfolio", request, evidence, prompts["portfolio"]) == outputs[-1]
    assert len(submitted) == 3
    with pytest.raises(ValueError, match="CODEX_RUN_AUTHORITY_CHANGED"):
        await model.analyze(
            "portfolio",
            request.model_copy(update={"instrument_id": uuid4()}),
            evidence,
            prompts["portfolio"],
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("reviewed", [[], ["bear:0"], ["bull:99"], ["external:0"]])
async def test_staged_critique_rejects_missing_self_or_fabricated_references(monkeypatch, reviewed):
    from nexus.agents import codex_staged

    request, evidence, prompts = _authority()
    first = _staged_batch("research", ROLE_ORDER[:5])
    second = _staged_batch("critique", ROLE_ORDER[5:7])
    second["roles"][0]["reviewed_claim_ids"] = reviewed
    invoke = AsyncMock(
        side_effect=[
            (json.dumps(first), UsageSnapshot(input_tokens=300, output_tokens=80)),
            (json.dumps(second), UsageSnapshot(input_tokens=300, output_tokens=80)),
        ]
    )
    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    model = codex_staged.StagedCodexCommittee(prompts=prompts, tools=_tools())
    await model.analyze("orchestrator", request, evidence, prompts["orchestrator"])
    with pytest.raises(ValueError, match="CODEX_REVIEW_REFERENCE_INVALID"):
        await model.analyze("bear", request, evidence, prompts["bear"])
    with pytest.raises(ValueError, match="CODEX_REQUEST_ALREADY_FAILED"):
        await model.analyze("bear", request, evidence, prompts["bear"])
    assert invoke.await_count == 2
    assert model.usage.input_tokens == 600
    assert tuple(model._outputs) == ROLE_ORDER[:5]


@pytest.mark.asyncio
async def test_staged_failure_preserves_known_usage_and_does_not_retry_uncertain_call(monkeypatch):
    from nexus.agents import codex_staged

    request, evidence, prompts = _authority()
    invoke = AsyncMock(
        side_effect=[
            (
                json.dumps(_staged_batch("research", ROLE_ORDER[:5])),
                UsageSnapshot(input_tokens=300, output_tokens=80),
            ),
            TimeoutError("secret-canary"),
        ]
    )
    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    model = codex_staged.StagedCodexCommittee(prompts=prompts, tools=_tools())
    await model.analyze("orchestrator", request, evidence, prompts["orchestrator"])
    run = SimpleNamespace(retry_count=0)
    result = await _analyze_with_one_retry(
        AsyncMock(),
        run,
        committee=model,
        role="bear",
        request=request,
        evidence_refs=evidence,
        prompt=prompts["bear"],
    )
    assert result is None and run.retry_count == 0
    assert model.failure_reason == "model_usage_uncertain"
    assert model.usage == UsageSnapshot(input_tokens=300, output_tokens=80, provider_calls=2)
    assert invoke.await_count == 2


@pytest.mark.asyncio
async def test_staged_budget_exhaustion_stops_before_next_provider_call(monkeypatch):
    from nexus.agents import codex_staged

    request, evidence, prompts = _authority()
    invoke = AsyncMock(
        return_value=(
            json.dumps(_staged_batch("research", ROLE_ORDER[:5])),
            UsageSnapshot(input_tokens=codex_sdk.MAX_INPUT_TOKENS - 1, output_tokens=80),
        )
    )
    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    model = codex_staged.StagedCodexCommittee(prompts=prompts, tools=_tools())
    with pytest.raises(ValueError, match="CODEX_STAGE_ORDER_INVALID"):
        await model.analyze("portfolio", request, evidence, prompts["portfolio"])
    invoke.assert_not_awaited()
    await model.analyze("orchestrator", request, evidence, prompts["orchestrator"])
    with pytest.raises(ValueError, match="CODEX_AGGREGATE_ALLOWANCE_EXHAUSTED"):
        await model.analyze("bear", request, evidence, prompts["bear"])
    invoke.assert_awaited_once()


@pytest.mark.asyncio
async def test_staged_deadline_leaves_worker_time_to_persist_terminal_state(monkeypatch):
    from nexus.agents import codex_staged

    request, evidence, prompts = _authority()

    async def stalled(*args, **kwargs):
        await asyncio.sleep(1)

    invoke = AsyncMock(side_effect=stalled)
    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    deadline = asyncio.get_running_loop().time() + 5.05
    model = codex_staged.StagedCodexCommittee(prompts=prompts, tools=_tools(), deadline=deadline)
    with pytest.raises(TimeoutError):
        await model.analyze("orchestrator", request, evidence, prompts["orchestrator"])
    assert asyncio.get_running_loop().time() < deadline
    assert model.failure_reason == "model_usage_uncertain"
    assert model.usage.provider_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["citation", "synthesis_review"])
async def test_staged_invalid_evidence_or_ignored_critique_cannot_complete(monkeypatch, invalid):
    from nexus.agents import codex_staged

    request, evidence, prompts = _authority()

    async def invoke(payload, **kwargs):
        batch = _staged_batch(payload["stage"], [p["role"] for p in payload["prompts"]])
        if payload["stage"] == "synthesis":
            if invalid == "citation":
                batch["roles"][0]["claims"][0]["citations"] = [len(evidence)]
            else:
                batch["roles"][0]["reviewed_claim_ids"] = ["bull:0"]
        return json.dumps(batch), UsageSnapshot(input_tokens=300, output_tokens=80)

    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    model = codex_staged.StagedCodexCommittee(prompts=prompts, tools=_tools())
    await model.analyze("orchestrator", request, evidence, prompts["orchestrator"])
    await model.analyze("bear", request, evidence, prompts["bear"])
    with pytest.raises(ValueError, match="CODEX_(CITATION|REVIEW_REFERENCE)_INVALID"):
        await model.analyze("portfolio", request, evidence, prompts["portfolio"])
    assert tuple(model._outputs) == ROLE_ORDER[:7]


def _sdk(monkeypatch):
    result = SimpleNamespace(
        status=TurnStatus.completed,
        final_response=json.dumps(_batch()),
        items=[],
        usage=SimpleNamespace(total=SimpleNamespace(input_tokens=300, output_tokens=80)),
    )
    thread = SimpleNamespace(run=AsyncMock(return_value=result))
    client = SimpleNamespace(
        _client=SimpleNamespace(
            request=AsyncMock(
                return_value=SimpleNamespace(
                    config=SimpleNamespace(model_extra={"mcp_servers": {"user-server": {}}})
                )
            )
        ),
        account=AsyncMock(
            return_value=SimpleNamespace(
                account=SimpleNamespace(root=ChatgptAccount.model_construct())
            )
        ),
        thread_start=AsyncMock(return_value=thread),
        close=AsyncMock(),
    )
    configs = []

    def create(config):
        configs.append(config)
        return client

    monkeypatch.setattr(codex_sdk, "AsyncCodex", create)
    return client, thread, result, configs


def _authority():
    request = ResearchRequest(
        instrument_id=uuid4(),
        data_snapshot_id=uuid4(),
        decision_time=datetime(2026, 9, 2, tzinfo=UTC),
    )
    evidence = (
        EvidenceRef(
            source="sec",
            object_id="filing:test",
            available_at=request.decision_time,
            location="s3://test/filing",
            content_hash="a" * 64,
        ),
    )
    return request, evidence, load_prompt_assets()


def _tools():
    return ReadOnlyTools(
        executors={
            name: lambda payload: {"status": "unavailable", "items": []}
            for name in ReadOnlyTools.names
        }
    )


def test_child_environment_excludes_secrets_and_parent_app_ipc(monkeypatch):
    for key in (
        "OPENAI_API_KEY",
        "CODEX_API_KEY",
        "DATABASE_URL",
        "FOSUN_PASSWORD",
        "CODEX_THREAD_ID",
        "CODEX_APP_SERVER_SOCKET",
        "PYTHONPATH",
    ):
        monkeypatch.setenv(key, "secret-canary")
    assert "secret-canary" not in json.dumps(codex_sdk.runtime_environment())
    command = codex_sdk.sdk_config(Path("/tmp/isolated")).launch_args_override
    assert command[:2] == ("/usr/bin/env", "-i")
    assert "secret-canary" not in " ".join(command)
    assert 'forced_login_method="chatgpt"' in command
    assert "features.shell_tool=false" in command
    assert "features.plugins=false" in command
    assert "mcp_servers={}" in command
    assert "model_providers.nexus_openai.stream_max_retries=0" in command
    assert "model_providers.nexus_openai.request_max_retries=0" in command
    assert "model_providers.nexus_openai.requires_openai_auth=true" in command
    assert "--dangerously-bypass-approvals-and-sandbox" not in command


def test_subscription_settings_do_not_require_key_and_cannot_start_providers(tmp_path, monkeypatch):
    from nexus.ops.openai_local import local_settings

    monkeypatch.setenv("OPENAI_API_KEY", "invalid key must be ignored")
    settings = local_settings(tmp_path, backend="codex_sdk")
    assert settings.openai_api_key is None
    assert settings.agent_runtime_mode == "codex_sdk"
    assert settings.provider_jobs_enabled is False
    with pytest.raises(ValueError):
        EngineSettings(agent_runtime_mode="codex_sdk", provider_jobs_enabled=True)
    with pytest.raises(ValueError):
        EngineSettings(agent_runtime_mode="codex_sdk", environment="production")


def test_local_command_defaults_to_sdk_without_prompting_for_a_key(monkeypatch, capsys):
    from nexus.ops.openai_local import main

    check = AsyncMock(return_value={"provider": "codex_sdk", "ready_for_model_requests": True})
    monkeypatch.setattr(codex_sdk, "sdk_status", check)
    monkeypatch.setenv("OPENAI_API_KEY", "invalid whitespace API key must be ignored")
    monkeypatch.setattr("sys.argv", ["nexus-openai-local", "status"])
    assert main() == 0
    assert json.loads(capsys.readouterr().out)["provider"] == "codex_sdk"
    check.assert_awaited_once()


@pytest.mark.asyncio
async def test_invalid_usage_is_not_zero_cost_success(monkeypatch):
    client, _, result, _ = _sdk(monkeypatch)
    result.usage.total.input_tokens = -1
    with pytest.raises(ValueError, match="CODEX_USAGE_INVALID"):
        await codex_sdk.invoke_codex({})
    client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_eight_roles_share_one_call_and_bind_real_citation_authority(monkeypatch):
    request, evidence, prompts = _authority()
    invoke = AsyncMock(
        return_value=(
            json.dumps(_batch()),
            UsageSnapshot(input_tokens=300, output_tokens=80, provider_calls=1),
        )
    )
    monkeypatch.setattr(codex_sdk, "invoke_codex", invoke)
    committee = codex_sdk.CodexCommittee(prompts=prompts, tools=_tools())
    outputs = [
        await committee.analyze(role, request, evidence, prompts[role]) for role in ROLE_ORDER
    ]
    assert invoke.await_count == 1
    assert sum(item.input_tokens for item in outputs) == 300
    assert sum(item.output_tokens for item in outputs) == 80
    assert all(item.claims[0].evidence_refs == evidence for item in outputs)
    assert all(item.prompt_hash == prompts[item.role].content_hash for item in outputs)
    other_request = request.model_copy(update={"instrument_id": uuid4()})
    with pytest.raises(ValueError, match="CODEX_RUN_AUTHORITY_CHANGED"):
        await committee.analyze("bear", other_request, evidence, prompts["bear"])
    assert invoke.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("citation", [-1, 100])
async def test_out_of_manifest_citations_reject_without_retry(monkeypatch, citation):
    request, evidence, prompts = _authority()
    invoke = AsyncMock(
        return_value=(
            json.dumps(_batch(citation)),
            UsageSnapshot(input_tokens=300, output_tokens=80),
        )
    )
    monkeypatch.setattr(codex_sdk, "invoke_codex", invoke)
    committee = codex_sdk.CodexCommittee(prompts=prompts, tools=_tools())
    run = type("Run", (), {"retry_count": 0})()
    output = await _analyze_with_one_retry(
        AsyncMock(),
        run,
        committee=committee,
        role="orchestrator",
        request=request,
        evidence_refs=evidence,
        prompt=prompts["orchestrator"],
    )
    assert output is None and run.retry_count == 0 and invoke.await_count == 1
    assert committee.usage.input_tokens == 300
    with pytest.raises(ValueError, match="CODEX_REQUEST_ALREADY_FAILED"):
        await committee.analyze("orchestrator", request, evidence, prompts["orchestrator"])
    assert invoke.await_count == 1


@pytest.mark.asyncio
async def test_sdk_uses_ephemeral_read_only_thread_schema_and_cleanup(monkeypatch):
    client, thread, _, configs = _sdk(monkeypatch)
    raw, usage = await codex_sdk.invoke_codex({"input": "canary"})
    assert len(codex_sdk._Batch.model_validate_json(raw).roles) == 8
    assert usage.input_tokens == 300 and usage.provider_calls == 1
    options = client.thread_start.call_args.kwargs
    assert options["ephemeral"] is True
    assert options["approval_mode"] is ApprovalMode.deny_all
    assert options["sandbox"] is Sandbox.read_only
    assert options["config"] == {"mcp_servers": {"user-server": {"enabled": False}}}
    assert "canary" in thread.run.call_args.args[0]
    assert thread.run.call_args.kwargs["output_schema"]["additionalProperties"] is False
    assert thread.run.call_args.kwargs["approval_mode"] is ApprovalMode.deny_all
    client.close.assert_awaited_once()
    assert not Path(configs[0].cwd).exists()


@pytest.mark.asyncio
async def test_timeout_closes_sdk_without_retry(monkeypatch):
    client, thread, _, _ = _sdk(monkeypatch)

    async def stall(*args, **kwargs):
        await asyncio.sleep(10)

    thread.run.side_effect = stall
    monkeypatch.setattr(codex_sdk, "TIMEOUT_SECONDS", 0.01)
    with pytest.raises(TimeoutError):
        await codex_sdk.invoke_codex({})
    client.close.assert_awaited_once()
    thread.run.assert_awaited_once()


@pytest.mark.asyncio
async def test_unexpected_tool_event_fails_closed(monkeypatch):
    _, _, result, _ = _sdk(monkeypatch)
    result.items = [SimpleNamespace(root=SimpleNamespace(type="commandExecution"))]
    with pytest.raises(ValueError, match="CODEX_UNEXPECTED_TOOL"):
        await codex_sdk.invoke_codex({})


@pytest.mark.asyncio
async def test_missing_login_never_starts_model_and_startup_failure_closes(monkeypatch):
    client, thread, _, _ = _sdk(monkeypatch)
    client.account.return_value.account = None
    with pytest.raises(ValueError, match="CODEX_CHATGPT_LOGIN_REQUIRED"):
        await codex_sdk.invoke_codex({})
    thread.run.assert_not_awaited()
    client.close.assert_awaited_once()
    client.close.reset_mock()
    client.account.side_effect = RuntimeError("startup failure")
    with pytest.raises(RuntimeError, match="startup failure"):
        await codex_sdk.invoke_codex({})
    client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_late_invalid_citation_does_not_publish_partial_batch(monkeypatch):
    request, evidence, prompts = _authority()
    batch = _batch()
    batch["roles"][-1]["claims"][0]["citations"] = [100]
    invoke = AsyncMock(return_value=(json.dumps(batch), UsageSnapshot(input_tokens=300)))
    monkeypatch.setattr(codex_sdk, "invoke_codex", invoke)
    committee = codex_sdk.CodexCommittee(prompts=prompts, tools=_tools())
    with pytest.raises(ValueError, match="CODEX_CITATION_INVALID"):
        await committee.analyze("orchestrator", request, evidence, prompts["orchestrator"])
    with pytest.raises(ValueError, match="CODEX_REQUEST_ALREADY_FAILED"):
        await committee.analyze("orchestrator", request, evidence, prompts["orchestrator"])
    invoke.assert_awaited_once()
