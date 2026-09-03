"""Offline SDK transport, citation, cleanup and no-retry boundary tests."""

import asyncio
import hashlib
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
    submitted_models = {}

    async def invoke(payload, *, output_model):
        submitted.append(payload)
        submitted_models[payload["stage"]] = output_model
        batch = _staged_batch(payload["stage"], [p["role"] for p in payload["prompts"]])
        if payload["stage"] == "critique":
            batch["roles"][0]["claims"][0]["text"] = "成交容量未知，不能判断退出速度。"
            batch["roles"][1]["claims"][0]["text"] = "估值时间缺失，尚不能验证可比性。"
        elif payload["stage"] == "synthesis":
            assert "first claim must assess each bear objection" in payload["instructions"]
            assert "second claim must assess each evidence_audit finding" in payload["instructions"]
            assert "Restating the orchestrator's question" in payload["instructions"]
            batch["roles"][0]["claims"] = [
                {
                    "text": "未决：bear:0缺少成交容量，需补充证据才能判断退出速度。",
                    "citations": [0],
                },
                {"text": "未决：evidence_audit:0缺少估值时间，需先核实可比性。", "citations": [0]},
            ]
        assert output_model.model_validate(batch)
        return json.dumps(batch), UsageSnapshot(input_tokens=300, output_tokens=80)

    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    model = codex_staged.StagedCodexCommittee(prompts=prompts, tools=_tools())
    outputs = [await model.analyze(role, request, evidence, prompts[role]) for role in ROLE_ORDER]
    assert [p["stage"] for p in submitted] == ["research", "critique", "synthesis"]
    assert [len(p["previous_claims"]) for p in submitted] == [0, 5, 7]
    assert {p["role"]: p["text"] for p in submitted[2]["previous_claims"][-2:]} == {
        "bear": "成交容量未知，不能判断退出速度。",
        "evidence_audit": "估值时间缺失，尚不能验证可比性。",
    }
    assert all(
        "mutually incompatible facts" in p["instructions"]
        and "Missing data, risk concerns or differing opinions alone" in p["instructions"]
        for p in submitted
    )
    assert len(outputs[-1].claims) == 2
    assert {p["claim_id"] for p in submitted[1]["previous_claims"]} == {
        f"{role}:0" for role in ROLE_ORDER[:5]
    }
    assert all(p["frozen_inputs"] == submitted[0]["frozen_inputs"] for p in submitted)
    assert len({o.frozen_inputs_hash for o in outputs}) == 1
    assert outputs[0].frozen_inputs_hash is not None
    assert len({o.stage_input_hash for o in outputs}) == 3
    for payload in submitted:
        expected_hash = codex_sdk.codex_request_hash(payload, submitted_models[payload["stage"]])
        assert all(
            o.stage_input_hash == expected_hash for o in outputs if o.stage == payload["stage"]
        )
    changed = {**submitted[1], "previous_claims": []}
    assert codex_sdk.codex_request_hash(changed, submitted_models["critique"]) != (
        outputs[5].stage_input_hash
    )
    assert [p["requested_roles"] for p in submitted] == [
        list(ROLE_ORDER[:5]),
        list(ROLE_ORDER[5:7]),
        list(ROLE_ORDER[7:]),
    ]
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
@pytest.mark.parametrize("extra_research_claim", [False, True])
async def test_submitted_stage_schema_only_allows_the_actual_prior_claim_ids(
    monkeypatch, extra_research_claim
):
    from nexus.agents import codex_staged

    request, evidence, prompts = _authority()
    checked_stages = []

    async def invoke(payload, *, output_model):
        stage = payload["stage"]
        batch = _staged_batch(stage, payload["requested_roles"])
        if stage == "research":
            if extra_research_claim:
                batch["roles"][0]["claims"].append(
                    {"text": "补充的冻结证据仍不能支持交易。", "citations": [0]}
                )
        else:
            expected_ids = {
                "orchestrator:0",
                "fundamental:0",
                "market_state:0",
                "macro_sector:0",
                "bull:0",
            }
            if extra_research_claim:
                expected_ids.add("orchestrator:1")
            if stage == "synthesis":
                expected_ids.update(("bear:0", "evidence_audit:0"))
            # Validate the actual schema handed to the SDK, not a test-only schema.
            schema = codex_sdk.pydantic_function_tool(output_model)["function"]["parameters"]
            assert any(
                set(definition.get("enum", ())) == expected_ids
                for definition in schema.get("$defs", {}).values()
            )
            invalid = json.loads(json.dumps(batch))
            invalid["roles"][0]["reviewed_claim_ids"] = (
                ["external:0"] if stage == "critique" else ["bear:0", "portfolio:0"]
            )
            with pytest.raises(ValueError):
                output_model.model_validate(invalid)
            checked_stages.append(stage)
        assert output_model.model_validate(batch)
        return json.dumps(batch), UsageSnapshot(input_tokens=300, output_tokens=80)

    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    model = codex_staged.StagedCodexCommittee(prompts=prompts, tools=_tools())
    for role in ROLE_ORDER:
        await model.analyze(role, request, evidence, prompts[role])
    assert checked_stages == ["critique", "synthesis"]
    assert model.usage.provider_calls == 3


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
    assert model.failure_reason == "model_stage_review_invalid"


@pytest.mark.asyncio
async def test_staged_roles_can_be_reordered_without_changing_identity(monkeypatch):
    from nexus.agents import codex_staged

    request, evidence, prompts = _authority()
    batch = _staged_batch("research", ROLE_ORDER[:5])
    batch["roles"].reverse()
    monkeypatch.setattr(
        codex_staged,
        "invoke_codex",
        AsyncMock(
            return_value=(json.dumps(batch), UsageSnapshot(input_tokens=300, output_tokens=80))
        ),
    )
    model = codex_staged.StagedCodexCommittee(prompts=prompts, tools=_tools())
    first = await model.analyze("orchestrator", request, evidence, prompts["orchestrator"])
    assert first.role == "orchestrator" and first.input_tokens == 300
    assert tuple(model._outputs) == ROLE_ORDER[:5]


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["schema", "duplicate", "wrong_stage", "citation"])
async def test_staged_failure_categories_are_safe_and_terminal(monkeypatch, invalid):
    from nexus.agents import codex_staged

    request, evidence, prompts = _authority()
    batch = _staged_batch("research", ROLE_ORDER[:5])
    if invalid == "schema":
        batch["secret-canary"] = "not-to-be-persisted"
    elif invalid == "duplicate":
        batch["roles"][1] = batch["roles"][0]
    elif invalid == "wrong_stage":
        batch["roles"][0]["role"] = "portfolio"
    else:
        batch["roles"][0]["claims"][0]["citations"] = [len(evidence)]
    invoke = AsyncMock(
        return_value=(json.dumps(batch), UsageSnapshot(input_tokens=300, output_tokens=80))
    )
    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    model = codex_staged.StagedCodexCommittee(prompts=prompts, tools=_tools())
    with pytest.raises(ValueError):
        await model.analyze("orchestrator", request, evidence, prompts["orchestrator"])
    expected = "roles" if invalid in {"duplicate", "wrong_stage"} else invalid
    assert model.failure_reason == f"model_stage_{expected}_invalid"
    assert not model._outputs and model.usage.input_tokens == 300
    with pytest.raises(ValueError, match="CODEX_REQUEST_ALREADY_FAILED"):
        await model.analyze("orchestrator", request, evidence, prompts["orchestrator"])
    invoke.assert_awaited_once()


def test_stage_schemas_restrict_roles_counts_and_review_shape():
    from nexus.agents.codex_staged import STAGE_MODELS, STAGES

    for stage, roles in STAGES:
        batch = _staged_batch(stage, roles)
        schema = STAGE_MODELS[stage]
        assert schema.model_validate(batch)
        batch["roles"].append(batch["roles"][0])
        with pytest.raises(ValueError):
            schema.model_validate(batch)


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
    assert model.last_stage_input.stage == "critique"


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
    assert model.failure_reason == "model_stage_allowance_exhausted"
    assert model.last_stage_input is None


@pytest.mark.asyncio
async def test_worker_allows_three_individually_bounded_stages_past_40_seconds(monkeypatch):
    from nexus.agents import codex_staged
    from nexus.worker import _agent_registry

    request, evidence, prompts = _authority()
    spec = _agent_registry(
        EngineSettings(agent_runtime_mode="codex_sdk"),
        responses_client=None,
        tools=None,
    ).specs[0]
    loop = asyncio.get_running_loop()
    started = loop.time()
    elapsed = 0.0

    async def invoke(payload, *, output_model):
        nonlocal elapsed
        elapsed += 25  # Each stage fits its 35s limit; all three need 75s.
        return json.dumps(_staged_batch(payload["stage"], payload["requested_roles"])), (
            UsageSnapshot(input_tokens=300, output_tokens=80)
        )

    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    with monkeypatch.context() as timer:
        timer.setattr(loop, "time", lambda: started + elapsed)
        model = codex_staged.StagedCodexCommittee(
            prompts=prompts,
            tools=_tools(),
            deadline=started + spec.timeout_seconds,
        )
        outputs = [
            await model.analyze(role, request, evidence, prompts[role]) for role in ROLE_ORDER
        ]
    assert len(outputs) == 8 and model.usage.provider_calls == 3
    assert elapsed == 75
    assert spec.max_attempts == 1


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


@pytest.mark.asyncio
@pytest.mark.parametrize("staged", [False, True])
@pytest.mark.parametrize("adjustment", ["raw", "all", None])
async def test_sdk_keeps_issuer_evidence_and_close_separate_from_adjusted_features(
    monkeypatch, staged, adjustment
):
    from nexus.agents import codex_staged

    request, evidence, prompts = _authority()
    security = {
        "status": "ok",
        "items": [
            {
                "source": "ssga",
                "provider_object_id": "synthetic-issuer-distribution",
                "revision": "fixture-v1",
                "available_at": request.decision_time.isoformat(),
                "content_hash": "d" * 64,
                "issuer_distribution": {
                    "dividend_per_share": "0.125000",
                    "short_term_capital_gain": None,
                    "historical_backtest_eligible": False,
                },
            }
        ],
    }
    unavailable = {"status": "unavailable", "items": [], "reason": "no_frozen_price_authority"}
    calls = []

    def price(payload):
        features = payload["features"]
        calls.append(features)
        if adjustment is None:
            return unavailable
        if adjustment == "raw" and any(feature != "close" for feature in features):
            return {**unavailable, "reason": "adjusted_price_authority_unavailable"}
        return {
            "status": "ok",
            "items": [
                {
                    "adjustment": adjustment,
                    "features": {feature: "123.456789" for feature in features},
                }
            ],
        }

    tools = ReadOnlyTools(
        executors={
            **{name: lambda payload: unavailable for name in ReadOnlyTools.names},
            "get_security_snapshot": lambda payload: security,
            "get_price_features": price,
        }
    )
    submitted = []

    async def invoke(payload, **kwargs):
        submitted.append(payload)
        batch = _staged_batch(payload["stage"], payload["requested_roles"]) if staged else _batch()
        return json.dumps(batch), UsageSnapshot(input_tokens=300, output_tokens=80)

    module = codex_staged if staged else codex_sdk
    monkeypatch.setattr(module, "invoke_codex", invoke)
    model_class = codex_staged.StagedCodexCommittee if staged else codex_sdk.CodexCommittee
    model = model_class(prompts=prompts, tools=tools)
    outputs = [await model.analyze(role, request, evidence, prompts[role]) for role in ROLE_ORDER]
    assert len(submitted) == (3 if staged else 1)
    if staged:
        fingerprint = hashlib.sha256(
            json.dumps(
                submitted[0]["frozen_inputs"],
                sort_keys=True,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        assert {output.frozen_inputs_hash for output in outputs} == {fingerprint}
    for payload in submitted:
        frozen = payload["frozen_inputs"]
        assert frozen["security_snapshot"] == security
        if adjustment is None:
            assert frozen["price_close"] == frozen["price_features"] == unavailable
        else:
            assert frozen["price_close"]["items"] == [
                {
                    "adjustment": adjustment,
                    "features": {"close": "123.456789"},
                }
            ]
            if adjustment == "raw":
                assert frozen["price_features"]["status"] == "unavailable"
                assert frozen["price_features"]["reason"] == "adjusted_price_authority_unavailable"
            else:
                assert frozen["price_features"]["status"] == "ok"
    assert calls == [("close",), ("sma_20", "sma_200", "rsi_14", "volatility_20")]


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
@pytest.mark.parametrize("invalid", ["incomplete", "tool", "size", "missing_usage"])
async def test_sdk_rejected_result_retains_available_measured_usage(monkeypatch, invalid):
    client, _, result, _ = _sdk(monkeypatch)
    result.usage.total.output_tokens = 9000
    if invalid == "incomplete":
        result.status = TurnStatus.failed
    elif invalid == "tool":
        result.items = [SimpleNamespace(root=SimpleNamespace(type="commandExecution"))]
    elif invalid == "size":
        result.final_response = "x" * (codex_sdk._MAX_OUTPUT_BYTES + 1)
    else:
        result.usage = None
    with pytest.raises(codex_sdk.CodexInvocationError) as caught:
        await codex_sdk.invoke_codex({})
    expected = (
        None
        if invalid == "missing_usage"
        else UsageSnapshot(input_tokens=300, output_tokens=9000, provider_calls=1)
    )
    assert caught.value.usage == expected
    assert str(caught.value).startswith("CODEX_")
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
async def test_sdk_submits_the_request_bound_review_schema(monkeypatch):
    from nexus.agents import codex_staged

    client, thread, _, _ = _sdk(monkeypatch)
    prior_claim_ids = ("orchestrator:0", "bull:0")
    output_model = codex_staged._stage_model_for_prior_claims("critique", prior_claim_ids)
    await codex_sdk.invoke_codex({"stage": "critique"}, output_model=output_model)
    schema = thread.run.call_args.kwargs["output_schema"]
    assert any(
        set(definition.get("enum", ())) == set(prior_claim_ids)
        for definition in schema["$defs"].values()
    )
    client.close.assert_awaited_once()


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
@pytest.mark.parametrize("external_cancel", [False, True])
async def test_timeout_wakes_sdk_notification_thread_before_unregister(
    monkeypatch, external_cancel
):
    import queue

    client, thread, _, _ = _sdk(monkeypatch)
    notifications = queue.Queue()
    registered = False
    closed_while_registered = False
    started = asyncio.Event()

    async def stream(*args, **kwargs):
        nonlocal registered
        registered = True
        started.set()
        try:
            # Mirrors SDK to_thread(queue.get); finite timeout keeps a regression safe.
            await asyncio.to_thread(notifications.get, True, 1)
            raise ConnectionError("closed")
        finally:
            registered = False

    async def close():
        nonlocal closed_while_registered
        closed_while_registered = registered
        if registered:
            notifications.put(None)

    thread.run.side_effect = stream
    client.close.side_effect = close
    monkeypatch.setattr(codex_sdk, "TIMEOUT_SECONDS", 1 if external_cancel else 0.02)
    task = asyncio.create_task(codex_sdk.invoke_codex({}))
    await started.wait()
    if external_cancel:
        task.cancel()
    with pytest.raises(asyncio.CancelledError if external_cancel else TimeoutError):
        await task
    assert closed_while_registered
    assert not registered
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


def _assessment_payload(previous, *, invalid=None):
    ids = [item["claim_id"] for item in previous if item["role"] in {"bear", "evidence_audit"}]
    reviews = [
        {
            "claim_id": claim_id,
            "status": "accepted",
            "rationale": "承认该缺口，仍需冻结证据。",
            "citations": [0],
            "resolved": False,
            "missing_evidence": ["同口径估值"],
            "prevented_conclusion": "无法完成经济比较。",
        }
        for claim_id in ids
    ]
    if invalid == "missing":
        reviews.pop()
    elif invalid == "duplicate":
        reviews[-1] = reviews[0]
    elif invalid == "foreign":
        reviews[0]["claim_id"] = "bull:0"
    elif invalid == "citation":
        reviews[0]["citations"] = [999]
    return {
        "synthesis_assessment": {
            "stance": "buy_candidate",
            "claims": [{"text": "审查具体回应已保留，现有缺口仍阻止结论。", "citations": [0]}],
            "reviews": reviews,
            "review_conclusion": "sufficient" if invalid == "sufficient" else "insufficient",
            "uncertainties": ["资料仍不充分。"],
            "conflicting_facts": False,
        }
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid", [None, "missing", "duplicate", "foreign", "citation", "sufficient"]
)
async def test_v3_final_assessment_covers_every_review_claim_and_validates_exact_schema(
    monkeypatch, invalid
):
    from nexus.agents import codex_staged

    request, evidence, prompts = _authority()
    calls = []

    async def invoke(payload, *, output_model):
        calls.append(payload["stage"])
        if payload["stage"] == "synthesis":
            result = _assessment_payload(payload["previous_claims"], invalid=invalid)
        else:
            result = _staged_batch(payload["stage"], payload["requested_roles"])
            if payload["stage"] == "critique":
                for role in result["roles"]:
                    role["claims"].append({"text": "另一项独立反证也必须回应。", "citations": [0]})
        return json.dumps(result), UsageSnapshot(input_tokens=300, output_tokens=80)

    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    model = codex_staged.StructuredStagedCodexCommittee(prompts=prompts, tools=_tools())
    for role in ROLE_ORDER[:-1]:
        await model.analyze(role, request, evidence, prompts[role])
    if invalid:
        with pytest.raises(ValueError):
            await model.analyze("portfolio", request, evidence, prompts["portfolio"])
        assert "portfolio" not in model._outputs
        assert model.failure_reason is not None
    else:
        final = await model.analyze("portfolio", request, evidence, prompts["portfolio"])
        assert final.synthesis_assessment is not None
        assert {item.claim_id for item in final.synthesis_assessment.reviews} == {
            "bear:0",
            "bear:1",
            "evidence_audit:0",
            "evidence_audit:1",
        }
        assert all(not item.resolved for item in final.synthesis_assessment.reviews)
        assert final.vote == "abstain"
    assert calls == ["research", "critique", "synthesis"]


@pytest.mark.asyncio
@pytest.mark.parametrize("runtime", ["fixture", "responses"])
async def test_sdk_v3_cannot_fall_back_to_other_runtime(monkeypatch, runtime):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from nexus import worker
    from nexus.settings import EngineSettings

    session = SimpleNamespace(
        get=AsyncMock(
            return_value=SimpleNamespace(
                version="codex-sdk-chatgpt-v3",
                model_id="gpt-5.4-mini",
            )
        )
    )
    frozen_reader = AsyncMock(side_effect=AssertionError("must reject before runtime preparation"))
    monkeypatch.setattr(worker, "load_frozen_evidence", frozen_reader)
    client = AsyncMock()
    settings = EngineSettings(
        agent_runtime_mode=runtime,
        openai_reasoning_model="gpt-5.4-mini",
        openai_extraction_model="gpt-5.4-mini",
        openai_api_key="synthetic-not-a-key",
    )
    committee = await worker._committee(
        session,
        settings,
        SimpleNamespace(model_config_id=uuid4()),
        responses_client=client,
        tools=None,
    )
    assert isinstance(committee, worker._UnavailableResponsesCommittee)
    frozen_reader.assert_not_awaited()
    assert client.mock_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("role_index", [0, 4])
async def test_stage_domain_rejection_keeps_safe_category_and_atomic_batch(monkeypatch, role_index):
    from types import SimpleNamespace

    from nexus.agents import codex_staged
    from nexus.agents.execution import SAFE_MODEL_FAILURES, _analyze_with_one_retry

    request, evidence, prompts = _authority()
    body = _staged_batch("research", ROLE_ORDER[:5])
    body["roles"][role_index]["claims"][0]["text"] = "synthetic-private-canary; blocked"
    raw = json.dumps(body)
    codex_staged._ResearchBatch.model_validate_json(raw)
    invoke = AsyncMock(return_value=(raw, UsageSnapshot(input_tokens=300, output_tokens=80)))
    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    model = codex_staged.StructuredStagedCodexCommittee(prompts=prompts, tools=_tools())
    run = SimpleNamespace(retry_count=0)
    output = await _analyze_with_one_retry(
        AsyncMock(),
        run,
        committee=model,
        role="orchestrator",
        request=request,
        evidence_refs=evidence,
        prompt=prompts["orchestrator"],
    )
    assert output is None and model._outputs == {}
    assert model.failure_reason == "model_stage_output_invalid"
    assert model.failure_reason in SAFE_MODEL_FAILURES
    assert model.usage.input_tokens == 300 and model.usage.output_tokens == 80
    assert model.last_stage_input.stage == "research"
    assert run.retry_count == 0 and invoke.await_count == 1


@pytest.mark.asyncio
async def test_unknown_conversion_bug_is_not_classified_as_model_output(monkeypatch):
    from nexus.agents import codex_staged

    request, evidence, prompts = _authority()
    raw = json.dumps(_staged_batch("research", ROLE_ORDER[:5]))
    invoke = AsyncMock(return_value=(raw, UsageSnapshot(input_tokens=300, output_tokens=80)))
    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)

    def unexpected_bug(**kwargs):
        raise RuntimeError("synthetic-program-bug")

    monkeypatch.setattr(codex_staged, "ResearchClaim", unexpected_bug)
    model = codex_staged.StructuredStagedCodexCommittee(prompts=prompts, tools=_tools())
    with pytest.raises(RuntimeError, match="synthetic-program-bug"):
        await model.analyze("orchestrator", request, evidence, prompts["orchestrator"])
    assert model.failure_reason == "model_execution_failed"
    assert model._outputs == {} and invoke.await_count == 1
