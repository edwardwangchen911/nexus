"""Offline SDK transport, citation, cleanup and no-retry boundary tests."""

import asyncio
import hashlib
import json
from copy import deepcopy
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


def _restore_view(value):
    """Test-only inverse of the documented model-facing JSON view."""
    if isinstance(value, list):
        return [_restore_view(item) for item in value]
    if not isinstance(value, dict):
        return value
    if "$nexus_view" in value:
        common = _restore_view(value["common_scope"])
        return {name: {**common, **_restore_view(tool)} for name, tool in value["tools"].items()}
    if "$nexus_columns" in value:
        return [
            dict(zip(value["$nexus_columns"], [_restore_view(cell) for cell in row], strict=True))
            for row in value["rows"]
        ]
    return {key: _restore_view(item) for key, item in value.items()}


def _view_payload():
    scope = {
        "schema_version": "agent_read_only_tool_result_v1",
        "instrument_id": "synthetic-instrument",
        "data_snapshot_id": "synthetic-snapshot",
        "as_of": "2026-09-04T00:00:00Z",
        "evidence_hash": "a" * 64,
        "portfolio_scope": {"portfolio_snapshot_id": "synthetic-p", "diagnostic_run_id": None},
    }
    rows = [
        {
            "exact_decimal_string": "0.00000000000001",
            "unknown": None,
            "boolean": False,
            "count": index,
            "text": "中文证据",
            "nested": [{"metric": "x", "value": 0}] * 10,
        }
        for index in range(20)
    ]
    return {
        "instructions": "Frozen evidence only.",
        "request": {"id": "synthetic"},
        "frozen_inputs": {
            "security": {**scope, "status": "ok", "reason": None, "items": rows},
            "macro": {**scope, "status": "unavailable", "reason": "missing", "items": []},
        },
    }


def test_staged_input_view_is_lossless_and_smaller_including_format_note():
    from nexus.agents.codex_input_view import project_staged_payload

    payload = _view_payload()
    original = deepcopy(payload)
    view = project_staged_payload(payload)
    assert view is not payload and payload == original
    assert len(codex_sdk.codex_request_wire(view).encode()) < len(
        codex_sdk.codex_request_wire(payload).encode()
    )
    assert "$nexus_columns" in view["instructions"] and "common_scope" in view["instructions"]
    assert view["instructions"].startswith(original["instructions"])
    assert json.dumps(_restore_view(view["frozen_inputs"]), sort_keys=True) == json.dumps(
        original["frozen_inputs"], sort_keys=True
    )
    assert view["request"] == original["request"]
    assert set(view["frozen_inputs"]["common_scope"]) == {
        "schema_version",
        "instrument_id",
        "data_snapshot_id",
        "as_of",
        "evidence_hash",
        "portfolio_scope",
    }
    encoded_rows = view["frozen_inputs"]["tools"]["security"]["items"]
    assert encoded_rows["$nexus_columns"] == list(original["frozen_inputs"]["security"]["items"][0])
    assert _restore_view(encoded_rows)[-1]["count"] == 19


@pytest.mark.parametrize("left,right", [(True, 1), (1, 1.0), (None, "missing")])
def test_staged_input_view_does_not_hoist_different_json_types(left, right):
    from nexus.agents.codex_input_view import project_staged_payload

    payload = _view_payload()
    payload["frozen_inputs"]["security"]["instrument_id"] = left
    if right == "missing":
        del payload["frozen_inputs"]["macro"]["instrument_id"]
    else:
        payload["frozen_inputs"]["macro"]["instrument_id"] = right
    view = project_staged_payload(payload)
    assert "instrument_id" not in view["frozen_inputs"]["common_scope"]
    assert json.dumps(_restore_view(view["frozen_inputs"]), sort_keys=True) == json.dumps(
        payload["frozen_inputs"], sort_keys=True
    )


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [{"a": 1}],
        [{"a": 1}, {"b": 2}],
        [{"a": 1, "b": 2}, {"b": 3, "a": 4}],
        [None, False, 0, "中文"],
    ],
)
def test_staged_input_view_preserves_non_columnar_lists(rows):
    from nexus.agents.codex_input_view import project_staged_payload

    payload = _view_payload()
    payload["frozen_inputs"]["security"]["other"] = rows
    view = project_staged_payload(payload)
    assert view["frozen_inputs"]["tools"]["security"]["other"] == rows


@pytest.mark.parametrize("marker", ["$nexus_view", "$nexus_columns"])
def test_staged_input_view_marker_collision_falls_back_to_complete_input(marker):
    from nexus.agents.codex_input_view import project_staged_payload

    payload = _view_payload()
    payload["frozen_inputs"]["security"]["items"][0]["nested"].append({marker: 1})
    before = deepcopy(payload)
    assert project_staged_payload(payload) is payload
    assert payload == before


def test_staged_input_view_without_net_savings_keeps_original_payload():
    from nexus.agents.codex_input_view import project_staged_payload

    payload = {"instructions": "Frozen only", "frozen_inputs": {"tool": {"items": [{"x": 0}]}}}
    assert project_staged_payload(payload) is payload


def test_staged_input_view_counts_format_note_before_accepting_savings():
    from nexus.agents.codex_input_view import project_staged_payload

    keys = ["exact_source_financial_metric_name", "exact_source_financial_metric_value"]
    rows = [dict(zip(keys, ["synthetic", 0], strict=True)) for _ in range(4)]
    payload = {"instructions": "Frozen only", "frozen_inputs": {"tool": {"items": rows}}}
    without_note = {
        **payload,
        "frozen_inputs": {
            "$nexus_view": 1,
            "common_scope": {},
            "tools": {
                "tool": {
                    "items": {
                        "$nexus_columns": keys,
                        "rows": [["synthetic", 0]] * 4,
                    }
                }
            },
        },
    }
    assert len(codex_sdk.codex_request_wire(without_note).encode()) < len(
        codex_sdk.codex_request_wire(payload).encode()
    )
    assert project_staged_payload(payload) is payload


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
    assert {p["role"]: (p["vote"], p["text"]) for p in submitted[2]["previous_claims"][-2:]} == {
        "bear": ("abstain", "成交容量未知，不能判断退出速度。"),
        "evidence_audit": ("abstain", "估值时间缺失，尚不能验证可比性。"),
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
    assert model.failure_diagnostic.invocation_phase == "aggregate_wait"
    assert model.failure_diagnostic.invocation_kind == "timeout"
    assert model.failure_diagnostic.response_hash is None


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
@pytest.mark.parametrize("over_budget", [0, 1])
async def test_staged_budget_uses_actual_compact_utf8_wire(monkeypatch, tmp_path, over_budget):
    from nexus.agents import codex_staged, codex_token_budget

    monkeypatch.setattr(codex_token_budget, "tokenizer_cache_path", lambda: tmp_path / "missing")

    request, evidence, prompts = _authority()
    client, thread, result, _ = _sdk(monkeypatch)
    frozen = {"status": "available", "items": [{"text": "合成资料"}] * 1000}
    tools = ReadOnlyTools(
        executors={
            name: (lambda payload: frozen)
            if name == "get_security_snapshot"
            else (lambda payload: {"status": "unavailable", "items": []})
            for name in ReadOnlyTools.names
        }
    )
    sent = []
    first_stage_usage = 0

    async def run(wire, **options):
        sent.append(wire)
        payload = json.loads(wire)
        result.final_response = json.dumps(
            _staged_batch(payload["stage"], payload["requested_roles"])
        )
        result.usage.total.input_tokens = first_stage_usage if payload["stage"] == "research" else 0
        return result

    thread.run.side_effect = run
    # Capture the real invoke_codex wire with a zero-usage synthetic transport.
    # The boundary case below changes only reported usage, never frozen facts.
    probe = codex_staged.StagedCodexCommittee(prompts=prompts, tools=tools)
    await probe.analyze("orchestrator", request, evidence, prompts["orchestrator"])
    expected = await probe.analyze("bear", request, evidence, prompts["bear"])
    expected_wire = sent[-1]
    wire_bytes = len(expected_wire.encode("utf-8"))
    assert wire_bytes > len(expected_wire)
    assert len(json.dumps(json.loads(expected_wire), ensure_ascii=False).encode()) > wire_bytes
    strict_schema_bytes = len(
        json.dumps(
            thread.run.call_args.kwargs["output_schema"], ensure_ascii=False, separators=(",", ":")
        ).encode()
    )
    instructions_bytes = sum(
        len(client.thread_start.call_args.kwargs[name].encode())
        for name in ("base_instructions", "developer_instructions")
    )
    first_stage_usage = (
        64000 - 8192 - wire_bytes - strict_schema_bytes - instructions_bytes + over_budget
    )
    assert first_stage_usage > 0
    sent.clear()

    model = codex_staged.StagedCodexCommittee(prompts=prompts, tools=tools)
    await model.analyze("orchestrator", request, evidence, prompts["orchestrator"])
    if over_budget:
        with pytest.raises(ValueError, match="CODEX_AGGREGATE_ALLOWANCE_EXHAUSTED"):
            await model.analyze("bear", request, evidence, prompts["bear"])
        assert len(sent) == 1 and model.usage.provider_calls == 1
        assert tuple(model._outputs) == ROLE_ORDER[:5]
    else:
        output = await model.analyze("bear", request, evidence, prompts["bear"])
        assert sent[-1] == expected_wire
        assert output.stage_input_hash == expected.stage_input_hash
        assert output.frozen_inputs_hash == expected.frozen_inputs_hash
        assert _restore_view(json.loads(sent[-1])["frozen_inputs"])["security_snapshot"] == frozen
        assert model.usage.input_tokens == first_stage_usage
        assert model.usage.provider_calls == 2


@pytest.mark.asyncio
async def test_sdk_keeps_compact_unicode_wire_and_request_hash(monkeypatch):
    _, thread, _, _ = _sdk(monkeypatch)
    payload = {"title": "合成 中文", "facts": [0, None, '引号"与换行\n']}
    before = codex_sdk.codex_request_hash(payload, codex_sdk._Batch)
    await codex_sdk.invoke_codex(payload)
    assert thread.run.call_args.args[0] == (
        '{"title":"合成 中文","facts":[0,null,"引号\\"与换行\\n"]}'
    )
    assert codex_sdk.codex_request_hash(payload, codex_sdk._Batch) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [1, 2, 3])
@pytest.mark.parametrize("raw_semantics", [False, True])
async def test_staged_view_reaches_real_sdk_wire_without_mutating_frozen_inputs(
    monkeypatch, version, raw_semantics
):
    from nexus.agents import codex_staged

    request, evidence, prompts = _authority()
    wrappers = _view_payload()["frozen_inputs"]
    wrappers["financial_facts"] = {
        **wrappers["macro"],
        "status": "ok",
        "reason": None,
        "items": [
            {
                "evidence_ref_id": "synthetic-financial-evidence",
                "fact": "revenue",
                "value": value,
                "unit": "USD",
                "period_start": start,
                "period_end": end,
                "period_kind": "duration",
                "accepted_at": "2025-08-01T00:00:00+00:00",
                "revision": "v1",
            }
            for value, start, end in [
                ("90.125", "2025-01-01", "2025-03-31"),
                ("100", "2025-04-01", "2025-06-30"),
            ]
        ],
    }
    if raw_semantics:
        for index, row in enumerate(wrappers["financial_facts"]["items"]):
            row.update(
                raw_taxonomy="us-gaap",
                raw_tag=(
                    "Revenues"
                    if index == 0
                    else "RevenueFromContractWithCustomerExcludingAssessedTax"
                ),
            )
    for wrapper in wrappers.values():
        wrapper["portfolio_scope"] = None
    original = deepcopy(wrappers)
    tools = ReadOnlyTools(
        executors={
            name: (lambda payload: wrappers["security"])
            if name == "get_security_snapshot"
            else (lambda payload: wrappers["financial_facts"])
            if name == "get_financial_facts"
            else (lambda payload: wrappers["macro"])
            for name in ReadOnlyTools.names
        }
    )
    _, thread, result, _ = _sdk(monkeypatch)
    sent = []

    async def run(wire, **options):
        payload = json.loads(wire)
        sent.append((payload, options["output_schema"]))
        stage = payload.get("stage")
        batch = (
            _batch()
            if stage is None
            else (
                _assessment_payload(payload["previous_claims"])
                if version == 3 and stage == "synthesis"
                else _staged_batch(stage, payload["requested_roles"])
            )
        )
        result.final_response = json.dumps(batch)
        return result

    thread.run.side_effect = run
    committee_type = {
        1: codex_sdk.CodexCommittee,
        2: codex_staged.StagedCodexCommittee,
        3: codex_staged.StructuredStagedCodexCommittee,
    }[version]
    model = committee_type(prompts=prompts, tools=tools)
    outputs = [await model.analyze(role, request, evidence, prompts[role]) for role in ROLE_ORDER]
    assert len(outputs) == 8 and wrappers == original
    for payload, _ in sent:
        financial = _restore_view(payload["frozen_inputs"])["financial_facts"]
        assert financial == original["financial_facts"]
        rows = financial["items"]
        assert len(rows) == 2
        if raw_semantics:
            assert [(row["raw_taxonomy"], row["raw_tag"]) for row in rows] == [
                ("us-gaap", "Revenues"),
                ("us-gaap", "RevenueFromContractWithCustomerExcludingAssessedTax"),
            ]
        else:
            assert all("raw_taxonomy" not in row and "raw_tag" not in row for row in rows)
    if version == 1:
        assert len(sent) == 1 and "$nexus_view" not in sent[0][0]["frozen_inputs"]
        assert sent[0][0]["frozen_inputs"]["security_snapshot"] == original["security"]
    else:
        assert len(sent) == 3
        original_hash = hashlib.sha256(
            json.dumps(
                model._frozen_inputs, sort_keys=True, ensure_ascii=False, separators=(",", ":")
            ).encode()
        ).hexdigest()
        for payload, schema in sent:
            assert payload["frozen_inputs"]["$nexus_view"] == 1
            assert _restore_view(payload["frozen_inputs"]) == model._frozen_inputs
            actual_hash = hashlib.sha256(
                json.dumps(
                    {"payload": payload, "output_schema": schema},
                    sort_keys=True,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()
            assert all(
                output.stage_input_hash == actual_hash
                for output in outputs
                if output.stage == payload["stage"]
            )
        assert all(output.frozen_inputs_hash == original_hash for output in outputs)


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [1, 2, 3])
async def test_sdk_wire_preserves_current_trend_scope_separately_from_raw_close(
    monkeypatch, version
):
    """Catch lost source/coverage/null/qualification fields at the real SDK wire.

    Tool responses are synthetic boundary fixtures, not a DB/import acceptance.
    Only the SDK client is replaced; prefetch, projection and all stages are real.
    """
    from nexus.agents import codex_staged

    request, evidence, prompts = _authority()
    security = _view_payload()["frozen_inputs"]["security"]
    scope = {
        "schema_version": "agent_read_only_tool_result_v1",
        "instrument_id": str(request.instrument_id),
        "data_snapshot_id": str(request.data_snapshot_id),
        "as_of": request.decision_time.isoformat(),
        "evidence_hash": "a" * 64,
        "portfolio_scope": None,
    }
    security.update(scope)
    unavailable = {**scope, "status": "unavailable", "reason": "missing", "items": []}
    raw_close = {
        **scope,
        "status": "ok",
        "reason": None,
        "items": [
            {
                "evidence_ref_id": "00000000-0000-4000-8000-000000000001",
                "source": "alpaca",
                "feed": "sip",
                "adjustment": "raw",
                "as_of_session": "2026-08-31",
                "features": {"close": "246.913578"},
            }
        ],
    }
    split_trend = {
        **scope,
        "status": "partial",
        "reason": "insufficient_price_history",
        "items": [
            {
                "evidence_ref_id": "00000000-0000-4000-8000-000000000002",
                "source": "alpaca_current_trends",
                "feed": "sip",
                "adjustment": "split",
                "currency": "USD",
                "timeframe": "1Day",
                "identity_asof": "2026-09-01",
                "as_of_session": "2026-08-31",
                "last_observed_on": "2026-08-31",
                "received_at": "2026-09-01T12:00:00Z",
                "available_at": "2026-09-01T12:01:00Z",
                "coverage": {
                    "observed_sessions": 21,
                    "expected_sessions": 22,
                    "contiguous_tail_sessions": 21,
                    "missing_sessions": ["2026-07-31"],
                },
                "features": {
                    "sma_20": "123.456789",
                    "sma_200": None,
                    "rsi_14": "0",
                    "volatility_20": "0.000000000001",
                },
                "missing_features": ["sma_200"],
                "historical_backtest_eligible": False,
                "market_price_eligible": False,
                "total_return_eligible": False,
                "execution_locked": True,
            }
        ],
    }
    expected_inputs = deepcopy(
        {
            "security_snapshot": security,
            "filing_disclosures": unavailable,
            "financial_facts": unavailable,
            "trailing_valuation": {
                "schema_version": "agent_trailing_valuation_v1",
                "status": "unavailable",
                "reason": "decision_quote_unavailable",
            },
            "price_close": raw_close,
            "price_features": split_trend,
            "macro": unavailable,
        }
    )
    expected_hash = hashlib.sha256(
        json.dumps(
            expected_inputs, sort_keys=True, ensure_ascii=False, separators=(",", ":")
        ).encode()
    ).hexdigest()
    price_calls = []

    def price(payload):
        price_calls.append(payload["features"])
        return raw_close if payload["features"] == ("close",) else split_trend

    tools = ReadOnlyTools(
        executors={
            **{name: lambda payload: unavailable for name in ReadOnlyTools.names},
            "get_security_snapshot": lambda payload: security,
            "get_price_features": price,
        }
    )
    _, thread, result, _ = _sdk(monkeypatch)
    sent = []

    async def run(wire, **options):
        payload = json.loads(wire)
        sent.append((payload, options["output_schema"]))
        stage = payload.get("stage")
        batch = (
            _batch()
            if stage is None
            else (
                _assessment_payload(payload["previous_claims"])
                if version == 3 and stage == "synthesis"
                else _staged_batch(stage, payload["requested_roles"])
            )
        )
        result.final_response = json.dumps(batch)
        return result

    thread.run.side_effect = run
    committee = {
        1: codex_sdk.CodexCommittee,
        2: codex_staged.StagedCodexCommittee,
        3: codex_staged.StructuredStagedCodexCommittee,
    }[version](prompts=prompts, tools=tools)
    outputs = [
        await committee.analyze(role, request, evidence, prompts[role]) for role in ROLE_ORDER
    ]
    assert [payload.get("stage") for payload, _ in sent] == (
        [None] if version == 1 else ["research", "critique", "synthesis"]
    )
    assert price_calls == [("close",), ("sma_20", "sma_200", "rsi_14", "volatility_20")]
    assert raw_close == expected_inputs["price_close"]
    assert split_trend == expected_inputs["price_features"]
    for payload, schema in sent:
        actual = _restore_view(payload["frozen_inputs"])
        assert actual == expected_inputs
        if version > 1:
            assert payload["frozen_inputs"]["$nexus_view"] == 1
            wire_hash = hashlib.sha256(
                json.dumps(
                    {"payload": payload, "output_schema": schema},
                    sort_keys=True,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()
            assert {
                output.stage_input_hash for output in outputs if output.stage == payload["stage"]
            } == {wire_hash}
    if version > 1:
        assert {output.frozen_inputs_hash for output in outputs} == {expected_hash}


@pytest.mark.asyncio
async def test_sdk_derives_valuation_from_the_same_frozen_tool_results(monkeypatch):
    request, _, prompts = _authority()
    security = {"status": "ok", "items": [{"source": "paper-context"}]}
    financial = {"status": "ok", "items": [{"fact": "diluted_eps"}]}
    unavailable = {"status": "unavailable", "reason": "missing", "items": []}
    derived = {
        "schema_version": "agent_trailing_valuation_v1",
        "status": "available",
        "reason": None,
    }
    seen = []

    def derive(security_input, financial_input):
        seen.append((security_input, financial_input))
        return derived

    monkeypatch.setattr(codex_sdk, "derive_trailing_valuation", derive)
    tools = ReadOnlyTools(
        executors={
            **{name: lambda payload: unavailable for name in ReadOnlyTools.names},
            "get_security_snapshot": lambda payload: security,
            "get_financial_facts": lambda payload: financial,
        }
    )

    frozen = await codex_sdk.CodexCommittee(prompts=prompts, tools=tools)._inputs(request)

    assert seen == [(security, financial)]
    assert frozen["security_snapshot"] == security
    assert frozen["financial_facts"] == financial
    assert frozen["trailing_valuation"] == derived


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
        elapsed += 25  # Each stage fits its 40s limit; all three need 75s.
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
    # This test reserves only 50ms for transport; tokenizer cold-start cost is
    # covered separately and must not decide which deadline boundary is tested.
    monkeypatch.setattr(codex_staged, "codex_input_allowance", lambda *args: 8192)

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
    assert model.failure_diagnostic.invocation_phase == "aggregate_wait"
    assert model.failure_diagnostic.invocation_kind == "timeout"


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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "phase", ["client_init", "account", "config_read", "thread_start", "turn_run", "client_close"]
)
async def test_sdk_invocation_failure_retains_only_safe_phase(monkeypatch, phase):
    client, thread, _, _ = _sdk(monkeypatch)
    error = RuntimeError("private-exception-canary")
    if phase == "client_init":

        def fail(config):
            raise error

        monkeypatch.setattr(codex_sdk, "AsyncCodex", fail)
    else:
        target = {
            "account": client.account,
            "config_read": client._client.request,
            "thread_start": client.thread_start,
            "turn_run": thread.run,
            "client_close": client.close,
        }[phase]
        target.side_effect = error
    with pytest.raises(codex_sdk.CodexInvocationError) as caught:
        await codex_sdk.invoke_codex({})
    assert caught.value.phase == phase
    assert caught.value.kind == "unexpected_exception"
    assert caught.value.provider_call_started is (phase in {"turn_run", "client_close"})
    assert "private-exception-canary" not in str(caught.value)
    if phase != "client_init":
        client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_temp_directory_cleanup_after_turn_retains_dispatch_fact(monkeypatch):
    _, thread, _, _ = _sdk(monkeypatch)
    original = codex_sdk.TemporaryDirectory

    class BrokenCleanup(original):
        def __exit__(self, *args):
            super().__exit__(*args)
            raise OSError("private-cleanup-canary")

    monkeypatch.setattr(codex_sdk, "TemporaryDirectory", BrokenCleanup)
    with pytest.raises(codex_sdk.CodexInvocationError) as caught:
        await codex_sdk.invoke_codex_portfolio({})
    thread.run.assert_awaited_once()
    assert caught.value.provider_call_started is True
    assert "private-cleanup-canary" not in str(caught.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout", [False, True])
async def test_staged_real_sdk_failure_builds_invocation_diagnostic(monkeypatch, timeout):
    from nexus.agents import codex_staged

    client, thread, _, _ = _sdk(monkeypatch)
    thread.run.side_effect = (
        TimeoutError("private-canary") if timeout else RuntimeError("private-canary")
    )
    request, evidence, prompts = _authority()
    model = codex_staged.StagedCodexCommittee(prompts=prompts, tools=_tools())
    run = SimpleNamespace(retry_count=0)
    assert (
        await _analyze_with_one_retry(
            AsyncMock(),
            run,
            committee=model,
            role="orchestrator",
            request=request,
            evidence_refs=evidence,
            prompt=prompts["orchestrator"],
        )
        is None
    )
    diagnostic = model.failure_diagnostic
    assert diagnostic is not None
    assert diagnostic.model_dump(mode="json") == {
        "stage": "research",
        "boundary": "invocation",
        "role": None,
        "response_hash": None,
        "validation_kind": None,
        "invocation_phase": "turn_run",
        "invocation_kind": "timeout" if timeout else "unexpected_exception",
    }
    assert model.failure_reason == "model_usage_uncertain"
    assert model.usage == UsageSnapshot(provider_calls=1)
    assert run.retry_count == 0
    thread.run.assert_awaited_once()
    client.close.assert_awaited_once()


def test_invocation_diagnostic_is_disjoint_from_legacy_validation():
    from nexus.agents.committee import StageFailureDiagnostic
    from pydantic import ValidationError

    old = {
        "stage": "research",
        "boundary": "wire_validation",
        "role": None,
        "response_hash": "a" * 64,
        "validation_kind": "schema_validation",
    }
    assert StageFailureDiagnostic.model_validate(old).model_dump(mode="json") == old
    new = {
        **old,
        "boundary": "invocation",
        "response_hash": None,
        "validation_kind": None,
        "invocation_phase": "turn_run",
        "invocation_kind": "timeout",
    }
    assert StageFailureDiagnostic.model_validate(new).model_dump(mode="json") == new
    for update in (
        {"response_hash": "a" * 64},
        {"role": "bull"},
        {"validation_kind": "unsafe_text"},
        {"invocation_kind": "private-canary"},
        {"invocation_phase": None},
    ):
        with pytest.raises(ValidationError):
            StageFailureDiagnostic.model_validate({**new, **update})
    with pytest.raises(ValidationError):
        StageFailureDiagnostic.model_validate({**old, "response_hash": None})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind",
    [
        "response_incomplete",
        "unexpected_tool",
        "output_too_large",
        "usage_missing",
        "usage_invalid",
    ],
)
async def test_staged_sdk_rejection_retains_safe_typed_reason_not_raw(monkeypatch, kind):
    from nexus.agents import codex_staged

    _, thread, result, _ = _sdk(monkeypatch)
    if kind == "response_incomplete":
        result.status = TurnStatus.failed
    elif kind == "unexpected_tool":
        result.items = [SimpleNamespace(root=SimpleNamespace(type="commandExecution"))]
    elif kind == "output_too_large":
        result.final_response = "private-canary" * codex_sdk._MAX_OUTPUT_BYTES
    elif kind == "usage_missing":
        result.usage = None
    else:
        result.usage.total.input_tokens = -1
    request, evidence, prompts = _authority()
    model = codex_staged.StagedCodexCommittee(prompts=prompts, tools=_tools())
    with pytest.raises(codex_sdk.CodexInvocationError) as caught:
        await model.analyze("orchestrator", request, evidence, prompts["orchestrator"])
    assert caught.value.kind == kind
    assert model.failure_diagnostic.invocation_kind == kind
    assert model.failure_diagnostic.invocation_phase == "response_check"
    assert model.failure_diagnostic.response_hash is None
    assert "private-canary" not in model.failure_diagnostic.model_dump_json()
    assert model.usage == UsageSnapshot(provider_calls=1)  # Existing uncertain-call accounting.
    assert model.failure_reason == "model_usage_uncertain"
    thread.run.assert_awaited_once()


@pytest.mark.asyncio
async def test_plain_exception_cannot_spoof_typed_sdk_failure(monkeypatch):
    _, thread, _, _ = _sdk(monkeypatch)
    thread.run.side_effect = ValueError("CODEX_CHATGPT_LOGIN_REQUIRED private-canary")
    with pytest.raises(codex_sdk.CodexInvocationError) as caught:
        await codex_sdk.invoke_codex({})
    assert caught.value.kind == "unexpected_exception"
    assert caught.value.phase == "turn_run"
    assert str(caught.value) == "CODEX_UNEXPECTED_EXCEPTION"


@pytest.mark.asyncio
async def test_staged_external_cancel_propagates_without_invented_diagnostic(monkeypatch):
    from nexus.agents import codex_staged

    client, thread, _, _ = _sdk(monkeypatch)
    started = asyncio.Event()
    released = asyncio.Event()

    async def waiting(*args, **kwargs):
        started.set()
        await released.wait()
        raise RuntimeError("private-cancel-canary")

    async def close():
        released.set()

    thread.run.side_effect = waiting
    client.close.side_effect = close
    request, evidence, prompts = _authority()
    model = codex_staged.StagedCodexCommittee(prompts=prompts, tools=_tools())
    task = asyncio.create_task(
        model.analyze("orchestrator", request, evidence, prompts["orchestrator"])
    )
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert model.failure_diagnostic is None
    assert model.usage == UsageSnapshot(provider_calls=1)
    thread.run.assert_awaited_once()
    client.close.assert_awaited_once()


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


@pytest.mark.asyncio
async def test_staged_sdk_explains_frozen_paper_decision_price_context(monkeypatch):
    from nexus.agents import codex_staged

    request, evidence, prompts = _authority()
    unavailable = {"status": "unavailable", "items": [], "reason": "no_frozen_price_authority"}
    paper_context = {
        "decision_quote": {"source": "schwab", "bid": "99.9", "ask": "100.1"},
        "prior_daily": {"source": "alpaca", "close": "100.0"},
        "execution_locked": True,
    }
    security = {
        "status": "ok",
        "items": [
            {
                "source": "nexus_agent_paper_context",
                "provider_object_id": "cycle:instrument",
                "revision": "b" * 64,
                "available_at": request.decision_time.isoformat(),
                "content_hash": "b" * 64,
                "agent_paper_context": paper_context,
            }
        ],
    }
    tools = ReadOnlyTools(
        executors={
            **{name: lambda payload: unavailable for name in ReadOnlyTools.names},
            "get_security_snapshot": lambda payload: security,
        }
    )
    submitted = []

    async def invoke(payload, **kwargs):
        submitted.append(payload)
        body = (
            _assessment_payload(payload["previous_claims"])
            if payload["stage"] == "synthesis"
            else _staged_batch(payload["stage"], payload["requested_roles"])
        )
        return (
            json.dumps(body),
            UsageSnapshot(input_tokens=300, output_tokens=80),
        )

    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    model = codex_staged.StructuredStagedCodexCommittee(
        prompts=prompts,
        tools=tools,
        inline_citations=False,
    )
    for role in ROLE_ORDER:
        await model.analyze(role, request, evidence, prompts[role])

    assert len(submitted) == 3
    for payload in submitted:
        assert payload["frozen_inputs"]["security_snapshot"] == security
        assert "current decision-price context" in payload["instructions"]
        assert "not execution or fill authority" in payload["instructions"]


def test_child_environment_excludes_secrets_and_parent_app_ipc(monkeypatch):
    safe_runtime = {
        "LANGUAGE": "zh_CN:en",
        "LC_CTYPE": "UTF-8",
        "SSL_CERT_FILE": "/private/runtime/cert.pem",
        "SSL_CERT_DIR": "/private/runtime/certs",
        "REQUESTS_CA_BUNDLE": "/private/runtime/requests.pem",
        "NODE_EXTRA_CA_CERTS": "/private/runtime/node.pem",
    }
    for key, value in safe_runtime.items():
        monkeypatch.setenv(key, value)
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
    environment = codex_sdk.runtime_environment()
    assert "secret-canary" not in json.dumps(environment)
    assert {key: environment.get(key) for key in safe_runtime} == safe_runtime
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
@pytest.mark.parametrize("over_budget", [0, 1])
async def test_invoke_preflight_includes_actual_strict_schema_and_instructions(
    monkeypatch, tmp_path, over_budget
):
    from nexus.agents import codex_token_budget
    from openai import pydantic_function_tool

    monkeypatch.setattr(codex_token_budget, "tokenizer_cache_path", lambda: tmp_path / "missing")
    client, thread, _, configs = _sdk(monkeypatch)
    await codex_sdk.invoke_codex({"text": ""})
    schema = pydantic_function_tool(codex_sdk._Batch)["function"]["parameters"]
    assert thread.run.call_args.kwargs["output_schema"] == schema
    explicit_bytes = len(json.dumps(schema, ensure_ascii=False, separators=(",", ":")).encode())
    explicit_bytes += sum(
        len(client.thread_start.call_args.kwargs[name].encode())
        for name in ("base_instructions", "developer_instructions")
    )
    payload = {"text": "x" * (64000 - 8192 - explicit_bytes - len('{"text":""}') + over_budget)}
    configs.clear()
    thread.run.reset_mock()
    if over_budget:
        with pytest.raises(ValueError, match="CODEX_INPUT_TOO_LARGE"):
            await codex_sdk.invoke_codex(payload)
        assert not configs
        thread.run.assert_not_awaited()
    else:
        await codex_sdk.invoke_codex(payload)
        assert thread.run.call_args.args[0] == codex_sdk.codex_request_wire(payload)


@pytest.mark.asyncio
async def test_original_wire_byte_guard_still_applies_with_small_token_count(monkeypatch):
    from nexus.agents import codex_token_budget

    monkeypatch.setattr(codex_token_budget, "payload_input_units", lambda *args, **kwargs: 1)
    _, thread, _, configs = _sdk(monkeypatch)
    with pytest.raises(ValueError, match="CODEX_INPUT_TOO_LARGE"):
        await codex_sdk.invoke_codex({"text": "中" * 19000})
    assert not configs
    thread.run.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [1, 2, 3])
@pytest.mark.parametrize("usage_field, limit", [("input_tokens", 64000), ("output_tokens", 8192)])
async def test_actual_usage_gate_still_rejects_after_preflight(
    monkeypatch, version, usage_field, limit
):
    from nexus.agents import codex_staged

    request, evidence, prompts = _authority()
    _, thread, result, _ = _sdk(monkeypatch)
    setattr(result.usage.total, usage_field, limit + 1)
    model = {
        1: codex_sdk.CodexCommittee,
        2: codex_staged.StagedCodexCommittee,
        3: codex_staged.StructuredStagedCodexCommittee,
    }[version](prompts=prompts, tools=_tools())
    with pytest.raises(ValueError, match="CODEX_TOKEN_LIMIT_EXCEEDED"):
        await model.analyze("orchestrator", request, evidence, prompts["orchestrator"])
    assert not model._outputs
    assert getattr(model.usage, usage_field) == limit + 1
    thread.run.assert_awaited_once()


@pytest.mark.asyncio
async def test_staged_oversized_wire_is_rejected_before_counting_a_provider_call(monkeypatch):
    from nexus.agents import codex_staged, codex_token_budget

    monkeypatch.setattr(codex_token_budget, "payload_input_units", lambda *args, **kwargs: 1)
    request, evidence, prompts = _authority()
    _, thread, _, configs = _sdk(monkeypatch)
    tools = ReadOnlyTools(
        executors={name: lambda payload: {"text": "中" * 19000} for name in ReadOnlyTools.names}
    )
    model = codex_staged.StagedCodexCommittee(prompts=prompts, tools=tools)
    with pytest.raises(ValueError, match="CODEX_AGGREGATE_ALLOWANCE_EXHAUSTED"):
        await model.analyze("orchestrator", request, evidence, prompts["orchestrator"])
    assert model.usage.provider_calls == 0 and not configs
    thread.run.assert_not_awaited()


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
    assert options["model"] == "gpt-5.6-terra"
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
async def test_research_sdk_keeps_a_tail_latency_margin_without_changing_portfolio(monkeypatch):
    observed: list[int] = []

    async def invoke(payload, *, output_model, max_wire_bytes, timeout_seconds, invocation_state):
        del payload, output_model, max_wire_bytes
        assert invocation_state.provider_call_started is False
        observed.append(timeout_seconds)
        return "{}", UsageSnapshot()

    monkeypatch.setattr(codex_sdk, "_invoke_codex", invoke)

    await codex_sdk.invoke_codex({})
    await codex_sdk.invoke_codex_portfolio({})

    assert observed == [40, 45]


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
async def test_timeout_force_stops_sdk_process_when_close_stalls(monkeypatch):
    client, thread, _, _ = _sdk(monkeypatch)
    stopped = asyncio.Event()

    class Process:
        killed = False

        def poll(self):
            return None

        def kill(self):
            self.killed = True
            stopped.set()

    process = Process()
    client._client._sync = SimpleNamespace(_proc=process)

    async def stall(*args, **kwargs):
        await asyncio.sleep(10)

    async def close():
        await stopped.wait()

    thread.run.side_effect = stall
    client.close.side_effect = close
    monkeypatch.setattr(codex_sdk, "TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(codex_sdk, "CLIENT_CLOSE_TIMEOUT_SECONDS", 0.01)
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(codex_sdk.invoke_codex({}), timeout=0.2)
    assert process.killed
    client.close.assert_awaited_once()


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
    with pytest.raises(codex_sdk.CodexInvocationError, match="CODEX_UNEXPECTED_EXCEPTION"):
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
    "changes, conclusion, expected_kind",
    [
        (
            {"resolved": True, "missing_evidence": [], "prevented_conclusion": ""},
            "insufficient",
            "review_resolution_inconsistent",
        ),
        (
            {"resolved": True, "prevented_conclusion": None},
            "insufficient",
            "review_resolution_inconsistent",
        ),
        (
            {
                "resolved": True,
                "status": "unresolved",
                "missing_evidence": [],
                "prevented_conclusion": None,
            },
            "insufficient",
            "review_resolution_inconsistent",
        ),
        ({"missing_evidence": []}, "insufficient", "review_evidence_incomplete"),
        ({"missing_evidence": [" "]}, "insufficient", "review_evidence_incomplete"),
        ({"prevented_conclusion": None}, "insufficient", "review_evidence_incomplete"),
        ({"prevented_conclusion": " "}, "insufficient", "review_evidence_incomplete"),
        ({"rationale": " "}, "insufficient", "review_rationale_blank"),
        ({}, "sufficient", "review_sufficiency_inconsistent"),
    ],
)
async def test_v3_schema_valid_semantic_failure_keeps_precise_safe_terminal_diagnostic(
    monkeypatch, changes, conclusion, expected_kind
):
    import jsonschema
    from nexus.agents import codex_staged
    from nexus.agents.committee import StageFailureDiagnostic

    request, evidence, prompts = _authority()
    _, thread, result, _ = _sdk(monkeypatch)
    sent = []
    canary = "synthetic-private-assessment-canary"

    async def run(wire, **options):
        payload = json.loads(wire)
        sent.append(payload["stage"])
        if payload["stage"] == "synthesis":
            body = _assessment_payload(payload["previous_claims"])
            body["synthesis_assessment"]["reviews"][0].update(changes)
            body["synthesis_assessment"]["review_conclusion"] = conclusion
            body["synthesis_assessment"]["uncertainties"] = [canary]
        else:
            body = _staged_batch(payload["stage"], payload["requested_roles"])
        jsonschema.validate(body, options["output_schema"])
        result.final_response = json.dumps(body)
        return result

    thread.run.side_effect = run
    model = codex_staged.StructuredStagedCodexCommittee(prompts=prompts, tools=_tools())
    for role in ROLE_ORDER[:-1]:
        await model.analyze(role, request, evidence, prompts[role])
    prior = dict(model._outputs)
    run_state = SimpleNamespace(retry_count=0)
    output = await _analyze_with_one_retry(
        AsyncMock(),
        run_state,
        committee=model,
        role="portfolio",
        request=request,
        evidence_refs=evidence,
        prompt=prompts["portfolio"],
    )
    assert output is None and model._outputs == prior and len(prior) == 7
    assert sent == ["research", "critique", "synthesis"] and run_state.retry_count == 0
    assert model.usage == UsageSnapshot(input_tokens=900, output_tokens=240, provider_calls=3)
    assert model.failure_reason == "model_stage_review_invalid"
    diagnostic = model.failure_diagnostic.model_dump(mode="json")
    assert diagnostic == {
        "stage": "synthesis",
        "boundary": "assessment_binding",
        "role": "portfolio",
        "response_hash": hashlib.sha256(result.final_response.encode()).hexdigest(),
        "validation_kind": expected_kind,
    }
    StageFailureDiagnostic.model_validate(diagnostic)
    assert canary not in json.dumps(diagnostic)
    with pytest.raises(ValueError, match="CODEX_REQUEST_ALREADY_FAILED"):
        await model.analyze("portfolio", request, evidence, prompts["portfolio"])
    assert len(sent) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("resolved", [False, True])
async def test_structured_request_preserves_audit_vote_and_resolution_contract(
    monkeypatch, resolved
):
    import jsonschema
    from nexus.agents import codex_staged

    request, evidence, prompts = _authority()
    _, thread, result, _ = _sdk(monkeypatch)
    expected_hash = None

    async def run(wire, **options):
        nonlocal expected_hash
        payload = json.loads(wire)
        if payload["stage"] == "synthesis":
            # Inspect the actual SDK-bound schema and wire, not a detached helper.
            descriptions = " ".join(
                field.get("description", "")
                for definition in options["output_schema"]["$defs"].values()
                for field in definition.get("properties", {}).values()
            )
            for text in (
                "resolved=true",
                "resolved=false",
                "null",
                "empty string",
                "non-whitespace",
                "sufficient",
                "evidence_audit previous claim has vote other than support",
            ):
                assert text in payload["instructions"]
                if text != "evidence_audit previous claim has vote other than support":
                    assert text in descriptions
            assert all(
                item["vote"] in {"support", "oppose", "abstain"}
                for item in payload["previous_claims"]
            )
            expected_hash = hashlib.sha256(
                json.dumps(
                    {"payload": payload, "output_schema": options["output_schema"]},
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()
            body = _assessment_payload(payload["previous_claims"])
            if resolved:
                for review in body["synthesis_assessment"]["reviews"]:
                    review.update(resolved=True, missing_evidence=[], prevented_conclusion=None)
                body["synthesis_assessment"]["review_conclusion"] = "sufficient"
        else:
            assert "resolved=true" not in payload["instructions"]
            body = _staged_batch(payload["stage"], payload["requested_roles"])
        jsonschema.validate(body, options["output_schema"])
        result.final_response = json.dumps(body)
        return result

    thread.run.side_effect = run
    model = codex_staged.StructuredStagedCodexCommittee(prompts=prompts, tools=_tools())
    for role in ROLE_ORDER:
        output = await model.analyze(role, request, evidence, prompts[role])
    assert len(model._outputs) == 8 and model.failure_diagnostic is None
    assert all(review.resolved is resolved for review in output.synthesis_assessment.reviews)
    assert output.stage_input_hash == expected_hash


@pytest.mark.parametrize(
    "messages, expected",
    [
        (["unrecognized synthetic-private-canary"], "domain_validation"),
        (["review rationale is required synthetic-private-canary"], "domain_validation"),
        (["review rationale is required", "research intent contains unsafe text"], "unsafe_text"),
    ],
)
def test_review_diagnostic_never_serializes_unknown_error_input_or_context(messages, expected):
    from nexus.agents import codex_staged
    from nexus.agents.committee import StageFailureDiagnostic
    from pydantic import ValidationError

    _, _, prompts = _authority()
    model = codex_staged.StructuredStagedCodexCommittee(prompts=prompts, tools=_tools())
    model.failure_diagnostic = StageFailureDiagnostic(
        stage="synthesis",
        boundary="assessment_binding",
        role="portfolio",
        response_hash="a" * 64,
    )
    error = ValidationError.from_exception_data(
        "synthetic",
        [
            {
                "type": "value_error",
                "loc": ("synthetic-private-location",),
                "input": "synthetic-private-input",
                "ctx": {"error": ValueError(message)},
            }
            for message in messages
        ],
    )
    model._record_domain_validation(error)
    wire = model.failure_diagnostic.model_dump_json()
    assert model.failure_diagnostic.validation_kind == expected
    assert "synthetic-private" not in wire
    StageFailureDiagnostic.model_validate_json(wire)


def test_v3_semantic_descriptions_do_not_change_v1_v2_schema_hashes():
    from nexus.agents import codex_staged

    models = [
        codex_sdk._Batch,
        codex_staged._ResearchBatch,
        codex_staged._stage_model_for_prior_claims("critique", ("bull:0",)),
        codex_staged._stage_model_for_prior_claims("synthesis", ("bear:0", "evidence_audit:0")),
    ]
    expected = [
        "5399522d63aa78bc385c356d6ebecd7a825c741426bcfdf98103e710b65fd981",
        "9753c1495a862ec39a5119fb7e01de85065ce67f805aadace87e4eff5d44c52a",
        "7b9dd3a2300b103671e0ebddf2ecd5c455f95b09ac48e7eaa78e24b090ec0f9a",
        "2a64f99511dfc90e4f0140b2bd0a6b00ee6c0b0ad9cfe2bc9ab13fcdadfbc71c",
    ]
    assert [
        codex_sdk.codex_request_hash({"input": "synthetic"}, model) for model in models
    ] == expected


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
@pytest.mark.parametrize("version", ["codex-sdk-chatgpt-v3", "codex-sdk-chatgpt-v3-input96k"])
async def test_sdk_v3_cannot_fall_back_to_other_runtime(monkeypatch, runtime, version):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from nexus import worker
    from nexus.settings import EngineSettings

    session = SimpleNamespace(
        get=AsyncMock(
            return_value=SimpleNamespace(
                version=version,
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
    diagnostic = model.failure_diagnostic.model_dump(mode="json")
    assert diagnostic == {
        "stage": "research",
        "boundary": "role_binding",
        "role": ROLE_ORDER[role_index],
        "response_hash": hashlib.sha256(raw.encode()).hexdigest(),
        "validation_kind": "unsafe_text",
    }
    assert "synthetic-private-canary" not in json.dumps(diagnostic)


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
    assert model.failure_diagnostic.boundary == "role_binding"
    assert model.failure_diagnostic.validation_kind is None


@pytest.mark.asyncio
async def test_staged_request_advertises_existing_text_restrictions_and_clears_diagnostic(
    monkeypatch,
):
    from nexus.agents import codex_staged

    request, evidence, prompts = _authority()

    async def invoke(payload, *, output_model):
        constraints = payload["text_constraints"]["forbidden_substrings_case_insensitive"]
        assert ";" in constraints and "sh " in constraints and "https://" in constraints
        return json.dumps(_staged_batch("research", ROLE_ORDER[:5])), UsageSnapshot(
            input_tokens=300, output_tokens=80
        )

    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    model = codex_staged.StructuredStagedCodexCommittee(prompts=prompts, tools=_tools())
    result = await model.analyze("orchestrator", request, evidence, prompts["orchestrator"])
    assert result.role == "orchestrator" and model.failure_diagnostic is None
