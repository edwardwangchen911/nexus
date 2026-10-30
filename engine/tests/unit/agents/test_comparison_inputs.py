import copy
import hashlib
import json
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from nexus.agents.budget import BudgetExhausted
from nexus.agents.committee import INDEPENDENT_MONITOR_COMPACT_PROMPT_VERSION, ROLE_ORDER
from nexus.agents.comparison_compiler import compile_comparison_stage
from nexus.agents.comparison_inputs import TOOLS, assert_outbound_safe, prepare_comparison_input
from nexus.monitoring.independent_context import IndependentMonitorContext, content
from nexus.ops.research_comparison import main, prepare_archive, prepare_file


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def source_member(*, instrument=None, private=None, candidate_ids=None, symbol=None):
    instrument = instrument or str(uuid4())
    as_of = "2026-10-27T13:30:00Z"
    context = IndependentMonitorContext.model_validate_json(
        json.dumps(
            {
                "request_id": str(uuid4()),
                "request_hash": "1" * 64,
                "account_id": str(uuid4()),
                "candidate_batch_id": str(uuid4()),
                "batch_hash": "2" * 64,
                **(private or {}),
                "candidate_ids": candidate_ids or [instrument],
                "instrument_id": instrument,
                "decision_time": as_of,
                "source_snapshot_id": str(uuid4()),
                "source_snapshot_hash": "3" * 64,
                "predecessors": [
                    {"kind": kind, "plan_id": None, "generation": None}
                    for kind in ("price", "disclosure")
                ],
                "disclosure_monitoring": {
                    "schema_version": "disclosure_monitor_context_v1",
                    "instrument_id": instrument,
                    "research_as_of": as_of,
                    "baseline": None,
                    "gap_reason": "SEC_BASELINE_MISSING",
                },
            }
        )
    )
    context_hash = hashlib.sha256(content(context)).hexdigest()
    evidence = [
        {
            "source": "SEC",
            "object_id": "synthetic-filing",
            "available_at": as_of,
            "location": "fixture://synthetic-filing",
            "content_hash": "4" * 64,
        },
        {
            "source": "nexus_independent_monitor_research",
            "object_id": f"{context.request_id}:{instrument}",
            "available_at": as_of,
            "location": f"memory://sha256/{context_hash}",
            "content_hash": context_hash,
        },
    ]
    manifest = [
        {
            "id": str(uuid4()),
            "source": e["source"],
            "provider_object_id": e["object_id"],
            "revision": e["content_hash"],
            "available_at": e["available_at"],
            "location": e["location"],
            "content_hash": e["content_hash"],
        }
        for e in evidence
    ]
    snapshot = str(uuid4())
    envelope = {
        "schema_version": "1",
        "instrument_id": instrument,
        "data_snapshot_id": snapshot,
        "as_of": as_of,
        "evidence_hash": digest(manifest),
        "status": "ready",
        "reason": None,
    }
    member = {
        "run_id": str(uuid4()),
        "profile": "codex-sdk-chatgpt-v11-bounded-stage-budget",
        "request": {
            "instrument_id": instrument,
            "data_snapshot_id": snapshot,
            "decision_time": as_of,
        },
        "prompts": {
            role: {
                "role": role,
                "version": INDEPENDENT_MONITOR_COMPACT_PROMPT_VERSION,
                "content": "synthetic prompt\n## Claim and audit scope\nSynthetic scope.",
                "content_hash": hashlib.sha256(
                    b"synthetic prompt\n## Claim and audit scope\nSynthetic scope."
                ).hexdigest(),
            }
            for role in ROLE_ORDER
        },
        "evidence": evidence,
        "inputs": {
            "security_snapshot": {
                **envelope,
                "tool": "security_snapshot",
                "purpose": "independent_monitor_research_v1",
                "portfolio_context_status": "not_applicable",
                "independent_monitor_context": context.model_dump(mode="json"),
                "items": [
                    {
                        "evidence_ref_id": m["id"],
                        **{k: v for k, v in m.items() if k not in {"id", "location"}},
                    }
                    for m in manifest
                ],
            },
            "financial_facts": {
                **envelope,
                "tool": "financial_facts",
                "items": [
                    {
                        "fact": "eps",
                        "raw_taxonomy": "us-gaap",
                        "raw_tag": "EarningsPerShareDiluted",
                        "unit": "USD/shares",
                        "value": "1.23",
                        "evidence_ref_id": manifest[0]["id"],
                    }
                ],
            },
        },
    }
    if symbol:
        member["inputs"]["security_snapshot"]["items"][0][
            "research_observation_identity_binding"
        ] = {"symbol": symbol, "instrument_id": instrument}
        for tool in TOOLS - set(member["inputs"]):
            member["inputs"][tool] = {**envelope, "tool": tool, "items": []}
        for tool in ("price_close", "price_features"):
            member["inputs"][tool]["items"] = [{"symbol": symbol, "instrument_id": instrument}]
    return member


def test_rebinding_is_deterministic_preserves_company_evidence_and_historical_scope():
    source = source_member()
    original = copy.deepcopy(source)
    experiment = uuid4()
    result = prepare_comparison_input(source, experiment_id=experiment)
    again = prepare_comparison_input(source, experiment_id=experiment)
    assert result.payload == again.payload
    assert result.proof == again.proof
    assert source == original
    assert result.payload["evidence"][0] == source["evidence"][0]
    assert (
        result.payload["inputs"]["financial_facts"]["items"]
        == source["inputs"]["financial_facts"]["items"]
    )
    old = source["inputs"]["security_snapshot"]["independent_monitor_context"]
    new = result.payload["inputs"]["security_snapshot"]["independent_monitor_context"]
    for key in (
        "instrument_id",
        "candidate_ids",
        "decision_time",
        "source_snapshot_id",
        "source_snapshot_hash",
        "predecessors",
        "disclosure_monitoring",
    ):
        assert new[key] == old[key]
    assert new["account_id"] != old["account_id"]
    assert new["execution_locked"] is True and new["formal_consumption_allowed"] is False
    assert result.proof["semantic_difference"] == []
    assert result.proof["company_evidence_unchanged"] is True
    assert_outbound_safe({"messages": [{"content": json.dumps(result.payload)}]}, result.forbidden)
    assert prepare_comparison_input(source, experiment_id=uuid4()).input_hash != result.input_hash


@pytest.mark.parametrize(
    "corruption",
    ["manifest", "context", "prompt", "duplicate", "missing", "envelope", "predecessor"],
)
def test_inconsistent_frozen_graph_rejected(corruption):
    source = source_member()
    security = source["inputs"]["security_snapshot"]
    if corruption == "manifest":
        security["items"][0]["revision"] = "changed"
    elif corruption == "context":
        security["independent_monitor_context"]["account_id"] = str(uuid4())
    elif corruption == "prompt":
        source["prompts"]["bull"]["content"] += " changed"
    elif corruption == "duplicate":
        source["evidence"].append(source["evidence"][0])
    elif corruption == "missing":
        source["evidence"].pop(0)
    elif corruption == "envelope":
        source["inputs"]["financial_facts"]["data_snapshot_id"] = str(uuid4())
    else:
        security["independent_monitor_context"]["predecessors"][0].update(
            plan_id=str(uuid4()), generation=1
        )
    with pytest.raises(ValueError, match="COMPARISON_SOURCE_INVALID"):
        prepare_comparison_input(source, experiment_id=uuid4())


def test_unexpected_private_identity_path_fails_instead_of_global_replacement():
    source = source_member()
    source["inputs"]["financial_facts"]["extra"] = source["run_id"]
    with pytest.raises(ValueError, match="COMPARISON_PRIVATE_BINDING_REMAINED"):
        prepare_comparison_input(source, experiment_id=uuid4())


def test_full_outbound_tree_including_keys_and_embedded_json_is_scanned():
    old = str(UUID(int=123))
    for tree in (
        {old: "x"},
        {"messages": [{"content": old.upper()}]},
        {"content": json.dumps({"id": old}).replace("-", "\\u002d")},
    ):
        with pytest.raises(ValueError, match="COMPARISON_PRIVATE_BINDING_REMAINED"):
            assert_outbound_safe(tree, (old,))
    for unsupported in (UUID(old), b"unscanned", float("nan"), float("inf")):
        with pytest.raises(ValueError, match="COMPARISON_OUTBOUND_VALUE_INVALID"):
            assert_outbound_safe({"content": unsupported}, (old,))


def test_normal_compiler_rejects_input_drift_and_prior_output_private_binding():
    prepared = prepare_comparison_input(source_member(), experiment_id=uuid4())
    expected = prepared.input_hash
    payload, _ = compile_comparison_stage(
        prepared, expected_input_hash=expected, stage="research", previous=[]
    )
    assert_outbound_safe(payload, prepared.forbidden)
    with pytest.raises(ValueError, match="COMPARISON_PRIVATE_BINDING_REMAINED"):
        compile_comparison_stage(
            prepared,
            expected_input_hash=expected,
            stage="critique",
            previous=[{"unexpected": prepared.forbidden[0]}],
        )
    prepared.payload["inputs"]["financial_facts"]["items"][0]["value"] = "99.99"
    with pytest.raises(ValueError, match="COMPARISON_INPUT_CHANGED"):
        compile_comparison_stage(
            prepared, expected_input_hash=expected, stage="research", previous=[]
        )


def archive_source():
    ids = [str(uuid4()) for _ in range(3)]
    private = {
        "request_id": str(uuid4()),
        "account_id": str(uuid4()),
        "candidate_batch_id": str(uuid4()),
    }
    return {
        "members": [
            source_member(instrument=i, private=private, candidate_ids=ids, symbol=s)
            for i, s in zip(ids, ("AAPL", "AVGO", "NVDA"), strict=True)
        ]
    }


def test_normal_prepare_entry_is_offline_private_exclusive_and_deterministic(tmp_path):
    source = json.dumps(archive_source()).encode()
    expected = hashlib.sha256(source).hexdigest()
    experiment = uuid4()
    prepared = prepare_archive(source, expected_source_hash=expected, experiment_id=experiment)
    assert prepared == prepare_archive(
        source, expected_source_hash=expected, experiment_id=experiment
    )
    assert prepared["dispatch_enabled"] is False
    assert prepared["model_calls"] == prepared["database_writes"] == 0
    assert [r["symbol"] for r in prepared["members"]] == ["AAPL", "AVGO", "NVDA"]
    path, output = tmp_path / "source.json", tmp_path / "prepared.json"
    path.write_bytes(source)
    started = datetime.now(UTC)
    receipt = prepare_file(
        source=path, expected_source_hash=expected, experiment_id=experiment, output=output
    )
    artifact = output.read_bytes()
    assert receipt["source_unchanged"] and path.read_bytes() == source
    assert output.stat().st_mode & 0o777 == 0o600
    assert (
        started <= datetime.fromisoformat(json.loads(artifact)["rebuilt_at"]) <= datetime.now(UTC)
    )
    with pytest.raises(FileExistsError):
        prepare_file(
            source=path, expected_source_hash=expected, experiment_id=experiment, output=output
        )
    assert output.read_bytes() == artifact


def test_full_capacity_check_preserves_eligible_legacy_preparation(monkeypatch):
    from nexus.agents import comparison_preparation

    source = json.dumps(archive_source()).encode()
    arguments = dict(expected_source_hash=hashlib.sha256(source).hexdigest(), experiment_id=uuid4())
    with monkeypatch.context() as legacy:
        legacy.setattr(comparison_preparation, "validate_comparison_capacity", lambda _: None)
        old = prepare_archive(source, **arguments)
    assert prepare_archive(source, **arguments) == old


@pytest.mark.parametrize("member_index", [0, 1, 2])
@pytest.mark.parametrize("excerpt_size,first_exceeded", [(50000, "synthesis"), (65000, "critique")])
def test_prepare_rejects_later_stage_capacity_before_creating_artifact(
    tmp_path, member_index, excerpt_size, first_exceeded
):
    from nexus.agents.codex_capacity import candidate_capacity_envelope
    from nexus.agents.codex_sdk import codex_request_wire
    from nexus.agents.committee import INDEPENDENT_MONITOR_PROMPT_VERSION
    from nexus.agents.comparison_execution import FrozenComparisonCommittee
    from nexus.domain.evidence import EvidenceRef

    archive = archive_source()
    member = archive["members"][member_index]
    for prompt in member["prompts"].values():
        prompt["version"] = INDEPENDENT_MONITOR_PROMPT_VERSION
    member["inputs"]["filing_disclosures"]["items"] = [{"excerpt": "x" * excerpt_size}]
    experiment = uuid4()
    item = prepare_comparison_input(member, experiment_id=experiment)
    payload, _ = compile_comparison_stage(
        item, expected_input_hash=item.input_hash, stage="research", previous=[]
    )
    assert len(codex_request_wire(payload).encode()) < 100479
    compiler = FrozenComparisonCommittee(item, deadline=300)
    envelope = candidate_capacity_envelope(
        compiler,
        request=compiler.frozen_request,
        evidence_refs=tuple(EvidenceRef.model_validate(r) for r in item.payload["evidence"]),
        inputs=item.payload["inputs"],
    )
    assert (
        next(row["stage"] for row in envelope["stages"] if row["wire_upper_bytes"] > 100479)
        == first_exceeded
    )
    source, output = tmp_path / "source.json", tmp_path / "prepared.json"
    raw = json.dumps(archive).encode()
    source.write_bytes(raw)
    with pytest.raises(ValueError, match="^CODEX_CAPACITY_POOL_WIRE_EXCEEDED$"):
        prepare_file(
            source=source,
            expected_source_hash=hashlib.sha256(raw).hexdigest(),
            experiment_id=experiment,
            output=output,
        )
    assert not output.exists()
    assert source.read_bytes() == raw


@pytest.mark.parametrize("corruption", ["symbol", "source_hash", "pool", "identity", "time"])
def test_normal_prepare_rejects_wrong_pool_or_source(corruption):
    archive = archive_source()
    member = archive["members"][0]
    if corruption == "symbol":
        member["inputs"]["price_close"]["items"][0]["symbol"] = "MSFT"
    elif corruption == "pool":
        member["inputs"]["security_snapshot"]["independent_monitor_context"]["candidate_ids"].pop()
    elif corruption == "identity":
        member["inputs"]["security_snapshot"]["items"][0]["research_observation_identity_binding"][
            "instrument_id"
        ] = str(uuid4())
    elif corruption == "time":
        member["request"]["decision_time"] = "2026-10-30T13:30:00Z"
    source = json.dumps(archive).encode()
    expected = "0" * 64 if corruption == "source_hash" else hashlib.sha256(source).hexdigest()
    with pytest.raises(ValueError, match="COMPARISON_"):
        prepare_archive(source, expected_source_hash=expected, experiment_id=uuid4())


def test_cli_failure_does_not_print_original_private_values(tmp_path, monkeypatch, capsys):
    source = tmp_path / "source.json"
    source.write_text(json.dumps({"members": [{"private": "sensitive-synthetic-value"}]}))
    monkeypatch.setattr(
        "sys.argv",
        [
            "nexus-engine-research-comparison",
            "prepare",
            "--source",
            str(source),
            "--source-sha256",
            hashlib.sha256(source.read_bytes()).hexdigest(),
            "--experiment-id",
            str(uuid4()),
            "--output",
            str(tmp_path / "output.json"),
        ],
    )
    assert main() == 1
    captured = capsys.readouterr()
    assert "sensitive-synthetic-value" not in captured.out + captured.err
    assert json.loads(captured.out)["status"] == "rejected"


@pytest.mark.parametrize(
    "error,reason",
    [
        (BudgetExhausted("private diagnostic"), "COMPARISON_BUDGET_EXHAUSTED"),
        (ValueError("FORMAL_BUDGET_COVERAGE_UNKNOWN"), "FORMAL_BUDGET_COVERAGE_UNKNOWN"),
        (ValueError("private diagnostic"), "COMPARISON_PREPARATION_FAILED"),
    ],
)
def test_native_cli_preserves_safe_budget_reason_without_raw_diagnostics(
    error, reason, tmp_path, monkeypatch, capsys
):
    async def refuse(args):
        raise error

    monkeypatch.setattr("nexus.ops.research_comparison.native_command", refuse)
    monkeypatch.setattr(
        "sys.argv",
        [
            "nexus-engine-research-comparison",
            "admit-position",
            "--repo-root",
            str(tmp_path),
            "--position",
            "1",
            "--source",
            "unused",
            "--prepared",
            "unused",
            "--manifest",
            "unused",
            "--model-config-id",
            str(uuid4()),
        ],
    )
    assert main() == 1
    output = capsys.readouterr().out
    assert json.loads(output)["reason"] == reason
    assert "private diagnostic" not in output
