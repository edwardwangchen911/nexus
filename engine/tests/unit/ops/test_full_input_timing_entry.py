import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest


@pytest.mark.asyncio
async def test_normal_diagnostic_entry_marks_attempt_before_call_and_never_replays(
    tmp_path, monkeypatch
):
    from nexus.ops import full_input_timing_diagnostic as ops

    output = tmp_path / "case"
    tmp_path.chmod(0o700)
    calls = []
    committee = SimpleNamespace(
        stage_timings={},
        diagnostic_stream_timings={},
        failure_diagnostic=None,
        failure_reason=None,
        usage=SimpleNamespace(provider_calls=0, input_tokens=0, output_tokens=0),
    )

    async def analyze(*args):
        assert (output / "attempt.json").exists()
        calls.append(1)
        raise TimeoutError()

    committee.analyze = analyze
    case = SimpleNamespace(
        committee=committee,
        request=object(),
        evidence=(),
        prompts={r: object() for r in ops.ROLE_ORDER},
        summary={"candidate_count": 3},
    )
    monkeypatch.setattr(ops, "load_case", AsyncMock(return_value=case))
    result = await ops.execute_timing_diagnostic(object(), run_id=uuid4(), output_dir=output)
    assert result["status"] == "failed"
    assert result["formal_acceptance"] is False
    assert result["business_writes"] == 0
    assert result["retry_allowed"] is False
    assert result["failure_stage"] == "research"
    assert result["wall_elapsed_ms"] >= 0
    assert result["source_load_validate_compile_ms"] >= 0
    assert result["stage_wall_elapsed_ms"]["research"] >= 0
    assert len(calls) == 1
    with pytest.raises(ValueError, match="TIMING_DIAGNOSTIC_ALREADY_ATTEMPTED"):
        await ops.execute_timing_diagnostic(object(), run_id=uuid4(), output_dir=output)
    assert len(calls) == 1
    assert json.loads((output / "result.json").read_text())["status"] == "failed"


@pytest.mark.asyncio
async def test_diagnostic_rejects_public_directory_before_loading(tmp_path, monkeypatch):
    from nexus.ops import full_input_timing_diagnostic as ops

    tmp_path.chmod(0o755)
    loader = AsyncMock()
    monkeypatch.setattr(ops, "load_case", loader)
    with pytest.raises(ValueError, match="DIAGNOSTIC_DIRECTORY_UNSAFE"):
        await ops.execute_timing_diagnostic(object(), run_id=uuid4(), output_dir=tmp_path / "case")
    loader.assert_not_awaited()


@pytest.mark.parametrize(
    "version",
    ["codex-sdk-chatgpt-v9-full-input-capacity", "codex-sdk-chatgpt-v10-flexible-stage-budget"],
)
@pytest.mark.parametrize(
    "damage", [None, "omitted", "hash", "model", "active", "mixed-version", "unsupported"]
)
def test_source_pool_uses_manifest_chain_and_accepts_completed(damage, version):
    from nexus.ops.full_input_timing_diagnostic import validate_source_pool
    from unit.test_research_capacity_pool import case

    cycle, events, runs = case()
    model = uuid4()
    for run in runs:
        run.status = "completed"
        run.model_config_id = model
        run.model_hash = "a" * 64
        run.pricing_version = version
    if damage == "mixed-version":
        runs[1].pricing_version = "codex-sdk-chatgpt-v8-stage-capacity"
    if damage == "unsupported":
        for run in runs:
            run.pricing_version = "unrecognized"
    if damage == "omitted":
        runs = runs[:1]
    if damage == "hash":
        cycle.candidate_hash = "f" * 64
    if damage == "model":
        runs[1].model_config_id = uuid4()
    if damage == "active":
        runs[1].status = "running"
    if damage:
        with pytest.raises(ValueError):
            validate_source_pool(cycle, events, runs, runs[0])
    else:
        assert len(validate_source_pool(cycle, events, runs, runs[0])) == 2


def test_cli_uses_canonical_source_attempt_directory(tmp_path, monkeypatch, capsys):
    import sys

    from nexus.ops import full_input_timing_diagnostic as ops

    (tmp_path / ".secrets").mkdir(mode=0o700)
    rid = uuid4()
    settings = object()
    monkeypatch.setattr(ops, "local_settings", lambda root, backend: settings)
    execute = AsyncMock(
        return_value={
            "status": "observed",
            "profile": ops.PROFILE,
            "business_writes": 0,
            "formal_acceptance": False,
            "retry_allowed": False,
        }
    )
    monkeypatch.setattr(ops, "execute_timing_diagnostic", execute)
    monkeypatch.setattr(
        sys, "argv", ["diagnostic", "--repo-root", str(tmp_path), "--source-run-id", str(rid)]
    )
    assert ops.main() == 0
    assert execute.call_args.kwargs[
        "output_dir"
    ] == tmp_path / ".secrets/nexus-full-input-timing" / str(rid)
    assert execute.call_args.kwargs["run_id"] == rid
    assert str(rid) not in capsys.readouterr().out


@pytest.mark.asyncio
async def test_source_failure_is_sanitized_and_timed_without_model_call(tmp_path, monkeypatch):
    from nexus.ops import full_input_timing_diagnostic as ops

    tmp_path.chmod(0o700)
    monkeypatch.setattr(ops, "load_case", AsyncMock(side_effect=ValueError("secret raw body")))
    result = await ops.execute_timing_diagnostic(
        object(), run_id=uuid4(), output_dir=tmp_path / "one"
    )
    assert result["failure_stage"] == "source_load_validate_compile"
    assert result["code"] == "TIMING_DIAGNOSTIC_NOT_COMPLETED"
    assert result["wall_elapsed_ms"] >= result["source_load_validate_compile_ms"] >= 0
    assert "secret" not in json.dumps(result)
