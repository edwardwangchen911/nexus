"""A new bounded observation retains its source and cannot repeat a dispatch."""

import json

import pytest
from unit.agents.test_historical_assessment_source import assessment


@pytest.fixture
def source(tmp_path, monkeypatch, capsys):
    return assessment(tmp_path, monkeypatch, capsys)


@pytest.mark.asyncio
async def test_independent_observation_reuses_research_and_has_real_deadline(source, monkeypatch):
    from nexus.agents.responses_budget import UsageSnapshot
    from nexus.ops import synthesis_observation as obs
    from nexus.paper.historical_assessment_source import read_archive

    before = source.read_bytes()
    root = source.parents[3]

    async def allowed(*args, **kwargs):
        pass

    monkeypatch.setattr(obs, "check_current_configuration", allowed)
    directory = await obs.prepare(root, source)
    original = read_archive(source)
    calls = []

    async def invoke(payload, **options):
        calls.append(options)
        result = {**original["outputs"][3]["intent"], "input_hash": payload["input_hash"]}
        return json.dumps(result), UsageSnapshot(
            input_tokens=100, output_tokens=50, provider_calls=1, tool_calls=0
        )

    monkeypatch.setattr(obs, "invoke_codex", invoke)
    result = await obs.run(directory)
    assert result["status"] == "observed"
    assert len(calls) == 1 and calls[0]["timeout_seconds"] == 83
    assert calls[0]["deadline_at"] is not None
    derived = obs.read_observation(directory)
    assert derived["outputs"][:3] == original["outputs"][:3]
    assert derived["case"]["completion"]["id"] != original["case"]["completion"]["id"]
    assert derived["synthesis_observation"]["source_archive_hash"] == original["archive_hash"]
    assert source.read_bytes() == before
    with pytest.raises(ValueError, match="OBSERVATION_ALREADY_ATTEMPTED"):
        await obs.run(directory)
    assert len(calls) == 1
    with pytest.raises(ValueError, match="OBSERVATION_ALREADY_EXISTS"):
        await obs.prepare(root, source)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["timeout", "invalid", "unknown"])
async def test_failure_keeps_claim_and_usage_no_retry(source, monkeypatch, failure):
    from nexus.agents.responses_budget import UsageSnapshot
    from nexus.ops import synthesis_observation as obs

    async def allowed(*args, **kwargs):
        pass

    monkeypatch.setattr(obs, "check_current_configuration", allowed)
    directory = await obs.prepare(source.parents[3], source)
    calls = []

    async def invoke(payload, **options):
        calls.append(1)
        if failure == "timeout":
            raise TimeoutError()
        if failure == "unknown":
            raise RuntimeError("private must not leak")
        return "{}", UsageSnapshot(
            input_tokens=100, output_tokens=50, provider_calls=1, tool_calls=0
        )

    monkeypatch.setattr(obs, "invoke_codex", invoke)
    result = await obs.run(directory)
    assert result["status"] == "unknown"
    assert result["usage_complete"] is (failure == "invalid")
    assert "private must not leak" not in json.dumps(result)
    assert (directory / "claim.json").exists()
    with pytest.raises(ValueError, match="OBSERVATION_ALREADY_ATTEMPTED"):
        await obs.run(directory)
    assert len(calls) == 1
    with pytest.raises(ValueError, match="OBSERVATION_NOT_COMPLETE"):
        obs.read_observation(directory)


@pytest.mark.asyncio
async def test_sdk_boundary_receives_common_deadline_and_read_revalidates(source, monkeypatch):
    from datetime import UTC, datetime
    from uuid import uuid4

    from nexus.agents import codex_sdk
    from nexus.agents.responses_budget import UsageSnapshot
    from nexus.ops import synthesis_observation as obs
    from nexus.paper.historical_assessment_source import project_archive, read_archive

    async def allowed(*args, **kwargs):
        pass

    monkeypatch.setattr(obs, "check_current_configuration", allowed)
    directory = await obs.prepare(source.parents[3], source)
    original = read_archive(source)
    calls = []

    async def actual_boundary(
        payload, *, invocation_state, output_model, max_wire_bytes, timeout_seconds
    ):
        calls.append(timeout_seconds)
        assert timeout_seconds == 83
        assert invocation_state.stage_deadline is not None
        assert max_wire_bytes == 64000
        return json.dumps(
            {**original["outputs"][3]["intent"], "input_hash": payload["input_hash"]}
        ), UsageSnapshot(100, 50, 0, 1)

    monkeypatch.setattr(codex_sdk, "_invoke_codex", actual_boundary)
    assert (await obs.run(directory))["status"] == "observed"
    assert calls == [83]
    derived = obs.read_observation(directory)
    projected = project_archive(
        derived, cycle_id=uuid4(), model_id=uuid4(), imported_at=datetime.now(UTC)
    )
    assert all(c["action"] == "defer" for c in projected["synthesis"]["intent"]["choices"])
    receipt = directory / "result.json"
    saved = json.loads(receipt.read_text())
    for patch in (
        {"elapsed_ms": 90001},
        {"elapsed_ms": -1},
        {"usage": {**saved["usage"], "provider_calls": 2}},
    ):
        receipt.write_text(json.dumps({**saved, **patch}))
        with pytest.raises(ValueError, match="OBSERVATION_(BINDING|IDENTITY)_INVALID"):
            obs.read_observation(directory)
    receipt.write_text(json.dumps(saved))


@pytest.mark.asyncio
async def test_future_source_refused_before_claim_or_model(source, monkeypatch):
    from datetime import UTC, datetime, timedelta

    from nexus.ops import synthesis_observation as obs

    async def allowed(*args, **kwargs):
        pass

    monkeypatch.setattr(obs, "check_current_configuration", allowed)
    directory = await obs.prepare(source.parents[3], source)
    real_load = obs._load

    def future(*args, **kwargs):
        manifest, archive = real_load(*args, **kwargs)
        archive["observed_finished_at"] = (datetime.now(UTC) + timedelta(days=1)).isoformat()
        return manifest, archive

    monkeypatch.setattr(obs, "_load", future)
    with pytest.raises(ValueError, match="OBSERVATION_SOURCE_CHANGED"):
        await obs.run(directory)
    assert not (directory / "claim.json").exists()


@pytest.mark.asyncio
async def test_concurrent_claim_allows_only_one_call(source, monkeypatch):
    import asyncio

    from nexus.ops import synthesis_observation as obs

    async def allowed(*args, **kwargs):
        await asyncio.sleep(0)

    monkeypatch.setattr(obs, "check_current_configuration", allowed)
    directory = await obs.prepare(source.parents[3], source)
    calls = []

    async def invoke(*args, **kwargs):
        calls.append(1)
        raise asyncio.CancelledError()

    monkeypatch.setattr(obs, "invoke_codex", invoke)
    results = await asyncio.gather(obs.run(directory), obs.run(directory), return_exceptions=True)
    assert calls == [1]
    assert sum(isinstance(r, ValueError) for r in results) == 1
    assert json.loads((directory / "result.json").read_text())["code"] == "OBSERVATION_CANCELLED"


@pytest.mark.parametrize(
    "error_code,expected",
    [
        ("OBSERVATION_NOT_COMPLETE", "OBSERVATION_NOT_COMPLETE"),
        ("OBSERVATION_SOURCE_CHANGED", "OBSERVATION_SOURCE_CHANGED"),
        ("OBSERVATION_BINDING_INVALID", "OBSERVATION_BINDING_INVALID"),
        ("ARCHIVE_ISOLATED_DATABASE_REQUIRED", "ARCHIVE_ISOLATED_DATABASE_REQUIRED"),
        ("OBSERVATION_private_response_do_not_print", "UNKNOWN"),
    ],
)
def test_consumer_cli_preserves_only_fixed_observation_codes(
    monkeypatch, capsys, error_code, expected
):
    import sys

    from nexus.ops import historical_assessment, openai_local

    monkeypatch.setattr(openai_local, "local_settings", lambda *args, **kwargs: object())

    async def refuse(*args, **kwargs):
        assert kwargs["synthesis_observation"] is True
        raise ValueError(error_code)

    monkeypatch.setattr(historical_assessment, "execute", refuse)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "historical_assessment",
            "--observation-dir",
            "/private/case",
            "--source-repo",
            "/private/repo",
        ],
    )
    assert historical_assessment.main() == 1
    assert json.loads(capsys.readouterr().out) == {"status": "failed", "code": expected}
