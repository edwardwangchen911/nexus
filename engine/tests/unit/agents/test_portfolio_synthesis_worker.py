"""One-turn synthesis prompt and transport boundaries."""

from copy import deepcopy
from pathlib import Path
from uuid import UUID

import pytest
from nexus.agents.codex_sdk import codex_request_wire
from nexus.agents.responses_budget import UsageSnapshot


def _decode_portfolio_view(value: object, strings: tuple[str, ...] = ()) -> object:
    """Independent inverse of the model-facing column and string-table notation."""
    if isinstance(value, str) and value.startswith("$nexus:") and strings:
        return strings[int(value.removeprefix("$nexus:"))]
    if isinstance(value, list):
        return [_decode_portfolio_view(item, strings) for item in value]
    if not isinstance(value, dict):
        return value
    if "$nexus_strings" in value:
        strings = tuple(value["$nexus_strings"])
        value = {key: item for key, item in value.items() if key != "$nexus_strings"}
    if "$nexus_columns" in value:
        return [
            dict(
                zip(
                    value["$nexus_columns"],
                    (_decode_portfolio_view(item, strings) for item in row),
                    strict=True,
                )
            )
            for row in value["rows"]
        ]
    return {key: _decode_portfolio_view(item, strings) for key, item in value.items()}


@pytest.mark.asyncio
async def test_three_large_security_outputs_fit_synthesis_wire_without_losing_frozen_facts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nexus.agents import portfolio_synthesis_worker as worker

    shared_authority = "a" * 64
    frozen = {
        "candidate_instrument_ids": [str(UUID(int=index)) for index in (1, 2, 3)],
        "security_outputs": [
            {
                "instrument_id": str(UUID(int=index)),
                "research_output_id": str(UUID(int=index + 10)),
                "intent": {
                    "display_symbol": symbol,
                    "thesis": symbol * 4300,
                    "evidence_refs": [
                        {"source_hash": shared_authority, "instrument_id": str(UUID(int=index))}
                        for _ in range(55)
                    ],
                },
            }
            for index, symbol in enumerate(("AAPL", "AVGO", "NVDA"), 1)
        ],
    }
    payload = {
        "instructions": "Use only frozen facts.",
        "input_hash": "b" * 64,
        "frozen_input": frozen,
    }
    original = deepcopy(payload)
    assert len(codex_request_wire(payload).encode()) > 64000
    submitted: dict[str, object] = {}

    async def fake_invoke(value, *, output_model, max_wire_bytes):
        submitted.update(value)
        return "{}", UsageSnapshot(input_tokens=1, output_tokens=1, provider_calls=1)

    monkeypatch.setattr(worker, "invoke_codex_portfolio", fake_invoke)
    await worker.CodexSynthesisTransport().invoke(payload)

    assert len(codex_request_wire(submitted).encode()) <= 64000
    assert _decode_portfolio_view(submitted["frozen_input"]) == frozen
    assert payload == original


def test_prompt_bounds_investment_choices_without_execution_authority():
    path = (
        Path(__file__).parents[3] / "src/nexus/agents/prompts/portfolio-management-synthesis-v1.md"
    )
    assert path.exists(), "the exact registered synthesis prompt must exist"
    prompt = " ".join(path.read_text().lower().split())
    for boundary in (
        "weights",
        "amounts",
        "quantities",
        "orders",
        "risk overrides",
        "unbound instruments",
        "strict priority",
        "abstain",
    ):
        assert boundary in prompt


def test_synthesis_worker_exposes_explicit_once_entrypoint():
    from nexus.worker import run_portfolio_synthesis_worker_once

    assert callable(run_portfolio_synthesis_worker_once)


def test_candidate_transport_requires_explicit_exact_canonical_model():
    from datetime import datetime
    from decimal import Decimal

    from nexus.agents.codex_candidate import candidate_config, candidate_config_hash
    from nexus.agents.codex_versions import CANDIDATE_BUDGET_CONFIG_VERSION
    from nexus.agents.portfolio_synthesis_worker import CodexSynthesisTransport
    from nexus.persistence.models.agents import AgentModelConfigRecord

    config = candidate_config()
    model = AgentModelConfigRecord(
        version=CANDIDATE_BUDGET_CONFIG_VERSION,
        config_hash=candidate_config_hash(),
        model_id=config["model_id"],
        max_input_tokens=96000,
        max_output_tokens=8192,
        max_tool_calls=0,
        input_usd_per_million=Decimal(0),
        output_usd_per_million=Decimal(0),
        pricing_available_at=datetime.fromisoformat(config["pricing_available_at"]),
        pricing_expires_at=datetime.fromisoformat(config["pricing_expires_at"]),
    )
    assert not CodexSynthesisTransport().supports(model)
    assert CodexSynthesisTransport(candidate_stage_capacity=True).supports(model)
    model.config_hash = "0" * 64
    assert not CodexSynthesisTransport(candidate_stage_capacity=True).supports(model)


@pytest.mark.parametrize("flag", [1, "true", None])
def test_candidate_transport_profile_is_strict_boolean(flag):
    from nexus.agents.portfolio_synthesis_worker import CodexSynthesisTransport

    with pytest.raises(ValueError, match="CODEX_CANDIDATE_ISOLATION_REQUIRED"):
        CodexSynthesisTransport(candidate_stage_capacity=flag)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed",
    [
        {"candidate_synthesis_profile": "true"},
        {"lease_seconds": 119},
        {"job_ids": ()},
        {"locked_job_guard": None},
        {"job_types": ("agent.paper.research",)},
    ],
)
async def test_candidate_synthesis_claim_profile_requires_exact_locked_scope(changed):
    from datetime import UTC, datetime
    from uuid import uuid4

    from nexus.jobs.models import QueueName
    from nexus.jobs.repository import claim_next_job

    async def guard(session, job):
        raise AssertionError("invalid profile reached guard")

    args = dict(
        queue=QueueName.GENERAL,
        worker_id="profile-test",
        now=datetime(2026, 10, 10, tzinfo=UTC),
        lease_seconds=120,
        job_types=("agent.portfolio_synthesis",),
        job_ids=(uuid4(),),
        locked_job_guard=guard,
        candidate_synthesis_profile=True,
    )
    # None deliberately has no SQL methods: invalid arguments must reject before SQL.
    with pytest.raises(
        ValueError,
        match="CANDIDATE_SYNTHESIS_PROFILE_REQUIRES_LOCKED_SCOPE|LOCKED_JOB_GUARD_REQUIRES_EXACT_SCOPE",
    ):
        await claim_next_job(None, **(args | changed))


@pytest.mark.asyncio
async def test_formal_handler_deadline_includes_setup_and_does_not_fabricate_terminal(monkeypatch):
    import asyncio
    from unittest.mock import AsyncMock

    from nexus.agents import portfolio_synthesis_worker as worker

    # Reproduce the CI clock value whose subtraction loses float precision.
    from nexus.agents.synthesis_lifecycle import SynthesisDeadline

    monkeypatch.setattr(worker, "SynthesisDeadline", lambda _: SynthesisDeadline(201.463161707))
    timers = []
    timeout_at = asyncio.timeout_at

    def capture(at):
        timer = timeout_at(at)
        timers.append(timer)
        return timer

    monkeypatch.setattr(asyncio, "timeout_at", capture)

    async def blocked(*args, **kwargs):
        lifecycle = kwargs["lifecycle"]
        assert lifecycle.work_at == lifecycle.started_at + 83
        assert lifecycle.cleanup_at == lifecycle.started_at + 90
        assert lifecycle.handler_at == lifecycle.started_at + 105
        timers[0].reschedule(asyncio.get_running_loop().time())
        await asyncio.Event().wait()

    monkeypatch.setattr(worker, "_run_synthesis_job_once", blocked)
    finish = AsyncMock()
    monkeypatch.setattr(worker, "_finish", finish)
    with pytest.raises(ValueError, match="^PORTFOLIO_SYNTHESIS_HANDLER_RESULT_UNKNOWN$"):
        await worker.run_synthesis_job_once(
            None, transport=None, worker_id="isolated", clock=None, formal_stage_capacity=True
        )
    finish.assert_not_called()


@pytest.mark.asyncio
async def test_commit_timeout_does_not_write_replacement_terminal(monkeypatch):
    from unittest.mock import AsyncMock

    from nexus.agents import portfolio_synthesis_worker as worker

    async def uncertain(*args, **kwargs):
        raise TimeoutError("commit acknowledgement unknown")

    monkeypatch.setattr(worker, "_run_synthesis_job_once", uncertain)
    finish = AsyncMock()
    monkeypatch.setattr(worker, "_finish", finish)
    with pytest.raises(ValueError, match="^PORTFOLIO_SYNTHESIS_HANDLER_RESULT_UNKNOWN$"):
        await worker.run_synthesis_job_once(
            None, transport=None, worker_id="isolated", clock=None, formal_stage_capacity=True
        )
    finish.assert_not_called()


def test_handler_unknown_emits_only_fixed_safe_diagnostic(capsys):
    import json
    from datetime import UTC, datetime

    from nexus.agents.portfolio_synthesis_worker import SynthesisHandlerResultUnknown
    from nexus.ops.agent_paper_loop import AgentPaperLoopAction, _emit_agent_paper_unknown_failure

    error = SynthesisHandlerResultUnknown()
    error.__cause__ = TimeoutError("PRIVATE_COMMIT_DETAILS")
    _emit_agent_paper_unknown_failure(
        "execute",
        datetime(2026, 10, 20, tzinfo=UTC),
        action=AgentPaperLoopAction.RUN_RESEARCH_JOB,
        error=error,
    )
    raw = capsys.readouterr().err
    payload = json.loads(raw)
    assert payload["failure_phase"] == "portfolio_synthesis_handler"
    assert payload["failure_code"] == "PORTFOLIO_SYNTHESIS_HANDLER_RESULT_UNKNOWN"
    assert "PRIVATE_COMMIT_DETAILS" not in raw
