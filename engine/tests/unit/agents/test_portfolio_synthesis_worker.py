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
