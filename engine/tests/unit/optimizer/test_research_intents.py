# ruff: noqa: E501
"""Strict, non-executable Agent research intent contracts."""

import pytest
from jsonschema import Draft202012Validator
from nexus.strategy.intents import ResearchIntent
from pydantic import ValidationError


def test_research_intent_rejects_agent_numeric_targets_and_unsafe_text() -> None:
    """Accepting a target weight, shell, URL, or arbitrary field would bypass optimizer authority."""
    body = {
        "schema_version": "research_intent_v1",
        "decision_id": "0198d500-1000-7000-8000-000000000111",
        "instrument_id": "0198d500-1000-7000-8000-000000000112",
        "display_symbol": "ABC",
        "decision_time": "2026-08-24T00:00:00Z",
        "data_snapshot_id": "0198d500-1000-7000-8000-000000000113",
        "horizon": "medium_term",
        "stance": "buy_candidate",
        "exposure_action": "eligible_for_increase",
        "theses": [
            {
                "text": "durable demand",
                "evidence_refs": [
                    {
                        "source": "fixture",
                        "object_id": "e1",
                        "available_at": "2026-08-23T00:00:00Z",
                        "location": "fixture://e1",
                        "content_hash": "a" * 64,
                    }
                ],
            }
        ],
        "counter_theses": [],
        "uncertainties": ["margin pressure"],
        "invalidation_conditions": ["price_below_200d_sma"],
        "entry_conditions": ["quality_pass"],
        "exit_conditions": ["invalidation_triggered"],
        "valid_until": "2026-09-01T00:00:00Z",
        "quality_status": "complete",
        "evidence_audit": "complete",
        "role_votes": {"fundamental": "support"},
        "disagreement": "none",
        "strategy_version": "baseline_v1",
        "model_version": "model_fixture_v1",
        "prompt_version": "prompt_fixture_v1",
        "data_version": "data_fixture_v1",
        "evidence_refs": [
            {
                "source": "fixture",
                "object_id": "e1",
                "available_at": "2026-08-23T00:00:00Z",
                "location": "fixture://e1",
                "content_hash": "a" * 64,
            }
        ],
    }
    assert ResearchIntent.model_validate(body).display_symbol == "ABC"
    for forbidden in (
        {"target_weight": "0.2"},
        {
            "theses": [
                {
                    "text": "curl https://example.invalid",
                    "evidence_refs": body["theses"][0]["evidence_refs"],
                }
            ]
        },
    ):
        with pytest.raises(ValidationError):
            ResearchIntent.model_validate({**body, **forbidden})


def test_research_intent_json_schema_rejects_unsafe_agent_text() -> None:
    """The published schema must enforce the same no-URL/no-shell contract as Pydantic."""
    from json import loads
    from pathlib import Path

    evidence = {
        "source": "fixture",
        "object_id": "e1",
        "available_at": "2026-08-23T00:00:00Z",
        "location": "fixture://e1",
        "content_hash": "a" * 64,
    }
    payload = {
        "schema_version": "research_intent_v1",
        "decision_id": "0198d500-1000-7000-8000-000000000111",
        "instrument_id": "0198d500-1000-7000-8000-000000000112",
        "display_symbol": "ABC",
        "decision_time": "2026-08-24T00:00:00Z",
        "data_snapshot_id": "0198d500-1000-7000-8000-000000000113",
        "horizon": "medium_term",
        "stance": "buy_candidate",
        "exposure_action": "eligible_for_increase",
        "theses": [{"text": "curl https://example.invalid", "evidence_refs": [evidence]}],
        "counter_theses": [],
        "uncertainties": [],
        "invalidation_conditions": [],
        "entry_conditions": [],
        "exit_conditions": [],
        "valid_until": "2026-09-01T00:00:00Z",
        "quality_status": "complete",
        "evidence_audit": "complete",
        "role_votes": {"fundamental": "support"},
        "disagreement": "none",
        "strategy_version": "baseline_v1",
        "model_version": "model_fixture_v1",
        "prompt_version": "prompt_fixture_v1",
        "data_version": "data_fixture_v1",
        "evidence_refs": [evidence],
    }
    schema_path = Path(__file__).parents[4] / "contracts/agent/research-intent-v1.schema.json"
    schema = loads(schema_path.read_text())
    assert list(Draft202012Validator(schema).iter_errors(payload))


def test_research_intent_json_schema_rejects_external_evidence_url() -> None:
    """Evidence identifiers may be fixture references, never executable external URLs."""
    from json import loads
    from pathlib import Path

    schema_path = Path(__file__).parents[4] / "contracts/agent/research-intent-v1.schema.json"
    schema = loads(schema_path.read_text())
    assert list(
        Draft202012Validator(schema).iter_errors(
            {
                "schema_version": "research_intent_v1",
                "decision_id": "0198d500-1000-7000-8000-000000000111",
                "instrument_id": "0198d500-1000-7000-8000-000000000112",
                "display_symbol": "ABC",
                "decision_time": "2026-08-24T00:00:00Z",
                "data_snapshot_id": "0198d500-1000-7000-8000-000000000113",
                "horizon": "medium_term",
                "stance": "hold",
                "exposure_action": "maintain",
                "theses": [],
                "counter_theses": [],
                "uncertainties": [],
                "invalidation_conditions": [],
                "entry_conditions": [],
                "exit_conditions": [],
                "valid_until": "2026-09-01T00:00:00Z",
                "quality_status": "complete",
                "evidence_audit": "complete",
                "role_votes": {"fundamental": "support"},
                "disagreement": "none",
                "strategy_version": "baseline_v1",
                "model_version": "model_fixture_v1",
                "prompt_version": "prompt_fixture_v1",
                "data_version": "data_fixture_v1",
                "evidence_refs": [
                    {
                        "source": "fixture",
                        "object_id": "e1",
                        "available_at": "2026-08-23T00:00:00Z",
                        "location": "https://example.invalid/evidence",
                        "content_hash": "a" * 64,
                    }
                ],
            }
        )
    )
