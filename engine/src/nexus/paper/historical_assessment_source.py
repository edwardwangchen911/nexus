"""Immutable archive provenance and explicit normalization, never trading authority."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid5

from nexus.agents.portfolio_synthesis_contracts import (
    PortfolioSynthesisInputV1,
    PortfolioSynthesisIntentV1,
    canonical_portfolio_synthesis_hash,
    validate_portfolio_synthesis_intent,
)
from nexus.domain.time import require_utc
from nexus.ops.historical_pool_diagnostic import ASSESSMENT_PROFILE, CaseStore, verify_results
from nexus.strategy.intents import ResearchIntent

PROFILE = "historical-archive-consumer-v1"
MODEL_HASH = canonical_portfolio_synthesis_hash(
    {"profile": PROFILE, "model_calls": 0, "execution_allowed": False}
)


def read_archive(path: Path) -> dict[str, Any]:
    """Validate all originals before making a new, explicitly derived business identity."""
    with CaseStore.open(path, read_only=True) as store:
        before = hashlib.sha256(path.read_bytes()).hexdigest()
        summary = store.summary()
        case = store.case()
        outputs = [
            json.loads(row[0])
            for row in store.db.execute("SELECT payload FROM historical_outputs ORDER BY ordinal")
        ]
    if hashlib.sha256(path.read_bytes()).hexdigest() != before:
        raise ValueError("ARCHIVE_CHANGED_DURING_READ")
    if case["profile"] != ASSESSMENT_PROFILE or summary["status"] != "observed":
        raise ValueError("ARCHIVE_ASSESSMENT_REQUIRED")
    verify_results(case, outputs)
    if not any(row["intent"]["quality_status"] == "partial" for row in outputs[:3]) or any(
        choice["action"] != "defer" for choice in outputs[3]["intent"]["choices"]
    ):
        raise ValueError("ARCHIVE_NONEXECUTING_RESULT_REQUIRED")
    return dict(
        case=case,
        outputs=outputs,
        archive_hash=before,
        observed_started_at=summary["observation_started_at"],
        observed_finished_at=summary["observation_finished_at"],
    )


def project_archive(
    archive: dict[str, Any], *, cycle_id: UUID, model_id: UUID, imported_at: datetime
) -> dict[str, Any]:
    """Only identities are normalized; original findings, gaps and times remain immutable."""
    require_utc(imported_at)
    case, outputs = archive["case"], archive["outputs"]
    verify_results(case, outputs)
    evidence = datetime.fromisoformat(case["evidence_as_of"])
    started = datetime.fromisoformat(archive["observed_started_at"])
    finished = datetime.fromisoformat(archive["observed_finished_at"])
    if not evidence <= started <= finished <= imported_at:
        raise ValueError("ARCHIVE_AVAILABILITY_INVALID")
    research = []
    securities = []
    for member, raw, security in zip(
        case["members"], outputs[:3], outputs[3]["frozen_input"]["security_outputs"], strict=True
    ):
        instrument = member["request"]["instrument_id"]
        run_id = str(uuid5(cycle_id, f"research:{instrument}"))
        output_id = str(uuid5(cycle_id, f"output:{instrument}"))
        intent = ResearchIntent.model_validate({**raw["intent"], "decision_id": run_id})
        intent_json = intent.model_dump(mode="json")
        payload = {
            "archive_profile": PROFILE,
            "source_output_id": raw["output_id"],
            "roles": raw["roles"],
        }
        content_hash = canonical_portfolio_synthesis_hash(
            dict(status="completed", role=None, payload=payload, intent=intent_json)
        )
        research.append(
            dict(
                run_id=run_id,
                output_id=output_id,
                source_run_id=member["source_run_id"],
                source_output_id=raw["output_id"],
                instrument_id=instrument,
                intent=intent_json,
                payload=payload,
                content_hash=content_hash,
                evidence_ids=member["evidence_ids"],
            )
        )
        securities.append(
            {
                **security,
                "research_output_id": output_id,
                "content_hash": content_hash,
                "intent": intent_json,
                "intent_hash": canonical_portfolio_synthesis_hash(intent_json),
            }
        )
    # This is an archive-processing window, not a renewal of any execution window.
    frozen = PortfolioSynthesisInputV1.model_validate(
        {
            **outputs[3]["frozen_input"],
            "cycle_id": str(cycle_id),
            "security_outputs": securities,
            "model_config_id": str(model_id),
            "model_hash": MODEL_HASH,
            "pricing_version": PROFILE,
            "admitted_at": imported_at.isoformat(),
            "expires_at": (imported_at + timedelta(seconds=60)).isoformat(),
        }
    )
    input_hash = canonical_portfolio_synthesis_hash(frozen)
    raw_intent = outputs[3]["intent"]
    mapping = {
        raw["output_id"]: derived["output_id"]
        for raw, derived in zip(outputs[:3], research, strict=True)
    }

    # The new input binding is explicitly derived, never attributed to the old model call.
    def replace(value: Any) -> Any:
        if isinstance(value, dict):
            return {key: replace(child) for key, child in value.items()}
        if isinstance(value, list):
            return [replace(child) for child in value]
        if isinstance(value, str):
            return mapping.get(value, value)
        return value

    normalized = replace(raw_intent)
    normalized["input_hash"] = input_hash
    synthesis_intent = PortfolioSynthesisIntentV1.model_validate(normalized)
    validate_portfolio_synthesis_intent(synthesis_intent, frozen)
    run_id = str(uuid5(cycle_id, "synthesis"))
    output_id = str(uuid5(cycle_id, "synthesis-output"))
    content_hash = canonical_portfolio_synthesis_hash(
        {
            "schema_version": "portfolio_synthesis_terminal_v1",
            "status": "completed",
            "intent": synthesis_intent.model_dump(mode="json"),
        }
    )
    return dict(
        research=research,
        synthesis=dict(
            run_id=run_id,
            output_id=output_id,
            frozen_input=frozen.model_dump(mode="json"),
            intent=synthesis_intent.model_dump(mode="json"),
            input_hash=input_hash,
            content_hash=content_hash,
            binding_hash=canonical_portfolio_synthesis_hash(
                {
                    "schema_version": "portfolio_synthesis_binding_v1",
                    "run_id": UUID(run_id),
                    "input_hash": input_hash,
                    "content_hash": content_hash,
                }
            ),
        ),
    )
