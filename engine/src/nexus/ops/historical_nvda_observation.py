"""One explicitly separate NVDA observation, preserving the prior unknown case."""

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from sqlalchemy import func, select, text

from nexus.agents.codex_formal_stage import require_formal_stage_model_profile
from nexus.agents.codex_versions import BOUNDED_STAGE_BUDGET_CONFIG_VERSION
from nexus.agents.persistence import _shared_daily_budget_used
from nexus.agents.synthesis import staged_intent_semantics_valid
from nexus.domain.evidence import EvidenceRef
from nexus.ops.historical_pool_diagnostic import (
    NVDA_PROFILE,
    PROFILE,
    CaseStore,
    _hash,
    preflight_frozen_pool,
    validate_case,
)
from nexus.ops.openai_local import local_settings
from nexus.ops.research_budget_diagnostic import _private_directory
from nexus.persistence.session import session_factory
from nexus.strategy.intents import ResearchIntent

OBSERVATION_LIMITS = dict(
    research_calls=3,
    synthesis_calls=0,
    research_seconds=300,
    input_tokens=96000,
    output_tokens=8192,
)


def target_index(case: dict[str, Any]) -> int:
    try:
        metadata = case["observation"]
        UUID(metadata["id"])
        restored = {key: value for key, value in case.items() if key != "observation"}
        restored["profile"] = PROFILE
        matches = [i for i, row in enumerate(case["members"]) if row["symbol"] == "NVDA"]
        if (
            case["profile"] != NVDA_PROFILE
            or metadata["version"] != 1
            or metadata["limits"] != OBSERVATION_LIMITS
            or metadata["source_case_hash"] != _hash(restored)
            or not isinstance(metadata["source_receipt_hash"], str)
            or len(metadata["source_receipt_hash"]) != 64
            or any(c not in "0123456789abcdef" for c in metadata["source_receipt_hash"])
            or len(matches) != 1
            or metadata["target_instrument_id"]
            != case["members"][matches[0]]["request"]["instrument_id"]
        ):
            raise ValueError
        return matches[0]
    except (KeyError, TypeError, ValueError, IndexError):
        raise ValueError("HISTORICAL_OBSERVATION_INVALID") from None


def observation_directory(root: Path, cycle_id: UUID) -> Path:
    return root / ".secrets/nexus-historical-nvda-observations" / f"{cycle_id}-v1"


def read_source(root: Path, cycle_id: UUID) -> tuple[dict[str, Any], str]:
    parent = root / ".secrets/nexus-historical-three-stock"
    _private_directory(root / ".secrets")
    _private_directory(parent)
    with CaseStore.open(parent / f"{cycle_id}-v11/case.sqlite3", read_only=True) as store:
        store.db.execute("BEGIN")
        case = store.case()
        summary = store.summary()
        validate_case(case)
        matches = [i for i, row in enumerate(case["members"]) if row["symbol"] == "NVDA"]
        if (
            case["profile"] != PROFILE
            or "observation" in case
            or case["source_cycle_id"] != str(cycle_id)
            or len(matches) != 1
            or summary["status"] != "unknown"
            or summary["observation_started_at"] is None
            or summary["observation_finished_at"] is None
            or summary["stage"] != f"research_{matches[0] + 1}"
            or summary["diagnostic"].get("code") != "HISTORICAL_RESEARCH_NOT_ACTIONABLE"
        ):
            raise ValueError("HISTORICAL_OBSERVATION_INVALID")
        # Bind all persisted facts, including any earlier outputs; never update the source.
        facts = dict(
            case=list(store.db.execute("SELECT * FROM historical_case")),
            outputs=list(store.db.execute("SELECT * FROM historical_outputs ORDER BY ordinal")),
        )
        private = store.private_rejection()
        if private is not None:
            facts["rejection"] = [private]
        receipt = _hash(facts)
        return case, receipt


async def check_current_configuration(root: Path, case: dict[str, Any]) -> None:
    settings = local_settings(root, backend="codex_sdk")
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            await session.execute(
                text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            )
            now = await session.scalar(select(func.clock_timestamp()))
            if not isinstance(now, datetime):
                raise ValueError("HISTORICAL_CASE_CLOCK_INVALID")
            model = await require_formal_stage_model_profile(
                session, model_config_id=settings.agent_paper_formal_model_config_id, now=now
            )
            if (
                str(settings.agent_paper_account_id) != case["synthesis_template"]["account_id"]
                or model.version != BOUNDED_STAGE_BUDGET_CONFIG_VERSION
                or model.config_hash != case["model_hash"]
                or model.model_id != case["model_id"]
            ):
                raise ValueError("HISTORICAL_CASE_V11_REQUIRED")
            used = await _shared_daily_budget_used(session, business_date=now.date())
            if used + model.worst_case_cost_usd > settings.agent_daily_budget_usd:
                raise ValueError("HISTORICAL_CASE_BUDGET_UNAVAILABLE")
    finally:
        await factory.kw["bind"].dispose()


async def prepare_nvda_observation(root: Path, cycle_id: UUID) -> Path:
    root = root.resolve()
    directory = observation_directory(root, cycle_id)
    _private_directory(root / ".secrets")
    if directory.exists():
        raise ValueError("HISTORICAL_CASE_ALREADY_EXISTS")
    source, receipt = read_source(root, cycle_id)
    case = json.loads(json.dumps(source))
    index = next(i for i, row in enumerate(source["members"]) if row["symbol"] == "NVDA")
    case.update(
        profile=NVDA_PROFILE,
        observation=dict(
            version=1,
            id=str(uuid4()),
            repo_root=str(root),
            source_case_hash=_hash(source),
            source_receipt_hash=receipt,
            target_instrument_id=source["members"][index]["request"]["instrument_id"],
            prepared_at=datetime.now(UTC).isoformat(),
            limits=dict(OBSERVATION_LIMITS),
        ),
    )
    async with asyncio.timeout(195):
        await check_current_configuration(root, case)
        await preflight_frozen_pool(case)
    if not directory.parent.exists():
        directory.parent.mkdir(mode=0o700)
    _private_directory(directory.parent)
    directory.mkdir(mode=0o700)
    path = directory / "case.sqlite3"
    with CaseStore.create(path, case):
        pass
    return path


async def validate_observation(store: CaseStore) -> int:
    case = store.case()
    index = target_index(case)
    root = Path(case["observation"]["repo_root"])
    cycle_id = UUID(case["source_cycle_id"])
    path = Path(store.db.execute("PRAGMA database_list").fetchone()[2])
    directory = observation_directory(root, cycle_id)
    _private_directory(directory.parent)
    if path != directory / "case.sqlite3" or root != root.resolve():
        raise ValueError("HISTORICAL_OBSERVATION_INVALID")
    source, receipt = read_source(root, cycle_id)
    restored = {key: value for key, value in case.items() if key != "observation"}
    restored["profile"] = PROFILE
    if (
        _hash(source) != case["observation"]["source_case_hash"]
        or receipt != case["observation"]["source_receipt_hash"]
        or restored != source
    ):
        raise ValueError("HISTORICAL_OBSERVATION_SOURCE_CHANGED")
    async with asyncio.timeout(195):
        await check_current_configuration(root, case)
    return index


def verify_nvda_result(case: dict[str, Any], outputs: list[dict[str, Any]]) -> None:
    index = target_index(case)
    if len(outputs) != 1:
        raise ValueError("HISTORICAL_CASE_READBACK_INVALID")
    output = outputs[0]
    intent = ResearchIntent.model_validate(output["intent"])
    member = case["members"][index]
    if (
        str(intent.instrument_id) != member["request"]["instrument_id"]
        or str(intent.data_snapshot_id) != member["request"]["data_snapshot_id"]
        or intent.decision_time.isoformat() != case["evidence_as_of"]
        or intent.display_symbol != "NVDA"
        or intent.decision_id != uuid5(NAMESPACE_URL, f"nexus-research:{output['output_id']}")
        or intent.evidence_refs != tuple(EvidenceRef.model_validate(v) for v in member["evidence"])
        or not staged_intent_semantics_valid(intent, BOUNDED_STAGE_BUDGET_CONFIG_VERSION)
        or intent.quality_status != "complete"
        or output["usage"]["provider_calls"] != 3
        or not 0 <= output["usage"]["input_tokens"] <= 96000
        or not 0 <= output["usage"]["output_tokens"] <= 8192
    ):
        raise ValueError("HISTORICAL_CASE_READBACK_INVALID")
