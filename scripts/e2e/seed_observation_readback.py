"""Offline synthetic v10/v11 history, only inside the guarded real-E2E database."""

import asyncio
import hashlib
import json
import os
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import asyncpg
from nexus.agents.committee import ROLE_ORDER, load_prompt_assets
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    AgentResearchOutputEvidenceRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from nexus.strategy.intents import ResearchIntent
from real_fixture_db_guard import ADVISORY_LOCK_KEY, asyncpg_dsn

NOW = datetime(2026, 8, 24, 12, tzinfo=UTC)
INSTRUMENT_ID = UUID("0198d500-1000-7000-8000-000000009001")
SNAPSHOT_ID = UUID("0198d500-1000-7000-8000-000000009002")
VERSIONS = (
    "codex-sdk-chatgpt-v10-flexible-stage-budget",
    "codex-sdk-chatgpt-v11-bounded-stage-budget",
)


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


async def seed(database_url):
    # The existing parent guard must own the lock. Standalone or unsafe execution fails closed.
    connection = await asyncpg.connect(asyncpg_dsn(database_url), timeout=10)
    try:
        if await connection.fetchval(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", ADVISORY_LOCK_KEY
        ):
            raise RuntimeError("offline observation seed requires active database guard")
    finally:
        await connection.close()
    factory = session_factory(EngineSettings(database_url=database_url))
    prompts = load_prompt_assets()
    async with factory() as session, session.begin():
        prompt_records = {
            role: AgentPromptVersionRecord(
                role=role,
                version=asset.version,
                content_hash=asset.content_hash,
                content=asset.content,
            )
            for role, asset in prompts.items()
        }
        evidence = ResearchEvidenceRef(
            source="sec",
            provider_object_id="synthetic-observation-filing",
            revision="v1",
            available_at=NOW - timedelta(days=1),
            location="memory://sha256/" + "e" * 64,
            content_hash="e" * 64,
            byte_length=1,
        )
        session.add_all(
            [
                Instrument(
                    id=INSTRUMENT_ID,
                    asset_type="equity",
                    exchange="NASDAQ",
                    currency="USD",
                    tick_size=Decimal("0.01"),
                    risk_bucket="equity",
                ),
                DataSnapshot(
                    id=SNAPSHOT_ID,
                    source="fixture",
                    provider_object_id="observation-e2e",
                    revision="v1",
                    observed_at=NOW,
                    available_at=NOW,
                    received_at=NOW,
                    schema_version="1",
                    payload={},
                ),
                evidence,
                *prompt_records.values(),
            ]
        )
        await session.flush()
        session.add(
            InstrumentAlias(
                instrument_id=INSTRUMENT_ID,
                symbol="OBS",
                alias_type="ticker",
                valid_from=date(2020, 1, 1),
                valid_to=None,
            )
        )
        ref = dict(
            source=evidence.source,
            object_id=evidence.provider_object_id,
            available_at=evidence.available_at.isoformat().replace("+00:00", "Z"),
            location=evidence.location,
            content_hash=evidence.content_hash,
        )
        manifest = [
            {
                **ref,
                "id": str(evidence.id),
                "revision": "v1",
                "provider_object_id": ref["object_id"],
            }
        ]
        del manifest[0]["object_id"]
        prompt_manifest = {
            role: dict(id=str(record.id), version=record.version, content_hash=record.content_hash)
            for role, record in prompt_records.items()
        }
        for number, version in enumerate(VERSIONS, 10):
            run_id = UUID(f"0198d500-1000-7000-8000-0000000090{number}")
            model = AgentModelConfigRecord(
                model_id="synthetic-observation-model",
                version=version,
                config_hash="2" * 64,
                max_input_tokens=1000,
                max_tool_calls=0,
                max_output_tokens=1000,
                input_usd_per_million=Decimal("0"),
                output_usd_per_million=Decimal("0"),
                pricing_available_at=NOW - timedelta(days=1),
                pricing_expires_at=NOW + timedelta(days=30),
            )
            session.add(model)
            await session.flush()
            claim = {"text": "合成财报只支持历史收入观察。", "evidence_refs": [ref]}
            doc = ResearchIntent.model_validate(
                {
                    "schema_version": "research_intent_v1",
                    "decision_id": str(run_id),
                    "instrument_id": str(INSTRUMENT_ID),
                    "display_symbol": "OBS",
                    "decision_time": NOW,
                    "data_snapshot_id": str(SNAPSHOT_ID),
                    "horizon": "medium_term",
                    "stance": "watch",
                    "exposure_action": "none",
                    "theses": [claim],
                    "counter_theses": [
                        {"text": "合成反方异议：缺少同口径盈利预测。", "evidence_refs": [ref]}
                    ],
                    "uncertainties": ["合成不确定性：盈利持续性尚未证实。"],
                    "invalidation_conditions": [],
                    "entry_conditions": [],
                    "exit_conditions": [],
                    "valid_until": NOW + timedelta(days=1),
                    "quality_status": "partial",
                    "evidence_audit": "partial",
                    "role_votes": [{"role": role, "vote": "abstain"} for role in ROLE_ORDER],
                    "disagreement": "合成分歧：历史收入不能替代盈利预测。",
                    "strategy_version": "synthetic-observation-v1",
                    "model_version": model.model_id,
                    "prompt_version": prompts["orchestrator"].version,
                    "data_version": "v1",
                    "evidence_refs": [ref],
                    "synthesis_assessment": {
                        "stance": "watch",
                        "claims": [claim],
                        "review_conclusion": "limited",
                        "uncertainties": ["合成不确定性：盈利持续性尚未证实。"],
                        "reviews": [
                            {
                                "claim_id": f"{role}:0",
                                "status": "unresolved",
                                "rationale": "原始缺口仍未解决。",
                                "evidence_refs": [ref],
                                "resolved": False,
                                "missing_evidence": ["合成缺口：同口径盈利预测缺失。"],
                                "prevented_conclusion": "合成受阻结论：无法判断是否增加敞口。",
                            }
                            for role in ("bear", "evidence_audit")
                        ],
                    },
                }
            ).model_dump(mode="json")
            run = AgentResearchRunRecord(
                id=run_id,
                instrument_id=INSTRUMENT_ID,
                data_snapshot_id=SNAPSHOT_ID,
                prompt_version_id=prompt_records["orchestrator"].id,
                model_config_id=model.id,
                decision_time=NOW,
                status="completed",
                actor_id="synthetic-observation-e2e",
                idempotency_key=f"observation-{number}",
                schema_hash="a" * 64,
                model_hash=model.config_hash,
                prompt_hash=digest(prompt_manifest),
                prompt_manifest=prompt_manifest,
                evidence_hash=digest(manifest),
                evidence_manifest=manifest,
                tool_hash="b" * 64,
                input_hash="c" * 64,
                reserved_cost_usd=Decimal("0"),
                settled_cost_usd=None,
                pricing_version=version,
            )
            session.add(run)
            await session.flush()
            content = dict(
                status="completed", role=None, payload={"quality_status": "partial"}, intent=doc
            )
            output = AgentResearchOutputRecord(
                run_id=run.id, sequence=1, **content, content_hash=digest(content)
            )
            session.add(output)
            await session.flush()
            session.add(
                AgentResearchOutputEvidenceRecord(
                    output_id=output.id,
                    evidence_ref_id=evidence.id,
                    source=evidence.source,
                    provider_object_id=evidence.provider_object_id,
                    revision=evidence.revision,
                    available_at=evidence.available_at,
                    location=evidence.location,
                    content_hash=evidence.content_hash,
                )
            )


if __name__ == "__main__":
    try:
        if (
            os.environ.get("NEXUS_ENV") != "test"
            or os.environ.get("AGENT_RUNTIME_MODE") != "fixture"
            or os.environ.get("NEXUS_PROVIDER_JOBS_ENABLED") != "false"
        ):
            raise RuntimeError("offline observation seed environment is invalid")
        asyncio.run(seed(os.environ.get("DATABASE_URL", "")))
    except Exception:
        raise SystemExit("offline observation seed failed") from None
