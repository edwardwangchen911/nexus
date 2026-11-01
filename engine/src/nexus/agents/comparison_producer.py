"""Atomic native preparation for one immutable comparison position, without dispatch."""

import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid5

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.committee import ROLE_ORDER
from nexus.agents.comparison_inputs import _hash, _manifest
from nexus.agents.comparison_manifest import ComparisonManifest, parse_comparison_manifest
from nexus.agents.comparison_registration import register_comparison, reserve_comparison_chain
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import _schema_hash, _tool_hash
from nexus.agents.platform_funding import (
    PlatformFunding,
    require_funding_binding,
    require_platform_funding,
)
from nexus.agents.platform_manifest import PlatformManifest
from nexus.agents.platform_profile import platform_model_matches
from nexus.agents.research_maintenance import require_research_open
from nexus.agents.research_only import require_research_only_snapshot
from nexus.jobs.models import job_input_hash
from nexus.monitoring.independent_context import (
    SOURCE as CONTEXT_SOURCE,
)
from nexus.monitoring.independent_context import (
    IndependentMonitorContext,
    content,
    require_independent_snapshot,
)
from nexus.persistence.models.agents import (
    AgentDataSnapshotEvidenceRecord,
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.persistence.models.runtime import Job

SOURCE = "nexus_research_comparison"
SCHEMA = "comparison-input-v1"


@dataclass(frozen=True)
class ComparisonAdmission:
    run_id: UUID
    job_id: UUID
    reservation_id: UUID


async def _original_authority(
    session: AsyncSession, member: dict[str, Any], sdk_hash: str
) -> dict[str, AgentPromptVersionRecord]:
    request = ResearchRequest.model_validate(member["request"])
    original = await session.get(AgentResearchRunRecord, UUID(member["run_id"]))
    evidence = _manifest(member)
    if (
        original is None
        or original.instrument_id != request.instrument_id
        or original.data_snapshot_id != request.data_snapshot_id
        or original.decision_time != request.decision_time
        or original.model_hash != sdk_hash
        or original.evidence_manifest != evidence
        or original.evidence_hash != _hash(evidence)
        or original.prompt_hash != _hash(original.prompt_manifest)
    ):
        raise ValueError("COMPARISON_ORIGINAL_AUTHORITY_INVALID")
    snapshot = await session.get(DataSnapshot, request.data_snapshot_id)
    context = require_independent_snapshot(snapshot)
    if (
        context.model_dump(mode="json")
        != member["inputs"]["security_snapshot"]["independent_monitor_context"]
    ):
        raise ValueError("COMPARISON_ORIGINAL_AUTHORITY_INVALID")
    base = await session.get(DataSnapshot, context.source_snapshot_id)
    require_research_only_snapshot(base)
    evidence_ids = None if base is None else base.payload.get("evidence_ids")
    if (
        base is None
        or base.revision != context.source_snapshot_hash
        or base.provider_object_id != str(request.instrument_id)
        or base.observed_at != request.decision_time
        or not isinstance(evidence_ids, list)
        or not set(evidence_ids).issubset(
            {r["id"] for r in evidence if r["source"] != CONTEXT_SOURCE}
        )
    ):
        raise ValueError("COMPARISON_ORIGINAL_AUTHORITY_INVALID")
    for row in evidence:
        record = await session.get(ResearchEvidenceRef, UUID(row["id"]))
        if (
            record is None
            or any(
                getattr(record, key) != row[key]
                for key in ("source", "provider_object_id", "revision", "location", "content_hash")
            )
            or record.available_at != datetime.fromisoformat(row["available_at"])
        ):
            raise ValueError("COMPARISON_ORIGINAL_AUTHORITY_INVALID")
    prompts: dict[str, AgentPromptVersionRecord] = {}
    for role in ROLE_ORDER:
        bound = original.prompt_manifest[role]
        if not isinstance(bound, dict):
            raise ValueError("COMPARISON_PROMPT_INVALID")
        prompt = await session.get(AgentPromptVersionRecord, UUID(str(bound["id"])))
        expected = member["prompts"][role]
        if (
            prompt is None
            or prompt.role != role
            or prompt.version != expected["version"]
            or prompt.content != expected["content"]
            or prompt.content_hash != expected["content_hash"]
            or bound
            != {
                "id": str(prompt.id),
                "version": prompt.version,
                "content_hash": prompt.content_hash,
            }
        ):
            raise ValueError("COMPARISON_PROMPT_INVALID")
        prompts[role] = prompt
    return prompts


async def admit_comparison_position(
    session: AsyncSession,
    *,
    source: bytes,
    prepared: dict[str, Any],
    manifest: ComparisonManifest,
    ordinal: int,
    model_config_id: UUID,
    daily_limit_usd: Decimal,
    platform_funding: PlatformFunding | None = None,
) -> ComparisonAdmission:
    """Caller commits; a caught refusal rolls back every new native identity.

    Re-entry rejects even after a completed admission. Recovery must read the
    existing registry rather than create another Run or release its budget.
    """
    from nexus.agents.comparison_preparation import prepare_archive

    require_research_open()
    manifest = parse_comparison_manifest(manifest.model_dump())
    if isinstance(manifest, PlatformManifest):
        require_funding_binding(manifest, platform_funding)
    expected = prepare_archive(
        source,
        expected_source_hash=manifest.source_manifest_hash,
        experiment_id=manifest.experiment_id,
        purpose=manifest.purpose,
        **(
            {
                "platform_price_version": manifest.price_version,
                "platform_units_version": manifest.allocations[0].units_version,
            }
            if isinstance(manifest, PlatformManifest)
            else {}
        ),
    )
    if (
        json.dumps({k: v for k, v in prepared.items() if k != "rebuilt_at"}, sort_keys=True)
        != json.dumps(expected, sort_keys=True)
        or tuple(expected["input_hashes"]) != manifest.input_hashes
        or tuple(UUID(x) for x in expected["instrument_ids"]) != manifest.instrument_ids
        or expected["identity_equivalence_hash"] != manifest.identity_equivalence_hash
        or expected["outbound_safety_hash"] != manifest.outbound_safety_hash
    ):
        raise ValueError("COMPARISON_PREPARATION_CHANGED")
    if (
        isinstance(manifest, PlatformManifest)
        and json.loads(manifest.model_dump_json())["allocations"] != expected["allocations"]
    ):
        raise ValueError("PLATFORM_ALLOCATION_CHANGED")
    input_hash = manifest.input_hash_for(ordinal)
    symbol = manifest.positions[ordinal - 1][1]
    member = next(row["payload"] for row in expected["members"] if row["symbol"] == symbol)
    request = ResearchRequest.model_validate(member["request"])
    original_member = next(
        row
        for row in json.loads(source)["members"]
        if row["request"]["instrument_id"] == str(request.instrument_id)
    )
    async with session.begin_nested():
        await register_comparison(session, manifest)
        if await session.scalar(
            text(
                "SELECT EXISTS(SELECT 1 FROM research_comparison_chain "
                "WHERE experiment_id=:id AND ordinal=:ordinal)"
            ),
            {"id": manifest.experiment_id, "ordinal": ordinal},
        ):
            raise ValueError("COMPARISON_POSITION_CONSUMED")
        now = await session.scalar(text("SELECT clock_timestamp()"))
        if (
            now is None
            or not request.decision_time <= datetime.fromisoformat(prepared["rebuilt_at"]) <= now
        ):
            raise ValueError("COMPARISON_PREPARATION_CLOCK_INVALID")
        if isinstance(manifest, PlatformManifest):
            await require_platform_funding(session, manifest, platform_funding)
        prompts = await _original_authority(session, original_member, manifest.sdk_config_hash)
        model = await session.get(AgentModelConfigRecord, model_config_id)
        expected_model = (
            manifest.sdk_config_hash
            if manifest.positions[ordinal - 1][2] == "sdk"
            else manifest.api_config_hash
        )
        if (
            model is None
            or model.config_hash != expected_model
            or model.model_id != "gpt-5.6-terra"
            or (model.max_input_tokens, model.max_output_tokens, model.max_tool_calls)
            != (816000 if isinstance(manifest, PlatformManifest) else 96000, 8192, 0)
            or isinstance(manifest, PlatformManifest)
            and not platform_model_matches(model)
            or model.created_at > now
            or not model.pricing_available_at <= now < model.pricing_expires_at
        ):
            raise ValueError("COMPARISON_CONFIG_INVALID")
        frozen = {
            "purpose": manifest.purpose,
            "experiment_id": str(manifest.experiment_id),
            "input_hash": input_hash,
            "source_manifest_hash": manifest.source_manifest_hash,
            "frozen": member,
            "execution_locked": True,
            "formal_consumption_allowed": False,
        }
        snapshot = await session.get(DataSnapshot, request.data_snapshot_id)
        if snapshot is not None:
            if (
                snapshot.source != SOURCE
                or snapshot.schema_version != SCHEMA
                or snapshot.payload != frozen
                or snapshot.revision != _hash(frozen)
                or snapshot.observed_at != request.decision_time
                or snapshot.provider_object_id
                != f"{manifest.experiment_id}:{request.instrument_id}"
                or not snapshot.observed_at
                <= snapshot.created_at
                == snapshot.received_at
                == snapshot.available_at
                <= now
            ):
                raise ValueError("COMPARISON_SNAPSHOT_CONFLICT")
        else:
            snapshot = DataSnapshot(
                id=request.data_snapshot_id,
                source=SOURCE,
                provider_object_id=f"{manifest.experiment_id}:{request.instrument_id}",
                revision=_hash(frozen),
                observed_at=request.decision_time,
                available_at=now,
                received_at=now,
                created_at=now,
                schema_version=SCHEMA,
                payload=frozen,
            )
            session.add(snapshot)
            context = IndependentMonitorContext.model_validate_json(
                json.dumps(member["inputs"]["security_snapshot"]["independent_monitor_context"])
            )
            evidence = _manifest(member)
            own = next(row for row in evidence if row["source"] == CONTEXT_SOURCE)
            session.add(
                ResearchEvidenceRef(
                    id=UUID(own["id"]),
                    source=own["source"],
                    provider_object_id=own["provider_object_id"],
                    revision=own["revision"],
                    available_at=request.decision_time,
                    location=own["location"],
                    content_hash=own["content_hash"],
                    byte_length=len(content(context)),
                    created_at=now,
                )
            )
            await session.flush()
            for row in evidence:
                session.add(
                    AgentDataSnapshotEvidenceRecord(
                        data_snapshot_id=snapshot.id,
                        instrument_id=request.instrument_id,
                        evidence_ref_id=UUID(row["id"]),
                        authority_kind="research",
                        selected_at=None,
                        **{
                            k: row[k]
                            for k in (
                                "source",
                                "provider_object_id",
                                "revision",
                                "location",
                                "content_hash",
                            )
                        },
                        available_at=datetime.fromisoformat(row["available_at"]),
                        created_at=now,
                    )
                )
        await session.flush()
        links = list(
            await session.scalars(
                select(AgentDataSnapshotEvidenceRecord).where(
                    AgentDataSnapshotEvidenceRecord.data_snapshot_id == snapshot.id,
                    AgentDataSnapshotEvidenceRecord.instrument_id == request.instrument_id,
                )
            )
        )
        by_id = {str(row.evidence_ref_id): row for row in links}
        evidence = _manifest(member)
        if set(by_id) != {row["id"] for row in evidence}:
            raise ValueError("COMPARISON_SNAPSHOT_BINDING_INVALID")
        for row in evidence:
            link = by_id[row["id"]]
            if (
                link.authority_kind != "research"
                or link.selected_at is not None
                or any(
                    getattr(link, k) != row[k]
                    for k in (
                        "source",
                        "provider_object_id",
                        "revision",
                        "location",
                        "content_hash",
                    )
                )
                or link.available_at != datetime.fromisoformat(row["available_at"])
            ):
                raise ValueError("COMPARISON_SNAPSHOT_BINDING_INVALID")
        prompt_manifest = {
            role: {"id": str(row.id), "version": row.version, "content_hash": row.content_hash}
            for role, row in prompts.items()
        }
        run_id = uuid5(manifest.experiment_id, f"run:{ordinal}")
        job_id = uuid5(manifest.experiment_id, f"job:{ordinal}")
        key = f"comparison:{manifest.experiment_id}:{ordinal}"
        run = AgentResearchRunRecord(
            id=run_id,
            instrument_id=request.instrument_id,
            data_snapshot_id=request.data_snapshot_id,
            prompt_version_id=prompts["orchestrator"].id,
            model_config_id=model.id,
            decision_time=request.decision_time,
            actor_id=manifest.actor,
            idempotency_key=key,
            status="prepared",
            retry_count=0,
            schema_hash=_schema_hash(portfolio_prompt_version=prompts["portfolio"].version),
            model_hash=model.config_hash,
            prompt_hash=_hash(prompt_manifest),
            prompt_manifest=prompt_manifest,
            evidence_hash=_hash(_manifest(member)),
            evidence_manifest=_manifest(member),
            tool_hash=_tool_hash(),
            input_hash=input_hash,
            reserved_cost_usd=(
                manifest.reservation_for(ordinal)
                if isinstance(manifest, PlatformManifest)
                else model.worst_case_cost_usd
            ),
            settled_cost_usd=None,
            pricing_version=model.version,
            input_tokens=None,
            output_tokens=None,
            created_at=now,
        )
        session.add(run)
        payload = {"run_id": str(run_id)}
        session.add(
            Job(
                id=job_id,
                job_key=key,
                input_hash=job_input_hash(payload),
                status="queued",
                schema_version="comparison-v1",
                payload=payload,
                job_type="agent.research.comparison",
                queue="strategy_rnd",
                scope_kind="instrument",
                scope_id=str(request.instrument_id),
                effective_at=request.decision_time,
                priority=0,
                retry_policy="never",
                max_attempts=1,
                attempt_count=0,
                manual_retry_count=0,
                available_at=now,
            )
        )
        await session.flush()
        reservation = await reserve_comparison_chain(
            session,
            experiment_id=manifest.experiment_id,
            ordinal=ordinal,
            run_id=run_id,
            job_id=job_id,
            daily_limit_usd=daily_limit_usd,
        )
        from nexus.agents.research_stages.routes import register_prepared_runs

        await register_prepared_runs(session, (run_id,))
        return ComparisonAdmission(run_id, job_id, reservation)
