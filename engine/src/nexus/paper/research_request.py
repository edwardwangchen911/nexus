"""Versioned pure research ownership; never creates a cycle or execution authority."""

from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.committee import AgentRole
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import admit_research_run
from nexus.jobs.models import JobType, job_input_hash
from nexus.paper.candidate_batch import (
    load_agent_paper_candidate_batch,
    validate_agent_paper_candidate_batch,
)
from nexus.paper.manual_request import (
    ManualRequestView,
    ResearchManifestView,
    ResearchMemberView,
    read_manual_request,
    record_manual_request_event,
    require_manual_account_available,
)
from nexus.paper.target_service import lock_portfolio_account
from nexus.persistence.models.agents import (
    AgentPromptVersionRecord,
    AgentResearchEventRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.manual_paper import (
    ManualPaperRequest,
    ResearchRequestManifest,
    ResearchRequestMember,
)
from nexus.persistence.models.runtime import Job, JobAttempt, JobLease
from nexus.settings import EngineSettings


async def database_now(session: AsyncSession) -> datetime:
    value = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    return value.astimezone(UTC)


def member_hash(run: AgentResearchRunRecord, job: Job) -> str:
    return job_input_hash(
        {
            "run_id": str(run.id),
            "job_id": str(job.id),
            "instrument_id": str(run.instrument_id),
            "snapshot_id": str(run.data_snapshot_id),
            "research_as_of": run.decision_time.isoformat(),
            "model_id": str(run.model_config_id),
            "model_hash": run.model_hash,
            "prompt_hash": run.prompt_hash,
            "evidence_hash": run.evidence_hash,
            "input_hash": run.input_hash,
            "tool_hash": run.tool_hash,
            "schema_hash": run.schema_hash,
            "pricing_version": run.pricing_version,
            "reserved_cost_usd": format(run.reserved_cost_usd.normalize(), "f"),
            "job_input_hash": job.input_hash,
        }
    )


def manifest_hash(row: ManualPaperRequest, at: datetime, hashes: list[str]) -> str:
    return job_input_hash(
        {
            "contract": "independent_monitor_research_v1"
            if row.contract_version == 3
            else "research_only_v2",
            "request_id": str(row.id),
            "request_hash": row.request_hash,
            "batch_hash": row.batch_hash,
            "model_hash": row.model_hash,
            "research_as_of": at.isoformat(),
            "members": hashes,
        }
    )


async def read_research_manifest(
    session: AsyncSession, row: ManualPaperRequest
) -> ResearchManifestView | None:
    manifest = await session.scalar(
        select(ResearchRequestManifest).where(ResearchRequestManifest.request_id == row.id)
    )
    if manifest is None:
        return None
    members = list(
        await session.scalars(
            select(ResearchRequestMember)
            .where(ResearchRequestMember.manifest_id == manifest.id)
            .order_by(ResearchRequestMember.ordinal)
        )
    )
    batch = await load_agent_paper_candidate_batch(session, batch_id=row.candidate_batch_id)
    if (
        row.contract_version not in {2, 3}
        or row.mode != {2: "research_only", 3: "research_and_monitor"}.get(row.contract_version)
        or batch is None
        or batch.batch_hash != row.batch_hash
        or batch.account_id != row.account_id
        or len(members) != manifest.member_count
        or tuple(member.instrument_id for member in members) != batch.instrument_ids
        or manifest.research_as_of < row.requested_at
        or [m.ordinal for m in members] != list(range(len(members)))
        or manifest.manifest_hash
        != manifest_hash(row, manifest.research_as_of, [m.member_hash for m in members])
    ):
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    views = []
    for member in members:
        run = await session.get(AgentResearchRunRecord, member.run_id)
        job = await session.get(Job, member.job_id)
        snapshot = await session.get(DataSnapshot, member.data_snapshot_id)
        if (
            run is None
            or job is None
            or snapshot is None
            or run.instrument_id != member.instrument_id
            or run.data_snapshot_id != member.data_snapshot_id
            or run.decision_time != manifest.research_as_of
            or run.model_config_id != row.model_config_id
            or run.model_hash != row.model_hash
            or run.actor_id
            != (
                "local-independent-monitor-research-v1"
                if row.contract_version == 3
                else "local-research-only-v2"
            )
            or run.idempotency_key
            != (
                f"{'independent-monitor' if row.contract_version == 3 else 'research-only'}"
                f":{row.id}:{member.instrument_id}"
            )
            or snapshot.payload.get("purpose")
            != (
                "independent_monitor_research_v1"
                if row.contract_version == 3
                else "research_only_v2"
            )
            or job.payload != {"run_id": str(run.id)}
            or job.job_type != "agent.paper.research"
            or job.scope_id != str(run.instrument_id)
            or job.effective_at != run.decision_time
            or member.member_hash != member_hash(run, job)
        ):
            raise ValueError("MANUAL_RESULT_UNKNOWN")
        output = await session.scalar(
            select(AgentResearchOutputRecord)
            .where(AgentResearchOutputRecord.run_id == run.id)
            .order_by(AgentResearchOutputRecord.sequence.desc())
            .limit(1)
        )
        # Output lifecycle and content sufficiency are separate facts.
        quality: str | None = None
        if output is not None:
            raw_quality = output.payload.get("quality_status")
            if isinstance(raw_quality, str) and raw_quality in {"complete", "partial"}:
                quality = raw_quality
        views.append(
            ResearchMemberView(
                instrument_id=run.instrument_id,
                run_id=run.id,
                job_id=job.id,
                status=run.status,
                quality=quality,
                usage_known=run.settled_cost_usd is not None,
            )
        )
    return ResearchManifestView(
        manifest_id=manifest.id,
        research_as_of=manifest.research_as_of,
        manifest_hash=manifest.manifest_hash,
        members=views,
    )


async def prepare_research_request(
    session: AsyncSession, settings: EngineSettings, *, request_id: UUID
) -> ManualRequestView:
    """Caller owns one atomic preparation transaction. No provider or model calls."""
    from nexus.agents.snapshots import prepare_research_snapshot
    from nexus.research.observation_identity_storage import register_observation_identity_binding

    row = await session.get(ManualPaperRequest, request_id)
    if (
        row is None
        or row.account_id != settings.agent_paper_account_id
        or row.contract_version not in {2, 3}
    ):
        raise ValueError("MANUAL_REQUEST_INVALID")
    await lock_portfolio_account(session, row.account_id)
    if row.contract_version == 3:
        from nexus.paper.independent_unknown_acceptance import require_independent_execution_open

        await require_independent_execution_open(session, request_id=row.id)
    view = await read_manual_request(session, row)
    if view.status != "preparing":
        return view
    from nexus.agents.research_maintenance import require_research_open

    require_research_open()
    now = await database_now(session)
    await require_manual_account_available(
        session, account_id=row.account_id, now=now, excluding_request_id=row.id
    )
    batch, _ = await validate_agent_paper_candidate_batch(
        session, batch_id=row.candidate_batch_id, account_id=row.account_id, as_of=now
    )
    if (
        batch.batch_hash != row.batch_hash
        or settings.agent_paper_formal_model_config_id != row.model_config_id
    ):
        raise ValueError("MANUAL_CONFIGURATION_UNAVAILABLE")
    prompts = await require_research_profile(session, row=row, settings=settings)
    for instrument_id in batch.instrument_ids:
        await register_observation_identity_binding(session, instrument_id=instrument_id)
    await session.flush()
    at = await database_now(session)
    native = []
    for instrument_id in batch.instrument_ids:
        snapshot = await prepare_research_snapshot(
            session, instrument_id, as_of=at, research_only=True
        )
        if snapshot is None:
            raise ValueError("RESEARCH_SNAPSHOT_MISSING")
        if row.contract_version == 3:
            from nexus.monitoring.independent_storage import freeze_independent_snapshot

            snapshot = await freeze_independent_snapshot(session, request=row, base=snapshot)
        request = ResearchRequest(
            instrument_id=instrument_id, data_snapshot_id=snapshot.id, decision_time=at
        )
        admission = await admit_research_run(
            session,
            actor_id="local-independent-monitor-research-v1"
            if row.contract_version == 3
            else "local-research-only-v2",
            route="independent-monitor-research-v1"
            if row.contract_version == 3
            else "research-only-v2",
            idempotency_key=(
                f"{'independent-monitor' if row.contract_version == 3 else 'research-only'}"
                f":{row.id}:{instrument_id}"
            ),
            request_hash=job_input_hash(request.model_dump(mode="json")),
            request=request,
            prompt_version_id=prompts["orchestrator"].id,
            model_config_id=row.model_config_id,
            daily_limit_usd=settings.agent_daily_budget_usd,
            now=at,
            job_type=JobType.AGENT_PAPER_RESEARCH,
            formal_stage_capacity=True,
            research_only=row.contract_version == 2,
            independent_monitor=row.contract_version == 3,
        )
        run = await session.get(AgentResearchRunRecord, admission.run_id)
        job = await session.scalar(
            select(Job).where(
                Job.payload["run_id"].as_string() == str(admission.run_id),
                Job.job_type == "agent.paper.research",
            )
        )
        if run is None or job is None or run.model_hash != row.model_hash:
            raise ValueError("MANUAL_RESULT_UNKNOWN")
        native.append((run, job))
    hashes = [member_hash(run, job) for run, job in native]
    manifest = ResearchRequestManifest(
        request_id=row.id,
        research_as_of=at,
        member_count=len(native),
        manifest_hash=manifest_hash(row, at, hashes),
    )
    session.add(manifest)
    await session.flush()
    for index, (run, job) in enumerate(native):
        session.add(
            ResearchRequestMember(
                manifest_id=manifest.id,
                ordinal=index,
                instrument_id=run.instrument_id,
                data_snapshot_id=run.data_snapshot_id,
                run_id=run.id,
                job_id=job.id,
                member_hash=hashes[index],
            )
        )
    await session.flush()
    if row.contract_version == 3:
        from nexus.agents.research_stages.routes import register_new_prepared_runs

        await register_new_prepared_runs(session, tuple(run.id for run, _ in native))
    return await record_manual_request_event(
        session, row, status="active", code="MANUAL_RESEARCH_STARTED"
    )


async def finish_research_request(
    session: AsyncSession, *, row: ManualPaperRequest, cancel_unused: bool = False
) -> ManualRequestView:
    await lock_portfolio_account(session, row.account_id)
    view = await read_manual_request(session, row)
    if view.status in {"completed", "unavailable", "unknown"}:
        return view
    if view.status == "preparing" and view.research is None and cancel_unused:
        # Preparation commits all native runs and the complete manifest atomically.
        # The caller holds the host lock; a preparing receipt alone never grants replay.
        orphan = await session.scalar(
            select(AgentResearchRunRecord.id)
            .where(
                AgentResearchRunRecord.idempotency_key.startswith(
                    f"{'independent-monitor' if row.contract_version == 3 else 'research-only'}"
                    f":{row.id}:"
                )
            )
            .limit(1)
        )
        if orphan is not None:
            raise ValueError("MANUAL_RESULT_UNKNOWN")
        return await record_manual_request_event(
            session, row, status="unavailable", code="MANUAL_INPUTS_UNAVAILABLE"
        )
    if view.research is None or view.status != "active":
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    now = await database_now(session)
    from nexus.paper.research_orphan import recover_formal_research_orphans

    await recover_formal_research_orphans(
        session,
        cycle=None,
        run_ids=tuple(m.run_id for m in view.research.members),
        now=now,
        research_request=row,
    )
    if cancel_unused:
        from nexus.paper.manual_cancel import cancel_unused_manual_research

        await cancel_unused_manual_research(
            session, cycle=None, events=(), now=now, research_request=row
        )
    view = await read_manual_request(session, row)
    assert view.research is not None
    jobs = [await session.get(Job, member.job_id) for member in view.research.members]
    if any(
        job is None or job.status not in {"succeeded", "failed", "skipped", "dead_letter"}
        for job in jobs
    ):
        raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")
    await require_research_jobs_settled(
        session, row=row, view=view.research, now=now, allow_unknown_usage=True
    )
    if any(not member.usage_known for member in view.research.members):
        return await record_manual_request_event(
            session, row, status="unknown", code="MANUAL_RESULT_UNKNOWN"
        )
    complete = all(member.status == "completed" for member in view.research.members)
    return await record_manual_request_event(
        session,
        row,
        status="completed" if complete else "unavailable",
        code="MANUAL_RESEARCH_FINISHED" if complete else "MANUAL_INPUTS_UNAVAILABLE",
    )


async def _legacy_require_research_jobs_settled(
    session: AsyncSession,
    *,
    row: ManualPaperRequest,
    view: ResearchManifestView,
    now: datetime,
    allow_unknown_usage: bool = False,
) -> None:
    """Native terminal facts plus exact consumed cancellation fences; no absent-cycle shortcut."""
    if not allow_unknown_usage and any(not member.usage_known for member in view.members):
        raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")
    for member in view.members:
        job = await session.get(Job, member.job_id)
        run = await session.get(AgentResearchRunRecord, member.run_id)
        if (
            job is None
            or run is None
            or job.status not in {"succeeded", "failed", "skipped", "dead_letter"}
            or run.status not in {"completed", "failed", "abstained"}
        ):
            raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")
        if (
            job.status == "skipped"
            and job.result_code == "AGENT_ABSTAINED"
            and await session.scalar(
                select(AgentResearchEventRecord.id).where(
                    AgentResearchEventRecord.run_id == run.id,
                    AgentResearchEventRecord.sequence == 2,
                    AgentResearchEventRecord.state == "abstained",
                    AgentResearchEventRecord.details == {"reason": "model_research_stopped"},
                )
            )
        ):
            from nexus.paper.research_failure import require_claimed_uncalled_research_closed

            await require_claimed_uncalled_research_closed(
                session, run_id=member.run_id, job_id=member.job_id, now=now
            )
            continue
        leases = list(
            await session.scalars(
                select(JobLease).where(JobLease.job_id == job.id, JobLease.expires_at > now)
            )
        )
        for lease in leases:
            # A committed native terminal consumes its fence even while the retained
            # lease audit has a future expiry. Running/uncertain attempts never qualify.
            consumed = await session.scalar(
                select(JobAttempt.id)
                .where(
                    JobAttempt.job_id == job.id,
                    JobAttempt.lease_token == lease.lease_token,
                    JobAttempt.fence_token == lease.fence_token,
                    JobAttempt.attempt_no == job.attempt_count,
                    JobAttempt.outcome == job.status,
                    JobAttempt.finished_at == job.finished_at,
                    JobAttempt.finished_at <= now,
                )
                .limit(1)
            )
            if (
                consumed is not None
                and job.attempt_count == 1
                and job.manual_retry_count == 0
                and job.status in {"succeeded", "failed"}
                and run.settled_cost_usd is not None
                and run.input_tokens is not None
                and run.output_tokens is not None
            ):
                continue
            if (
                job.status != "skipped"
                or job.result_code != "MANUAL_UNUSED_RESEARCH_CANCELLED"
                or job.attempt_count != 1
                or job.manual_retry_count != 0
                or lease.worker_id != f"manual-cancel:{row.id}"
                or run.status != "abstained"
                or run.settled_cost_usd != 0
                or run.input_tokens != 0
                or run.output_tokens != 0
                or not await session.scalar(
                    select(JobAttempt.id)
                    .where(
                        JobAttempt.job_id == job.id,
                        JobAttempt.lease_token == lease.lease_token,
                        JobAttempt.fence_token == lease.fence_token,
                        JobAttempt.attempt_no == 1,
                        JobAttempt.outcome == "skipped",
                        JobAttempt.finished_at <= now,
                    )
                    .limit(1)
                )
            ):
                raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")


async def require_research_profile(
    session: AsyncSession, *, row: ManualPaperRequest, settings: EngineSettings
) -> dict[AgentRole, AgentPromptVersionRecord]:
    from nexus.agents.persistence import load_complete_prompt_authority
    from nexus.agents.research_only import RESEARCH_ONLY_INSTRUCTIONS, research_only_prompts_valid

    profile = row.research_profile
    from nexus.agents.independent_monitor import INSTRUCTIONS, prompts_valid
    from nexus.agents.research_dispatch import uses_two_flight

    parallel = uses_two_flight(profile)
    if (
        not isinstance(profile, dict)
        or set(profile)
        != {"runtime_mode", "daily_limit_usd", "orchestrator_id", "prompt_hashes"}
        | ({"execution_policy"} if parallel else set())
        or settings.agent_runtime_mode is None
        or profile["runtime_mode"] != settings.agent_runtime_mode.value
        or profile["runtime_mode"] != "codex_sdk"
        or profile["daily_limit_usd"] != format(settings.agent_daily_budget_usd.normalize(), "f")
    ):
        raise ValueError("MANUAL_CONFIGURATION_UNAVAILABLE")
    try:
        identifier = UUID(str(profile["orchestrator_id"]))
    except ValueError:
        raise ValueError("MANUAL_CONFIGURATION_UNAVAILABLE") from None
    prompts = await load_complete_prompt_authority(session, orchestrator_id=identifier)
    base = await load_complete_prompt_authority(session)
    if (
        prompts is None
        or base is None
        or not (
            prompts_valid(prompts)
            if row.contract_version == 3
            else research_only_prompts_valid(prompts)
        )
        or {role: p.content_hash for role, p in prompts.items()} != profile["prompt_hashes"]
        or any(
            prompts[role].content
            != base[role].content
            + "\n\n"
            + (INSTRUCTIONS if row.contract_version == 3 else RESEARCH_ONLY_INSTRUCTIONS)
            for role in prompts
        )
    ):
        raise ValueError("MANUAL_CONFIGURATION_UNAVAILABLE")
    return prompts


async def require_research_jobs_settled(
    session: AsyncSession,
    *,
    row: ManualPaperRequest,
    view: ResearchManifestView,
    now: datetime,
    allow_unknown_usage: bool = False,
) -> None:
    from nexus.agents.research_stages.public import _research_settled

    # A historical admission exception validates the complete original manifest.
    # It never changes financial facts or authorizes unrelated/substituted members.
    # The proof builder uses allow_unknown_usage=True to validate native closure
    # without recursively consulting the historical disposition.
    if (
        not allow_unknown_usage
        and row.contract_version == 3
        and any(not member.usage_known for member in view.members)
    ):
        from nexus.paper.independent_unknown_acceptance import independent_unknown_accepted

        if await independent_unknown_accepted(session, row, now=now):
            current = await read_manual_request(session, row)
            if current.research != view:
                raise ValueError("INDEPENDENT_UNKNOWN_ACCEPTANCE_BLOCKED")
            return
    await _research_settled(
        session, row=row, view=view, now=now, allow_unknown_usage=allow_unknown_usage
    )
