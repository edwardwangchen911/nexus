"""Authoritative complete-cycle binding for explicit isolated or formal v9 Workers."""

from collections.abc import Sequence

from pydantic import TypeAdapter
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_staged import StructuredStagedCodexCommittee
from nexus.paper.agent_cycle import _events, _fact_uuids
from nexus.paper.agent_cycle_models import (
    CandidateManifestEntry,
    CycleStatus,
    candidate_manifest_hash,
    canonical_cycle_event_hash,
    parse_candidate_manifest,
)
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.paper import AgentPaperCycleEventRecord, AgentPaperCycleRecord
from nexus.settings import EngineSettings


async def capacity_output_mode_qualified(
    session: AsyncSession, run: AgentResearchRunRecord, *, formal: bool
) -> bool:
    """Prove v9's original whole-cycle mode; version alone cannot choose a consumer."""
    from nexus.agents.codex_versions import (
        BOUNDED_STAGE_BUDGET_CONFIG_VERSION,
        FLEXIBLE_STAGE_BUDGET_CONFIG_VERSION,
        FULL_INPUT_CAPACITY_CONFIG_VERSION,
    )
    from nexus.agents.paper_context import read_agent_paper_research_context

    try:
        context = await read_agent_paper_research_context(
            session,
            snapshot_id=run.data_snapshot_id,
            instrument_id=run.instrument_id,
            as_of=run.decision_time,
        )
        if context is None:
            return False
        cycle = await session.get(AgentPaperCycleRecord, context[0].cycle_id)
        if cycle is None or cycle.candidate_batch_id is None:
            return False
        events = await _events(session, cycle.id)
        ids = _fact_uuids(events, "research_run_ids")
        if run.id not in ids or not 1 <= len(ids) <= 5:
            return False
        runs = []
        for rid in ids:
            member = await session.get(AgentResearchRunRecord, rid, populate_existing=True)
            if member is None:
                return False
            runs.append(member)
        paired = derive_cycle_members(cycle, events, runs)
        mode = "formal" if formal else "candidate"
        other = "candidate" if formal else "formal"
        facts = events[0].fact_ids
        return (
            events[0].result_code == "CYCLE_PREPARED"
            and facts.get(f"{mode}_stage_capacity") is True
            and facts.get(f"{mode}_model_config_id") == str(run.model_config_id)
            and facts.get(f"{mode}_model_hash") == run.model_hash
            and not any(key.startswith(f"{other}_") for key in facts)
            and all(
                member.model_config_id == run.model_config_id
                and member.model_hash == run.model_hash
                and member.pricing_version == run.pricing_version
                and run.pricing_version
                in {
                    FULL_INPUT_CAPACITY_CONFIG_VERSION,
                    FLEXIBLE_STAGE_BUDGET_CONFIG_VERSION,
                    BOUNDED_STAGE_BUDGET_CONFIG_VERSION,
                }
                for _, member in paired
            )
        )
    except (ValueError, TypeError, KeyError):
        return False


def derive_cycle_members(
    cycle: AgentPaperCycleRecord,
    events: tuple[AgentPaperCycleEventRecord, ...],
    runs: Sequence[AgentResearchRunRecord],
) -> tuple[tuple[CandidateManifestEntry, AgentResearchRunRecord], ...]:
    """Validate the frozen whole pool; augmented snapshots are checked by their reader."""
    invalid = "WORKLOAD_RUN_AUTHORITY_INVALID"
    manifest = parse_candidate_manifest(cycle.candidate_manifest, allow_legacy=False)
    if (
        cycle.execution_locked is not True
        or not 1 <= len(manifest) <= 5
        or len({r.instrument_id for r in manifest}) != len(manifest)
        or candidate_manifest_hash(manifest) != cycle.candidate_hash
        or not events
    ):
        raise ValueError(invalid)
    previous = None
    status_adapter: TypeAdapter[CycleStatus] = TypeAdapter(CycleStatus)
    for index, event in enumerate(events, 1):
        try:
            to_status = status_adapter.validate_python(event.to_status)
            from_status = (
                None
                if event.from_status is None
                else status_adapter.validate_python(event.from_status)
            )
        except ValueError:
            raise ValueError(invalid) from None
        if (
            event.cycle_id != cycle.id
            or event.sequence != index
            or event.from_status != previous
            or event.event_hash
            != canonical_cycle_event_hash(
                cycle_id=event.cycle_id,
                sequence=event.sequence,
                from_status=from_status,
                to_status=to_status,
                result_code=event.result_code,
                fact_ids=event.fact_ids,
                occurred_at=event.occurred_at,
            )
        ):
            raise ValueError(invalid)
        previous = event.to_status
    ids = _fact_uuids(events, "research_run_ids")
    if (
        len(ids) != len(manifest)
        or len(set(ids)) != len(ids)
        or len(runs) != len(ids)
        or {r.id for r in runs} != set(ids)
        or len({r.instrument_id for r in runs}) != len(runs)
        or any(r.decision_time != cycle.decision_time for r in runs)
    ):
        raise ValueError(invalid)
    by_instrument = {r.instrument_id: r for r in runs}
    if set(by_instrument) != {r.instrument_id for r in manifest}:
        raise ValueError(invalid)
    return tuple((row, by_instrument[row.instrument_id]) for row in manifest)


async def prepare_cycle_capacity_pool(
    session: AsyncSession,
    settings: EngineSettings,
    run: AgentResearchRunRecord,
    *,
    deadline: float,
    formal_stage_capacity: bool = False,
) -> StructuredStagedCodexCommittee:
    """Derive every member from persisted authority, not from a caller's subset."""
    from nexus.agents.codex_candidate import require_candidate_database
    from nexus.agents.codex_capacity import CapacityPoolMember, preflight_capacity_pool
    from nexus.agents.codex_versions import (
        ISOLATED_CAPACITY_CONFIG_VERSIONS,
    )
    from nexus.agents.execution_authority import load_frozen_evidence
    from nexus.agents.models import ResearchRequest
    from nexus.agents.paper_context import (
        FrozenAgentPaperContextStatus,
        read_agent_paper_research_context,
        revalidate_frozen_agent_paper_context,
    )
    from nexus.agents.synthesis import domain_evidence
    from nexus.worker import _committee

    invalid = "CODEX_CAPACITY_POOL_BINDING_INVALID"
    if type(formal_stage_capacity) is not bool:
        raise ValueError(invalid)
    if run.pricing_version not in ISOLATED_CAPACITY_CONFIG_VERSIONS or (
        formal_stage_capacity and run.pricing_version not in ISOLATED_CAPACITY_CONFIG_VERSIONS
    ):
        raise ValueError(invalid)
    if formal_stage_capacity:
        from sqlalchemy import func, select
        from sqlalchemy.engine import make_url

        if settings.database_url is None:
            raise ValueError(invalid)
        url = make_url(settings.database_url)
        if (
            url.host not in {"127.0.0.1", "localhost", "::1"}
            or set(url.query) & {"host", "hostaddr", "port", "dsn", "database", "dbname"}
            or await session.scalar(select(func.current_database())) != url.database
        ):
            raise ValueError(invalid)
    else:
        await require_candidate_database(session)
    from nexus.agents.research_only import is_research_only
    from nexus.persistence.models.data import DataSnapshot

    snapshot = await session.get(DataSnapshot, run.data_snapshot_id)
    if snapshot is not None and snapshot.payload.get("purpose") in {
        "price_monitor_increment_v1",
        "monitor_increment_v2",
    }:
        if not formal_stage_capacity:
            raise ValueError(invalid)
        from nexus.monitoring.capacity import prepare_monitor_capacity_pool

        return await prepare_monitor_capacity_pool(session, settings, run, deadline=deadline)
    if is_research_only(snapshot):
        if not formal_stage_capacity:
            raise ValueError(invalid)
        from nexus.agents.research_only_capacity import prepare_research_only_capacity_pool

        return await prepare_research_only_capacity_pool(session, settings, run, deadline=deadline)
    context = await read_agent_paper_research_context(
        session,
        snapshot_id=run.data_snapshot_id,
        instrument_id=run.instrument_id,
        as_of=run.decision_time,
    )
    if context is None:
        raise ValueError(invalid)
    cycle = await session.get(AgentPaperCycleRecord, context[0].cycle_id)
    if cycle is None:
        raise ValueError(invalid)
    events = await _events(session, cycle.id)
    ids = _fact_uuids(events, "research_run_ids")
    if not 1 <= len(ids) <= 5 or run.id not in ids or len(set(ids)) != len(ids):
        raise ValueError(invalid)
    runs = []
    for rid in ids:
        member = await session.get(AgentResearchRunRecord, rid, populate_existing=True)
        if member is None:
            raise ValueError(invalid)
        runs.append(member)
    paired = derive_cycle_members(cycle, events, runs)
    facts = events[0].fact_ids
    expected_mode = (
        (
            facts.get("formal_stage_capacity") is True
            and facts.get("formal_model_config_id") == str(run.model_config_id)
            and facts.get("formal_model_hash") == run.model_hash
            and not any(key.startswith("candidate_") for key in facts)
        )
        if formal_stage_capacity
        else (
            facts.get("candidate_stage_capacity") is True
            and facts.get("candidate_model_config_id") == str(run.model_config_id)
            and facts.get("candidate_model_hash") == run.model_hash
            and not any(key.startswith("formal_") for key in facts)
        )
    )
    if (
        events[0].result_code != "CYCLE_PREPARED"
        or events[0].to_status != "prepared"
        or not expected_mode
    ):
        raise ValueError(invalid)
    members = []
    selected = None
    for row, member in paired:
        if (
            member.model_config_id != run.model_config_id
            or member.model_hash != run.model_hash
            or member.pricing_version != run.pricing_version
            or member.status not in {"prepared", "completed"}
        ):
            raise ValueError(invalid)
        bound = await read_agent_paper_research_context(
            session,
            snapshot_id=member.data_snapshot_id,
            instrument_id=member.instrument_id,
            as_of=member.decision_time,
        )
        if bound is None or bound[0].cycle_id != cycle.id:
            raise ValueError(invalid)
        evidence = await load_frozen_evidence(session, member, allow_fixture_evidence=False)
        if (
            await revalidate_frozen_agent_paper_context(
                session, run=member, evidence_records=evidence
            )
            is not FrozenAgentPaperContextStatus.VALID
        ):
            raise ValueError(invalid)
        committee = await _committee(
            session,
            settings,
            member,
            responses_client=None,
            tools=None,
            deadline=deadline,
            full_input_capacity=not formal_stage_capacity,
            formal_stage_capacity=formal_stage_capacity,
        )
        if not isinstance(committee, StructuredStagedCodexCommittee):
            raise ValueError(invalid)
        request = ResearchRequest(
            instrument_id=member.instrument_id,
            data_snapshot_id=member.data_snapshot_id,
            decision_time=member.decision_time,
        )
        members.append(
            CapacityPoolMember(committee, request, tuple(domain_evidence(e) for e in evidence))
        )
        if member.id == run.id:
            selected = committee
    await preflight_capacity_pool(
        members, expected_instrument_ids=tuple(row.instrument_id for row, _ in paired)
    )
    if selected is None:
        raise ValueError(invalid)
    return selected
