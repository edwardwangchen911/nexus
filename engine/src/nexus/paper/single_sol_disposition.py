"""Prove the scoped v4 two-stage failure without releasing funds or dispatching."""

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.platform_profile import sol_business_model_matches
from nexus.agents.platform_stage_accounting import read_stage_accounting_model
from nexus.agents.research_stages.protocol import stage_evidence, usage_known
from nexus.paper.independent_unknown_proof import BLOCKED
from nexus.paper.manual_request import ResearchManifestView
from nexus.paper.single_sol_owner import binding_for
from nexus.persistence.models.manual_paper import ManualPaperRequest

SCOPE_PIN = Path(".secrets/nexus-agent-paper/single-sol-disposition.sha256")


def require_designated_proof(root: Path, digest: str) -> None:
    # Deployment-local designation is separate from the user's explicit acceptance.
    # The CLI cannot create, replace or refresh its own scope.
    from nexus.ops.platform_credentials import PlatformCredentialError, _private_bytes

    try:
        raw = _private_bytes(root / SCOPE_PIN, 65)
    except PlatformCredentialError:
        raise ValueError(BLOCKED) from None
    if raw not in (digest.encode("ascii"), digest.encode("ascii") + b"\n"):
        raise ValueError(BLOCKED)


async def single_sol_history(
    session: AsyncSession, row: ManualPaperRequest, view: ResearchManifestView
) -> dict[str, object]:
    binding = binding_for(row)
    if len(view.members) != 1:
        raise ValueError(BLOCKED)
    member = view.members[0]
    model = await read_stage_accounting_model(session, row.model_config_id)
    if (
        model is None
        or not sol_business_model_matches(model)
        or model.max_tool_calls != 0
        or member.instrument_id != binding.instrument_id
        or member.status != "failed"
        or member.usage_known
    ):
        raise ValueError(BLOCKED)
    # read_manual_request already verifies the request, batch, manifest, native
    # owner and snapshot. The common terminal reader checks the attempt and fence.
    guard = await session.scalar(
        text("""
        SELECT to_jsonb(g)::text FROM single_sol_budget_scopes g
        JOIN agent_daily_budget_reservations b ON b.run_id=g.source_run_id
        JOIN research_stage_route r ON r.run_id=g.source_run_id
        WHERE g.source_run_id=:run AND g.budget_day=:day AND g.account_id=:account
        AND g.model_id=:model AND g.calendar_id=:calendar AND g.selected_id=:instrument
        AND g.batch_id=:batch AND g.maximum=:maximum AND r.job_id=:job AND r.scope_id=:request
        AND r.kind='independent' AND r.platform_binding IS NOT NULL
        AND g.budget_day=b.business_date AND b.state='reserved' AND b.settled_usd IS NULL
        AND g.maximum=b.reserved_usd AND b.pricing_version=:pricing
        AND NOT EXISTS (SELECT FROM single_sol_budget_credits WHERE run_id=g.source_run_id)
        AND NOT EXISTS (SELECT FROM research_stage_authorization a WHERE a.run_id=g.source_run_id
                        AND public.nexus_sol_native_authorization_valid(a) IS NOT TRUE)
        """),
        dict(
            run=member.run_id,
            day=binding.budget_day,
            account=row.account_id,
            model=model.id,
            calendar=binding.calendar_id,
            instrument=member.instrument_id,
            batch=row.candidate_batch_id,
            maximum=model.worst_case_cost_usd,
            job=member.job_id,
            request=row.id,
            pricing=model.version,
        ),
    )
    stages = await stage_evidence(session, member.run_id)
    if guard is None or [s["stage"] for s in stages] != [1, 2]:
        raise ValueError(BLOCKED)
    first, second = stages
    if (
        first["confirmed"] is not True
        or first["valid"] is not True
        or first["eligible"] is not True
        or first["verified"] is not True
        or not usage_known([first], model=model)
        or second["confirmed"] is not True
        or second["verified"] is not None
        or second["response_hash"] is not None
        or second["platform_response"] is not None
    ):
        raise ValueError(BLOCKED)
    from nexus.jobs.models import job_input_hash

    return {
        "intent_hash": binding.intent_hash,
        "budget_scope_hash": job_input_hash({"scope": guard}),
    }


async def require_current_capacity(
    session: AsyncSession, row: ManualPaperRequest, now: datetime
) -> None:
    from nexus.agents.persistence import _shared_daily_budget_used, try_lock_budget_day
    from nexus.agents.research_usage import require_research_accounting_clear
    from nexus.paper.economic_budget_hold import held_pair_budget
    from nexus.paper.formal_budget_protection import latest_binding
    from nexus.paper.research_request import database_now
    from nexus.paper.single_sol_budget import require_single_sol_capacity

    day = binding_for(row).budget_day
    current_day = now.astimezone(UTC).date()
    if day > current_day or (await database_now(session)).date() != current_day:
        raise ValueError(BLOCKED)
    for protected in sorted({day, current_day}):
        if not await try_lock_budget_day(session, protected.isoformat()):
            raise ValueError(BLOCKED)
    guard = (
        await session.execute(
            text("""
        SELECT g.source_run_id,g.daily_limit FROM single_sol_budget_scopes g
        JOIN research_request_members m ON m.run_id=g.source_run_id
        JOIN research_request_manifests f ON f.id=m.manifest_id
        WHERE f.request_id=:request AND g.budget_day=:day"""),
            {"request": row.id, "day": day},
        )
    ).one_or_none()
    if (
        guard is None
        or await latest_binding(session, business_date=day) is not None
        or await held_pair_budget(session, business_date=day)
    ):
        raise ValueError(BLOCKED)
    await require_research_accounting_clear(session)
    if current_day != day:
        if (
            await _shared_daily_budget_used(session, business_date=day) > guard.daily_limit
            or (await database_now(session)).date() != current_day
        ):
            raise ValueError(BLOCKED)
        return
    # Zero additional spend. The native allocator checks the DB day and all
    # still-unfunded obligations without converting any credit for this reader.
    if not await require_single_sol_capacity(
        session,
        kind="dispatch",
        run_id=guard.source_run_id,
        now=now,
        daily_limit_usd=guard.daily_limit,
        amount=Decimal(0),
    ):
        raise ValueError(BLOCKED)
