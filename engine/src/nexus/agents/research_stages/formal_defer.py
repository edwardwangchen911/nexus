"""Wait for a stopped original formal claim without granting another send right."""

from uuid import UUID

from sqlalchemy import func, select, text

from nexus.agents.budget import ResearchAccountingBusy
from nexus.jobs.repository import ClaimedJob
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings


class FormalGrantLockBusy(ResearchAccountingBusy):
    """The formal grant did not acquire its account or budget lock."""


class FormalStageGrantBusy(ResearchAccountingBusy):
    def __init__(self, *, scope_id: UUID, run_id: UUID, stage: int, claim: ClaimedJob) -> None:
        super().__init__()
        self.scope_id, self.run_id, self.stage, self.claim = scope_id, run_id, stage, claim


async def deferred_lease_remaining(settings: EngineSettings, notice: FormalStageGrantBusy) -> float:
    """Read the exact stopped claim. Only normal orphan recovery can settle it."""
    claim = notice.claim
    factory = session_factory(settings)
    try:
        async with factory.begin() as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            expiry = await session.scalar(
                text("""
                SELECT l.expires_at
                FROM research_stage_scope s JOIN research_stage_member m ON m.scope_id=s.id
                JOIN research_stage_route r
                  ON (r.scope_id,r.run_id,r.job_id)=(s.id,m.run_id,m.job_id)
                JOIN jobs j ON j.id=m.job_id JOIN job_leases l ON l.job_id=j.id
                JOIN job_attempts a ON a.job_id=j.id AND a.attempt_no=j.attempt_count
                WHERE s.id=:scope AND s.cycle_id=s.id AND s.stopped AND r.kind='formal'
                  AND r.platform_binding IS NOT NULL AND m.run_id=:run AND j.id=:job
                  AND j.payload->>'run_id'=CAST(:run AS text) AND j.status='running'
                  AND j.attempt_count=:attempt AND a.outcome='running' AND a.finished_at IS NULL
                  AND l.worker_id=:worker AND a.worker_id=l.worker_id
                  AND l.lease_token=:token AND a.lease_token=l.lease_token
                  AND l.fence_token=:fence AND a.fence_token=l.fence_token AND l.expires_at=:expiry
                  AND NOT EXISTS (SELECT 1 FROM research_stage_terminal t WHERE t.run_id=m.run_id)
                  AND (SELECT count(*) FROM research_stage_authorization z WHERE z.run_id=m.run_id)
                      = :prior_count
                  AND (SELECT count(*) FROM research_stage_authorization z
                       JOIN research_stage_observation o USING(run_id,stage)
                       JOIN research_stage_verification v USING(run_id,stage)
                       JOIN research_stage_cleanup c USING(run_id,stage)
                       WHERE z.run_id=m.run_id AND z.stage BETWEEN 1 AND :prior_count
                         AND z.job_id=j.id AND z.worker_id=l.worker_id
                         AND z.lease_token=l.lease_token
                         AND z.attempt_no=a.attempt_no AND z.fence_token=l.fence_token
                         AND o.eligible AND o.valid AND v.verified AND c.confirmed) = :prior_count
            """),
                dict(
                    scope=notice.scope_id,
                    run=notice.run_id,
                    job=claim.job_id,
                    attempt=claim.attempt_no,
                    worker=claim.worker_id,
                    token=claim.lease_token,
                    fence=claim.fence_token,
                    expiry=claim.expires_at,
                    prior_count=notice.stage - 1,
                ),
            )
            if notice.stage not in (2, 3) or expiry is None:
                raise ValueError("FORMAL_STAGE_DEFER_BINDING_INVALID")
            now = await session.scalar(select(func.clock_timestamp()))
            return max(0.0, (expiry - now).total_seconds())
    finally:
        await factory.kw["bind"].dispose()
