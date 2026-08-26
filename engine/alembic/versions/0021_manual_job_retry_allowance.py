"""Allow two explicit manual Job retries without weakening automatic limits.

Revision ID: 0021_manual_job_retry_allowance
Revises: 0020_resilient_automation_audit
Create Date: 2026-08-27
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0021_manual_job_retry_allowance"
down_revision: str | Sequence[str] | None = "0020_resilient_automation_audit"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Grant one claim allowance per audited manual retry transition."""
    op.drop_constraint("jobs_retry_allowed", "jobs", type_="check")
    op.create_check_constraint(
        "jobs_retry_allowed",
        "jobs",
        "retry_policy IN ('automatic_safe','manual_safe','never') "
        "AND max_attempts BETWEEN 1 AND 3 "
        "AND attempt_count BETWEEN 0 AND max_attempts + manual_retry_count "
        "AND manual_retry_count BETWEEN 0 AND 2",
    )
    _replace_job_guard(manual_allowance=True)


def downgrade() -> None:
    """Restore the prior bound only when no extra manual attempt must be retained."""
    op.drop_constraint("jobs_retry_allowed", "jobs", type_="check")
    op.create_check_constraint(
        "jobs_retry_allowed",
        "jobs",
        "retry_policy IN ('automatic_safe','manual_safe','never') "
        "AND max_attempts BETWEEN 1 AND 3 "
        "AND attempt_count BETWEEN 0 AND max_attempts "
        "AND manual_retry_count BETWEEN 0 AND 2",
    )
    _replace_job_guard(manual_allowance=False)


def _replace_job_guard(*, manual_allowance: bool) -> None:
    extra_guard = (
        """
          IF NEW.manual_retry_count > OLD.manual_retry_count AND NOT (
            NEW.manual_retry_count = OLD.manual_retry_count + 1
            AND OLD.retry_policy = 'manual_safe'
            AND OLD.status IN ('failed','dead_letter')
            AND NEW.status = 'queued'
          ) THEN
            RAISE EXCEPTION 'manual retry transition is inconsistent'
              USING ERRCODE = '23514';
          END IF;
          IF OLD.status IN ('failed','dead_letter') AND NEW.status = 'queued' AND NOT (
            OLD.retry_policy = 'manual_safe'
            AND NEW.manual_retry_count = OLD.manual_retry_count + 1
            AND NEW.attempt_count = OLD.attempt_count
          ) THEN
            RAISE EXCEPTION 'manual retry authority is required'
              USING ERRCODE = '23514';
          END IF;
    """
        if manual_allowance
        else ""
    )
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION nexus_guard_job_state()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF TG_OP = 'DELETE' THEN
            RAISE EXCEPTION 'job state is not deletable' USING ERRCODE = '23514';
          END IF;
          IF NEW.job_key IS DISTINCT FROM OLD.job_key
             OR NEW.input_hash IS DISTINCT FROM OLD.input_hash
             OR NEW.schema_version IS DISTINCT FROM OLD.schema_version
             OR NEW.payload IS DISTINCT FROM OLD.payload
             OR NEW.job_type IS DISTINCT FROM OLD.job_type
             OR NEW.queue IS DISTINCT FROM OLD.queue
             OR NEW.scope_kind IS DISTINCT FROM OLD.scope_kind
             OR NEW.scope_id IS DISTINCT FROM OLD.scope_id
             OR NEW.effective_at IS DISTINCT FROM OLD.effective_at
             OR NEW.priority IS DISTINCT FROM OLD.priority
             OR NEW.retry_policy IS DISTINCT FROM OLD.retry_policy
             OR NEW.max_attempts IS DISTINCT FROM OLD.max_attempts THEN
            RAISE EXCEPTION 'job authority is immutable' USING ERRCODE = '23514';
          END IF;
          IF NEW.version <> OLD.version + 1
             OR NEW.attempt_count < OLD.attempt_count
             OR NEW.manual_retry_count < OLD.manual_retry_count THEN
            RAISE EXCEPTION 'job version is inconsistent' USING ERRCODE = '23514';
          END IF;
          IF OLD.status IN ('succeeded','skipped') THEN
            RAISE EXCEPTION 'terminal job is immutable' USING ERRCODE = '23514';
          END IF;
          {extra_guard}
          IF NOT (
            (OLD.status = 'queued' AND NEW.status = 'running')
            OR (OLD.status = 'running' AND NEW.status IN
                ('queued','succeeded','failed','skipped','dead_letter'))
            OR (OLD.status = 'failed' AND NEW.status IN ('queued','dead_letter'))
            OR (OLD.status = 'dead_letter' AND NEW.status = 'queued')
          ) THEN
            RAISE EXCEPTION 'job transition is inconsistent' USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
