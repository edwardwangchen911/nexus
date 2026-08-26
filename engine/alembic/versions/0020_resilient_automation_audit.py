"""Add fenced jobs, recoverable outbox delivery, and a sealed audit chain.

Revision ID: 0020_resilient_automation_audit
Revises: 0019_paper_performance_root
Create Date: 2026-08-27
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from datetime import UTC, datetime

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0020_resilient_automation_audit"
down_revision: str | Sequence[str] | None = "0019_paper_performance_root"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

UTC_TIMESTAMP = sa.DateTime(timezone=True)
UUID = postgresql.UUID(as_uuid=True)
JSONB = postgresql.JSONB(astext_type=sa.Text())
ZERO_HASH = "0" * 64
_MAX_SAFE_INTEGER = 9_007_199_254_740_991


def _strict_json_value(value: object) -> object:
    if value is None or isinstance(value, str | bool):
        return value
    if isinstance(value, int):
        if not -_MAX_SAFE_INTEGER <= value <= _MAX_SAFE_INTEGER:
            raise ValueError("legacy audit integer exceeds the canonical safe range")
        return value
    if isinstance(value, float):
        raise TypeError("legacy audit payload contains a binary float")
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("legacy audit payload contains a non-string key")
        return {key: _strict_json_value(value[key]) for key in sorted(value)}
    if isinstance(value, tuple | list):
        return [_strict_json_value(item) for item in value]
    raise TypeError(f"legacy audit payload contains {type(value).__name__}")


def _canonical_json(value: object) -> str:
    return json.dumps(
        _strict_json_value(value),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _canonical_audit_event(row: sa.RowMapping, event_seq: int) -> str:
    occurred_at = row["created_at"]
    if not isinstance(occurred_at, datetime):
        raise TypeError("legacy audit created_at is invalid")
    return _canonical_json(
        {
            "event_seq": event_seq,
            "occurred_at": occurred_at.astimezone(UTC).strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ"
            ),
            "actor_id": row["actor_id"],
            "action": row["action"],
            "schema_version": row["schema_version"],
            "payload": row["payload"],
        }
    )


def _backfill_audit_chain() -> None:
    connection = op.get_bind()
    rows = connection.execute(
        sa.text(
            "SELECT id, actor_id, action, schema_version, payload, created_at "
            "FROM audit_events ORDER BY created_at, id"
        )
    ).mappings()
    previous_hash = ZERO_HASH
    for event_seq, row in enumerate(rows, start=1):
        canonical_event = _canonical_audit_event(row, event_seq)
        event_hash = hashlib.sha256(
            (previous_hash + canonical_event).encode("utf-8")
        ).hexdigest()
        connection.execute(
            sa.text(
                "UPDATE audit_events SET event_seq=:event_seq, occurred_at=created_at, "
                "previous_hash=:previous_hash, canonical_event=:canonical_event, "
                "event_hash=:event_hash, chain_version='audit_jcs_sha256_v1' "
                "WHERE id=:id"
            ),
            {
                "event_seq": event_seq,
                "previous_hash": previous_hash,
                "canonical_event": canonical_event,
                "event_hash": event_hash,
                "id": row["id"],
            },
        )
        previous_hash = event_hash


def _add_runtime_columns() -> None:
    op.alter_column(
        "jobs",
        "job_key",
        existing_type=sa.String(255),
        type_=sa.String(512),
        existing_nullable=False,
    )
    for column in (
        sa.Column("job_type", sa.String(50)),
        sa.Column("queue", sa.String(20)),
        sa.Column("scope_kind", sa.String(20)),
        sa.Column("scope_id", sa.String(100)),
        sa.Column("effective_at", UTC_TIMESTAMP),
        sa.Column("priority", sa.Integer()),
        sa.Column("retry_policy", sa.String(20)),
        sa.Column("max_attempts", sa.Integer(), server_default="3"),
        sa.Column("attempt_count", sa.Integer(), server_default="0"),
        sa.Column("manual_retry_count", sa.Integer(), server_default="0"),
        sa.Column("available_at", UTC_TIMESTAMP),
        sa.Column("started_at", UTC_TIMESTAMP),
        sa.Column("finished_at", UTC_TIMESTAMP),
        sa.Column("last_error_code", sa.String(100)),
        sa.Column("result_code", sa.String(100)),
        sa.Column("version", sa.Integer(), server_default="1"),
    ):
        op.add_column("jobs", column)

    for column in (
        sa.Column("lease_token", UUID),
        sa.Column("fence_token", sa.BigInteger()),
        sa.Column("acquired_at", UTC_TIMESTAMP),
        sa.Column("heartbeat_at", UTC_TIMESTAMP),
    ):
        op.add_column("job_leases", column)

    for column in (
        sa.Column("status", sa.String(20), server_default="queued"),
        sa.Column("attempt_count", sa.Integer(), server_default="0"),
        sa.Column("max_attempts", sa.Integer(), server_default="3"),
        sa.Column("available_at", UTC_TIMESTAMP, server_default=sa.func.now()),
        sa.Column("last_error_code", sa.String(100)),
        sa.Column("dead_lettered_at", UTC_TIMESTAMP),
    ):
        op.add_column("outbox_events", column)

    for column in (
        sa.Column("event_seq", sa.BigInteger()),
        sa.Column("occurred_at", UTC_TIMESTAMP),
        sa.Column("previous_hash", sa.String(64)),
        sa.Column("canonical_event", sa.Text()),
        sa.Column("event_hash", sa.String(64)),
        sa.Column("chain_version", sa.String(32)),
    ):
        op.add_column("audit_events", column)


def _create_runtime_tables() -> None:
    op.create_table(
        "job_attempts",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("job_id", UUID, nullable=False),
        sa.Column("attempt_no", sa.Integer(), nullable=False),
        sa.Column("lease_token", UUID, nullable=False),
        sa.Column("fence_token", sa.BigInteger(), nullable=False),
        sa.Column("worker_id", sa.String(255), nullable=False),
        sa.Column("started_at", UTC_TIMESTAMP, nullable=False),
        sa.Column("finished_at", UTC_TIMESTAMP),
        sa.Column("outcome", sa.String(20), nullable=False),
        sa.Column("error_code", sa.String(100)),
        sa.Column(
            "created_at", UTC_TIMESTAMP, nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(
            ["job_id"], ["jobs.id"], name="fk_job_attempts_job"
        ),
        sa.UniqueConstraint("job_id", "attempt_no", name="uq_job_attempts_job_no"),
        sa.UniqueConstraint("lease_token", name="uq_job_attempts_lease_token"),
        sa.CheckConstraint("attempt_no >= 1", name="attempt_no_positive"),
        sa.CheckConstraint("fence_token >= 1", name="fence_positive"),
        sa.CheckConstraint(
            "outcome IN ('running','succeeded','skipped','failed','lost')",
            name="outcome_allowed",
        ),
        sa.CheckConstraint(
            "(outcome = 'running' AND finished_at IS NULL AND error_code IS NULL) OR "
            "(outcome IN ('succeeded','skipped') AND finished_at IS NOT NULL "
            "AND error_code IS NULL) OR "
            "(outcome IN ('failed','lost') AND finished_at IS NOT NULL "
            "AND error_code IS NOT NULL)",
            name="terminal_shape",
        ),
    )
    op.create_table(
        "job_dependencies",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("job_id", UUID, nullable=False),
        sa.Column("depends_on_job_id", UUID, nullable=False),
        sa.Column("required_outcome", sa.String(30), nullable=False),
        sa.Column(
            "created_at", UTC_TIMESTAMP, nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(
            ["job_id"], ["jobs.id"], name="fk_job_dependencies_job"
        ),
        sa.ForeignKeyConstraint(
            ["depends_on_job_id"],
            ["jobs.id"],
            name="fk_job_dependencies_parent",
        ),
        sa.UniqueConstraint(
            "job_id", "depends_on_job_id", name="uq_job_dependencies_edge"
        ),
        sa.CheckConstraint("job_id <> depends_on_job_id", name="not_self"),
        sa.CheckConstraint(
            "required_outcome IN ('succeeded','succeeded_or_skipped')",
            name="required_outcome_allowed",
        ),
    )
    op.create_table(
        "worker_heartbeats",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("queue", sa.String(20), nullable=False),
        sa.Column("worker_id", sa.String(255), nullable=False),
        sa.Column("process_version", sa.String(64), nullable=False),
        sa.Column("migration_head", sa.String(64), nullable=False),
        sa.Column("started_at", UTC_TIMESTAMP, nullable=False),
        sa.Column("seen_at", UTC_TIMESTAMP, nullable=False),
        sa.Column(
            "created_at", UTC_TIMESTAMP, nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint(
            "queue", "worker_id", name="uq_worker_heartbeats_queue_worker"
        ),
        sa.CheckConstraint(
            "queue IN ('general','broker','strategy_rnd')", name="queue_allowed"
        ),
        sa.CheckConstraint("seen_at >= started_at", name="time_order"),
    )
    op.create_table(
        "outbox_delivery_attempts",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("outbox_event_id", UUID, nullable=False),
        sa.Column("attempt_no", sa.Integer(), nullable=False),
        sa.Column("worker_id", sa.String(255), nullable=False),
        sa.Column("lease_token", UUID, nullable=False),
        sa.Column("started_at", UTC_TIMESTAMP, nullable=False),
        sa.Column("finished_at", UTC_TIMESTAMP),
        sa.Column("outcome", sa.String(20), nullable=False),
        sa.Column("error_code", sa.String(100)),
        sa.Column(
            "created_at", UTC_TIMESTAMP, nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(
            ["outbox_event_id"],
            ["outbox_events.id"],
            name="fk_outbox_delivery_attempts_event",
        ),
        sa.UniqueConstraint(
            "outbox_event_id",
            "attempt_no",
            name="uq_outbox_delivery_attempts_event_no",
        ),
        sa.UniqueConstraint(
            "lease_token", name="uq_outbox_delivery_attempts_lease_token"
        ),
        sa.CheckConstraint("attempt_no >= 1", name="attempt_no_positive"),
        sa.CheckConstraint(
            "outcome IN ('delivering','published','failed','lost')",
            name="outcome_allowed",
        ),
        sa.CheckConstraint(
            "(outcome = 'delivering' AND finished_at IS NULL AND error_code IS NULL) OR "
            "(outcome = 'published' AND finished_at IS NOT NULL AND error_code IS NULL) OR "
            "(outcome IN ('failed','lost') AND finished_at IS NOT NULL "
            "AND error_code IS NOT NULL)",
            name="terminal_shape",
        ),
    )
    op.create_table(
        "audit_chain_manifests",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("session", sa.Date(), nullable=False),
        sa.Column("first_event_seq", sa.BigInteger(), nullable=False),
        sa.Column("last_event_seq", sa.BigInteger(), nullable=False),
        sa.Column("head_hash", sa.String(64), nullable=False),
        sa.Column("object_uri", sa.Text(), nullable=False),
        sa.Column("object_sha256", sa.String(64), nullable=False),
        sa.Column("byte_size", sa.BigInteger(), nullable=False),
        sa.Column(
            "created_at", UTC_TIMESTAMP, nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("session", name="uq_audit_chain_manifests_session"),
        sa.UniqueConstraint(
            "head_hash", name="uq_audit_chain_manifests_head_hash"
        ),
        sa.CheckConstraint(
            "first_event_seq >= 1 AND last_event_seq >= first_event_seq",
            name="seq_bounds",
        ),
        sa.CheckConstraint(
            "head_hash ~ '^[0-9a-f]{64}$' AND "
            "object_sha256 ~ '^[0-9a-f]{64}$'",
            name="hashes_canonical",
        ),
        sa.CheckConstraint("byte_size > 0", name="byte_size_positive"),
    )


def _backfill_runtime() -> None:
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM jobs
            WHERE schema_version <> 'agent_research_job_v1'
              AND job_key NOT LIKE 'optimization-proposal:%'
          ) THEN
            RAISE EXCEPTION 'legacy job type cannot be mapped' USING ERRCODE = '23514';
          END IF;
          IF EXISTS (
            SELECT 1 FROM jobs
            WHERE status NOT IN
              ('QUEUED','RUNNING','COMPLETED','ABSTAINED','FAILED')
          ) THEN
            RAISE EXCEPTION 'legacy job status cannot be mapped' USING ERRCODE = '23514';
          END IF;
        END;
        $$
        """
    )
    op.execute(
        """
        UPDATE jobs SET
          job_type = CASE
            WHEN schema_version = 'agent_research_job_v1' THEN 'agent.research'
            ELSE 'optimization.propose' END,
          queue = 'general',
          scope_kind = 'global',
          scope_id = 'global',
          effective_at = created_at,
          priority = 180,
          retry_policy = CASE
            WHEN schema_version = 'agent_research_job_v1' THEN 'automatic_safe'
            ELSE 'manual_safe' END,
          max_attempts = 3,
          attempt_count = CASE WHEN status = 'QUEUED' THEN 0 ELSE 1 END,
          manual_retry_count = 0,
          available_at = created_at,
          started_at = CASE WHEN status = 'QUEUED' THEN NULL ELSE created_at END,
          finished_at = CASE
            WHEN status IN ('COMPLETED','ABSTAINED','FAILED') THEN created_at
            ELSE NULL END,
          last_error_code = CASE WHEN status = 'FAILED' THEN 'legacy_failed' ELSE NULL END,
          result_code = CASE
            WHEN status = 'COMPLETED' THEN 'legacy_completed'
            WHEN status = 'ABSTAINED' THEN 'legacy_abstained'
            ELSE NULL END,
          version = 1,
          status = CASE status
            WHEN 'QUEUED' THEN 'queued'
            WHEN 'RUNNING' THEN 'running'
            WHEN 'COMPLETED' THEN 'succeeded'
            WHEN 'ABSTAINED' THEN 'skipped'
            WHEN 'FAILED' THEN 'failed' END
        """
    )
    op.execute(
        """
        UPDATE jobs SET job_key =
          job_type || ':' || scope_kind || ':' || scope_id || ':' ||
          to_char(effective_at AT TIME ZONE 'UTC',
                  'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') || ':' || input_hash
        """
    )
    op.execute(
        """
        INSERT INTO job_attempts
          (id, job_id, attempt_no, lease_token, fence_token, worker_id,
           started_at, finished_at, outcome, error_code, created_at)
        SELECT
          id, id, 1, id, 1, 'legacy-migration', started_at, finished_at,
          CASE status
            WHEN 'running' THEN 'running'
            WHEN 'succeeded' THEN 'succeeded'
            WHEN 'skipped' THEN 'skipped'
            ELSE 'failed' END,
          CASE WHEN status = 'failed' THEN 'legacy_failed' ELSE NULL END,
          created_at
        FROM jobs WHERE status <> 'queued'
        """
    )
    op.execute(
        """
        DELETE FROM job_leases
        WHERE job_id IN (SELECT id FROM jobs WHERE status <> 'running')
        """
    )
    op.execute(
        """
        UPDATE job_leases lease SET
          lease_token = lease.id,
          fence_token = 1,
          acquired_at = job.created_at,
          heartbeat_at = job.created_at,
          expires_at = GREATEST(lease.expires_at, job.created_at + interval '30 seconds')
        FROM jobs job WHERE job.id = lease.job_id
        """
    )
    op.execute(
        """
        INSERT INTO job_leases
          (id, job_id, worker_id, expires_at, created_at,
           lease_token, fence_token, acquired_at, heartbeat_at)
        SELECT id, id, 'legacy-migration', created_at + interval '30 seconds',
               created_at, id, 1, created_at, created_at
        FROM jobs job
        WHERE status = 'running'
          AND NOT EXISTS (SELECT 1 FROM job_leases lease WHERE lease.job_id = job.id)
        """
    )
    op.execute(
        """
        UPDATE outbox_events SET
          status = CASE WHEN published_at IS NULL THEN 'queued' ELSE 'published' END,
          attempt_count = CASE WHEN published_at IS NULL THEN 0 ELSE 1 END,
          max_attempts = 3,
          available_at = created_at,
          dead_lettered_at = NULL,
          last_error_code = NULL
        """
    )
    op.execute(
        """
        INSERT INTO outbox_delivery_attempts
          (id, outbox_event_id, attempt_no, worker_id, lease_token,
           started_at, finished_at, outcome, error_code, created_at)
        SELECT id, id, 1, 'legacy-migration', id, created_at, published_at,
               'published', NULL, created_at
        FROM outbox_events WHERE published_at IS NOT NULL
        """
    )
    _backfill_audit_chain()


def _seal_runtime_schema() -> None:
    for name in (
        "job_type",
        "queue",
        "scope_kind",
        "scope_id",
        "effective_at",
        "priority",
        "retry_policy",
        "max_attempts",
        "attempt_count",
        "manual_retry_count",
        "available_at",
        "version",
    ):
        op.alter_column("jobs", name, nullable=False)
    for name in ("lease_token", "fence_token", "acquired_at", "heartbeat_at"):
        op.alter_column("job_leases", name, nullable=False)
    for name in ("status", "attempt_count", "max_attempts", "available_at"):
        op.alter_column("outbox_events", name, nullable=False)
    for name in (
        "event_seq",
        "occurred_at",
        "previous_hash",
        "canonical_event",
        "event_hash",
        "chain_version",
    ):
        op.alter_column("audit_events", name, nullable=False)

    op.create_check_constraint(
        "jobs_status_allowed",
        "jobs",
        "status IN ('queued','running','succeeded','failed','skipped','dead_letter')",
    )
    op.create_check_constraint(
        "jobs_queue_allowed",
        "jobs",
        "queue IN ('general','broker','strategy_rnd')",
    )
    op.create_check_constraint(
        "jobs_scope_allowed",
        "jobs",
        "scope_kind IN ('global','portfolio','instrument','provider') "
        "AND scope_id ~ '^[A-Za-z0-9._-]{1,100}$'",
    )
    op.create_check_constraint(
        "jobs_retry_allowed",
        "jobs",
        "retry_policy IN ('automatic_safe','manual_safe','never') "
        "AND max_attempts BETWEEN 1 AND 3 "
        "AND attempt_count BETWEEN 0 AND max_attempts "
        "AND manual_retry_count BETWEEN 0 AND 2",
    )
    op.create_check_constraint(
        "jobs_hash_payload",
        "jobs",
        "input_hash ~ '^[0-9a-f]{64}$' AND jsonb_typeof(payload) = 'object'",
    )
    op.create_check_constraint(
        "jobs_time_shape",
        "jobs",
        "(status = 'queued' AND started_at IS NULL AND finished_at IS NULL) OR "
        "(status = 'running' AND started_at IS NOT NULL AND finished_at IS NULL) OR "
        "(status IN ('succeeded','failed','skipped','dead_letter') "
        "AND started_at IS NOT NULL AND finished_at IS NOT NULL "
        "AND finished_at >= started_at)",
    )
    op.create_check_constraint(
        "job_leases_values",
        "job_leases",
        "fence_token >= 1 AND expires_at > heartbeat_at "
        "AND heartbeat_at >= acquired_at",
    )
    op.create_check_constraint(
        "outbox_status_allowed",
        "outbox_events",
        "status IN ('queued','delivering','published','dead_letter')",
    )
    op.create_check_constraint(
        "outbox_attempts_bounded",
        "outbox_events",
        "max_attempts BETWEEN 1 AND 10 AND attempt_count BETWEEN 0 AND max_attempts",
    )
    op.create_check_constraint(
        "outbox_terminal_shape",
        "outbox_events",
        "(status = 'published' AND published_at IS NOT NULL "
        "AND dead_lettered_at IS NULL) OR "
        "(status = 'dead_letter' AND published_at IS NULL "
        "AND dead_lettered_at IS NOT NULL) OR "
        "(status IN ('queued','delivering') AND published_at IS NULL "
        "AND dead_lettered_at IS NULL)",
    )
    op.create_unique_constraint(
        "uq_job_leases_lease_token", "job_leases", ["lease_token"]
    )
    op.create_unique_constraint(
        "uq_audit_events_event_seq", "audit_events", ["event_seq"]
    )
    op.create_unique_constraint(
        "uq_audit_events_event_hash", "audit_events", ["event_hash"]
    )
    op.create_check_constraint(
        "audit_events_hashes",
        "audit_events",
        "previous_hash ~ '^[0-9a-f]{64}$' AND event_hash ~ '^[0-9a-f]{64}$' "
        "AND chain_version = 'audit_jcs_sha256_v1'",
    )
    op.create_index(
        "ix_jobs_claim",
        "jobs",
        ["queue", "status", "priority", "available_at", "created_at", "id"],
    )
    op.create_index(
        "ix_outbox_events_delivery",
        "outbox_events",
        ["status", "available_at", "created_at", "id"],
    )


def _create_runtime_guards() -> None:
    op.execute(
        """
        CREATE FUNCTION nexus_guard_job_state()
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
    op.execute(
        "CREATE TRIGGER trg_jobs_state BEFORE UPDATE OR DELETE ON jobs "
        "FOR EACH ROW EXECUTE FUNCTION nexus_guard_job_state()"
    )
    op.execute(
        """
        CREATE FUNCTION nexus_guard_job_attempt()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF TG_OP = 'DELETE' THEN
            RAISE EXCEPTION 'job attempt is not deletable' USING ERRCODE = '23514';
          END IF;
          IF OLD.outcome <> 'running'
             OR NEW.job_id IS DISTINCT FROM OLD.job_id
             OR NEW.attempt_no IS DISTINCT FROM OLD.attempt_no
             OR NEW.lease_token IS DISTINCT FROM OLD.lease_token
             OR NEW.fence_token IS DISTINCT FROM OLD.fence_token
             OR NEW.worker_id IS DISTINCT FROM OLD.worker_id
             OR NEW.started_at IS DISTINCT FROM OLD.started_at
             OR NEW.created_at IS DISTINCT FROM OLD.created_at
             OR NEW.outcome = 'running' THEN
            RAISE EXCEPTION 'job attempt authority is immutable'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_job_attempts_state BEFORE UPDATE OR DELETE ON job_attempts "
        "FOR EACH ROW EXECUTE FUNCTION nexus_guard_job_attempt()"
    )
    op.execute(
        """
        CREATE FUNCTION nexus_guard_job_lease()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF TG_OP = 'DELETE' THEN
            RAISE EXCEPTION 'job lease is not deletable' USING ERRCODE = '23514';
          END IF;
          IF NEW.job_id IS DISTINCT FROM OLD.job_id
             OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
            RAISE EXCEPTION 'job lease authority is immutable'
              USING ERRCODE = '23514';
          END IF;
          IF NEW.fence_token = OLD.fence_token THEN
            IF NEW.lease_token IS DISTINCT FROM OLD.lease_token
               OR NEW.worker_id IS DISTINCT FROM OLD.worker_id
               OR NEW.acquired_at IS DISTINCT FROM OLD.acquired_at
               OR NEW.heartbeat_at < OLD.heartbeat_at
               OR NEW.expires_at < OLD.expires_at THEN
              RAISE EXCEPTION 'job lease heartbeat is inconsistent'
                USING ERRCODE = '23514';
            END IF;
          ELSIF NEW.fence_token = OLD.fence_token + 1 THEN
            IF NEW.lease_token IS NOT DISTINCT FROM OLD.lease_token
               OR NEW.acquired_at < OLD.heartbeat_at
               OR NEW.heartbeat_at IS DISTINCT FROM NEW.acquired_at THEN
              RAISE EXCEPTION 'job lease reclaim is inconsistent'
                USING ERRCODE = '23514';
            END IF;
          ELSE
            RAISE EXCEPTION 'job lease fence is inconsistent'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_job_leases_state BEFORE UPDATE OR DELETE ON job_leases "
        "FOR EACH ROW EXECUTE FUNCTION nexus_guard_job_lease()"
    )
    op.execute(
        """
        CREATE FUNCTION nexus_guard_worker_heartbeat()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF TG_OP = 'DELETE'
             OR NEW.queue IS DISTINCT FROM OLD.queue
             OR NEW.worker_id IS DISTINCT FROM OLD.worker_id
             OR NEW.started_at IS DISTINCT FROM OLD.started_at
             OR NEW.created_at IS DISTINCT FROM OLD.created_at
             OR NEW.seen_at < OLD.seen_at THEN
            RAISE EXCEPTION 'worker heartbeat is inconsistent'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_worker_heartbeats_state BEFORE UPDATE OR DELETE "
        "ON worker_heartbeats FOR EACH ROW EXECUTE FUNCTION "
        "nexus_guard_worker_heartbeat()"
    )
    op.execute(
        """
        CREATE FUNCTION nexus_guard_outbox_event()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF TG_OP = 'DELETE' THEN
            RAISE EXCEPTION 'outbox event is not deletable' USING ERRCODE = '23514';
          END IF;
          IF OLD.status IN ('published','dead_letter') THEN
            RAISE EXCEPTION 'terminal outbox event is immutable'
              USING ERRCODE = '23514';
          END IF;
          IF NEW.event_type IS DISTINCT FROM OLD.event_type
             OR NEW.aggregate_id IS DISTINCT FROM OLD.aggregate_id
             OR NEW.schema_version IS DISTINCT FROM OLD.schema_version
             OR NEW.payload IS DISTINCT FROM OLD.payload
             OR NEW.max_attempts IS DISTINCT FROM OLD.max_attempts
             OR NEW.created_at IS DISTINCT FROM OLD.created_at
             OR NEW.attempt_count < OLD.attempt_count THEN
            RAISE EXCEPTION 'outbox authority is immutable'
              USING ERRCODE = '23514';
          END IF;
          IF NOT (
            (OLD.status = 'queued' AND NEW.status = 'delivering'
              AND NEW.attempt_count = OLD.attempt_count + 1)
            OR (OLD.status = 'queued' AND NEW.status = 'published'
              AND NEW.attempt_count = OLD.attempt_count + 1)
            OR (OLD.status = 'delivering' AND NEW.status IN
                ('queued','published','dead_letter')
              AND NEW.attempt_count = OLD.attempt_count)
          ) THEN
            RAISE EXCEPTION 'outbox transition is inconsistent'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_outbox_events_state BEFORE UPDATE OR DELETE "
        "ON outbox_events FOR EACH ROW EXECUTE FUNCTION nexus_guard_outbox_event()"
    )
    op.execute(
        """
        CREATE FUNCTION nexus_guard_outbox_attempt()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF TG_OP = 'DELETE'
             OR OLD.outcome <> 'delivering'
             OR NEW.outbox_event_id IS DISTINCT FROM OLD.outbox_event_id
             OR NEW.attempt_no IS DISTINCT FROM OLD.attempt_no
             OR NEW.worker_id IS DISTINCT FROM OLD.worker_id
             OR NEW.lease_token IS DISTINCT FROM OLD.lease_token
             OR NEW.started_at IS DISTINCT FROM OLD.started_at
             OR NEW.created_at IS DISTINCT FROM OLD.created_at
             OR NEW.outcome = 'delivering' THEN
            RAISE EXCEPTION 'outbox attempt authority is immutable'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_outbox_delivery_attempts_state BEFORE UPDATE OR DELETE "
        "ON outbox_delivery_attempts FOR EACH ROW EXECUTE FUNCTION "
        "nexus_guard_outbox_attempt()"
    )
    op.execute(
        """
        CREATE FUNCTION nexus_guard_job_dependency_cycle()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE cycle_found boolean;
        BEGIN
          WITH RECURSIVE ancestors(job_id) AS (
            SELECT dependency.depends_on_job_id
            FROM job_dependencies dependency
            WHERE dependency.job_id = NEW.depends_on_job_id
            UNION
            SELECT dependency.depends_on_job_id
            FROM job_dependencies dependency
            JOIN ancestors ON dependency.job_id = ancestors.job_id
          )
          SELECT EXISTS (
            SELECT 1 FROM ancestors WHERE job_id = NEW.job_id
          ) INTO cycle_found;
          IF cycle_found THEN
            RAISE EXCEPTION 'job dependency cycle is invalid'
              USING ERRCODE = '23514';
          END IF;
          RETURN NULL;
        END;
        $$
        """
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_job_dependencies_cycle "
        "AFTER INSERT ON job_dependencies DEFERRABLE INITIALLY DEFERRED "
        "FOR EACH ROW EXECUTE FUNCTION nexus_guard_job_dependency_cycle()"
    )
    op.execute(
        """
        CREATE FUNCTION nexus_prevent_runtime_fact_change()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          RAISE EXCEPTION 'runtime authority fact is immutable' USING ERRCODE = '23514';
        END;
        $$
        """
    )
    for table_name in ("job_dependencies", "audit_chain_manifests"):
        op.execute(
            f"CREATE TRIGGER trg_{table_name}_immutable BEFORE UPDATE OR DELETE "
            f"ON {table_name} FOR EACH ROW EXECUTE FUNCTION "
            "nexus_prevent_runtime_fact_change()"
        )
    op.execute(
        """
        CREATE FUNCTION nexus_guard_audit_chain()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE expected_seq bigint;
        DECLARE expected_previous text;
        DECLARE expected_event jsonb;
        DECLARE expected_hash text;
        BEGIN
          PERFORM pg_advisory_xact_lock(
            hashtextextended('nexus:audit-chain', 2020)
          );
          SELECT event_seq + 1, event_hash
            INTO expected_seq, expected_previous
          FROM audit_events ORDER BY event_seq DESC LIMIT 1;
          IF NOT FOUND THEN
            expected_seq := 1;
            expected_previous := repeat('0', 64);
          END IF;
          expected_event := jsonb_build_object(
            'action', NEW.action,
            'actor_id', NEW.actor_id,
            'event_seq', NEW.event_seq,
            'occurred_at', to_char(
              NEW.occurred_at AT TIME ZONE 'UTC',
              'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
            ),
            'payload', NEW.payload,
            'schema_version', NEW.schema_version
          );
          expected_hash := encode(
            sha256(convert_to(NEW.previous_hash || NEW.canonical_event, 'UTF8')),
            'hex'
          );
          IF NEW.event_seq <> expected_seq
             OR NEW.previous_hash <> expected_previous
             OR NEW.canonical_event::jsonb IS DISTINCT FROM expected_event
             OR NEW.event_hash <> expected_hash
             OR NEW.chain_version <> 'audit_jcs_sha256_v1' THEN
            RAISE EXCEPTION 'audit chain is inconsistent' USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_audit_events_chain BEFORE INSERT ON audit_events "
        "FOR EACH ROW EXECUTE FUNCTION nexus_guard_audit_chain()"
    )
    op.execute(
        """
        CREATE FUNCTION nexus_prevent_audit_change()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          RAISE EXCEPTION 'audit event is immutable' USING ERRCODE = '23514';
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_audit_events_immutable BEFORE UPDATE OR DELETE "
        "ON audit_events FOR EACH ROW EXECUTE FUNCTION nexus_prevent_audit_change()"
    )
    for table_name in (
        "job_attempts",
        "job_dependencies",
        "worker_heartbeats",
        "outbox_delivery_attempts",
        "audit_chain_manifests",
    ):
        op.execute(
            f"CREATE TRIGGER trg_{table_name}_created_at_immutable "
            f"BEFORE UPDATE ON {table_name} FOR EACH ROW "
            "EXECUTE FUNCTION nexus_prevent_created_at_change()"
        )


def upgrade() -> None:
    """Install Task 20 coordination and audit authorities."""
    op.execute("DROP TRIGGER trg_agent_jobs_change ON jobs")
    op.execute("DROP FUNCTION nexus_validate_agent_job_change()")
    _add_runtime_columns()
    _create_runtime_tables()
    _backfill_runtime()
    _seal_runtime_schema()
    _create_runtime_guards()


def downgrade() -> None:
    """Remove Task 20 authorities for a fact-free local rollback only."""
    for table_name in (
        "job_attempts",
        "job_dependencies",
        "worker_heartbeats",
        "outbox_delivery_attempts",
        "audit_chain_manifests",
    ):
        op.execute(f"DROP TRIGGER trg_{table_name}_created_at_immutable ON {table_name}")
    op.execute("DROP TRIGGER trg_audit_events_immutable ON audit_events")
    op.execute("DROP TRIGGER trg_audit_events_chain ON audit_events")
    op.execute("DROP TRIGGER trg_audit_chain_manifests_immutable ON audit_chain_manifests")
    op.execute("DROP TRIGGER trg_job_dependencies_immutable ON job_dependencies")
    op.execute("DROP TRIGGER trg_job_dependencies_cycle ON job_dependencies")
    op.execute(
        "DROP TRIGGER trg_outbox_delivery_attempts_state ON outbox_delivery_attempts"
    )
    op.execute("DROP TRIGGER trg_outbox_events_state ON outbox_events")
    op.execute("DROP TRIGGER trg_worker_heartbeats_state ON worker_heartbeats")
    op.execute("DROP TRIGGER trg_job_leases_state ON job_leases")
    op.execute("DROP TRIGGER trg_job_attempts_state ON job_attempts")
    op.execute("DROP TRIGGER trg_jobs_state ON jobs")
    op.execute("DROP FUNCTION nexus_prevent_audit_change()")
    op.execute("DROP FUNCTION nexus_guard_audit_chain()")
    op.execute("DROP FUNCTION nexus_prevent_runtime_fact_change()")
    op.execute("DROP FUNCTION nexus_guard_job_dependency_cycle()")
    op.execute("DROP FUNCTION nexus_guard_outbox_attempt()")
    op.execute("DROP FUNCTION nexus_guard_outbox_event()")
    op.execute("DROP FUNCTION nexus_guard_worker_heartbeat()")
    op.execute("DROP FUNCTION nexus_guard_job_lease()")
    op.execute("DROP FUNCTION nexus_guard_job_attempt()")
    op.execute("DROP FUNCTION nexus_guard_job_state()")

    op.drop_index("ix_outbox_events_delivery", table_name="outbox_events")
    op.drop_index("ix_jobs_claim", table_name="jobs")
    op.drop_constraint("audit_events_hashes", "audit_events", type_="check")
    op.drop_constraint("uq_audit_events_event_hash", "audit_events", type_="unique")
    op.drop_constraint("uq_audit_events_event_seq", "audit_events", type_="unique")
    op.drop_constraint("uq_job_leases_lease_token", "job_leases", type_="unique")
    op.drop_constraint("outbox_terminal_shape", "outbox_events", type_="check")
    op.drop_constraint("outbox_attempts_bounded", "outbox_events", type_="check")
    op.drop_constraint("outbox_status_allowed", "outbox_events", type_="check")
    op.drop_constraint("job_leases_values", "job_leases", type_="check")
    op.drop_constraint("jobs_time_shape", "jobs", type_="check")
    op.drop_constraint("jobs_hash_payload", "jobs", type_="check")
    op.drop_constraint("jobs_retry_allowed", "jobs", type_="check")
    op.drop_constraint("jobs_scope_allowed", "jobs", type_="check")
    op.drop_constraint("jobs_queue_allowed", "jobs", type_="check")
    op.drop_constraint("jobs_status_allowed", "jobs", type_="check")

    for table_name in (
        "audit_chain_manifests",
        "outbox_delivery_attempts",
        "worker_heartbeats",
        "job_dependencies",
        "job_attempts",
    ):
        op.drop_table(table_name)

    for name in reversed(
        (
            "event_seq",
            "occurred_at",
            "previous_hash",
            "canonical_event",
            "event_hash",
            "chain_version",
        )
    ):
        op.drop_column("audit_events", name)
    for name in reversed(
        (
            "status",
            "attempt_count",
            "max_attempts",
            "available_at",
            "last_error_code",
            "dead_lettered_at",
        )
    ):
        op.drop_column("outbox_events", name)
    for name in reversed(
        ("lease_token", "fence_token", "acquired_at", "heartbeat_at")
    ):
        op.drop_column("job_leases", name)

    op.execute(
        """
        UPDATE jobs SET
          job_key = CASE
            WHEN job_type = 'agent.research' THEN 'agent-research:' || id::text
            ELSE 'optimization-proposal:' || id::text END,
          schema_version = CASE
            WHEN job_type = 'agent.research' THEN 'agent_research_job_v1'
            ELSE schema_version END,
          status = CASE status
          WHEN 'queued' THEN 'QUEUED'
          WHEN 'running' THEN 'RUNNING'
          WHEN 'succeeded' THEN 'COMPLETED'
          WHEN 'skipped' THEN 'ABSTAINED'
          WHEN 'failed' THEN 'FAILED'
          WHEN 'dead_letter' THEN 'FAILED' END
        """
    )
    for name in reversed(
        (
            "job_type",
            "queue",
            "scope_kind",
            "scope_id",
            "effective_at",
            "priority",
            "retry_policy",
            "max_attempts",
            "attempt_count",
            "manual_retry_count",
            "available_at",
            "started_at",
            "finished_at",
            "last_error_code",
            "result_code",
            "version",
        )
    ):
        op.drop_column("jobs", name)
    op.alter_column(
        "jobs",
        "job_key",
        existing_type=sa.String(512),
        type_=sa.String(255),
        existing_nullable=False,
    )
    op.execute(
        "CREATE FUNCTION nexus_validate_agent_job_change() RETURNS trigger LANGUAGE plpgsql "
        "AS $$ BEGIN "
        "IF OLD.schema_version <> 'agent_research_job_v1' THEN "
        "IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF; END IF; "
        "IF TG_OP = 'DELETE' THEN RAISE EXCEPTION 'agent job is durable' "
        "USING ERRCODE = '23514'; END IF; "
        "IF (to_jsonb(NEW) - 'status') IS DISTINCT FROM (to_jsonb(OLD) - 'status') THEN "
        "RAISE EXCEPTION 'agent job authority is immutable' USING ERRCODE = '23514'; END IF; "
        "IF (OLD.status = 'QUEUED' AND NEW.status = 'RUNNING') OR "
        "(OLD.status = 'RUNNING' AND NEW.status IN ('COMPLETED','FAILED','ABSTAINED')) "
        "THEN RETURN NEW; END IF; "
        "RAISE EXCEPTION 'invalid agent job transition' USING ERRCODE = '23514'; END; $$"
    )
    op.execute(
        "CREATE TRIGGER trg_agent_jobs_change BEFORE UPDATE OR DELETE ON jobs "
        "FOR EACH ROW EXECUTE FUNCTION nexus_validate_agent_job_change()"
    )
