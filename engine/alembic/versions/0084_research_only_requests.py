"""Immutable pure-research manifests; old manual requests remain version one."""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0084_research_only_requests"
down_revision = "0083_observation_identity"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "manual_paper_requests", sa.Column("research_profile", postgresql.JSONB(), nullable=True)
    )
    op.add_column(
        "manual_paper_requests",
        sa.Column("contract_version", sa.Integer(), nullable=False, server_default="1"),
    )
    op.create_check_constraint(
        "contract_version_valid",
        "manual_paper_requests",
        "contract_version=1 OR (contract_version=2 AND mode='research_only')",
    )
    op.create_table(
        "research_request_manifests",
        sa.Column("id", postgresql.UUID(), primary_key=True),
        sa.Column(
            "request_id",
            postgresql.UUID(),
            sa.ForeignKey("manual_paper_requests.id"),
            nullable=False,
        ),
        sa.Column("research_as_of", sa.DateTime(timezone=True), nullable=False),
        sa.Column("member_count", sa.Integer(), nullable=False),
        sa.Column("manifest_hash", sa.String(64), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("request_id", name="uq_research_manifest_request"),
        sa.CheckConstraint("member_count BETWEEN 1 AND 5", name="member_count_valid"),
        sa.CheckConstraint("manifest_hash ~ '^[0-9a-f]{64}$'", name="manifest_hash_valid"),
    )
    op.create_table(
        "research_request_members",
        sa.Column("id", postgresql.UUID(), primary_key=True),
        sa.Column(
            "manifest_id",
            postgresql.UUID(),
            sa.ForeignKey("research_request_manifests.id"),
            nullable=False,
        ),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column(
            "instrument_id", postgresql.UUID(), sa.ForeignKey("instruments.id"), nullable=False
        ),
        sa.Column(
            "data_snapshot_id",
            postgresql.UUID(),
            sa.ForeignKey("data_snapshots.id"),
            nullable=False,
        ),
        sa.Column(
            "run_id", postgresql.UUID(), sa.ForeignKey("agent_research_runs.id"), nullable=False
        ),
        sa.Column("job_id", postgresql.UUID(), sa.ForeignKey("jobs.id"), nullable=False),
        sa.Column("member_hash", sa.String(64), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("manifest_id", "instrument_id", name="uq_research_member_instrument"),
        sa.UniqueConstraint("manifest_id", "ordinal", name="uq_research_member_ordinal"),
        sa.UniqueConstraint("run_id", name="uq_research_member_run"),
        sa.UniqueConstraint("job_id", name="uq_research_member_job"),
        sa.CheckConstraint("ordinal BETWEEN 0 AND 4", name="ordinal_valid"),
        sa.CheckConstraint("member_hash ~ '^[0-9a-f]{64}$'", name="member_hash_valid"),
    )
    for table in ("research_request_manifests", "research_request_members"):
        op.execute(
            f"CREATE TRIGGER research_request_immutable BEFORE UPDATE OR DELETE ON {table} FOR EACH ROW EXECUTE FUNCTION nexus_manual_request_immutable()"
        )
    op.execute("""CREATE FUNCTION nexus_research_manifest_complete() RETURNS trigger LANGUAGE plpgsql AS $$
    DECLARE m research_request_manifests; r manual_paper_requests; n integer;
    BEGIN
      IF TG_TABLE_NAME='research_request_manifests' THEN
        SELECT * INTO m FROM research_request_manifests WHERE id=NEW.id;
      ELSE
        SELECT * INTO m FROM research_request_manifests WHERE id=NEW.manifest_id;
      END IF;
      SELECT * INTO r FROM manual_paper_requests WHERE id=m.request_id;
      SELECT count(*) INTO n FROM research_request_members WHERE manifest_id=m.id;
      IF r.contract_version<>2 OR r.mode<>'research_only' OR m.research_as_of<r.requested_at
        OR m.research_as_of>clock_timestamp() OR n<>m.member_count
        OR (SELECT array_agg(instrument_id ORDER BY ordinal) FROM research_request_members WHERE manifest_id=m.id)
          IS DISTINCT FROM (SELECT candidate_instrument_ids FROM agent_paper_candidate_batches WHERE id=r.candidate_batch_id)
        OR EXISTS(
          SELECT 1 FROM research_request_members x
          JOIN agent_research_runs a ON a.id=x.run_id JOIN jobs j ON j.id=x.job_id
          JOIN data_snapshots s ON s.id=x.data_snapshot_id
          WHERE x.manifest_id=m.id AND (a.instrument_id<>x.instrument_id OR a.data_snapshot_id<>x.data_snapshot_id
            OR a.decision_time<>m.research_as_of OR a.model_config_id<>r.model_config_id OR a.model_hash<>r.model_hash
            OR s.payload->>'purpose' IS DISTINCT FROM 'research_only_v2'
            OR j.payload<>jsonb_build_object('run_id',a.id::text) OR j.job_type<>'agent.paper.research'))
      THEN RAISE EXCEPTION 'RESEARCH_MANIFEST_INVALID'; END IF;
      RETURN NEW;
    END $$""")
    op.execute(
        "CREATE CONSTRAINT TRIGGER research_manifest_complete AFTER INSERT ON research_request_manifests DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION nexus_research_manifest_complete()"
    )

    op.execute(
        "CREATE CONSTRAINT TRIGGER research_member_complete AFTER INSERT ON research_request_members DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION nexus_research_manifest_complete()"
    )


def downgrade() -> None:
    op.execute(
        "DO $$ BEGIN IF EXISTS(SELECT 1 FROM manual_paper_requests WHERE contract_version=2) THEN RAISE EXCEPTION 'RESEARCH_REQUEST_DOWNGRADE_FORBIDDEN'; END IF; END $$"
    )
    op.drop_table("research_request_members")
    op.drop_table("research_request_manifests")
    op.execute("DROP FUNCTION nexus_research_manifest_complete()")
    op.drop_constraint("contract_version_valid", "manual_paper_requests", type_="check")
    op.drop_column("manual_paper_requests", "contract_version")
    op.drop_column("manual_paper_requests", "research_profile")
