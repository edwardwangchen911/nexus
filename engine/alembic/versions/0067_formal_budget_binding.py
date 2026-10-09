"""Optional immutable formal budget-day authority. No existing day is enabled.

Revision ID: 0067_formal_budget_binding
Revises: 0066_research_identity_binding
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0067_formal_budget_binding"
down_revision = "0066_research_identity_binding"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "formal_budget_bindings",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("budget_domain", sa.String(32), nullable=False),
        sa.Column("business_date", sa.Date(), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column(
            "previous_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("formal_budget_bindings.id")
        ),
        sa.Column("idempotency_key", sa.String(200), nullable=False, unique=True),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("schema_version", sa.String(64), nullable=False),
        sa.Column("daily_limit_usd", sa.Numeric(38, 12), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("applied_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "audit_event_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("audit_events.id"),
            nullable=False,
            unique=True,
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("budget_domain", "business_date", "sequence"),
        sa.CheckConstraint("budget_domain = 'agent_global_v1'", name="domain_fixed"),
        sa.CheckConstraint("sequence > 0", name="sequence_positive"),
        sa.CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="hash_canonical"),
        sa.CheckConstraint("daily_limit_usd >= 0", name="limit_nonnegative"),
    )
    op.execute("""
        CREATE FUNCTION reject_formal_budget_binding_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN
            RAISE EXCEPTION 'FORMAL_BUDGET_BINDING_IMMUTABLE';
        END $$;
    """)
    op.execute("""
        CREATE TRIGGER formal_budget_binding_immutable BEFORE UPDATE OR DELETE
        ON formal_budget_bindings FOR EACH ROW
        EXECUTE FUNCTION reject_formal_budget_binding_mutation();
    """)
    for table in ("agent_paper_cycles", "idempotency_records"):
        op.add_column(table, sa.Column("formal_budget_binding_id", postgresql.UUID(as_uuid=True)))
        op.create_foreign_key(
            f"fk_{table}_formal_budget_binding",
            table,
            "formal_budget_bindings",
            ["formal_budget_binding_id"],
            ["id"],
        )
    op.execute("""
        CREATE FUNCTION reject_formal_budget_pin_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN
            IF OLD.formal_budget_binding_id IS NOT NULL AND
               OLD.formal_budget_binding_id IS DISTINCT FROM NEW.formal_budget_binding_id THEN
                RAISE EXCEPTION 'FORMAL_BUDGET_PIN_IMMUTABLE';
            END IF;
            RETURN NEW;
        END $$;
    """)
    for table in ("agent_paper_cycles", "idempotency_records"):
        op.execute(f"""
            CREATE TRIGGER formal_budget_pin_immutable BEFORE UPDATE
            ON {table} FOR EACH ROW EXECUTE FUNCTION reject_formal_budget_pin_mutation();
        """)


def downgrade() -> None:
    # Refuse to discard commitments. Turning off producers does not erase facts.
    op.execute("""
        DO $$ BEGIN
            IF EXISTS (SELECT 1 FROM formal_budget_bindings) THEN
                RAISE EXCEPTION 'FORMAL_BUDGET_BINDING_ROLLBACK_REQUIRES_EMPTY';
            END IF;
        END $$;
    """)
    for table in ("agent_paper_cycles", "idempotency_records"):
        op.execute(f"DROP TRIGGER formal_budget_pin_immutable ON {table}")
        op.drop_constraint(f"fk_{table}_formal_budget_binding", table, type_="foreignkey")
        op.drop_column(table, "formal_budget_binding_id")
    op.drop_table("formal_budget_bindings")
    op.execute("DROP FUNCTION reject_formal_budget_binding_mutation()")
    op.execute("DROP FUNCTION reject_formal_budget_pin_mutation()")
