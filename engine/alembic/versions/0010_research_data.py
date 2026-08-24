"""Add immutable point-in-time research evidence storage."""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "0010_research_data"
down_revision: str | None = "0009_optimization"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_UUID = postgresql.UUID(as_uuid=True)
_NUMERIC = sa.Numeric(38, 12)
_RESEARCH_TABLES = (
    "research_evidence_refs",
    "filings",
    "financial_facts",
    "corporate_actions",
    "macro_observations",
)


def upgrade() -> None:
    op.create_table(
        "research_evidence_refs",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("source", sa.String(100), nullable=False),
        sa.Column("provider_object_id", sa.String(255), nullable=False),
        sa.Column("revision", sa.String(255), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("location", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("byte_length", sa.BigInteger(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint(
            "source", "provider_object_id", "revision", name="uq_research_evidence_refs_object"
        ),
        sa.UniqueConstraint(
            "id",
            "source",
            "provider_object_id",
            "revision",
            name="uq_research_evidence_refs_owner_identity",
        ),
        sa.CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="content_hash_canonical"),
        sa.CheckConstraint("byte_length >= 0", name="byte_length_nonnegative"),
        sa.CheckConstraint(
            "(location LIKE 'memory://sha256/' || content_hash || '%') OR "
            "(location LIKE 'package://%' AND location LIKE '%@sha256:' || content_hash || '%') OR "
            "(location LIKE 's3://%' AND (location LIKE '%/sha256/' || content_hash || '%' "
            "OR location LIKE '%versionId=%'))",
            name="location_content_addressed",
        ),
    )
    op.create_table(
        "filings",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("instrument_id", _UUID, nullable=False),
        sa.Column("evidence_ref_id", _UUID, nullable=False),
        sa.Column("source", sa.String(100), nullable=False),
        sa.Column("provider_object_id", sa.String(255), nullable=False),
        sa.Column("accession_number", sa.String(32), nullable=False),
        sa.Column("form", sa.String(32), nullable=False),
        sa.Column("filed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revision", sa.String(255), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(
            ["instrument_id"], ["instruments.id"], name="fk_filings_instrument_id_instruments"
        ),
        sa.ForeignKeyConstraint(
            ["evidence_ref_id", "source", "provider_object_id", "revision"],
            [
                "research_evidence_refs.id",
                "research_evidence_refs.source",
                "research_evidence_refs.provider_object_id",
                "research_evidence_refs.revision",
            ],
            name="fk_filings_evidence_owner",
        ),
        sa.UniqueConstraint(
            "source", "accession_number", "revision", name="uq_filings_accession_revision"
        ),
        sa.UniqueConstraint(
            "id",
            "instrument_id",
            "accession_number",
            "revision",
            "evidence_ref_id",
            name="uq_filings_fact_owner",
        ),
    )
    op.create_table(
        "financial_facts",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("filing_id", _UUID, nullable=False),
        sa.Column("evidence_ref_id", _UUID, nullable=False),
        sa.Column("instrument_id", _UUID, nullable=False),
        sa.Column("canonical_fact", sa.String(64), nullable=False),
        sa.Column("raw_taxonomy", sa.String(100), nullable=False),
        sa.Column("raw_tag", sa.String(255), nullable=False),
        sa.Column("unit", sa.String(100), nullable=False),
        sa.Column("value", _NUMERIC, nullable=False),
        sa.Column("period_kind", sa.String(16), nullable=False),
        sa.Column("period_start", sa.Date(), nullable=True),
        sa.Column("period_end", sa.Date(), nullable=False),
        sa.Column("fiscal_year", sa.Integer(), nullable=True),
        sa.Column("fiscal_period", sa.String(16), nullable=True),
        sa.Column("form", sa.String(32), nullable=False),
        sa.Column("filed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("accession_number", sa.String(32), nullable=False),
        sa.Column("frame", sa.String(100), nullable=True),
        sa.Column("revision", sa.String(255), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(
            ["filing_id"], ["filings.id"], name="fk_financial_facts_filing_id_filings"
        ),
        sa.ForeignKeyConstraint(
            ["evidence_ref_id"],
            ["research_evidence_refs.id"],
            name="fk_financial_facts_evidence_ref",
        ),
        sa.ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name="fk_financial_facts_instrument_id_instruments",
        ),
        sa.ForeignKeyConstraint(
            ["filing_id", "instrument_id", "accession_number", "revision", "evidence_ref_id"],
            [
                "filings.id",
                "filings.instrument_id",
                "filings.accession_number",
                "filings.revision",
                "filings.evidence_ref_id",
            ],
            name="fk_financial_facts_filing_identity",
        ),
        sa.UniqueConstraint(
            "instrument_id",
            "canonical_fact",
            "period_kind",
            "period_start",
            "period_end",
            "unit",
            "accession_number",
            "revision",
            name="uq_financial_facts_point_in_time",
            postgresql_nulls_not_distinct=True,
        ),
        sa.CheckConstraint(
            "canonical_fact IN ('revenue', 'gross_profit', 'operating_income', 'net_income', 'operating_cash_flow', 'capital_expenditure', 'total_assets', 'total_liabilities', 'stockholders_equity', 'diluted_shares', 'diluted_eps')",
            name="canonical_fact_allowed",
        ),
        sa.CheckConstraint("period_kind IN ('instant', 'duration')", name="period_kind_allowed"),
        sa.CheckConstraint(
            "(period_kind = 'instant' AND period_start IS NULL) OR (period_kind = 'duration' AND period_start IS NOT NULL AND period_start <= period_end)",
            name="period_shape_allowed",
        ),
    )
    op.create_table(
        "corporate_actions",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("evidence_ref_id", _UUID, nullable=False),
        sa.Column("instrument_id", _UUID, nullable=True),
        sa.Column("source", sa.String(100), nullable=False),
        sa.Column("provider_event_id", sa.String(255), nullable=False),
        sa.Column("action_type", sa.String(100), nullable=False),
        sa.Column("status", sa.String(50), nullable=False),
        sa.Column("process_date", sa.Date(), nullable=False),
        sa.Column("ex_date", sa.Date(), nullable=True),
        sa.Column("record_date", sa.Date(), nullable=True),
        sa.Column("payable_date", sa.Date(), nullable=True),
        sa.Column("rate", _NUMERIC, nullable=True),
        sa.Column("revision", sa.String(255), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name="fk_corporate_actions_instrument_id_instruments",
        ),
        sa.ForeignKeyConstraint(
            ["evidence_ref_id"],
            ["research_evidence_refs.id"],
            name="fk_corporate_actions_evidence_ref",
        ),
        sa.ForeignKeyConstraint(
            ["evidence_ref_id", "source", "provider_event_id", "revision"],
            [
                "research_evidence_refs.id",
                "research_evidence_refs.source",
                "research_evidence_refs.provider_object_id",
                "research_evidence_refs.revision",
            ],
            name="fk_corporate_actions_evidence_owner",
        ),
        sa.UniqueConstraint(
            "source", "provider_event_id", "revision", name="uq_corporate_actions_event_revision"
        ),
    )
    op.add_column(
        "macro_observations", sa.Column("provider_object_id", sa.String(255), nullable=True)
    )
    op.add_column("macro_observations", sa.Column("real_time_start", sa.Date(), nullable=True))
    op.add_column("macro_observations", sa.Column("real_time_end", sa.Date(), nullable=True))
    op.add_column("macro_observations", sa.Column("evidence_ref_id", _UUID, nullable=True))
    op.add_column("macro_observations", sa.Column("calendar_evidence_ref_id", _UUID, nullable=True))
    op.add_column("macro_observations", sa.Column("calendar_source", sa.String(100), nullable=True))
    op.add_column(
        "macro_observations", sa.Column("calendar_provider_object_id", sa.String(255), nullable=True)
    )
    op.add_column("macro_observations", sa.Column("calendar_revision", sa.String(255), nullable=True))
    op.add_column(
        "macro_observations",
        sa.Column("display_sanity_only", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.create_foreign_key(
        "fk_macro_observations_evidence_ref",
        "macro_observations",
        "research_evidence_refs",
        ["evidence_ref_id"],
        ["id"],
    )
    op.create_foreign_key(
        "fk_macro_observations_evidence_owner",
        "macro_observations",
        "research_evidence_refs",
        ["evidence_ref_id", "source", "provider_object_id", "revision"],
        ["id", "source", "provider_object_id", "revision"],
    )
    op.create_foreign_key(
        "fk_macro_observations_calendar_evidence_ref",
        "macro_observations",
        "research_evidence_refs",
        ["calendar_evidence_ref_id"],
        ["id"],
    )
    op.create_foreign_key(
        "fk_macro_observations_calendar_evidence_owner",
        "macro_observations",
        "research_evidence_refs",
        [
            "calendar_evidence_ref_id",
            "calendar_source",
            "calendar_provider_object_id",
            "calendar_revision",
        ],
        ["id", "source", "provider_object_id", "revision"],
    )
    op.create_unique_constraint(
        "uq_macro_observations_provider_vintage",
        "macro_observations",
        ["source", "series", "provider_object_id", "real_time_start", "revision"],
        postgresql_nulls_not_distinct=True,
    )
    op.create_check_constraint(
        "series_allowed",
        "macro_observations",
        "series IN ('DFF', 'DGS2', 'DGS10', 'CPIAUCSL', 'UNRATE', 'INDPRO', 'VIXCLS', 'DEXHKUS')",
    )
    op.create_check_constraint(
        "real_time_period_allowed",
        "macro_observations",
        "real_time_start IS NULL OR real_time_end IS NOT NULL AND real_time_end >= real_time_start",
    )
    op.create_check_constraint(
        "macro_fred_authority_required",
        "macro_observations",
        "source <> 'fred' OR (provider_object_id IS NOT NULL AND real_time_start IS NOT NULL "
        "AND real_time_end IS NOT NULL AND evidence_ref_id IS NOT NULL "
        "AND calendar_evidence_ref_id IS NOT NULL AND calendar_source IS NOT NULL "
        "AND calendar_provider_object_id IS NOT NULL AND calendar_revision IS NOT NULL)",
    )
    op.create_check_constraint(
        "dexhkus_display_sanity_only",
        "macro_observations",
        "(series = 'DEXHKUS') = display_sanity_only",
    )
    op.execute(
        "CREATE FUNCTION nexus_prevent_research_change() RETURNS trigger LANGUAGE plpgsql AS $$ "
        "BEGIN RAISE EXCEPTION 'research evidence is append-only' USING ERRCODE = '23514'; END; $$"
    )
    for table in _RESEARCH_TABLES:
        op.execute(
            f"CREATE TRIGGER trg_{table}_immutable BEFORE UPDATE OR DELETE ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION nexus_prevent_research_change()"
        )
    for table in _RESEARCH_TABLES[:-1]:
        op.execute(
            f"CREATE TRIGGER trg_{table}_created_at_immutable BEFORE UPDATE ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION nexus_prevent_created_at_change()"
        )


def downgrade() -> None:
    for table in _RESEARCH_TABLES[:-1]:
        op.execute(f"DROP TRIGGER trg_{table}_created_at_immutable ON {table}")
    for table in _RESEARCH_TABLES:
        op.execute(f"DROP TRIGGER trg_{table}_immutable ON {table}")
    op.execute("DROP FUNCTION nexus_prevent_research_change()")
    op.drop_constraint("dexhkus_display_sanity_only", "macro_observations", type_="check")
    op.drop_constraint("macro_fred_authority_required", "macro_observations", type_="check")
    op.drop_constraint("real_time_period_allowed", "macro_observations", type_="check")
    op.drop_constraint("series_allowed", "macro_observations", type_="check")
    op.drop_constraint(
        "uq_macro_observations_provider_vintage", "macro_observations", type_="unique"
    )
    op.drop_constraint(
        "fk_macro_observations_calendar_evidence_owner", "macro_observations", type_="foreignkey"
    )
    op.drop_constraint(
        "fk_macro_observations_calendar_evidence_ref", "macro_observations", type_="foreignkey"
    )
    op.drop_constraint(
        "fk_macro_observations_evidence_owner", "macro_observations", type_="foreignkey"
    )
    op.drop_constraint(
        "fk_macro_observations_evidence_ref", "macro_observations", type_="foreignkey"
    )
    op.drop_column("macro_observations", "display_sanity_only")
    op.drop_column("macro_observations", "calendar_revision")
    op.drop_column("macro_observations", "calendar_provider_object_id")
    op.drop_column("macro_observations", "calendar_source")
    op.drop_column("macro_observations", "calendar_evidence_ref_id")
    op.drop_column("macro_observations", "evidence_ref_id")
    op.drop_column("macro_observations", "real_time_end")
    op.drop_column("macro_observations", "real_time_start")
    op.drop_column("macro_observations", "provider_object_id")
    op.drop_table("corporate_actions")
    op.drop_table("financial_facts")
    op.drop_table("filings")
    op.drop_table("research_evidence_refs")
