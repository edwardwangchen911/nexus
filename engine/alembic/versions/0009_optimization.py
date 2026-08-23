"""Add immutable three-way optimization proposal persistence."""

from collections.abc import Sequence
from uuid import UUID

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "0009_optimization"
down_revision: str | None = "0008_diagnostics"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_UUID = postgresql.UUID(as_uuid=True)
_NUMERIC = sa.Numeric(38, 12)


def upgrade() -> None:
    op.create_table(
        "optimization_policies",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("key", sa.String(100), nullable=False),
        sa.Column("version", sa.String(100), nullable=False),
        sa.Column("schema_version", sa.String(32), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("key", "version", name="uq_optimization_policies_key_version"),
    )
    op.create_table(
        "cost_policies",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("key", sa.String(100), nullable=False),
        sa.Column("version", sa.String(100), nullable=False),
        sa.Column("schema_version", sa.String(32), nullable=False),
        sa.Column("parameters", postgresql.JSONB(), nullable=False),
        sa.Column("evidence_refs", postgresql.JSONB(), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("key", "version", name="uq_cost_policies_key_version"),
    )
    op.create_table(
        "baseline_selection_authorities",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("policy_version", sa.String(32), nullable=False),
        sa.Column("data_snapshot_id", _UUID, nullable=False),
        sa.Column("decision_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("evidence_refs", postgresql.JSONB(), nullable=False),
        sa.Column("candidates", postgresql.JSONB(), nullable=False),
        sa.Column("selected_instrument_ids", postgresql.JSONB(), nullable=False),
        sa.Column("evidence_hash", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["data_snapshot_id"], ["data_snapshots.id"]),
        sa.UniqueConstraint("id", "data_snapshot_id", name="uq_selection_authorities_id_data"),
        sa.CheckConstraint("policy_version = 'baseline_v1'", name="policy_version_allowed"),
        sa.CheckConstraint("available_at <= decision_at", name="available_before_decision"),
        sa.CheckConstraint(
            "evidence_hash ~ '^[0-9a-f]{64}$'", name="evidence_hash_canonical"
        ),
    )
    op.create_table(
        "optimization_admission_authorities",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("portfolio_snapshot_id", _UUID, nullable=False),
        sa.Column("diagnostic_report_id", _UUID, nullable=False),
        sa.Column("data_snapshot_id", _UUID, nullable=False),
        sa.Column("source_policy_id", _UUID, nullable=False),
        sa.Column("optimizer_policy_id", _UUID, nullable=False),
        sa.Column("cost_policy_id", _UUID, nullable=False),
        sa.Column("diagnostic_input_hash", sa.String(64), nullable=False),
        sa.Column("valuation_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("baseline_ready", sa.Boolean(), nullable=False),
        sa.Column("diagnostic_complete", sa.Boolean(), nullable=False),
        sa.Column("source_conflict", sa.Boolean(), nullable=False),
        sa.Column("partial", sa.Boolean(), nullable=False),
        sa.Column("requires_fx", sa.Boolean(), nullable=False),
        sa.Column("cost_complete", sa.Boolean(), nullable=False),
        sa.Column("authority_stale", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("selection_authority_id", _UUID, nullable=True),
        sa.Column("portfolio_scope", sa.String(30), nullable=False, server_default="real_read_only"),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(["portfolio_snapshot_id"], ["portfolio_snapshots.id"]),
        sa.ForeignKeyConstraint(["diagnostic_report_id"], ["portfolio_diagnostic_reports.id"]),
        sa.ForeignKeyConstraint(["data_snapshot_id"], ["data_snapshots.id"]),
        sa.ForeignKeyConstraint(["source_policy_id"], ["source_policies.id"]),
        sa.ForeignKeyConstraint(["optimizer_policy_id"], ["optimization_policies.id"]),
        sa.ForeignKeyConstraint(["cost_policy_id"], ["cost_policies.id"]),
        sa.ForeignKeyConstraint(["selection_authority_id"], ["baseline_selection_authorities.id"]),
        sa.ForeignKeyConstraint(
            ["selection_authority_id", "data_snapshot_id"],
            ["baseline_selection_authorities.id", "baseline_selection_authorities.data_snapshot_id"],
            name="fk_admission_selection_same_data",
        ),
        sa.UniqueConstraint(
            "portfolio_snapshot_id",
            "data_snapshot_id",
            "cost_policy_id",
            name="uq_optimization_admission_authorities_binding",
        ),
        sa.CheckConstraint("portfolio_scope IN ('real_read_only','internal_paper','fosun_simulation')", name="portfolio_scope_allowed"),
    )
    op.create_table(
        "optimization_proposal_groups",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("baseline_portfolio_snapshot_id", _UUID, nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="draft"),
        sa.Column("version", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("policy_id", _UUID, nullable=False),
        sa.Column("portfolio_scope", sa.String(30), nullable=False, server_default="real_read_only"),
        sa.Column("selection_authority_id", _UUID, nullable=True),
        sa.Column("diagnostic_report_id", _UUID, nullable=False),
        sa.Column("data_snapshot_id", _UUID, nullable=False),
        sa.Column("source_policy_id", _UUID, nullable=False),
        sa.Column("cost_policy_id", _UUID, nullable=False),
        sa.Column("diagnostic_input_hash", sa.String(64), nullable=False),
        sa.Column("valuation_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("baseline_ready", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("diagnostic_complete", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("source_conflict", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("partial", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("requires_fx", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("cost_complete", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("authority_stale", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(["baseline_portfolio_snapshot_id"], ["portfolio_snapshots.id"]),
        sa.ForeignKeyConstraint(["policy_id"], ["optimization_policies.id"]),
        sa.ForeignKeyConstraint(["diagnostic_report_id"], ["portfolio_diagnostic_reports.id"]),
        sa.ForeignKeyConstraint(["data_snapshot_id"], ["data_snapshots.id"]),
        sa.ForeignKeyConstraint(["source_policy_id"], ["source_policies.id"]),
        sa.ForeignKeyConstraint(["cost_policy_id"], ["cost_policies.id"]),
        sa.ForeignKeyConstraint(["selection_authority_id"], ["baseline_selection_authorities.id"]),
        sa.ForeignKeyConstraint(
            ["selection_authority_id", "data_snapshot_id"],
            ["baseline_selection_authorities.id", "baseline_selection_authorities.data_snapshot_id"],
            name="fk_group_selection_same_data",
        ),
        sa.UniqueConstraint("id", "baseline_portfolio_snapshot_id", name="uq_optimization_groups_id_baseline"),
        sa.CheckConstraint("portfolio_scope IN ('real_read_only','internal_paper','fosun_simulation')", name="portfolio_scope_allowed"),
        sa.CheckConstraint(
            "status IN ('draft','compared','shadowing','validated','rejected','expired')",
            name="status_allowed",
        ),
    )
    op.create_table(
        "optimization_proposals",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("proposal_group_id", _UUID, nullable=False),
        sa.Column("variant", sa.String(30), nullable=False),
        sa.Column("baseline_portfolio_snapshot_id", _UUID, nullable=False),
        sa.Column("analysis_only", sa.Boolean(), nullable=False),
        sa.Column("strategy_evidence_complete", sa.Boolean(), nullable=False),
        sa.Column("portfolio_scope", sa.String(30), nullable=False, server_default="real_read_only"),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(["proposal_group_id", "baseline_portfolio_snapshot_id"], ["optimization_proposal_groups.id", "optimization_proposal_groups.baseline_portfolio_snapshot_id"], name="fk_optimization_proposals_group_baseline"),
        sa.UniqueConstraint(
            "proposal_group_id", "variant", name="uq_optimization_proposals_group_variant"
        ),
        sa.CheckConstraint(
            "variant IN ('hold_current','minimum_change','strategy_target')", name="variant_allowed"
        ),
        sa.CheckConstraint("portfolio_scope IN ('real_read_only','internal_paper','fosun_simulation')", name="portfolio_scope_allowed"),
    )
    op.create_table(
        "proposal_comparisons",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("proposal_id", _UUID, nullable=False),
        sa.Column("metrics", postgresql.JSONB(), nullable=False),
        sa.Column("tax_impact", sa.String(20), nullable=False, server_default="unknown"),
        sa.Column("requires_fx", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("non_executable", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("cost_complete", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(["proposal_id"], ["optimization_proposals.id"]),
        sa.UniqueConstraint("proposal_id", name="uq_proposal_comparisons_proposal"),
        sa.CheckConstraint("tax_impact = 'unknown'", name="tax_impact_unknown"),
    )
    op.create_table(
        "proposal_positions",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("proposal_id", _UUID, nullable=False),
        sa.Column("instrument_id", _UUID, nullable=False),
        sa.Column("current_weight", _NUMERIC, nullable=False),
        sa.Column("target_weight", _NUMERIC, nullable=False),
        sa.Column("action", sa.String(20), nullable=False),
        sa.Column("reason_code", sa.String(100), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(["proposal_id"], ["optimization_proposals.id"]),
        sa.ForeignKeyConstraint(["instrument_id"], ["instruments.id"]),
        sa.UniqueConstraint(
            "proposal_id", "instrument_id", name="uq_proposal_positions_proposal_instrument"
        ),
    )
    op.execute(
        sa.text(
            "INSERT INTO optimization_policies (id, key, version, schema_version, payload) VALUES (:id, 'optimizer', 'optimizer_v1', '1', '{}'::jsonb)"
        ).bindparams(id=UUID("e690b91d-1e24-5de3-b2b2-566a463fca25"))
    )
    op.execute(
        sa.text(
            "INSERT INTO cost_policies (id, key, version, schema_version, parameters, evidence_refs, available_at) VALUES (:id, 'cost_model', 'cost_policy_v1', '1', CAST(:parameters AS jsonb), CAST(:evidence_refs AS jsonb), '2020-01-01T00:00:00+00:00')"
        ).bindparams(
            id=UUID("d9cdf971-59f6-5a52-9df4-4e1f861d8075"),
            parameters='{"commission_bps":"0","slippage_bps":"0","tax_treatment":"unknown"}',
            evidence_refs='["cost_policy_v1_methodology"]',
        )
    )
    op.execute(
        "CREATE FUNCTION nexus_prevent_optimization_change() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'optimization record is immutable' USING ERRCODE = '23514'; END; $$"
    )
    op.execute(
        "CREATE FUNCTION nexus_validate_selection_authority() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
        "IF jsonb_typeof(NEW.evidence_refs) <> 'array' OR jsonb_array_length(NEW.evidence_refs) < 1 OR EXISTS (SELECT 1 FROM jsonb_array_elements(NEW.evidence_refs) ref WHERE jsonb_typeof(ref) <> 'string' OR btrim(ref #>> '{}') = '') THEN RAISE EXCEPTION 'selection evidence refs are invalid' USING ERRCODE = '23514'; END IF; "
        "IF jsonb_typeof(NEW.candidates) <> 'array' OR jsonb_array_length(NEW.candidates) < 1 OR EXISTS (SELECT 1 FROM jsonb_array_elements(NEW.candidates) candidate WHERE jsonb_typeof(candidate) <> 'object' OR (SELECT count(*) FROM jsonb_object_keys(candidate)) <> 2 OR jsonb_typeof(candidate->'instrument_id') <> 'string' OR candidate->>'instrument_id' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' OR jsonb_typeof(candidate->'provenance_refs') <> 'array' OR jsonb_array_length(candidate->'provenance_refs') < 1 OR EXISTS (SELECT 1 FROM jsonb_array_elements(candidate->'provenance_refs') ref WHERE jsonb_typeof(ref) <> 'string' OR btrim(ref #>> '{}') = '')) THEN RAISE EXCEPTION 'selection candidates are invalid' USING ERRCODE = '23514'; END IF; "
        "IF (SELECT count(*) FROM jsonb_array_elements(NEW.candidates)) <> (SELECT count(DISTINCT candidate->>'instrument_id') FROM jsonb_array_elements(NEW.candidates) candidate) THEN RAISE EXCEPTION 'selection candidates must be unique' USING ERRCODE = '23514'; END IF; "
        "IF jsonb_typeof(NEW.selected_instrument_ids) <> 'array' OR jsonb_array_length(NEW.selected_instrument_ids) NOT BETWEEN 1 AND 6 OR EXISTS (SELECT 1 FROM jsonb_array_elements(NEW.selected_instrument_ids) selected WHERE jsonb_typeof(selected) <> 'string' OR selected #>> '{}' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$') OR (SELECT count(*) FROM jsonb_array_elements(NEW.selected_instrument_ids)) <> (SELECT count(DISTINCT selected #>> '{}') FROM jsonb_array_elements(NEW.selected_instrument_ids) selected) OR EXISTS (SELECT 1 FROM jsonb_array_elements(NEW.selected_instrument_ids) selected WHERE selected #>> '{}' NOT IN (SELECT candidate->>'instrument_id' FROM jsonb_array_elements(NEW.candidates) candidate)) THEN RAISE EXCEPTION 'selected instruments are invalid' USING ERRCODE = '23514'; END IF; "
        "RETURN NEW; END; $$"
    )
    op.execute(
        "CREATE TRIGGER trg_baseline_selection_authorities_validate BEFORE INSERT OR UPDATE ON baseline_selection_authorities FOR EACH ROW EXECUTE FUNCTION nexus_validate_selection_authority()"
    )
    op.execute(
        "CREATE FUNCTION nexus_optimization_group_lifecycle() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
        "IF NEW.baseline_portfolio_snapshot_id IS DISTINCT FROM OLD.baseline_portfolio_snapshot_id OR NEW.portfolio_scope IS DISTINCT FROM OLD.portfolio_scope OR NEW.selection_authority_id IS DISTINCT FROM OLD.selection_authority_id OR NEW.policy_id IS DISTINCT FROM OLD.policy_id OR NEW.diagnostic_report_id IS DISTINCT FROM OLD.diagnostic_report_id OR NEW.data_snapshot_id IS DISTINCT FROM OLD.data_snapshot_id OR NEW.source_policy_id IS DISTINCT FROM OLD.source_policy_id OR NEW.cost_policy_id IS DISTINCT FROM OLD.cost_policy_id OR NEW.diagnostic_input_hash IS DISTINCT FROM OLD.diagnostic_input_hash OR NEW.valuation_time IS DISTINCT FROM OLD.valuation_time OR NEW.baseline_ready IS DISTINCT FROM OLD.baseline_ready OR NEW.diagnostic_complete IS DISTINCT FROM OLD.diagnostic_complete OR NEW.source_conflict IS DISTINCT FROM OLD.source_conflict OR NEW.partial IS DISTINCT FROM OLD.partial OR NEW.requires_fx IS DISTINCT FROM OLD.requires_fx OR NEW.cost_complete IS DISTINCT FROM OLD.cost_complete OR NEW.authority_stale IS DISTINCT FROM OLD.authority_stale THEN RAISE EXCEPTION 'optimization group authority is immutable' USING ERRCODE = '23514'; END IF; "
        "IF NEW.version <> OLD.version + 1 THEN RAISE EXCEPTION 'optimization group version conflict' USING ERRCODE = '40001'; END IF; "
        "IF (OLD.status = 'draft' AND NEW.status NOT IN ('compared','rejected','expired')) OR (OLD.status = 'compared' AND NEW.status NOT IN ('shadowing','rejected','expired')) OR (OLD.status = 'shadowing' AND NEW.status NOT IN ('validated','rejected','expired')) OR (OLD.status IN ('validated','rejected','expired')) THEN RAISE EXCEPTION 'optimization lifecycle transition invalid' USING ERRCODE = '23514'; END IF; "
        "IF NEW.status = 'shadowing' AND EXISTS (SELECT 1 FROM proposal_comparisons c JOIN optimization_proposals p ON p.id = c.proposal_id WHERE p.proposal_group_id = OLD.id AND (jsonb_typeof(c.metrics->'dimensions') IS DISTINCT FROM 'object' OR jsonb_typeof(c.metrics->'dimensions'->'hard_violations') IS DISTINCT FROM 'number' OR c.metrics->'dimensions'->'tax_impact'->>'status' IS DISTINCT FROM 'unknown' OR EXISTS (SELECT 1 FROM unnest(ARRAY['top1','top3','hhi','sector_exposure','currency_exposure','volatility','drawdown','correlation','liquidity','turnover','cost']) dimension WHERE c.metrics->'dimensions'->dimension->>'status' IS DISTINCT FROM 'known') OR c.metrics->'authority' IS DISTINCT FROM jsonb_build_object('diagnostic_report_id', OLD.diagnostic_report_id::text, 'data_snapshot_id', OLD.data_snapshot_id::text, 'source_policy_id', OLD.source_policy_id::text, 'optimizer_policy_id', OLD.policy_id::text, 'cost_policy_id', OLD.cost_policy_id::text))) THEN RAISE EXCEPTION 'PROPOSAL_NOT_EXECUTABLE' USING ERRCODE = '23514'; END IF; "
        "IF NEW.status = 'shadowing' AND (NOT OLD.baseline_ready OR NOT OLD.diagnostic_complete OR OLD.source_conflict OR OLD.partial OR OLD.requires_fx OR NOT OLD.cost_complete OR OLD.authority_stale OR OLD.selection_authority_id IS NULL OR NOT EXISTS (SELECT 1 FROM baseline_selection_authorities b WHERE b.id = OLD.selection_authority_id AND b.data_snapshot_id = OLD.data_snapshot_id AND b.policy_version = 'baseline_v1' AND b.available_at <= OLD.valuation_time AND b.decision_at <= OLD.valuation_time) OR OLD.source_policy_id IS DISTINCT FROM '3c60888d-e92d-5e04-913c-7d427004cfef'::uuid OR OLD.cost_policy_id IS DISTINCT FROM 'd9cdf971-59f6-5a52-9df4-4e1f861d8075'::uuid OR NOT EXISTS (SELECT 1 FROM source_policies s WHERE s.id = OLD.source_policy_id AND s.key = 'market_price' AND s.version = 'source_policy_v1' AND s.schema_version = '1' AND s.created_at <= OLD.valuation_time AND s.payload = '{\"priority\":[\"fosun\",\"alpaca\",\"fixture\"],\"fosun\":{\"mode\":\"read_only\",\"operation\":\"fosun_get_bars\",\"market_scope\":\"US\"},\"alpaca\":{\"feed\":\"sip\",\"modes\":[\"delayed\",\"fixture\"]},\"fixture\":{\"mode\":\"fixture\"}}'::jsonb) OR NOT EXISTS (SELECT 1 FROM cost_policies c WHERE c.id = OLD.cost_policy_id AND c.key = 'cost_model' AND c.version = 'cost_policy_v1' AND c.schema_version = '1' AND c.parameters = '{\"commission_bps\":\"0\",\"slippage_bps\":\"0\",\"tax_treatment\":\"unknown\"}'::jsonb AND c.evidence_refs = '[\"cost_policy_v1_methodology\"]'::jsonb AND c.available_at <= OLD.valuation_time) OR NOT EXISTS (SELECT 1 FROM optimization_proposals p WHERE p.proposal_group_id = OLD.id AND p.variant = 'strategy_target' AND p.strategy_evidence_complete AND (OLD.portfolio_scope = 'real_read_only' OR NOT p.analysis_only)) OR (SELECT count(*) FROM proposal_comparisons c JOIN optimization_proposals p ON p.id = c.proposal_id WHERE p.proposal_group_id = OLD.id AND NOT c.non_executable AND NOT c.requires_fx AND c.cost_complete AND COALESCE((c.metrics->>'ranking_eligible')::boolean, false) AND COALESCE((c.metrics->>'constraint_complete')::boolean, false) AND COALESCE((c.metrics->>'eligibility_complete')::boolean, false) AND COALESCE((c.metrics->>'evidence_complete')::boolean, false) AND NOT COALESCE((c.metrics->>'stale')::boolean, true) AND COALESCE(c.metrics->'unknown_metrics', 'null'::jsonb) = '[]'::jsonb AND c.metrics->>'cost' IS NOT NULL) <> 3) THEN RAISE EXCEPTION 'PROPOSAL_NOT_EXECUTABLE' USING ERRCODE = '23514'; END IF; RETURN NEW; END; $$"
    )
    op.execute(
        "CREATE FUNCTION nexus_require_three_optimization_variants() RETURNS trigger LANGUAGE plpgsql AS $$ DECLARE group_ref uuid; variant_count integer; comparison_count integer; scope_count integer; BEGIN IF TG_TABLE_NAME = 'optimization_proposal_groups' THEN group_ref := NEW.id; ELSIF TG_TABLE_NAME = 'proposal_comparisons' THEN SELECT p.proposal_group_id INTO group_ref FROM optimization_proposals p WHERE p.id = COALESCE(NEW.proposal_id, OLD.proposal_id); ELSE group_ref := COALESCE(NEW.proposal_group_id, OLD.proposal_group_id); END IF; SELECT count(*) INTO variant_count FROM optimization_proposals WHERE proposal_group_id = group_ref; SELECT count(*) INTO comparison_count FROM proposal_comparisons c JOIN optimization_proposals p ON p.id = c.proposal_id WHERE p.proposal_group_id = group_ref; SELECT count(*) INTO scope_count FROM optimization_proposals p JOIN optimization_proposal_groups g ON g.id = p.proposal_group_id WHERE p.proposal_group_id = group_ref AND p.portfolio_scope = g.portfolio_scope AND p.analysis_only = (g.portfolio_scope = 'real_read_only'); IF variant_count <> 3 OR comparison_count <> 3 OR scope_count <> 3 THEN RAISE EXCEPTION 'optimization group requires exact scope-derived variants and comparisons' USING ERRCODE = '23514'; END IF; RETURN NULL; END; $$"
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_optimization_groups_three_variants AFTER INSERT ON optimization_proposal_groups DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION nexus_require_three_optimization_variants()"
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_optimization_proposals_three_variants AFTER INSERT OR DELETE ON optimization_proposals DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION nexus_require_three_optimization_variants()"
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_proposal_comparisons_three_variants AFTER INSERT OR DELETE ON proposal_comparisons DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION nexus_require_three_optimization_variants()"
    )
    op.execute(
        "CREATE TRIGGER trg_optimization_groups_lifecycle BEFORE UPDATE ON optimization_proposal_groups FOR EACH ROW EXECUTE FUNCTION nexus_optimization_group_lifecycle()"
    )
    op.execute(
        "CREATE FUNCTION nexus_validate_strategy_position() RETURNS trigger LANGUAGE plpgsql AS $$ DECLARE proposal_variant text; evidence_complete boolean; selection_ref uuid; BEGIN SELECT p.variant, p.strategy_evidence_complete, g.selection_authority_id INTO proposal_variant, evidence_complete, selection_ref FROM optimization_proposals p JOIN optimization_proposal_groups g ON g.id = p.proposal_group_id WHERE p.id = NEW.proposal_id; IF proposal_variant = 'strategy_target' AND NOT evidence_complete AND NEW.target_weight IS DISTINCT FROM NEW.current_weight THEN RAISE EXCEPTION 'incomplete strategy must preserve baseline' USING ERRCODE = '23514'; END IF; IF proposal_variant = 'strategy_target' AND evidence_complete AND (selection_ref IS NULL OR NEW.target_weight > 0.125 OR (NEW.target_weight > 0 AND NOT EXISTS (SELECT 1 FROM baseline_selection_authorities b, jsonb_array_elements_text(b.selected_instrument_ids) selected WHERE b.id = selection_ref AND selected = NEW.instrument_id::text))) THEN RAISE EXCEPTION 'strategy target exceeds bound selection' USING ERRCODE = '23514'; END IF; RETURN NEW; END; $$"
    )
    op.execute(
        "CREATE TRIGGER trg_proposal_positions_strategy_binding BEFORE INSERT ON proposal_positions FOR EACH ROW EXECUTE FUNCTION nexus_validate_strategy_position()"
    )
    for table in (
        "optimization_policies",
        "cost_policies",
        "baseline_selection_authorities",
        "optimization_admission_authorities",
        "optimization_proposal_groups",
        "optimization_proposals",
        "proposal_positions",
        "proposal_comparisons",
    ):
        op.execute(
            f"CREATE TRIGGER trg_{table}_created_at_immutable BEFORE UPDATE ON {table} FOR EACH ROW EXECUTE FUNCTION nexus_prevent_created_at_change()"
        )
    for table in (
        "optimization_policies",
        "cost_policies",
        "baseline_selection_authorities",
        "optimization_admission_authorities",
        "optimization_proposals",
        "proposal_positions",
        "proposal_comparisons",
    ):
        op.execute(
            f"CREATE TRIGGER trg_{table}_immutable BEFORE UPDATE OR DELETE ON {table} FOR EACH ROW EXECUTE FUNCTION nexus_prevent_optimization_change()"
        )


def downgrade() -> None:
    op.execute("DROP TRIGGER trg_proposal_positions_strategy_binding ON proposal_positions")
    op.execute("DROP FUNCTION nexus_validate_strategy_position()")
    op.execute(
        "DROP TRIGGER trg_baseline_selection_authorities_validate ON baseline_selection_authorities"
    )
    for table in (
        "proposal_comparisons",
        "proposal_positions",
        "optimization_proposals",
        "optimization_proposal_groups",
        "optimization_admission_authorities",
        "baseline_selection_authorities",
        "cost_policies",
        "optimization_policies",
    ):
        op.execute(f"DROP TRIGGER trg_{table}_created_at_immutable ON {table}")
    op.execute("DROP TRIGGER trg_proposal_comparisons_three_variants ON proposal_comparisons")
    op.execute("DROP TRIGGER trg_optimization_proposals_three_variants ON optimization_proposals")
    op.execute("DROP TRIGGER trg_optimization_groups_lifecycle ON optimization_proposal_groups")
    op.execute(
        "DROP TRIGGER trg_optimization_groups_three_variants ON optimization_proposal_groups"
    )
    op.execute("DROP FUNCTION nexus_require_three_optimization_variants()")
    op.execute("DROP FUNCTION nexus_optimization_group_lifecycle()")
    for table in (
        "proposal_comparisons",
        "proposal_positions",
        "optimization_proposals",
        "optimization_admission_authorities",
        "baseline_selection_authorities",
        "cost_policies",
        "optimization_policies",
    ):
        op.execute(f"DROP TRIGGER trg_{table}_immutable ON {table}")
    op.execute("DROP FUNCTION nexus_prevent_optimization_change()")
    op.execute("DROP FUNCTION nexus_validate_selection_authority()")
    op.drop_table("proposal_comparisons")
    op.drop_table("proposal_positions")
    op.drop_table("optimization_proposals")
    op.drop_table("optimization_proposal_groups")
    op.drop_table("optimization_admission_authorities")
    op.drop_table("baseline_selection_authorities")
    op.drop_table("cost_policies")
    op.drop_table("optimization_policies")
