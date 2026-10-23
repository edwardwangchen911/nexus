"""Allow exact owner-designated review ownership for future research snapshots.

Revision ID: 0086_accounting_adoption
Revises: 0085_manual_host_exits
"""

from alembic import op

revision = "0086_accounting_adoption"
down_revision = "0085_manual_host_exits"
branch_labels = None
depends_on = None

_OLD = """              ) OR EXISTS (
                SELECT 1 FROM macro_observations"""
_NEW = """              ) OR EXISTS (
                SELECT 1 FROM public.data_snapshots AS review
                JOIN public.data_snapshots AS receipt
                  ON receipt.id::text = review.payload->>'receipt_id'
                JOIN public.data_snapshots AS designation
                  ON designation.provider_object_id = review.payload->>'designation_id'
                WHERE NEW.source = 'nexus_accounting_adoption'
                  AND review.source = NEW.source
                  AND review.schema_version = 'accounting_adoption_v1'
                  AND review.provider_object_id = NEW.provider_object_id
                  AND review.revision = NEW.revision
                  AND review.revision = NEW.content_hash
                  AND review.payload->>'instrument_id' = NEW.instrument_id::text
                  AND review.payload->>'contract' = 'as_disclosed_eps_v1'
                  AND review.available_at = NEW.available_at
                  AND review.observed_at = review.available_at
                  AND review.received_at = review.available_at
                  AND review.created_at = review.available_at
                  AND review.created_at = authority.created_at
                  AND review.created_at <= snapshot_time
                  AND receipt.source = 'nexus_accounting_review_receipt'
                  AND receipt.schema_version = 'accounting_receipt_v1'
                  AND receipt.provider_object_id = review.payload->>'submission_id'
                  AND receipt.payload->>'status' = 'received_unverified'
                  AND receipt.payload->'material' = review.payload->'material'
                  AND receipt.payload->'submission' = review.payload->'submission'
                  AND receipt.created_at <= designation.created_at
                  AND designation.source = 'nexus_accounting_designation'
                  AND designation.schema_version = 'accounting_designation_v1'
                  AND designation.revision = review.payload->>'designation_sha256'
                  AND designation.payload->>'receipt_id' = receipt.id::text
                  AND designation.payload->'designation'->>'purpose' = 'owner_designated_ai_as_disclosed_eps'
                  AND designation.created_at <= review.created_at
              ) OR EXISTS (
                SELECT 1 FROM macro_observations"""


def _replace(old: str, new: str) -> None:
    op.execute(f"""DO $migration$
    DECLARE definition text;
      old_fragment text := $old${old}$old$;
      new_fragment text := $new${new}$new$;
    BEGIN
      SELECT pg_get_functiondef('public.nexus_validate_agent_snapshot_evidence()'::regprocedure)
        INTO definition;
      IF definition IS NULL OR
        (length(definition)-length(replace(definition,old_fragment,'')))/length(old_fragment) <> 1 THEN
        RAISE EXCEPTION 'accounting adoption migration requires the expected ownership guard';
      END IF;
      EXECUTE replace(definition,old_fragment,new_fragment);
    END; $migration$;""")


def upgrade() -> None:
    _replace(_OLD, _NEW)


def downgrade() -> None:
    op.execute("""DO $migration$ BEGIN
      IF EXISTS (SELECT 1 FROM public.agent_data_snapshot_evidence
                 WHERE source='nexus_accounting_adoption') THEN
        RAISE EXCEPTION 'cannot remove ownership support for frozen accounting reviews';
      END IF;
    END; $migration$;""")
    _replace(_NEW, _OLD)
