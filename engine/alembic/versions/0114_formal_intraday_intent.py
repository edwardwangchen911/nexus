"""Same-session explicit consent, independent of admitted Paper work."""

from alembic import op

revision = "0114_formal_intraday_intent"
down_revision = "0113_sol_business_profile"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE formal_intraday_intents (
            id uuid PRIMARY KEY, account_id uuid NOT NULL REFERENCES paper_accounts(id),
            batch_id uuid NOT NULL REFERENCES agent_paper_candidate_batches(id),
            model_id uuid NOT NULL REFERENCES agent_model_configs(id),
            key_hash varchar(64) NOT NULL, scope_hash varchar(64) NOT NULL,
            calendar_id uuid NOT NULL, session_date date NOT NULL,
            registered_at timestamptz NOT NULL, expires_at timestamptz NOT NULL,
            state text NOT NULL DEFAULT 'pending', code text NOT NULL,
            checked_at timestamptz, request_id uuid UNIQUE REFERENCES manual_paper_requests(id),
            UNIQUE (account_id,key_hash),
            CHECK (registered_at < expires_at AND key_hash ~ '^[0-9a-f]{64}$'
                AND scope_hash ~ '^[0-9a-f]{64}$'),
            CHECK (state IN ('pending','checking','converted','expired','cancelled','rejected')),
            CHECK ((state='converted')=(request_id IS NOT NULL)),
            CHECK (request_id IS NULL OR request_id=id)
        )
    """)
    op.execute("""
        CREATE UNIQUE INDEX uq_formal_intent_pending_account ON formal_intraday_intents(account_id)
        WHERE state IN ('pending','checking')
    """)
    op.execute("""
        CREATE FUNCTION nexus_guard_intraday_intent() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_OP='DELETE' THEN RAISE EXCEPTION 'FORMAL_INTENT_IMMUTABLE'; END IF;
            IF TG_OP='INSERT' THEN
                IF NEW.state<>'pending' THEN RAISE EXCEPTION 'FORMAL_INTENT_INVALID'; END IF;
                RETURN NEW;
            END IF;
            IF (to_jsonb(OLD)-ARRAY['state','code','checked_at','request_id'])
                IS DISTINCT FROM (to_jsonb(NEW)-ARRAY['state','code','checked_at','request_id'])
                OR OLD.state NOT IN ('pending','checking')
                OR (OLD.state='pending' AND NEW.state='converted')
                OR (OLD.state='checking' AND NEW.state IN ('cancelled','checking'))
            THEN RAISE EXCEPTION 'FORMAL_INTENT_IMMUTABLE'; END IF;
            IF NEW.state='converted' AND NOT EXISTS(
                SELECT FROM manual_paper_requests r WHERE r.id=NEW.id AND r.account_id=NEW.account_id
                AND r.key_hash=NEW.key_hash AND r.candidate_batch_id=NEW.batch_id
                AND r.model_config_id=NEW.model_id AND r.mode='research_and_paper'
                AND r.requested_at>=NEW.registered_at AND r.requested_at<NEW.expires_at
            ) THEN RAISE EXCEPTION 'FORMAL_INTENT_INVALID'; END IF;
            RETURN NEW;
        END $$
    """)
    op.execute("""
        CREATE TRIGGER formal_intent_guard BEFORE INSERT OR UPDATE OR DELETE ON formal_intraday_intents
        FOR EACH ROW EXECUTE FUNCTION nexus_guard_intraday_intent()
    """)


def downgrade() -> None:
    op.execute("""
        DO $$ BEGIN IF EXISTS(SELECT FROM formal_intraday_intents) THEN
            RAISE EXCEPTION 'FORMAL_INTENT_DOWNGRADE_FORBIDDEN'; END IF; END $$;
    """)
    op.execute("DROP TABLE formal_intraday_intents")
    op.execute("DROP FUNCTION nexus_guard_intraday_intent()")
