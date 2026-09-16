"""Compare paired Agent lanes on account-neutral market authority.

Revision ID: 0063_agent_version_pair_context
Revises: 0062_agent_version_evaluation
Create Date: 2026-09-17
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0063_agent_version_pair_context"
down_revision: str | Sequence[str] | None = "0062_agent_version_evaluation"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _pair_guard_sql(*, account_neutral: bool) -> str:
    market_declaration = (
        "expected_market_evidence_hash varchar(64);" if account_neutral else ""
    )
    market_assignment = (
        """
          expected_market_evidence_hash := pg_catalog.encode(
            public.digest(
              pg_catalog.convert_to(
                'agent_version_market_evidence_v1|' || incumbent_cycle.candidate_hash,
                'UTF8'
              ),
              'sha256'
            ),
            'hex'
          );
        """
        if account_neutral
        else ""
    )
    market_checks = (
        """
             OR NEW.market_evidence_hash IS DISTINCT FROM expected_market_evidence_hash
        """
        if account_neutral
        else """
             OR incumbent_run.data_snapshot_id <> challenger_run.data_snapshot_id
             OR incumbent_run.evidence_hash <> challenger_run.evidence_hash
             OR NEW.market_evidence_hash <> incumbent_run.evidence_hash
        """
    )
    return f"""
        CREATE OR REPLACE FUNCTION nexus_agent_version_pair_valid()
        RETURNS trigger LANGUAGE plpgsql
        SET search_path TO pg_catalog, public, pg_temp AS $$
        DECLARE
          evaluation public.agent_version_evaluations%ROWTYPE;
          incumbent_cycle public.agent_paper_cycles%ROWTYPE;
          challenger_cycle public.agent_paper_cycles%ROWTYPE;
          incumbent_run public.agent_research_runs%ROWTYPE;
          challenger_run public.agent_research_runs%ROWTYPE;
          incumbent_decision public.strategy_decisions%ROWTYPE;
          challenger_decision public.strategy_decisions%ROWTYPE;
          expected_starting_projection_hash varchar(64);
          {market_declaration}
        BEGIN
          SELECT * INTO evaluation FROM public.agent_version_evaluations
          WHERE id=NEW.evaluation_id FOR SHARE;
          PERFORM pg_catalog.pg_advisory_xact_lock(
            pg_catalog.hashtextextended('nexus:agent-version-evaluation:' || NEW.evaluation_id::text, 0)
          );
          SELECT * INTO incumbent_cycle FROM public.agent_paper_cycles
          WHERE id=NEW.incumbent_cycle_id FOR SHARE;
          SELECT * INTO challenger_cycle FROM public.agent_paper_cycles
          WHERE id=NEW.challenger_cycle_id FOR SHARE;
          SELECT * INTO incumbent_run FROM public.agent_research_runs
          WHERE id=NEW.incumbent_run_id FOR SHARE;
          SELECT * INTO challenger_run FROM public.agent_research_runs
          WHERE id=NEW.challenger_run_id FOR SHARE;
          SELECT * INTO incumbent_decision FROM public.strategy_decisions
          WHERE agent_paper_cycle_id=NEW.incumbent_cycle_id FOR SHARE;
          SELECT * INTO challenger_decision FROM public.strategy_decisions
          WHERE agent_paper_cycle_id=NEW.challenger_cycle_id FOR SHARE;
          expected_starting_projection_hash := pg_catalog.encode(
            public.digest(
              pg_catalog.convert_to(
                'agent_version_starting_projection_pair_v1|' ||
                incumbent_decision.portfolio_projection_hash || '|' ||
                challenger_decision.portfolio_projection_hash,
                'UTF8'
              ),
              'sha256'
            ),
            'hex'
          );
          {market_assignment}
          IF evaluation.id IS NULL OR incumbent_cycle.id IS NULL OR challenger_cycle.id IS NULL
             OR incumbent_run.id IS NULL OR challenger_run.id IS NULL
             OR incumbent_decision.id IS NULL OR challenger_decision.id IS NULL
             OR EXISTS (
               SELECT 1 FROM public.agent_version_evaluation_decisions decision
               WHERE decision.evaluation_id=NEW.evaluation_id
             )
             OR incumbent_cycle.account_id <> evaluation.incumbent_account_id
             OR challenger_cycle.account_id <> evaluation.challenger_account_id
             OR incumbent_cycle.decision_slot_at <> NEW.decision_slot_at
             OR challenger_cycle.decision_slot_at <> NEW.decision_slot_at
             OR NOT incumbent_cycle.execution_locked OR NOT challenger_cycle.execution_locked
             OR incumbent_cycle.decision_price_policy_id <>
                challenger_cycle.decision_price_policy_id
             OR incumbent_cycle.execution_price_policy_id <>
                challenger_cycle.execution_price_policy_id
             OR incumbent_cycle.sizing_policy_hash <> challenger_cycle.sizing_policy_hash
             OR incumbent_cycle.universe_snapshot_id <> challenger_cycle.universe_snapshot_id
             OR incumbent_cycle.candidate_hash <> challenger_cycle.candidate_hash
             OR incumbent_cycle.candidate_manifest <> challenger_cycle.candidate_manifest
             OR incumbent_run.actor_id <> 'local-agent-version-incumbent'
             OR challenger_run.actor_id <> 'local-agent-version-challenger'
             OR incumbent_run.instrument_id <> evaluation.instrument_id
             OR challenger_run.instrument_id <> evaluation.instrument_id
             OR incumbent_run.prompt_hash <> evaluation.incumbent_prompt_hash
             OR incumbent_run.model_hash <> evaluation.incumbent_model_hash
             OR challenger_run.prompt_hash <> evaluation.challenger_prompt_hash
             OR challenger_run.model_hash <> evaluation.challenger_model_hash
             OR challenger_run.prompt_version_id <> evaluation.challenger_prompt_version_id
             OR challenger_run.model_config_id <> evaluation.challenger_model_config_id
             {market_checks}
             OR incumbent_decision.account_id <> evaluation.incumbent_account_id
             OR challenger_decision.account_id <> evaluation.challenger_account_id
             OR incumbent_decision.calendar_authority_id <> evaluation.calendar_authority_id
             OR challenger_decision.calendar_authority_id <> evaluation.calendar_authority_id
             OR incumbent_decision.source_policy_id <> challenger_decision.source_policy_id
             OR incumbent_decision.execution_source_policy_id IS DISTINCT FROM
                challenger_decision.execution_source_policy_id
             OR NEW.starting_projection_hash IS DISTINCT FROM
                expected_starting_projection_hash
             OR (NEW.phase='train' AND NOT NEW.decision_slot_at=ANY(evaluation.train_slots))
             OR (NEW.phase='oos' AND NOT NEW.decision_slot_at=ANY(evaluation.oos_slots))
             OR NOT EXISTS (
               SELECT 1 FROM public.agent_paper_cycle_events event
               WHERE event.cycle_id=incumbent_cycle.id
                 AND event.fact_ids->'research_run_ids' ? incumbent_run.id::text
             )
             OR NOT EXISTS (
               SELECT 1 FROM public.agent_paper_cycle_events event
               WHERE event.cycle_id=challenger_cycle.id
                 AND event.fact_ids->'research_run_ids' ? challenger_run.id::text
             )
             OR EXISTS (
               SELECT 1 FROM public.broker_execution_intents intent
               JOIN public.strategy_decisions decision
                 ON decision.id=intent.strategy_decision_id
               JOIN public.broker_account_bindings binding
                 ON binding.id=intent.account_binding_id
               WHERE decision.account_id IN (
                 evaluation.incumbent_account_id,evaluation.challenger_account_id
               )
             )
          THEN
            RAISE EXCEPTION 'Agent version cycle pair is invalid' USING ERRCODE='23514';
          END IF;
          RETURN NEW;
        END;
        $$
    """


def _require_empty_pairs() -> None:
    op.execute("LOCK TABLE public.agent_version_cycle_pairs IN ACCESS EXCLUSIVE MODE")
    op.execute(
        """
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM public.agent_version_cycle_pairs) THEN
            RAISE EXCEPTION 'agent version cycle pairs prevent evidence semantic change'
              USING ERRCODE='P3D01';
          END IF;
        END $$
        """
    )


def upgrade() -> None:
    _require_empty_pairs()
    op.execute(_pair_guard_sql(account_neutral=True))


def downgrade() -> None:
    _require_empty_pairs()
    op.execute(_pair_guard_sql(account_neutral=False))
