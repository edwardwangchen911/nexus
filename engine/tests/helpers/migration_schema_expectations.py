"""Expected reflected schema after migration head."""

EXPECTED_TABLES = {
    "provider_runs",
    "provider_capabilities",
    "source_policies",
    "data_snapshots",
    "quality_reports",
    "data_artifacts",
    "price_series_manifests",
    "macro_observations",
    "integration_status",
    "jobs",
    "job_leases",
    "outbox_events",
    "idempotency_records",
    "audit_events",
    "used_nonces",
    "instruments",
    "instrument_aliases",
    "universe_snapshots",
    "universe_members",
    "sector_exposures",
    "latest_quotes",
    "watchlists",
    "watchlist_members",
    "accounts",
    "portfolios",
    "broker_sync_runs",
    "broker_sync_attempts",
    "portfolio_snapshots",
    "cash_snapshots",
    "position_snapshots",
    "broker_order_snapshots",
    "broker_cash_flows",
    "broker_cash_flow_watermarks",
    "broker_sync_requests",
    "account_external_id_envelopes",
    "diagnostic_policies",
    "portfolio_diagnostic_reports",
    "portfolio_diagnostic_items",
    "portfolio_diagnostic_evidence_refs",
    "optimization_policies",
    "cost_policies",
    "baseline_selection_authorities",
    "optimization_admission_authorities",
    "optimization_proposal_groups",
    "optimization_proposals",
    "proposal_positions",
    "proposal_comparisons",
    "research_evidence_refs",
    "filings",
    "financial_facts",
    "corporate_actions",
    "agent_prompt_versions",
    "agent_model_configs",
    "agent_research_runs",
    "agent_research_outputs",
}

OPTIONAL_MIGRATION_TABLE_GROUPS = (
    frozenset(
        {
            "diagnostic_policies",
            "portfolio_diagnostic_reports",
            "portfolio_diagnostic_items",
            "portfolio_diagnostic_evidence_refs",
        }
    ),
    frozenset(
        {
            "optimization_policies",
            "cost_policies",
            "baseline_selection_authorities",
            "optimization_admission_authorities",
            "optimization_proposal_groups",
            "optimization_proposals",
            "proposal_positions",
            "proposal_comparisons",
        }
    ),
    frozenset(
        {
            "research_evidence_refs",
            "filings",
            "financial_facts",
            "corporate_actions",
        }
    ),
    frozenset(
        {
            "agent_prompt_versions",
            "agent_model_configs",
            "agent_research_runs",
            "agent_research_outputs",
        }
    ),
)

EXPECTED_FOREIGN_KEYS = {
    "quality_reports": "fk_quality_reports_data_snapshot_id_data_snapshots",
    "price_series_manifests": "fk_price_series_manifests_data_artifact_id_data_artifacts",
    "job_leases": "fk_job_leases_job_id_jobs",
    "instrument_aliases": "fk_instrument_aliases_instrument_id_instruments",
    "universe_members": {
        "fk_universe_members_instrument_id_instruments",
        "fk_universe_members_universe_snapshot_id_universe_snapshots",
    },
    "sector_exposures": "fk_sector_exposures_instrument_id_instruments",
    "latest_quotes": "fk_latest_quotes_instrument_id_instruments",
    "watchlist_members": {
        "fk_watchlist_members_instrument_id_instruments",
        "fk_watchlist_members_watchlist_id_watchlists",
    },
    "portfolios": "fk_portfolios_account_id_accounts",
    "broker_sync_runs": {
        "fk_broker_sync_runs_account_id_accounts",
        "fk_broker_sync_runs_portfolio_id_portfolios",
    },
    "broker_sync_attempts": "fk_broker_sync_attempts_sync_run_id_runs",
    "portfolio_snapshots": {
        "fk_portfolio_snapshots_portfolio_id_portfolios",
        "fk_portfolio_snapshots_stale_source_same_portfolio",
        "fk_portfolio_snapshots_sync_run_id_runs",
    },
    "cash_snapshots": "fk_cash_snapshots_portfolio_snapshot_id_snapshots",
    "position_snapshots": {
        "fk_position_snapshots_instrument_id_instruments",
        "fk_position_snapshots_portfolio_snapshot_id_snapshots",
    },
    "broker_order_snapshots": {
        "fk_broker_order_snapshots_instrument_id_instruments",
        "fk_broker_order_snapshots_portfolio_snapshot_id_snapshots",
    },
    "broker_cash_flows": {
        "fk_broker_cash_flows_account_id_accounts",
        "fk_broker_cash_flows_sync_run_id_runs",
    },
    "broker_cash_flow_watermarks": "fk_cash_flow_watermarks_account_id_accounts",
    "broker_sync_requests": {
        "fk_broker_sync_requests_sync_run_id_runs",
        "fk_broker_sync_requests_idempotency_record_id_records",
        "fk_broker_sync_requests_account_id_accounts",
    },
    "account_external_id_envelopes": ("fk_account_external_id_envelopes_account_id_accounts"),
    "portfolio_diagnostic_reports": {
        "fk_diag_reports_portfolio",
        "fk_diag_reports_snapshot",
        "fk_diag_reports_snapshot_same_portfolio",
        "fk_diag_reports_policy",
    },
    "portfolio_diagnostic_items": "fk_diag_items_report",
    "portfolio_diagnostic_evidence_refs": "fk_diag_evidence_item",
    "filings": {
        "fk_filings_instrument_id_instruments",
        "fk_filings_evidence_owner",
    },
    "financial_facts": {
        "fk_financial_facts_filing_id_filings",
        "fk_financial_facts_filing_identity",
        "fk_financial_facts_evidence_ref",
        "fk_financial_facts_instrument_id_instruments",
    },
    "corporate_actions": {
        "fk_corporate_actions_evidence_ref",
        "fk_corporate_actions_evidence_owner",
        "fk_corporate_actions_instrument_id_instruments",
    },
    "agent_research_runs": {
        "fk_agent_runs_instrument",
        "fk_agent_runs_data_snapshot",
        "fk_agent_runs_prompt_version",
        "fk_agent_runs_model_config",
    },
    "agent_research_outputs": "fk_agent_outputs_run",
}
