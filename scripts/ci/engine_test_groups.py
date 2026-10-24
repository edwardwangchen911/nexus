"""Run unchanged Engine CI coverage in isolated, serial groups."""

from __future__ import annotations

import argparse
import subprocess
import sys

# The previous merge-gate inventory, in its original order. Add new selectors here.
MERGE_SELECTORS = (
    "engine/tests/unit",
    "engine/tests/contract",
    "engine/tests/evals",
    "engine/tests/property",
    "engine/tests/test_health.py",
    "engine/tests/integration/test_temporary_database_lifecycle.py",
    "engine/tests/integration/test_job_checkpoint.py",
    "engine/tests/integration/test_job_worker.py",
    "engine/tests/integration/test_portfolio_api.py",
    "engine/tests/integration/test_optimization_writer.py",
    "engine/tests/integration/test_formal_optimization_producer.py",
    "engine/tests/integration/test_scenario_spread_cost_authority_pg.py",
    "engine/tests/integration/test_scenario_selection_migration.py",
    "engine/tests/integration/test_fosun_identity_selection.py",
    "engine/tests/integration/test_fosun_instrument_discovery_storage.py",
    "engine/tests/integration/test_fosun_refresh_order_pg.py",
    "engine/tests/integration/test_fosun_security_classification_storage.py",
    "engine/tests/integration/test_fosun_security_classification_migration.py",
    "engine/tests/integration/test_fosun_push_local.py",
    "engine/tests/integration/test_market_database_series_pg.py",
    "engine/tests/integration/test_real_fixture_db_guard.py",
    "engine/tests/integration/test_risk_authority_pg.py",
    "engine/tests/integration/test_paper_idempotency_pg.py",
    "engine/tests/integration/test_paper_db_guards.py",
    "engine/tests/integration/test_broker_simulation_reconciliation.py",
    "engine/tests/integration/test_agent_paper_cycle_migration.py",
    "engine/tests/integration/test_agent_paper_market_migration.py",
    "engine/tests/integration/test_agent_paper_decision_pg.py",
    "engine/tests/integration/test_agent_paper_cycle_pg.py",
    "engine/tests/integration/test_agent_paper_market_pg.py",
    "engine/tests/integration/test_fred_research_context.py",
    "engine/tests/integration/test_fred_generic_snapshot.py",
    "engine/tests/integration/test_agent_paper_cycle_e2e.py",
    "engine/tests/integration/test_partial_portfolio_closeout.py",
    "engine/tests/integration/test_historical_assessment_guards.py",
    "engine/tests/integration/test_portfolio_decision_e2e.py",
    "engine/tests/integration/test_portfolio_decision_admission_pg.py",
    "engine/tests/integration/test_manual_paper_api_pg.py",
    "engine/tests/integration/test_research_request_pg.py",
    "engine/tests/integration/test_research_two_flight_pg.py",
    "engine/tests/integration/test_manual_host_exit_pg.py",
    "engine/tests/integration/test_research_only_runtime.py",
    "engine/tests/integration/test_observation_identity.py",
    "engine/tests/integration/test_observation_identity_migration.py",
    "engine/tests/integration/test_manual_paper_job_binding_pg.py",
    "engine/tests/integration/test_synthesis_usage_acceptance_pg.py",
    "engine/tests/integration/test_synthesis_sdk_lifecycle_pg.py",
    "engine/tests/integration/test_synthesis_failure_facts_pg.py",
    "engine/tests/integration/test_observation_readback_pg.py",
    "engine/tests/integration/test_formal_synthesis_orphan_pg.py",
    "engine/tests/integration/test_formal_research_orphan_pg.py",
    "engine/tests/integration/test_research_orphan_seal_pg.py",
    "engine/tests/integration/test_candidate_multistock_path.py::test_v11_formal_synthesis_sdk_lifecycle",
    "engine/tests/integration/test_current_preparation_recovery_pg.py",
    "engine/tests/integration/test_agent_paper_identity_preparation.py",
    "engine/tests/integration/test_manual_paper_cycle_pg.py",
    "engine/tests/integration/test_manual_partial_history_seal_pg.py::test_explicit_manual_seal_retains_known_lower_bound_and_closes_unused_jobs",
    "engine/tests/integration/test_manual_partial_history_seal_pg.py::test_v11_failed_research_seal_keeps_unknown_budget_and_allows_new_request",
    "engine/tests/integration/test_history_seal_preparation_scope_pg.py",
    "engine/tests/integration/test_manual_partial_history_seal_pg.py::test_identity_diagnostic_preserves_seal_transaction_rollback",
    "engine/tests/integration/test_sec_exhibit_import.py",
    "engine/tests/integration/test_sec_index_pg.py",
    "engine/tests/integration/test_candidate_multistock_path.py::test_v10_manual_entry_to_result",
    "engine/tests/integration/test_candidate_multistock_path.py::test_v10_manual_entry_failure_preserves_recovery_gates",
    "engine/tests/integration/test_candidate_multistock_path.py::test_v11_manual_entry_to_result",
    "engine/tests/integration/test_candidate_multistock_path.py::test_v11_manual_entry_failure_preserves_recovery_gates",
    "engine/tests/integration/test_candidate_multistock_path.py::test_v11_delayed_research_observation_keeps_session_boundary",
    "engine/tests/integration/test_capacity_rejection_pg.py",
    "engine/tests/integration/test_unknown_preparation_ack_pg.py",
    "engine/tests/integration/test_scheduled_unknown_acceptance_pg.py",
    "engine/tests/integration/test_formal_v10_runtime_admission.py",
    "engine/tests/integration/test_formal_v11_output_migration.py",
    "engine/tests/integration/test_formal_stage_registration.py",
    "engine/tests/integration/test_full_input_capacity_cycle.py::test_formal_v10_research_deadline_preserves_unknown_usage_and_closes_cycle",
    "engine/tests/integration/test_full_input_capacity_cycle.py::test_formal_v11_complete_pool_through_sdk",
    "engine/tests/integration/test_full_input_capacity_cycle.py::test_v11_preclaim_expiry_leaves_no_claim_attempt_output_or_sdk",
    "engine/tests/integration/test_full_input_capacity_cycle.py::test_formal_v11_deadline_keeps_late_result_unknown_and_closes_cycle",
    "engine/tests/integration/test_formal_resume_observation_clock.py",
    "engine/tests/integration/test_accounting_review_intake.py",
    "engine/tests/integration/test_accounting_adoption.py",
    "engine/tests/integration/test_workbench_activity_pg.py",
    "engine/tests/integration/test_workbench_activity_boundaries_pg.py",
    "engine/tests/integration/test_workbench_research_summary_pg.py",
    "engine/tests/integration/test_workbench_read_service.py",
    "engine/tests/integration/test_formal_history_seal_pg.py",
    "engine/tests/integration/test_research_monitoring_pg.py",
    "engine/tests/integration/test_research_monitoring_migration.py",
    "engine/tests/integration/test_monitoring_producer_pg.py",
    "engine/tests/integration/test_monitor_increment_pg.py",
    "engine/tests/integration/test_monitor_pending_pg.py",
    "engine/tests/integration/test_monitor_daily_pending_pg.py",
    "engine/tests/integration/test_monitor_view_pg.py",
    "engine/tests/integration/test_monitor_kinds_pg.py",
    "engine/tests/integration/test_disclosure_context_pg.py",
    "engine/tests/integration/test_disclosure_producer_pg.py",
    "engine/tests/integration/test_disclosure_observations_pg.py",
    "engine/tests/integration/test_monitor_batches_pg.py",
    "engine/tests/integration/test_monitor_filing_content_pg.py",
    "engine/tests/integration/test_monitor_content_collector_pg.py",
    "engine/tests/integration/test_monitor_mixed_db_pg.py",
    "engine/tests/integration/test_monitor_event_receipts_pg.py",
    "engine/tests/integration/test_monitor_mixed_increment_pg.py",
    "engine/tests/integration/test_monitor_daily_receipts_pg.py",
    "engine/tests/integration/test_monitor_frozen_members_pg.py",
    "engine/tests/integration/test_monitor_dispatch_pg.py",
    "engine/tests/integration/test_monitor_preclaim_pg.py",
    "engine/tests/integration/test_monitor_source_qualification_pg.py",
    "engine/tests/integration/test_monitor_reconciliation_pg.py",
    "engine/tests/integration/test_monitor_plan_replacement_pg.py",
    "engine/tests/integration/test_research_monitor_notification_pg.py",
)

# CI progress timestamps place this boundary near half of PostgreSQL wall time.
PG_SPLIT = (
    "engine/tests/integration/test_candidate_multistock_path.py"
    "::test_v11_formal_synthesis_sdk_lifecycle"
)


def selectors_for(group: str) -> tuple[str, ...]:
    split = MERGE_SELECTORS.index(PG_SPLIT)
    return {
        "fast": MERGE_SELECTORS[:5],
        "pg-a": MERGE_SELECTORS[5:split],
        "pg-b": MERGE_SELECTORS[split:],
        "full": ("engine/tests",),
    }[group]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("group", choices=("fast", "pg-a", "pg-b", "full"))
    parser.add_argument("--collect-only", action="store_true")
    args = parser.parse_args()
    command = [sys.executable, "-m", "pytest", "-q", "--import-mode=importlib"]
    if args.group != "full":
        command += ["-m", "not performance"]
    command += ["--durations=30", "--durations-min=1"]
    if args.collect_only:
        command.append("--collect-only")
    else:
        command.append(f"--junitxml=test-results/engine-{args.group}.xml")
    command.extend(selectors_for(args.group))
    return subprocess.run(command, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
