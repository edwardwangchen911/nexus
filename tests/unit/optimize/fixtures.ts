import type { components } from "../../../app/lib/generated/schema";

export const portfolioId = "0198d500-1000-7000-8000-000000001301";
export const snapshotId = "0198d500-1000-7000-8000-000000001302";
export const groupId = "0198d500-1000-7000-8000-000000001303";
export const proposalIds = {
  hold_current: "0198d500-1000-7000-8000-000000001304",
  minimum_change: "0198d500-1000-7000-8000-000000001305",
  strategy_target: "0198d500-1000-7000-8000-000000001306",
} as const;
export const instrumentIds = {
  aapl: "0198d500-1000-7000-8000-000000001307",
  spy: "0198d500-1000-7000-8000-000000001308",
} as const;

type DiagnosticReport = components["schemas"]["PortfolioDiagnosticResponse"];
type ProposalGroup = components["schemas"]["OptimizationProposalGroupResponse"];
type Overview = components["schemas"]["OverviewResponse"];

const evidence = {
  source: "nexus_db",
  object_id: "diagnostic:fixture",
  available_at: "2026-08-24T00:00:00Z",
  location: "db://diagnostics/fixture",
  content_hash: "a".repeat(64),
};

export function diagnosticReport(
  optimizationNeed: DiagnosticReport["optimization_need"] = "OPTIMIZATION_CANDIDATE",
): DiagnosticReport {
  return {
    portfolio_snapshot_id: snapshotId,
    snapshot_as_of: "2026-08-24T00:00:00Z",
    trading_day: "2026-08-24",
    policy_version: "real_advisory_policy_v1",
    optimization_need: optimizationNeed,
    items: [
      {
        diagnostic_code: "CONCENTRATION_TOP1",
        metric_value: "0.20",
        unit: "ratio",
        reference_limit: "0.15",
        headroom: "-0.05",
        headroom_reason: null,
        policy_scope: "portfolio",
        severity: "warning",
        actionability: "reduce_only",
        unknown_reason: null,
        evidence_refs: [evidence],
      },
      {
        diagnostic_code: "LIQUIDITY_DAYS_TO_EXIT",
        metric_value: null,
        unit: "trading_days",
        reference_limit: "2",
        headroom: null,
        headroom_reason: "ADV20_MISSING",
        policy_scope: "portfolio",
        severity: "warning",
        actionability: "research",
        unknown_reason: "ADV20_MISSING",
        evidence_refs: [evidence],
      },
      {
        diagnostic_code: "DRAWDOWN_252",
        metric_value: null,
        unit: "ratio",
        reference_limit: "0.08",
        headroom: null,
        headroom_reason: "DRAWDOWN_252_COVERAGE_INSUFFICIENT",
        policy_scope: "portfolio",
        severity: "info",
        actionability: "research",
        unknown_reason: "DRAWDOWN_252_COVERAGE_INSUFFICIENT",
        evidence_refs: [evidence],
      },
      {
        diagnostic_code: "SOURCE_AUTHORITY",
        metric_value: null,
        unit: "state",
        reference_limit: null,
        headroom: null,
        headroom_reason: "SOURCE_CONFLICT",
        policy_scope: "portfolio",
        severity: "critical",
        actionability: "blocked",
        unknown_reason: "SOURCE_CONFLICT",
        evidence_refs: [evidence],
      },
    ],
  } as DiagnosticReport;
}

function known(value: string) {
  return { status: "known" as const, value, reason: null };
}

function unknown(reason: string) {
  return { status: "unknown" as const, value: null, reason };
}

function proposal(
  variant: keyof typeof proposalIds,
  options: { analysisOnly?: boolean; evidenceComplete?: boolean; unknownCost?: boolean } = {},
) {
  const targetWeight = variant === "hold_current" ? "0.20" : variant === "minimum_change" ? "0.15" : "0.125";
  const unknownCost = options.unknownCost ?? variant === "strategy_target";
  return {
    proposal_id: proposalIds[variant],
    variant,
    analysis_only: options.analysisOnly ?? true,
    strategy_evidence_complete: options.evidenceComplete ?? variant !== "strategy_target",
    portfolio_scope: "real_read_only",
    positions: [
      {
        instrument_id: instrumentIds.aapl,
        current_weight: "0.20",
        target_weight: targetWeight,
        action: targetWeight === "0.20" ? "maintain" : "reduce",
        reason_code: "CONCENTRATION_TOP1",
      },
      {
        instrument_id: instrumentIds.spy,
        current_weight: "0.10",
        target_weight: "0.10",
        action: "maintain",
        reason_code: "BASELINE_HOLDING",
      },
    ],
    comparison: {
      metrics: {
        authority: {
          diagnostic_report_id: "0198d500-1000-7000-8000-000000001312",
          data_snapshot_id: "0198d500-1000-7000-8000-000000001309",
          source_policy_id: "0198d500-1000-7000-8000-000000001310",
          cost_policy_id: "0198d500-1000-7000-8000-000000001311",
          optimizer_policy_id: "0198d500-1000-7000-8000-000000001313",
        },
        unknown_metrics: unknownCost ? ["cost"] : [],
        constraint_complete: true,
        eligibility_complete: true,
        evidence_complete: !unknownCost,
        stale: false,
        ranking_eligible: !unknownCost,
        dimensions: {
          hard_violations: variant === "hold_current" ? 1 : 0,
          top1: known(targetWeight),
          top3: known(variant === "hold_current" ? "0.30" : "0.25"),
          hhi: known("0.52"),
          sector_exposure: known("0.25"),
          currency_exposure: known("0.30"),
          volatility: unknown("VOLATILITY_POST_PROPOSAL_NOT_PROVEN"),
          drawdown: unknown("DRAWDOWN_POST_PROPOSAL_NOT_PROVEN"),
          correlation: unknown("CORRELATION_POST_PROPOSAL_NOT_PROVEN"),
          liquidity: known("0.90"),
          turnover: known(variant === "hold_current" ? "0" : "0.05"),
          cost: unknownCost ? unknown("COST_NOT_PROVEN") : known("0.001"),
          tax_impact: unknown("TAX_DATA_UNAVAILABLE"),
        },
      },
      tax_impact: "unknown",
      requires_fx: false,
      non_executable: unknownCost,
      cost_complete: !unknownCost,
    },
  };
}

export function proposalGroup(
  overrides: Partial<ProposalGroup> & { status?: ProposalGroup["status"] } = {},
): ProposalGroup {
  return {
    proposal_group_id: groupId,
    status: "compared",
    version: 4,
    baseline_portfolio_snapshot_id: snapshotId,
    portfolio_scope: "real_read_only",
    diagnostic_input_hash: "b".repeat(64),
    data_snapshot_id: "0198d500-1000-7000-8000-000000001309",
    source_policy_id: "0198d500-1000-7000-8000-000000001310",
    cost_policy_id: "0198d500-1000-7000-8000-000000001311",
    proposals: [
      proposal("hold_current", { analysisOnly: true, evidenceComplete: true }),
      proposal("minimum_change", { analysisOnly: true, evidenceComplete: true }),
      proposal("strategy_target", { analysisOnly: true, evidenceComplete: false, unknownCost: true }),
    ],
    shadow_eligibility: {
      eligible: false,
      reasons: ["STRATEGY_EVIDENCE_INCOMPLETE", "COST_NOT_PROVEN"],
    },
    ...overrides,
  } as ProposalGroup;
}

export function overviewData(overrides: Partial<Overview> = {}): Overview {
  return {
    portfolio_id: portfolioId,
    portfolio_snapshot_id: snapshotId,
    snapshot_as_of: "2026-08-24T00:00:00Z",
    trading_day: "2026-08-24",
    diagnostic_report: diagnosticReport(),
    proposal_group: { proposal_group_id: groupId, status: "compared", version: 4 },
    proposal_admission: {
      portfolio_snapshot_id: snapshotId,
      data_snapshot_id: "0198d500-1000-7000-8000-000000001309",
      cost_policy_id: "0198d500-1000-7000-8000-000000001311",
    },
    proposal_admission_reason: null,
    ...overrides,
  } as Overview;
}
