import "server-only";

import { v7 as uuidv7, validate as validateUuid, version as uuidVersion } from "uuid";
import {
  integrationStatusRuntimeSchema,
  brokerSyncRunRuntimeSchema,
  brokerSimulationRuntimeSchema,
  brokerIntentRuntimeSchema,
  brokerActionRuntimeSchema,
  dailyBriefRuntimeSchema,
  marketPageRuntimeSchema,
  newsRuntimeSchema,
  optimizationJobRuntimeSchema,
  optimizationLifecycleRuntimeSchema,
  optimizationProposalGroupRuntimeSchema,
  overviewRuntimeSchema,
  paperCancelRuntimeSchema,
  paperOrderPageRuntimeSchema,
  paperPortfolioRuntimeSchema,
  portfolioPageRuntimeSchema,
  portfolioDiagnosticRuntimeSchema,
  portfolioDecisionRunRuntimeSchema,
  portfolioScenarioRuntimeSchema,
  portfolioSnapshotRuntimeSchema,
  readinessRuntimeSchema,
  researchAdmissionRuntimeSchema,
  researchRunRuntimeSchema,
  riskAuditPageRuntimeSchema,
  runtimeContractSchemas,
  strategySummaryRuntimeSchema,
  strategyEvolutionRuntimeSchema,
  experimentDetailRuntimeSchema,
  evolutionActionRuntimeSchema,
  shadowGroupRuntimeSchema,
  shadowStartRuntimeSchema,
  watchlistMembershipRuntimeSchema,
  whatIfRuntimeSchema,
  jobRetryRuntimeSchema,
  drawdownAcknowledgeRuntimeSchema,
} from "../generated/runtime-contract";
import { integrationProviders } from "../integrations/providers";
import { sameUuid } from "../optimization/uuid";

export type NexusErrorCode = "UNAUTHENTICATED" | "FORBIDDEN" | "VALIDATION_ERROR" | "NOT_FOUND" | "VERSION_CONFLICT" | "IDEMPOTENCY_CONFLICT" | "PROVIDER_UNAVAILABLE" | "EVOLUTION_BLOCKED" | "AUTHORITY_UNAVAILABLE" | "SIMULATION_WRITE_DISABLED" | "SCENARIO_BASELINE_NOT_READY" | "SCENARIO_DIAGNOSTIC_MISMATCH" | "SCENARIO_AS_OF_MISMATCH" | "SCENARIO_OBJECTIVE_UNSUPPORTED" | "SCENARIO_INPUT_INVALID" | "SCENARIO_US_EQUITY_SCOPE_INVALID" | "PORTFOLIO_DECISION_INPUT_UNSAFE" | "PORTFOLIO_DECISION_AUTHORITY_MISMATCH" | "PORTFOLIO_DECISION_BUDGET_EXHAUSTED" | "INTERNAL_ERROR";
export const ERROR_STATUSES = {
  UNAUTHENTICATED: [401], FORBIDDEN: [403], VALIDATION_ERROR: [400, 422, 429], NOT_FOUND: [404], VERSION_CONFLICT: [409], IDEMPOTENCY_CONFLICT: [409], DATA_PARTIAL: [409], DATA_STALE: [409], BROKER_STALE: [409], SOURCE_CONFLICT: [409], CAPABILITY_EXPIRED: [409], PROPOSAL_NOT_EXECUTABLE: [409], ACCOUNT_TYPE_MISMATCH: [422], APPROVAL_REQUIRED: [409], RISK_REJECTED: [422], DRAWDOWN_LOCKED: [409], RECONCILIATION_REQUIRED: [409], SIMULATION_WRITE_DISABLED: [409], BUDGET_EXHAUSTED: [409, 429], EVOLUTION_BLOCKED: [422], AUTHORITY_UNAVAILABLE: [503], PROVIDER_UNAVAILABLE: [503], SCENARIO_BASELINE_NOT_READY: [409], SCENARIO_DIAGNOSTIC_MISMATCH: [409], SCENARIO_AS_OF_MISMATCH: [409], SCENARIO_OBJECTIVE_UNSUPPORTED: [422], SCENARIO_INPUT_INVALID: [409], SCENARIO_US_EQUITY_SCOPE_INVALID: [409], PORTFOLIO_DECISION_INPUT_UNSAFE: [422], PORTFOLIO_DECISION_AUTHORITY_MISMATCH: [409], PORTFOLIO_DECISION_BUDGET_EXHAUSTED: [409], INTERNAL_ERROR: [500],
} as const;
export type StableEngineErrorCode = keyof typeof ERROR_STATUSES;
const DRAFT_FX_MESSAGE = "当前快照含非美元现金，缺少汇率估值，暂不能生成可比草稿。";
export const SAFE_ENGINE_ERROR_MESSAGES: Record<StableEngineErrorCode, string> = {
  UNAUTHENTICATED: "Authentication is required.",
  FORBIDDEN: "Access is not permitted.",
  VALIDATION_ERROR: "The request is invalid.",
  NOT_FOUND: "The requested resource was not found.",
  VERSION_CONFLICT: "The resource changed. Refresh and try again.",
  IDEMPOTENCY_CONFLICT: "The idempotency key was already used for a different request.",
  DATA_PARTIAL: "Required data is incomplete.",
  DATA_STALE: "Required data is stale.",
  BROKER_STALE: "Broker data is stale.",
  SOURCE_CONFLICT: "Data sources disagree.",
  CAPABILITY_EXPIRED: "The provider capability is unavailable or expired.",
  PROPOSAL_NOT_EXECUTABLE: "The proposal is not executable.",
  ACCOUNT_TYPE_MISMATCH: "The account type is not permitted for this action.",
  APPROVAL_REQUIRED: "Human approval is required.",
  RISK_REJECTED: "The action was rejected by deterministic risk controls.",
  DRAWDOWN_LOCKED: "The drawdown lock is active.",
  RECONCILIATION_REQUIRED: "Reconciliation is required before continuing.",
  SIMULATION_WRITE_DISABLED: "External broker simulation writes are disabled.",
  BUDGET_EXHAUSTED: "The configured budget is exhausted.",
  EVOLUTION_BLOCKED: "The strategy evolution action is blocked.",
  AUTHORITY_UNAVAILABLE: "The strategy evolution authority is unavailable.",
  PROVIDER_UNAVAILABLE: "The provider is unavailable.",
  SCENARIO_BASELINE_NOT_READY: "The frozen portfolio baseline is not ready for scenario analysis.",
  SCENARIO_DIAGNOSTIC_MISMATCH: "The diagnostic report does not match the portfolio snapshot.",
  SCENARIO_AS_OF_MISMATCH: "The valuation time does not match the frozen evidence.",
  SCENARIO_OBJECTIVE_UNSUPPORTED: "The requested scenario objective is not supported.",
  SCENARIO_INPUT_INVALID: "The frozen portfolio inputs are invalid for scenario analysis.",
  SCENARIO_US_EQUITY_SCOPE_INVALID: "The USD equity scope cannot be established from frozen identities.",
  PORTFOLIO_DECISION_INPUT_UNSAFE: "The portfolio decision request is unsafe.",
  PORTFOLIO_DECISION_AUTHORITY_MISMATCH: "The portfolio decision authority does not match the request.",
  PORTFOLIO_DECISION_BUDGET_EXHAUSTED: "The portfolio decision budget is exhausted.",
  INTERNAL_ERROR: "The request could not be completed.",
};

export class NexusBffError extends Error {
  constructor(readonly status: number, readonly code: NexusErrorCode, message: string) { super(message); }
}

export function errorResponse(status: number, code: NexusErrorCode, message: string): Response {
  return Response.json({ error: { code, message, request_id: uuidv7(), details: {} } }, { status });
}

export function safeBffErrorResponse(error: unknown): Response {
  if (error instanceof NexusBffError) return errorResponse(error.status, error.code, error.message);
  return errorResponse(500, "INTERNAL_ERROR", "The request could not be completed.");
}

const SAFE_AUDIT_OUTCOME_FIELDS = new Set([
  "completeness",
  "error_code",
  "outcome",
  "quality_status",
  "reason_code",
  "result",
  "retryable",
  "status",
]);

export function isAllowedAuditExportEvent(value: unknown): boolean {
  if (!matchesRuntimeSchema(value, runtimeContractSchemas.RiskAuditEventResponse)
    || !isPlain(value) || !isPlain(value.outcome)) return false;
  return Object.keys(value.outcome).every((key) => SAFE_AUDIT_OUTCOME_FIELDS.has(key));
}

export function rebuildAllowedEngineResponse(status: number, value: unknown, upstreamHeaders = new Headers(), requestUrl?: string, schemaPath?: string, requestMethod?: string): Response | null {
  const pathname = enginePathname(requestUrl, schemaPath);
  const readinessStatus = status === 503 && ["/health/ready", "/internal/v1/readiness"].includes(pathname);
  if (([200, 201, 202].includes(status) || readinessStatus) && isObject(value, ["data", "meta"])) {
    const { data, meta } = value;
    const safeData = allowedSuccessData(data, requestUrl, schemaPath, requestMethod);
    if (safeData !== INVALID_DATA && isObject(meta, ["request_id", "schema_version", "as_of"])
      && isV7(meta.request_id) && meta.schema_version === "1" && isUtcIso(meta.as_of)
      && responseIdentityValid(safeData, meta, requestUrl)) {
      return Response.json({ data: safeData, meta }, { status, headers: safeHeaders(upstreamHeaders, schemaPath ? undefined : requestUrl) });
    }
  }
  if ([400, 401, 403, 404, 409, 422, 429, 500, 502, 503, 504].includes(status) && isObject(value, ["error"])) {
    const { error } = value;
    if (isObject(error, ["code", "message", "request_id", "details"]) && isStableCode(error.code) && allowsStatus(error.code, status) && typeof error.message === "string" && isV7(error.request_id) && isPlain(error.details)) {
      return Response.json({
        error: {
          code: error.code,
          message: pathname === "/internal/v1/optimization-proposal-drafts"
            && error.code === "DATA_PARTIAL"
            && (error.details.reason === "DRAFT_FX_VALUATION_UNAVAILABLE"
              || error.message === DRAFT_FX_MESSAGE)
            ? DRAFT_FX_MESSAGE
            : SAFE_ENGINE_ERROR_MESSAGES[error.code],
          request_id: error.request_id,
          details: {},
        },
      }, { status, headers: safeHeaders(upstreamHeaders, schemaPath ? undefined : requestUrl) });
    }
  }
  return null;
}

const INVALID_DATA = Symbol("invalid engine data");

function allowedSuccessData(data: unknown, requestUrl?: string, schemaPath?: string, requestMethod?: string): unknown | typeof INVALID_DATA {
  const pathname = enginePathname(requestUrl, schemaPath);
  if (["/health/ready", "/internal/v1/readiness"].includes(pathname)) {
    return requestMethod?.toUpperCase() === "GET"
      && matchesRuntimeSchema(data, readinessRuntimeSchema) && isPlain(data)
      ? data
      : INVALID_DATA;
  }
  if (pathname === "/internal/v1/risk-audit") {
    if (requestMethod?.toUpperCase() !== "GET"
      || !matchesRuntimeSchema(data, riskAuditPageRuntimeSchema) || !isPlain(data)
      || !Array.isArray(data.items) || data.items.length > 100
      || data.items.some((item) => !isAllowedAuditExportEvent(item))
      || !isPlain(data.summary) || !isPlain(data.summary.failed_jobs)
      || !Array.isArray(data.summary.failed_jobs.retryable)
      || data.summary.failed_jobs.retryable.length > 20
      || data.next_cursor !== null && (typeof data.next_cursor !== "string" || data.next_cursor.length > 2048)) {
      return INVALID_DATA;
    }
    return data;
  }
  const jobRetry = /^\/internal\/v1\/jobs\/([^/]+)\/retry$/.exec(pathname);
  if (jobRetry && validateUuid(jobRetry[1])) {
    return requestMethod?.toUpperCase() === "POST"
      && matchesRuntimeSchema(data, jobRetryRuntimeSchema) && isPlain(data)
      && sameUuid(data.job_id, jobRetry[1])
      ? data
      : INVALID_DATA;
  }
  if (pathname === "/internal/v1/risk/drawdown-lock/acknowledge") {
    return requestMethod?.toUpperCase() === "POST"
      && matchesRuntimeSchema(data, drawdownAcknowledgeRuntimeSchema) && isPlain(data)
      ? data
      : INVALID_DATA;
  }
  if (pathname === "/internal/v1/broker-simulation") {
    return requestMethod?.toUpperCase() === "GET"
      && matchesRuntimeSchema(data, brokerSimulationRuntimeSchema) && isPlain(data)
      && brokerSimulationValid(data)
      ? data
      : INVALID_DATA;
  }
  if (pathname === "/internal/v1/broker-simulation/intents") {
    return requestMethod?.toUpperCase() === "POST"
      && matchesRuntimeSchema(data, brokerIntentRuntimeSchema) && isPlain(data)
      && brokerIntentValid(data)
      ? data
      : INVALID_DATA;
  }
  const brokerAction = brokerActionFor(pathname, requestMethod);
  if (brokerAction !== null) {
    return matchesRuntimeSchema(data, brokerActionRuntimeSchema) && isPlain(data)
      && data.action === brokerAction.action
      && sameUuid(data.resource_id, brokerAction.resourceId)
      && data.dispatch_created === false
      ? data
      : INVALID_DATA;
  }
  if (pathname === "/internal/v1/system-status") {
    return isObject(data, ["status"]) && data.status === "ok" ? { status: "ok" } : INVALID_DATA;
  }
  if (pathname === "/internal/v1/integrations") {
    if (!Array.isArray(data)) return INVALID_DATA;
    const statuses = data.map((status) => safeIntegrationStatus(status));
    return statuses.every((status) => status !== null) ? statuses : INVALID_DATA;
  }
  const integrationAction = /^\/internal\/v1\/integrations\/([^/]+)\/(test|mode)$/.exec(pathname);
  if (integrationAction && integrationProviders.includes(integrationAction[1] as (typeof integrationProviders)[number])) {
    const status = safeIntegrationStatus(data);
    return status?.provider === integrationAction[1] ? status : INVALID_DATA;
  }
  const news = /^\/internal\/v1\/instruments\/([^/]+)\/news$/.exec(pathname);
  if (news) {
    return matchesRuntimeSchema(data, newsRuntimeSchema) && isPlain(data)
      && sameUuid(data.instrument_id, news[1]) ? data : INVALID_DATA;
  }
  if (pathname === "/internal/v1/market") {
    if (!matchesRuntimeSchema(data, marketPageRuntimeSchema) || !isPlain(data)) return INVALID_DATA;
    if (!Array.isArray(data.items) || data.items.length > 100) return INVALID_DATA;
    if (data.next_cursor !== null && (typeof data.next_cursor !== "string" || data.next_cursor.length > 2048)) return INVALID_DATA;
    const selected = data.selected_series;
    if (selected !== null && (!isPlain(selected) || !Array.isArray(selected.bars) || selected.bars.length > 500)) return INVALID_DATA;
    return data;
  }
  if (pathname === "/internal/v1/portfolios") {
    if (!matchesRuntimeSchema(data, portfolioPageRuntimeSchema) || !isPlain(data)) return INVALID_DATA;
    return Array.isArray(data.items) && data.items.length <= 50 ? data : INVALID_DATA;
  }
  if (pathname === "/internal/v1/overview") {
    if (!matchesRuntimeSchema(data, overviewRuntimeSchema) || !isPlain(data)) return INVALID_DATA;
    if (data.overview_mode === "PORTFOLIO_FACTS_ONLY") {
      if (!isPlain(data.portfolio_snapshot)
        || data.portfolio_snapshot.portfolio_id !== data.portfolio_id
        || data.portfolio_snapshot.portfolio_snapshot_id !== data.portfolio_snapshot_id
        || data.portfolio_snapshot.as_of !== data.snapshot_as_of
        || data.provider !== "fosun"
        || data.diagnostic_report !== null
        || data.diagnostic_unavailable_reason !== (data.portfolio_snapshot.status === "READY" ? "DIAGNOSTIC_NOT_AVAILABLE" : "SNAPSHOT_NOT_READY")
        || data.proposal_group !== null
        || data.proposal_admission !== null
        || data.proposal_admission_reason !== null) return INVALID_DATA;
      return data;
    }
    if (data.overview_mode !== "DIAGNOSTIC" || !isPlain(data.diagnostic_report) || data.diagnostic_unavailable_reason !== null) return INVALID_DATA;
    if (data.portfolio_snapshot !== null || data.portfolio_snapshot_id !== data.diagnostic_report.portfolio_snapshot_id) return INVALID_DATA;
    if (data.proposal_admission !== null && (!isPlain(data.proposal_admission) || data.proposal_admission.portfolio_snapshot_id !== data.portfolio_snapshot_id)) return INVALID_DATA;
    return data;
  }
  if (pathname === "/internal/v1/paper/portfolio") {
    if (!matchesRuntimeSchema(data, paperPortfolioRuntimeSchema) || !isPlain(data)
      || !Array.isArray(data.positions) || data.positions.length > 500
      || !Array.isArray(data.attributions) || data.attributions.length > 500
      || !paperPortfolioValid(data)) return INVALID_DATA;
    return data;
  }
  if (pathname === "/internal/v1/paper/orders") {
    if (requestMethod?.toUpperCase() !== "GET"
      || !matchesRuntimeSchema(data, paperOrderPageRuntimeSchema) || !isPlain(data)
      || !Array.isArray(data.items) || data.items.length > 50
      || data.items.some((order) => !paperOrderValid(order))
      || data.next_cursor !== null && (typeof data.next_cursor !== "string" || data.next_cursor.length > 2048)) return INVALID_DATA;
    return data;
  }
  const paperCancel = /^\/internal\/v1\/paper\/orders\/([^/]+)\/cancel$/.exec(pathname);
  if (paperCancel && validateUuid(paperCancel[1])) {
    return requestMethod?.toUpperCase() === "POST"
      && matchesRuntimeSchema(data, paperCancelRuntimeSchema) && isPlain(data)
      && sameUuid(data.order_id, paperCancel[1])
      ? data
      : INVALID_DATA;
  }
  const portfolioDiagnostic = /^\/internal\/v1\/portfolios\/([^/]+)\/diagnostics\/latest$/.exec(pathname);
  if (portfolioDiagnostic && validateUuid(portfolioDiagnostic[1])) {
    return matchesRuntimeSchema(data, portfolioDiagnosticRuntimeSchema) && isPlain(data)
      ? data
      : INVALID_DATA;
  }
  const portfolioSnapshot = /^\/internal\/v1\/portfolios\/([^/]+)\/snapshots\/latest$/.exec(pathname);
  if (portfolioSnapshot && validateUuid(portfolioSnapshot[1])) {
    return matchesRuntimeSchema(data, portfolioSnapshotRuntimeSchema) && isPlain(data)
      ? data
      : INVALID_DATA;
  }
  const brokerSyncRun = /^\/internal\/v1\/broker-sync-runs(?:\/([^/]+))?$/.exec(pathname);
  if (brokerSyncRun && (!brokerSyncRun[1] || validateUuid(brokerSyncRun[1]))) {
    return matchesRuntimeSchema(data, brokerSyncRunRuntimeSchema) && isPlain(data)
      ? data
      : INVALID_DATA;
  }
  const watchlistAction = /^\/internal\/v1\/watchlist\/([^/]+)$/.exec(pathname);
  if (watchlistAction && validateUuid(watchlistAction[1])) {
    return matchesRuntimeSchema(data, watchlistMembershipRuntimeSchema) && isPlain(data)
      ? data
      : INVALID_DATA;
  }
  if (pathname === "/internal/v1/optimization-proposals") {
    return matchesRuntimeSchema(data, optimizationJobRuntimeSchema) && isPlain(data)
      ? data
      : INVALID_DATA;
  }
  const proposalGroup = /^\/internal\/v1\/optimization-proposals\/([^/]+)$/.exec(pathname);
  const draftCreation = pathname === "/internal/v1/optimization-proposal-drafts";
  if (draftCreation || proposalGroup && validateUuid(proposalGroup[1])) {
    if (!matchesRuntimeSchema(data, optimizationProposalGroupRuntimeSchema) || !isPlain(data) || !Array.isArray(data.proposals)) return INVALID_DATA;
    const variants = data.proposals.map((proposal) => isPlain(proposal) ? proposal.variant : null);
    const ids = data.proposals.map((proposal) => isPlain(proposal) ? proposal.proposal_id : null);
    const economic = data.economic_comparison;
    const economicValid = economic == null || isPlain(economic) && (
      economic.status === "compared"
        ? economic.recommendation !== null
        : economic.recommendation === null && Array.isArray(economic.reasons)
          && economic.reasons.length > 0
    );
    const bound = draftCreation
      ? data.portfolio_scope === "real_read_only" && data.proposals.every((proposal) =>
        isPlain(proposal) && proposal.analysis_only === true && isPlain(proposal.comparison)
        && proposal.comparison.non_executable === true && isPlain(proposal.comparison.metrics)
        && proposal.comparison.metrics.ranking_eligible === false)
      : proposalGroup !== null && sameUuid(data.proposal_group_id, proposalGroup[1]);
    return bound && economicValid && validDraftResearchContext(data)
      && variants.join(":") === "hold_current:minimum_change:strategy_target"
      && ids.every((id) => typeof id === "string") && new Set(ids).size === 3
      ? data
      : INVALID_DATA;
  }
  const proposalLifecycle = /^\/internal\/v1\/optimization-proposals\/([^/]+)\/(shadow|reject)$/.exec(pathname);
  if (proposalLifecycle && validateUuid(proposalLifecycle[1])) {
    const method = requestMethod?.toUpperCase();
    if (proposalLifecycle[2] === "shadow" && method === "POST") {
      return shadowStartValid(data, proposalLifecycle[1]) ? data : INVALID_DATA;
    }
    if (proposalLifecycle[2] === "shadow" && method === "GET") {
      return shadowGroupValid(data, proposalLifecycle[1]) ? data : INVALID_DATA;
    }
    if (proposalLifecycle[2] === "reject" && method === "POST") {
      return matchesRuntimeSchema(data, optimizationLifecycleRuntimeSchema) && isPlain(data)
        && sameUuid(data.proposal_group_id, proposalLifecycle[1])
        ? data
        : INVALID_DATA;
    }
    return INVALID_DATA;
  }
  if (pathname === "/internal/v1/what-if/evaluate") {
    return matchesRuntimeSchema(data, whatIfRuntimeSchema) && isPlain(data)
      ? data
      : INVALID_DATA;
  }
  if (pathname === "/internal/v1/portfolio-scenarios/evaluate") {
    return matchesRuntimeSchema(data, portfolioScenarioRuntimeSchema) && isPlain(data)
      && data.execution_locked === true
      && data.analysis_status === "partial"
      && data.analysis_comparable === true
      && data.execution_eligible === false
      && data.valuation_scope === "currency:USD"
      && scenarioSemanticsValid(data)
      ? data
      : INVALID_DATA;
  }
  if (pathname === "/internal/v1/portfolio-decision-runs/latest") {
    return requestMethod?.toUpperCase() === "GET"
      && matchesRuntimeSchema(data, portfolioDecisionRunRuntimeSchema)
      && isPlain(data)
      && data.execution_locked === true
      && portfolioDecisionStateValid(data)
      ? data
      : INVALID_DATA;
  }
  const portfolioDecision = /^\/(?:api|internal)\/v1\/portfolio-decision-runs(?:\/([^/]+))?$/.exec(pathname);
  if (portfolioDecision && (!portfolioDecision[1] || validateUuid(portfolioDecision[1]))) {
    const method = requestMethod?.toUpperCase();
    if ((portfolioDecision[1] === undefined && method !== "POST")
      || (portfolioDecision[1] !== undefined && method !== "GET")
      || !matchesRuntimeSchema(data, portfolioDecisionRunRuntimeSchema)
      || !isPlain(data)
      || portfolioDecision[1] !== undefined && !sameUuid(data.run_id, portfolioDecision[1])
      || data.execution_locked !== true
      || !portfolioDecisionStateValid(data)) {
      return INVALID_DATA;
    }
    return data;
  }
  if (pathname === "/internal/v1/research-runs") {
    return matchesRuntimeSchema(data, researchRunRuntimeSchema) && isPlain(data) && researchAuthorityValid(data)
      ? data
      : INVALID_DATA;
  }
  if (pathname === "/internal/v1/strategies") {
    if (!Array.isArray(data) || data.length > 100) return INVALID_DATA;
    const strategies = data.filter((strategy) =>
      matchesRuntimeSchema(strategy, strategySummaryRuntimeSchema) && isPlain(strategy));
    if (strategies.length !== data.length) return INVALID_DATA;
    const champions = strategies.filter((strategy) => strategy.champion === true);
    return strategies.every(strategyEvidenceValid)
      && champions.length === 1
      && champions[0].strategy_version === "baseline_v1"
      && champions[0].historical_agent_overlay === false
      && champions[0].live_execution_enabled === false
      ? data
      : INVALID_DATA;
  }
  if (pathname === "/internal/v1/strategy-evolution") {
    if (requestMethod?.toUpperCase() !== "GET"
      || !matchesRuntimeSchema(data, strategyEvolutionRuntimeSchema) || !isPlain(data)
      || !Array.isArray(data.experiments) || data.experiments.length > 100
      || !Array.isArray(data.registry_events) || data.registry_events.length > 20
      || data.next_cursor !== null && (typeof data.next_cursor !== "string" || data.next_cursor.length > 2048)
      || data.live_execution_enabled !== false
      || (["partial", "stale"].includes(String(data.status)) && data.write_enabled !== false)
      || !evolutionOverviewValid(data)) {
      return INVALID_DATA;
    }
    return data;
  }
  const evolutionExperiment = /^\/internal\/v1\/strategy-experiments\/([^/]+)$/.exec(pathname);
  if (evolutionExperiment && validateUuid(evolutionExperiment[1])) {
    return requestMethod?.toUpperCase() === "GET"
      && matchesRuntimeSchema(data, experimentDetailRuntimeSchema) && isPlain(data)
      && isPlain(data.experiment) && sameUuid(data.experiment.experiment_id, evolutionExperiment[1])
      && evolutionDetailValid(data)
      && data.live_execution_enabled === false
      && (!["partial", "stale"].includes(String(data.integrity_status)) || data.write_enabled === false)
      ? data
      : INVALID_DATA;
  }
  const expectedEvolutionAction = evolutionActionFor(pathname, requestMethod);
  if (expectedEvolutionAction !== null) {
    return matchesRuntimeSchema(data, evolutionActionRuntimeSchema) && isPlain(data)
      && data.live_execution_enabled === false
      && (Array.isArray(expectedEvolutionAction)
        ? expectedEvolutionAction.includes(String(data.action))
        : data.action === expectedEvolutionAction)
      ? data
      : INVALID_DATA;
  }
  const researchRun = /^\/internal\/v1\/research-runs\/([^/]+)$/.exec(pathname);
  if (researchRun && validateUuid(researchRun[1])) {
    return matchesRuntimeSchema(data, researchRunRuntimeSchema) && isPlain(data) && researchAuthorityValid(data)
      && sameUuid(data.run_id, researchRun[1])
      ? data
      : INVALID_DATA;
  }
  const instrumentResearch = /^\/internal\/v1\/instruments\/([^/]+)\/research$/.exec(pathname);
  if (instrumentResearch && validateUuid(instrumentResearch[1])) {
    return matchesRuntimeSchema(data, researchRunRuntimeSchema) && isPlain(data) && researchAuthorityValid(data)
      && sameUuid(data.instrument_id, instrumentResearch[1])
      ? data
      : INVALID_DATA;
  }
  const researchAdmission = /^\/internal\/v1\/instruments\/([^/]+)\/research-admission$/.exec(pathname);
  if (researchAdmission && validateUuid(researchAdmission[1])) {
    return matchesRuntimeSchema(data, researchAdmissionRuntimeSchema) && isPlain(data)
      && sameUuid(data.instrument_id, researchAdmission[1]) && researchAdmissionValid(data)
      ? data
      : INVALID_DATA;
  }
  const brief = /^\/internal\/v1\/briefs\/(\d{4}-\d{2}-\d{2})$/.exec(pathname);
  if (brief && isIsoDate(brief[1])) {
    return matchesRuntimeSchema(data, dailyBriefRuntimeSchema) && isPlain(data)
      && data.session_date === brief[1]
      ? data
      : INVALID_DATA;
  }
  return schemaPath === undefined && isPlain(data) ? data : INVALID_DATA;
}

function portfolioDecisionStateValid(data: Record<string, unknown>): boolean {
  if (data.status === "prepared" || data.status === "running") return data.output === null;
  if (!isPlain(data.output) || data.output.status !== data.status) return false;
  if (data.status === "completed") {
    return data.output.scenario_result_hash === data.expected_scenario_result_hash
      && isPlain(data.output.intent);
  }
  return (data.status === "abstained" || data.status === "failed")
    && data.output.scenario_result_hash === null
    && data.output.intent === null;
}

function scenarioSemanticsValid(data: Record<string, unknown>): boolean {
  if (!Array.isArray(data.positions)
    || !isPlain(data.baseline) || !isPlain(data.candidate)
    || typeof data.cash_current_weight !== "string"
    || typeof data.cash_target_weight !== "string") return false;
  const seen = new Set<string>();
  const currentWeights = [data.cash_current_weight];
  const targetWeights = [data.cash_target_weight];
  for (const position of data.positions) {
    if (!isPlain(position) || typeof position.instrument_id !== "string"
      || typeof position.current_weight !== "string"
      || typeof position.target_weight !== "string") return false;
    const identity = position.instrument_id.toLowerCase();
    if (seen.has(identity)) return false;
    seen.add(identity);
    const order = compareUnsignedDecimals(position.target_weight, position.current_weight);
    if (order === null || order > 0) return false;
    if (order < 0
      ? position.action !== "reduce" || position.reason_code !== "CONCENTRATION_REDUCTION"
      : position.action !== "maintain" || position.reason_code !== "BASELINE_WEIGHT") return false;
    currentWeights.push(position.current_weight);
    targetWeights.push(position.target_weight);
  }
  if (!decimalSumIsOne(currentWeights) || !decimalSumIsOne(targetWeights)
    || !scenarioMetricSetValid(data.baseline) || !scenarioMetricSetValid(data.candidate)) {
    return false;
  }
  const baselineCash = data.baseline.cash_weight;
  const candidateCash = data.candidate.cash_weight;
  return isPlain(baselineCash) && isPlain(candidateCash)
    && baselineCash.status === "known" && candidateCash.status === "known"
    && typeof baselineCash.value === "string" && typeof candidateCash.value === "string"
    && compareUnsignedDecimals(baselineCash.value, data.cash_current_weight) === 0
    && compareUnsignedDecimals(candidateCash.value, data.cash_target_weight) === 0
    && scenarioCostBreakdownValid(data.cost_breakdown, data.candidate, data.valuation_time);
}

function scenarioMetricSetValid(value: Record<string, unknown>): boolean {
  return Object.values(value).every((metric) => isPlain(metric)
    && (metric.status === "known"
      ? typeof metric.value === "string" && parseUnsignedDecimal(metric.value) !== null
        && metric.reason === null
      : metric.status === "unknown" && metric.value === null
        && typeof metric.reason === "string" && metric.reason.length > 0));
}

function scenarioCostBreakdownValid(
  value: unknown,
  candidate: Record<string, unknown>,
  valuationTime: unknown,
): boolean {
  if (!isPlain(value) || !isPlain(candidate.estimated_cost)
    || contextMicroseconds(value.valuation_time) !== contextMicroseconds(valuationTime)) return false;
  const authorityAbsent = value.evidence_version === null
    && value.evidence_hash === null && value.available_at === null;
  const authorityComplete = typeof value.evidence_version === "string"
    && value.evidence_version.length > 0
    && typeof value.evidence_hash === "string"
    && /^[0-9a-f]{64}$/.test(value.evidence_hash)
    && contextMicroseconds(value.available_at) !== null
    && contextMicroseconds(value.valuation_time) !== null
    && contextMicroseconds(value.available_at)! <= contextMicroseconds(value.valuation_time)!;
  if (!authorityAbsent && !authorityComplete) return false;

  const componentNames = ["commission", "spread", "slippage", "regulatory_fees"];
  const components = componentNames.map((name) => value[name]);
  if (!components.every((metric) => scenarioCostMetricValid(metric, false))
    || !scenarioCostMetricValid(value.tax_impact, true)
    || !scenarioCostMetricValid(value.total_estimated_cost_ex_tax, false)) return false;
  const allMetrics = [...components, value.tax_impact, value.total_estimated_cost_ex_tax];
  if (authorityAbsent && allMetrics.some((metric) => isPlain(metric) && metric.status === "known")) {
    return false;
  }

  const total = value.total_estimated_cost_ex_tax;
  const allComponentsKnown = components.every((metric) => isPlain(metric) && metric.status === "known");
  if (!isPlain(total)
    || (allComponentsKnown
      ? total.status !== "known"
        || !decimalListSumEquals(
          components.map((metric) => isPlain(metric) ? metric.value : null),
          total.value,
        )
      : total.status !== "unknown")) return false;
  return scenarioMetricsEqual(candidate.estimated_cost, total);
}

function scenarioCostMetricValid(value: unknown, allowNegative: boolean): boolean {
  if (!isPlain(value)) return false;
  if (value.status === "unknown") {
    return value.value === null && typeof value.reason === "string" && value.reason.length > 0;
  }
  if (value.status !== "known" || value.reason !== null) return false;
  const parsed = parseExactDecimal(value.value);
  return parsed !== null && (allowNegative || parsed.coefficient >= BigInt(0));
}

function scenarioMetricsEqual(left: unknown, right: unknown): boolean {
  if (!isPlain(left) || !isPlain(right) || left.status !== right.status
    || left.reason !== right.reason) return false;
  if (left.value === null || right.value === null) return left.value === right.value;
  const a = parseExactDecimal(left.value);
  const b = parseExactDecimal(right.value);
  if (a === null || b === null) return false;
  const scale = Math.max(a.scale, b.scale);
  return a.coefficient * BigInt(10) ** BigInt(scale - a.scale)
    === b.coefficient * BigInt(10) ** BigInt(scale - b.scale);
}

function parseUnsignedDecimal(value: string): { units: bigint; scale: number } | null {
  const match = /^(0|[1-9]\d*)(?:\.(\d+))?$/.exec(value);
  if (!match) return null;
  const fraction = match[2] ?? "";
  return { units: BigInt(`${match[1]}${fraction}`), scale: fraction.length };
}

function compareUnsignedDecimals(left: string, right: string): number | null {
  const a = parseUnsignedDecimal(left);
  const b = parseUnsignedDecimal(right);
  if (a === null || b === null) return null;
  const scale = Math.max(a.scale, b.scale);
  const leftUnits = a.units * BigInt(10) ** BigInt(scale - a.scale);
  const rightUnits = b.units * BigInt(10) ** BigInt(scale - b.scale);
  return leftUnits < rightUnits ? -1 : leftUnits > rightUnits ? 1 : 0;
}

function decimalSumIsOne(values: string[]): boolean {
  const parsed = values.map(parseUnsignedDecimal);
  if (parsed.some((value) => value === null)) return false;
  const exact = parsed as { units: bigint; scale: number }[];
  const scale = Math.max(...exact.map((value) => value.scale));
  const total = exact.reduce(
    (sum, value) => sum + value.units * BigInt(10) ** BigInt(scale - value.scale),
    BigInt(0),
  );
  return total === BigInt(10) ** BigInt(scale);
}

// The Engine has already verified the stored hash; keep its context-only/time/identity
// contract intact at the browser boundary instead of granting authority from prose.
function validDraftResearchContext(group: Record<string, unknown>): boolean {
  const context = group.research_context;
  if (context == null) return true;
  if (!isPlain(context) || !sameUuid(context.portfolio_snapshot_id, group.baseline_portfolio_snapshot_id)
    || !isPlain(group.shadow_eligibility) || group.shadow_eligibility.eligible !== false
    || !Array.isArray(group.proposals) || !group.proposals.every((proposal) =>
      isPlain(proposal) && proposal.analysis_only === true && isPlain(proposal.comparison)
      && proposal.comparison.non_executable === true && isPlain(proposal.comparison.metrics)
      && proposal.comparison.metrics.ranking_eligible === false
      && isPlain(proposal.comparison.metrics.authority)
      && sameUuid(context.diagnostic_report_id, proposal.comparison.metrics.authority.diagnostic_report_id))) return false;
  const values = [context.valuation_time, context.research_as_of, context.output_available_at, context.context_as_of].map(contextMicroseconds);
  if (!values.every((value): value is bigint => value !== null)) return false;
  if (values.some((value, index) => index > 0 && value < values[index - 1])) return false;
  if (context.intent != null) {
    const intent = context.intent;
    return isPlain(intent) && context.run_status === "completed" && context.unavailable_reason === null
      && sameUuid(intent.instrument_id, context.instrument_id)
      && sameUuid(intent.data_snapshot_id, context.research_data_snapshot_id)
      && contextMicroseconds(intent.decision_time) === values[1]
      && intent.quality_status === context.quality_status;
  }
  return context.quality_status === "partial" && (
    context.run_status === "completed" && context.unavailable_reason === "STAGED_ACTION_SEMANTICS_UNVERIFIED"
    || context.run_status === "failed" && context.unavailable_reason === "RESEARCH_FAILED"
    || context.run_status === "abstained" && context.unavailable_reason === "RESEARCH_ABSTAINED"
  );
}

function contextMicroseconds(value: unknown): bigint | null {
  if (typeof value !== "string") return null;
  const match = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,6}))?(?:Z|\+00:00)$/.exec(value);
  if (!match) return null;
  const seconds = Date.parse(`${match[1]}Z`);
  return Number.isFinite(seconds)
    ? BigInt(seconds) * BigInt(1000) + BigInt((match[2] ?? "").padEnd(6, "0"))
    : null;
}

function shadowStartValid(data: unknown, groupId: string): data is Record<string, unknown> {
  if (!matchesRuntimeSchema(data, shadowStartRuntimeSchema) || !isPlain(data)) return false;
  return sameUuid(data.proposal_group_id, groupId)
    && data.status === "shadowing"
    && orderedShadowWindow(data);
}

function paperOrderValid(value: unknown): boolean {
  if (!isPlain(value) || !Array.isArray(value.events) || value.events.length < 1
    || !Array.isArray(value.fills)) return false;
  const events = value.events;
  const lastEvent = events.at(-1);
  if (!events.every((event, index) => isPlain(event)
      && event.event_seq === index + 1
      && (index === 0 ? event.from_status === null : event.from_status === (events[index - 1] as Record<string, unknown>).to_status))
    || !isPlain(lastEvent)
    || value.current_event_seq !== lastEvent.event_seq
    || value.status !== lastEvent.to_status) return false;
  const shouldBeCancellable = ["submitted", "partially_filled"].includes(String(value.status))
    && decimalPositive(value.remaining_quantity);
  if (value.cancellable !== shouldBeCancellable) return false;
  const fills = value.fills;
  return decimalPositive(value.quantity)
    && decimalPositive(value.worst_price)
    && decimalNonnegative(value.filled_quantity)
    && decimalNonnegative(value.remaining_quantity)
    && decimalSumEquals(value.filled_quantity, value.remaining_quantity, value.quantity)
    && fills.every((fill, index) => isPlain(fill)
      && fill.fill_seq === index + 1
      && decimalPositive(fill.quantity))
    && decimalListSumEquals(
      fills.map((fill) => (fill as Record<string, unknown>).quantity),
      value.filled_quantity,
    );
}

function paperPortfolioValid(value: Record<string, unknown>): boolean {
  const positions = value.positions as unknown[];
  if (value.performance === null) {
    return value.data_state === "EMPTY" && positions.length === 0
      && (value.attributions as unknown[]).length === 0;
  }
  if (value.data_state === "EMPTY" || !isPlain(value.performance)) return false;
  const performance = value.performance;
  if (!decimalNonnegative(performance.available_cash)
    || !decimalNonnegative(performance.reserved_cash)
    || !decimalSumEquals(performance.available_cash, performance.reserved_cash, performance.cash)
    || !decimalSumEquals(performance.cash, performance.market_value, performance.nav)
    || decimalCompare(performance.peak_nav, performance.nav) < 0) return false;
  if (!positions.every((position) => isPlain(position)
      && decimalNonnegative(position.quantity)
      && decimalNonnegative(position.reserved_quantity)
      && decimalNonnegative(position.available_quantity)
      && decimalSumEquals(position.available_quantity, position.reserved_quantity, position.quantity)
      && decimalProductEquals(position.quantity, position.reference_close, position.market_value)
      && decimalSumEquals(position.cost_basis, position.unrealized_pnl, position.market_value))) return false;
  return decimalListSumEquals(
    positions.map((position) => (position as Record<string, unknown>).market_value),
    performance.market_value,
  );
}

type ExactDecimal = { coefficient: bigint; scale: number };

function parseExactDecimal(value: unknown): ExactDecimal | null {
  if (typeof value !== "string") return null;
  const match = /^(-?)(0|[1-9]\d*)(?:\.(\d+))?$/.exec(value);
  if (!match) return null;
  const fraction = match[3] ?? "";
  const coefficient = BigInt(`${match[1]}${match[2]}${fraction}`);
  return { coefficient, scale: fraction.length };
}

function decimalPositive(value: unknown): boolean {
  const parsed = parseExactDecimal(value);
  return parsed !== null && parsed.coefficient > BigInt(0);
}

function decimalNonnegative(value: unknown): boolean {
  const parsed = parseExactDecimal(value);
  return parsed !== null && parsed.coefficient >= BigInt(0);
}

function decimalSumEquals(left: unknown, right: unknown, expected: unknown): boolean {
  const values = [left, right, expected].map(parseExactDecimal);
  if (values.some((value) => value === null)) return false;
  const decimals = values as ExactDecimal[];
  const scale = Math.max(...decimals.map((value) => value.scale));
  const scaled = decimals.map((value) => value.coefficient * BigInt(10) ** BigInt(scale - value.scale));
  return scaled[0] + scaled[1] === scaled[2];
}

function decimalListSumEquals(values: unknown[], expected: unknown): boolean {
  const decimals = values.map(parseExactDecimal);
  const target = parseExactDecimal(expected);
  if (target === null || decimals.some((value) => value === null)) return false;
  const exact = decimals as ExactDecimal[];
  const scale = Math.max(target.scale, 0, ...exact.map((value) => value.scale));
  const sum = exact.reduce(
    (total, value) => total + value.coefficient * BigInt(10) ** BigInt(scale - value.scale),
    BigInt(0),
  );
  return sum === target.coefficient * BigInt(10) ** BigInt(scale - target.scale);
}

function decimalProductEquals(left: unknown, right: unknown, expected: unknown): boolean {
  const multiplicand = parseExactDecimal(left);
  const multiplier = parseExactDecimal(right);
  const target = parseExactDecimal(expected);
  if (multiplicand === null || multiplier === null || target === null) return false;
  const product = multiplicand.coefficient * multiplier.coefficient;
  const productScale = multiplicand.scale + multiplier.scale;
  const scale = Math.max(productScale, target.scale);
  return product * BigInt(10) ** BigInt(scale - productScale)
    === target.coefficient * BigInt(10) ** BigInt(scale - target.scale);
}

function decimalCompare(left: unknown, right: unknown): number {
  const first = parseExactDecimal(left);
  const second = parseExactDecimal(right);
  if (first === null || second === null) return Number.NaN;
  const scale = Math.max(first.scale, second.scale);
  const leftValue = first.coefficient * BigInt(10) ** BigInt(scale - first.scale);
  const rightValue = second.coefficient * BigInt(10) ** BigInt(scale - second.scale);
  return leftValue < rightValue ? -1 : leftValue > rightValue ? 1 : 0;
}

function shadowGroupValid(data: unknown, groupId: string): data is Record<string, unknown> {
  if (!matchesRuntimeSchema(data, shadowGroupRuntimeSchema) || !isPlain(data)
    || !sameUuid(data.proposal_group_id, groupId) || !orderedShadowWindow(data)
    || !isPlain(data.coverage) || !isPlain(data.freshness) || !isPlain(data.safety)
    || !Array.isArray(data.portfolios) || !Array.isArray(data.checkpoints)) return false;

  const portfolios = data.portfolios;
  const expectedVariants = ["hold_current", "minimum_change", "strategy_target"];
  if (portfolios.map((item) => isPlain(item) ? item.variant : null).join(":") !== expectedVariants.join(":")) return false;
  const portfolioIds = portfolios.map((item) => isPlain(item) ? item.shadow_portfolio_id : null);
  if (!portfolioIds.every((value) => typeof value === "string") || new Set(portfolioIds).size !== 3) return false;

  const navSessions = portfolios.map((portfolio) => {
    if (!isPlain(portfolio) || !Array.isArray(portfolio.nav) || portfolio.nav.length > 63) return null;
    const sessions = portfolio.nav.map((point) => isPlain(point) ? point.session : null);
    return sessions.every((session) => typeof session === "string")
      && strictlyIncreasing(sessions as string[]) ? sessions as string[] : null;
  });
  if (navSessions.some((sessions) => sessions === null)) return false;
  const firstSessions = navSessions[0] as string[];
  if (!navSessions.every((sessions) => (sessions as string[]).join(":") === firstSessions.join(":"))
    || data.coverage.completed !== firstSessions.length) return false;

  const completed = data.coverage.completed;
  const nextExpected = data.coverage.next_expected_session;
  const missing = data.coverage.missing_sessions;
  if (!Number.isInteger(completed) || completed < 0 || completed > 63
    || (completed === 63) !== (nextExpected === null)
    || !Array.isArray(missing) || !missing.every((session) => typeof session === "string")
    || !strictlyIncreasing(missing as string[])) return false;

  if ((data.freshness.status === "current") !== (data.freshness.reason === null)
    || (data.freshness.latest_session === null) !== (data.freshness.available_at === null)) return false;

  const ordinals = data.checkpoints.map((checkpoint) => isPlain(checkpoint) ? checkpoint.ordinal : null);
  if (!ordinals.every((ordinal) => ordinal === 5 || ordinal === 21 || ordinal === 63)
    || !strictlyIncreasing(ordinals as number[])
    || !data.checkpoints.every(shadowCheckpointValid)) return false;

  if (data.safety.creates_orders !== false || data.safety.trading_approval !== false) return false;
  if (data.sealed === true) {
    if (!(["validated", "rejected", "expired"].includes(String(data.seal_reason)))
      || data.proposal_status !== data.seal_reason) return false;
  } else if (data.seal_reason !== null || data.proposal_status !== "shadowing") return false;
  return true;
}

function shadowCheckpointValid(value: unknown): boolean {
  if (!isPlain(value) || !Array.isArray(value.metrics) || value.metrics.length !== 3) return false;
  const variants = value.metrics.map((metric) => isPlain(metric) ? metric.variant : null);
  if (variants.join(":") !== "hold_current:minimum_change:strategy_target") return false;
  const expectedStatus = value.status === "complete" ? "known" : "unknown";
  if ((value.status === "complete") !== (value.reason === null)) return false;
  const metricKeys = [
    "net_cumulative_return",
    "return_delta_vs_hold",
    "max_drawdown",
    "drawdown_improvement",
    "annualized_volatility",
    "volatility_improvement",
    "cumulative_turnover",
    "fee_drag_index_points",
  ];
  return value.metrics.every((variantMetric) => {
    if (!isPlain(variantMetric) || !isPlain(variantMetric.metrics)) return false;
    const metrics = variantMetric.metrics;
    return metricKeys.every((key) => {
      const metric = metrics[key];
      return isPlain(metric) && metric.status === expectedStatus;
    });
  });
}

function orderedShadowWindow(data: Record<string, unknown>): boolean {
  return typeof data.start_session === "string"
    && typeof data.evaluation_end_session === "string"
    && typeof data.grace_end_session === "string"
    && data.start_session <= data.evaluation_end_session
    && data.evaluation_end_session < data.grace_end_session;
}

function strictlyIncreasing(values: Array<string | number>): boolean {
  return values.every((value, index) => index === 0 || values[index - 1] < value);
}

function safeIntegrationStatus(value: unknown): Record<string, unknown> | null {
  if (!matchesRuntimeSchema(value, integrationStatusRuntimeSchema) || !isPlain(value)) return null;
  if (value.contract_hash !== null && (typeof value.contract_hash !== "string" || !/^[a-f0-9]{64}$/.test(value.contract_hash))) return null;
  return Object.fromEntries(
    Object.keys(integrationStatusRuntimeSchema.properties).map((key) => [key, value[key]]),
  );
}

function matchesRuntimeSchema(value: unknown, schema: unknown): boolean {
  if (!isPlain(schema)) return false;
  if (typeof schema.$ref === "string") {
    const prefix = "#/components/schemas/";
    if (!schema.$ref.startsWith(prefix)) return false;
    const referenced = (runtimeContractSchemas as Record<string, unknown>)[schema.$ref.slice(prefix.length)];
    return referenced !== undefined && matchesRuntimeSchema(value, referenced);
  }
  if (Array.isArray(schema.anyOf)) return schema.anyOf.some((candidate) => matchesRuntimeSchema(value, candidate));
  if (Array.isArray(schema.oneOf)) return schema.oneOf.filter((candidate) => matchesRuntimeSchema(value, candidate)).length === 1;
  if (Array.isArray(schema.allOf)) return schema.allOf.every((candidate) => matchesRuntimeSchema(value, candidate));
  if ("const" in schema && !Object.is(schema.const, value)) return false;
  if (Array.isArray(schema.enum) && !schema.enum.some((candidate) => Object.is(candidate, value))) return false;
  if (schema.type === "null") return value === null;
  if (schema.type === "boolean") return typeof value === "boolean";
  if (schema.type === "string") return matchesRuntimeString(value, schema);
  if (schema.type === "integer" || schema.type === "number") {
    return typeof value === "number"
      && Number.isFinite(value)
      && (schema.type !== "integer" || Number.isInteger(value))
      && (typeof schema.minimum !== "number" || value >= schema.minimum)
      && (typeof schema.maximum !== "number" || value <= schema.maximum);
  }
  if (schema.type === "array") {
    if (!Array.isArray(value)
      || (typeof schema.minItems === "number" && value.length < schema.minItems)
      || (typeof schema.maxItems === "number" && value.length > schema.maxItems)) return false;
    const prefixItems = schema.prefixItems;
    if (Array.isArray(prefixItems)) {
      if (schema.items === false && value.length > prefixItems.length) return false;
      return value.every((item, index) => index < prefixItems.length
        ? matchesRuntimeSchema(item, prefixItems[index])
        : isPlain(schema.items) && matchesRuntimeSchema(item, schema.items));
    }
    return isPlain(schema.items) && value.every((item) => matchesRuntimeSchema(item, schema.items));
  }
  if (schema.type !== "object" || !isPlain(value)) return false;
  if (!isPlain(schema.properties)
    && !isPlain(schema.additionalProperties)
    && schema.additionalProperties !== false) return false;
  const properties = isPlain(schema.properties) ? schema.properties : {};
  const required = Array.isArray(schema.required) && schema.required.every((key) => typeof key === "string")
    ? schema.required as string[]
    : [];
  if (!required.every((key) => key in value)) return false;
  if (schema.additionalProperties === false && Object.keys(value).some((key) => !(key in properties))) return false;
  return Object.entries(value).every(([key, item]) => {
    if (key in properties) return matchesRuntimeSchema(item, properties[key]);
    return isPlain(schema.additionalProperties)
      && matchesRuntimeSchema(item, schema.additionalProperties);
  });
}

function matchesRuntimeString(value: unknown, schema: Record<string, unknown>): boolean {
  if (typeof value !== "string") return false;
  if (typeof schema.minLength === "number" && value.length < schema.minLength) return false;
  if (typeof schema.maxLength === "number" && value.length > schema.maxLength) return false;
  if (typeof schema.pattern === "string") {
    try { if (!new RegExp(schema.pattern).test(value)) return false; } catch { return false; }
  }
  if (schema.format === "date-time" && !isUtcIso(value)) return false;
  if (schema.format === "date" && !isIsoDate(value)) return false;
  if (schema.format === "uuid" && !validateUuid(value)) return false;
  return true;
}

function enginePathname(requestUrl?: string, schemaPath?: string): string {
  if (schemaPath) return schemaPath;
  if (!requestUrl) return "";
  try {
    const pathname = new URL(requestUrl).pathname;
    if (!pathname.startsWith("/api/v1/")) return pathname;
    if (pathname === "/api/v1/strategy-evolution/experiments") return "/internal/v1/strategy-experiments";
    if (pathname === "/api/v1/strategy-evolution/registry/control") return "/internal/v1/strategy-registry/control";
    if (pathname === "/api/v1/strategy-evolution/registry/rollback") return "/internal/v1/strategy-registry/rollback";
    const experiment = /^\/api\/v1\/strategy-evolution\/experiments\/([^/]+)(?:\/(veto))?$/.exec(pathname);
    if (experiment) return `/internal/v1/strategy-experiments/${experiment[1]}${experiment[2] ? "/veto" : ""}`;
    return pathname.replace("/api/v1/", "/internal/v1/");
  } catch { return ""; }
}

function evolutionActionFor(pathname: string, requestMethod?: string): string | string[] | null {
  if (requestMethod?.toUpperCase() !== "POST") return null;
  if (pathname === "/internal/v1/strategy-experiments") return "generate_family";
  if (/^\/internal\/v1\/strategy-experiments\/[^/]+\/veto$/.test(pathname)) return "veto_experiment";
  if (pathname === "/internal/v1/strategy-registry/control") return ["pause_auto_promotion", "resume_auto_promotion"];
  if (pathname === "/internal/v1/strategy-registry/rollback") return "rollback_registry";
  return null;
}

function brokerActionFor(
  pathname: string,
  requestMethod?: string,
): { action: "approve" | "reject" | "acknowledge"; resourceId: string } | null {
  if (requestMethod?.toUpperCase() !== "POST") return null;
  const intent = /^\/internal\/v1\/broker-simulation\/intents\/([^/]+)\/(approve|reject)$/.exec(pathname);
  if (intent && validateUuid(intent[1])) {
    return { action: intent[2] as "approve" | "reject", resourceId: intent[1] };
  }
  const reconciliation = /^\/internal\/v1\/broker-simulation\/reconciliation\/([^/]+)\/acknowledge$/.exec(pathname);
  return reconciliation && validateUuid(reconciliation[1])
    ? { action: "acknowledge", resourceId: reconciliation[1] }
    : null;
}

function brokerSimulationValid(data: Record<string, unknown>): boolean {
  if (!isPlain(data.capability) || !isPlain(data.actions)
    || data.capability.live_trading !== false
    || data.actions.can_dispatch !== false
    || data.actions.can_modify !== false
    || data.actions.can_cancel !== false
    || (data.capability.external_activation === "external_activation_blocked"
      && data.capability.simulation_write_enabled !== false)
    || !Array.isArray(data.events) || data.events.length > 200) return false;
  if (data.intent !== null && (!isPlain(data.intent) || !brokerIntentValid(data.intent))) return false;
  if (data.account !== null && !isPlain(data.account)) return false;
  if (data.reconciliation !== null && !isPlain(data.reconciliation)) return false;
  const sequences = data.events.map((event) => isPlain(event) ? event.sequence : null);
  if (!sequences.every((sequence) => Number.isSafeInteger(sequence))
    || !strictlyIncreasing(sequences as number[])) return false;
  if (isPlain(data.intent)) {
    const orderIds = new Set((data.intent.orders as Array<Record<string, unknown>>).map((order) => order.order_id));
    if (data.events.some((event) => !isPlain(event) || !orderIds.has(event.order_id))) return false;
  } else if (data.events.length > 0) return false;
  return true;
}

function brokerIntentValid(data: Record<string, unknown>): boolean {
  if (!Array.isArray(data.orders) || data.orders.length > 100
    || !Array.isArray(data.evidence_hashes) || data.evidence_hashes.length > 100
    || !data.evidence_hashes.every((hash) => typeof hash === "string" && /^[0-9a-f]{64}$/.test(hash))) return false;
  const orderIds = data.orders.map((order) => isPlain(order) ? order.order_id : null);
  return orderIds.every((id) => typeof id === "string") && new Set(orderIds).size === orderIds.length;
}

function evolutionOverviewValid(data: Record<string, unknown>): boolean {
  if (!isPlain(data.champion) || !Array.isArray(data.experiments) || !Array.isArray(data.registry_events)) return false;
  const experimentIds = data.experiments.map((experiment) => isPlain(experiment) ? experiment.experiment_id : null);
  if (experimentIds.some((value) => typeof value !== "string") || new Set(experimentIds).size !== experimentIds.length) return false;
  if (!data.registry_events.every((event) => isPlain(event) && Number.isSafeInteger(event.sequence))) return false;
  const sequences = data.registry_events.map((event) => Number((event as Record<string, unknown>).sequence));
  return strictlyIncreasing(sequences)
    && (sequences.length === 0 || sequences.at(-1) === data.champion.event_sequence);
}

function evolutionDetailValid(data: Record<string, unknown>): boolean {
  if (!isPlain(data.experiment) || !isPlain(data.family) || !Array.isArray(data.events)) return false;
  if (!sameUuid(data.experiment.family_id, data.family.family_id)) return false;
  return data.events.every((event, index) => isPlain(event) && event.sequence === index + 1);
}

function responseIdentityValid(data: unknown, meta: Record<string, unknown>, requestUrl?: string): boolean {
  if (!requestUrl) return true;
  try {
    const url = new URL(requestUrl);
    if (/^\/(?:api|internal)\/v1\/instruments\/[^/]+\/research-admission$/.test(url.pathname)) {
      const values = url.searchParams.getAll("as_of");
      return values.length === 1 && isUtcIso(values[0]) && meta.as_of === values[0]
        && isPlain(data) && data.decision_time === values[0];
    }
    if (/^\/(?:api|internal)\/v1\/instruments\/[^/]+\/research$/.test(url.pathname)) {
      const values = url.searchParams.getAll("as_of");
      return values.length === 1 && isUtcIso(values[0]) && meta.as_of === values[0]
        && isPlain(data) && typeof data.decision_time === "string"
        && Date.parse(data.decision_time) <= Date.parse(values[0]);
    }
    return true;
  } catch { return false; }
}

function researchAdmissionValid(data: Record<string, unknown>): boolean {
  if (!researchTrendsValid(data)) return false;
  if (typeof data.eligible !== "boolean") return false;
  const hasSnapshot = typeof data.data_snapshot_id === "string" && validateUuid(data.data_snapshot_id);
  if (data.eligible) return data.reason === null && hasSnapshot;
  if (data.reason === null) return false;
  if (["instrument_not_found", "decision_time_in_future", "data_snapshot_unavailable"].includes(String(data.reason))) {
    return data.data_snapshot_id === null;
  }
  return hasSnapshot;
}

function researchAuthorityValid(data: Record<string, unknown>): boolean {
  if (!researchTrendsValid(data)) return false;
  if (data.intent === null) return true;
  if (!isPlain(data.intent)) return false;
  return sameUuid(data.intent.instrument_id, data.instrument_id)
    && sameUuid(data.intent.data_snapshot_id, data.data_snapshot_id)
    && typeof data.intent.decision_time === "string"
    && typeof data.decision_time === "string"
    && Date.parse(data.intent.decision_time) === Date.parse(data.decision_time);
}

function researchTrendsValid(data: Record<string, unknown>): boolean {
  // Only this optional supplemental source; no latest-data joins or indicator calculations.
  if (!isPlain(data.inputs) || data.inputs.current_price_trends === undefined) return true;
  const trends = data.inputs.current_price_trends;
  const cutoff = contextMicroseconds(data.decision_time);
  if (!Array.isArray(trends) || cutoff === null) return false;
  return trends.every((trend) => {
    if (!isPlain(trend) || !isPlain(trend.features) || !Array.isArray(trend.missing_sessions)) return false;
    const received = contextMicroseconds(trend.received_at);
    const available = contextMicroseconds(trend.available_at);
    if (received === null || available === null || received > available || available > cutoff) return false;
    if (String(trend.identity_asof) > String(trend.received_at).slice(0, 10) || trend.revision !== trend.content_hash) return false;
    const start = String(trend.query_start), end = String(trend.query_end);
    if (start > end || end > String(trend.received_at).slice(0, 10)) return false;
    const liquidity = trend.liquidity_observation;
    if (liquidity !== undefined && liquidity !== null) {
      if (!isPlain(liquidity) || !Array.isArray(liquidity.missing_sessions)) return false;
      const windowStart = String(liquidity.window_start), windowEnd = String(liquidity.window_end);
      const days = liquidity.missing_sessions as string[];
      const count = Number(liquidity.observed_sessions), size = count + days.length;
      if (windowStart < start || windowStart > windowEnd || windowEnd !== end || size > 20
        || days.some((day, index) => day < windowStart || day > windowEnd || index > 0 && day <= days[index - 1])) return false;
      if (liquidity.status === "available") {
        if (!decimalNonnegative(liquidity.value) || count !== 20 || days.length !== 0) return false;
      } else if (liquidity.value !== null || count >= 20
        || liquidity.status === "missing_sessions" && (days.length === 0 || size !== 20)
        || liquidity.status === "insufficient_sessions" && size >= 20) return false;
    }
    const observed = Number(trend.observed_sessions), expected = Number(trend.expected_sessions);
    const tail = Number(trend.contiguous_tail_sessions), total = Number(trend.missing_sessions_total);
    const missing = trend.missing_sessions as string[];
    if (observed + total !== expected || tail > observed
      || missing.length !== Math.min(total, 20) || trend.missing_sessions_truncated !== (total > 20)
      || missing.some((day, index) => day < start || day > end || index > 0 && day <= missing[index - 1])) return false;
    if (observed === 0) {
      if (trend.first_observed_on !== null || trend.last_observed_on !== null) return false;
    } else if (trend.first_observed_on === null || trend.last_observed_on === null
      || String(trend.first_observed_on) < start || String(trend.first_observed_on) > String(trend.last_observed_on)
      || String(trend.last_observed_on) > end) return false;
    const features = trend.features;
    if ([features.close, features.sma_20, features.sma_200].some((value) => value !== null && !decimalPositive(value))) return false;
    if (features.rsi_14 !== null && (!decimalNonnegative(features.rsi_14) || decimalCompare(features.rsi_14, "100") > 0)) return false;
    if (features.volatility_20 !== null && !decimalNonnegative(features.volatility_20)) return false;
    return Object.entries({ close: 1, sma_20: 20, sma_200: 200, rsi_14: 15, volatility_20: 21 })
      .every(([key, required]) => features[key] === null || tail >= required);
  });
}

function strategyEvidenceValid(data: Record<string, unknown>): boolean {
  if (data.latest_backtest === null) return true;
  if (!isPlain(data.latest_backtest)) return false;
  const evidence = data.latest_backtest;
  const hashes = [evidence.result_hash, evidence.data_hash, evidence.code_hash];
  if (!hashes.every((value) => typeof value === "string" && /^[0-9a-f]{64}$/.test(value))) return false;
  if (!isIsoDate(evidence.start_session) || !isIsoDate(evidence.end_session)
    || String(evidence.start_session) > String(evidence.end_session)) return false;
  if (!Number.isInteger(evidence.oos_window_count) || Number(evidence.oos_window_count) < 0) return false;
  if (!isUtcIso(evidence.completed_at) || !Array.isArray(evidence.comparison_keys)) return false;
  const keys = evidence.comparison_keys;
  if (keys.length !== new Set(keys).size
    || !keys.every((key) => ["spy", "equal_weight", "champion"].includes(String(key)))) return false;
  const complete = keys.length === 3
    && ["spy", "equal_weight", "champion"].every((key) => keys.includes(key));
  if (typeof evidence.benchmark_comparison_complete !== "boolean"
    || evidence.benchmark_comparison_complete !== complete
    || !Array.isArray(evidence.data_sources)
    || !evidence.data_sources.every((source) => typeof source === "string" && source.length > 0)
    || evidence.data_sources.length === 0
    || evidence.data_sources.length !== new Set(evidence.data_sources).size) return false;
  const sources = evidence.data_sources as string[];
  const domains = ["price", "universe", "sector", "corporate_action", "calendar"];
  const byDomain = Object.fromEntries(domains.map((domain) => [
    domain,
    sources.filter((source) => source.startsWith(`${domain}:`)),
  ])) as Record<string, string[]>;
  if (domains.some((domain) => byDomain[domain].length === 0)) return false;
  const marketSources = ["price", "universe", "sector", "corporate_action"]
    .flatMap((domain) => byDomain[domain]);
  const fixtureFlags = marketSources.map((source) => source.split(":").includes("fixture"));
  const priceModes = new Set(byDomain.price.map((source) => source.split(":").at(-1)));
  if (evidence.data_mode === "fixture") return fixtureFlags.every(Boolean);
  if (evidence.data_mode === "delayed" || evidence.data_mode === "real_time") {
    return !fixtureFlags.some(Boolean) && priceModes.size === 1 && priceModes.has(evidence.data_mode);
  }
  return evidence.data_mode === "mixed"
    && ((fixtureFlags.some(Boolean) && !fixtureFlags.every(Boolean)) || priceModes.size > 1);
}


function isV7(value: unknown): boolean { return typeof value === "string" && validateUuid(value) && uuidVersion(value) === 7; }
function isUtcIso(value: unknown): boolean { if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$/.test(value)) return false; const date = new Date(value); return !Number.isNaN(date.valueOf()) && date.toISOString().slice(0, 19) === value.slice(0, 19); }
function isIsoDate(value: unknown): boolean { if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return false; const date = new Date(`${value}T00:00:00Z`); return !Number.isNaN(date.valueOf()) && date.toISOString().slice(0, 10) === value; }
function isStableCode(value: unknown): value is StableEngineErrorCode { return typeof value === "string" && Object.hasOwn(ERROR_STATUSES, value); }
function allowsStatus(code: unknown, status: number): boolean { return isStableCode(code) && ERROR_STATUSES[code].includes(status as never); }
function safeHeaders(upstream: Headers, requestUrl?: string): Headers { const headers = new Headers(); for (const name of ["retry-after", "etag"]) { const value = upstream.get(name); if (value) headers.set(name, value); } const location = upstream.get("location"); if (location && requestUrl) try { const target = new URL(location, requestUrl); const origin = new URL(requestUrl).origin; if (target.origin === origin && !target.username && !target.password) headers.set("location", `${target.pathname}${target.search}${target.hash}`); } catch { return headers; } return headers; }
function isObject(value: unknown, keys: string[]): value is Record<string, unknown> { return isPlain(value) && Object.keys(value).length === keys.length && keys.every((key) => key in value); }
function isPlain(value: unknown): value is Record<string, unknown> { return typeof value === "object" && value !== null && !Array.isArray(value); }
