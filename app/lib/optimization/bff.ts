import { validate as validateUuid } from "uuid";

import { NexusBffError } from "../engine/errors";

const DECIMAL = /^(?:0(?:\.\d{1,38})?|1(?:\.0{1,38})?)$/;

export type ExactJson = { bytes: Uint8Array; value: Record<string, unknown> };

export async function readExactJson(request: Request, message: string): Promise<ExactJson> {
  if (request.headers.get("content-type")?.split(";", 1)[0]?.trim().toLowerCase() !== "application/json") {
    throw invalid(message);
  }
  const bytes = new Uint8Array(await request.arrayBuffer());
  let value: unknown;
  try { value = JSON.parse(new TextDecoder().decode(bytes)); } catch { throw invalid(message); }
  if (!isPlain(value)) throw invalid(message);
  return { bytes, value };
}

export function requireIdempotencyKey(request: Request): string {
  const value = request.headers.get("idempotency-key");
  if (!value || value.length > 255 || /[^\x21-\x7E]/.test(value)) {
    throw invalid("The idempotency key is invalid.");
  }
  return value;
}

export function requireProposalAdmission(value: Record<string, unknown>): void {
  requireExactKeys(value, ["portfolio_snapshot_id", "data_snapshot_id", "cost_policy_id"]);
  for (const key of ["portfolio_snapshot_id", "data_snapshot_id", "cost_policy_id"] as const) {
    if (typeof value[key] !== "string" || !validateUuid(value[key])) {
      throw invalid("The proposal request is invalid.");
    }
  }
}

export function requireProposalDraft(value: Record<string, unknown>): void {
  requireExactKeys(value, ["portfolio_snapshot_id", "diagnostic_report_id"]);
  for (const key of ["portfolio_snapshot_id", "diagnostic_report_id"] as const) {
    if (typeof value[key] !== "string" || !validateUuid(value[key])) {
      throw invalid("The draft request is invalid.");
    }
  }
}

export function requireLifecycleVersion(request: Request, value: Record<string, unknown>): number {
  requireExactKeys(value, ["version"]);
  const version = value.version;
  if (!Number.isSafeInteger(version) || (version as number) < 0) {
    throw invalid("The proposal version is invalid.");
  }
  if (request.headers.get("if-match") !== `"${version}"`) {
    throw invalid("If-Match must match the proposal version.");
  }
  return version as number;
}

export function requireWhatIf(value: Record<string, unknown>): void {
  requireExactKeys(value, ["proposal_group_id", "proposal_id", "policy_version", "positions"]);
  if (typeof value.proposal_group_id !== "string" || !validateUuid(value.proposal_group_id)
    || typeof value.proposal_id !== "string" || !validateUuid(value.proposal_id)
    || value.policy_version !== "optimizer_v1"
    || !Array.isArray(value.positions) || value.positions.length < 1 || value.positions.length > 30) {
    throw invalid("The what-if request is invalid.");
  }
  const seen = new Set<string>();
  for (const position of value.positions) {
    if (!isPlain(position)) throw invalid("The what-if request is invalid.");
    requireExactKeys(position, ["instrument_id", "weight"]);
    if (typeof position.instrument_id !== "string" || !validateUuid(position.instrument_id)
      || seen.has(position.instrument_id)
      || typeof position.weight !== "string" || !DECIMAL.test(position.weight)) {
      throw invalid("The what-if request is invalid.");
    }
    seen.add(position.instrument_id);
  }
}

export function requirePortfolioScenarioRequest(value: Record<string, unknown>): void {
  requireExactKeys(value, [
    "schema_version",
    "portfolio_snapshot_id",
    "diagnostic_report_id",
    "as_of",
    "constraints",
  ]);
  if (value.schema_version !== "portfolio_scenario_request_v1"
    || typeof value.portfolio_snapshot_id !== "string" || !validateUuid(value.portfolio_snapshot_id)
    || typeof value.diagnostic_report_id !== "string" || !validateUuid(value.diagnostic_report_id)
    || typeof value.as_of !== "string" || !isUtcIso(value.as_of)
    || !isPlain(value.constraints)) {
    throw invalid("The portfolio scenario request is invalid.");
  }
  requireExactKeys(value.constraints, [
    "schema_version",
    "objective",
    "max_turnover",
    "minimum_cash",
    "universe_policy",
    "allow_new_positions",
  ]);
  if (value.constraints.schema_version !== "adjustment_scenario_constraints_v1"
    || value.constraints.objective !== "reduce_security_concentration"
    || typeof value.constraints.max_turnover !== "string" || !DECIMAL.test(value.constraints.max_turnover)
    || typeof value.constraints.minimum_cash !== "string" || !DECIMAL.test(value.constraints.minimum_cash)
    || value.constraints.universe_policy !== "current_holdings_only"
    || value.constraints.allow_new_positions !== false) {
    throw invalid("The portfolio scenario request is invalid.");
  }
}

export function requirePortfolioScenarioSelection(
  request: Request,
  value: Record<string, unknown>,
): { portfolioSnapshotId: string; diagnosticReportId: string; version: number } {
  requireExactKeys(value, ["schema_version", "scenario_request", "choice", "version"]);
  if (value.schema_version !== "portfolio_scenario_selection_v1"
    || !isPlain(value.scenario_request)
    || !["accepted_for_analysis", "rejected", "watch"].includes(String(value.choice))
    || !Number.isSafeInteger(value.version) || (value.version as number) < 0) {
    throw invalid("The portfolio scenario selection is invalid.");
  }
  requirePortfolioScenarioRequest(value.scenario_request);
  if (request.headers.get("if-match") !== `"${value.version}"`) {
    throw invalid("If-Match must match the scenario selection version.");
  }
  return {
    portfolioSnapshotId: value.scenario_request.portfolio_snapshot_id as string,
    diagnosticReportId: value.scenario_request.diagnostic_report_id as string,
    version: value.version as number,
  };
}

export function scenarioResultHash(request: Request): string {
  const match = /^\/api\/v1\/portfolio-scenarios\/([0-9a-f]{64})\/selection$/.exec(
    new URL(request.url).pathname,
  );
  if (!match) throw invalid("The portfolio scenario result is invalid.");
  return match[1];
}

export function requirePortfolioScenarioSelectionResponse(
  envelope: unknown,
  expected: { resultHash: string; portfolioSnapshotId?: string; diagnosticReportId?: string },
): void {
  if (!isPlain(envelope) || !isPlain(envelope.data)) throw invalidResponse();
  const data = envelope.data;
  if (data.schema_version !== "portfolio_scenario_selection_v1"
    || data.scenario_result_hash !== expected.resultHash
    || expected.portfolioSnapshotId !== undefined
      && data.portfolio_snapshot_id !== expected.portfolioSnapshotId
    || expected.diagnosticReportId !== undefined
      && data.diagnostic_report_id !== expected.diagnosticReportId
    || !["accepted_for_analysis", "rejected", "watch"].includes(String(data.choice))
    || !Number.isSafeInteger(data.version) || (data.version as number) < 1
    || typeof data.selected_at !== "string" || !isUtcIso(data.selected_at)
    || data.execution_eligible !== false
    || data.execution_locked !== true
    || data.execution_authorized !== false) {
    throw invalidResponse();
  }
}

export function proposalGroupId(request: Request, action?: "shadow" | "reject"): string {
  const suffix = action ? `/${action}` : "";
  const match = new RegExp(`^/api/v1/optimization-proposals/([^/]+)${suffix}$`).exec(new URL(request.url).pathname);
  const value = match?.[1];
  if (!value || !validateUuid(value)) throw invalid("The proposal group is invalid.");
  return value;
}

function requireExactKeys(value: Record<string, unknown>, keys: string[]): void {
  if (Object.keys(value).length !== keys.length || !keys.every((key) => key in value)) {
    throw invalid("The request body is invalid.");
  }
}

function invalid(message: string): NexusBffError {
  return new NexusBffError(400, "VALIDATION_ERROR", message);
}

function invalidResponse(): NexusBffError {
  return new NexusBffError(500, "INTERNAL_ERROR", "The request could not be completed.");
}

function isPlain(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isUtcIso(value: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$/.test(value)) return false;
  const parsed = new Date(value);
  return !Number.isNaN(parsed.valueOf()) && parsed.toISOString().slice(0, 19) === value.slice(0, 19);
}
