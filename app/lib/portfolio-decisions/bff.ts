import { validate as validateUuid } from "uuid";

import { NexusBffError } from "../engine/errors";
import {
  readExactJson,
  requireIdempotencyKey,
  requirePortfolioScenarioRequest,
  type ExactJson,
} from "../optimization/bff";
import { sameUuid } from "../optimization/uuid";

const HASH64 = /^[0-9a-f]{64}$/;
const RUN_PATH = /^\/api\/v1\/portfolio-decision-runs\/([^/]+)$/;
const LATEST_PATH = "/api/v1/portfolio-decision-runs/latest";
const SCOPE_KEYS = [
  "portfolio_snapshot_id",
  "diagnostic_report_id",
  "expected_scenario_result_hash",
] as const;

export type PortfolioDecisionScope = {
  portfolioSnapshotId: string;
  diagnosticReportId: string;
  expectedScenarioResultHash: string;
};

export async function readPortfolioDecisionCreate(request: Request): Promise<ExactJson> {
  const body = await readExactJson(request, "The portfolio decision request is invalid.");
  requireExactKeys(body.value, [
    "schema_version",
    "portfolio_snapshot_id",
    "diagnostic_report_id",
    "expected_scenario_result_hash",
    "scenario_request",
  ]);
  if (body.value.schema_version !== "portfolio_decision_create_v1"
    || typeof body.value.portfolio_snapshot_id !== "string"
    || !validateUuid(body.value.portfolio_snapshot_id)
    || typeof body.value.diagnostic_report_id !== "string"
    || !validateUuid(body.value.diagnostic_report_id)
    || typeof body.value.expected_scenario_result_hash !== "string"
    || !HASH64.test(body.value.expected_scenario_result_hash)
    || !isPlain(body.value.scenario_request)) {
    throw invalid("The portfolio decision request is invalid.");
  }
  requirePortfolioScenarioRequest(body.value.scenario_request);
  if (!sameUuid(body.value.portfolio_snapshot_id, body.value.scenario_request.portfolio_snapshot_id)
    || !sameUuid(body.value.diagnostic_report_id, body.value.scenario_request.diagnostic_report_id)) {
    throw invalid("The portfolio decision request scope is invalid.");
  }
  return body;
}

export function decisionIdempotencyKey(request: Request): string {
  return requireIdempotencyKey(request);
}

export function readPortfolioDecisionScope(request: Request): {
  runId: string;
  scope: PortfolioDecisionScope;
} {
  const url = new URL(request.url);
  const runId = RUN_PATH.exec(url.pathname)?.[1];
  if (!runId || !validateUuid(runId)) throw invalid("The portfolio decision run is invalid.");
  return { runId, scope: readScope(url) };
}

export function readLatestPortfolioDecisionScope(request: Request): PortfolioDecisionScope {
  const url = new URL(request.url);
  if (url.pathname !== LATEST_PATH) throw invalid("The portfolio decision run is invalid.");
  return readScope(url);
}

function readScope(url: URL): PortfolioDecisionScope {
  const names = [...url.searchParams.keys()];
  if (names.length !== SCOPE_KEYS.length
    || new Set(names).size !== SCOPE_KEYS.length
    || !SCOPE_KEYS.every((key) => names.includes(key))) {
    throw invalid("The portfolio decision scope is invalid.");
  }
  const portfolioSnapshotId = url.searchParams.get(SCOPE_KEYS[0]);
  const diagnosticReportId = url.searchParams.get(SCOPE_KEYS[1]);
  const expectedScenarioResultHash = url.searchParams.get(SCOPE_KEYS[2]);
  if (!portfolioSnapshotId || !validateUuid(portfolioSnapshotId)
    || !diagnosticReportId || !validateUuid(diagnosticReportId)
    || !expectedScenarioResultHash || !HASH64.test(expectedScenarioResultHash)) {
    throw invalid("The portfolio decision scope is invalid.");
  }
  return { portfolioSnapshotId, diagnosticReportId, expectedScenarioResultHash };
}

export function requirePortfolioDecisionResponse(
  value: unknown,
  scope: PortfolioDecisionScope,
  runId?: string,
): asserts value is { data: Record<string, unknown>; meta: Record<string, unknown> } {
  if (!isPlain(value) || !isPlain(value.data) || !isPlain(value.meta)
    || typeof value.data.run_id !== "string" || !validateUuid(value.data.run_id)
    || runId !== undefined && !sameUuid(value.data.run_id, runId)
    || !sameUuid(value.data.portfolio_snapshot_id, scope.portfolioSnapshotId)
    || !sameUuid(value.data.diagnostic_report_id, scope.diagnosticReportId)
    || value.data.expected_scenario_result_hash !== scope.expectedScenarioResultHash
    || value.data.execution_locked !== true
    || !decisionStateValid(value.data, scope.expectedScenarioResultHash)) {
    throw new NexusBffError(500, "INTERNAL_ERROR", "The request could not be completed.");
  }
}

function decisionStateValid(data: Record<string, unknown>, expectedHash: string): boolean {
  if (data.status === "prepared" || data.status === "running") return data.output === null;
  if (!isPlain(data.output) || data.output.status !== data.status) return false;
  if (data.status === "completed") {
    return data.output.scenario_result_hash === expectedHash && isPlain(data.output.intent);
  }
  return (data.status === "abstained" || data.status === "failed")
    && data.output.scenario_result_hash === null
    && data.output.intent === null;
}

function requireExactKeys(value: Record<string, unknown>, keys: string[]): void {
  if (Object.keys(value).length !== keys.length || !keys.every((key) => key in value)) {
    throw invalid("The portfolio decision request is invalid.");
  }
}

function isPlain(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function invalid(message: string): NexusBffError {
  return new NexusBffError(400, "VALIDATION_ERROR", message);
}
