import { validate as validateUuid } from "uuid";

import { NexusBffError } from "../engine/errors";

const AUDIT_KINDS = new Set(["quality", "risk", "agent_cost", "job", "reconciliation", "audit"]);
const AUDIT_QUERY_KEYS = new Set(["cursor", "from", "kind", "limit", "to"]);
const EXPORT_QUERY_KEYS = new Set(["from", "kind", "to"]);

export function rejectRiskAuditQuery(request: Request, message: string): void {
  if ([...new URL(request.url).searchParams].length > 0) throw invalid(message);
}

export function canonicalRiskAuditQuery(search: URLSearchParams, exportRequest = false): string {
  const allowedKeys = exportRequest ? EXPORT_QUERY_KEYS : AUDIT_QUERY_KEYS;
  const seen = new Set<string>();
  const values = new Map<string, string>();
  const entries: Array<[string, string]> = [];
  for (const [key, value] of search) {
    if (!allowedKeys.has(key) || seen.has(key) || !validAuditQueryValue(key, value)) {
      throw invalid(exportRequest ? "The audit export query is invalid." : "The risk-audit query is invalid.");
    }
    seen.add(key);
    values.set(key, value);
    entries.push([strictPercentEncode(key), strictPercentEncode(value)]);
  }
  if (exportRequest) {
    const from = values.get("from");
    const to = values.get("to");
    if (!from || !to) throw invalid("The audit export requires a bounded time range.");
    const duration = Date.parse(to) - Date.parse(from);
    if (duration <= 0 || duration > 31 * 24 * 60 * 60 * 1000) {
      throw invalid("The audit export range is invalid.");
    }
  }
  entries.sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0);
  return entries.map(([key, value]) => `${key}=${value}`).join("&");
}

export function riskAuditJobId(request: Request): string {
  const match = /^\/api\/v1\/jobs\/([^/]+)\/retry$/.exec(new URL(request.url).pathname);
  const value = match?.[1];
  if (!value || !validateUuid(value)) throw invalid("The recovery job is invalid.");
  return value;
}

export async function readEmptyRecoveryRequest(request: Request): Promise<Uint8Array> {
  const value = await readJson(request, "The recovery request is invalid.");
  if (!isPlain(value) || Object.keys(value).length !== 0) throw invalid("The recovery request is invalid.");
  return new TextEncoder().encode("{}");
}

export async function readDrawdownAcknowledge(request: Request): Promise<{ body: Uint8Array; eventSeq: number }> {
  const value = await readJson(request, "The drawdown acknowledgement is invalid.");
  if (!isPlain(value) || Object.keys(value).length !== 1
    || !Number.isSafeInteger(value.expected_event_seq)
    || Number(value.expected_event_seq) < 1) {
    throw invalid("The drawdown acknowledgement is invalid.");
  }
  const eventSeq = Number(value.expected_event_seq);
  return {
    body: new TextEncoder().encode(JSON.stringify({ expected_event_seq: eventSeq })),
    eventSeq,
  };
}

export function requireRecoveryHeaders(request: Request): { idempotencyKey: string; ifMatch: string; version: number } {
  const idempotencyKey = request.headers.get("idempotency-key");
  const ifMatch = request.headers.get("if-match");
  if (!idempotencyKey || idempotencyKey.length > 255 || /[^\x21-\x7E]/.test(idempotencyKey)) {
    throw invalid("The idempotency key is invalid.");
  }
  const match = /^"([1-9]\d*)"$/.exec(ifMatch ?? "");
  const version = match ? Number(match[1]) : Number.NaN;
  if (!Number.isSafeInteger(version)) throw invalid("The expected version is invalid.");
  return { idempotencyKey, ifMatch: `"${version}"`, version };
}

function validAuditQueryValue(key: string, value: string): boolean {
  if (key === "limit") return /^\d+$/.test(value) && Number(value) >= 1 && Number(value) <= 100;
  if (key === "kind") return AUDIT_KINDS.has(value);
  if (key === "cursor") return value.length >= 1 && value.length <= 2048;
  if (key === "from" || key === "to") return isUtcIso(value);
  return false;
}

async function readJson(request: Request, message: string): Promise<unknown> {
  if (request.headers.get("content-type")?.split(";", 1)[0]?.trim().toLowerCase() !== "application/json") {
    throw invalid(message);
  }
  try { return JSON.parse(await request.text()); } catch { throw invalid(message); }
}

function isUtcIso(value: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$/.test(value)) return false;
  const date = new Date(value);
  return !Number.isNaN(date.valueOf()) && date.toISOString().slice(0, 19) === value.slice(0, 19);
}

function strictPercentEncode(value: string): string {
  return encodeURIComponent(value).replace(/[!'()*]/g, (character) =>
    `%${character.charCodeAt(0).toString(16).toUpperCase()}`,
  );
}

function invalid(message: string): NexusBffError {
  return new NexusBffError(400, "VALIDATION_ERROR", message);
}

function isPlain(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
