import { validate as validateUuid } from "uuid";

import { NexusBffError } from "../engine/errors";

const ORDER_STATUSES = new Set([
  "proposed",
  "risk_rejected",
  "risk_approved",
  "awaiting_approval",
  "submitted",
  "partially_filled",
  "filled",
  "cancelled",
  "rejected",
  "reconciling",
]);
const ORDER_QUERY_KEYS = new Set(["cursor", "limit", "status"]);
const CYCLE_QUERY_KEYS = new Set(["decision_session", "limit"]);

export function rejectQuery(request: Request, message: string): void {
  if ([...new URL(request.url).searchParams].length > 0) throw invalid(message);
}

export function canonicalPaperOrderQuery(search: URLSearchParams): string {
  const seen = new Set<string>();
  const entries: Array<[string, string]> = [];
  for (const [key, value] of search) {
    if (!ORDER_QUERY_KEYS.has(key) || seen.has(key) || !validOrderQueryValue(key, value)) {
      throw invalid("The paper order query is invalid.");
    }
    seen.add(key);
    entries.push([strictPercentEncode(key), strictPercentEncode(value)]);
  }
  entries.sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0);
  return entries.map(([key, value]) => `${key}=${value}`).join("&");
}

export function canonicalPaperCycleQuery(search: URLSearchParams): string {
  const seen = new Set<string>();
  const entries: Array<[string, string]> = [];
  for (const [key, value] of search) {
    if (!CYCLE_QUERY_KEYS.has(key) || seen.has(key) || !validCycleQueryValue(key, value)) {
      throw invalid("The Agent paper cycle query is invalid.");
    }
    seen.add(key);
    entries.push([strictPercentEncode(key), strictPercentEncode(value)]);
  }
  entries.sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0);
  return entries.map(([key, value]) => `${key}=${value}`).join("&");
}

export function paperOrderId(request: Request): string {
  const match = /^\/api\/v1\/paper\/orders\/([^/]+)\/cancel$/.exec(new URL(request.url).pathname);
  const value = match?.[1];
  if (!value || !validateUuid(value)) throw invalid("The paper order is invalid.");
  return value;
}

export function paperCycleId(request: Request): string {
  const match = /^\/api\/v1\/paper\/cycles\/([^/]+)$/.exec(new URL(request.url).pathname);
  const value = match?.[1];
  if (!value || !validateUuid(value)) throw invalid("The Agent paper cycle is invalid.");
  return value;
}

export function paperPlanId(request: Request): string {
  const match = /^\/api\/v1\/paper\/plans\/([^/]+)$/.exec(new URL(request.url).pathname);
  const value = match?.[1];
  if (!value || !validateUuid(value)) throw invalid("The paper plan is invalid.");
  return value;
}

export function paperCancelResultOrderId(request: Request): string {
  const match = /^\/api\/v1\/paper\/orders\/([^/]+)\/cancel-result$/.exec(new URL(request.url).pathname);
  const value = match?.[1];
  if (!value || !validateUuid(value)) throw invalid("The paper order is invalid.");
  return value;
}

export async function readPaperCancel(request: Request): Promise<Uint8Array> {
  if (request.headers.get("content-type")?.split(";", 1)[0]?.trim().toLowerCase() !== "application/json") {
    throw invalid("The paper cancellation request is invalid.");
  }
  let value: unknown;
  try { value = JSON.parse(await request.text()); } catch { throw invalid("The paper cancellation request is invalid."); }
  if (!isPlain(value)
    || Object.keys(value).length !== 1
    || !("expected_event_seq" in value)
    || !Number.isSafeInteger(value.expected_event_seq)
    || (value.expected_event_seq as number) < 1) {
    throw invalid("The paper cancellation request is invalid.");
  }
  return new TextEncoder().encode(JSON.stringify({ expected_event_seq: value.expected_event_seq }));
}

export function requirePaperIdempotencyKey(request: Request): string {
  const value = request.headers.get("idempotency-key");
  if (!value || value.length > 255 || /[^\x21-\x7E]/.test(value)) {
    throw invalid("The idempotency key is invalid.");
  }
  return value;
}

function validOrderQueryValue(key: string, value: string): boolean {
  if (key === "limit") return /^\d+$/.test(value) && Number(value) >= 1 && Number(value) <= 50;
  if (key === "status") return ORDER_STATUSES.has(value);
  if (key === "cursor") return value.length >= 1 && value.length <= 2048;
  return false;
}

function validCycleQueryValue(key: string, value: string): boolean {
  if (key === "limit") return /^\d+$/.test(value) && Number(value) >= 1 && Number(value) <= 20;
  if (key !== "decision_session" || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const parsed = new Date(`${value}T00:00:00Z`);
  return Number.isFinite(parsed.getTime()) && parsed.toISOString().slice(0, 10) === value;
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
