import { validate as validateUuid } from "uuid";

import { NexusBffError } from "../engine/errors";

const UTC_ISO = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$/;

export async function readResearchRequest(request: Request): Promise<{
  bytes: Uint8Array;
  value: { instrument_id: string; data_snapshot_id: string; decision_time: string };
}> {
  if (request.headers.get("content-type")?.split(";", 1)[0]?.trim().toLowerCase() !== "application/json") {
    throw invalid("The research request is invalid.");
  }
  const bytes = new Uint8Array(await request.arrayBuffer());
  let value: unknown;
  try { value = JSON.parse(new TextDecoder().decode(bytes)); } catch { throw invalid("The research request is invalid."); }
  if (!isPlain(value) || !hasExactKeys(value, ["instrument_id", "data_snapshot_id", "decision_time"])) {
    throw invalid("The research request is invalid.");
  }
  if (typeof value.instrument_id !== "string" || !validateUuid(value.instrument_id)
    || typeof value.data_snapshot_id !== "string" || !validateUuid(value.data_snapshot_id)
    || typeof value.decision_time !== "string" || !isUtcIso(value.decision_time)) {
    throw invalid("The research request is invalid.");
  }
  return { bytes, value: value as { instrument_id: string; data_snapshot_id: string; decision_time: string } };
}

export function requireResearchIdempotencyKey(request: Request): string {
  const value = request.headers.get("idempotency-key");
  if (!value || value.length > 255 || /[^\x21-\x7E]/.test(value)) {
    throw invalid("The idempotency key is invalid.");
  }
  return value;
}

export function routeUuid(request: Request, pattern: RegExp, message: string): string {
  const value = pattern.exec(new URL(request.url).pathname)?.[1];
  if (!value || !validateUuid(value)) throw invalid(message);
  return value;
}

export function canonicalResearchAsOf(request: Request): string {
  const url = new URL(request.url);
  const raw = url.search.startsWith("?") ? url.search.slice(1) : url.search;
  if (!raw || !hasValidPercentEscapes(raw)) throw invalid("The research query is invalid.");
  const entries = [...url.searchParams.entries()];
  if (entries.length !== 1 || entries[0][0] !== "as_of" || !isUtcIso(entries[0][1])) {
    throw invalid("The research query is invalid.");
  }
  return `as_of=${encodeURIComponent(entries[0][1])}`;
}

export function routeSessionDate(request: Request, pattern: RegExp): string {
  const value = pattern.exec(new URL(request.url).pathname)?.[1];
  if (!value || !isIsoDate(value)) throw invalid("The brief session date is invalid.");
  return value;
}

function isUtcIso(value: string): boolean {
  if (!UTC_ISO.test(value)) return false;
  const parsed = new Date(value);
  return !Number.isNaN(parsed.valueOf()) && parsed.toISOString().slice(0, 19) === value.slice(0, 19);
}

function isIsoDate(value: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const parsed = new Date(`${value}T00:00:00Z`);
  return !Number.isNaN(parsed.valueOf()) && parsed.toISOString().slice(0, 10) === value;
}

function hasValidPercentEscapes(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    if (value[index] === "%") {
      if (!/^[0-9a-fA-F]{2}$/.test(value.slice(index + 1, index + 3))) return false;
      index += 2;
    }
  }
  return true;
}

function hasExactKeys(value: Record<string, unknown>, keys: string[]): boolean {
  return Object.keys(value).length === keys.length && keys.every((key) => key in value);
}

function isPlain(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function invalid(message: string): NexusBffError {
  return new NexusBffError(400, "VALIDATION_ERROR", message);
}
