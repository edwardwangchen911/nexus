import "server-only";

import { validate as validateUuid } from "uuid";

import { NexusBffError } from "../engine/errors";

const STRONG_REGISTRY_ETAG = /^"g[1-9]\d*-v[1-9]\d*"$/;
const CONTROL_ACTIONS = new Set(["pause_auto_promotion", "resume_auto_promotion"]);

export function canonicalEvolutionQuery(search: URLSearchParams): string {
  const seen = new Set<string>();
  const entries: Array<[string, string]> = [];
  for (const [key, value] of search) {
    if (!new Set(["cursor", "limit"]).has(key) || seen.has(key)) throw invalid("The strategy evolution query is invalid.");
    if (key === "limit" && (!/^\d+$/.test(value) || Number(value) < 1 || Number(value) > 100)) throw invalid("The strategy evolution query is invalid.");
    if (key === "cursor" && (value.length < 1 || value.length > 2048)) throw invalid("The strategy evolution query is invalid.");
    seen.add(key);
    entries.push([strictPercentEncode(key), strictPercentEncode(value)]);
  }
  entries.sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0);
  return entries.map(([key, value]) => `${key}=${value}`).join("&");
}

export async function readStartExperiment(request: Request): Promise<Uint8Array> {
  const value = await readJsonObject(request, "The experiment request is invalid.");
  requireExactKeys(value, []);
  return bytes({});
}

export async function readVeto(request: Request): Promise<Uint8Array> {
  const value = await readJsonObject(request, "The experiment veto is invalid.");
  requireExactKeys(value, ["reason"]);
  if (value.reason !== "USER_VETO") throw invalid("The experiment veto is invalid.");
  return bytes({ reason: "USER_VETO" });
}

export async function readRegistryControl(request: Request): Promise<{ body: Uint8Array; action: "pause_auto_promotion" | "resume_auto_promotion" }> {
  const value = await readJsonObject(request, "The registry control is invalid.");
  requireExactKeys(value, ["action"]);
  if (typeof value.action !== "string" || !CONTROL_ACTIONS.has(value.action)) throw invalid("The registry control is invalid.");
  const action = value.action as "pause_auto_promotion" | "resume_auto_promotion";
  return { body: bytes({ action }), action };
}

export async function readRollback(request: Request): Promise<Uint8Array> {
  const value = await readJsonObject(request, "The registry rollback is invalid.");
  requireExactKeys(value, ["reason"]);
  if (value.reason !== "USER_REQUESTED") throw invalid("The registry rollback is invalid.");
  return bytes({ reason: "USER_REQUESTED" });
}

export function evolutionExperimentId(request: Request, veto = false): string {
  const suffix = veto ? "/veto" : "";
  const match = new RegExp(`^/api/v1/strategy-evolution/experiments/([^/]+)${suffix}$`).exec(new URL(request.url).pathname);
  const value = match?.[1];
  if (!value || !validateUuid(value)) throw invalid("The strategy experiment is invalid.");
  return value;
}

export function requireEvolutionWriteHeaders(request: Request): { idempotencyKey: string; ifMatch: string } {
  const idempotencyKey = request.headers.get("idempotency-key");
  const ifMatch = request.headers.get("if-match");
  if (!idempotencyKey || idempotencyKey.length > 255 || /[^\x21-\x7E]/.test(idempotencyKey)) {
    throw invalid("The idempotency key is invalid.");
  }
  if (!ifMatch || !STRONG_REGISTRY_ETAG.test(ifMatch)) throw invalid("The registry version is invalid.");
  return { idempotencyKey, ifMatch };
}

export function rewriteEvolutionLocation(response: Response): Response {
  if (response.status !== 202) return response;
  const headers = new Headers({
    "content-type": response.headers.get("content-type") ?? "application/json",
    location: "/api/v1/strategy-evolution",
  });
  const etag = response.headers.get("etag");
  if (etag) headers.set("etag", etag);
  return new Response(response.body, { status: 202, headers });
}

async function readJsonObject(request: Request, message: string): Promise<Record<string, unknown>> {
  if (request.headers.get("content-type")?.split(";", 1)[0]?.trim().toLowerCase() !== "application/json") throw invalid(message);
  let value: unknown;
  try { value = JSON.parse(await request.text()); } catch { throw invalid(message); }
  if (!isPlain(value)) throw invalid(message);
  return value;
}

function requireExactKeys(value: Record<string, unknown>, keys: string[]): void {
  if (Object.keys(value).length !== keys.length || !keys.every((key) => key in value)) throw invalid("The request body is invalid.");
}

function bytes(value: Record<string, string>): Uint8Array {
  return new TextEncoder().encode(JSON.stringify(value));
}

function strictPercentEncode(value: string): string {
  return encodeURIComponent(value).replace(/[!'()*]/g, (character) => `%${character.charCodeAt(0).toString(16).toUpperCase()}`);
}

function invalid(message: string): NexusBffError {
  return new NexusBffError(400, "VALIDATION_ERROR", message);
}

function isPlain(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
