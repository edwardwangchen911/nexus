import { validate as validateUuid } from "uuid";

import { NexusBffError } from "../engine/errors";

type CanonicalBody = { bytes: Uint8Array; value: Record<string, unknown> };

export async function readPrepare(request: Request): Promise<Uint8Array> {
  const body = await readCanonicalJson(request);
  exactKeys(body.value, ["proposal_group_id", "proposal_id"]);
  for (const key of ["proposal_group_id", "proposal_id"] as const) {
    if (typeof body.value[key] !== "string" || !validateUuid(body.value[key])) invalid();
  }
  return body.bytes;
}

export async function readApproval(request: Request): Promise<Uint8Array> {
  const body = await readCanonicalJson(request);
  exactKeys(body.value, ["version"]);
  requirePositiveVersion(body.value.version);
  const authority = request.headers.get("if-match");
  if (!authority || !/^[0-9a-f]{64}$/.test(authority)) {
    throw new NexusBffError(400, "VALIDATION_ERROR", "If-Match must identify the immutable intent body.");
  }
  return body.bytes;
}

export async function readRejection(request: Request): Promise<Uint8Array> {
  const body = await readCanonicalJson(request);
  exactKeys(body.value, ["version", "reason"]);
  const version = requirePositiveVersion(body.value.version);
  if (body.value.reason !== "user_rejected" && body.value.reason !== "authority_changed") invalid();
  requireQuotedAuthority(request, String(version));
  return body.bytes;
}

export async function readOrderMutation(request: Request, action: "modify" | "cancel"): Promise<Uint8Array> {
  const body = await readCanonicalJson(request);
  exactKeys(body.value, ["expected_event_sequence", "reason"]);
  const sequence = body.value.expected_event_sequence;
  if (!Number.isSafeInteger(sequence) || (sequence as number) < 0) invalid();
  const reasons = action === "modify"
    ? new Set(["replace_price", "replace_quantity"])
    : new Set(["user_cancel", "risk_cancel"]);
  if (typeof body.value.reason !== "string" || !reasons.has(body.value.reason)) invalid();
  requireQuotedAuthority(request, String(sequence));
  return body.bytes;
}

export async function readReconciliationAcknowledge(request: Request): Promise<Uint8Array> {
  const body = await readCanonicalJson(request);
  exactKeys(body.value, []);
  const authority = request.headers.get("if-match");
  if (authority !== '"matched"' && authority !== '"conflict"') {
    throw new NexusBffError(400, "VALIDATION_ERROR", "If-Match must identify the reconciliation status.");
  }
  return body.bytes;
}

export function brokerResourceId(
  request: Request,
  resource: "intent" | "order" | "reconciliation",
  action: "approve" | "reject" | "modify" | "cancel" | "acknowledge",
): string {
  const segment = resource === "reconciliation" ? "reconciliation" : `${resource}s`;
  const match = new RegExp(`^/api/v1/broker-simulation/${segment}/([^/]+)/${action}$`).exec(new URL(request.url).pathname);
  const id = match?.[1];
  if (!id || !validateUuid(id)) invalid();
  return id;
}

export function requireBrokerIdempotencyKey(request: Request): string {
  const value = request.headers.get("idempotency-key");
  if (!value || value.length > 255 || /[^\x21-\x7E]/.test(value)) {
    throw new NexusBffError(400, "VALIDATION_ERROR", "The idempotency key is invalid.");
  }
  return value;
}

export function rejectBrokerQuery(request: Request): void {
  if (new URL(request.url).search !== "") invalid();
}

async function readCanonicalJson(request: Request): Promise<CanonicalBody> {
  if (request.headers.get("content-type")?.split(";", 1)[0]?.trim().toLowerCase() !== "application/json") invalid();
  let value: unknown;
  try { value = JSON.parse(await request.text()); } catch { invalid(); }
  if (!isPlain(value)) invalid();
  return { value, bytes: new TextEncoder().encode(JSON.stringify(value)) };
}

function exactKeys(value: Record<string, unknown>, keys: string[]): void {
  if (Object.keys(value).length !== keys.length || !keys.every((key) => key in value)) invalid();
}

function requirePositiveVersion(value: unknown): number {
  if (!Number.isSafeInteger(value) || (value as number) < 1) invalid();
  return value as number;
}

function requireQuotedAuthority(request: Request, expected: string): void {
  if (request.headers.get("if-match") !== `"${expected}"`) {
    throw new NexusBffError(400, "VALIDATION_ERROR", "If-Match does not match the current authority.");
  }
}

function invalid(): never {
  throw new NexusBffError(400, "VALIDATION_ERROR", "The broker simulation request is invalid.");
}

function isPlain(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

