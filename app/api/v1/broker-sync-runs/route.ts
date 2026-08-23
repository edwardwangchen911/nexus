import { validate as validateUuid } from "uuid";
import { engineFetch } from "../../../lib/engine/client";
import { NexusBffError } from "../../../lib/engine/errors";
import { withNexusBff } from "../../../lib/engine/with-nexus-bff";

export const POST = withNexusBff(async (request, actor) => {
  const bytes = new Uint8Array(await request.arrayBuffer());
  const contentType = request.headers.get("content-type")?.split(";", 1)[0]?.trim().toLowerCase();
  if (contentType !== "application/json") {
    throw new NexusBffError(400, "VALIDATION_ERROR", "The sync request is invalid.");
  }
  let value: unknown;
  try { value = JSON.parse(new TextDecoder().decode(bytes)); } catch {
    throw new NexusBffError(400, "VALIDATION_ERROR", "The sync request is invalid.");
  }
  if (!isSyncBody(value)) {
    throw new NexusBffError(400, "VALIDATION_ERROR", "The sync request is invalid.");
  }
  const idempotencyKey = request.headers.get("idempotency-key");
  if (!idempotencyKey || idempotencyKey.length > 255 || /[^\x21-\x7E]/.test(idempotencyKey)) {
    throw new NexusBffError(400, "VALIDATION_ERROR", "The idempotency key is invalid.");
  }
  const upstream = await engineFetch("/internal/v1/broker-sync-runs", {
    method: "POST",
    actor,
    body: bytes,
    headers: { "content-type": "application/json", "idempotency-key": idempotencyKey },
  });
  if (upstream.status !== 202) return upstream;
  const envelope = await upstream.json() as { data?: { broker_sync_run_id?: unknown } };
  const runId = envelope.data?.broker_sync_run_id;
  if (typeof runId !== "string" || !validateUuid(runId)) {
    throw new NexusBffError(500, "INTERNAL_ERROR", "The request could not be completed.");
  }
  return Response.json(envelope, {
    status: 202,
    headers: { location: `/api/v1/broker-sync-runs/${runId}` },
  });
});

function isSyncBody(value: unknown): value is { portfolio_id: string } {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    && Object.keys(value).length === 1
    && "portfolio_id" in value
    && typeof value.portfolio_id === "string"
    && validateUuid(value.portfolio_id);
}
