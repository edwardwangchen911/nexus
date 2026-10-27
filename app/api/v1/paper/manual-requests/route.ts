import { engineFetch } from "../../../../lib/engine/client";
import { withNexusBff } from "../../../../lib/engine/with-nexus-bff";
import { NexusBffError } from "../../../../lib/engine/errors";
import { rejectQuery, requirePaperIdempotencyKey } from "../../../../lib/paper/bff";

export const GET = withNexusBff(async (request, actor) => {
  rejectQuery(request, "The manual request lookup is invalid.");
  return engineFetch("/internal/v1/paper/manual-requests", { method: "GET", actor, headers: { "idempotency-key": requirePaperIdempotencyKey(request) } });
});
export const POST = withNexusBff(async (request, actor) => {
  rejectQuery(request, "The manual request is invalid.");
  let body: unknown;
  try { body = await request.json(); } catch { throw new NexusBffError(400, "VALIDATION_ERROR", "The manual request is invalid."); }
  if (request.headers.get("content-type")?.split(";", 1)[0].trim().toLowerCase() !== "application/json"
    || !body || typeof body !== "object" || Array.isArray(body)
    || Object.keys(body).length !== 1 || !("mode" in body)
    || !["research_only", "research_and_paper", "research_and_monitor"].includes(String(body.mode))) {
    throw new NexusBffError(400, "VALIDATION_ERROR", "The manual request is invalid.");
  }
  return engineFetch("/internal/v1/paper/manual-requests", { method: "POST", actor,
    body: new TextEncoder().encode(JSON.stringify(body)),
    headers: { "content-type": "application/json", "idempotency-key": requirePaperIdempotencyKey(request) },
  });
});
