import { validate as validateUuid } from "uuid";

import { engineFetch } from "../../../lib/engine/client";
import { NexusBffError } from "../../../lib/engine/errors";
import { withNexusBff } from "../../../lib/engine/with-nexus-bff";
import { readResearchRequest, requireResearchIdempotencyKey } from "../../../lib/research/bff";

export const POST = withNexusBff(async (request, actor) => {
  const body = await readResearchRequest(request);
  const upstream = await engineFetch("/internal/v1/research-runs", {
    method: "POST",
    actor,
    body: body.bytes,
    headers: {
      "content-type": "application/json",
      "idempotency-key": requireResearchIdempotencyKey(request),
    },
  });
  if (upstream.status !== 202) return upstream;
  const envelope = await upstream.json() as { data?: { run_id?: unknown } };
  const runId = envelope.data?.run_id;
  if (typeof runId !== "string" || !validateUuid(runId)) {
    throw new NexusBffError(500, "INTERNAL_ERROR", "The request could not be completed.");
  }
  return Response.json(envelope, {
    status: 202,
    headers: { location: `/api/v1/research-runs/${runId}` },
  });
});
