import { validate as validateUuid } from "uuid";

import { engineFetch } from "../../../lib/engine/client";
import { NexusBffError } from "../../../lib/engine/errors";
import { withNexusBff } from "../../../lib/engine/with-nexus-bff";
import { sameUuid } from "../../../lib/optimization/uuid";
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
  const envelope = await upstream.json() as { data?: { run_id?: unknown; instrument_id?: unknown; data_snapshot_id?: unknown; decision_time?: unknown } };
  const runId = envelope.data?.run_id;
  if (typeof runId !== "string" || !validateUuid(runId)
    || !sameUuid(envelope.data?.instrument_id, body.value.instrument_id)
    || !sameUuid(envelope.data?.data_snapshot_id, body.value.data_snapshot_id)
    || typeof envelope.data?.decision_time !== "string"
    || Date.parse(envelope.data.decision_time) !== Date.parse(body.value.decision_time)) {
    throw new NexusBffError(500, "INTERNAL_ERROR", "The request could not be completed.");
  }
  return Response.json(envelope, {
    status: 202,
    headers: { location: `/api/v1/research-runs/${runId}` },
  });
});
