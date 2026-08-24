import { validate as validateUuid } from "uuid";

import { engineFetch } from "../../../lib/engine/client";
import { NexusBffError } from "../../../lib/engine/errors";
import { withNexusBff } from "../../../lib/engine/with-nexus-bff";
import { readExactJson, requireIdempotencyKey, requireProposalAdmission } from "../../../lib/optimization/bff";

export const POST = withNexusBff(async (request, actor) => {
  const body = await readExactJson(request, "The proposal request is invalid.");
  requireProposalAdmission(body.value);
  const upstream = await engineFetch("/internal/v1/optimization-proposals", {
    method: "POST",
    actor,
    body: body.bytes,
    headers: {
      "content-type": "application/json",
      "idempotency-key": requireIdempotencyKey(request),
    },
  });
  if (upstream.status !== 202) return upstream;
  const envelope = await upstream.json() as { data?: { job_id?: unknown } };
  const jobId = envelope.data?.job_id;
  if (typeof jobId !== "string" || !validateUuid(jobId)) {
    throw new NexusBffError(500, "INTERNAL_ERROR", "The request could not be completed.");
  }
  return Response.json(envelope, { status: 202, headers: { location: `/api/v1/jobs/${jobId}` } });
});
