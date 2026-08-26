import { engineFetch } from "../../../../../lib/engine/client";
import { withNexusBff } from "../../../../../lib/engine/with-nexus-bff";
import {
  readEmptyRecoveryRequest,
  requireRecoveryHeaders,
  riskAuditJobId,
} from "../../../../../lib/risk-audit/bff";

export const POST = withNexusBff(async (request, actor) => {
  const jobId = riskAuditJobId(request);
  const body = await readEmptyRecoveryRequest(request);
  const headers = requireRecoveryHeaders(request);
  return engineFetch({
    template: "/internal/v1/jobs/{job_id}/retry",
    path: `/internal/v1/jobs/${jobId}/retry`,
  }, {
    method: "POST",
    actor,
    body,
    headers: {
      "content-type": "application/json",
      "idempotency-key": headers.idempotencyKey,
      "if-match": headers.ifMatch,
    },
  });
}, { requireIdempotencyKey: true, requireVersion: true });
