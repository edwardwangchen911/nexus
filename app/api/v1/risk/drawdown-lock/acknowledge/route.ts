import { engineFetch } from "../../../../../lib/engine/client";
import { NexusBffError } from "../../../../../lib/engine/errors";
import { withNexusBff } from "../../../../../lib/engine/with-nexus-bff";
import {
  readDrawdownAcknowledge,
  requireRecoveryHeaders,
} from "../../../../../lib/risk-audit/bff";

export const POST = withNexusBff(async (request, actor) => {
  const { body, eventSeq } = await readDrawdownAcknowledge(request);
  const headers = requireRecoveryHeaders(request);
  if (headers.version !== eventSeq) {
    throw new NexusBffError(400, "VALIDATION_ERROR", "If-Match must equal the expected drawdown event sequence.");
  }
  return engineFetch("/internal/v1/risk/drawdown-lock/acknowledge", {
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
