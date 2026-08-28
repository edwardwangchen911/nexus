import { engineFetch } from "../../../../../../lib/engine/client";
import { withNexusBff } from "../../../../../../lib/engine/with-nexus-bff";
import { brokerResourceId, readReconciliationAcknowledge, requireBrokerIdempotencyKey } from "../../../../../../lib/broker-simulation/bff";

export const POST = withNexusBff(async (request, actor) => {
  const runId = brokerResourceId(request, "reconciliation", "acknowledge");
  return engineFetch({
    template: "/internal/v1/broker-simulation/reconciliation/{run_id}/acknowledge",
    path: `/internal/v1/broker-simulation/reconciliation/${runId}/acknowledge`,
  }, {
    method: "POST",
    actor,
    body: await readReconciliationAcknowledge(request),
    headers: {
      "content-type": "application/json",
      "idempotency-key": requireBrokerIdempotencyKey(request),
      "if-match": request.headers.get("if-match")!,
    },
  });
}, { requireIdempotencyKey: true, requireVersion: true });

