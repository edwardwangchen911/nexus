import { engineFetch } from "../../../lib/engine/client";
import { withNexusBff } from "../../../lib/engine/with-nexus-bff";
import { rejectRiskAuditQuery } from "../../../lib/risk-audit/bff";

export const GET = withNexusBff(async (request, actor) => {
  rejectRiskAuditQuery(request, "The readiness query is invalid.");
  return engineFetch("/health/ready", { method: "GET", actor });
});
