import { engineFetch } from "../../../lib/engine/client";
import { withNexusBff } from "../../../lib/engine/with-nexus-bff";
import { canonicalRiskAuditQuery } from "../../../lib/risk-audit/bff";

export const GET = withNexusBff(async (request, actor) => {
  const query = canonicalRiskAuditQuery(new URL(request.url).searchParams);
  return engineFetch({
    template: "/internal/v1/risk-audit",
    path: "/internal/v1/risk-audit",
    query,
  }, { method: "GET", actor });
});
