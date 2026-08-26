import { engineFetch } from "../../../../lib/engine/client";
import { withNexusBff } from "../../../../lib/engine/with-nexus-bff";
import { canonicalRiskAuditQuery } from "../../../../lib/risk-audit/bff";

export const GET = withNexusBff(async (request, actor) => {
  const query = canonicalRiskAuditQuery(new URL(request.url).searchParams, true);
  return engineFetch({
    template: "/internal/v1/risk-audit/export",
    path: "/internal/v1/risk-audit/export",
    query,
  }, {
    method: "GET",
    actor,
    headers: { accept: "application/x-ndjson" },
  });
}, { responseKind: "audit-export" });
