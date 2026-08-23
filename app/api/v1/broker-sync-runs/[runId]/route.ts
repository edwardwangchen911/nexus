import { validate as validateUuid } from "uuid";
import { engineFetch } from "../../../../lib/engine/client";
import { NexusBffError } from "../../../../lib/engine/errors";
import { withNexusBff } from "../../../../lib/engine/with-nexus-bff";

const PATH = /^\/api\/v1\/broker-sync-runs\/([^/]+)$/;

export const GET = withNexusBff(async (request, actor) => {
  const runId = PATH.exec(new URL(request.url).pathname)?.[1];
  if (!runId || !validateUuid(runId)) {
    throw new NexusBffError(400, "VALIDATION_ERROR", "The sync run is invalid.");
  }
  return engineFetch({
    template: "/internal/v1/broker-sync-runs/{run_id}",
    path: `/internal/v1/broker-sync-runs/${runId}`,
  }, { method: "GET", actor });
});
