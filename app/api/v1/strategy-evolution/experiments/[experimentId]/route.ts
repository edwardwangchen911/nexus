import { engineFetch } from "../../../../../lib/engine/client";
import { withNexusBff } from "../../../../../lib/engine/with-nexus-bff";
import { evolutionExperimentId } from "../../../../../lib/strategy-evolution/bff";

export const GET = withNexusBff(async (request, actor) => {
  const experimentId = evolutionExperimentId(request);
  return engineFetch({
    template: "/internal/v1/strategy-experiments/{experiment_id}",
    path: `/internal/v1/strategy-experiments/${experimentId}`,
  }, { method: "GET", actor, headers: { accept: "application/json" } });
});
