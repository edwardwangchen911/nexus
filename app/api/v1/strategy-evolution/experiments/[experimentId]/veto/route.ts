import { engineFetch } from "../../../../../../lib/engine/client";
import { withNexusBff } from "../../../../../../lib/engine/with-nexus-bff";
import { evolutionExperimentId, readVeto, requireEvolutionWriteHeaders } from "../../../../../../lib/strategy-evolution/bff";

export const POST = withNexusBff(async (request, actor) => {
  const experimentId = evolutionExperimentId(request, true);
  const body = await readVeto(request);
  const headers = requireEvolutionWriteHeaders(request);
  return engineFetch({
    template: "/internal/v1/strategy-experiments/{experiment_id}/veto",
    path: `/internal/v1/strategy-experiments/${experimentId}/veto`,
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
