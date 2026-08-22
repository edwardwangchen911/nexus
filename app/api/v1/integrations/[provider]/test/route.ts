import { engineFetch } from "../../../../../lib/engine/client";
import { NexusBffError } from "../../../../../lib/engine/errors";
import { withNexusBff } from "../../../../../lib/engine/with-nexus-bff";
import { forwardedIntegrationHeaders, integrationProviderFromRequest } from "../../../../../lib/integrations/providers";

export const POST = withNexusBff(async (request, actor) => {
  const provider = integrationProviderFromRequest(request, "test");
  if (!provider) throw new NexusBffError(400, "VALIDATION_ERROR", "The provider is invalid.");
  const body = new Uint8Array(await request.arrayBuffer());
  return engineFetch(
    {
      template: "/internal/v1/integrations/{provider}/test",
      path: `/internal/v1/integrations/${provider}/test`,
    },
    { method: "POST", actor, body, headers: forwardedIntegrationHeaders(request) },
  );
}, { requireVersion: true });
