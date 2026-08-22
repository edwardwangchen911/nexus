import type { components } from "../generated/schema";

export type IntegrationStatus = components["schemas"]["IntegrationStatusResponse"];
export type IntegrationProvider = IntegrationStatus["provider"];
export type IntegrationListEnvelope = components["schemas"]["IntegrationStatusListEnvelope"];
export type IntegrationMutationEnvelope = components["schemas"]["IntegrationModeChangeResponse"];

export const integrationProviders = ["fosun", "alpaca", "sec", "fred", "openai"] as const satisfies readonly IntegrationProvider[];
type MissingProvider = Exclude<IntegrationProvider, (typeof integrationProviders)[number]>;
const providerListIsExhaustive: MissingProvider extends never ? true : never = true;
void providerListIsExhaustive;

export function integrationProviderFromRequest(request: Request, action: "test" | "mode"): IntegrationProvider | null {
  const match = new RegExp(`^/api/v1/integrations/([^/]+)/${action}$`).exec(new URL(request.url).pathname);
  return match && integrationProviders.includes(match[1] as IntegrationProvider)
    ? (match[1] as IntegrationProvider)
    : null;
}

export function forwardedIntegrationHeaders(request: Request): Record<string, string> {
  const headers: Record<string, string> = {};
  for (const name of ["content-type", "idempotency-key", "if-match"] as const) {
    const value = request.headers.get(name);
    if (value !== null) headers[name] = value;
  }
  return headers;
}
