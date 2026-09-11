export type NexusAuthMode = "sites" | "dev";

export function assertAllowedUser(userId: string | null, env: NodeJS.ProcessEnv): void {
  const mode = env.NEXUS_AUTH_MODE ?? "sites";
  const isProduction = env.NEXUS_ENV === "production" || env.NODE_ENV === "production";

  if (mode !== "sites" && mode !== "dev") {
    throw new Error("NEXUS_AUTH_MODE must be sites or dev");
  }
  if (mode === "dev") {
    if (isProduction) {
      throw new Error("development authentication is not permitted in production");
    }
    return;
  }

  if (!userId) {
    throw new Error("Sites authentication requires an authenticated Sites user ID");
  }
}
