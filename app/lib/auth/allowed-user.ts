import { createHash, timingSafeEqual } from "node:crypto";

export type NexusAuthMode = "sites" | "dev";

function hashUserId(value: string): Buffer {
  return createHash("sha256").update(value, "utf8").digest();
}

export function isAllowedUser(userId: string, allowedUserId: string): boolean {
  return timingSafeEqual(hashUserId(userId), hashUserId(allowedUserId));
}

export function assertAllowedUser(userId: string | null, env: NodeJS.ProcessEnv): void {
  const mode = env.NEXUS_AUTH_MODE ?? "sites";
  const isProduction = env.NEXUS_ENV === "production";

  if (mode !== "sites" && mode !== "dev") {
    throw new Error("NEXUS_AUTH_MODE must be sites or dev");
  }
  if (mode === "dev") {
    if (isProduction) {
      throw new Error("development authentication is not permitted in production");
    }
    return;
  }

  const allowedUserId = env.NEXUS_ALLOWED_USER_ID;
  if (!allowedUserId) {
    throw new Error("NEXUS_ALLOWED_USER_ID must be configured for Sites authentication");
  }
  if (!userId || !isAllowedUser(userId, allowedUserId)) {
    throw new Error("NEXUS user is not allowed");
  }
}
