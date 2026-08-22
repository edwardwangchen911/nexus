import { describe, expect, it } from "vitest";
import { assertAllowedUser, isAllowedUser } from "../../../app/lib/auth/allowed-user";

describe("isAllowedUser", () => {
  it("accepts the configured user ID", () => {
    expect(isAllowedUser("user-123", "user-123")).toBe(true);
  });

  it("rejects a different user ID", () => {
    expect(isAllowedUser("user-123", "user-456")).toBe(false);
  });

  it("rejects IDs with unequal lengths", () => {
    expect(isAllowedUser("user-123", "user-1234")).toBe(false);
  });
});

describe("assertAllowedUser", () => {
  it("rejects production when no allowlist is configured", () => {
    expect(() =>
      assertAllowedUser("user-123", {
        NODE_ENV: "test",
        NEXUS_AUTH_MODE: "sites",
        NEXUS_ENV: "production",
      }),
    ).toThrow("NEXUS_ALLOWED_USER_ID");
  });

  it("permits development mode outside production", () => {
    expect(() =>
      assertAllowedUser(null, { ...process.env, NEXUS_AUTH_MODE: "dev", NEXUS_ENV: "development" }),
    ).not.toThrow();
  });

  it("rejects development mode in production", () => {
    expect(() =>
      assertAllowedUser("user-123", { ...process.env,
        NEXUS_AUTH_MODE: "dev",
        NEXUS_ENV: "production",
      }),
    ).toThrow("development authentication");
  });
});
