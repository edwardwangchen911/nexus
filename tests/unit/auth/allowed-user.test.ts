import { describe, expect, it } from "vitest";
import { assertAllowedUser } from "../../../app/lib/auth/allowed-user";

describe("assertAllowedUser", () => {
  it("permits an authenticated Sites user without an application allowlist", () => {
    expect(() =>
      assertAllowedUser("site-user-123", {
        NODE_ENV: "test",
        NEXUS_AUTH_MODE: "sites",
        NEXUS_ENV: "production",
      }),
    ).not.toThrow();
  });

  it("does not compare a Sites user ID with a legacy account user ID", () => {
    expect(() =>
      assertAllowedUser("site-user-123", {
        NODE_ENV: "production",
        NEXUS_AUTH_MODE: "sites",
        NEXUS_ENV: "production",
        NEXUS_ALLOWED_USER_ID: "account-user-456",
      }),
    ).not.toThrow();
  });

  it("rejects Sites authentication without a user ID", () => {
    expect(() =>
      assertAllowedUser(null, {
        NODE_ENV: "production",
        NEXUS_AUTH_MODE: "sites",
        NEXUS_ENV: "production",
      }),
    ).toThrow("authenticated Sites user ID");
  });

  it("permits development mode outside production", () => {
    expect(() =>
      assertAllowedUser(null, { ...process.env, NEXUS_AUTH_MODE: "dev", NEXUS_ENV: "development" }),
    ).not.toThrow();
  });

  it("rejects development mode in production", () => {
    expect(() =>
      assertAllowedUser("user-123", {
        ...process.env,
        NEXUS_AUTH_MODE: "dev",
        NEXUS_ENV: "production",
      }),
    ).toThrow("development authentication");
  });

  it("rejects development mode when NODE_ENV is production", () => {
    expect(() =>
      assertAllowedUser("user-123", {
        NEXUS_AUTH_MODE: "dev",
        NEXUS_ENV: "development",
        NODE_ENV: "production",
      }),
    ).toThrow("development authentication");
  });
});
