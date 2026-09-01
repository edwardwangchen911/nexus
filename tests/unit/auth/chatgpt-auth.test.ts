import { describe, expect, it } from "vitest";
import { localDevelopmentUser } from "../../../app/chatgpt-auth";

describe("localDevelopmentUser", () => {
  it("provides a fixed local identity in non-production dev mode", () => {
    expect(
      localDevelopmentUser({
        NODE_ENV: "test",
        NEXUS_AUTH_MODE: "dev",
        NEXUS_ENV: "development",
      }),
    ).toEqual({
      userId: "nexus-local-dev",
      displayName: "NEXUS Local",
      email: "local@nexus.invalid",
      fullName: "NEXUS Local",
    });
  });

  it("stays disabled for Sites authentication", () => {
    expect(
      localDevelopmentUser({
        NODE_ENV: "test",
        NEXUS_AUTH_MODE: "sites",
        NEXUS_ENV: "development",
      }),
    ).toBeNull();
  });

  it("stays disabled in production", () => {
    expect(
      localDevelopmentUser({
        NODE_ENV: "test",
        NEXUS_AUTH_MODE: "dev",
        NEXUS_ENV: "production",
      }),
    ).toBeNull();
  });
});
