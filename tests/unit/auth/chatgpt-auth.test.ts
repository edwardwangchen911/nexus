import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const { headersMock, redirectMock } = vi.hoisted(() => ({
  headersMock: vi.fn(),
  redirectMock: vi.fn(),
}));

vi.mock("next/headers", () => ({ headers: headersMock }));
vi.mock("next/navigation", () => ({ redirect: redirectMock }));

import {
  localDevelopmentUser,
  requireNexusUser,
} from "../../../app/chatgpt-auth";

beforeEach(() => {
  headersMock.mockReset();
  redirectMock.mockReset();
  vi.stubEnv("NEXUS_AUTH_MODE", "sites");
  vi.stubEnv("NEXUS_ENV", "production");
  vi.stubEnv("NEXUS_ALLOWED_USER_ID", "account-user-456");
});

afterEach(() => {
  vi.unstubAllEnvs();
});

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

  it("stays disabled when NODE_ENV is production", () => {
    expect(
      localDevelopmentUser({
        NODE_ENV: "production",
        NEXUS_AUTH_MODE: "dev",
        NEXUS_ENV: "development",
      }),
    ).toBeNull();
  });
});

describe("requireNexusUser", () => {
  it("permits a complete Sites identity without comparing email or a legacy account ID", async () => {
    headersMock.mockResolvedValue(
      new Headers({
        "oai-authenticated-user-id": "site-user-123",
        "oai-authenticated-user-email": "owner@example.com",
      }),
    );

    await expect(requireNexusUser("/overview")).resolves.toMatchObject({
      userId: "site-user-123",
      email: "owner@example.com",
    });
  });

  it("redirects an anonymous page request to ChatGPT sign-in", async () => {
    headersMock.mockResolvedValue(new Headers());
    redirectMock.mockImplementation((path: string) => {
      throw new Error(`redirect:${path}`);
    });

    await expect(requireNexusUser("/overview")).rejects.toThrow(
      "redirect:/signin-with-chatgpt?return_to=%2Foverview",
    );
  });
});
