import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const { getChatGPTUserMock } = vi.hoisted(() => ({
  getChatGPTUserMock: vi.fn(),
}));

vi.mock("../../../app/chatgpt-auth", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../app/chatgpt-auth")>()),
  getChatGPTUser: getChatGPTUserMock,
}));

import { requireNexusApiActor } from "../../../app/lib/auth/nexus-user";
import { withNexusBff } from "../../../app/lib/engine/with-nexus-bff";

beforeEach(() => {
  getChatGPTUserMock.mockReset();
  vi.stubEnv("NEXUS_AUTH_MODE", "sites");
  vi.stubEnv("NEXUS_ENV", "production");
  vi.stubEnv("NEXUS_ALLOWED_USER_ID", "account-user-456");
});

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("requireNexusApiActor", () => {
  it("returns 401 when the authenticated Sites identity is missing", async () => {
    getChatGPTUserMock.mockResolvedValue(null);

    await expect(
      requireNexusApiActor(new Request("https://nexus.local/api/v1/example")),
    ).rejects.toEqual(
      expect.objectContaining({
        status: 401,
        message: "Authentication is required.",
      }),
    );
  });

  it("returns a 401 response for an anonymous API request", async () => {
    getChatGPTUserMock.mockResolvedValue(null);
    const handler = vi.fn(async () => Response.json({}));

    const response = await withNexusBff(handler)(
      new Request("https://nexus.local/api/v1/example"),
    );

    expect(response.status).toBe(401);
    await expect(response.json()).resolves.toMatchObject({
      error: {
        code: "UNAUTHENTICATED",
        message: "Authentication is required.",
      },
    });
    expect(handler).not.toHaveBeenCalled();
  });

  it("preserves the Sites user ID as the audit actor ID", async () => {
    getChatGPTUserMock.mockResolvedValue({
      userId: "site-user-123",
      displayName: "Owner",
      email: "owner@example.com",
      fullName: "Owner",
    });

    await expect(
      requireNexusApiActor(new Request("https://nexus.local/api/v1/example")),
    ).resolves.toMatchObject({ userId: "site-user-123" });
  });
});
