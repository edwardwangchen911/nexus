import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const { headersMock } = vi.hoisted(() => ({ headersMock: vi.fn() }));

vi.mock("next/headers", () => ({ headers: headersMock }));

import {
  requireNexusApiActor,
  requireNexusPageUser,
} from "../../../app/lib/auth/nexus-user";
import { withNexusBff } from "../../../app/lib/engine/with-nexus-bff";

beforeEach(() => {
  headersMock.mockReset();
  headersMock.mockResolvedValue(new Headers({ host: "localhost:3000" }));
  vi.unstubAllEnvs();
});

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("local-only Nexus identity", () => {
  it("uses one fixed audit identity for a loopback page request", async () => {
    await expect(requireNexusPageUser()).resolves.toEqual({
      userId: "nexus-local",
      displayName: "NEXUS Local",
      email: "local@nexus.invalid",
      fullName: "NEXUS Local",
    });
  });

  it.each([
    "http://localhost:3000/api/v1/example",
    "http://127.0.0.1:3000/api/v1/example",
    "http://[::1]:3000/api/v1/example",
  ])("permits the loopback BFF origin %s", async (url) => {
    await expect(requireNexusApiActor(new Request(url))).resolves.toMatchObject({
      userId: "nexus-local",
    });
  });

  it("rejects a non-loopback BFF origin", async () => {
    await expect(
      requireNexusApiActor(new Request("https://nexus.example/api/v1/example")),
    ).rejects.toEqual(
      expect.objectContaining({
        status: 403,
        message: "NEXUS is available only on this Mac.",
      }),
    );
  });

  it("returns a 403 response before a non-loopback BFF handler runs", async () => {
    const handler = vi.fn(async () => Response.json({}));

    const response = await withNexusBff(handler)(
      new Request("https://nexus.example/api/v1/example"),
    );

    expect(response.status).toBe(403);
    await expect(response.json()).resolves.toMatchObject({
      error: {
        code: "FORBIDDEN",
        message: "NEXUS is available only on this Mac.",
      },
    });
    expect(handler).not.toHaveBeenCalled();
  });
});
