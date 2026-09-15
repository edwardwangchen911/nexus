import "server-only";

import { headers } from "next/headers";
import { v7 as uuidv7 } from "uuid";

export type NexusActor = { userId: string; requestId: string };
export type NexusLocalUser = {
  userId: string;
  displayName: string;
  email: string;
  fullName: string;
};

const LOCAL_USER: NexusLocalUser = {
  userId: "nexus-local",
  displayName: "NEXUS Local",
  email: "local@nexus.invalid",
  fullName: "NEXUS Local",
};

const LOOPBACK_HOSTNAMES = new Set(["localhost", "127.0.0.1", "[::1]", "::1"]);

export class NexusApiAuthError extends Error {
  constructor(readonly status: 401 | 403, message: string) {
    super(message);
  }
}

export async function requireNexusPageUser(): Promise<NexusLocalUser> {
  const requestHeaders = await headers();
  assertLoopbackHost(requestHeaders.get("host"));
  return LOCAL_USER;
}

export async function requireNexusApiActor(request: Request): Promise<NexusActor> {
  assertLoopbackHostname(new URL(request.url).hostname);
  return { userId: LOCAL_USER.userId, requestId: uuidv7() };
}

function assertLoopbackHost(host: string | null): void {
  if (!host) throw localOnlyError();
  try {
    assertLoopbackHostname(new URL(`http://${host}`).hostname);
  } catch (error) {
    if (error instanceof NexusApiAuthError) throw error;
    throw localOnlyError();
  }
}

function assertLoopbackHostname(hostname: string): void {
  if (!LOOPBACK_HOSTNAMES.has(hostname.toLowerCase())) throw localOnlyError();
}

function localOnlyError(): NexusApiAuthError {
  return new NexusApiAuthError(403, "NEXUS is available only on this Mac.");
}
