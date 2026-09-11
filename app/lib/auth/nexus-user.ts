import "server-only";

import { v7 as uuidv7 } from "uuid";
import { getChatGPTUser, requireNexusUser, type ChatGPTUser } from "../../chatgpt-auth";
import { assertAllowedUser } from "./allowed-user";

export type NexusActor = { userId: string; requestId: string };

export class NexusApiAuthError extends Error {
  constructor(readonly status: 401 | 403, message: string) {
    super(message);
  }
}

export async function requireNexusPageUser(returnTo: string): Promise<ChatGPTUser> {
  return requireNexusUser(returnTo);
}

export async function requireNexusApiActor(request: Request): Promise<NexusActor> {
  void request;
  const user = await getChatGPTUser();
  if (!user) throw new NexusApiAuthError(401, "Authentication is required.");
  assertAllowedUser(user.userId, process.env);
  return { userId: user.userId, requestId: uuidv7() };
}
