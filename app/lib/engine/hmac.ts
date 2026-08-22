import "server-only";

import { createHash, createHmac } from "node:crypto";

export type EngineHmacInput = {
  keyId: string;
  secret: string;
  method: string;
  pathAndQuery: string;
  timestamp: number;
  nonce: string;
  actorId: string;
  requestId: string;
  body: Uint8Array;
};

export function createEngineHmacHeaders(input: EngineHmacInput): Record<string, string> {
  const bodyHash = createHash("sha256").update(input.body).digest("hex");
  const canonical = [
    input.method.toUpperCase(),
    normalizePathAndQuery(input.pathAndQuery),
    String(input.timestamp),
    input.nonce,
    bodyHash,
  ].join("\n");
  const signature = createHmac("sha256", input.secret)
    .update(canonical, "utf8")
    .digest("base64url");
  return {
    "X-Nexus-Key-Id": input.keyId,
    "X-Nexus-Timestamp": String(input.timestamp),
    "X-Nexus-Nonce": input.nonce,
    "X-Nexus-Body-SHA256": bodyHash,
    "X-Nexus-Signature": signature,
    "X-Nexus-Actor-Id": input.actorId,
    "X-Request-Id": input.requestId,
  };
}

function normalizePathAndQuery(pathAndQuery: string): string {
  const questionMark = pathAndQuery.indexOf("?");
  const rawPath = questionMark === -1 ? pathAndQuery : pathAndQuery.slice(0, questionMark);
  if (!rawPath.startsWith("/")) throw new Error("engine paths must be absolute");
  const rawQuery = questionMark === -1 ? "" : pathAndQuery.slice(questionMark + 1);
  const pairs = Array.from(new URLSearchParams(rawQuery), ([key, value]) => [
    strictPercentEncode(key),
    strictPercentEncode(value),
  ] as const).sort(([leftKey, leftValue], [rightKey, rightValue]) => {
    const keyOrder = Buffer.compare(Buffer.from(leftKey), Buffer.from(rightKey));
    return keyOrder || Buffer.compare(Buffer.from(leftValue), Buffer.from(rightValue));
  });
  return pairs.length === 0
    ? rawPath
    : `${rawPath}?${pairs.map(([key, value]) => `${key}=${value}`).join("&")}`;
}

function strictPercentEncode(value: string): string {
  return encodeURIComponent(value).replace(/[!'()*]/g, (character) =>
    `%${character.charCodeAt(0).toString(16).toUpperCase()}`,
  );
}
