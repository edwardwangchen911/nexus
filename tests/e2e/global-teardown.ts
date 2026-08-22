import { access, rm } from "node:fs/promises";

const markerPath = ".playwright-env-created";

export default async function globalTeardown() {
  try {
    await access(markerPath);
  } catch {
    return;
  }

  await rm(".env", { force: true });
  await rm(markerPath, { force: true });
}
