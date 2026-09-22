import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./tests/e2e-local",
  testMatch: "agent-paper-readonly.spec.ts",
  fullyParallel: false,
  retries: 0,
  workers: 1,
  reporter: "list",
  use: {
    baseURL: "http://127.0.0.1:3000",
    browserName: "chromium",
    channel: "chrome",
    viewport: { width: 1440, height: 900 },
    screenshot: "off",
    trace: "off",
    video: "off",
  },
});
