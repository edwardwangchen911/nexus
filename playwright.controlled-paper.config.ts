import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./tests/e2e-local",
  testMatch: ["portfolio-controlled.spec.ts", "agent-paper-readonly.spec.ts"],
  fullyParallel: false,
  retries: 0,
  workers: 1,
  reporter: "list",
  use: {
    baseURL: process.env.NEXUS_CONTROLLED_WEB_URL,
    browserName: "chromium",
    channel: process.env.NEXUS_E2E_BROWSER_CHANNEL ?? "chrome",
    viewport: { width: 1440, height: 900 },
    screenshot: "off",
    trace: "off",
    video: "off",
  },
});
