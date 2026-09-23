import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./tests/e2e-local",
  testMatch: "portfolio-controlled.spec.ts",
  fullyParallel: false,
  retries: 0,
  workers: 1,
  reporter: "list",
  use: {
    baseURL: process.env.NEXUS_CONTROLLED_WEB_URL,
    browserName: "chromium",
    channel: "chrome",
    viewport: { width: 1440, height: 900 },
    screenshot: "off",
    trace: "off",
    video: "off",
  },
});
