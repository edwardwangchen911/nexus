import { defineConfig } from "vitest/config";
import { fileURLToPath } from "node:url";

export default defineConfig({
  resolve: {
    alias: {
      "server-only": fileURLToPath(
        new URL("./node_modules/server-only/empty.js", import.meta.url),
      ),
      "next/headers": fileURLToPath(
        new URL("./node_modules/vinext/dist/shims/headers.js", import.meta.url),
      ),
      "next/navigation": fileURLToPath(
        new URL("./node_modules/vinext/dist/shims/navigation.js", import.meta.url),
      ),
    },
  },
  test: {
    environment: "jsdom",
    globals: true,
    include: ["tests/unit/**/*.test.{ts,tsx}"],
    setupFiles: ["./tests/setup.ts"],
  },
});
