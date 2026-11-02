import { createRequire } from "node:module";
import { readFileSync } from "node:fs";
import { expect, it } from "vitest";

const fromRoot = createRequire(import.meta.url);
const legacyConsumers = [
  "@eslint/config-array", "@eslint/eslintrc", "@redocly/openapi-core",
  "eslint", "eslint-plugin-jsx-a11y", "eslint-plugin-react",
];

it("runs legacy glob calls through the installed upstream matcher", () => {
  const adapter = createRequire(fromRoot.resolve("minimatch"));
  const upstream = adapter("upstream-minimatch");
  const metadata = adapter("upstream-minimatch/package.json");
  expect([metadata.name, metadata.version]).toEqual(["minimatch", "11.0.1"]);
  for (const consumer of legacyConsumers) {
    const load = createRequire(fromRoot.resolve(consumer));
    const match = load("minimatch");
    expect(match).toBe(upstream.minimatch);
    expect(new match.Minimatch("**/*.{js,ts}").match("src/lib.ts")).toBe(true);
    expect(match("LabelField", "Label*")).toBe(true);
    expect(match("example.local/api/schema", "*.local/api/**")).toBe(true);
    expect(match("other.local/private/schema", "*.local/api/**")).toBe(false);
  }
  const parser = createRequire(fromRoot.resolve("@typescript-eslint/typescript-estree"));
  expect(parser("minimatch/package.json").version).toBe(metadata.version);
  expect(typeof parser("minimatch").minimatch).toBe("function");
  expect(readFileSync(fromRoot.resolve("minimatch"))).toEqual(
    readFileSync("tools/minimatch-compat/index.cjs"),
  );
});

it("keeps Redocly header patterns scoped to the matching schema URL", async () => {
  const redocly = createRequire(fromRoot.resolve("@redocly/openapi-core"));
  const { readFileFromUrl } = redocly("./utils.js");
  const urls = ["https://docs.example.test/api/schema", "https://other.example.test/api/schema"];
  const observed: Record<string, string>[] = [];
  for (const url of urls) {
    await readFileFromUrl(url, {
      headers: [{ matches: "docs.example.test/api/**", name: "X-Fixture", value: "local" }],
      customFetch: async (_url: string, init: { headers: Record<string, string> }) => {
        observed.push(init.headers);
        return { ok: true, text: async () => "{}", headers: new Map() };
      },
    });
  }
  expect(observed).toEqual([{ "X-Fixture": "local" }, {}]);
});

it("preserves React and accessibility rule decisions for component patterns", async () => {
  const { ESLint } = fromRoot("eslint");
  const linter = new ESLint({
    overrideConfigFile: true,
    overrideConfig: [{
      files: ["**/*.jsx"],
      languageOptions: { parserOptions: { ecmaFeatures: { jsx: true } } },
      plugins: { react: fromRoot("eslint-plugin-react"), "jsx-a11y": fromRoot("eslint-plugin-jsx-a11y") },
      settings: { react: { version: "19" } },
      rules: {
        "react/forbid-component-props": ["error", { forbid: [{ propNamePattern: "unsafe*" }] }],
        "jsx-a11y/label-has-associated-control": ["error", { controlComponents: ["Control*"], assert: "nesting" }],
      },
    }],
  });
  const lint = async (jsx: string) => {
    const [result] = await linter.lintText(jsx, { filePath: "fixture.jsx" });
    return result.messages.map((message: { ruleId: string | null }) => message.ruleId);
  };
  expect(await lint('<label>Field<ControlText /></label>;')).toEqual([]);
  expect(await lint('<label>Field<Widget unsafeColor="red" /></label>;')).toEqual([
    "jsx-a11y/label-has-associated-control", "react/forbid-component-props",
  ]);
});
