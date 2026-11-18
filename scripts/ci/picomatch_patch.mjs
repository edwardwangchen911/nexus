import { createHash } from "node:crypto";
import { lstatSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
/* global nexusOriginalRepeatedExtglob */

// Local source remediation, not a vulnerability waiver. Exact upstream bytes
// bind every installed and embedded parser; upgrades require a fresh review.
export const targets = {
  "picomatch/lib/parse.js": ["3a91656f440069aa8f8e15c9d695b9f6b2209df1eab2be8127c72cbcca45ff5f", 1],
  "vite/node_modules/picomatch/lib/parse.js": ["4e1e05447668aa1e94366c5516a2bcd8f5fff800cc92b1d0cd7d77703b8618c4", 1],
  "tinyglobby/node_modules/picomatch/lib/parse.js": ["ce57c04e04994b83e2ca566170fa192e6fe19d710bea6da00ce2f76d01c0dea4", 1],
  "vitest/node_modules/picomatch/lib/parse.js": ["73211a8ba85ce257b827abd8960e52c9750385b3fd18e8ef1080b114d774eb23", 1],
  "@cloudflare/vite-plugin/dist/index.mjs": ["fe86b2c409772cb658564eec127c71b79f7e024acf4eeeff784e2ef1cafd8849", 1],
  "playwright-core/lib/utilsBundle.js": ["580f571bf063e2256b51b3946a035bc452fc0e22808d5547b198baea39923a32", 2],
  "playwright-core/lib/serverRegistry.js": ["e9db9d4eaa8c3690f700ea3047af7b0b6e0b16937a969589678c51db92272ae9", 2],
  "playwright/lib/matchers/expect.js": ["b22e4f194830dceb0eb67ceb44523f49bbf49f96fba50cc097b677e94ef360d7", 1],
};

// Preserve upstream's safe flattening and a small, provably bounded grammar.
// Everything else uses its existing literal fallback. Do not infer safety from
// quotes, brackets or escapes: public options change their parsing semantics.
function analyzeRepeatedExtglob(body, options) {
  const prior = nexusOriginalRepeatedExtglob(body, options);
  if (prior.safeOutput) return prior;
  const branches = body.split("|");
  if (branches.every((branch) => /^[A-Za-z0-9_./-]+$/.test(branch))) {
    const ignoreCase = options.nocase === true || (typeof options.flags === "string" && options.flags.includes("i"));
    if (ignoreCase && branches.length > 1) return { risky: true };
    // Prefix-free literal alternatives have an unambiguous repeated language.
    const overlap = branches.some((a, i) => branches.some((b, j) => i !== j && a.startsWith(b)));
    return overlap ? { risky: true } : prior;
  }
  // One positive character class is bounded; no quote, escape, negation, POSIX
  // nesting or raw regex operator is admitted. nobracket turns '*' into a glob.
  if (options.nobracket !== true && /^\[[A-Za-z0-9*?._-]+\]$/.test(body)) return prior;
  if (options.unescape !== true && /^\\[*+?]$/.test(body)) return prior;
  return { risky: true };
}
const prefix = `/* NEXUS_PICOMATCH_GHSA_fc5q_v1 */\n${analyzeRepeatedExtglob.toString()}\n`;
const marker = /\b(?:const|var) analyzeRepeatedExtglob = \(body, options2?\) => \{/g;
const sha = (value) => createHash("sha256").update(value).digest("hex");

export function patchSource(source, expectedHash, count) {
  if (sha(source) !== expectedHash) throw new Error("PICOMATCH_UPSTREAM_IDENTITY_CHANGED");
  let seen = 0;
  const patched = source.replace(marker, (match) => {
    seen++;
    return prefix + match.replace("analyzeRepeatedExtglob", "nexusOriginalRepeatedExtglob");
  });
  if (seen !== count) throw new Error("PICOMATCH_PARSER_INVENTORY_CHANGED");
  return patched;
}

export function processTree(root, apply = false) {
  const directory = path.join(root, "node_modules");
  const identity = JSON.parse(readFileSync(new URL("./picomatch_sources.json", import.meta.url), "utf8"));
  if (sha(readFileSync(path.join(root, "package-lock.json"))) !== identity.lock_sha256) throw new Error("PICOMATCH_LOCK_IDENTITY_CHANGED");
  if (apply && lstatSync(directory).isSymbolicLink()) throw new Error("PICOMATCH_REFUSE_SHARED_DEPENDENCY_MUTATION");
  for (const files of Object.values(identity.packages)) {
    for (const [relative, expected] of Object.entries(files)) {
      const filename = path.join(directory, relative);
      if (!lstatSync(filename).isFile() || sha(readFileSync(filename)) !== expected) throw new Error("PICOMATCH_PACKAGE_IDENTITY_CHANGED");
    }
  }
  const changes = [];
  for (const [relative, [expected, count]] of Object.entries(targets)) {
    const filename = path.join(directory, relative);
    for (let parent = path.dirname(filename); parent !== directory; parent = path.dirname(parent)) {
      if (lstatSync(parent).isSymbolicLink()) throw new Error("PICOMATCH_NESTED_SOURCE_SYMLINK");
    }
    if (!lstatSync(filename).isFile()) throw new Error("PICOMATCH_SOURCE_NOT_REGULAR");
    const installed = readFileSync(filename, "utf8");
    // Reversing our exact transformation must recover the registered upstream
    // hash. No marker, stamp, version string or self-reported receipt suffices.
    const original = installed.split(prefix).join("").replaceAll("nexusOriginalRepeatedExtglob =", "analyzeRepeatedExtglob =");
    const patched = patchSource(original, expected, count);
    if (installed !== patched && (!apply || installed !== original)) throw new Error("PICOMATCH_PATCH_MISSING_OR_CHANGED");
    changes.push({ filename, patched, changed: installed !== patched, sha256: sha(patched), parsers: count });
  }
  // Validate every target before touching any file; an identity mismatch fails closed.
  if (apply) for (const item of changes) if (item.changed) writeFileSync(item.filename, item.patched);
  return { status: "source_patch_verified", advisory: "GHSA-fc5q-39xc-vjpf", parsers: 10, files: changes.map(({ filename, sha256, parsers }) => ({ path: path.relative(directory, filename), sha256, parsers })) };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  console.log(JSON.stringify(processTree(path.resolve(process.argv[3] ?? "."), process.argv[2] === "--apply")));
}
