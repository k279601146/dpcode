import { assert, describe, it } from "@effect/vitest";
import { resolve } from "node:path";

import { listCcbVendorPathCandidates, resolveCcbVendorPath } from "./ccbVendorPath";

describe("ccbVendorPath", () => {
  it("resolves the repo-local vendor path from source files", () => {
    const repoRoot = resolve("D:\\repo");
    const baseDir = resolve(repoRoot, "apps/server/src/provider/Layers");
    const expected = resolve(repoRoot, "CCB-claude-best-t3code");

    const resolved = resolveCcbVendorPath({
      baseDir,
      cwd: resolve(repoRoot, "apps/server"),
      exists: (candidate) => candidate === expected,
    });

    assert.strictEqual(resolved, expected);
  });

  it("resolves the repo-local vendor path from bundled dist output", () => {
    const repoRoot = resolve("D:\\repo");
    const baseDir = resolve(repoRoot, "apps/server/dist");
    const expected = resolve(repoRoot, "CCB-claude-best-t3code");

    const resolved = resolveCcbVendorPath({
      baseDir,
      cwd: resolve(repoRoot, "apps/server"),
      exists: (candidate) => candidate === expected,
    });

    assert.strictEqual(resolved, expected);
  });

  it("prefers an explicit environment override when it exists", () => {
    const repoRoot = resolve("D:\\repo");
    const baseDir = resolve(repoRoot, "apps/server/dist");
    const override = resolve("D:\\custom-ccb");

    const resolved = resolveCcbVendorPath({
      baseDir,
      cwd: resolve(repoRoot, "apps/server"),
      env: { DPCODE_CCB_VENDOR_PATH: override },
      exists: (candidate) => candidate === override,
    });

    assert.strictEqual(resolved, override);
  });

  it("lists unique candidates in fallback order", () => {
    const repoRoot = resolve("D:\\repo");
    const baseDir = resolve(repoRoot, "apps/server/dist");
    const cwd = resolve(repoRoot, "apps/server");

    const candidates = listCcbVendorPathCandidates({
      baseDir,
      cwd,
      env: {},
    });

    assert.deepStrictEqual(candidates, [
      resolve(cwd, "CCB-claude-best-t3code"),
      resolve(cwd, "..", "CCB-claude-best-t3code"),
      resolve(baseDir, "../../../../../CCB-claude-best-t3code"),
      resolve(baseDir, "../../../CCB-claude-best-t3code"),
    ]);
  });
});
