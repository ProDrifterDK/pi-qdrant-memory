import { readFile, readdir } from "node:fs/promises";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

async function recursiveFiles(root: string): Promise<string[]> {
  const names = await readdir(root, { recursive: true });
  return names.filter(name => /\.(ts|js|json|md|yml|sh)$/.test(name)).map(name => join(root, name));
}
async function activeFiles(): Promise<string[]> {
  const paths: string[] = [];
  for (const root of ["src", "tests", "dist"]) paths.push.apply(paths, await recursiveFiles(root));
  paths.push("README.md", "package.json", "compatibility.json", ".github/workflows/ci.yml", "docs/configuration.md", "docs/security.md");
  return paths.filter(path => !path.endsWith("no-hermes.test.ts"));
}
const deletedPaths = ["src/admin/hermes-contract.ts", "src/admin/import-hermes.ts", "src/admin/import-plan.ts", "tests/unit/hermes-contract.test.ts", "tests/unit/import-hermes.test.ts", "tests/unit/import-plan.test.ts"];
const forbidden = [["import", "-hermes"], ["hermes", "-contract"], ["import", "Hermes"], ["SOURCE", "_QDRANT_"], ["admin", ".source"], ["hermes", "_memory"]].map(parts => parts.join(""));
const importOrExecutableSurface = /(?:\b(?:import|export)\b[^\n]*(?:from\s*)?["'`]?|\b(?:command|collection|source|endpoint|credential)\s*[:=]|\b(?:npm|node|pi|prime|qdrant)\s+)/iu;
const importedSpecifier = /(?:\bfrom\s*|\bimport\s*)["']([^"']+)["']/giu;


describe("v2 active surface", () => {
  it("checks executable retired paths without rejecting negative fixtures", async () => {
    const paths = await activeFiles();
    const source = await Promise.all(paths.map(async path => [path, await readFile(path, "utf8")] as const));
    const runtimeHits = source.filter(([path, value]) => !path.startsWith("tests/") && forbidden.some(token => value.includes(token)) && importOrExecutableSurface.test(value)).map(([path]) => path);
    const testHits = source.filter(([path, value]) => path.startsWith("tests/") && Array.from(value.matchAll(importedSpecifier)).some(([, specifier]) => forbidden.some(token => specifier.includes(token)))).map(([path]) => path);
    const existingDeleted = (await Promise.all(deletedPaths.map(async path => { try { await readFile(path); return path; } catch { return null; } }))).filter((path): path is string => path !== null);
    expect(runtimeHits.concat(testHits, existingDeleted)).toEqual([]);
  });
});
