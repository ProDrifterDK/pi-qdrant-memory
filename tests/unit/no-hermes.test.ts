import { createHash } from "node:crypto";
import { readFile, readdir } from "node:fs/promises";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

async function recursiveFiles(root: string): Promise<string[]> {
  const names = await readdir(root, { recursive: true });
  return names.filter((name) => /\.(ts|js|json|md|yml|sh|py)$/u.test(name)).map((name) => join(root, name));
}
async function activeReleaseFiles(): Promise<string[]> {
  const paths: string[] = [];
  for (const root of ["src", "tests", "dist"]) paths.push.apply(paths, await recursiveFiles(root));
  paths.push("README.md", "package.json", "compatibility.json", ".github/workflows/ci.yml", "docs/configuration.md", "docs/security.md");
  return paths.filter((path) => !path.includes("docs/superpowers/") && !path.endsWith("no-hermes.test.ts"));
}
const placeholderTokens = [["TO", "DO"], ["T", "BD"]].map((parts) => parts.join(""));
const retiredTokens = [["import", "-hermes"], ["hermes", "_memory"], ["SOURCE", "_QDRANT_"], ["qdrant", "-admin"]].map((parts) => parts.join(""));
const forbiddenImports = [["@qdrant", "/js-client-rest"], ["qdrant", "-client"], ["python", "-shell"]].map((parts) => parts.join(""));
const retiredPaths = ["src/admin/qdrant-admin.ts", "tests/unit/admin-client.test.ts", "src/admin/hermes-contract.ts", "src/admin/import-hermes.ts", "src/admin/import-plan.ts", "src/admin/secret-scan.ts", "src/clients/qdrant-readonly.ts"];
const executableSurface = /(?:\b(?:import|export)\b[^\n]*(?:from\s*)?["'`]?|\b(?:command|collection|source|endpoint|credential|dependency|script)\s*[:=]|\b(?:npm|node|pi|prime|qdrant)\s+)/iu;
const importedSpecifier = /(?:\bfrom\s*|\bimport\s*)["']([^"']+)["']/giu;

async function existing(paths: readonly string[]): Promise<string[]> {
  return (await Promise.all(paths.map(async (path) => { try { await readFile(path); return path; } catch { return null; } }))).filter((path): path is string => path !== null);
}

describe("v2 active release surface", () => {
  it("contains no placeholders or retired executable paths", async () => {
    const paths = await activeReleaseFiles();
    const source = await Promise.all(paths.map(async (path) => [path, await readFile(path, "utf8")] as const));
    expect(source.filter(([, value]) => placeholderTokens.some((token) => value.includes(token))).map(([path]) => path)).toEqual([]);
    const runtimeHits = source.filter(([path, value]) => !path.startsWith("tests/") && retiredTokens.some((token) => value.includes(token)) && executableSurface.test(value)).map(([path]) => path);
    const testHits = source.filter(([path, value]) => path.startsWith("tests/") && Array.from(value.matchAll(importedSpecifier)).some(([, specifier]) => retiredTokens.some((token) => specifier.includes(token)))).map(([path]) => path);
    expect(runtimeHits.concat(testHits, await existing(retiredPaths))).toEqual([]);
    expect(source.filter(([, value]) => Array.from(value.matchAll(importedSpecifier)).some(([, specifier]) => forbiddenImports.some((token) => specifier.includes(token)))).map(([path]) => path)).toEqual([]);
    expect(paths.some((path) => path.endsWith(".py") || /(?:^|[\\/])daemon(?:[.\\/-]|$)/iu.test(path))).toBe(false);
  });

  it("pins exact release, dependency, compatibility, and tool metadata", async () => {
    const pkg = JSON.parse(await readFile("package.json", "utf8"));
    expect(pkg.version).toBe("2.1.2"); expect(pkg.engines).toMatchObject({ node: ">=22.19", npm: ">=11.10" });
    expect(pkg.dependencies).toEqual({ "umap-js": "1.4.0" });
    expect(pkg.peerDependencies?.["@earendil-works/pi-ai"]).toBe("*"); expect(pkg.devDependencies?.["@earendil-works/pi-ai"]).toBe("0.84.1"); expect(pkg.dependencies?.["@earendil-works/pi-ai"]).toBeUndefined();
    expect(pkg.files).toEqual(["dist", "README.md", "LICENSE", "docs/configuration.md", "docs/security.md", "compatibility.json", "src/vendor/umap-license-apache-2.0.txt"]);
    expect(pkg.scripts.publish).toBeUndefined(); expect(Object.entries(pkg.scripts).some(([name, command]) => /daemon|python/iu.test(`${name} ${command}`))).toBe(false);

    const compatibility = JSON.parse(await readFile("compatibility.json", "utf8"));
    expect(compatibility).toMatchObject({ schema: 2, pi: { minimumVersion: "0.84.1", latestTestedVersion: "0.84.1" }, primeAgent: { minimumCommit: "a18809e00ea30638584d87b3afea7285a9d7296c", latestTestedCommit: "a18809e00ea30638584d87b3afea7285a9d7296c" }, qdrant: { minimumVersion: "1.17.0", latestTestedVersion: "1.17.1" } });

    const extension = await readFile("src/extension.ts", "utf8");
    expect(extension.match(/\.registerTool\(/gu)).toHaveLength(1); expect(extension).toContain("createMemorySearchTool");
    const tool = await readFile("src/tool.ts", "utf8");
    expect(tool).toContain('"qdrant_memory_search"');
    expect(tool).not.toContain('name: "memory_search"');
    const ci = await readFile(".github/workflows/ci.yml", "utf8"); expect(ci).toContain("npm ci --include=dev"); expect(ci).toContain("tests/compat/run-isolated-smokes.sh");
    const isolated = await readFile("tests/compat/run-isolated-smokes.sh", "utf8"); expect(isolated).toContain("qdrant/qdrant:v1.17.1");
  });

  it("ships the exact umap-js 1.4.0 Apache-2.0 license text", async () => {
    const notice = await readFile("src/vendor/umap-license-apache-2.0.txt");
    expect(createHash("sha256").update(notice).digest("hex")).toBe("cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30");
  });
});
