import assert from "node:assert/strict";
import { readFile, realpath } from "node:fs/promises";
import { dirname, resolve } from "node:path";

export const EXPECTED_COMPATIBILITY = Object.freeze({
  schema: 2,
  primeAgent: {
    repository: "https://github.com/PrimeIntellect-ai/prime-agent.git",
    minimumCommit: "a18809e00ea30638584d87b3afea7285a9d7296c",
    latestTestedCommit: "a18809e00ea30638584d87b3afea7285a9d7296c",
  },
  pi: {
    package: "@earendil-works/pi-coding-agent",
    minimumVersion: "0.84.1",
    latestTestedVersion: "0.84.1",
  },
  qdrant: { minimumVersion: "1.17.0", latestTestedVersion: "1.17.1" },
});

export function managedEnvironment(name) {
  return name.startsWith("PI_QDRANT_MEMORY_") || [
    "RLM_DEPTH",
    "PRIME_AGENT_CODING_AGENT_DIR",
    "PI_CODING_AGENT_DIR",
    "PI_SUBAGENT_CHILD",
    "PI_SUBAGENT_DEPTH",
    "XDG_CONFIG_HOME",
  ].includes(name);
}

export async function assertScopedExtensionPath(extensionPath, hostRoot, repositoryRoot) {
  const resolvedExtension = await realpath(resolve(extensionPath));
  const resolvedHost = await realpath(resolve(hostRoot));
  const resolvedRepository = await realpath(resolve(repositoryRoot));
  assert.ok(resolvedExtension.startsWith(`${resolvedHost}/`), `extension must be inside temp host: ${resolvedExtension}`);
  assert.ok(!resolvedExtension.startsWith(`${resolvedRepository}/`), "compatibility smoke must not load repository dist");
  assert.ok(resolvedExtension.endsWith("/dist/extension.js"), "extension must load the packed dist entry");
  return resolvedExtension;
}

export async function assertResolvedPiAi(extensionPath, expectedVersion, resolvePackage) {
  const resolved = resolvePackage("@earendil-works/pi-ai");
  const packageJson = JSON.parse(await readFile(resolve(dirname(resolved), "..", "package.json"), "utf8"));
  assert.equal(packageJson.version, expectedVersion);
  const source = await readFile(extensionPath, "utf8");
  const bridge = await readFile(resolve(dirname(extensionPath), "curation", "llm.js"), "utf8");
  assert.match(source, /Reflect\.get/);
  assert.match(bridge, /Reflect\.get/);
  assert.match(bridge, /completeSimple/);
  return resolved;
}

export function piHeader(sessionId, child = false) {
  return { id: sessionId, ...(child ? { parentSession: "compat-root" } : {}) };
}

export function primeHeader(sessionId, depth = 0) {
  return { id: sessionId, rlmDepth: depth };
}

export function childEnvironment(host) {
  return host === "pi" ? { PI_SUBAGENT_CHILD: "1", PI_SUBAGENT_DEPTH: "1" } : { RLM_DEPTH: "1" };
}

/** Exact packed-host completion fixture. Pi must use registry.complete; Prime
 * must use only reflected completeSimple after structural auth. */
export function completionFixture(host, baseUrl) {
  const calls = { registry: 0, auth: 0, namespace: 0 };
  const model = Object.freeze({ id: "compat-model", name: "compat-model", api: "openai-completions", provider: "openai", baseUrl, reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 16_384, maxTokens: 2_048 });
  const destination = Object.freeze({ id: "llm:compat", residency: "local", dataUse: "memory" });
  const assertCompletionCall = (receivedModel, context, options) => {
    assert.equal(receivedModel, model);
    assert.equal(context.messages.length, 1);
    assert.equal(context.messages[0].role, "user");
    assert.equal(context.messages[0].content, "compatibility completion envelope");
    assert.equal(options.maxTokens, 128);
    assert.equal(options.timeoutMs, 2_000);
    assert.equal(options.temperature, 0);
    assert.ok(options.signal instanceof AbortSignal);
  };
  const registry = host === "pi"
    ? { getAvailable: () => [model], complete: async (receivedModel, context, options) => { calls.registry += 1; assertCompletionCall(receivedModel, context, options); return { content: [{ type: "text", text: "pi reflected registry completion" }] }; } }
    : { getAvailable: () => [model], getApiKeyAndHeaders: async (receivedModel) => { calls.auth += 1; assert.equal(receivedModel, model); return { ok: true, apiKey: "compat-auth", headers: { "x-compat": "prime", "x-null": null } }; } };
  return { calls, model, destination, registry };
}
