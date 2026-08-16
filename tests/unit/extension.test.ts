import {
  SessionManager,
  type ContextEvent,
  type ExtensionAPI,
  type ExtensionContext,
  type ExtensionFactory,
  type ToolDefinition,
} from "@earendil-works/pi-coding-agent";
import { mkdir, mkdtemp, readFile, readdir, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, vi } from "vitest";
import extension, { activateBootstrapWorkerPolicy, createMemoryExtension, type MemoryLifecycleCoordinator } from "../../src/extension.js";
import { MEMORY_CONTEXT_CUSTOM_TYPE } from "../../src/format.js";
import { canonicalStringify, deterministicUuid, sha256Hex } from "../../src/domain/canonical.js";
import { canonicalRecordHash, type ControlRecord, type EpisodeRecord, type ProcessingPolicyRecord } from "../../src/domain/records.js";
import { processingPolicyHash, intersectPolicies, type ProcessingPolicy } from "../../src/domain/policy.js";
import { COLLECTION_CONTROL_ID, V2_CONTRACT_HASH, bootstrapControlHash, controlPayload } from "../../src/qdrant/schema.js";
import { physicalPointIdFor } from "../../src/qdrant/client.js";
import { recordPayload } from "../../src/qdrant/write.js";
import { destinationForEndpoint } from "../../src/security/egress.js";
import type { ProjectIdentity } from "../../src/project.js";

type Handler = (event: any, ctx: ExtensionContext) => unknown;
type AgentMessages = ContextEvent["messages"];

function fakeApi() {
  const tools: ToolDefinition[] = [];
  const handlers = new Map<string, Handler[]>();
  const api = {
    registerTool(tool: ToolDefinition) { tools.push(tool); },
    on(event: string, handler: Handler) {
      const registered = handlers.get(event) ?? [];
      registered.push(handler);
      handlers.set(event, registered);
    },
  } as unknown as ExtensionAPI;
  const handler = (event: string): Handler => {
    const found = handlers.get(event)?.[0];
    if (found === undefined) throw new Error(`Missing ${event} handler`);
    return found;
  };
  return { api, tools, handlers, handler };
}

function ctx(input: {
  sessionId?: string;
  header?: unknown;
  branch?: unknown[];
  entries?: unknown[];
  model?: { id: string; provider: string; baseUrl: string };
} = {}) {
  const branch = input.branch ?? [];
  const entries = input.entries ?? [];
  const notifications: Array<{ message: string; type?: string }> = [];
  const value = {
    cwd: "/workspace/project",
    hasUI: true,
    signal: undefined,
    ui: {
      notify(message: string, type?: string) { notifications.push({ message, type }); },
    },
    sessionManager: {
      getSessionId: () => input.sessionId ?? "session-1",
      getBranch: () => branch,
      getEntries: () => entries,
      getHeader: () => input.header ?? null,
    },
    model: input.model ?? { id: "model", provider: "provider", baseUrl: "http://127.0.0.1:9999/v1" },
  } as unknown as ExtensionContext;
  return { value, branch, entries, notifications };
}

function user(text: string, timestamp = 1): AgentMessages[number] {
  return { role: "user", content: text, timestamp } as AgentMessages[number];
}

function hostConfig(autoRecall = true, capture = false) {
  return JSON.stringify({
    embeddings: { dimension: 1024 },
    retrieval: {
      topK: 5,
      candidatesPerLane: 2,
      contextBudgetChars: 1200,
      toolResultBudgetChars: 8000,
      timeoutMs: 2500,
    },
    prime: { enabled: true, autoRecall },
    outbox: { nodeId: "node-test" },
    coordination: { readConsistency: 1, maxClockSkewMs: 300000 },
    ...(capture ? { capture: { enabled: true, episodeRetentionDays: 30 }, privacy: { egressMode: "local_only", allowActiveModelFallback: true } } : {}),
    pi: { enabled: true, autoRecall },
  });
}

function registeredProject(): ProjectIdentity {
  return { id: "project-id", label: "project", identityKind: "registered", registrationValid: true, canonicalPath: "/workspace/project", fingerprint: "fingerprint-project" };
}

function runtimeFetch(host: "prime" | "pi", input: { bootstrap?: boolean; divergentActivation?: boolean; failWrites?: boolean; includeLlm?: boolean; originProvider?: string } = {}) {
  const calls: Array<{ url: string; body?: any }> = [];
  const collection = host === "pi" ? "pi_memory" : "prime_memory";
  const qdrantDestination = destinationForEndpoint("http://127.0.0.1:6333", "node-test", { residency: "local", dataUse: "memory" });
  const embeddingDestination = destinationForEndpoint("http://127.0.0.1:8080/v1", "node-test", { residency: "local", dataUse: "memory" });
  const llmDestination = destinationForEndpoint("http://127.0.0.1:9999/v1", "node-test", { residency: "local", dataUse: "memory" });
  // Mirror the session worker policy the extension derives via capturePolicy:
  // revision capture-lifecycle-v1, and an llm destination only when the test
  // config authorizes one (fallback or exact allowlist).
  const policyBase: ProcessingPolicy = { id: "pending", ownerHost: host, destinationIds: { qdrant: qdrantDestination.id, embedding: embeddingDestination.id, ...(input.includeLlm === false ? {} : { llm: llmDestination.id }) }, originProvider: input.originProvider ?? "provider", allowCrossProviderReplay: false, expiresAt: null, residency: "local", dataUse: "memory", policyRevision: "capture-lifecycle-v1" };
  const policy: ProcessingPolicy = { ...policyBase, id: processingPolicyHash(policyBase) };
  const canonical = <T extends { contentHash: string }>(value: T): T => ({ ...value, contentHash: canonicalRecordHash(value as never) });
  const policyRecord = canonical<ProcessingPolicyRecord>({ recordType: "processing_policy", id: policy.id, ownerHost: host, schemaRevision: 1, createdAt: "2026-08-13T15:00:00.000Z", privacyEpoch: 0, processingPolicyId: policy.id, expiresAt: null, policy, canonicalHash: policy.id, contentHash: "pending" });
  const control = input.bootstrap === true
    ? bootstrapControlFixture(host)
    : canonical<ControlRecord>({ recordType: "collection_control", id: COLLECTION_CONTROL_ID, ownerHost: host, schemaRevision: 1, createdAt: "2026-08-13T15:00:00.000Z", privacyEpoch: 0, processingPolicyId: policy.id, expiresAt: null, version: 1, activeGeneration: null, activeBaseGeneration: null, coordinationPolicyEpoch: 1, coordinationPolicyHash: "coord-hash", state: "active", scanCursor: null, lastForgetBarrier: null, revokedDestinationIds: [], contentHash: "pending" });
  const episode = canonical<EpisodeRecord>({ recordType: "episode", id: "11111111-1111-5111-8111-111111111111", ownerHost: host, schemaRevision: 1, createdAt: "2026-08-13T15:00:00.000Z", privacyEpoch: 0, processingPolicyId: policy.id, expiresAt: null, sourceEntryId: "entry-1", host, projectId: "project-id", projectIdentityKind: "registered", sessionId: "session-1", turnId: "turn-1", agentRole: "root", depth: 0, eventKind: "user", eventAt: "2026-08-13T15:00:00.000Z", modelId: "model", embeddingDimension: 1024, originProvider: input.originProvider ?? "provider", destinationId: llmDestination.id, status: "active", redactionStatus: "unchanged", secretScan: "passed", text: "Portable recalled context for root agents", vector: Array.from({ length: 1024 }, () => Math.fround(0.1)), contentHash: "pending" });
  const points = input.bootstrap === true
    ? new Map([[COLLECTION_CONTROL_ID, { id: COLLECTION_CONTROL_ID, payload: controlPayload(control) }]])
    : new Map([
    [COLLECTION_CONTROL_ID, { id: COLLECTION_CONTROL_ID, payload: recordPayload(control) }],
    [physicalPointIdFor("processing_policy", policy.id), { id: physicalPointIdFor("processing_policy", policy.id), payload: recordPayload(policyRecord) }],
    [episode.id, { id: episode.id, payload: recordPayload(episode), vector: { semantic: episode.vector! } }],
  ]);
  const fetchImpl = vi.fn(async (rawUrl: string, init?: RequestInit) => {
    const parsedUrl = new URL(rawUrl); const body = init?.body === undefined ? undefined : JSON.parse(String(init.body)); calls.push({ url: rawUrl, body });
    if (parsedUrl.pathname === "/healthz") return new Response("ok");
    if (parsedUrl.pathname === `/collections/${collection}`) return new Response(JSON.stringify({ result: { config: { params: { vectors: { semantic: { size: 1024, distance: "Dot" } } } } } }));
    if (parsedUrl.pathname.endsWith("/embeddings")) return new Response(JSON.stringify({ data: [{ embedding: Array.from({ length: 1024 }, () => 0.1) }] }));
    if (parsedUrl.pathname.endsWith("/points/scroll")) {
      const must = body?.filter?.must ?? [];
      const recordTypes = new Set(must.flatMap((condition: any) => condition.key === "record_type" ? condition.match?.any ?? [condition.match?.value] : []));
      const privacyEpoch = must.find((condition: any) => condition.key === "privacy_epoch")?.match?.value;
      const result = [...points.values()].filter((point: any) => (recordTypes.size === 0 || recordTypes.has(point.payload?.record_type)) && (privacyEpoch === undefined || point.payload?.privacy_epoch === privacyEpoch));
      return new Response(JSON.stringify({ result: { points: result, next_page_offset: null } }));
    }
    if (parsedUrl.pathname.endsWith("/points/search")) {
      const wantsEpisode = body?.filter?.must?.some((condition: any) => condition.key === "record_type" && (condition.match?.value === "episode" || condition.match?.any?.includes("episode")));
      return new Response(JSON.stringify({ result: wantsEpisode ? [{ id: episode.id, score: 0.9, payload: recordPayload(episode), vector: { semantic: episode.vector! } }] : [] }));
    }
    if (parsedUrl.pathname.endsWith("/points") && init?.method === "POST") {
      const result = (body?.ids ?? []).map((id: string) => points.get(id)).filter(Boolean);
      return new Response(JSON.stringify({ result, status: "ok" }));
    }
    if (parsedUrl.pathname.endsWith("/points") && init?.method === "PUT") {
      if (input.failWrites === true) return new Response("writes disabled", { status: 500 });
      for (const point of body?.points ?? []) {
        if (input.divergentActivation === true && point.payload?.record_type === "collection_control") {
          // Simulate a divergent racer winning the activation CAS: the stored
          // control advances to v1 bound to a DIFFERENT worker policy.
          const divergentBase: ProcessingPolicy = { ...policyBase, expiresAt: "2030-01-01T00:00:00.000Z" };
          const divergentPolicy: ProcessingPolicy = { ...divergentBase, id: processingPolicyHash(divergentBase) };
          const divergentPending = { ...bootstrapControlFixture(host), version: 1, processingPolicyId: divergentPolicy.id, coordinationPolicyEpoch: 1, coordinationPolicyHash: divergentPolicy.id, contentHash: "pending" };
          const divergent: ControlRecord = { ...divergentPending, contentHash: canonicalRecordHash(divergentPending as ControlRecord) };
          points.set(COLLECTION_CONTROL_ID, { id: COLLECTION_CONTROL_ID, payload: recordPayload(divergent) });
          continue;
        }
        points.set(point.id, point);
      }
      return new Response(JSON.stringify({ result: { status: "acknowledged" }, status: "ok" }));
    }
    return new Response("not found", { status: 404 });
  });
  return { fetchImpl: fetchImpl as typeof fetch, calls, points };
}

function factoryFor(host: "prime" | "pi", input: { autoRecall?: boolean; fetchImpl?: typeof fetch } = {}) {
  const runtime = runtimeFetch(host);
  return {
    factory: createMemoryExtension({
      env: { PI_QDRANT_MEMORY_HOST: host },
      argv: [],
      homeDir: "/home/test",
      readTextFile: async () => hostConfig(input.autoRecall ?? true),
      fetchImpl: input.fetchImpl ?? runtime.fetchImpl,
      projectResolver: async () => registeredProject(),
    }),
    calls: runtime.calls,
    fetchImpl: runtime.fetchImpl,
  };
}

async function invokeBefore(handler: Handler, prompt: string, context: ExtensionContext) {
  return handler({ type: "before_agent_start", prompt, systemPrompt: "", systemPromptOptions: {} }, context);
}

describe("portable memory extension", () => {
  it("exports an async-compatible default ExtensionFactory", () => {
    expect(extension).toBeTypeOf("function");
    expect(extension satisfies ExtensionFactory).toBe(extension);
  });

  it("registers the unavailable tool and all lifecycle hooks when host resolution is unknown", async () => {
    const fetchImpl = vi.fn<typeof fetch>();
    const factory = createMemoryExtension({
      env: {},
      argv: [],
      homeDir: "/home/test",
      readTextFile: vi.fn(async () => hostConfig()),
      fetchImpl,
    });
    const fake = fakeApi();
    await factory(fake.api);

    expect(fake.tools.map((tool) => tool.name)).toEqual(["qdrant_memory_search"]);
    expect(fake.tools.some((tool) => tool.name === "memory_search")).toBe(false);
    expect([...fake.handlers.keys()].sort()).toEqual([
      "agent_end",
      "before_agent_start",
      "context",
      "session_before_compact",
      "session_shutdown",
      "session_start",
    ]);
    expect(fake.handlers.has("agent_settled")).toBe(false);

    const context = ctx();
    await invokeBefore(fake.handler("before_agent_start"), "A natural language recall request", context.value);
    const result = await fake.handler("context")({ type: "context", messages: [user("A natural language recall request")] }, context.value);
    expect(result).toBeUndefined();
    const toolResult = await fake.tools[0]!.execute(
      "call-1",
      { query: "A natural language recall request" },
      undefined,
      undefined,
      context.value,
    );
    expect(toolResult.content).toEqual([{ type: "text", text: "Memory search is temporarily unavailable." }]);
    expect(fetchImpl).not.toHaveBeenCalled();

    await fake.handler("session_start")({ type: "session_start", reason: "startup" }, context.value);
    await fake.handler("session_start")({ type: "session_start", reason: "reload" }, context.value);
    expect(context.notifications).toEqual([{
      message: "pi-qdrant-memory: recall unavailable (host).",
      type: "warning",
    }]);
  });

  it("prefetches once for Prime roots and injects one copied ephemeral custom message", async () => {
    const runtime = factoryFor("prime");
    const fake = fakeApi();
    await runtime.factory(fake.api);
    const context = ctx({ header: { rlmDepth: 0 } });
    const messages = [user("Portable recalled context for root agents")];

    await invokeBefore(fake.handler("before_agent_start"), "Portable recalled context for root agents", context.value);
    const result = await fake.handler("context")({ type: "context", messages }, context.value) as { messages: AgentMessages };
    const recalled = result.messages.filter((message) => message.role === "custom" && message.customType === MEMORY_CONTEXT_CUSTOM_TYPE);

    expect(fake.tools.map((tool) => tool.name)).toEqual(["qdrant_memory_search"]);
    expect(fake.tools.some((tool) => tool.name === "memory_search")).toBe(false);
    expect(recalled).toHaveLength(1);
    expect(recalled[0]).toMatchObject({
      role: "custom",
      customType: MEMORY_CONTEXT_CUSTOM_TYPE,
      display: false,
      details: { hitCount: 1 },
    });
    expect(result.messages).not.toBe(messages);
    expect(context.value.sessionManager.getBranch()).not.toContainEqual(expect.objectContaining({ customType: MEMORY_CONTEXT_CUSTOM_TYPE }));
    expect(runtime.calls.filter((call) => new URL(call.url).pathname.endsWith("/points/scroll"))).toHaveLength(1);
    expect(runtime.calls.filter((call) => new URL(call.url).pathname.endsWith("/embeddings"))).toHaveLength(0);
  });

  it("keeps the explicit tool but disables auto recall for Prime children and invalid depth", async () => {
    const runtime = factoryFor("prime");
    const fake = fakeApi();
    await runtime.factory(fake.api);
    const child = ctx({ header: { rlmDepth: 2 } });

    await invokeBefore(fake.handler("before_agent_start"), "A child prompt that must not auto recall", child.value);
    expect(await fake.handler("context")({ type: "context", messages: [user("A child prompt that must not auto recall")] }, child.value)).toBeUndefined();
    expect(runtime.calls).toEqual([]);

    const toolResult = await fake.tools[0]!.execute(
      "call-child",
      { query: "Portable recalled context for root agents", limit: 3 },
      undefined,
      undefined,
      child.value,
    );
    expect(toolResult.content[0]).toMatchObject({ type: "text" });
    expect((toolResult.content[0] as { text: string }).text).toContain("<memory-context");

    const invalid = ctx({ header: { rlmDepth: "invalid" } });
    const count = runtime.calls.length;
    await invokeBefore(fake.handler("before_agent_start"), "Invalid depth must fail closed", invalid.value);
    expect(await fake.handler("context")({ type: "context", messages: [user("Invalid depth must fail closed")] }, invalid.value)).toBeUndefined();
    expect(runtime.calls).toHaveLength(count);
  });

  it("enables Pi auto-recall only for a verified root depth", async () => {
    const runtime = factoryFor("pi");
    const fake = fakeApi();
    await runtime.factory(fake.api);
    const context = ctx({ header: { rlmDepth: 0 } });
    const messages = [user("Portable recalled context for root agents")];

    await invokeBefore(fake.handler("before_agent_start"), "Portable recalled context for root agents", context.value);
    const result = await fake.handler("context")({ type: "context", messages }, context.value) as { messages: AgentMessages };
    expect(result.messages.some((message) => message.role === "custom" && message.customType === MEMORY_CONTEXT_CUSTOM_TYPE)).toBe(true);

    const disabledRuntime = factoryFor("pi", { autoRecall: false });
    const disabledFake = fakeApi();
    await disabledRuntime.factory(disabledFake.api);
    await invokeBefore(disabledFake.handler("before_agent_start"), "Pi config disables this recall prompt", context.value);
    expect(await disabledFake.handler("context")({ type: "context", messages }, context.value)).toBeUndefined();
  });

  it("revalidates settled context retries and clears in-flight recall state on lifecycle", async () => {
    const runtime = factoryFor("prime");
    const fake = fakeApi();
    await runtime.factory(fake.api);
    const context = ctx({ header: { rlmDepth: 0 } });
    const messages = [user("Portable recalled context for root agents")];

    const first = await fake.handler("context")({ type: "context", messages }, context.value) as { messages: AgentMessages };
    const retry = await fake.handler("context")({ type: "context", messages: first.messages }, context.value) as { messages: AgentMessages };
    expect(retry.messages.filter((message) => message.role === "custom" && message.customType === MEMORY_CONTEXT_CUSTOM_TYPE)).toHaveLength(1);
    expect(runtime.calls.filter((call) => new URL(call.url).pathname.endsWith("/points/scroll"))).toHaveLength(2);

    await fake.handler("session_start")({ type: "session_start", reason: "resume" }, context.value);
    expect(runtime.calls.some((call) => new URL(call.url).pathname === "/healthz")).toBe(true);
    await fake.handler("context")({ type: "context", messages }, context.value);
    expect(runtime.calls.filter((call) => new URL(call.url).pathname.endsWith("/points/scroll"))).toHaveLength(3);

    await fake.handler("session_shutdown")({ type: "session_shutdown", reason: "quit" }, context.value);
    await fake.handler("context")({ type: "context", messages }, context.value);
    expect(runtime.calls.filter((call) => new URL(call.url).pathname.endsWith("/points/scroll"))).toHaveLength(4);
  });
});


describe("derived local node identity", () => {
  it("initializes recall binding without creating a capture producer when nodeId is omitted", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "pi-qdrant-derived-node-"));
    const env = { PI_QDRANT_MEMORY_HOST: "pi", PI_CODING_AGENT_DIR: join(homeDir, ".pi", "agent") };
    const configured = JSON.parse(hostConfig(true, false)); delete configured.outbox.nodeId;
    const runtime = runtimeFetch("pi"); const fake = fakeApi();
    const factory = createMemoryExtension({ env, argv: [], homeDir, readTextFile: async () => JSON.stringify(configured), fetchImpl: runtime.fetchImpl });
    await factory(fake.api);
    await fake.handler("session_start")({ type: "session_start", reason: "startup" }, ctx().value);
    expect(runtime.fetchImpl).toHaveBeenCalled();
    const outboxRoot = join(env.PI_CODING_AGENT_DIR, "pi-qdrant-memory", "outbox");
    const nodes = (await readdir(outboxRoot)).filter((name) => name.startsWith("node-"));
    expect(nodes).toHaveLength(1);
    expect(await readdir(join(outboxRoot, nodes[0]!))).toEqual(["node.json"]);
  });
});

describe("autonomous lifecycle wiring", () => {
  function lifecycleDouble(input: { fail?: boolean; failCaptureCount?: number; scheduleResult?: boolean; captureEpisodes?: (lifecycle: string) => readonly unknown[] } = {}) {
    const calls: Array<{ method: string; value?: unknown }> = []; let remainingCaptureFailures = input.failCaptureCount ?? 0;
    const coordinator: MemoryLifecycleCoordinator = {
      async start(value) { calls.push({ method: "start", value }); if (input.fail) throw new Error("raw secret must not leak"); },
      async recover(value) { calls.push({ method: "recover", value }); if (input.fail) throw new Error("raw secret must not leak"); },
      async capture(value) {
        const persisted = value.getEntries();
        calls.push({ method: "capture", value: { lifecycle: value.lifecycle, entries: persisted, keys: Object.keys(value).sort() } });
        if (remainingCaptureFailures > 0) { remainingCaptureFailures -= 1; throw new Error("transient admission failure"); }
        if (input.fail) throw new Error("raw secret must not leak");
        return (input.captureEpisodes?.(value.lifecycle) ?? [{ id: `episode-${value.lifecycle}`, processingPolicyId: "policy", eventKind: value.lifecycle === "agent_end" ? "tool_result" : "user" }]) as never;
      },
      async deliver(value = {}) { calls.push({ method: "deliver", value }); if (input.fail) throw new Error("raw secret must not leak"); },
      async scheduleRoot(value) { calls.push({ method: "scheduleRoot", value }); if (input.fail) throw new Error("raw secret must not leak"); return input.scheduleResult; },
      async shutdown(value) { calls.push({ method: "shutdown", value }); if (input.fail) throw new Error("raw secret must not leak"); },
      clear() { calls.push({ method: "clear" }); },
    };
    return { coordinator, calls };
  }

  it("uses persisted getEntries at every capture event, ignores event arrays, and schedules only root work", async () => {
    const lifecycle = lifecycleDouble();
    const runtime = runtimeFetch("pi");
    const factory = createMemoryExtension({
      env: { PI_QDRANT_MEMORY_HOST: "pi" }, argv: [], homeDir: "/home/test",
      readTextFile: async () => hostConfig(true, true), fetchImpl: runtime.fetchImpl,
      projectResolver: async () => registeredProject(),
      lifecycleCoordinator: lifecycle.coordinator,
    });
    const fake = fakeApi(); await factory(fake.api);
    const persisted = [{ id: "old", type: "message", message: { role: "user", content: "before cutoff" } }];
    const context = ctx({ header: { parentSession: null }, entries: persisted });
    await fake.handler("session_start")({ type: "session_start", reason: "startup" }, context.value);
    const startCall = lifecycle.calls.find((call) => call.method === "start")?.value as { getEntries(): readonly unknown[]; project: { id: string }; marker: { rootWorkAllowed: boolean } };
    expect(startCall.project.id).toBe("project-id");
    expect(startCall.marker.rootWorkAllowed).toBe(true);
    expect(startCall.getEntries()).toEqual([{ id: "old", type: "message", message: { role: "user", content: "before cutoff" } }]);
    persisted.push(
      { id: "new", type: "message", message: { role: "user", content: "persisted after cutoff" } },
      { id: "memory", type: "message", message: { role: "custom", customType: MEMORY_CONTEXT_CUSTOM_TYPE, content: "must not recapture" } },
    );

    await expect(fake.handler("agent_end")({ type: "agent_end", messages: [{ role: "user", content: "EVENT-ONLY-SECRET" }] }, context.value)).resolves.toBeUndefined();
    await expect(fake.handler("session_before_compact")({ type: "session_before_compact", branchEntries: [{ id: "event-array-must-be-ignored" }] }, context.value)).resolves.toBeUndefined();
    await expect(fake.handler("session_shutdown")({ type: "session_shutdown", reason: "quit", messages: [{ content: "ignored" }] }, context.value)).resolves.toBeUndefined();

    const captureCalls = lifecycle.calls.filter((call) => call.method === "capture").map((call) => call.value as { lifecycle: string; entries: unknown[]; keys: string[] });
    expect(captureCalls.map((call) => call.lifecycle)).toEqual(["agent_end", "session_before_compact", "session_shutdown"]);
    expect(captureCalls.every((call) => call.entries === persisted)).toBe(true);
    expect(JSON.stringify(captureCalls)).toContain("must not recapture"); // raw persisted source; hardened capture selector must exclude it downstream.
    expect(JSON.stringify(captureCalls)).not.toContain("EVENT-ONLY-SECRET");
    expect(captureCalls.every((call) => !call.keys.includes("messages") && call.keys.includes("getEntries"))).toBe(true);
    expect(lifecycle.calls.filter((call) => call.method === "scheduleRoot").map((call) => (call.value as { reason: string }).reason)).toEqual(["compact"]);
    expect(lifecycle.calls.some((call) => call.method === "shutdown")).toBe(true);
    expect(lifecycle.calls.at(-1)?.method).toBe("clear");
  });

  it("discovers only closed producer directories for guarded restart adoption", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "pi-qdrant-recovery-"));
    const agentDir = join(homeDir, ".pi", "agent");
    const closed = join(agentDir, "pi-qdrant-memory", "outbox", "node-closed", "producer-closed");
    const active = join(agentDir, "pi-qdrant-memory", "outbox", "node-active", "producer-active");
    await mkdir(closed, { recursive: true }); await mkdir(active, { recursive: true });
    await writeFile(join(closed, "state.json"), JSON.stringify({ state: "closed" }));
    await writeFile(join(active, "state.json"), JSON.stringify({ state: "active" }));
    const lifecycle = lifecycleDouble(); const runtime = runtimeFetch("pi");
    const factory = createMemoryExtension({ env: { PI_QDRANT_MEMORY_HOST: "pi", PI_CODING_AGENT_DIR: agentDir }, argv: [], homeDir, readTextFile: async () => hostConfig(true, true), fetchImpl: runtime.fetchImpl, projectResolver: async () => registeredProject(), lifecycleCoordinator: lifecycle.coordinator });
    const fake = fakeApi(); await factory(fake.api);
    await fake.handler("session_start")({ type: "session_start", reason: "startup" }, ctx().value);
    const recovery = lifecycle.calls.find((call) => call.method === "recover")?.value;
    expect(recovery).toEqual([closed]);
  });

  it("discovers a closed producer whose crash happened after reservation but before its first job temp", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "pi-qdrant-recovery-reservation-"));
    const agentDir = join(homeDir, ".pi", "agent");
    const producerUuid = "11111111-1111-4111-8111-111111111111";
    const producer = join(agentDir, "pi-qdrant-memory", "outbox", "node-a", producerUuid);
    await mkdir(join(producer, "jobs"), { recursive: true, mode: 0o700 });
    await mkdir(join(producer, "control"), { mode: 0o700 });
    await mkdir(join(producer, "quarantine"), { mode: 0o700 });
    await writeFile(join(producer, "state.json"), JSON.stringify({ state: "closed", heartbeatAt: 1, closedAt: 2 }), { mode: 0o600 });
    const reservation = {
      version: 1,
      reservationId: deterministicUuid("pi-qdrant-memory-v2:outbox-reservation", "node-a", producerUuid, "33333333-3333-4333-8333-333333333333"),
      jobId: "33333333-3333-4333-8333-333333333333",
      jobAuditHash: "a".repeat(64), policyId: "b".repeat(64), deadline: null, nodeId: "node-a", producerUuid,
      requestedBytes: 128, auditHash: "",
    };
    const reservationWithoutAudit = { ...reservation }; delete reservationWithoutAudit.auditHash;
    reservation.auditHash = sha256Hex(canonicalStringify(reservationWithoutAudit));
    await mkdir(join(agentDir, "pi-qdrant-memory", "outbox", "reservations"), { recursive: true, mode: 0o700 });
    await writeFile(join(agentDir, "pi-qdrant-memory", "outbox", "reservations", `${reservation.reservationId}.json`), canonicalStringify(reservation), { mode: 0o600 });
    const lifecycle = lifecycleDouble(); const runtime = runtimeFetch("pi");
    const factory = createMemoryExtension({ env: { PI_QDRANT_MEMORY_HOST: "pi", PI_CODING_AGENT_DIR: agentDir }, argv: [], homeDir, readTextFile: async () => hostConfig(true, true), fetchImpl: runtime.fetchImpl, projectResolver: async () => registeredProject(), lifecycleCoordinator: lifecycle.coordinator });
    const fake = fakeApi(); await factory(fake.api);
    await fake.handler("session_start")({ type: "session_start", reason: "startup" }, ctx().value);
    expect(lifecycle.calls.find((call) => call.method === "recover")?.value).toEqual([producer]);
  });

  it("adopts a stale-active 65th producer without letting 64 healthy producers consume the recovery budget", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "pi-qdrant-recovery-fair-")); const agentDir = join(homeDir, ".pi", "agent"); const node = join(agentDir, "pi-qdrant-memory", "outbox", "node-a"); const operationNow = 1_000_000;
    for (let index = 0; index < 64; index += 1) { const path = join(node, `producer-${String(index).padStart(3, "0")}`); await mkdir(path, { recursive: true }); await writeFile(join(path, "state.json"), JSON.stringify({ state: "active", heartbeatAt: operationNow })); }
    const stale = join(node, "producer-064"); await mkdir(stale, { recursive: true }); await writeFile(join(stale, "state.json"), JSON.stringify({ state: "active", heartbeatAt: 0 }));
    const lifecycle = lifecycleDouble(); const runtime = runtimeFetch("pi"); const factory = createMemoryExtension({ env: { PI_QDRANT_MEMORY_HOST: "pi", PI_CODING_AGENT_DIR: agentDir }, argv: [], homeDir, now: () => operationNow, readTextFile: async () => hostConfig(true, true), fetchImpl: runtime.fetchImpl, projectResolver: async () => registeredProject(), lifecycleCoordinator: lifecycle.coordinator });
    const fake = fakeApi(); await factory(fake.api); await fake.handler("session_start")({ type: "session_start", reason: "startup" }, ctx({ header: null }).value);
    const recovered = lifecycle.calls.find((call) => call.method === "recover")?.value as string[];
    expect(recovered).toEqual([stale]);
  });

  it("rotates durable recovery pages across repeated restarts without lexical starvation", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "pi-qdrant-recovery-rotation-")); const agentDir = join(homeDir, ".pi", "agent"); const node = join(agentDir, "pi-qdrant-memory", "outbox", "node-a"); const operationNow = 1_000_000;
    const producerPaths: string[] = [];
    const advanceRotation = async (path: string): Promise<void> => {
      const producerUuid = path.split("/").at(-1)!; let previous = -1; try { previous = JSON.parse(await readFile(join(path, "recovery.json"), "utf8")).recoveredAt as number; } catch { /* first recovery */ }
      const recoveredAt = Math.max(operationNow, previous + 1); const value: Record<string, unknown> = { version: 1, kind: "recovery_rotation", producerUuid, recoveredAt, auditHash: "" }; const hashInput = { ...value }; delete hashInput.auditHash; value.auditHash = sha256Hex(canonicalStringify(hashInput)); await writeFile(join(path, "recovery.json"), canonicalStringify(value), { mode: 0o600 });
    };
    for (let index = 0; index < 65; index += 1) {
      const producerUuid = `00000000-0000-4000-8000-${String(index).padStart(12, "0")}`; const path = join(node, producerUuid); producerPaths.push(path); await mkdir(join(path, "jobs"), { recursive: true }); await mkdir(join(path, "control")); await mkdir(join(path, "quarantine")); await writeFile(join(path, "state.json"), JSON.stringify({ state: "closed" })); await writeFile(join(path, "jobs", "pending.json"), "{}");
    }
    const start = async (): Promise<string[]> => {
      const lifecycle = lifecycleDouble(); const runtime = runtimeFetch("pi"); const factory = createMemoryExtension({ env: { PI_QDRANT_MEMORY_HOST: "pi", PI_CODING_AGENT_DIR: agentDir }, argv: [], homeDir, now: () => operationNow, readTextFile: async () => hostConfig(true, true), fetchImpl: runtime.fetchImpl, projectResolver: async () => registeredProject(), lifecycleCoordinator: lifecycle.coordinator }); const fake = fakeApi(); await factory(fake.api); await fake.handler("session_start")({ type: "session_start", reason: "startup" }, ctx({ header: null }).value); return lifecycle.calls.find((call) => call.method === "recover")?.value as string[];
    };
    const first = await start(); expect(first).toHaveLength(64); expect(first).toContain(producerPaths[0]); expect(first).not.toContain(producerPaths[64]); for (const path of first) await advanceRotation(path);
    const second = await start(); expect(second).toHaveLength(64); expect(second[0]).toBe(producerPaths[64]); expect(second).not.toContain(producerPaths[63]); for (const path of second) await advanceRotation(path);
    const third = await start(); expect(third).toHaveLength(64); expect(third).toContain(producerPaths[63]); expect(new Set([...first, ...second, ...third])).toEqual(new Set(producerPaths));
  });

  it("schedules root work at the configured turn threshold without waiting for compaction", async () => {
    const lifecycle = lifecycleDouble(); const runtime = runtimeFetch("pi");
    const config = JSON.parse(hostConfig(true, true)); config.curation = { turnTrigger: 2, toolTrigger: 99, maxInputTokens: 12000 };
    const factory = createMemoryExtension({ env: { PI_QDRANT_MEMORY_HOST: "pi" }, argv: [], homeDir: "/home/test", readTextFile: async () => JSON.stringify(config), fetchImpl: runtime.fetchImpl, projectResolver: async () => registeredProject(), lifecycleCoordinator: lifecycle.coordinator });
    const fake = fakeApi(); await factory(fake.api); const context = ctx({ entries: [] });
    await fake.handler("session_start")({ type: "session_start", reason: "startup" }, context.value);
    await fake.handler("agent_end")({ type: "agent_end", messages: [] }, context.value);
    expect(lifecycle.calls.filter((call) => call.method === "scheduleRoot")).toHaveLength(0);
    await fake.handler("agent_end")({ type: "agent_end", messages: [] }, context.value);
    const scheduled = lifecycle.calls.filter((call) => call.method === "scheduleRoot");
    expect(scheduled).toHaveLength(1);
    expect(scheduled[0]!.value).toMatchObject({ reason: "threshold", marker: { rootWorkAllowed: true } });
  });

  it("retains unscheduled root work when the production coordinator is unavailable", async () => {
    const lifecycle = lifecycleDouble({ scheduleResult: false }); const runtime = runtimeFetch("pi");
    const config = JSON.parse(hostConfig(true, true)); config.curation = { turnTrigger: 2, toolTrigger: 99, maxInputTokens: 12000 };
    const factory = createMemoryExtension({ env: { PI_QDRANT_MEMORY_HOST: "pi" }, argv: [], homeDir: "/home/test", readTextFile: async () => JSON.stringify(config), fetchImpl: runtime.fetchImpl, projectResolver: async () => registeredProject(), lifecycleCoordinator: lifecycle.coordinator });
    const fake = fakeApi(); await factory(fake.api); const context = ctx({ entries: [] });
    await fake.handler("session_start")({ type: "session_start", reason: "startup" }, context.value);
    await fake.handler("agent_end")({ type: "agent_end", messages: [] }, context.value);
    await fake.handler("agent_end")({ type: "agent_end", messages: [] }, context.value);
    await fake.handler("agent_end")({ type: "agent_end", messages: [] }, context.value);
    expect(lifecycle.calls.filter((call) => call.method === "scheduleRoot")).toHaveLength(2);
  });

  it("deduplicates fixed lifecycle warnings per failure kind and keeps hostile header reads fail-open", async () => {
    const base = lifecycleDouble(); const warnings: string[] = []; const runtime = runtimeFetch("pi");
    base.coordinator.deliver = async () => { throw new Error("delivery secret"); };
    base.coordinator.scheduleRoot = async () => { throw new Error("root secret"); };
    const config = JSON.parse(hostConfig(true, true)); config.curation = { turnTrigger: 1, toolTrigger: 99, maxInputTokens: 12000 };
    const factory = createMemoryExtension({ env: { PI_QDRANT_MEMORY_HOST: "pi" }, argv: [], homeDir: "/home/test", readTextFile: async () => JSON.stringify(config), fetchImpl: runtime.fetchImpl, projectResolver: async () => registeredProject(), lifecycleCoordinator: base.coordinator, warningSink: (warning) => warnings.push(warning.message) });
    const fake = fakeApi(); await factory(fake.api); const context = ctx({ entries: [] });
    await fake.handler("session_start")({ type: "session_start", reason: "startup" }, context.value);
    await expect(fake.handler("agent_end")({ type: "agent_end", messages: [] }, context.value)).resolves.toBeUndefined();
    expect(warnings).toEqual(["pi-qdrant-memory: lifecycle unavailable (delivery).", "pi-qdrant-memory: lifecycle unavailable (root)."]);
    const scheduledBeforeInvalidHeader = base.calls.filter((call) => call.method === "scheduleRoot").length;
    (context.value.sessionManager as any).getHeader = () => { throw new Error("header secret"); };
    await expect(fake.handler("agent_end")({ type: "agent_end", messages: [] }, context.value)).resolves.toBeUndefined();
    expect(base.calls.filter((call) => call.method === "scheduleRoot")).toHaveLength(scheduledBeforeInvalidHeader);
    expect(warnings.at(-1)).toBe("pi-qdrant-memory: lifecycle unavailable (capture).");
    expect(warnings.join(" ")).not.toContain("secret");
  });

  it.each([
    ["pi", { parentSession: "parent-session" }, {}],
    ["pi", {}, { PI_SUBAGENT_CHILD: "1" }],
    ["prime", { rlmDepth: 2 }, {}],
    ["prime", { rlmDepth: 0 }, { RLM_DEPTH: "1" }],
  ] as const)("never auto-recalls or schedules root work for %s children or ambiguous markers", async (host, header, markerEnv) => {
    const lifecycle = lifecycleDouble(); const runtime = runtimeFetch(host);
    const factory = createMemoryExtension({
      env: { PI_QDRANT_MEMORY_HOST: host, ...markerEnv }, argv: [], homeDir: "/home/test",
      readTextFile: async () => hostConfig(true, true), fetchImpl: runtime.fetchImpl,
      projectResolver: async () => registeredProject(),
      lifecycleCoordinator: lifecycle.coordinator,
    });
    const fake = fakeApi(); await factory(fake.api); const context = ctx({ header, entries: [] });
    await fake.handler("session_start")({ type: "session_start", reason: "startup" }, context.value);
    const afterHealth = runtime.calls.length;
    await invokeBefore(fake.handler("before_agent_start"), "child prompt that must not recall", context.value);
    expect(await fake.handler("context")({ type: "context", messages: [user("child prompt that must not recall")] }, context.value)).toBeUndefined();
    await fake.handler("session_before_compact")({ type: "session_before_compact", branchEntries: [] }, context.value);
    expect(lifecycle.calls.filter((call) => ["start", "capture", "deliver", "scheduleRoot"].includes(call.method))).toHaveLength(0);
    expect(runtime.calls).toHaveLength(afterHealth);
  });

  it("fails every lifecycle turn open and emits only fixed redacted warnings", async () => {
    const lifecycle = lifecycleDouble({ fail: true }); lifecycle.coordinator.clear = () => { throw new Error("clear secret"); }; const warnings: string[] = []; const runtime = runtimeFetch("pi");
    const factory = createMemoryExtension({
      env: { PI_QDRANT_MEMORY_HOST: "pi" }, argv: [], homeDir: "/home/test",
      readTextFile: async () => hostConfig(true, true), fetchImpl: runtime.fetchImpl,
      projectResolver: async () => registeredProject(),
      lifecycleCoordinator: lifecycle.coordinator,
      warningSink: (warning) => { warnings.push(warning.message); },
    });
    const fake = fakeApi(); await factory(fake.api); const context = ctx({ entries: [] });
    await expect(fake.handler("session_start")({ type: "session_start", reason: "startup" }, context.value)).resolves.toBeUndefined();
    await expect(fake.handler("agent_end")({ type: "agent_end", messages: [{ content: "raw secret" }] }, context.value)).resolves.toBeUndefined();
    await expect(fake.handler("session_before_compact")({ type: "session_before_compact", branchEntries: [] }, context.value)).resolves.toBeUndefined();
    await expect(fake.handler("session_shutdown")({ type: "session_shutdown", reason: "quit" }, context.value)).resolves.toBeUndefined();
    expect(warnings.length).toBeGreaterThan(0);
    expect(warnings.every((warning) => /^pi-qdrant-memory: lifecycle unavailable \([a-z_]+\)\.$/u.test(warning))).toBe(true);
    expect(warnings.join(" ")).not.toContain("raw secret");
  });
  it("permits valid local-only capture and fails closed for invalid checkout identity", async () => {
    for (const [registrationValid, expectedStarts] of [[true, 1], [false, 0]] as const) {
      const lifecycle = lifecycleDouble(); const runtime = runtimeFetch("pi");
      const factory = createMemoryExtension({ env: { PI_QDRANT_MEMORY_HOST: "pi" }, argv: [], homeDir: "/home/test",
        readTextFile: async () => hostConfig(true, true), fetchImpl: runtime.fetchImpl,
        projectResolver: async () => ({ id: "local-project", label: "local", identityKind: "local_only", registrationValid, canonicalPath: "/workspace/project", fingerprint: "fingerprint-local", reason: registrationValid ? "unregistered" : "fingerprint_mismatch" }),
        lifecycleCoordinator: lifecycle.coordinator });
      const fake = fakeApi(); await factory(fake.api); const context = ctx({ header: { parentSession: null } });
      await fake.handler("session_start")({ type: "session_start", reason: "startup" }, context.value);
      await fake.handler("agent_end")({ type: "agent_end", messages: [] }, context.value);
      expect(lifecycle.calls.filter((call) => call.method === "start")).toHaveLength(expectedStarts);
      expect(lifecycle.calls.filter((call) => call.method === "capture")).toHaveLength(expectedStarts);
    }
  });

  it("rejects registered identities without an exact canonical path and fingerprint", async () => {
    for (const project of [
      { id: "project-id", label: "project", identityKind: "registered" as const, registrationValid: true, fingerprint: "fingerprint" },
      { id: "project-id", label: "project", identityKind: "registered" as const, registrationValid: true, canonicalPath: "/workspace/project" },
      { id: "project-id", label: "project", identityKind: "registered" as const, registrationValid: true, canonicalPath: "relative/project", fingerprint: "fingerprint" },
    ]) {
      const lifecycle = lifecycleDouble(); const runtime = runtimeFetch("pi");
      const factory = createMemoryExtension({ env: { PI_QDRANT_MEMORY_HOST: "pi" }, argv: [], homeDir: "/home/test", readTextFile: async () => hostConfig(true, true), fetchImpl: runtime.fetchImpl,
        projectResolver: async () => project, lifecycleCoordinator: lifecycle.coordinator });
      const fake = fakeApi(); await factory(fake.api); await fake.handler("session_start")({ type: "session_start", reason: "startup" }, ctx({ header: { parentSession: null } }).value);
      expect(lifecycle.calls.filter((call) => call.method === "start")).toHaveLength(0);
    }
  });

  it("revalidates the exact project binding before every capture and fails closed on fingerprint drift", async () => {
    const lifecycle = lifecycleDouble(); const runtime = runtimeFetch("pi"); let resolves = 0;
    const factory = createMemoryExtension({ env: { PI_QDRANT_MEMORY_HOST: "pi" }, argv: [], homeDir: "/home/test", readTextFile: async () => hostConfig(true, true), fetchImpl: runtime.fetchImpl,
      projectResolver: async () => ({ id: "project-id", label: "project", identityKind: "registered", registrationValid: resolves++ < 1, canonicalPath: "/repo", fingerprint: resolves <= 1 ? "fingerprint-a" : "fingerprint-b", ...(resolves <= 1 ? {} : { reason: "fingerprint_mismatch" as const }) }), lifecycleCoordinator: lifecycle.coordinator });
    const fake = fakeApi(); await factory(fake.api); const context = ctx({ header: { parentSession: null } });
    await fake.handler("session_start")({ type: "session_start", reason: "startup" }, context.value);
    await fake.handler("agent_end")({ type: "agent_end", messages: [] }, context.value);
    expect(lifecycle.calls.filter((call) => call.method === "start")).toHaveLength(1);
    expect(lifecycle.calls.filter((call) => call.method === "capture")).toHaveLength(0);
    expect(context.notifications).toEqual([{ message: "pi-qdrant-memory: lifecycle unavailable (capture).", type: "warning" }]);
  });

  it("requires exact canonical path and fingerprint for local-only identities", async () => {
    const invalidIdentities: ProjectIdentity[] = [
      { id: "local-project", label: "local", identityKind: "local_only", registrationValid: true, fingerprint: "fingerprint-local" },
      { id: "local-project", label: "local", identityKind: "local_only", registrationValid: true, canonicalPath: "/workspace/project" },
      { id: "local-project", label: "local", identityKind: "local_only", registrationValid: true, canonicalPath: "relative/project", fingerprint: "fingerprint-local" },
      { id: "local-project", label: "local", identityKind: "local_only", registrationValid: true, canonicalPath: "/workspace/project", fingerprint: "fingerprint-local", reason: "fingerprint_mismatch" },
      { id: "local-project", label: "local", identityKind: "local_only", registrationValid: true, canonicalPath: "/workspace/project", fingerprint: "fingerprint-local", reason: "symlink_escape" },
      { id: "registered-project", label: "registered", identityKind: "registered", registrationValid: true, canonicalPath: "/workspace/project", fingerprint: "fingerprint-registered", reason: "unregistered" },
    ];
    for (const project of invalidIdentities) {
      const lifecycle = lifecycleDouble(); const runtime = runtimeFetch("pi");
      const factory = createMemoryExtension({ env: { PI_QDRANT_MEMORY_HOST: "pi" }, argv: [], homeDir: "/home/test", readTextFile: async () => hostConfig(true, true), fetchImpl: runtime.fetchImpl,
        projectResolver: async () => project, lifecycleCoordinator: lifecycle.coordinator });
      const fake = fakeApi(); await factory(fake.api); await fake.handler("session_start")({ type: "session_start", reason: "startup" }, ctx({ header: { parentSession: null } }).value);
      expect(lifecycle.calls.filter((call) => call.method === "start")).toHaveLength(0);
    }

    const changes = [
      { canonicalPath: "/workspace/other", fingerprint: "fingerprint-local" },
      { canonicalPath: "/workspace/project", fingerprint: "fingerprint-changed" },
    ];
    for (const changed of changes) {
      const lifecycle = lifecycleDouble(); const runtime = runtimeFetch("pi"); let calls = 0;
      const factory = createMemoryExtension({ env: { PI_QDRANT_MEMORY_HOST: "pi" }, argv: [], homeDir: "/home/test", readTextFile: async () => hostConfig(true, true), fetchImpl: runtime.fetchImpl,
        projectResolver: async () => calls++ === 0
          ? { id: "local-project", label: "local", identityKind: "local_only", registrationValid: true, canonicalPath: "/workspace/project", fingerprint: "fingerprint-local" }
          : { id: "local-project", label: "local", identityKind: "local_only", registrationValid: true, ...changed }, lifecycleCoordinator: lifecycle.coordinator });
      const fake = fakeApi(); await factory(fake.api); const context = ctx({ header: { parentSession: null } });
      await fake.handler("session_start")({ type: "session_start", reason: "startup" }, context.value);
      await fake.handler("agent_end")({ type: "agent_end", messages: [] }, context.value);
      expect(lifecycle.calls.filter((call) => call.method === "capture")).toHaveLength(0);
    }
  });

  it("delegates shutdown to one bounded coordinator flush without root admission", async () => {
    const episodes = Array.from({ length: 1025 }, (_, index) => ({ id: `shutdown-${index}`, processingPolicyId: `policy-${index}`, eventKind: "user" }));
    const lifecycle = lifecycleDouble({ captureEpisodes: () => episodes }); const runtime = runtimeFetch("pi");
    const factory = createMemoryExtension({ env: { PI_QDRANT_MEMORY_HOST: "pi" }, argv: [], homeDir: "/home/test", readTextFile: async () => hostConfig(true, true), fetchImpl: runtime.fetchImpl,
      projectResolver: async () => registeredProject(), lifecycleCoordinator: lifecycle.coordinator });
    const fake = fakeApi(); await factory(fake.api); const context = ctx({ header: { parentSession: null } });
    await fake.handler("session_start")({ type: "session_start", reason: "startup" }, context.value);
    await fake.handler("session_shutdown")({ type: "session_shutdown", reason: "quit" }, context.value);
    expect(lifecycle.calls.filter((call) => call.method === "deliver")).toHaveLength(0);
    expect(lifecycle.calls.filter((call) => call.method === "scheduleRoot")).toHaveLength(0);
    expect(lifecycle.calls.filter((call) => call.method === "shutdown")).toHaveLength(1);
  });

  it("counts only tool_call episodes toward the tool trigger", async () => {
    let captureIndex = 0; const kinds = ["tool_result", "tool_error", "tool_call"] as const;
    const lifecycle = lifecycleDouble({ captureEpisodes: () => [{ id: `episode-${captureIndex}`, processingPolicyId: "policy", eventKind: kinds[captureIndex++]! }] });
    const runtime = runtimeFetch("pi"); const configured = JSON.parse(hostConfig(true, true)); configured.curation = { turnTrigger: 100, toolTrigger: 1 };
    const factory = createMemoryExtension({ env: { PI_QDRANT_MEMORY_HOST: "pi" }, argv: [], homeDir: "/home/test", readTextFile: async () => JSON.stringify(configured), fetchImpl: runtime.fetchImpl,
      projectResolver: async () => registeredProject(), lifecycleCoordinator: lifecycle.coordinator });
    const fake = fakeApi(); await factory(fake.api); const context = ctx({ header: { parentSession: null } });
    await fake.handler("session_start")({ type: "session_start", reason: "startup" }, context.value);
    await fake.handler("agent_end")({ type: "agent_end", messages: [] }, context.value);
    await fake.handler("agent_end")({ type: "agent_end", messages: [] }, context.value);
    expect(lifecycle.calls.filter((call) => call.method === "scheduleRoot")).toHaveLength(0);
    await fake.handler("agent_end")({ type: "agent_end", messages: [] }, context.value);
    expect(lifecycle.calls.filter((call) => call.method === "scheduleRoot")).toHaveLength(1);
  });

  it("keeps a valid root marker retryable and delivers backlog after transient capture admission failure", async () => {
    const lifecycle = lifecycleDouble({ failCaptureCount: 1, captureEpisodes: () => [{ id: "retry-episode", processingPolicyId: "policy", eventKind: "tool_call" }] });
    const runtime = runtimeFetch("pi"); const configured = JSON.parse(hostConfig(true, true)); configured.curation = { turnTrigger: 100, toolTrigger: 1 };
    const factory = createMemoryExtension({ env: { PI_QDRANT_MEMORY_HOST: "pi" }, argv: [], homeDir: "/home/test", readTextFile: async () => JSON.stringify(configured), fetchImpl: runtime.fetchImpl,
      projectResolver: async () => registeredProject(), lifecycleCoordinator: lifecycle.coordinator });
    const fake = fakeApi(); await factory(fake.api); const context = ctx({ header: { parentSession: null } });
    await fake.handler("session_start")({ type: "session_start", reason: "startup" }, context.value);
    await fake.handler("agent_end")({ type: "agent_end", messages: [] }, context.value);
    await fake.handler("agent_end")({ type: "agent_end", messages: [] }, context.value);
    expect(lifecycle.calls.filter((call) => call.method === "capture")).toHaveLength(2);
    expect(lifecycle.calls.filter((call) => call.method === "deliver")).toHaveLength(2);
    expect(lifecycle.calls.filter((call) => call.method === "scheduleRoot")).toHaveLength(1);
    expect(context.notifications).toEqual([{ message: "pi-qdrant-memory: lifecycle unavailable (capture).", type: "warning" }]);
  });

  it("preserves 1025 pending episodes and splits batches across at most 64 producer policies", async () => {
    const episodes = Array.from({ length: 1025 }, (_, index) => ({ id: `episode-${index}`, processingPolicyId: `policy-${index}`, eventKind: "user" }));
    const lifecycle = lifecycleDouble({ captureEpisodes: () => episodes }); const runtime = runtimeFetch("pi");
    const configured = JSON.parse(hostConfig(true, true)); configured.curation = { turnTrigger: 1, toolTrigger: 100 };
    const factory = createMemoryExtension({ env: { PI_QDRANT_MEMORY_HOST: "pi" }, argv: [], homeDir: "/home/test", readTextFile: async () => JSON.stringify(configured), fetchImpl: runtime.fetchImpl,
      projectResolver: async () => registeredProject(), lifecycleCoordinator: lifecycle.coordinator });
    const fake = fakeApi(); await factory(fake.api); const context = ctx({ header: { parentSession: null } });
    await fake.handler("session_start")({ type: "session_start", reason: "startup" }, context.value);
    await fake.handler("agent_end")({ type: "agent_end", messages: [] }, context.value);
    const batches = lifecycle.calls.filter((call) => call.method === "scheduleRoot").map((call) => (call.value as { episodes: Array<{ processingPolicyId: string }> }).episodes);
    expect(batches).toHaveLength(17);
    expect(batches.reduce((count, batch) => count + batch.length, 0)).toBe(1025);
    expect(batches.every((batch) => batch.length <= 1024 && new Set(batch.map((episode) => episode.processingPolicyId)).size <= 64)).toBe(true);
  });

});


describe("production capture coordinator", () => {
  it("persists an activation cutoff, excludes custom memory, redacts before durable enqueue, and leaves pending work closed", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "pi-qdrant-lifecycle-"));
    const env = { PI_CODING_AGENT_DIR: join(homeDir, ".pi", "agent") };
    const now = vi.fn(() => Date.parse("2029-01-01T00:00:00.000Z"));
    const runtime = runtimeFetch("pi");
    const configured = JSON.parse(hostConfig(true, true)); configured.memoryModel = { modelId: "memory-provider/memory-model", timeoutMs: 30000, maxOutputTokens: 2048 };
    const api = fakeApi();
    const factory = createMemoryExtension({ env: { ...env, PI_QDRANT_MEMORY_HOST: "pi" }, argv: [], homeDir, now, readTextFile: async () => JSON.stringify(configured), fetchImpl: runtime.fetchImpl, projectResolver: async () => registeredProject() });
    await factory(api.api);
    const entries: any[] = [{ id: "before", type: "message", message: { role: "user", content: "before cutoff", timestamp: Date.parse("2028-12-31T23:59:00.000Z") } }];
    const context = ctx({ sessionId: "session-prod", entries });
    (context.value as any).modelRegistry = { getAvailable: () => [{ id: "memory-model", provider: "memory-provider", baseUrl: "http://127.0.0.1:9998/v1" }] };
    await api.handler("session_start")({ type: "session_start", reason: "startup" }, context.value);
    const readCallsBeforeCapture = runtime.fetchImpl.mock.calls.length;
    entries.push(
      { id: "custom", type: "message", message: { role: "custom", customType: MEMORY_CONTEXT_CUSTOM_TYPE, content: "recalled memory must not persist", timestamp: Date.parse("2029-01-01T00:00:01.000Z") } },
      { id: "after", type: "message", message: { role: "user", content: "hello sk-proj-abcdefghijklmnopqrstuv", timestamp: Date.parse("2029-01-01T00:00:02.000Z") } },
    );
    await api.handler("agent_end")({ type: "agent_end", messages: [{ content: "event arrays are ignored" }] }, context.value);
    await api.handler("session_shutdown")({ type: "session_shutdown", reason: "quit" }, context.value);
    const outboxRoot = join(env.PI_CODING_AGENT_DIR, "pi-qdrant-memory", "outbox");
    const nodeNames = (await readdir(outboxRoot)).filter((name) => name.startsWith("node-"));
    expect(nodeNames).toHaveLength(1);
    const producerNames = (await readdir(join(outboxRoot, nodeNames[0]!))).filter((name) => !name.endsWith(".json"));
    expect(producerNames).toHaveLength(1);
    const producerPath = join(outboxRoot, nodeNames[0]!, producerNames[0]!);
    expect(JSON.parse(await readFile(join(producerPath, "state.json"), "utf8"))).toMatchObject({ state: "closed" });
    const jobs = await readdir(join(producerPath, "jobs"));
    expect(jobs).toHaveLength(1);
    expect((await stat(join(producerPath, "jobs", jobs[0]!))).mode & 0o077).toBe(0);
    const payload = await readFile(join(producerPath, "jobs", jobs[0]!), "utf8");
    expect(JSON.parse(payload)).toMatchObject({ deadline: "2029-01-31T00:00:02.000Z", episodes: [{ eventAt: "2029-01-01T00:00:02.000Z", expiresAt: "2029-01-31T00:00:02.000Z" }] });
    expect(payload).toContain("[api_key redacted]");
    expect(payload).toContain('"modelId":"model"');
    expect(payload).toContain('"originProvider":"provider"');
    expect(payload).not.toContain("memory-model");
    expect(payload).not.toContain("memory-provider");
    expect(payload).not.toContain("sk-proj-");
    expect(payload).not.toContain("before cutoff");
    expect(payload).not.toContain("recalled memory must not persist");
    expect(payload).not.toContain("event arrays are ignored");
    expect(runtime.fetchImpl).toHaveBeenCalledTimes(readCallsBeforeCapture);
  });

  it("keeps redacted episode capture available when no generation model is authorized", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "pi-qdrant-no-model-"));
    const env = { PI_CODING_AGENT_DIR: join(homeDir, ".pi", "agent") };
    const configured = JSON.parse(hostConfig(true, true)); configured.privacy.allowActiveModelFallback = false;
    const runtime = runtimeFetch("pi"); const api = fakeApi();
    const factory = createMemoryExtension({ env: { ...env, PI_QDRANT_MEMORY_HOST: "pi" }, argv: [], homeDir, now: () => Date.parse("2029-01-01T00:00:00.000Z"), readTextFile: async () => JSON.stringify(configured), fetchImpl: runtime.fetchImpl, projectResolver: async () => registeredProject() });
    await factory(api.api);
    const entries: any[] = [];
    const context = ctx({ sessionId: "session-no-model", entries });
    await api.handler("session_start")({ type: "session_start", reason: "startup" }, context.value);
    entries.push({ id: "after", type: "message", message: { role: "user", content: "offline episode without generation", timestamp: Date.parse("2029-01-01T00:00:01.000Z") } });
    await api.handler("agent_end")({ type: "agent_end", messages: [] }, context.value);
    await api.handler("session_shutdown")({ type: "session_shutdown", reason: "quit" }, context.value);
    const outboxRoot = join(env.PI_CODING_AGENT_DIR, "pi-qdrant-memory", "outbox");
    const node = (await readdir(outboxRoot)).find((name) => name.startsWith("node-"))!;
    const producer = (await readdir(join(outboxRoot, node))).find((name) => !name.endsWith(".json"))!;
    const jobs = await readdir(join(outboxRoot, node, producer, "jobs"));
    expect(jobs).toHaveLength(1);
    const payload = await readFile(join(outboxRoot, node, producer, "jobs", jobs[0]!), "utf8");
    expect(payload).toContain("offline episode without generation");
    expect(payload).not.toContain('"llm"');
  });

  it("creates durable shutdown work without invoking the configured memory model", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "pi-qdrant-shutdown-job-"));
    const env = { PI_CODING_AGENT_DIR: join(homeDir, ".pi", "agent"), PI_QDRANT_MEMORY_HOST: "pi" };
    const configured = JSON.parse(hostConfig(true, true)); configured.curation = { turnTrigger: 100, toolTrigger: 100 }; configured.memoryModel = { modelId: "memory-provider/memory-model", timeoutMs: 30000, maxOutputTokens: 2048 };
    const runtime = runtimeFetch("pi", { includeLlm: false }); vi.stubGlobal("fetch", runtime.fetchImpl);
    try {
      const api = fakeApi(); const base = Date.now(); let completions = 0;
      const factory = createMemoryExtension({ env, argv: [], homeDir, now: () => base, readTextFile: async () => JSON.stringify(configured), projectResolver: async () => registeredProject() });
      await factory(api.api); const entries: any[] = []; const context = ctx({ sessionId: "session-shutdown-job", entries, header: null });
      (context.value as any).modelRegistry = { getAvailable: () => [{ id: "memory-model", provider: "memory-provider", baseUrl: "http://127.0.0.1:9998/v1" }], complete: async () => { completions += 1; throw new Error("model must not run during shutdown"); } };
      await api.handler("session_start")({ type: "session_start", reason: "startup" }, context.value);
      entries.push({ id: "shutdown-entry", type: "message", message: { role: "user", content: "durable shutdown episode", timestamp: base + 1 } });
      await api.handler("agent_end")({ type: "agent_end", messages: [] }, context.value);
      await api.handler("session_shutdown")({ type: "session_shutdown", reason: "quit" }, context.value);
      expect(completions).toBe(0);
      const payloads = [...runtime.points.values()].map((point) => point.payload as Record<string, unknown>);
      expect(payloads.some((payload) => payload.record_type === "episode" && payload.text === "durable shutdown episode")).toBe(true);
    } finally { vi.unstubAllGlobals(); }
  });

  it("filters prior privacy-epoch episodes during durable recovery", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "pi-qdrant-privacy-recovery-"));
    const env = { PI_CODING_AGENT_DIR: join(homeDir, ".pi", "agent"), PI_QDRANT_MEMORY_HOST: "pi" };
    const configured = JSON.parse(hostConfig(true, true)); configured.curation = { turnTrigger: 1, toolTrigger: 1 }; configured.memoryModel = { modelId: "memory-provider/memory-model", timeoutMs: 30000, maxOutputTokens: 2048 };
    const runtime = runtimeFetch("pi", { includeLlm: false });
    const seededEpisode = [...runtime.points.entries()].find(([, point]) => point.payload?.record_type === "episode");
    expect(seededEpisode).toBeDefined(); runtime.points.set(seededEpisode![0], { ...seededEpisode![1], payload: { ...seededEpisode![1].payload, privacy_epoch: -1 } });
    vi.stubGlobal("fetch", runtime.fetchImpl);
    try {
      const api = fakeApi(); let registryCalls = 0;
      const factory = createMemoryExtension({ env, argv: [], homeDir, readTextFile: async () => JSON.stringify(configured), projectResolver: async () => registeredProject() });
      await factory(api.api); const context = ctx({ sessionId: "session-privacy-recovery", entries: [], header: null });
      (context.value as any).modelRegistry = { getAvailable: () => { registryCalls += 1; return []; } };
      await api.handler("session_start")({ type: "session_start", reason: "startup" }, context.value); const registryCallsAfterStart = registryCalls;
      expect(runtime.calls.some((call) => new URL(call.url).pathname.endsWith("/points/scroll") && call.body?.filter?.must?.some((condition: any) => condition.key === "privacy_epoch" && condition.match?.value === 0))).toBe(true);
      expect([...runtime.points.values()].some((point) => point.payload?.record_type === "job")).toBe(false);
      expect(registryCalls).toBe(registryCallsAfterStart);
      await api.handler("session_shutdown")({ type: "session_shutdown", reason: "quit" }, context.value);
    } finally { vi.unstubAllGlobals(); }
  });

  it("snapshots capture authorization without registry access and retries after a durable job readback", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "pi-qdrant-model-order-"));
    const env = { PI_CODING_AGENT_DIR: join(homeDir, ".pi", "agent"), PI_QDRANT_MEMORY_HOST: "pi" };
    const configured = JSON.parse(hostConfig(true, true)); configured.curation = { turnTrigger: 1, toolTrigger: 100 }; configured.memoryModel = { modelId: "memory-provider/memory-model", timeoutMs: 30000, maxOutputTokens: 2048 };
    const exactLlmDestination = destinationForEndpoint("http://127.0.0.1:9999/v1", "node-test", { residency: "local", dataUse: "memory" });
    configured.privacy.allowedLlmDestinations = [{ id: exactLlmDestination.id, residency: exactLlmDestination.residency, dataUse: exactLlmDestination.dataUse }];
    const runtime = runtimeFetch("pi", { originProvider: "memory-provider" });
    for (const [id, point] of runtime.points) if (point.payload?.record_type === "episode") runtime.points.delete(id);
    vi.stubGlobal("fetch", runtime.fetchImpl);
    try {
      const api = fakeApi(); const base = Date.now(); let registryCalls = 0; let registryAvailable = false; let completions = 0;
      const factory = createMemoryExtension({ env, argv: [], homeDir, now: () => base, readTextFile: async () => JSON.stringify(configured), projectResolver: async () => registeredProject() });
      await factory(api.api); const manager = SessionManager.inMemory("/workspace/project"); const context = ctx({ header: null, model: { id: "active-model", provider: "memory-provider", baseUrl: "http://127.0.0.1:9998/v1" } });
      (context.value as any).sessionManager = manager;
      (context.value as any).modelRegistry = {
        getAvailable: () => {
          registryCalls += 1;
          const hasJob = [...runtime.points.values()].some((point) => point.payload?.record_type === "job");
          if (!hasJob) throw new Error("registry consulted before exact job readback");
          if (!registryAvailable) throw new Error("transient registry outage");
          return [{ id: "memory-model", provider: "memory-provider", baseUrl: "http://127.0.0.1:9999/v1", contextWindow: 100000, maxTokens: 8192 }];
        },
        getApiKeyAndHeaders: async () => ({ ok: true }),
        complete: async () => {
          completions += 1;
          const job = [...runtime.points.values()].find((point) => point.payload?.record_type === "job");
          const evidence = job?.payload?.membership?.[0];
          return { content: [{ type: "text", text: JSON.stringify({ items: [{ category: "fact", scope: "project", subject: "registry", predicate: "recovered", value: true, evidence: [evidence] }] }) }] };
        },
      };
      await api.handler("session_start")({ type: "session_start", reason: "startup" }, context.value);
      expect(registryCalls).toBe(0);
      manager.appendMessage(user("durable before registry", base + 1));
      await api.handler("agent_end")({ type: "agent_end", messages: [] }, context.value);
      expect(registryCalls).toBe(1);
      expect([...runtime.points.values()].some((point) => point.payload?.record_type === "job")).toBe(true);
      expect(completions).toBe(0);
      registryAvailable = true;
      await api.handler("agent_end")({ type: "agent_end", messages: [] }, context.value);
      expect(registryCalls).toBe(2);
      expect(completions).toBe(1);
      expect([...runtime.points.values()].some((point) => point.payload?.record_type === "curated_memory")).toBe(true);
      await api.handler("session_shutdown")({ type: "session_shutdown", reason: "quit" }, context.value);
      expect(registryCalls).toBe(2);
    } finally { vi.unstubAllGlobals(); }
  });

  it("returns the awaited production host handler while a varied high-cardinality RAPTOR build remains outstanding", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "pi-qdrant-raptor-background-")); const env = { PI_CODING_AGENT_DIR: join(homeDir, ".pi", "agent"), PI_QDRANT_MEMORY_HOST: "pi" }; const base = Date.parse("2026-08-13T16:00:00.000Z");
    const configured = JSON.parse(hostConfig(true, true)); configured.curation = { turnTrigger: 1, toolTrigger: 100, maxInputTokens: 12000 }; configured.raptor = { rebuildEpisodeDelta: 2, maxLevels: 3, summaryInputTokens: 12000, umapDimensions: 10, localNeighbors: 5, gmmMaxClusters: 4, membershipThreshold: 0.1 }; configured.memoryModel = { modelId: "provider/memory-model", timeoutMs: 30000, maxOutputTokens: 2048 };
    const exactLlmDestination = destinationForEndpoint("http://127.0.0.1:9999/v1", "node-test", { residency: "local", dataUse: "memory" }); configured.privacy.allowedLlmDestinations = [{ id: exactLlmDestination.id, residency: exactLlmDestination.residency, dataUse: exactLlmDestination.dataUse }];
    const runtime = runtimeFetch("pi"); const seedPolicyId = [...runtime.points.values()].find((point) => point.payload?.record_type === "processing_policy")!.payload.processing_policy_id as string;
    for (const [id, point] of runtime.points) if (point.payload?.record_type === "episode") runtime.points.delete(id);
    const seedPoints: Array<{ id: string; payload: ReturnType<typeof recordPayload>; vector: { semantic: readonly number[] } }> = [];
    for (let index = 0; index < 65; index += 1) {
      const id = `22222222-2222-5222-8222-${String(index).padStart(12, "0")}`; const eventAt = new Date(base - 10_000 + index).toISOString();
      const record = { recordType: "episode", id, ownerHost: "pi", schemaRevision: 1, createdAt: eventAt, privacyEpoch: 0, processingPolicyId: seedPolicyId, expiresAt: null, sourceEntryId: `seed-${index}`, host: "pi", projectId: "project-id", projectIdentityKind: "registered", sessionId: "session-raptor-background", turnId: `turn-${index}`, agentRole: "root", depth: 0, eventKind: "user", eventAt, modelId: "model", embeddingDimension: 1024, originProvider: "provider", destinationId: exactLlmDestination.id, status: "active", redactionStatus: "unchanged", secretScan: "passed", text: `Varied durable evidence ${index}`, vector: Array.from({ length: 1024 }, (_unused, dimension) => Math.sin(index * 0.17 + dimension * 0.013) + Math.cos(index * dimension * 0.0007)), contentHash: "pending" } satisfies EpisodeRecord;
      const canonical = { ...record, contentHash: canonicalRecordHash(record) }; seedPoints.push({ id, payload: recordPayload(canonical), vector: { semantic: canonical.vector! } });
    }
    vi.stubGlobal("fetch", runtime.fetchImpl);
    const api = fakeApi(); let completionCount = 0;
    const factory = createMemoryExtension({ env, argv: [], homeDir, now: () => base, readTextFile: async () => JSON.stringify(configured), projectResolver: async () => registeredProject() }); await factory(api.api);
    const manager = SessionManager.inMemory("/workspace/project"); const context = ctx({ sessionId: "session-raptor-background", header: null, model: { id: "active-model", provider: "provider", baseUrl: "http://127.0.0.1:9998/v1" } }); (context.value as any).sessionManager = manager;
    (context.value as any).modelRegistry = { getAvailable: () => [{ id: "memory-model", provider: "provider", baseUrl: "http://127.0.0.1:9999/v1", contextWindow: 100000, maxTokens: 8192 }], getApiKeyAndHeaders: async () => ({ ok: true }), complete: async () => { completionCount += 1; if (completionCount === 1) { const job = [...runtime.points.values()].find((point) => point.payload?.record_type === "job"); const evidence = job?.payload?.membership?.[0]; return { content: [{ type: "text", text: JSON.stringify({ items: [{ category: "fact", scope: "project", subject: "background", predicate: "curated", value: true, evidence: [evidence] }] }) }] }; } return await new Promise<never>(() => undefined); } };
    await api.handler("session_start")({ type: "session_start", reason: "startup" }, context.value); for (const point of seedPoints) runtime.points.set(point.id, point); manager.appendMessage(user("trigger background raptor", base + 1));
    const handlerResult = await api.handler("agent_end")({ type: "agent_end", messages: [] }, context.value); const completionsAtHandlerReturn = completionCount;
    // Curation completed, while the admitted full-corpus RAPTOR generation has
    // neither published nor added a second awaited completion to this turn.
    expect(handlerResult).toBeUndefined(); expect(completionsAtHandlerReturn).toBe(1); expect([...runtime.points.values()].some((point) => point.payload?.record_type === "raptor_manifest")).toBe(false);
    await api.handler("session_shutdown")({ type: "session_shutdown", reason: "quit" }, context.value); expect([...runtime.points.values()].some((point) => point.payload?.record_type === "raptor_manifest")).toBe(false); vi.unstubAllGlobals();
  }, 15_000);

  it("uses the production mutation path to persist a no-model curation job before shutdown", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "pi-qdrant-production-job-"));
    const env = { PI_CODING_AGENT_DIR: join(homeDir, ".pi", "agent"), PI_QDRANT_MEMORY_HOST: "pi" };
    const configured = JSON.parse(hostConfig(true, true)); configured.privacy.allowActiveModelFallback = false; configured.curation = { turnTrigger: 1, toolTrigger: 100 };
    const runtime = runtimeFetch("pi", { includeLlm: false }); vi.stubGlobal("fetch", runtime.fetchImpl);
    try {
      const api = fakeApi(); const base = Date.now();
      const factory = createMemoryExtension({ env, argv: [], homeDir, now: () => base, readTextFile: async () => JSON.stringify(configured), projectResolver: async () => registeredProject() });
      await factory(api.api); const entries: any[] = []; const context = ctx({ sessionId: "session-production-job", entries, header: null });
      await api.handler("session_start")({ type: "session_start", reason: "startup" }, context.value);
      entries.push({ id: "production-entry", type: "message", message: { role: "user", content: "durable without a generation model", timestamp: base + 1 } });
      await api.handler("agent_end")({ type: "agent_end", messages: [] }, context.value);
      const payloads = [...runtime.points.values()].map((point) => point.payload as Record<string, unknown>);
      expect(payloads.some((payload) => payload.record_type === "episode" && payload.text === "durable without a generation model")).toBe(true);
      expect(payloads.some((payload) => payload.record_type === "processing_policy")).toBe(true);
      expect(payloads.some((payload) => payload.record_type === "job" && Array.isArray(payload.membership) && payload.membership.length === 1)).toBe(true);
      expect(runtime.calls.some((call) => new URL(call.url).pathname.endsWith("/points") && call.body?.points?.some((point: any) => point.payload?.record_type === "job"))).toBe(true);
      await api.handler("session_shutdown")({ type: "session_shutdown", reason: "quit" }, context.value);
    } finally { vi.unstubAllGlobals(); }
  });

  it("fails closed with one bounded warning when a divergent activation winner would rebrand this session", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "pi-qdrant-divergent-activation-"));
    const env = { PI_CODING_AGENT_DIR: join(homeDir, ".pi", "agent"), PI_QDRANT_MEMORY_HOST: "pi" };
    const configured = JSON.parse(hostConfig(true, true)); configured.curation = { turnTrigger: 1, toolTrigger: 100 };
    const runtime = runtimeFetch("pi", { bootstrap: true, divergentActivation: true }); vi.stubGlobal("fetch", runtime.fetchImpl);
    try {
      const api = fakeApi(); const base = Date.now();
      const factory = createMemoryExtension({ env, argv: [], homeDir, now: () => base, readTextFile: async () => JSON.stringify(configured), projectResolver: async () => registeredProject() });
      await factory(api.api); const entries: any[] = []; const context = ctx({ sessionId: "session-divergent-activation", entries, header: null });
      await api.handler("session_start")({ type: "session_start", reason: "startup" }, context.value);
      expect(context.notifications).toEqual([{ message: "pi-qdrant-memory: worker policy activation pending; capture remains buffered locally.", type: "warning" }]);
      await api.handler("session_start")({ type: "session_start", reason: "reload" }, context.value);
      // The activation failure surfaces exactly once as a bounded, sanitized
      // warning; internals never leak into the notification.
      expect(context.notifications).toEqual([{ message: "pi-qdrant-memory: worker policy activation pending; capture remains buffered locally.", type: "warning" }]);
      entries.push({ id: "divergent-entry", type: "message", message: { role: "user", content: "divergent winner episode", timestamp: base + 1 } });
      await api.handler("agent_end")({ type: "agent_end", messages: [] }, context.value);
      await api.handler("session_shutdown")({ type: "session_shutdown", reason: "quit" }, context.value);
      // The losing session never delivers, schedules, or curates under the
      // winner's coordination identity: nothing leaves the durable outbox.
      const payloads = [...runtime.points.values()].map((point) => point.payload as Record<string, unknown>);
      expect(payloads.some((payload) => payload.record_type === "episode" && payload.text === "divergent winner episode")).toBe(false);
      expect(payloads.some((payload) => payload.record_type === "job")).toBe(false);
      expect(payloads.some((payload) => payload.record_type === "curated_memory")).toBe(false);
      const outboxRoot = join(env.PI_CODING_AGENT_DIR, "pi-qdrant-memory", "outbox");
      const node = (await readdir(outboxRoot)).find((name) => name.startsWith("node-"))!;
      const producers = (await readdir(join(outboxRoot, node))).filter((name) => !name.endsWith(".json"));
      const jobs = (await Promise.all(producers.map((producer) => readdir(join(outboxRoot, node, producer, "jobs"))))).flat();
      expect(jobs).toHaveLength(1);
    } finally { vi.unstubAllGlobals(); }
  });

});

function bootstrapControlFixture(host: "pi" | "prime" = "pi"): ControlRecord {
  const base = { ownerHost: host, schemaRevision: 1 as const, createdAt: "2026-08-16T00:00:00.000Z", privacyEpoch: 0, processingPolicyId: V2_CONTRACT_HASH, expiresAt: null, recordType: "collection_control" as const, id: COLLECTION_CONTROL_ID, version: 0, activeGeneration: null, activeBaseGeneration: null, coordinationPolicyEpoch: 0, coordinationPolicyHash: V2_CONTRACT_HASH, state: "active" as const, scanCursor: null, lastForgetBarrier: null, revokedDestinationIds: [] as string[], contentHash: "pending" };
  return { ...base, contentHash: bootstrapControlHash(base as ControlRecord) };
}

function workerPolicyFixture(host: "pi" | "prime" = "pi", expiresAt: string | null = null): ProcessingPolicy {
  const pending: ProcessingPolicy = { id: "pending", ownerHost: host, destinationIds: { qdrant: "qdrant-dest", embedding: "embed-dest", llm: "llm-dest" }, originProvider: "provider", allowCrossProviderReplay: false, expiresAt, residency: "local", dataUse: "memory", policyRevision: "capture-lifecycle-v1" };
  return { ...pending, id: processingPolicyHash(pending) };
}

function activationIo(control: ControlRecord, overrides: Partial<{
  readPolicy: (id: string) => Promise<ProcessingPolicyRecord | null>;
  insertPolicy: (record: ProcessingPolicyRecord) => Promise<unknown>;
  activate: (workerPolicyId: string) => Promise<ControlRecord | false>;
  readControl: () => Promise<ControlRecord>;
}> = {}) {
  const events: string[] = [];
  const inserted: ProcessingPolicyRecord[] = [];
  const activateCalls: string[] = [];
  const winnerFor = (policyId: string): ControlRecord => {
    const pending = { ...control, version: 1, processingPolicyId: policyId, coordinationPolicyEpoch: 1, coordinationPolicyHash: policyId, contentHash: "pending" };
    return { ...pending, contentHash: canonicalRecordHash(pending as ControlRecord) } as ControlRecord;
  };
  const io = {
    readPolicy: async (id: string) => { events.push("readPolicy"); return overrides.readPolicy === undefined ? null : overrides.readPolicy(id); },
    insertPolicy: async (record: ProcessingPolicyRecord) => { events.push("insert"); inserted.push(record); if (overrides.insertPolicy !== undefined) return overrides.insertPolicy(record); },
    activate: async (workerPolicyId: string) => { events.push("activate"); activateCalls.push(workerPolicyId); return overrides.activate === undefined ? winnerFor(workerPolicyId) : overrides.activate(workerPolicyId); },
    readControl: overrides.readControl ?? (async () => { throw new Error("readControl not stubbed"); }),
  };
  return { events, inserted, activateCalls, winnerFor, io };
}

describe("bootstrap worker policy activation", () => {
  it("persists the exact worker policy record before the verified activation transition", async () => {
    const control = bootstrapControlFixture();
    const policy = workerPolicyFixture();
    const harness = activationIo(control);
    const result = await activateBootstrapWorkerPolicy({ control, policy, now: () => Date.parse("2026-08-16T01:00:00.000Z"), io: harness.io });
    expect(result).toStrictEqual(harness.winnerFor(policy.id));
    expect(harness.events).toEqual(["readPolicy", "insert", "activate"]);
    expect(harness.inserted).toHaveLength(1);
    expect(harness.inserted[0]).toMatchObject({ recordType: "processing_policy", id: policy.id, ownerHost: "pi", schemaRevision: 1, createdAt: "2026-08-16T01:00:00.000Z", privacyEpoch: 0, processingPolicyId: policy.id, expiresAt: null, canonicalHash: policy.id });
    expect(harness.inserted[0]!.policy).toEqual(policy);
    expect(harness.inserted[0]!.contentHash).toBe(canonicalRecordHash(harness.inserted[0]!));
    expect(harness.activateCalls).toEqual([policy.id]);
    expect(result).toMatchObject({ id: COLLECTION_CONTROL_ID, ownerHost: "pi", version: 1, processingPolicyId: policy.id, coordinationPolicyEpoch: 1, coordinationPolicyHash: policy.id, privacyEpoch: 0, state: "active", activeGeneration: null, revokedDestinationIds: [] });
  });

  it("satisfies the admin enqueue precondition after activation", async () => {
    const control = bootstrapControlFixture();
    const policy = workerPolicyFixture();
    const harness = activationIo(control);
    const result = await activateBootstrapWorkerPolicy({ control, policy, io: harness.io });
    // src/admin/production.ts requires the active worker policy record to exist for control.processingPolicyId.
    expect(result.processingPolicyId).toBe(policy.id);
    expect(harness.inserted.map((record) => record.id)).toContain(result.processingPolicyId);
  });

  it("converges without a write when the worker policy record already exists with first-writer provenance", async () => {
    const control = bootstrapControlFixture();
    const policy = workerPolicyFixture();
    const pendingRecord = { recordType: "processing_policy" as const, id: policy.id, ownerHost: "pi" as const, schemaRevision: 1 as const, createdAt: "2026-08-15T00:00:30.000Z", privacyEpoch: 0, processingPolicyId: policy.id, expiresAt: null, contentHash: "pending", policy, canonicalHash: policy.id };
    const existing: ProcessingPolicyRecord = { ...pendingRecord, contentHash: canonicalRecordHash(pendingRecord as ProcessingPolicyRecord) };
    const harness = activationIo(control, { readPolicy: async () => existing });
    await expect(activateBootstrapWorkerPolicy({ control, policy, io: harness.io })).resolves.toStrictEqual(harness.winnerFor(policy.id));
    expect(harness.inserted).toHaveLength(0);
    expect(harness.activateCalls).toEqual([policy.id]);
  });

  it("fails closed when a same-ID worker policy record has a different content hash", async () => {
    const control = bootstrapControlFixture();
    const policy = workerPolicyFixture();
    const collidingPending = { recordType: "processing_policy" as const, id: policy.id, ownerHost: "pi" as const, schemaRevision: 1 as const, createdAt: "2026-08-15T00:00:30.000Z", privacyEpoch: 0, processingPolicyId: policy.id, expiresAt: null, contentHash: "pending", policy, canonicalHash: policy.id };
    const colliding: ProcessingPolicyRecord = { ...collidingPending, contentHash: "f".repeat(64) };
    const harness = activationIo(control, { readPolicy: async () => colliding });
    await expect(activateBootstrapWorkerPolicy({ control, policy, io: harness.io })).rejects.toThrow(/content hash collision/i);
    expect(harness.inserted).toHaveLength(0);
    expect(harness.activateCalls).toHaveLength(0);
  });

  it("is idempotent for an already-activated v1 control bound to the same worker policy", async () => {
    const policy = workerPolicyFixture();
    const controlPending = { ...bootstrapControlFixture(), version: 1, processingPolicyId: policy.id, coordinationPolicyEpoch: 1, coordinationPolicyHash: policy.id, contentHash: "pending" };
    const control: ControlRecord = { ...controlPending, contentHash: canonicalRecordHash(controlPending as ControlRecord) };
    const harness = activationIo(control);
    await expect(activateBootstrapWorkerPolicy({ control, policy, io: harness.io })).resolves.toBe(control);
    expect(harness.events).toEqual([]);
  });

  it("fails closed when an already-activated control belongs to a divergent worker policy", async () => {
    const policy = workerPolicyFixture();
    const other = workerPolicyFixture("pi", "2030-01-01T00:00:00.000Z");
    const controlPending = { ...bootstrapControlFixture(), version: 1, processingPolicyId: other.id, coordinationPolicyEpoch: 1, coordinationPolicyHash: other.id, contentHash: "pending" };
    const control: ControlRecord = { ...controlPending, contentHash: canonicalRecordHash(controlPending as ControlRecord) };
    const harness = activationIo(control);
    await expect(activateBootstrapWorkerPolicy({ control, policy, io: harness.io })).rejects.toThrow(/does not match/i);
    expect(harness.events).toEqual([]);
  });

  it("rejects an already-activated control with a forged content hash", async () => {
    const policy = workerPolicyFixture();
    const control = { ...activationIo(bootstrapControlFixture()).winnerFor(policy.id), contentHash: "0".repeat(64) };
    const harness = activationIo(control);
    await expect(activateBootstrapWorkerPolicy({ control, policy, io: harness.io })).rejects.toThrow(/does not match/i);
    expect(harness.events).toEqual([]);
  });

  it("fails closed for a non-active v0 control instead of activating it", async () => {
    const control: ControlRecord = { ...bootstrapControlFixture(), state: "draining" };
    const harness = activationIo(control);
    await expect(activateBootstrapWorkerPolicy({ control, policy: workerPolicyFixture(), io: harness.io })).rejects.toThrow(/does not match/i);
    expect(harness.events).toEqual([]);
  });

  it("rejects a forged v0 bootstrap before any write", async () => {
    const control: ControlRecord = { ...bootstrapControlFixture(), contentHash: "0".repeat(64) };
    const harness = activationIo(control);
    await expect(activateBootstrapWorkerPolicy({ control, policy: workerPolicyFixture(), io: harness.io })).rejects.toThrow(/bootstrap/i);
    expect(harness.inserted).toHaveLength(0);
    expect(harness.activateCalls).toHaveLength(0);
  });

  it("rejects a forged v0 bootstrap whose self-hashed identities point at a real policy", async () => {
    const policy = workerPolicyFixture();
    // A forged bootstrap claiming real policy identities under a self-consistent
    // hash must fail: genuine v0 placeholders are exactly V2_CONTRACT_HASH.
    const forgedBase = { ...bootstrapControlFixture(), processingPolicyId: policy.id, coordinationPolicyHash: policy.id, contentHash: "pending" };
    const forged: ControlRecord = { ...forgedBase, contentHash: bootstrapControlHash(forgedBase as ControlRecord) };
    const harness = activationIo(forged);
    await expect(activateBootstrapWorkerPolicy({ control: forged, policy, io: harness.io })).rejects.toThrow(/bootstrap/i);
    expect(harness.events).toEqual([]);
  });

  it("rejects a worker policy whose content does not match its address or host", async () => {
    const control = bootstrapControlFixture();
    const wrongHost = workerPolicyFixture("prime");
    const harness = activationIo(control);
    await expect(activateBootstrapWorkerPolicy({ control, policy: wrongHost, io: harness.io })).rejects.toThrow(/worker policy/i);
    expect(harness.events).toEqual([]);
  });

  it("converges on a same-policy race winner without retrying", async () => {
    const control = bootstrapControlFixture();
    const policy = workerPolicyFixture();
    const harness = activationIo(control, { activate: async () => false, readControl: async () => harness.winnerFor(policy.id) });
    await expect(activateBootstrapWorkerPolicy({ control, policy, io: harness.io })).resolves.toMatchObject({ version: 1, processingPolicyId: policy.id, coordinationPolicyHash: policy.id });
    expect(harness.activateCalls).toEqual([policy.id]);
    expect(harness.inserted).toHaveLength(1);
  });

  it("fails closed on a divergent race winner instead of rebranding to it", async () => {
    const control = bootstrapControlFixture();
    const policy = workerPolicyFixture();
    const other = workerPolicyFixture("pi", "2030-01-01T00:00:00.000Z");
    const harness = activationIo(control, { activate: async () => false, readControl: async () => harness.winnerFor(other.id) });
    await expect(activateBootstrapWorkerPolicy({ control, policy, io: harness.io })).rejects.toThrow(/did not converge/i);
    expect(harness.activateCalls).toEqual([policy.id]);
  });

  it("fails closed on a foreign-host race winner", async () => {
    const control = bootstrapControlFixture();
    const policy = workerPolicyFixture();
    const harness = activationIo(control, { activate: async () => false, readControl: async () => {
      const pending = { ...harness.winnerFor(policy.id), ownerHost: "prime" as const, contentHash: "pending" };
      return { ...pending, contentHash: canonicalRecordHash(pending as ControlRecord) };
    } });
    await expect(activateBootstrapWorkerPolicy({ control, policy, io: harness.io })).rejects.toThrow(/did not converge/i);
  });

  it("propagates policy-insert failure without attempting activation", async () => {
    const control = bootstrapControlFixture();
    const harness = activationIo(control, { insertPolicy: async () => { throw new Error("insert failed"); } });
    await expect(activateBootstrapWorkerPolicy({ control, policy: workerPolicyFixture(), io: harness.io })).rejects.toThrow("insert failed");
    expect(harness.activateCalls).toHaveLength(0);
  });

  it("propagates activation transport failure", async () => {
    const control = bootstrapControlFixture();
    const harness = activationIo(control, { activate: async () => { throw new Error("cas failed"); } });
    await expect(activateBootstrapWorkerPolicy({ control, policy: workerPolicyFixture(), io: harness.io })).rejects.toThrow("cas failed");
    expect(harness.inserted).toHaveLength(1);
  });

  it("keeps bounded retention expiry on the worker policy record and preserves intersection identity", async () => {
    const control = bootstrapControlFixture();
    const policy = workerPolicyFixture("pi", "2030-01-01T00:00:00.000Z");
    const harness = activationIo(control, { activate: async () => { throw new Error("stop after insert"); } });
    await expect(activateBootstrapWorkerPolicy({ control, policy, io: harness.io })).rejects.toThrow("stop after insert");
    expect(harness.inserted[0]!.expiresAt).toBe("2030-01-01T00:00:00.000Z");
    // The lifecycle drain path and the admin enqueue path must compute the same intersection identity.
    expect(intersectPolicies([policy], policy)?.id).toBe(policy.id);
  });
});
