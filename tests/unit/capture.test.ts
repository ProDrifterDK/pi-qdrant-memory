import { mkdtemp, readFile, stat, symlink, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, relative } from "node:path";
import { describe, expect, it, vi } from "vitest";
import {
  CAPTURE_LIFECYCLES,
  activateCapture,
  capturePersistedEntries,
  clearCaptureActivation,
  persistCaptureActivationFile,
  resolveAgentMarker,
  captureRootWorkAllowed,
  resolveCaptureAgentDirectory,
} from "../../src/capture/episode.js";
import { selectPersistedEntries } from "../../src/capture/select.js";
import { parsePersistedMemoryRecord } from "../../src/domain/records.js";
import { sha256Hex } from "../../src/domain/canonical.js";

type Entry = { id: string; type: string; message?: unknown; timestamp?: string };
const msg = (id: string, role: string, content: unknown, extra: Record<string, unknown> = {}): Entry => ({ id, type: "message", message: { role, content, ...extra } });

describe("Task 4 persisted capture", () => {
  it("selects only finalized persisted entries and excludes system/memory/thinking/partials", () => {
    const result = selectPersistedEntries([
      msg("system", "system", "never"), msg("dev", "developer", "never"), msg("custom", "custom", "never"),
      msg("memory", "user", "[memory_context] injected"),
      msg("think", "assistant", [{ type: "thinking", thinking: "private" }]),
      msg("partial", "assistant", "partial", { partial: true }),
      msg("u", "user", "final user"), msg("a", "assistant", "final assistant"),
      msg("call", "assistant", [{ type: "toolCall", name: "shell", arguments: { command: "ls" } }]),
      msg("memory-tool", "assistant", [{ type: "toolCall", name: "memory_search", arguments: { query: "x" } }]),
      msg("tool", "toolResult", [{ type: "text", text: "output" }, { type: "json", value: { status: "ok", code: 0 } }], { toolName: "shell", isError: false, status: "completed" }),
      msg("tool-error", "toolResult", [{ type: "text", text: "stderr boom" }], { toolName: "shell", isError: true, status: "error", stderr: "stderr boom", code: 2 }),
      msg("tool-partial", "toolResult", "partial", { partial: true }),
      msg("tool-memory", "toolResult", "memory", { toolName: "memory_search", status: "completed" }),
    ]);
    expect(result.map((entry) => entry.sourceEntryId)).toEqual(["u", "a", "call", "tool", "tool-error"]);
    const selectedSecret = selectPersistedEntries([msg("raw", "user", "Authorization: Bearer raw-secret-value-123456")]);
    expect(JSON.stringify(selectedSecret)).not.toContain("raw-secret-value-123456");
    expect(result.find((entry) => entry.sourceEntryId === "call")?.eventKind).toBe("tool_call");
    expect(result.find((entry) => entry.sourceEntryId === "tool-error")?.eventKind).toBe("tool_error");
  });

  it("excludes all memory-context wrapper forms and rejects array headers/contradictory Pi markers", () => {
    const result = selectPersistedEntries([
      msg("bracket", "assistant", "[memory-context] injected"),
      msg("legacy-bracket", "assistant", "before [memory-context] after"),
    ]);
    expect(result).toEqual([]);
    expect(resolveAgentMarker({ host: "prime", header: [] }).valid).toBe(false);
    expect(resolveAgentMarker({ host: "pi", header: { parentSession: null }, env: { PI_SUBAGENT_CHILD: "0", PI_SUBAGENT_DEPTH: "1" } })).toMatchObject({ valid: false, role: "child", rootWorkAllowed: false });
  });

  it("excludes memory wrappers and successful tool bodies, retaining only allowlisted fields/errors", () => {
    const result = selectPersistedEntries([
      msg("wrapped", "assistant", [{ type: "text", text: '<memory-context trust="untrusted">secret injected</memory-context>' }]),
      { id: "custom-memory", type: "custom", message: { role: "custom", customType: "pi-qdrant-memory-context", content: "hidden" } },
      msg("legacy", "assistant", [{ type: "text", text: "before [memory_context] after" }]),
      msg("success", "toolResult", "FULL SUCCESS BODY MUST NOT SURVIVE", { toolName: "shell", status: "completed", code: 0, stderr: "" }),
      msg("failure", "toolResult", "failure body", { toolName: "shell", status: "failed", code: 2, stderr: "bounded stderr" }),
    ]);
    expect(result.map((entry) => entry.sourceEntryId)).toEqual(["success", "failure"]);
    expect(result.find((entry) => entry.sourceEntryId === "failure")?.text).toContain("stderr");
    expect(JSON.stringify(result)).not.toContain("FULL SUCCESS BODY"); expect(result.find((entry) => entry.sourceEntryId === "success")?.text).not.toContain("FULL SUCCESS BODY");
    expect(JSON.stringify(result)).not.toContain("secret injected");
  });

  it("keeps successful tool results to strict metadata and promotes nonempty errors", () => {
    const success = selectPersistedEntries([msg("tool-success-meta", "tool", "TOP SECRET SUCCESS PAYLOAD", { toolName: "shell", status: "completed", code: 0, stderr: "warning", message: "TOP SECRET MESSAGE" })]);
    expect(success).toHaveLength(1); expect(success[0]?.eventKind).toBe("tool_result"); expect(success[0]?.text).toContain("status: completed"); expect(success[0]?.text).toContain("code: 0"); expect(success[0]?.text).not.toContain("TOP SECRET");
    const error = selectPersistedEntries([msg("tool-error-contradiction", "tool", "useful output", { toolName: "shell", status: "completed", code: 0, error: { detail: "short failure" } })]);
    expect(error).toHaveLength(1); expect(error[0]?.eventKind).toBe("tool_error"); expect(error[0]?.text).toContain("useful output");
    const detailsError = selectPersistedEntries([msg("tool-details-error", "tool", "details body", { toolName: "shell", status: "completed", code: 0, details: { status: "completed", code: 0, error: { reason: "failed" } } })]);
    expect(detailsError).toHaveLength(1); expect(detailsError[0]?.eventKind).toBe("tool_error"); expect(detailsError[0]?.text).toContain("details body");
    const timeout = selectPersistedEntries([msg("tool-timeout", "tool", "timeout diagnostic", { toolName: "shell", status: "timed_out" })]);
    expect(timeout).toHaveLength(1); expect(timeout[0]?.eventKind).toBe("tool_error"); expect(timeout[0]?.text).toContain("timeout diagnostic"); expect(timeout[0]?.errorFingerprint).toMatch(/^[a-f0-9]{32}$/u);
  });

  it("retains finalized tool calls whose safe name has no arguments", async () => {
    const state = new Map<string, string>(); const entries: Entry[] = [msg("before-tool-noargs", "user", "old")];
    const deps = { sessionId: "tool-noargs", host: "pi" as const, getEntries: () => entries.slice(), readActivation: async (key: string) => state.get(key), writeActivation: async (key: string, value: string) => { state.set(key, value); }, now: () => 1 };
    await activateCapture(deps); entries.push(msg("tool-noargs", "assistant", [{ type: "toolCall", name: "shell" }]), msg("memory-noargs", "assistant", [{ type: "toolCall", name: "memory_search" }]));
    const records = await capturePersistedEntries({ ...deps, lifecycle: "agent_end", activationDir: "/unused" });
    expect(records).toHaveLength(1); expect(records[0]).toMatchObject({ eventKind: "tool_call", toolName: "shell" }); expect(records[0]?.text).toBeUndefined(); expect(records[0]?.toolArgs).toBeUndefined();
  });

  it("enforces tool argument/result budgets and allows only useful bounded fields", () => {
    const result = selectPersistedEntries([
      msg("call-budget", "assistant", [{ type: "toolCall", name: "shell", arguments: { command: "x".repeat(10_000) } }]),
      msg("result-budget", "toolResult", "y".repeat(10_000), { toolName: "shell", status: "completed", stderr: "z".repeat(10_000) }),
    ], { toolArgsChars: 17, toolResultChars: 19 });
    expect(result.find((entry) => entry.sourceEntryId === "call-budget")?.toolArgs?.length).toBeLessThanOrEqual(17);
    expect(result.find((entry) => entry.sourceEntryId === "result-budget")?.text?.length).toBeLessThanOrEqual(19);
  });

  it("activates at the getEntries tail, persists restart-safe state, dedupes, and captures only later persisted entries", async () => {
    const state = new Map<string, string>();
    const entries: Entry[] = [msg("before", "user", "old")];
    const getEntries = vi.fn(() => entries.slice());
    const deps = { sessionId: "s/session", host: "pi" as const, getEntries, readActivation: async (key: string) => state.get(key), writeActivation: async (key: string, value: string) => { state.set(key, value); }, now: () => 100 };
    await activateCapture(deps);
    entries.push(msg("after", "user", "new"));
    const result = await capturePersistedEntries({ sessionId: "s/session", lifecycle: "agent_end", activationDir: "/safe", host: "pi", getEntries });
    expect(result.map((entry) => entry.sourceEntryId)).toEqual(["after"]);
    expect(await capturePersistedEntries({ sessionId: "s/session", lifecycle: "session_shutdown", activationDir: "/safe", host: "pi", getEntries })).toEqual([]);
    expect(getEntries).toHaveBeenCalled();
    expect(CAPTURE_LIFECYCLES).toEqual(["agent_end", "session_before_compact", "session_shutdown"]);
    expect(CAPTURE_LIFECYCLES).not.toContain("agent_settled" as never);
  });

  it("marks malformed child metadata as a leaf while disabling root work", () => {
    const marker = resolveAgentMarker({ host: "prime", header: { rlmDepth: "not-a-depth" } });
    expect(marker.role).toBe("child");
    expect(marker.rootWorkAllowed).toBe(false);
    expect(captureRootWorkAllowed({ host: "prime", header: { rlmDepth: "not-a-depth" } })).toBe(false);
    const piMarker = resolveAgentMarker({ host: "pi", header: { parentSession: { id: "parent" } }, env: { PI_SUBAGENT_CHILD: "0" } });
    expect(piMarker.role).toBe("child");
    expect(piMarker.valid).toBe(false);
  });

  it("supports both hosts, empty-tail sentinels, invalid markers, and exact lifecycle gating", async () => {
    const state = new Map<string, string>(); let entries: Entry[] = [];
    const getEntries = () => entries.slice();
    await activateCapture({ sessionId: "empty", host: "prime", getEntries, readActivation: async (k) => state.get(k), writeActivation: async (k, v) => { state.set(k, v); }, now: () => 1 });
    entries = [msg("first", "user", "first")];
    const result = await capturePersistedEntries({ sessionId: "empty", lifecycle: "session_before_compact", activationDir: "/safe", host: "prime", getEntries, marker: { host: "prime", header: { rlmDepth: "invalid" } } });
    expect(result[0]?.agentRole).toBe("child");
    await expect(capturePersistedEntries({ sessionId: "empty", lifecycle: "agent_settled" as never, activationDir: "/safe", host: "prime", getEntries })).rejects.toThrow();
  });

  it("uses only getEntries, applies project deny precedence, and has no raw logging path", async () => {
    const state = new Map<string, string>(); const entries: Entry[] = [msg("before", "user", "history")];
    const getEntries = vi.fn(() => entries.slice());
    await activateCapture({ sessionId: "source", host: "pi", getEntries, readActivation: async (key) => state.get(key), writeActivation: async (key, value) => { state.set(key, value); }, now: () => 1 });
    entries.push(msg("after", "user", "usable"));
    const result = await capturePersistedEntries(Object.assign({ sessionId: "source", lifecycle: "agent_end", activationDir: "/not-used", host: "pi", getEntries, projectId: "denied", projectDenylist: ["denied"] }, { messages: [msg("event-array", "user", "must-not-read")] }));
    expect(result).toEqual([]);
    expect(getEntries).toHaveBeenCalled();
    await activateCapture({ sessionId: "allowed", host: "pi", getEntries, readActivation: async (key) => state.get(key), writeActivation: async (key, value) => { state.set(key, value); }, now: () => 2 });
    entries.push(msg("after-allowed", "user", "usable"));
    const allowed = await capturePersistedEntries({ sessionId: "allowed", lifecycle: "agent_end", activationDir: "/not-used", host: "pi", getEntries, projectId: "allowed", projectAllowlist: ["allowed"] });
    expect(allowed.map((entry) => entry.sourceEntryId)).toEqual(["after-allowed"]);
    const sources = ["src/capture/episode.ts", "src/capture/select.ts", "src/capture/scanner.ts", "src/security/redaction.ts"].map((file) => readFile(file, "utf8"));
    expect((await Promise.all(sources)).join("\n")).not.toMatch(/console\.(?:log|warn|error)|process\.stderr/u);
  });

  it("keeps retry batches byte-identical when cursor persistence fails", async () => {
    const state = new Map<string, string>(); let writes = 0; const entries: Entry[] = [msg("before-deterministic", "user", "old")];
    const deps = { sessionId: "deterministic", host: "pi" as const, getEntries: () => entries.slice(), readActivation: async (key: string) => state.get(key), writeActivation: async (key: string, value: string) => { writes += 1; if (writes > 1 && writes < 3) throw new Error("cursor failure"); state.set(key, value); }, now: () => writes === 2 ? 2000 : 1000 };
    await activateCapture(deps); entries.push(msg("deterministic-after", "user", "stable"));
    const accepted: unknown[] = []; const first = await capturePersistedEntries({ ...deps, lifecycle: "agent_end", activationDir: "/unused", acceptEpisodes: async (batch) => { accepted.push(batch); } });
    const second = await capturePersistedEntries({ ...deps, lifecycle: "agent_end", activationDir: "/unused", acceptEpisodes: async (batch) => { accepted.push(batch); } });
    expect(first).toEqual([]); expect(second).toHaveLength(1); expect(accepted).toHaveLength(2); expect(accepted[0]).toEqual(accepted[1]);
  });

  it("fails closed when durable state is deleted/corrupt and preserves an existing file cutoff", async () => {
    const root = await mkdtemp(join(tmpdir(), "task4-state-")); const agent = join(root, "agent");
    const entries: Entry[] = [msg("before", "user", "old")];
    const activationDir = await persistCaptureActivationFile({ host: "pi", sessionId: "same", getEntries: () => entries, env: { PI_CODING_AGENT_DIR: agent }, homeDir: root, now: () => 1 });
    const filename = (await import("node:crypto")).createHash("sha256").update("same").digest("hex");
    const stateFile = join(activationDir, `state-${filename}.json`); const original = await readFile(stateFile, "utf8");
    entries.push(msg("after", "user", "after"));
    await rm(stateFile); clearCaptureActivation("same");
    expect(await capturePersistedEntries({ sessionId: "same", lifecycle: "agent_end", activationDir, host: "pi", getEntries: () => entries })).toEqual([]);
    await writeFile(stateFile, original, { mode: 0o600 });
    entries.push(msg("after2", "user", "after2")); clearCaptureActivation("same");
    expect((await capturePersistedEntries({ sessionId: "same", lifecycle: "agent_end", activationDir, host: "pi", getEntries: () => entries })).map((entry) => entry.sourceEntryId)).toEqual(["after", "after2"]);
    const beforeRepeated = await readFile(stateFile, "utf8");
    await persistCaptureActivationFile({ host: "pi", sessionId: "same", getEntries: () => entries.concat(msg("future", "user", "future")), env: { PI_CODING_AGENT_DIR: agent }, homeDir: root, now: () => 99 });
    expect(await readFile(stateFile, "utf8")).not.toContain("future"); expect(JSON.parse(await readFile(stateFile, "utf8")).tailEntryId).toBe(JSON.parse(beforeRepeated).tailEntryId);
    await writeFile(stateFile, "{}", { mode: 0o600 }); clearCaptureActivation("same");
    await expect(persistCaptureActivationFile({ host: "pi", sessionId: "same", getEntries: () => entries, env: { PI_CODING_AGENT_DIR: agent }, homeDir: root, now: () => 100 })).rejects.toThrow();
    await rm(root, { recursive: true, force: true });
  });

  it("does not let an injected passed scanner bypass the built-in rejection", async () => {
    const state = new Map<string, string>(); const entries: Entry[] = [msg("before-scan-seam", "user", "old")]; const high = "opaque 0123456789abcdef0123456789abcdef0123456789abcdef";
    const deps = { sessionId: "scan-seam", host: "pi" as const, getEntries: () => entries.slice(), readActivation: async (key: string) => state.get(key), writeActivation: async (key: string, value: string) => { state.set(key, value); }, now: () => 1 };
    await activateCapture(deps); entries.push(msg("scan-seam", "user", high));
    expect(await capturePersistedEntries({ ...deps, lifecycle: "agent_end", activationDir: "/unused", scan: () => "passed" })).toEqual([]);
    expect(JSON.parse([...state.values()][0]!).audit.scanner_rejected).toBe(1);
  });

  it("audits an injected scanner error durably without returning a passed/searchable record", async () => {
    const state = new Map<string, string>(); const entries: Entry[] = [msg("before-scan-error", "user", "old")]; const deps = { sessionId: "scan-error", host: "pi" as const, getEntries: () => entries.slice(), readActivation: async (key: string) => state.get(key), writeActivation: async (key: string, value: string) => { state.set(key, value); }, now: () => 1 };
    await activateCapture(deps); entries.push(msg("scan-error", "user", "safe"));
    expect(await capturePersistedEntries({ ...deps, lifecycle: "agent_end", activationDir: "/unused", scan: () => "error" })).toEqual([]);
    expect(JSON.parse([...state.values()][0]!).audit.scanner_error).toBeGreaterThan(0);
  });

  it("does not persist high-entropy producer/node/message IDs or their raw hashes", async () => {
    const state = new Map<string, string>(); const entries: Entry[] = [msg("before-identities", "user", "old")]; const token = "opaque 0123456789abcdef0123456789abcdef0123456789abcdef";
    const deps = { sessionId: "identity-session", host: "pi" as const, getEntries: () => entries.slice(), readActivation: async (key: string) => state.get(key), writeActivation: async (key: string, value: string) => { state.set(key, value); }, now: () => 1 };
    await activateCapture(deps); entries.push(msg(token, "user", "safe"));
    const records = await capturePersistedEntries({ ...deps, lifecycle: "agent_end", activationDir: "/unused", producerId: token, nodeId: token });
    const serialized = JSON.stringify(records); expect(serialized).not.toContain(token); expect(serialized).not.toContain(sha256Hex(token));
  });

  it("keeps message-id collisions distinct when unsafe IDs fall back to source entries", async () => {
    const state = new Map<string, string>(); const entries: Entry[] = [msg("before-message-id", "user", "old")];
    const deps = { sessionId: "message-id-collision", host: "pi" as const, getEntries: () => entries.slice(), readActivation: async (key: string) => state.get(key), writeActivation: async (key: string, value: string) => { state.set(key, value); }, now: () => 1 };
    await activateCapture(deps);
    entries.push(msg("e1", "user", "first", { id: "password=one" }), msg("e2", "user", "second", { id: "password=two" }));
    const records = await capturePersistedEntries({ ...deps, lifecycle: "agent_end", activationDir: "/unused" });
    expect(records).toHaveLength(2); expect(new Set(records.map((record) => record.id)).size).toBe(2);
    expect(records.map((record) => record.sourceEntryId)).toEqual(["e1", "e2"]);
    const serialized = JSON.stringify(records); expect(serialized).not.toContain("password=one"); expect(serialized).not.toContain("password=two");
    expect(serialized).not.toContain(sha256Hex("password=one")); expect(serialized).not.toContain(sha256Hex("password=two"));
  });

  it("quarantines invalid entries generically without poisoning the safe cutoff", async () => {
    const state = new Map<string, string>(); const entries: Entry[] = [msg("before-invalid", "user", "old")];
    const deps = { sessionId: "invalid-entry-progress", host: "pi" as const, getEntries: () => entries.slice(), readActivation: async (key: string) => state.get(key), writeActivation: async (key: string, value: string) => { state.set(key, value); }, now: () => 1 };
    await activateCapture(deps); const before = JSON.parse([...state.values()][0]!); entries.push(msg("password=one", "user", "unsafe entry"), msg("safe-after-invalid", "user", "safe suffix"));
    expect(await capturePersistedEntries({ ...deps, lifecycle: "agent_end", activationDir: "/unused" })).toEqual([]);
    const quarantined = JSON.parse([...state.values()][0]!); expect(quarantined.audit.invalid_entry).toBe(1); expect(quarantined.quarantineIds).toHaveLength(1); expect(quarantined.tailEntryId).toBe(before.tailEntryId); expect(quarantined.tailCount).toBe(before.tailCount); expect(quarantined.tailHash).toBe(before.tailHash);
    const retryState = [...state.values()][0]!; expect(await capturePersistedEntries({ ...deps, lifecycle: "agent_end", activationDir: "/unused" })).toEqual([]); expect([...state.values()][0]).toBe(retryState);
    const serialized = [...state.values()][0]!; expect(serialized).not.toContain("password=one"); expect(serialized).not.toContain(sha256Hex("password=one"));
    entries.splice(1, 1);
    const records = await capturePersistedEntries({ ...deps, lifecycle: "agent_end", activationDir: "/unused" }); expect(records.map((record) => record.sourceEntryId)).toEqual(["safe-after-invalid"]);
  });

  it("fails closed when the persisted source throws during lifecycle capture", async () => {
    const state = new Map<string, string>(); const entries: Entry[] = [msg("before-source-throw", "user", "old")]; let writes = 0;
    const activateDeps = { sessionId: "source-throw", host: "pi" as const, getEntries: () => entries.slice(), readActivation: async (key: string) => state.get(key), writeActivation: async (key: string, value: string) => { writes += 1; state.set(key, value); }, now: () => 1 };
    await activateCapture(activateDeps); const before = [...state.values()][0]!; writes = 0;
    expect(await capturePersistedEntries({ ...activateDeps, getEntries: () => { throw new Error("source unavailable"); }, lifecycle: "agent_end", activationDir: "/unused" })).toEqual([]);
    expect(writes).toBe(0); expect([...state.values()][0]).toBe(before);
  });

  it("separates host cache keys, rejects mismatched markers, audits rejected entries, and accepts atomically", async () => {
    const state = new Map<string, string>(); const entries: Entry[] = [msg("before-host", "user", "old")];
    const deps = (host: "pi" | "prime") => ({ sessionId: "host-session", host, getEntries: () => entries.slice(), readActivation: async (key: string) => state.get(key), writeActivation: async (key: string, value: string) => { state.set(key, value); }, now: () => 1 });
    await activateCapture(deps("pi")); await activateCapture(deps("prime")); entries.push(msg("reject", "user", "opaque 0123456789abcdef0123456789abcdef0123456789abcdef"));
    const rejected = await capturePersistedEntries({ sessionId: "host-session", lifecycle: "agent_end", activationDir: "/unused", host: "pi", getEntries: () => entries, marker: { host: "prime", header: { rlmDepth: 0 } } });
    expect(rejected).toEqual([]); const durable = [...state.values()].map((raw) => JSON.parse(raw)); expect(durable.some((item) => item.audit?.scanner_rejected > 0)).toBe(true);
    expect([...state.values()].join("\n")).not.toContain("0123456789abcdef0123456789abcdef0123456789abcdef");
    expect([...state.values()].join("\n")).not.toContain(sha256Hex("0123456789abcdef0123456789abcdef0123456789abcdef"));
    entries.push(msg("accepted", "user", "must-not-lose"));
    const failed = await capturePersistedEntries({ sessionId: "host-session", lifecycle: "agent_end", activationDir: "/unused", host: "pi", getEntries: () => entries, acceptEpisodes: async () => { throw new Error("outbox down"); } });
    expect(failed).toEqual([]);
    expect((await capturePersistedEntries({ sessionId: "host-session", lifecycle: "agent_end", activationDir: "/unused", host: "pi", getEntries: () => entries })).map((entry) => entry.sourceEntryId)).toContain("accepted");
  });

  it("persists quarantine audit even when episode acceptance fails, without double-counting retries", async () => {
    const state = new Map<string, string>(); const entries: Entry[] = [msg("before-audit", "user", "old")]; const deps = { sessionId: "audit", host: "pi" as const, getEntries: () => entries.slice(), readActivation: async (key: string) => state.get(key), writeActivation: async (key: string, value: string) => { state.set(key, value); }, now: () => 1 };
    await activateCapture(deps); entries.push(msg("audit-reject", "user", "opaque 0123456789abcdef0123456789abcdef0123456789abcdef"), msg("audit-safe", "user", "safe"));
    expect(await capturePersistedEntries({ ...deps, lifecycle: "agent_end", activationDir: "/unused", acceptEpisodes: async () => { throw new Error("down"); } })).toEqual([]);
    const raw = [...state.values()][0]!; expect(JSON.parse(raw).audit.scanner_rejected).toBeGreaterThan(0);
    const afterFirstAudit = JSON.parse(raw).audit.scanner_rejected;
    expect(await capturePersistedEntries({ ...deps, lifecycle: "agent_end", activationDir: "/unused", acceptEpisodes: async () => { throw new Error("down"); } })).toEqual([]);
    expect(JSON.parse([...state.values()][0]!).audit.scanner_rejected).toBe(afterFirstAudit);
  });

  it("does not lose safe episodes when quarantine audit and acceptance fail together", async () => {
    const state = new Map<string, string>(); const entries: Entry[] = [msg("before-mixed", "user", "old")];
    const deps = { sessionId: "mixed", host: "pi" as const, getEntries: () => entries.slice(), readActivation: async (key: string) => state.get(key), writeActivation: async (key: string, value: string) => { state.set(key, value); }, now: () => 1 };
    await activateCapture(deps);
    entries.push(msg("mixed-rejected", "user", "opaque 0123456789abcdef0123456789abcdef0123456789abcdef"), msg("mixed-safe", "user", "safe"));
    let calls = 0;
    expect(await capturePersistedEntries({ ...deps, lifecycle: "agent_end", activationDir: "/unused", acceptEpisodes: async () => { calls += 1; throw new Error("down"); } })).toEqual([]);
    const failedState = JSON.parse([...state.values()][0]!);
    expect(failedState.audit.scanner_rejected).toBe(1);
    expect(failedState.capturedIds).toEqual([]);
    expect(failedState.quarantineIds).toHaveLength(1);
    const retry = await capturePersistedEntries({ ...deps, lifecycle: "agent_end", activationDir: "/unused", acceptEpisodes: async () => { calls += 1; } });
    expect(retry.map((entry) => entry.sourceEntryId)).toEqual(["mixed-safe"]);
    expect(calls).toBe(2);
    const succeededState = JSON.parse([...state.values()][0]!);
    expect(succeededState.capturedIds).toHaveLength(1);
    expect(succeededState.audit.scanner_rejected).toBe(1);
  });

  it("rejects redacted session collisions while accepting a real UUID session", async () => {
    const entries: Entry[] = [msg("session-entry", "user", "safe")];
    for (const sessionId of ["password=one", "password=two"]) {
      const state = new Map<string, string>();
      await expect(activateCapture({ sessionId, host: "pi", getEntries: () => entries, readActivation: async (key) => state.get(key), writeActivation: async (key, value) => { state.set(key, value); }, now: () => 1 })).rejects.toThrow();
      expect(state.size).toBe(0);
    }
    const uuidState = new Map<string, string>();
    await expect(activateCapture({ sessionId: "019fdef5-34fc-7189-8d71-ca9f9f9d9fc7", host: "pi", getEntries: () => entries, readActivation: async (key) => uuidState.get(key), writeActivation: async (key, value) => { uuidState.set(key, value); }, now: () => 1 })).resolves.toBeUndefined();
  });

  it("preserves selector-generated error fingerprints as bounded digests", async () => {
    const state = new Map<string, string>(); const entries: Entry[] = [msg("before-fingerprint", "user", "old")];
    const deps = { sessionId: "fingerprint", host: "pi" as const, getEntries: () => entries.slice(), readActivation: async (key: string) => state.get(key), writeActivation: async (key: string, value: string) => { state.set(key, value); }, now: () => 1 };
    await activateCapture(deps);
    entries.push(msg("tool-fingerprint", "toolResult", "failure details", { toolName: "shell", status: "failed", stderr: "failure details", code: 2 }));
    const [record] = await capturePersistedEntries({ ...deps, lifecycle: "agent_end", activationDir: "/unused" });
    expect(record?.errorFingerprint).toMatch(/^[a-f0-9]{32}$/u);
    expect(() => parsePersistedMemoryRecord(record!)).not.toThrow();
  });

  it("fails closed before cursor hashing on unsafe persisted entry IDs", async () => {
    const state = new Map<string, string>(); const entries: Entry[] = [msg("before-tail", "user", "old")];
    const deps = { sessionId: "tail-safe", host: "pi" as const, getEntries: () => entries.slice(), readActivation: async (key: string) => state.get(key), writeActivation: async (key: string, value: string) => { state.set(key, value); }, now: () => 1 };
    await activateCapture(deps); const before = [...state.values()][0]!;
    entries.push(msg("password=one", "user", "unsafe id"));
    expect(await capturePersistedEntries({ ...deps, lifecycle: "agent_end", activationDir: "/unused" })).toEqual([]);
    const invalidState = [...state.values()][0]!; expect(invalidState).not.toBe(before); expect(JSON.parse(invalidState).audit.invalid_entry).toBe(1);
    entries[entries.length - 1] = msg("password=two", "user", "unsafe id");
    expect(await capturePersistedEntries({ ...deps, lifecycle: "agent_end", activationDir: "/unused" })).toEqual([]);
    expect([...state.values()][0]).toBe(invalidState);
  });

  it("advances the cursor for rejected-only and injected-memory suffixes", async () => {
    const make = async (id: string, content: unknown) => {
      const state = new Map<string, string>(); const entries: Entry[] = [msg(`${id}-before`, "user", "old")];
      const deps = { sessionId: id, host: "pi" as const, getEntries: () => entries.slice(), readActivation: async (key: string) => state.get(key), writeActivation: async (key: string, value: string) => { state.set(key, value); }, now: () => 1 };
      await activateCapture(deps); const before = JSON.parse([...state.values()][0]!); entries.push(msg(`${id}-suffix`, "user", content));
      expect(await capturePersistedEntries({ ...deps, lifecycle: "agent_end", activationDir: "/unused" })).toEqual([]);
      const after = JSON.parse([...state.values()][0]!); expect(after.tailCount).toBe(before.tailCount + 1);
      const writes = [...state.values()].length; expect(await capturePersistedEntries({ ...deps, lifecycle: "session_shutdown", activationDir: "/unused" })).toEqual([]); expect([...state.values()].length).toBe(writes);
    };
    await make("reject-progress", "opaque 0123456789abcdef0123456789abcdef0123456789abcdef");
    await make("memory-progress", "[memory-context] injected");
  });

  it("returns exact persisted records with no runtime fields and redacts secret-derived identities/fingerprints", async () => {
    const state = new Map<string, string>(); const entries: Entry[] = [msg("before-roundtrip", "user", "old")];
    const deps = { sessionId: "session-safe", host: "pi" as const, getEntries: () => entries.slice(), readActivation: async (key: string) => state.get(key), writeActivation: async (key: string, value: string) => { state.set(key, value); }, now: () => 10 };
    await activateCapture(deps); entries.push(msg("roundtrip-tool", "toolResult", "safe", { toolName: "shell", status: "failed", error: "Bearer raw-error-token-1234567890" }));
    const records = await capturePersistedEntries({ ...deps, lifecycle: "agent_end", activationDir: "/unused", producerId: "sk-abcdefghijklmnopqrstuvwxyz123456", nodeId: "Bearer raw-node-token-1234567890" });
    expect(records).toHaveLength(1); expect(() => parsePersistedMemoryRecord(records[0]!)).not.toThrow(); expect(Object.keys(records[0]!).sort()).not.toContain("redactionStatus");
    expect(JSON.stringify(records)).not.toContain("sk-abcdefghijklmnopqrstuvwxyz123456"); expect(JSON.stringify(records)).not.toContain("raw-error-token-1234567890"); expect(JSON.stringify(records)).not.toContain(sha256Hex("raw-error-token-1234567890"));
  });

  it("reloads persisted file state, keeps secrets out of returned episodes, and never accepts settled", async () => {
    const root = await mkdtemp(join(tmpdir(), "task4-restart-"));
    const agent = join(root, "agent");
    const entries: Entry[] = [msg("history", "user", "old")];
    const activationDir = await persistCaptureActivationFile({ host: "prime", sessionId: "restart", getEntries: () => entries, env: { PRIME_AGENT_CODING_AGENT_DIR: agent }, homeDir: root, now: () => 10 });
    clearCaptureActivation("restart");
    entries.push(msg("safe", "assistant", "safe"));
    expect((await capturePersistedEntries({ sessionId: "restart", lifecycle: "agent_end", activationDir, host: "prime", getEntries: () => entries })).map((entry) => entry.sourceEntryId)).toEqual(["safe"]);
    entries.push(msg("warm-cache", "assistant", "warm cache path validation"));
    const wrongActivationDir = join(root, "wrong", "pi-qdrant-memory", "capture");
    expect(await capturePersistedEntries({ sessionId: "restart", lifecycle: "agent_end", activationDir: relative(process.cwd(), activationDir), host: "prime", getEntries: () => entries })).toEqual([]);
    expect(await capturePersistedEntries({ sessionId: "restart", lifecycle: "agent_end", activationDir: wrongActivationDir, host: "prime", getEntries: () => entries })).toEqual([]);
    expect((await capturePersistedEntries({ sessionId: "restart", lifecycle: "agent_end", activationDir, host: "prime", getEntries: () => entries })).map((entry) => entry.sourceEntryId)).toEqual(["warm-cache"]);
    const originalCwd = process.cwd(); process.chdir(agent);
    try {
      clearCaptureActivation("restart"); entries.push(msg("home-cwd", "assistant", "default host state survives home cwd"));
      expect((await capturePersistedEntries({ sessionId: "restart", lifecycle: "agent_end", activationDir, host: "prime", getEntries: () => entries })).map((entry) => entry.sourceEntryId)).toEqual(["home-cwd"]);
    } finally { process.chdir(originalCwd); }
    clearCaptureActivation("restart");
    entries.push(msg("high", "user", "opaque 0123456789abcdef0123456789abcdef0123456789abcdef"));
    expect(await capturePersistedEntries({ sessionId: "restart", lifecycle: "agent_end", activationDir, host: "prime", getEntries: () => entries })).toEqual([]);
    clearCaptureActivation("restart");
    entries.push(msg("relative-only", "user", "relative path must not reload"));
    const relativeActivationDir = relative(process.cwd(), activationDir);
    expect(await capturePersistedEntries({ sessionId: "restart", lifecycle: "agent_end", activationDir: relativeActivationDir, host: "prime", getEntries: () => entries })).toEqual([]);
    expect((await capturePersistedEntries({ sessionId: "restart", lifecycle: "agent_end", activationDir, host: "prime", getEntries: () => entries })).map((entry) => entry.sourceEntryId)).toEqual(["relative-only"]);
    clearCaptureActivation("restart");
    expect(await capturePersistedEntries({ sessionId: "restart", lifecycle: "session_shutdown", activationDir, host: "prime", getEntries: () => entries })).toEqual([]);
    entries.push(msg("secret", "user", "Bearer secret-token-1234567890"));
    const safe = await capturePersistedEntries({ sessionId: "restart", lifecycle: "session_before_compact", activationDir, host: "prime", getEntries: () => entries });
    expect(JSON.stringify(safe)).not.toContain("secret-token-1234567890");
    await expect(capturePersistedEntries({ sessionId: "restart", lifecycle: "agent_settled" as never, activationDir, host: "prime", getEntries: () => entries })).rejects.toThrow();
    await rm(root, { recursive: true, force: true });
  });

  it("fails closed on activation persistence and validates host paths", async () => {
    const entries = [msg("x", "user", "x")];
    await expect(activateCapture({ sessionId: "x", host: "pi", getEntries: () => entries, readActivation: async () => undefined, writeActivation: async () => { throw new Error("disk"); }, now: () => 1 })).rejects.toThrow();
    expect(await resolveCaptureAgentDirectory({ host: "pi", homeDir: "/tmp/home", env: { PI_CODING_AGENT_DIR: "relative" } })).toBeNull();
    const dir = await mkdtemp(join(tmpdir(), "task4-"));
    const agent = join(dir, "agent");
    const resolved = await resolveCaptureAgentDirectory({ host: "pi", homeDir: dir, env: { PI_CODING_AGENT_DIR: agent } });
    expect(resolved).toBe(agent);
    const { persistCaptureActivationFile } = await import("../../src/capture/episode.js");
    await persistCaptureActivationFile({ host: "pi", sessionId: "x-file", getEntries: () => entries, env: { PI_CODING_AGENT_DIR: agent }, homeDir: dir, now: () => 1 });
    const mode = (await stat(join(agent, "pi-qdrant-memory", "capture"))).mode & 0o777;
    expect(mode).toBe(0o700);
    const file = join(agent, "pi-qdrant-memory", "capture", `state-${(await import("node:crypto")).createHash("sha256").update("x-file").digest("hex")}.json`);
    expect((await stat(file)).mode & 0o777).toBe(0o600);
    expect(JSON.parse(await readFile(file, "utf8")).sessionId).toBe("x-file");
    expect(await resolveCaptureAgentDirectory({ host: "pi", homeDir: dir, env: { PI_CODING_AGENT_DIR: agent, PRIME_AGENT_CODING_AGENT_DIR: join(dir, "prime") } })).toBeNull();
    const outside = join(dir, "outside"); const linked = join(dir, "linked"); await symlink(outside, linked);
    expect(await resolveCaptureAgentDirectory({ host: "pi", homeDir: dir, env: { PI_CODING_AGENT_DIR: linked } })).toBeNull();
  });
});
