import { mkdtemp, open, readFile, readdir, rm, stat } from "node:fs/promises";
import { spawn } from "node:child_process";
import { once } from "node:events";
import * as nodeFs from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, vi } from "vitest";
import { createOutbox, parseOutboxJob } from "../../src/outbox/store.js";
import { createOutboxDelivery, type OutboxJobProcessor } from "../../src/outbox/delivery.js";
import { canonicalRecordHash, type EpisodeRecord } from "../../src/domain/records.js";
import { processingPolicyHash, type ProcessingPolicy } from "../../src/domain/policy.js";
import { canonicalStringify, deterministicUuid, sha256Hex } from "../../src/domain/canonical.js";

function policy(expiresAt: string | null = "2030-01-01T00:00:00.000Z"): ProcessingPolicy {
  const pending: ProcessingPolicy = {
    id: "pending", ownerHost: "prime", destinationIds: { qdrant: "qdrant:local", embedding: "embed:local" },
    originProvider: "provider-local", allowCrossProviderReplay: false, expiresAt,
    residency: "local", dataUse: "memory", policyRevision: "policy-v1",
  };
  return { ...pending, id: processingPolicyHash(pending) };
}
function episode(current: ProcessingPolicy, id = "00000000-0000-5000-8000-000000000001", text = "already [token redacted]"): EpisodeRecord {
  const pending: EpisodeRecord = {
    recordType: "episode", id, ownerHost: "prime", schemaRevision: 1,
    createdAt: "2029-01-01T00:00:00.000Z", privacyEpoch: 0, processingPolicyId: current.id,
    expiresAt: current.expiresAt, contentHash: "pending", sourceEntryId: "entry-1", host: "prime",
    projectId: "project-1", projectIdentityKind: "registered", sessionId: "session-1", turnId: "turn-1",
    agentRole: "root", depth: 0, eventKind: "user", eventAt: "2029-01-01T00:00:00.000Z",
    modelId: "model-local", embeddingDimension: 1024, originProvider: "provider-local",
    destinationId: "qdrant:local", status: "active", secretScan: "passed", text,
  };
  return { ...pending, contentHash: canonicalRecordHash(pending) };
}

async function mode(path: string): Promise<number> { return (await stat(path)).mode & 0o777; }
function producerId(index: number): string { return `00000000-0000-4000-8000-${index.toString(16).padStart(12, "0")}`; }
function producerStateRecord(state: "active" | "closed", heartbeatAt: number, closedAt: number | null): Record<string, unknown> {
  const value: Record<string, unknown> = { version: 1, state, heartbeatAt, closedAt, auditHash: "" };
  const withoutAudit = { closedAt, heartbeatAt, state, version: 1 };
  value.auditHash = sha256Hex(canonicalStringify(withoutAudit));
  return value;
}
async function writeProducerState(producerPath: string, state: "active" | "closed", heartbeatAt: number, closedAt: number | null): Promise<void> {
  await nodeFs.writeFile(join(producerPath, "state.json"), canonicalStringify(producerStateRecord(state, heartbeatAt, closedAt)), { mode: 0o600 });
  const handle = await open(producerPath, "r");
  try { await handle.sync(); } finally { await handle.close(); }
}
const ADMISSION_LOCK_NAME = /^admission\.\d{16}\.lock$/u;
const ADMISSION_RETIREMENT_NAME = /^admission\.\d{16}\.retired$/u;
const ADMISSION_RETIREMENT_TEMP_NAME = /^\.admission\.\d{16}\.retired\.tmp-/u;
function activeReservationNames(names: readonly string[]): string[] { return names.filter((name) => !ADMISSION_LOCK_NAME.test(name) && !ADMISSION_RETIREMENT_NAME.test(name) && !ADMISSION_RETIREMENT_TEMP_NAME.test(name)); }
function activeAdmissionLockNames(names: readonly string[]): string[] { const retired = new Set(names.filter((name) => ADMISSION_RETIREMENT_NAME.test(name)).map((name) => name.replace(/\.retired$/u, ".lock"))); return names.filter((name) => ADMISSION_LOCK_NAME.test(name) && !retired.has(name)); }
function reservationRecord(nodeId: string, producerUuid: string, jobId: string, current: ProcessingPolicy, requestedBytes = 1): Record<string, unknown> { const value: Record<string, unknown> = { version: 1, reservationId: deterministicUuid("pi-qdrant-memory-v2:outbox-reservation", nodeId, producerUuid, jobId), jobId, jobAuditHash: sha256Hex(`job:${jobId}`), policyId: current.id, deadline: current.expiresAt, nodeId, producerUuid, requestedBytes, auditHash: "" }; value.auditHash = sha256Hex(canonicalStringify({ deadline: value.deadline, jobAuditHash: value.jobAuditHash, jobId: value.jobId, nodeId: value.nodeId, policyId: value.policyId, producerUuid: value.producerUuid, requestedBytes: value.requestedBytes, reservationId: value.reservationId, version: 1 })); return value; }
function retirementRecord(generation: number, reservation: Record<string, unknown>): Record<string, unknown> { const value: Record<string, unknown> = { version: 1, kind: "admission_lock_retired", generation, reservation, auditHash: "" }; value.auditHash = sha256Hex(canonicalStringify({ generation, kind: "admission_lock_retired", reservation, version: 1 })); return value; }
async function readAdmissionLock(reservations: string): Promise<Record<string, unknown>> {
  const lock = (await readdir(reservations)).find((name) => ADMISSION_LOCK_NAME.test(name));
  if (lock === undefined) throw new Error("expected an active generated admission lock");
  return JSON.parse(await readFile(join(reservations, lock), "utf8")) as Record<string, unknown>;
}

describe("Task 5 durable outbox", () => {
  it("exposes only the approved durable store and processor delivery seams", () => {
    expect(createOutbox).toBeTypeOf("function");
    expect(createOutboxDelivery).toBeTypeOf("function");
  });

  it("atomically accepts an exact redacted policy-bearing job under the validated host root", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-store-"));
    const operations: string[] = [];
    const fs = {
      ...nodeFs,
      open: async (...args: Parameters<typeof open>) => { operations.push(`open:${String(args[1])}`); return open(...args); },
      rename: async (from: string, to: string) => { operations.push(`rename:${from}->${to}`); return nodeFs.rename(from, to); },
    };
    try {
      const outbox = await createOutbox({ host: "prime", homeDir, env: {}, nodeId: "node-redacted", producerUuid: producerId(1), machineId: "machine-test-1", fs, now: () => Date.parse("2029-01-02T00:00:00.000Z") });
      const currentPolicy = policy();
      const stored = await outbox.enqueue({ episodes: [episode(currentPolicy)], policy: currentPolicy });
      const disk = JSON.parse(await readFile(stored.file, "utf8"));
      expect(stored.file).toBe(join(homeDir, ".prime", "agent", "pi-qdrant-memory", "outbox", "node-redacted", producerId(1), "jobs", `${stored.id}.json`));
      expect(disk).toEqual(expect.objectContaining({ id: stored.id, policyId: currentPolicy.id, deadline: currentPolicy.expiresAt, episodeIds: ["00000000-0000-5000-8000-000000000001"] }));
      expect(Object.keys(disk).sort()).toEqual(["auditHash", "createdAt", "deadline", "episodeIds", "episodes", "id", "nodeId", "ownerHost", "policy", "policyId", "producerUuid", "version"].sort());
      expect(JSON.stringify(disk)).toContain("[token redacted]");
      expect(JSON.stringify(disk)).not.toMatch(/Bearer |raw-secret/u);
      expect(await mode(stored.file)).toBe(0o600);
      expect(await mode(outbox.producerPath)).toBe(0o700);
      expect(operations.some((item) => item === "open:wx")).toBe(true);
      expect(operations.some((item) => item.startsWith("rename:") && item.endsWith(stored.file))).toBe(true);
      expect((await readdir(join(outbox.producerPath, "jobs"))).some((name) => name.includes(".tmp-"))).toBe(false);
      expect(await outbox.listPending()).toEqual([stored]);
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("rejects unsafe roots, symlinks, producer reuse, and ambiguous shared node identities", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-paths-"));
    try {
      await expect(createOutbox({ host: "prime", homeDir: "relative", nodeId: "node-a", producerUuid: producerId(2) })).rejects.toThrow(/absolute/u);
      await expect(createOutbox({ host: "prime", homeDir, env: { PI_CODING_AGENT_DIR: join(homeDir, "wrong") }, nodeId: "node-a", producerUuid: producerId(2) })).rejects.toThrow(/Contradictory/u);
      await expect(createOutbox({ host: "prime", homeDir, sharedFilesystem: true, producerUuid: producerId(2), machineId: "machine-a" })).rejects.toThrow(/explicit unique node/u);
      const first = await createOutbox({ host: "prime", homeDir, nodeId: "node-shared", producerUuid: producerId(2), sharedFilesystem: true, machineId: "machine-a" });
      await expect(createOutbox({ host: "prime", homeDir, nodeId: "node-shared", producerUuid: producerId(2), sharedFilesystem: true, machineId: "machine-a" })).rejects.toThrow();
      await expect(createOutbox({ host: "prime", homeDir, nodeId: "node-shared", producerUuid: producerId(5), sharedFilesystem: true, machineId: "machine-b" })).rejects.toThrow(/node identity/u);
      await first.closeProducer();
      const symlinkHome = join(homeDir, "linked");
      await nodeFs.symlink(join(homeDir, ".prime"), symlinkHome);
      await expect(createOutbox({ host: "prime", homeDir, env: { PRIME_AGENT_CODING_AGENT_DIR: join(symlinkHome, "agent") }, nodeId: "node-b", producerUuid: producerId(5) })).rejects.toThrow(/unsafe/u);
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("derives pseudonymous CSPRNG identities without persisting machine identity or hostname", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-identity-"));
    try {
      const bytes = Array.from({ length: 16 }, (_, index) => index + 1);
      const randomBytes = vi.fn((size: number) => Uint8Array.from(size === 16 ? bytes : Array.from({ length: size }, (_, index) => index % 251)));
      const outbox = await createOutbox({ host: "pi", homeDir, env: {}, machineId: "machine-raw-must-not-persist", randomBytes });
      expect(outbox.nodeId).toMatch(/^node-[a-f0-9]{32}$/u);
      expect(outbox.nodeId).not.toContain("machine-raw");
      expect(outbox.producerUuid).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u);
      expect(await readFile(join(outbox.producerPath, "producer.json"), "utf8")).not.toContain("machine-raw-must-not-persist");
      expect(await readFile("src/outbox/store.ts", "utf8")).not.toMatch(/hostname\s*\(/u);
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("rejects unstructured digests, forged fingerprints, unsafe node overrides, and non-UUID producers", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-field-validation-")); const rawHex = "0123456789abcdef".repeat(3); const rawHash = "0123456789abcdef".repeat(4);
    try {
      await expect(createOutbox({ host: "prime", homeDir, nodeId: "node-safe", producerUuid: "p-unsafe", machineId: "machine-fields" })).rejects.toThrow(/producer.*UUID|UUID.*producer/iu);
      await expect(createOutbox({ host: "prime", homeDir, nodeId: rawHex, producerUuid: producerId(40), machineId: "machine-fields" })).rejects.toThrow(/node/u);
      await expect(createOutbox({ host: "prime", homeDir, nodeId: "sk-proj-abcdefghijklmnopqrstuv", producerUuid: producerId(44), machineId: "machine-fields" })).rejects.toThrow(/node/u);
      const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-fields", producerUuid: producerId(41), machineId: "machine-fields" }); const current = policy();
      await expect(outbox.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000041", rawHex)], policy: current })).rejects.toThrow(/redacted/u);
      const fingerprint = { ...episode(current, "00000000-0000-5000-8000-000000000042"), errorFingerprint: "Bearer raw-secret-value-123456789", contentHash: "pending" } as EpisodeRecord; fingerprint.contentHash = canonicalRecordHash(fingerprint);
      await expect(outbox.enqueue({ episodes: [fingerprint], policy: current })).rejects.toThrow(/fingerprint/u);
      const unsafePending = { ...current, id: "pending", policyRevision: rawHash }; const unsafePolicy = { ...unsafePending, id: processingPolicyHash(unsafePending) };
      await expect(outbox.enqueue({ episodes: [episode(unsafePolicy, "00000000-0000-5000-8000-000000000043")], policy: unsafePolicy })).rejects.toThrow(/redacted/u);
      const source = { ...episode(current, "00000000-0000-5000-8000-000000000044"), sourceEntryId: rawHash, contentHash: "pending" } as EpisodeRecord; source.contentHash = canonicalRecordHash(source);
      await expect(outbox.enqueue({ episodes: [source], policy: current })).rejects.toThrow(/redacted/u);
      const model = { ...episode(current, "00000000-0000-5000-8000-000000000045"), modelId: rawHash, contentHash: "pending" } as EpisodeRecord; model.contentHash = canonicalRecordHash(model);
      await expect(outbox.enqueue({ episodes: [model], policy: current })).rejects.toThrow(/redacted/u);
      const destinationPending = { ...current, id: "pending", destinationIds: { ...current.destinationIds, qdrant: rawHash } }; const destinationPolicy = { ...destinationPending, id: processingPolicyHash(destinationPending) };
      await expect(outbox.enqueue({ episodes: [episode(destinationPolicy, "00000000-0000-5000-8000-000000000046")], policy: destinationPolicy })).rejects.toThrow(/redacted/u);
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("rejects a content-addressed cross-host policy inside a forged job envelope", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-policy-link-"));
    try {
      const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-policy-link", producerUuid: producerId(42), machineId: "machine-policy-link" }); const current = policy(); const job = await outbox.enqueue({ episodes: [episode(current)], policy: current }); const forged = JSON.parse(await readFile(job.file, "utf8"));
      const foreignPending = { ...forged.policy, id: "pending", ownerHost: "pi" }; forged.policy = { ...foreignPending, id: processingPolicyHash(foreignPending) }; forged.policyId = forged.policy.id;
      expect(() => parseOutboxJob(forged)).toThrow(/policy owner linkage/u);
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("rejects forged raw material and capacity overflow without discarding an accepted job", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-cap-")); const notifyFull = vi.fn();
    try {
      const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-cap", producerUuid: producerId(7), machineId: "machine-cap", maxJobs: 1, notifyFull });
      const current = policy();
      const raw = episode(current, "00000000-0000-5000-8000-000000000009", "Authorization: Bearer raw-secret-value-123456789");
      await expect(outbox.enqueue({ episodes: [raw], policy: current })).rejects.toThrow(/redacted/u);
      const unsafePending = { ...current, id: "pending", originProvider: "sk-proj-abcdefghijklmnopqrstuv" }; const unsafePolicy = { ...unsafePending, id: processingPolicyHash(unsafePending) };
      await expect(outbox.enqueue({ episodes: [episode(unsafePolicy)], policy: unsafePolicy })).rejects.toThrow(/redacted/u);
      const accepted = await outbox.enqueue({ episodes: [episode(current)], policy: current });
      await expect(outbox.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000002", "second safe episode")], policy: current })).rejects.toThrow(/capacity/u);
      expect(await readFile(accepted.file, "utf8")).toContain("already [token redacted]");
      expect((await outbox.listPending()).map((job) => job.id)).toEqual([accepted.id]);
      expect(notifyFull).toHaveBeenCalledWith(expect.objectContaining({ jobs: 1, captureAllowed: false }));
      expect(await outbox.outboxStatus()).toMatchObject({ jobs: 1, state: "active", captureAllowed: false });
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("recovers accepted jobs, quarantines malformed bytes to an audit-only record, and persists heartbeat/closed state", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-recovery-"));
    try {
      let clock = Date.parse("2029-01-02T00:00:00.000Z");
      const first = await createOutbox({ host: "prime", homeDir, nodeId: "node-recovery", producerUuid: producerId(18), machineId: "machine-recovery", now: () => clock });
      const current = policy(); const accepted = await first.enqueue({ episodes: [episode(current)], policy: current });
      clock += 1000; await first.heartbeat();
      expect((await first.outboxStatus()).heartbeatAt).toBe(clock);
      await nodeFs.writeFile(join(first.producerPath, "jobs", "malformed.json"), "Bearer raw-secret-value-123456789", { mode: 0o600 });
      expect(await first.listPending()).toEqual([accepted]);
      const quarantineNames = await readdir(join(first.producerPath, "quarantine"));
      const malformedName = quarantineNames.find((name) => name.startsWith("malformed-"));
      expect(malformedName).toBeDefined();
      const audit = await readFile(join(first.producerPath, "quarantine", malformedName!), "utf8");
      expect(audit).not.toContain("raw-secret-value"); expect(audit).toContain("sourceHash");
      await first.closeProducer(); expect(await first.outboxStatus()).toMatchObject({ state: "closed", jobs: 1 });
      await expect(first.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000003")], policy: current })).rejects.toThrow(/closed/u);
      const restarted = await createOutbox({ host: "prime", homeDir, nodeId: "node-recovery", producerUuid: producerId(17), machineId: "machine-recovery", now: () => clock + 1 });
      expect(await readFile(accepted.file, "utf8")).toContain(accepted.id);
      expect(await restarted.listPending()).toEqual([]);
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });


  it("deletes only after the injected processor reports delivered and durable deletion succeeds", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-delivered-"));
    try {
      const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-delivered", producerUuid: producerId(11), machineId: "machine-delivered" });
      const current = policy(); const job = await outbox.enqueue({ episodes: [episode(current)], policy: current });
      const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) };
      const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => Date.parse("2029-01-03T00:00:00.000Z"), maxClockSkewMs: 0 });
      expect(await delivery.deliver({})).toEqual({ delivered: 1, pending: 0, quarantined: 0 });
      expect(processor.process).toHaveBeenCalledWith(expect.objectContaining({ id: job.id }), expect.objectContaining({ signal: expect.any(AbortSignal) }));
      await expect(stat(job.file)).rejects.toMatchObject({ code: "ENOENT" });
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("keeps immutable jobs and retry metadata separate with bounded deterministic backoff", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-pending-"));
    try {
      let clock = Date.parse("2029-01-03T00:00:00.000Z");
      const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-pending", producerUuid: producerId(19), machineId: "machine-pending", now: () => clock });
      const current = policy(); const job = await outbox.enqueue({ episodes: [episode(current)], policy: current });
      const before = await readFile(job.file, "utf8");
      const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "pending", category: "downstream_unavailable" }) };
      const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => clock, maxClockSkewMs: 0, retryBaseMs: 100, retryMaxMs: 1000 });
      expect(await delivery.deliver({})).toEqual({ delivered: 0, pending: 1, quarantined: 0 });
      expect(await readFile(job.file, "utf8")).toBe(before);
      const controlFile = join(outbox.producerPath, "control", `${job.id}.json`);
      const firstControl = JSON.parse(await readFile(controlFile, "utf8"));
      expect(firstControl).toMatchObject({ attempts: 1, jobId: job.id, lastCategory: "downstream_unavailable" });
      expect(firstControl.nextAttemptAt).toBeGreaterThan(clock); expect(firstControl.nextAttemptAt).toBeLessThanOrEqual(clock + 100);
      await delivery.deliver({}); expect(processor.process).toHaveBeenCalledTimes(1);
      clock = firstControl.nextAttemptAt; await outbox.heartbeat(); await delivery.deliver({});
      expect(processor.process).toHaveBeenCalledTimes(2); expect(await readFile(job.file, "utf8")).toBe(before);
      expect(JSON.parse(await readFile(controlFile, "utf8")).attempts).toBe(2);
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("preserves processor-quarantined accepted payload immutably and never redelivers it", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-quarantine-"));
    try {
      const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-quarantine", producerUuid: producerId(20), machineId: "machine-quarantine", maxJobs: 1 });
      const current = policy(); const job = await outbox.enqueue({ episodes: [episode(current)], policy: current }); const before = await readFile(job.file, "utf8");
      const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "quarantined", category: "policy_mismatch" }) };
      const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => Date.parse("2029-01-03T00:00:00.000Z"), maxClockSkewMs: 0 });
      expect(await delivery.deliver({})).toEqual({ delivered: 0, pending: 0, quarantined: 1 });
      const quarantined = join(outbox.producerPath, "quarantine", `${job.id}.json`);
      expect(await readFile(quarantined, "utf8")).toBe(before);
      const reason = await readFile(join(outbox.producerPath, "quarantine", `${job.id}.reason.json`), "utf8");
      expect(reason).toContain("policy_mismatch"); expect(reason).not.toMatch(/Error:|stack|Bearer/u);
      await delivery.deliver({}); expect(processor.process).toHaveBeenCalledTimes(1); expect(await outbox.outboxStatus()).toMatchObject({ jobs: 1, captureAllowed: false });
      await expect(outbox.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000099", "new safe")], policy: current })).rejects.toThrow(/capacity/u); expect(await readFile(quarantined, "utf8")).toBe(before);
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("quarantines expired accepted jobs before processor invocation", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-expiry-"));
    try {
      const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-expiry", producerUuid: producerId(13), machineId: "machine-expiry" });
      const expiredPolicy = policy("2029-01-01T00:00:00.000Z"); const job = await outbox.enqueue({ episodes: [episode(expiredPolicy)], policy: expiredPolicy });
      const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) };
      const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => Date.parse("2029-01-01T00:00:00.001Z"), maxClockSkewMs: 0 });
      expect(await delivery.deliver({})).toEqual({ delivered: 0, pending: 0, quarantined: 1 });
      expect(processor.process).not.toHaveBeenCalled();
      await expect(stat(job.file)).rejects.toMatchObject({ code: "ENOENT" });
      await expect(stat(join(outbox.producerPath, "quarantine", `${job.id}.json`))).rejects.toMatchObject({ code: "ENOENT" });
      const audit = await readFile(join(outbox.producerPath, "quarantine", `${job.id}.expired.json`), "utf8"); expect(audit).toContain(job.auditHash); expect(audit).not.toMatch(/already \[token redacted\]|destinationIds|originProvider/u);
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("durably audits expiry before deletion and retries a failed payload unlink", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-expiry-fault-")); let failDelete = true;
    try {
      const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-expiry-fault", producerUuid: producerId(45), machineId: "machine-expiry-fault" }); const expiredPolicy = policy("2029-01-01T00:00:00.000Z"); const job = await outbox.enqueue({ episodes: [episode(expiredPolicy)], policy: expiredPolicy });
      const fs = { ...nodeFs, rm: async (path: Parameters<typeof nodeFs.rm>[0], options?: Parameters<typeof nodeFs.rm>[1]) => { if (failDelete && String(path) === job.file) throw new Error("expiry delete fault"); return options === undefined ? nodeFs.rm(path) : nodeFs.rm(path, options); } }; const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => Date.parse("2029-01-02T00:00:00.000Z"), maxClockSkewMs: 0, fs });
      expect(await delivery.deliver({})).toEqual({ delivered: 0, pending: 1, quarantined: 0 }); expect(await stat(job.file)).toBeDefined(); const auditFile = join(outbox.producerPath, "quarantine", `${job.id}.expired.json`); const audit = await readFile(auditFile, "utf8"); expect(audit).not.toContain("already [token redacted]");
      failDelete = false; expect(await delivery.deliver({})).toEqual({ delivered: 0, pending: 0, quarantined: 1 }); await expect(stat(job.file)).rejects.toMatchObject({ code: "ENOENT" }); expect(await readFile(auditFile, "utf8")).toBe(audit); expect(processor.process).not.toHaveBeenCalled();
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("adopts only canonical closed or stale producers and performs expiry checks before returning", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-adopt-"));
    try {
      let clock = Date.parse("2029-01-01T00:00:00.000Z");
      const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-adopt", producerUuid: producerId(4), machineId: "machine-adopt", now: () => clock });
      const expiredPolicy = policy("2029-01-01T00:00:00.001Z"); const job = await outbox.enqueue({ episodes: [episode(expiredPolicy)], policy: expiredPolicy });
      const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) };
      const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => clock, maxClockSkewMs: 0, heartbeatTimeoutMs: 100 });
      await expect(delivery.adopt("relative/path")).rejects.toThrow(/absolute|outside|canonical/u);
      await expect(delivery.adopt(homeDir)).rejects.toThrow(/outside|producer/u);
      await expect(delivery.adopt(outbox.producerPath)).rejects.toThrow(/active/u);
      await expect(delivery.adopt(`${outbox.producerPath}/../${outbox.producerUuid}`)).rejects.toThrow(/canonical/u);
      const link = join(outbox.root, "producer-link"); await nodeFs.symlink(outbox.producerPath, link);
      await expect(delivery.adopt(link)).rejects.toThrow(/symlink|producer|canonical/u);
      const unsafeState = await createOutbox({ host: "prime", homeDir, nodeId: "node-adopt", producerUuid: producerId(21), machineId: "machine-adopt", now: () => clock }); await unsafeState.closeProducer();
      const statePath = join(unsafeState.producerPath, "state.json"); const outsideState = join(homeDir, "outside-state.json"); await nodeFs.writeFile(outsideState, await readFile(statePath), { mode: 0o600 }); await nodeFs.rm(statePath); await nodeFs.symlink(outsideState, statePath);
      await expect(delivery.adopt(unsafeState.producerPath)).rejects.toThrow(/state|symlink|unsafe/u);
      clock += 101; await delivery.adopt(outbox.producerPath);
      await expect(stat(join(outbox.producerPath, "quarantine", `${job.id}.json`))).rejects.toMatchObject({ code: "ENOENT" });
      expect(await stat(join(outbox.producerPath, "quarantine", `${job.id}.expired.json`))).toBeDefined();
      expect(processor.process).not.toHaveBeenCalled();
      await delivery.adopt(outbox.producerPath);
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("bounds attempt deadlines, propagates abort, and leaves the accepted file after timeout", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-abort-"));
    try {
      const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-abort", producerUuid: producerId(3), machineId: "machine-abort" });
      const current = policy(); const job = await outbox.enqueue({ episodes: [episode(current)], policy: current }); let seenSignal: AbortSignal | undefined;
      const processor: OutboxJobProcessor = { process: vi.fn(async (_job, input) => { seenSignal = input.signal; await new Promise(() => undefined); return { status: "delivered" }; }) };
      const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => Date.parse("2029-01-03T00:00:00.000Z"), maxClockSkewMs: 0, attemptTimeoutMs: 5 });
      expect(await delivery.deliver({})).toEqual({ delivered: 0, pending: 1, quarantined: 0 });
      expect(seenSignal?.aborted).toBe(true); expect(await stat(job.file)).toBeDefined();
      const controller = new AbortController(); controller.abort();
      await delivery.deliver({ signal: controller.signal }); expect(processor.process).toHaveBeenCalledTimes(1);
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });


  it("survives file-sync, rename, and directory-sync fault windows without delivering orphan temps", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-faults-")); const operations: string[] = []; let fault: "none" | "file-sync" | "rename" | "dir-sync" = "none";
    const fs = {
      ...nodeFs,
      open: async (...args: Parameters<typeof open>) => {
        const handle = await open(...args); const path = String(args[0]); const flags = String(args[1]);
        return Object.assign(handle, { sync: async () => { operations.push(`sync:${flags}:${path}`); if (fault === "file-sync" && flags === "wx" && path.includes(`${join("jobs", "")}`)) throw new Error("sync fault"); if (fault === "dir-sync" && flags === "r" && path.endsWith(`${join("jobs")}`)) throw new Error("directory sync fault"); return Object.getPrototypeOf(handle).sync.call(handle); } });
      },
      rename: async (from: string, to: string) => { operations.push(`rename:${to}`); if (fault === "rename" && to.includes(`${join("jobs", "")}`)) throw new Error("rename fault"); return nodeFs.rename(from, to); },
    };
    try {
      const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-faults", producerUuid: producerId(15), machineId: "machine-faults", fs }); const current = policy(); const fileSyncInput = { episodes: [episode(current, "00000000-0000-5000-8000-000000000015")], policy: current }; const renameInput = { episodes: [episode(current, "00000000-0000-5000-8000-000000000016")], policy: current }; const directorySyncInput = { episodes: [episode(current, "00000000-0000-5000-8000-000000000017")], policy: current }; operations.length = 0;
      fault = "file-sync"; await expect(outbox.enqueue(fileSyncInput)).rejects.toThrow(/sync fault/u); expect(activeReservationNames(await readdir(join(outbox.producerPath, "jobs")))).toEqual([]); expect(await readdir(join(outbox.root, "reservations"))).toEqual(expect.arrayContaining(["admission.0000000000000000.retired"]));
      fault = "rename"; await expect(outbox.enqueue(renameInput)).rejects.toThrow(/rename fault/u); expect(activeReservationNames(await readdir(join(outbox.producerPath, "jobs")))).toEqual([]); expect(await readdir(join(outbox.root, "reservations"))).toEqual(expect.arrayContaining(["admission.0000000000000001.retired"]));
      fault = "dir-sync"; await expect(outbox.enqueue(directorySyncInput)).rejects.toThrow(/directory sync fault/u);
      const namesAfterRename = await readdir(join(outbox.producerPath, "jobs")); expect(namesAfterRename).toHaveLength(1); expect(namesAfterRename[0]).not.toContain(".tmp-");
      fault = "none"; const recovered = await outbox.enqueue(directorySyncInput); expect(recovered.file).toContain(namesAfterRename[0]!);
      const fileSync = operations.findIndex((item) => item.startsWith("sync:wx:") && item.includes("jobs")); const renameIndex = operations.findIndex((item) => item.startsWith("rename:") && item.includes("jobs")); const dirSync = operations.findIndex((item, index) => index > renameIndex && item.startsWith("sync:r:") && item.endsWith("jobs"));
      expect(fileSync).toBeGreaterThanOrEqual(0); expect(renameIndex).toBeGreaterThan(fileSync); expect(dirSync).toBeGreaterThan(renameIndex);
      const orphan = join(outbox.producerPath, "jobs", `${recovered.id}.json.tmp-orphan`); await nodeFs.writeFile(orphan, await readFile(recovered.file), { mode: 0o600 });
      const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => Date.parse("2029-01-03T00:00:00.000Z"), maxClockSkewMs: 0 });
      expect(await delivery.deliver({})).toEqual({ delivered: 1, pending: 0, quarantined: 0 }); expect(processor.process).toHaveBeenCalledTimes(1); expect(await stat(orphan)).toBeDefined();
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("enforces host-wide job and byte caps across producers while preserving prior acceptance", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-host-cap-"));
    try {
      const first = await createOutbox({ host: "prime", homeDir, nodeId: "node-host-cap", producerUuid: producerId(8), machineId: "machine-host-cap", maxJobs: 1 }); const current = policy(); const accepted = await first.enqueue({ episodes: [episode(current)], policy: current });
      const second = await createOutbox({ host: "prime", homeDir, nodeId: "node-host-cap", producerUuid: producerId(9), machineId: "machine-host-cap", maxJobs: 1 });
      await expect(second.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000002")], policy: current })).rejects.toThrow(/capacity/u); expect(await stat(accepted.file)).toBeDefined();
      const byteHome = await mkdtemp(join(tmpdir(), "task5-byte-cap-"));
      try {
        const bytes = await createOutbox({ host: "prime", homeDir: byteHome, nodeId: "node-byte-cap", producerUuid: producerId(6), machineId: "machine-byte-cap", maxBytes: 1_048_576 }); const small = await bytes.enqueue({ episodes: [episode(current)], policy: current });
        const many = Array.from({ length: 70 }, (_, index) => episode(current, `00000000-0000-5000-8000-${(index + 10).toString(16).padStart(12, "0")}`, `safe memory ${"x".repeat(15_850)}`));
        await expect(bytes.enqueue({ episodes: many, policy: current })).rejects.toThrow(/capacity/u); expect(await readFile(small.file, "utf8")).toContain(small.id);
      } finally { await rm(byteHome, { recursive: true, force: true }); }
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("quarantines malformed producer jobs while continuing safe delivery and never imports production egress clients", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-malformed-delivery-"));
    try {
      const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-malformed", producerUuid: producerId(16), machineId: "machine-malformed" }); const current = policy();
      const malformed = await outbox.enqueue({ episodes: [episode(current)], policy: current }); const valid = await outbox.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000002", "safe second")], policy: current });
      const forged = JSON.parse(await readFile(malformed.file, "utf8")); forged.nodeId = "wrong-node"; forged.rawError = "Bearer raw-secret-value-123456789"; await nodeFs.writeFile(malformed.file, JSON.stringify(forged), { mode: 0o600 });
      const outside = join(homeDir, "outside-job.json"); await nodeFs.writeFile(outside, "Bearer outside-raw-secret-value-123456789", { mode: 0o600 }); const symlinkJob = join(outbox.producerPath, "jobs", "symlink.json"); await nodeFs.symlink(outside, symlinkJob); const reads: string[] = [];
      const fs = { ...nodeFs, readFile: async (...args: Parameters<typeof nodeFs.readFile>) => { reads.push(String(args[0])); return nodeFs.readFile(...args); } };
      const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => Date.parse("2029-01-03T00:00:00.000Z"), maxClockSkewMs: 0, fs });
      expect(await delivery.deliver({})).toEqual({ delivered: 1, pending: 0, quarantined: 0 }); expect(processor.process).toHaveBeenCalledWith(expect.objectContaining({ id: valid.id }), expect.anything());
      const malformedAudit = (await readdir(join(outbox.producerPath, "quarantine"))).find((name) => name.startsWith("malformed-")); expect(malformedAudit).toBeDefined(); expect(await readFile(join(outbox.producerPath, "quarantine", malformedAudit!), "utf8")).not.toContain("raw-secret-value");
      expect(reads).not.toContain(symlinkJob); expect(await readFile(outside, "utf8")).toContain("outside-raw-secret");
      const sources = `${await readFile("src/outbox/store.ts", "utf8")}
${await readFile("src/outbox/delivery.ts", "utf8")}`;
      expect(sources).not.toMatch(/from ["'][^"']*(?:clients\/(?:qdrant|embeddings)|qdrant\/write|episode-writer)[^"']*["']|console\.(?:log|warn|error)|process\.stderr/iu); expect(sources).not.toMatch(/markDelivered/u);
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("keeps delivered jobs when deletion fails and bounds unsafe processor quarantine categories", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-delete-failure-")); let failDelete = true;
    try {
      const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-delete", producerUuid: producerId(10), machineId: "machine-delete" }); const current = policy(); const job = await outbox.enqueue({ episodes: [episode(current)], policy: current });
      const fs = { ...nodeFs, rm: async (path: Parameters<typeof nodeFs.rm>[0], options?: Parameters<typeof nodeFs.rm>[1]) => { if (failDelete && String(path) === job.file) throw new Error("delete fault"); return options === undefined ? nodeFs.rm(path) : nodeFs.rm(path, options); } };
      const delivered: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor: delivered, now: () => Date.parse("2029-01-03T00:00:00.000Z"), maxClockSkewMs: 0, fs });
      expect(await delivery.deliver({})).toEqual({ delivered: 0, pending: 1, quarantined: 0 }); expect(await stat(job.file)).toBeDefined();
      failDelete = false; const quarantining: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "quarantined", category: "Bearer raw-secret-value-123456789" }) }; const other = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor: quarantining, now: () => Date.parse("2029-01-03T00:00:00.001Z"), maxClockSkewMs: 0 }); await other.deliver({}); expect(quarantining.process).not.toHaveBeenCalled(); await expect(stat(job.file)).rejects.toMatchObject({ code: "ENOENT" });
      const quarantineJob = await outbox.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000010")], policy: current }); await other.deliver({}); const reason = await readFile(join(outbox.producerPath, "quarantine", `${quarantineJob.id}.reason.json`), "utf8"); expect(reason).toContain("processor_quarantined"); expect(reason).not.toContain("raw-secret-value");
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("keeps a durable fence authoritative when heartbeat refreshes after fencing", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-adopt-race-"));
    try {
      const clock = Date.parse("2029-01-03T00:00:00.000Z"); const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-adopt-race", producerUuid: producerId(43), machineId: "machine-adopt-race", now: () => clock }); await outbox.closeProducer(); const statePath = join(outbox.producerPath, "state.json"); let mutated = false;
      const fs = { ...nodeFs, readdir: async (...args: Parameters<typeof nodeFs.readdir>) => { if (!mutated && String(args[0]).includes(`${outbox.producerPath}/jobs.fenced-`)) { mutated = true; const state = { version: 1, state: "active", heartbeatAt: clock, closedAt: null, auditHash: "" }; state.auditHash = sha256Hex(canonicalStringify({ closedAt: state.closedAt, heartbeatAt: state.heartbeatAt, state: state.state, version: state.version })); await nodeFs.writeFile(statePath, canonicalStringify(state), { mode: 0o600 }); } return nodeFs.readdir(...args); } };
      const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => clock, maxClockSkewMs: 0, fs });
      await expect(delivery.adopt(outbox.producerPath)).resolves.toBeUndefined(); expect(mutated).toBe(true); expect((await readdir(outbox.producerPath)).filter((name) => name.startsWith("jobs.fenced-"))).toHaveLength(1);
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("allows duplicate offline adopters without using a local lock as delivery authority", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-duplicate-adopt-"));
    try {
      const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-duplicate", producerUuid: producerId(12), machineId: "machine-duplicate" }); const current = policy(); const job = await outbox.enqueue({ episodes: [episode(current)], policy: current }); await outbox.closeProducer();
      let calls = 0; let release!: () => void; const ready = new Promise<void>((resolve) => { release = resolve; });
      const processor: OutboxJobProcessor = { process: vi.fn(async () => { calls += 1; if (calls === 2) release(); await ready; return { status: "delivered" }; }) };
      const input = { outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => Date.parse("2029-01-03T00:00:00.000Z"), maxClockSkewMs: 0 }; const left = createOutboxDelivery(input); const right = createOutboxDelivery(input);
      await Promise.all([left.adopt(outbox.producerPath), right.adopt(outbox.producerPath)]); const results = await Promise.all([left.deliver({}), right.deliver({})]);
      expect(processor.process).toHaveBeenCalledTimes(2); expect(results.reduce((sum, item) => sum + item.delivered, 0)).toBeGreaterThanOrEqual(1); await expect(stat(job.file)).rejects.toMatchObject({ code: "ENOENT" });
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });


  it("returns promptly when an in-flight delivery is externally aborted and shutdown remains best effort", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-external-abort-"));
    try {
      const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-external-abort", producerUuid: producerId(14), machineId: "machine-external-abort" }); const current = policy(); const job = await outbox.enqueue({ episodes: [episode(current)], policy: current });
      let signal: AbortSignal | undefined; let markStarted!: () => void; const processStarted = new Promise<void>((resolve) => { markStarted = resolve; }); const processor: OutboxJobProcessor = { process: vi.fn(async (_job, input) => { signal = input.signal; markStarted(); await new Promise(() => undefined); return { status: "delivered" }; }) };
      const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => Date.parse("2029-01-03T00:00:00.000Z"), maxClockSkewMs: 0, attemptTimeoutMs: 100 }); const controller = new AbortController();
      const pending = delivery.deliver({ signal: controller.signal }); await processStarted; const started = Date.now(); controller.abort();
      expect(await pending).toEqual({ delivered: 0, pending: 1, quarantined: 0 }); expect(Date.now() - started).toBeLessThan(80); expect(signal?.aborted).toBe(true); expect(await stat(job.file)).toBeDefined();
      expect(await delivery.shutdown({ signal: controller.signal })).toEqual({ delivered: 0, pending: 0, quarantined: 0 }); expect(await stat(job.file)).toBeDefined();
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });


  it("leaves an accepted job untouched across a transient job read EIO and delivers it on retry", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-job-eio-"));
    try {
      const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-job-eio", producerUuid: producerId(50), machineId: "machine-job-eio" }); const current = policy(); const job = await outbox.enqueue({ episodes: [episode(current)], policy: current }); let failRead = true;
      const fs = { ...nodeFs, readFile: async (...args: Parameters<typeof nodeFs.readFile>) => { if (failRead && String(args[0]) === job.file) { failRead = false; throw Object.assign(new Error("transient read"), { code: "EIO" }); } return nodeFs.readFile(...args); } }; const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => Date.parse("2029-01-03T00:00:00.000Z"), maxClockSkewMs: 0, fs });
      expect(await delivery.deliver({})).toEqual({ delivered: 0, pending: 1, quarantined: 0 }); expect(await stat(job.file)).toBeDefined(); expect(processor.process).not.toHaveBeenCalled();
      expect(await delivery.deliver({})).toEqual({ delivered: 1, pending: 0, quarantined: 0 }); expect(processor.process).toHaveBeenCalledTimes(1);
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("rechecks expiry at the processor boundary when the clock crosses the deadline", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-expiry-boundary-"));
    try {
      const deadline = Date.parse("2029-01-03T00:00:00.000Z"); const current = policy(new Date(deadline).toISOString()); const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-expiry-boundary", producerUuid: producerId(51), machineId: "machine-expiry-boundary" }); const job = await outbox.enqueue({ episodes: [episode(current)], policy: current }); const times = [deadline - 1, deadline + 1]; let index = 0;
      const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => times[Math.min(index++, times.length - 1)]!, maxClockSkewMs: 0 });
      expect(await delivery.deliver({})).toEqual({ delivered: 0, pending: 0, quarantined: 1 }); expect(processor.process).not.toHaveBeenCalled(); await expect(stat(job.file)).rejects.toMatchObject({ code: "ENOENT" });
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("fsyncs each newly-created first-run ancestor before creating its child", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-mkdir-sync-")); const operations: string[] = [];
    const fs = { ...nodeFs, mkdir: async (path: Parameters<typeof nodeFs.mkdir>[0], options?: Parameters<typeof nodeFs.mkdir>[1]) => { operations.push(`mkdir:${String(path)}`); return options === undefined ? nodeFs.mkdir(path) : nodeFs.mkdir(path, options); }, open: async (...args: Parameters<typeof open>) => { const handle = await open(...args); const path = String(args[0]); const flags = String(args[1]); return Object.assign(handle, { sync: async () => { operations.push(`sync:${flags}:${path}`); return Object.getPrototypeOf(handle).sync.call(handle); } }); } };
    try {
      await createOutbox({ host: "prime", homeDir, nodeId: "node-mkdir-sync", producerUuid: producerId(52), machineId: "machine-mkdir-sync", fs });
      const created = [join(homeDir, ".prime"), join(homeDir, ".prime", "agent"), join(homeDir, ".prime", "agent", "pi-qdrant-memory"), join(homeDir, ".prime", "agent", "pi-qdrant-memory", "outbox"), join(homeDir, ".prime", "agent", "pi-qdrant-memory", "outbox", "node-mkdir-sync")];
      for (let index = 0; index < created.length; index += 1) { const dir = created[index]!; const mkdirIndex = operations.findIndex((item) => item === `mkdir:${dir}`); const parentSync = operations.findIndex((item, opIndex) => opIndex > mkdirIndex && item === `sync:r:${join(dir, "..")}`); const nextMkdir = index + 1 === created.length ? Number.POSITIVE_INFINITY : operations.findIndex((item) => item === `mkdir:${created[index + 1]}`); expect(mkdirIndex).toBeGreaterThanOrEqual(0); expect(parentSync).toBeGreaterThan(mkdirIndex); expect(parentSync).toBeLessThan(nextMkdir); }
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("keeps jobs active for transient and malformed retry-control state", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-control-errors-")); let clock = Date.parse("2029-01-03T00:00:00.000Z");
    try {
      const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-control-errors", producerUuid: producerId(53), machineId: "machine-control-errors", now: () => clock }); const current = policy(); const job = await outbox.enqueue({ episodes: [episode(current)], policy: current }); const pendingProcessor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "pending" }) }; const initial = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor: pendingProcessor, now: () => clock, maxClockSkewMs: 0, retryBaseMs: 100, retryMaxMs: 1000 }); await initial.deliver({}); const controlFile = join(outbox.producerPath, "control", `${job.id}.json`); clock = JSON.parse(await readFile(controlFile, "utf8")).nextAttemptAt;
      let failControl = true; const fs = { ...nodeFs, readFile: async (...args: Parameters<typeof nodeFs.readFile>) => { if (failControl && String(args[0]) === controlFile) { failControl = false; throw Object.assign(new Error("control EIO"), { code: "EIO" }); } return nodeFs.readFile(...args); } }; const delivered: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const retry = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor: delivered, now: () => clock, maxClockSkewMs: 0, fs });
      expect(await retry.deliver({})).toEqual({ delivered: 0, pending: 1, quarantined: 0 }); expect(delivered.process).not.toHaveBeenCalled(); expect(await stat(job.file)).toBeDefined();
      await nodeFs.writeFile(controlFile, "{broken", { mode: 0o600 }); expect(await retry.deliver({})).toEqual({ delivered: 0, pending: 1, quarantined: 0 }); expect(delivered.process).not.toHaveBeenCalled(); expect(await stat(job.file)).toBeDefined();
      expect((await readdir(join(outbox.producerPath, "quarantine"))).some((name) => name.startsWith(`control-${job.id}-`))).toBe(true);
      expect(await retry.deliver({})).toEqual({ delivered: 1, pending: 0, quarantined: 0 }); expect(delivered.process).toHaveBeenCalledTimes(1);
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("admits at most one concurrent host reservation at maxJobs one", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-reservation-race-"));
    try {
      const left = await createOutbox({ host: "prime", homeDir, nodeId: "node-reservation", producerUuid: producerId(54), machineId: "machine-reservation", maxJobs: 1 }); const right = await createOutbox({ host: "prime", homeDir, nodeId: "node-reservation", producerUuid: producerId(55), machineId: "machine-reservation", maxJobs: 1 }); const current = policy();
      const settled = await Promise.allSettled([left.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000054")], policy: current }), right.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000055")], policy: current })]); expect(settled.filter((item) => item.status === "fulfilled")).toHaveLength(1); expect((await left.outboxStatus()).jobs).toBe(1);
      const files = [...await readdir(join(left.producerPath, "jobs")), ...await readdir(join(right.producerPath, "jobs"))].filter((name) => name.endsWith(".json")); expect(files).toHaveLength(1);
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("replaces a poisoned expiry sidecar without retaining or following raw payload", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-expiry-poison-"));
    try {
      const current = policy("2029-01-01T00:00:00.000Z"); const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-expiry-poison", producerUuid: producerId(56), machineId: "machine-expiry-poison" }); const job = await outbox.enqueue({ episodes: [episode(current)], policy: current }); const auditFile = join(outbox.producerPath, "quarantine", `${job.id}.expired.json`); await nodeFs.writeFile(auditFile, "Bearer poisoned-expiry-raw-value-123456789", { mode: 0o600 }); const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => Date.parse("2029-01-02T00:00:00.000Z"), maxClockSkewMs: 0 });
      expect(await delivery.deliver({})).toEqual({ delivered: 0, pending: 0, quarantined: 1 }); const audit = await readFile(auditFile, "utf8"); expect(audit).toContain(job.auditHash); expect(audit).not.toContain("poisoned-expiry"); await expect(stat(job.file)).rejects.toMatchObject({ code: "ENOENT" });
      const linkedJob = await outbox.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000056")], policy: current }); const linkedAudit = join(outbox.producerPath, "quarantine", `${linkedJob.id}.expired.json`); const outside = join(homeDir, "outside-expiry"); await nodeFs.writeFile(outside, "Bearer outside-expiry-secret-123456789", { mode: 0o600 }); await nodeFs.symlink(outside, linkedAudit); expect(await delivery.deliver({})).toEqual({ delivered: 0, pending: 0, quarantined: 1 }); expect(await readFile(outside, "utf8")).toContain("outside-expiry-secret"); expect(await readFile(linkedAudit, "utf8")).not.toContain("outside-expiry-secret");
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("delivers only the current producer plus explicitly adopted producers", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-delivery-scope-"));
    try {
      const currentOutbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-delivery-scope", producerUuid: producerId(57), machineId: "machine-delivery-scope" }); const foreign = await createOutbox({ host: "prime", homeDir, nodeId: "node-delivery-scope", producerUuid: producerId(58), machineId: "machine-delivery-scope" }); const currentPolicy = policy(); await currentOutbox.enqueue({ episodes: [episode(currentPolicy, "00000000-0000-5000-8000-000000000057")], policy: currentPolicy }); const foreignJob = await foreign.enqueue({ episodes: [episode(currentPolicy, "00000000-0000-5000-8000-000000000058")], policy: currentPolicy }); const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const delivery = createOutboxDelivery({ outboxRoot: currentOutbox.root, producerPath: currentOutbox.producerPath, processor, now: () => Date.parse("2029-01-03T00:00:00.000Z"), maxClockSkewMs: 0 });
      expect(await delivery.deliver({})).toEqual({ delivered: 1, pending: 0, quarantined: 0 }); expect(await stat(foreignJob.file)).toBeDefined();
      const currentAgain = await currentOutbox.enqueue({ episodes: [episode(currentPolicy, "00000000-0000-5000-8000-000000000059")], policy: currentPolicy }); await foreign.closeProducer(); await delivery.adopt(foreign.producerPath); expect(await delivery.deliver({})).toEqual({ delivered: 2, pending: 0, quarantined: 0 }); await expect(stat(currentAgain.file)).rejects.toMatchObject({ code: "ENOENT" }); await expect(stat(foreignJob.file)).rejects.toMatchObject({ code: "ENOENT" });
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("counts every regular queue artifact toward the host byte cap without deleting it", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-artifact-cap-"));
    try {
      const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-artifact-cap", producerUuid: producerId(59), machineId: "machine-artifact-cap", maxBytes: 1_048_576 }); const artifact = join(outbox.producerPath, "quarantine", "accumulated-audit.json"); await nodeFs.writeFile(artifact, "x".repeat(1_048_000), { mode: 0o600 }); const current = policy();
      await expect(outbox.enqueue({ episodes: [episode(current)], policy: current })).rejects.toThrow(/capacity/u); expect((await stat(artifact)).size).toBe(1_048_000); expect((await outbox.outboxStatus()).bytes).toBeGreaterThanOrEqual(1_048_000);
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });


  it("leaves accepted listPending payloads untouched across transient reads", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-list-eio-")); let target = ""; let fail = false;
    const fs = { ...nodeFs, readFile: async (...args: Parameters<typeof nodeFs.readFile>) => { if (fail && String(args[0]) === target) { fail = false; throw Object.assign(new Error("list EIO"), { code: "EIO" }); } return nodeFs.readFile(...args); } };
    try { const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-list-eio", producerUuid: producerId(60), machineId: "machine-list-eio", fs }); const current = policy(); const job = await outbox.enqueue({ episodes: [episode(current)], policy: current }); target = job.file; fail = true; expect(await outbox.listPending()).toEqual([]); expect(await stat(job.file)).toBeDefined(); expect(await outbox.listPending()).toHaveLength(1); }
    finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("recovers durable reservations from crash windows only after offline producer proof", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-reservation-crash-")); let crash = true;
    const fs = { ...nodeFs, link: async (existingPath: Parameters<typeof nodeFs.link>[0], newPath: Parameters<typeof nodeFs.link>[1]) => { if (crash && ADMISSION_LOCK_NAME.test(String(newPath).split(/[\/]/u).at(-1) ?? "")) throw new Error("crash before job"); return nodeFs.link(existingPath, newPath); } };
    try {
      const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-reservation-crash", producerUuid: producerId(61), machineId: "machine-reservation-crash", fs }); const current = policy(); await expect(outbox.enqueue({ episodes: [episode(current)], policy: current })).rejects.toThrow(/crash before job/u); expect(activeReservationNames(await readdir(join(outbox.producerPath, "jobs")))).toEqual([]); expect(activeReservationNames(await readdir(join(outbox.root, "reservations")))).toEqual([]);
      await outbox.closeProducer(); crash = false; const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => Date.parse("2029-01-03T00:00:00.000Z"), maxClockSkewMs: 0 }); await delivery.adopt(outbox.producerPath); expect(activeReservationNames(await readdir(join(outbox.root, "reservations")))).toEqual([]);
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("serializes concurrent byte reservations against every existing artifact", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-byte-reservation-"));
    try {
      const left = await createOutbox({ host: "prime", homeDir, nodeId: "node-byte-reservation", producerUuid: producerId(62), machineId: "machine-byte-reservation", maxBytes: 1_048_576 }); const right = await createOutbox({ host: "prime", homeDir, nodeId: "node-byte-reservation", producerUuid: producerId(63), machineId: "machine-byte-reservation", maxBytes: 1_048_576 }); await nodeFs.writeFile(join(left.producerPath, "quarantine", "audit-fill"), "x".repeat(1_044_000), { mode: 0o600 }); const current = policy(); const settled = await Promise.allSettled([left.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000062")], policy: current }), right.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000063")], policy: current })]); expect(settled.filter((item) => item.status === "fulfilled")).toHaveLength(1); expect((await left.outboxStatus()).jobs).toBe(1);
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("rejects a raced first-run ancestor with unsafe permissions instead of chmodding it", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-mkdir-race-")); const raced = join(homeDir, ".prime"); let first = true;
    const fs = { ...nodeFs, lstat: async (path: Parameters<typeof nodeFs.lstat>[0], options?: Parameters<typeof nodeFs.lstat>[1]) => { if (first && String(path) === raced) { first = false; throw Object.assign(new Error("missing"), { code: "ENOENT" }); } return options === undefined ? nodeFs.lstat(path) : nodeFs.lstat(path, options); }, mkdir: async (path: Parameters<typeof nodeFs.mkdir>[0], options?: Parameters<typeof nodeFs.mkdir>[1]) => { if (String(path) === raced) { await nodeFs.mkdir(path, { mode: 0o755 }); throw Object.assign(new Error("raced"), { code: "EEXIST" }); } return options === undefined ? nodeFs.mkdir(path) : nodeFs.mkdir(path, options); } };
    try { await expect(createOutbox({ host: "prime", homeDir, nodeId: "node-mkdir-race", producerUuid: producerId(64), machineId: "machine-mkdir-race", fs })).rejects.toThrow(/permissions/u); expect((await stat(raced)).mode & 0o077).not.toBe(0); }
    finally { await rm(homeDir, { recursive: true, force: true }); }
  });


  it("keeps stale-open precommit reservations fenced through paused job and byte admissions", async () => {
    for (const kind of ["jobs", "bytes"] as const) {
      const homeDir = await mkdtemp(join(tmpdir(), `task5-stale-precommit-${kind}-`)); let reached!: () => void; let resume!: () => void; const paused = new Promise<void>((resolvePaused) => { reached = resolvePaused; }); const resumeGate = new Promise<void>((resolveResume) => { resume = resolveResume; }); let pauseOnce = true;
      const fsA = { ...nodeFs, rename: async (from: Parameters<typeof nodeFs.rename>[0], to: Parameters<typeof nodeFs.rename>[1]) => { if (pauseOnce && String(to).split(/[\\/]/u).at(-2) === "jobs" && String(to).endsWith(".json")) { pauseOnce = false; reached(); await resumeGate; } return nodeFs.rename(from, to); } };
      try {
        const limits = kind === "jobs" ? { maxJobs: 1 } : { maxBytes: 1_048_576 }; const producerA = await createOutbox({ host: "prime", homeDir, nodeId: `node-stale-${kind}`, producerUuid: producerId(kind === "jobs" ? 70 : 72), machineId: `machine-stale-${kind}`, now: () => 1_000, fs: fsA, ...limits }); const producerB = await createOutbox({ host: "prime", homeDir, nodeId: `node-stale-${kind}`, producerUuid: producerId(kind === "jobs" ? 71 : 73), machineId: `machine-stale-${kind}`, now: () => 1_000, ...limits }); if (kind === "bytes") await nodeFs.writeFile(join(producerA.producerPath, "quarantine", "audit-fill"), "x".repeat(1_044_000), { mode: 0o600 }); const current = policy();
        const acceptingA = producerA.enqueue({ episodes: [episode(current, kind === "jobs" ? "00000000-0000-5000-8000-000000000070" : "00000000-0000-5000-8000-000000000072")], policy: current }); const acceptingOutcome = acceptingA.then(() => true, () => false); await paused; const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const adopter = createOutboxDelivery({ outboxRoot: producerB.root, producerPath: producerB.producerPath, processor, now: () => 100_000, maxClockSkewMs: 0, heartbeatTimeoutMs: 100 }); await adopter.adopt(producerA.producerPath);
        expect(activeReservationNames(await readdir(join(producerA.root, "reservations")))).toEqual([]); const fenced = (await readdir(producerA.producerPath)).filter((name) => name.startsWith("jobs.fenced-")); expect(fenced).toHaveLength(1); resume(); expect(await acceptingOutcome).toBe(false); await expect(producerA.outboxStatus()).rejects.toThrow(/fenced/u); expect((await readdir(join(producerA.producerPath, fenced[0]!))).filter((name) => name.includes(".json.tmp-"))).toEqual([]); expect(await adopter.deliver({})).toEqual({ delivered: 1, pending: 0, quarantined: 0 }); const acceptedB = await producerB.enqueue({ episodes: [episode(current, kind === "jobs" ? "00000000-0000-5000-8000-000000000071" : "00000000-0000-5000-8000-000000000073")], policy: current }); expect(await stat(acceptedB.file)).toBeDefined();
      } finally { resume?.(); await rm(homeDir, { recursive: true, force: true }); }
    }
  }, 15_000);

  it("retries durable lock finalization before and after job commit without losing proof", async () => {
    const afterHome = await mkdtemp(join(tmpdir(), "task5-finalize-after-")); let failLockRm = true;
    const afterFs = { ...nodeFs, rm: async (path: Parameters<typeof nodeFs.rm>[0], options?: Parameters<typeof nodeFs.rm>[1]) => { if (failLockRm && ADMISSION_LOCK_NAME.test(String(path).split(/[\/]/u).at(-1) ?? "")) { failLockRm = false; throw Object.assign(new Error("after-commit lock EIO"), { code: "EIO" }); } return options === undefined ? nodeFs.rm(path) : nodeFs.rm(path, options); } };
    try {
      const outbox = await createOutbox({ host: "prime", homeDir: afterHome, nodeId: "node-finalize-after", producerUuid: producerId(74), machineId: "machine-finalize-after", fs: afterFs }); const current = policy(); await expect(outbox.enqueue({ episodes: [episode(current)], policy: current })).rejects.toThrow(/after-commit/u); expect(await outbox.listPending()).toHaveLength(1); expect(await readdir(join(outbox.root, "reservations"))).toEqual(expect.arrayContaining([expect.stringMatching(ADMISSION_LOCK_NAME)])); const recovered = await outbox.enqueue({ episodes: [episode(current)], policy: current }); expect(await stat(recovered.file)).toBeDefined(); expect(activeReservationNames(await readdir(join(outbox.root, "reservations")))).toEqual([]);
    } finally { await rm(afterHome, { recursive: true, force: true }); }

    const syncHome = await mkdtemp(join(tmpdir(), "task5-finalize-sync-")); let armSync = false; let failSync = true;
    const syncFs = { ...nodeFs, rm: async (path: Parameters<typeof nodeFs.rm>[0], options?: Parameters<typeof nodeFs.rm>[1]) => { const result = options === undefined ? await nodeFs.rm(path) : await nodeFs.rm(path, options); if (ADMISSION_LOCK_NAME.test(String(path).split(/[\/]/u).at(-1) ?? "")) armSync = true; return result; }, open: async (...args: Parameters<typeof open>) => { const handle = await open(...args); const path = String(args[0]); const flags = String(args[1]); return Object.assign(handle, { sync: async () => { if (armSync && failSync && flags === "r" && path.endsWith("reservations")) { failSync = false; armSync = false; throw Object.assign(new Error("lock fsync EIO"), { code: "EIO" }); } return Object.getPrototypeOf(handle).sync.call(handle); } }); } };
    try { const outbox = await createOutbox({ host: "prime", homeDir: syncHome, nodeId: "node-finalize-sync", producerUuid: producerId(75), machineId: "machine-finalize-sync", fs: syncFs }); const current = policy(); await expect(outbox.enqueue({ episodes: [episode(current)], policy: current })).rejects.toThrow(/lock fsync/u); expect(await outbox.listPending()).toHaveLength(1); expect(await readdir(join(outbox.root, "reservations"))).not.toEqual(expect.arrayContaining([expect.stringMatching(ADMISSION_LOCK_NAME)])); await outbox.enqueue({ episodes: [episode(current)], policy: current }); expect(activeReservationNames(await readdir(join(outbox.root, "reservations")))).toEqual([]); }
    finally { await rm(syncHome, { recursive: true, force: true }); }

    const beforeHome = await mkdtemp(join(tmpdir(), "task5-finalize-before-"));
    try {
      const winner = await createOutbox({ host: "prime", homeDir: beforeHome, nodeId: "node-finalize-before", producerUuid: producerId(76), machineId: "machine-finalize-before", maxJobs: 1 }); const current = policy(); await winner.enqueue({ episodes: [episode(current)], policy: current }); let failBefore = true; const beforeFs = { ...nodeFs, rm: async (path: Parameters<typeof nodeFs.rm>[0], options?: Parameters<typeof nodeFs.rm>[1]) => { if (failBefore && ADMISSION_LOCK_NAME.test(String(path).split(/[\/]/u).at(-1) ?? "")) { failBefore = false; throw Object.assign(new Error("precommit lock EIO"), { code: "EIO" }); } return options === undefined ? nodeFs.rm(path) : nodeFs.rm(path, options); } }; const rejected = await createOutbox({ host: "prime", homeDir: beforeHome, nodeId: "node-finalize-before", producerUuid: producerId(77), machineId: "machine-finalize-before", maxJobs: 1, fs: beforeFs }); const input = { episodes: [episode(current, "00000000-0000-5000-8000-000000000077")], policy: current }; await expect(rejected.enqueue(input)).rejects.toThrow(/precommit/u); await expect(rejected.enqueue(input)).rejects.toThrow(/retired/u); expect(activeReservationNames(await readdir(join(winner.root, "reservations")))).toEqual([]); expect((await winner.outboxStatus()).jobs).toBe(1);
    } finally { await rm(beforeHome, { recursive: true, force: true }); }
  });

  it("retains immutable proof across delivered, quarantine, and expiry finalization faults", async () => {
    for (const terminal of ["delivered", "quarantined", "expired"] as const) for (const fault of ["rm", "sync"] as const) {
      const homeDir = await mkdtemp(join(tmpdir(), `task5-terminal-${terminal}-${fault}-`)); let failStore = true;
      const storeFs = { ...nodeFs, rm: async (path: Parameters<typeof nodeFs.rm>[0], options?: Parameters<typeof nodeFs.rm>[1]) => { if (failStore && ADMISSION_LOCK_NAME.test(String(path).split(/[\/]/u).at(-1) ?? "")) { failStore = false; throw Object.assign(new Error("retain admission proof"), { code: "EIO" }); } return options === undefined ? nodeFs.rm(path) : nodeFs.rm(path, options); } };
      try {
        let clock = terminal === "expired" ? Date.parse("2028-12-31T00:00:00.000Z") : Date.parse("2029-01-02T00:00:00.000Z"); const expiresAt = terminal === "expired" ? "2029-01-01T00:00:00.000Z" : "2030-01-01T00:00:00.000Z"; const current = policy(expiresAt); const index = 80 + (["delivered", "quarantined", "expired"] as const).indexOf(terminal) * 4 + (fault === "rm" ? 0 : 1); const outbox = await createOutbox({ host: "prime", homeDir, nodeId: `node-terminal-${terminal}-${fault}`, producerUuid: producerId(index), machineId: `machine-terminal-${terminal}-${fault}`, now: () => clock, fs: storeFs }); await expect(outbox.enqueue({ episodes: [episode(current)], policy: current })).rejects.toThrow(/retain admission/u); const [job] = await outbox.listPending(); expect(job).toBeDefined(); clock = Date.parse("2029-01-03T00:00:00.000Z"); await outbox.heartbeat(); let failTerminal = true; let armTerminalSync = false;
        const terminalFs = { ...nodeFs, rm: async (path: Parameters<typeof nodeFs.rm>[0], options?: Parameters<typeof nodeFs.rm>[1]) => { if (failTerminal && fault === "rm" && ADMISSION_LOCK_NAME.test(String(path).split(/[\/]/u).at(-1) ?? "")) { failTerminal = false; throw Object.assign(new Error("terminal lock EIO"), { code: "EIO" }); } const result = options === undefined ? await nodeFs.rm(path) : await nodeFs.rm(path, options); if (failTerminal && fault === "sync" && ADMISSION_LOCK_NAME.test(String(path).split(/[\/]/u).at(-1) ?? "")) armTerminalSync = true; return result; }, open: async (...args: Parameters<typeof open>) => { const handle = await open(...args); const path = String(args[0]); const flags = String(args[1]); return Object.assign(handle, { sync: async () => { if (failTerminal && armTerminalSync && flags === "r" && path.endsWith("reservations")) { failTerminal = false; armTerminalSync = false; throw Object.assign(new Error("terminal sync EIO"), { code: "EIO" }); } return Object.getPrototypeOf(handle).sync.call(handle); } }); } }; const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue(terminal === "quarantined" ? { status: "quarantined", category: "terminal-test" } : { status: "delivered" }) }; const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => clock, maxClockSkewMs: 0, fs: terminalFs });
        const firstTerminal = await delivery.deliver({}); const terminalResult = terminal === "delivered" ? { delivered: 1, pending: 0, quarantined: 0 } : terminal === "quarantined" ? { delivered: 0, pending: 0, quarantined: 1 } : { delivered: 0, pending: 0, quarantined: 1 }; if (firstTerminal.pending === 1) { expect(firstTerminal).toEqual({ delivered: 0, pending: 1, quarantined: 0 }); if (terminal === "expired") { const fenced = (await readdir(outbox.producerPath)).find((name) => name.startsWith("jobs.fenced-")); expect(fenced).toBeDefined(); expect(await stat(join(outbox.producerPath, fenced!, `${job!.id}.json`))).toBeDefined(); } else if (terminal === "quarantined") expect(await stat(join(outbox.producerPath, "quarantine", `${job!.id}.json`))).toBeDefined(); else expect(await stat(job!.file)).toBeDefined(); expect(await delivery.deliver({})).toEqual(terminalResult); } else expect(firstTerminal).toEqual(terminalResult); expect(activeReservationNames(await readdir(join(outbox.root, "reservations")))).toEqual([]);
        if (terminal === "delivered") { await expect(stat(job!.file)).rejects.toMatchObject({ code: "ENOENT" }); const next = await outbox.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000099")], policy: current }); expect(await stat(next.file)).toBeDefined(); }
      } finally { await rm(homeDir, { recursive: true, force: true }); }
    }
  }, 20_000);


  it("expires accepted quarantine payloads to audit-only state without processor replay", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-quarantine-expiry-")); let clock = Date.parse("2029-01-01T00:00:00.000Z");
    try {
      const current = policy("2029-01-02T00:00:00.000Z"); const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-quarantine-expiry", producerUuid: producerId(100), machineId: "machine-quarantine-expiry" }); const job = await outbox.enqueue({ episodes: [episode(current)], policy: current }); const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "quarantined", category: "retained-before-expiry" }) }; const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => clock, maxClockSkewMs: 0 });
      expect(await delivery.deliver({})).toEqual({ delivered: 0, pending: 0, quarantined: 1 }); const payload = join(outbox.producerPath, "quarantine", `${job.id}.json`); expect(await readFile(payload, "utf8")).toContain("already [token redacted]"); clock = Date.parse("2029-01-03T00:00:00.000Z"); expect(await delivery.deliver({})).toEqual({ delivered: 0, pending: 0, quarantined: 1 }); expect(processor.process).toHaveBeenCalledTimes(1); await expect(stat(payload)).rejects.toMatchObject({ code: "ENOENT" }); await expect(stat(join(outbox.producerPath, "quarantine", `${job.id}.reason.json`))).rejects.toMatchObject({ code: "ENOENT" }); const remaining = await Promise.all((await readdir(join(outbox.producerPath, "quarantine"))).map((name) => readFile(join(outbox.producerPath, "quarantine", name), "utf8"))); expect(remaining.join("\n")).not.toContain("already [token redacted]"); expect(remaining.join("\n")).not.toContain("destinationIds"); expect(remaining.join("\n")).toContain(job.auditHash); const manual = await outbox.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000100")], policy: current }); await outbox.quarantine(manual, "manual-retention"); expect(await delivery.deliver({})).toEqual({ delivered: 0, pending: 0, quarantined: 1 }); await expect(stat(join(outbox.producerPath, "quarantine", `${manual.id}.json`))).rejects.toMatchObject({ code: "ENOENT" }); expect(processor.process).toHaveBeenCalledTimes(1);
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("cleans an unaccepted reservation after its first canonical-link directory fsync fails", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-reservation-link-sync-")); let arm = false; let fail = true;
    const fs = { ...nodeFs, link: async (existingPath: Parameters<typeof nodeFs.link>[0], newPath: Parameters<typeof nodeFs.link>[1]) => { const result = await nodeFs.link(existingPath, newPath); if (String(newPath).endsWith(".json")) arm = true; return result; }, open: async (...args: Parameters<typeof open>) => { const handle = await open(...args); const path = String(args[0]); const flags = String(args[1]); return Object.assign(handle, { sync: async () => { if (arm && fail && flags === "r" && path.endsWith("reservations")) { fail = false; arm = false; throw Object.assign(new Error("reservation link fsync EIO"), { code: "EIO" }); } return Object.getPrototypeOf(handle).sync.call(handle); } }); } };
    try { const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-reservation-link-sync", producerUuid: producerId(101), machineId: "machine-reservation-link-sync", maxJobs: 1, fs }); const current = policy(); await expect(outbox.enqueue({ episodes: [episode(current)], policy: current })).rejects.toThrow(/reservation link fsync/u); expect(activeReservationNames(await readdir(join(outbox.root, "reservations")))).toEqual([]); const accepted = await outbox.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000101")], policy: current }); expect(await stat(accepted.file)).toBeDefined(); expect((await outbox.outboxStatus()).jobs).toBe(1); }
    finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("uses durable delivered audits to reconcile ambiguous deletion without replay", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-delivered-reconcile-"));
    try {
      const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-delivered-reconcile", producerUuid: producerId(102), machineId: "machine-delivered-reconcile" }); const current = policy(); const job = await outbox.enqueue({ episodes: [episode(current)], policy: current }); let armJobsSync = false; let failJobsSync = true; let restoreOpens = 0;
      const fs = { ...nodeFs, rm: async (path: Parameters<typeof nodeFs.rm>[0], options?: Parameters<typeof nodeFs.rm>[1]) => { const result = options === undefined ? await nodeFs.rm(path) : await nodeFs.rm(path, options); if (String(path) === job.file) armJobsSync = true; return result; }, open: async (...args: Parameters<typeof open>) => { const path = String(args[0]); const flags = String(args[1]); if (flags === "wx" && path.startsWith(job.file)) { restoreOpens += 1; throw Object.assign(new Error("restore open EIO"), { code: "EIO" }); } const handle = await open(...args); return Object.assign(handle, { sync: async () => { if (armJobsSync && failJobsSync && flags === "r" && path === join(outbox.producerPath, "jobs")) { failJobsSync = false; armJobsSync = false; throw Object.assign(new Error("jobs fsync EIO"), { code: "EIO" }); } return Object.getPrototypeOf(handle).sync.call(handle); } }); } }; const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const first = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => Date.parse("2029-01-03T00:00:00.000Z"), maxClockSkewMs: 0, fs });
      expect(await first.deliver({})).toEqual({ delivered: 0, pending: 1, quarantined: 0 }); expect(processor.process).toHaveBeenCalledTimes(1); expect(restoreOpens).toBe(0); await expect(stat(job.file)).rejects.toMatchObject({ code: "ENOENT" }); const auditFile = join(outbox.producerPath, "quarantine", `${job.id}.delivered.json`); const audit = await readFile(auditFile, "utf8"); expect(audit).toContain(job.auditHash); expect(audit).not.toContain("already [token redacted]"); const restarted = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => Date.parse("2029-01-03T00:00:00.000Z"), maxClockSkewMs: 0, fs }); expect(await restarted.deliver({})).toEqual({ delivered: 0, pending: 0, quarantined: 0 }); expect(processor.process).toHaveBeenCalledTimes(1); await expect(stat(auditFile)).rejects.toMatchObject({ code: "ENOENT" }); const next = await outbox.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000102")], policy: current }); expect(await stat(next.file)).toBeDefined();
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("leaves the full accepted job when delivered-audit persistence fails", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-delivered-audit-fault-"));
    try { const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-delivered-audit-fault", producerUuid: producerId(103), machineId: "machine-delivered-audit-fault" }); const current = policy(); const job = await outbox.enqueue({ episodes: [episode(current)], policy: current }); let failAudit = true; const fs = { ...nodeFs, open: async (...args: Parameters<typeof open>) => { if (failAudit && String(args[0]).includes(".delivered.json.tmp-") && String(args[1]) === "wx") { failAudit = false; throw Object.assign(new Error("delivered audit EIO"), { code: "EIO" }); } return open(...args); } }; const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => Date.parse("2029-01-03T00:00:00.000Z"), maxClockSkewMs: 0, fs }); expect(await delivery.deliver({})).toEqual({ delivered: 0, pending: 1, quarantined: 0 }); expect(await stat(job.file)).toBeDefined(); expect(await delivery.deliver({})).toEqual({ delivered: 1, pending: 0, quarantined: 0 }); expect(processor.process).toHaveBeenCalledTimes(2); }
    finally { await rm(homeDir, { recursive: true, force: true }); }
  });


  it("promotes quarantine payloads destination-first across fsync faults in both APIs", async () => {
    for (const api of ["delivery", "store"] as const) for (const fault of ["destination", "source"] as const) {
      const homeDir = await mkdtemp(join(tmpdir(), `task5-quarantine-promotion-${api}-${fault}-`)); const operations: string[] = []; let faultOperations: string[] = []; let armed = false; let failed = false; let destinationLinked = false; let sourceRemoved = false; let source = ""; let destination = "";
      const fs = { ...nodeFs, link: async (existingPath: Parameters<typeof nodeFs.link>[0], newPath: Parameters<typeof nodeFs.link>[1]) => { if (armed && String(existingPath) === source && String(newPath) === destination) { operations.push("link-destination"); const result = await nodeFs.link(existingPath, newPath); destinationLinked = true; return result; } return nodeFs.link(existingPath, newPath); }, rm: async (path: Parameters<typeof nodeFs.rm>[0], options?: Parameters<typeof nodeFs.rm>[1]) => { if (armed && String(path) === source) { operations.push("rm-source"); const result = options === undefined ? await nodeFs.rm(path) : await nodeFs.rm(path, options); sourceRemoved = true; return result; } return options === undefined ? nodeFs.rm(path) : nodeFs.rm(path, options); }, open: async (...args: Parameters<typeof open>) => { const handle = await open(...args); const path = String(args[0]); const flags = String(args[1]); return Object.assign(handle, { sync: async () => { if (armed && flags === "r" && ((destinationLinked && path.endsWith("quarantine")) || (sourceRemoved && path.endsWith("jobs")))) { operations.push(path.endsWith("quarantine") ? "sync-destination" : "sync-source"); if (!failed && ((fault === "destination" && destinationLinked && path.endsWith("quarantine")) || (fault === "source" && sourceRemoved && path.endsWith("jobs")))) { failed = true; throw Object.assign(new Error(`${fault} fsync EIO`), { code: "EIO" }); } } return Object.getPrototypeOf(handle).sync.call(handle); } }); } };
      try {
        const index = 110 + (api === "store" ? 4 : 0) + (fault === "source" ? 1 : 0); const outbox = await createOutbox({ host: "prime", homeDir, nodeId: `node-promotion-${api}-${fault}`, producerUuid: producerId(index), machineId: `machine-promotion-${api}-${fault}`, fs: api === "store" ? fs : undefined }); const current = policy(); const job = await outbox.enqueue({ episodes: [episode(current)], policy: current }); source = job.file; destination = join(outbox.producerPath, "quarantine", `${job.id}.json`); armed = true;
        if (api === "store") { await expect(outbox.quarantine(job, "promotion-test")).rejects.toThrow(/fsync EIO/u); faultOperations = [...operations]; }
        else { const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "quarantined", category: "promotion-test" }) }; const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => Date.parse("2029-01-03T00:00:00.000Z"), maxClockSkewMs: 0, fs }); expect(await delivery.deliver({})).toEqual({ delivered: 0, pending: 1, quarantined: 0 }); faultOperations = [...operations]; expect(processor.process).toHaveBeenCalledTimes(1); await delivery.deliver({}); expect(processor.process).toHaveBeenCalledTimes(1); }
        expect(await stat(destination)).toBeDefined(); expect(await stat(join(outbox.producerPath, "quarantine", `${job.id}.reason.json`))).toBeDefined(); if (api === "store") await outbox.quarantine(job, "promotion-test"); await expect(stat(source)).rejects.toMatchObject({ code: "ENOENT" }); const linkIndex = faultOperations.indexOf("link-destination"); const destinationSync = faultOperations.indexOf("sync-destination"); const sourceRemove = faultOperations.indexOf("rm-source"); expect(linkIndex).toBeGreaterThanOrEqual(0); expect(destinationSync).toBeGreaterThan(linkIndex); if (fault === "source") { expect(sourceRemove).toBeGreaterThan(destinationSync); expect(faultOperations.indexOf("sync-source")).toBeGreaterThan(sourceRemove); } else expect(sourceRemove).toBe(-1);
      } finally { await rm(homeDir, { recursive: true, force: true }); }
    }
  });

  it("converges concurrent delivered and quarantine terminals in both interleavings and APIs", async () => {
    for (const api of ["delivery", "store"] as const) for (const ordering of ["quarantine-first", "delivered-first"] as const) {
      const homeDir = await mkdtemp(join(tmpdir(), `task5-terminal-race-${api}-${ordering}-`)); let source = ""; let destination = ""; let resolveQuarantineReached!: () => void; let resolveDeliveredAtRemove!: () => void; let resolveDeliveredRemoved!: () => void; const quarantineReached = new Promise<void>((resolveValue) => { resolveQuarantineReached = resolveValue; }); const deliveredAtRemove = new Promise<void>((resolveValue) => { resolveDeliveredAtRemove = resolveValue; }); const deliveredRemoved = new Promise<void>((resolveValue) => { resolveDeliveredRemoved = resolveValue; }); let processorCount = 0; let resolveBothProcessors!: () => void; const bothProcessors = new Promise<void>((resolveValue) => { resolveBothProcessors = resolveValue; });
      const quarantineFs = { ...nodeFs, link: async (existingPath: Parameters<typeof nodeFs.link>[0], newPath: Parameters<typeof nodeFs.link>[1]) => { if (String(existingPath) === source && String(newPath) === destination) { resolveQuarantineReached(); if (ordering === "delivered-first") await deliveredRemoved; const result = await nodeFs.link(existingPath, newPath); return result; } return nodeFs.link(existingPath, newPath); }, rm: async (path: Parameters<typeof nodeFs.rm>[0], options?: Parameters<typeof nodeFs.rm>[1]) => { if (String(path) === source && ordering === "quarantine-first") await deliveredAtRemove; return options === undefined ? nodeFs.rm(path) : nodeFs.rm(path, options); } };
      const deliveredFs = { ...nodeFs, rm: async (path: Parameters<typeof nodeFs.rm>[0], options?: Parameters<typeof nodeFs.rm>[1]) => { if (String(path) === source) { resolveDeliveredAtRemove(); if (ordering === "quarantine-first") await quarantineReached; const result = options === undefined ? await nodeFs.rm(path) : await nodeFs.rm(path, options); resolveDeliveredRemoved(); return result; } return options === undefined ? nodeFs.rm(path) : nodeFs.rm(path, options); } };
      try {
        const index = 120 + (api === "store" ? 4 : 0) + (ordering === "delivered-first" ? 1 : 0); const outbox = await createOutbox({ host: "prime", homeDir, nodeId: `node-terminal-race-${api}-${ordering}`, producerUuid: producerId(index), machineId: `machine-terminal-race-${api}-${ordering}`, maxJobs: 1, fs: api === "store" ? quarantineFs : undefined }); const current = policy(null); const job = await outbox.enqueue({ episodes: [episode(current)], policy: current }); source = job.file; destination = join(outbox.producerPath, "quarantine", `${job.id}.json`);
        const deliveredProcessor: OutboxJobProcessor = { process: vi.fn(async () => { if (api === "delivery") { processorCount += 1; if (processorCount === 2) resolveBothProcessors(); await bothProcessors; } else await quarantineReached; return { status: "delivered" }; }) }; const delivered = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor: deliveredProcessor, now: () => Date.parse("2029-01-03T00:00:00.000Z"), maxClockSkewMs: 0, fs: deliveredFs });
        let quarantinePromise: Promise<unknown>; if (api === "delivery") { const quarantineProcessor: OutboxJobProcessor = { process: vi.fn(async () => { processorCount += 1; if (processorCount === 2) resolveBothProcessors(); await bothProcessors; return { status: "quarantined", category: "race" }; }) }; const quarantineDelivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor: quarantineProcessor, now: () => Date.parse("2029-01-03T00:00:00.000Z"), maxClockSkewMs: 0, fs: quarantineFs }); quarantinePromise = quarantineDelivery.deliver({}); } else quarantinePromise = outbox.quarantine(job, "race"); const settled = await Promise.allSettled([delivered.deliver({}), quarantinePromise]); expect(settled).toHaveLength(2);
        await expect(stat(source)).rejects.toMatchObject({ code: "ENOENT" }); await expect(stat(destination)).rejects.toMatchObject({ code: "ENOENT" }); await expect(stat(join(outbox.producerPath, "quarantine", `${job.id}.reason.json`))).rejects.toMatchObject({ code: "ENOENT" }); await expect(stat(join(outbox.producerPath, "quarantine", `${job.id}.delivered.json`))).rejects.toMatchObject({ code: "ENOENT" }); const status = await outbox.outboxStatus(); expect(status.jobs).toBe(0); expect(status.captureAllowed).toBe(true); const next = await outbox.enqueue({ episodes: [episode(current, `00000000-0000-5000-8000-${index.toString(16).padStart(12, "0")}`)], policy: current }); expect(await stat(next.file)).toBeDefined();
      } finally { resolveQuarantineReached?.(); resolveDeliveredAtRemove?.(); resolveDeliveredRemoved?.(); resolveBothProcessors?.(); await rm(homeDir, { recursive: true, force: true }); }
    }
  }, 20_000);


  it("converges expiry against concurrent quarantine hardlinks in both interleavings and APIs", async () => {
    for (const api of ["delivery", "store"] as const) for (const ordering of ["quarantine-first", "expiry-first"] as const) {
      const homeDir = await mkdtemp(join(tmpdir(), `task5-expiry-race-${api}-${ordering}-`)); let source = ""; let destination = ""; let resolveQuarantineReached!: () => void; let resolveExpiryAtRemove!: () => void; let resolveExpiryRemoved!: () => void; const quarantineReached = new Promise<void>((resolveValue) => { resolveQuarantineReached = resolveValue; }); const expiryAtRemove = new Promise<void>((resolveValue) => { resolveExpiryAtRemove = resolveValue; }); const expiryRemoved = new Promise<void>((resolveValue) => { resolveExpiryRemoved = resolveValue; });
      const quarantineFs = { ...nodeFs, link: async (existingPath: Parameters<typeof nodeFs.link>[0], newPath: Parameters<typeof nodeFs.link>[1]) => { if (String(existingPath) === source && String(newPath) === destination) { resolveQuarantineReached(); if (ordering === "expiry-first") await expiryRemoved; return nodeFs.link(existingPath, newPath); } return nodeFs.link(existingPath, newPath); }, rm: async (path: Parameters<typeof nodeFs.rm>[0], options?: Parameters<typeof nodeFs.rm>[1]) => { if (String(path) === source && ordering === "quarantine-first") await expiryAtRemove; return options === undefined ? nodeFs.rm(path) : nodeFs.rm(path, options); } };
      const expiryFs = { ...nodeFs, rm: async (path: Parameters<typeof nodeFs.rm>[0], options?: Parameters<typeof nodeFs.rm>[1]) => { if (String(path) === source) { resolveExpiryAtRemove(); await quarantineReached; const result = options === undefined ? await nodeFs.rm(path) : await nodeFs.rm(path, options); resolveExpiryRemoved(); return result; } return options === undefined ? nodeFs.rm(path) : nodeFs.rm(path, options); } };
      try {
        const index = 130 + (api === "store" ? 4 : 0) + (ordering === "expiry-first" ? 1 : 0); const current = policy("2029-01-02T00:00:00.000Z"); const outbox = await createOutbox({ host: "prime", homeDir, nodeId: `node-expiry-race-${api}-${ordering}`, producerUuid: producerId(index), machineId: `machine-expiry-race-${api}-${ordering}`, maxJobs: 1, fs: api === "store" ? quarantineFs : undefined }); const job = await outbox.enqueue({ episodes: [episode(current)], policy: current }); source = job.file; destination = join(outbox.producerPath, "quarantine", `${job.id}.json`); const expiryProcessor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const expiryDelivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor: expiryProcessor, now: () => Date.parse("2029-01-03T00:00:00.000Z"), maxClockSkewMs: 0, fs: expiryFs }); let quarantinePromise: Promise<unknown>;
        if (api === "delivery") { const quarantineProcessor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "quarantined", category: "expiry-race" }) }; const quarantineDelivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor: quarantineProcessor, now: () => Date.parse("2029-01-01T00:00:00.000Z"), maxClockSkewMs: 0, fs: quarantineFs }); quarantinePromise = quarantineDelivery.deliver({}); } else quarantinePromise = outbox.quarantine(job, "expiry-race"); await Promise.allSettled([expiryDelivery.deliver({}), quarantinePromise]); expect(expiryProcessor.process).not.toHaveBeenCalled(); await expect(stat(source)).rejects.toMatchObject({ code: "ENOENT" }); await expect(stat(destination)).rejects.toMatchObject({ code: "ENOENT" }); await expect(stat(join(outbox.producerPath, "quarantine", `${job.id}.reason.json`))).rejects.toMatchObject({ code: "ENOENT" }); expect(await readFile(join(outbox.producerPath, "quarantine", `${job.id}.expired.json`), "utf8")).toContain(job.auditHash); const status = await outbox.outboxStatus(); expect(status.jobs).toBe(0); expect(status.captureAllowed).toBe(true); const next = await outbox.enqueue({ episodes: [episode(current, `00000000-0000-5000-8000-${index.toString(16).padStart(12, "0")}`)], policy: current }); expect(await stat(next.file)).toBeDefined();
      } finally { resolveQuarantineReached?.(); resolveExpiryAtRemove?.(); resolveExpiryRemoved?.(); await rm(homeDir, { recursive: true, force: true }); }
    }
  }, 20_000);

  it("accepts delivered proof from a quarantine copy completed before delivered readback", async () => {
    for (const api of ["delivery", "store"] as const) {
      const homeDir = await mkdtemp(join(tmpdir(), `task5-fast-quarantine-${api}-`)); let releaseDelivered!: () => void; let deliveredEntered!: () => void; const release = new Promise<void>((resolveValue) => { releaseDelivered = resolveValue; }); const entered = new Promise<void>((resolveValue) => { deliveredEntered = resolveValue; });
      try {
        const index = api === "delivery" ? 140 : 141; const current = policy(null); const outbox = await createOutbox({ host: "prime", homeDir, nodeId: `node-fast-quarantine-${api}`, producerUuid: producerId(index), machineId: `machine-fast-quarantine-${api}`, maxJobs: 1 }); const job = await outbox.enqueue({ episodes: [episode(current)], policy: current }); const deliveredProcessor: OutboxJobProcessor = { process: vi.fn(async () => { deliveredEntered(); await release; return { status: "delivered" }; }) }; const delivered = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor: deliveredProcessor, now: () => Date.parse("2029-01-03T00:00:00.000Z"), maxClockSkewMs: 0 }); const delivering = delivered.deliver({}); await entered;
        if (api === "delivery") { const quarantineProcessor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "quarantined", category: "fast" }) }; const quarantineDelivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor: quarantineProcessor, now: () => Date.parse("2029-01-03T00:00:00.000Z"), maxClockSkewMs: 0 }); expect(await quarantineDelivery.deliver({})).toEqual({ delivered: 0, pending: 0, quarantined: 1 }); } else await outbox.quarantine(job, "fast"); await expect(stat(job.file)).rejects.toMatchObject({ code: "ENOENT" }); expect(await stat(join(outbox.producerPath, "quarantine", `${job.id}.json`))).toBeDefined(); releaseDelivered(); expect(await delivering).toEqual({ delivered: 1, pending: 0, quarantined: 0 }); expect(deliveredProcessor.process).toHaveBeenCalledTimes(1); await expect(stat(join(outbox.producerPath, "quarantine", `${job.id}.json`))).rejects.toMatchObject({ code: "ENOENT" }); await expect(stat(join(outbox.producerPath, "quarantine", `${job.id}.reason.json`))).rejects.toMatchObject({ code: "ENOENT" }); await expect(stat(join(outbox.producerPath, "quarantine", `${job.id}.delivered.json`))).rejects.toMatchObject({ code: "ENOENT" }); const status = await outbox.outboxStatus(); expect(status.jobs).toBe(0); expect(status.captureAllowed).toBe(true); const next = await outbox.enqueue({ episodes: [episode(current, `00000000-0000-5000-8000-${index.toString(16).padStart(12, "0")}`)], policy: current }); expect(await stat(next.file)).toBeDefined();
      } finally { releaseDelivered?.(); await rm(homeDir, { recursive: true, force: true }); }
    }
  });


  it("removes late pending controls across expiry and quarantine terminals", async () => {
    for (const terminal of ["expired", "processor-quarantine", "store-quarantine"] as const) {
      const homeDir = await mkdtemp(join(tmpdir(), `task5-late-pending-${terminal}-`)); let controlWritten!: () => void; let terminalDone!: () => void; const written = new Promise<void>((resolveValue) => { controlWritten = resolveValue; }); const done = new Promise<void>((resolveValue) => { terminalDone = resolveValue; });
      try {
        const index = 150 + ["expired", "processor-quarantine", "store-quarantine"].indexOf(terminal); const current = policy("2029-01-02T00:00:00.000Z"); const outbox = await createOutbox({ host: "prime", homeDir, nodeId: `node-late-pending-${terminal}`, producerUuid: producerId(index), machineId: `machine-late-pending-${terminal}` }); const job = await outbox.enqueue({ episodes: [episode(current)], policy: current }); const controlFile = join(outbox.producerPath, "control", `${job.id}.json`); let gated = false; const pendingFs = { ...nodeFs, rename: async (from: Parameters<typeof nodeFs.rename>[0], to: Parameters<typeof nodeFs.rename>[1]) => { const result = await nodeFs.rename(from, to); if (!gated && String(to) === controlFile) { gated = true; controlWritten(); await done; } return result; } }; const pendingProcessor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "pending", category: "late" }) }; const pendingDelivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor: pendingProcessor, now: () => Date.parse("2029-01-01T00:00:00.000Z"), maxClockSkewMs: 0, fs: pendingFs }); const pending = pendingDelivery.deliver({}); await written;
        if (terminal === "expired") { const expiryProcessor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const expiry = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor: expiryProcessor, now: () => Date.parse("2029-01-03T00:00:00.000Z"), maxClockSkewMs: 0 }); await expiry.deliver({}); expect(expiryProcessor.process).not.toHaveBeenCalled(); }
        else if (terminal === "processor-quarantine") { const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "quarantined", category: "late-terminal" }) }; const quarantine = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => Date.parse("2029-01-01T12:00:00.000Z"), maxClockSkewMs: 0 }); await quarantine.deliver({}); }
        else await outbox.quarantine(job, "late-terminal"); terminalDone(); await pending; await expect(stat(controlFile)).rejects.toMatchObject({ code: "ENOENT" }); await expect(stat(job.file)).rejects.toMatchObject({ code: "ENOENT" }); if (terminal === "expired") expect(await readFile(join(outbox.producerPath, "quarantine", `${job.id}.expired.json`), "utf8")).toContain(job.auditHash); else expect(await stat(join(outbox.producerPath, "quarantine", `${job.id}.json`))).toBeDefined();
      } finally { terminalDone?.(); await rm(homeDir, { recursive: true, force: true }); }
    }
  });

  it("rejects relative configured agent roots before resolving against the working directory", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-relative-root-")); const relative = `task5-relative-${Date.now()}-${Math.random().toString(16).slice(2)}`; const cwdPath = join(process.cwd(), relative);
    try { await expect(createOutbox({ host: "prime", homeDir, env: { PRIME_AGENT_CODING_AGENT_DIR: relative }, nodeId: "node-relative-root", producerUuid: producerId(160) })).rejects.toThrow(/absolute/u); await expect(stat(cwdPath)).rejects.toMatchObject({ code: "ENOENT" }); }
    finally { await rm(homeDir, { recursive: true, force: true }); await rm(cwdPath, { recursive: true, force: true }); }
  });

  it("publishes salt, node, and producer identities without partial final files", async () => {
    for (const target of ["installation-salt", "node.json", "producer.json"] as const) {
      const homeDir = await mkdtemp(join(tmpdir(), `task5-exclusive-${target.replace(".json", "")}-`)); const nodeId = `node-exclusive-${target.replace(".json", "")}`; const producerUuid = producerId(161 + ["installation-salt", "node.json", "producer.json"].indexOf(target)); let failed = false;
      const fs = { ...nodeFs, open: async (...args: Parameters<typeof open>) => { const handle = await open(...args); const path = String(args[0]); const isTarget = path.includes(`${target}.create-`); if (!isTarget) return handle; const originalWrite = handle.writeFile.bind(handle); return Object.assign(handle, { writeFile: async () => originalWrite("x", "utf8"), sync: async () => { if (!failed) { failed = true; throw Object.assign(new Error(`${target} sync EIO`), { code: "EIO" }); } return Object.getPrototypeOf(handle).sync.call(handle); } }); } };
      try {
        await expect(createOutbox({ host: "prime", homeDir, nodeId, producerUuid, machineId: `machine-${target}`, fs })).rejects.toThrow(/sync EIO/u); const root = join(homeDir, ".prime", "agent", "pi-qdrant-memory", "outbox"); const finalFile = target === "installation-salt" ? join(root, target) : target === "node.json" ? join(root, nodeId, target) : join(root, nodeId, producerUuid, target); await expect(stat(finalFile)).rejects.toMatchObject({ code: "ENOENT" }); const artifacts = await readdir(root, { recursive: true }); expect(artifacts.some((name) => String(name).includes(".create-"))).toBe(false); const restarted = await createOutbox({ host: "prime", homeDir, nodeId, producerUuid, machineId: `machine-${target}` }); expect(await stat(join(restarted.producerPath, "producer.json"))).toBeDefined(); expect((await readdir(restarted.root, { recursive: true })).some((name) => String(name).includes(".create-"))).toBe(false);
      } finally { await rm(homeDir, { recursive: true, force: true }); }
    }
  });


  it("bases retry backoff on fresh processor completion time", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-completion-backoff-")); let clock = Date.parse("2029-01-01T00:00:00.000Z");
    try { const current = policy("2029-01-02T00:00:00.000Z"); const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-completion-backoff", producerUuid: producerId(170), machineId: "machine-completion-backoff" }); const job = await outbox.enqueue({ episodes: [episode(current)], policy: current }); const processor: OutboxJobProcessor = { process: vi.fn(async () => { clock += 10_000; return { status: "pending" }; }) }; const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => clock, maxClockSkewMs: 0, retryBaseMs: 100, retryMaxMs: 1_000 }); expect(await delivery.deliver({})).toEqual({ delivered: 0, pending: 1, quarantined: 0 }); const control = JSON.parse(await readFile(join(outbox.producerPath, "control", `${job.id}.json`), "utf8")); expect(control.nextAttemptAt).toBeGreaterThan(clock); expect(await delivery.deliver({})).toEqual({ delivered: 0, pending: 1, quarantined: 0 }); expect(processor.process).toHaveBeenCalledTimes(1); }
    finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("applies expiry at completion for slow pending, timeout, and quarantine but permits delivered", async () => {
    for (const outcome of ["pending", "timeout", "quarantined", "delivered"] as const) {
      const homeDir = await mkdtemp(join(tmpdir(), `task5-completion-expiry-${outcome}-`)); let clock = Date.parse("2029-01-01T00:00:00.000Z");
      try { const deadline = new Date(clock + 5_000).toISOString(); const current = policy(deadline); const index = 171 + ["pending", "timeout", "quarantined", "delivered"].indexOf(outcome); const outbox = await createOutbox({ host: "prime", homeDir, nodeId: `node-completion-expiry-${outcome}`, producerUuid: producerId(index), machineId: `machine-completion-expiry-${outcome}` }); const job = await outbox.enqueue({ episodes: [episode(current)], policy: current }); const processor: OutboxJobProcessor = { process: vi.fn(async () => { clock += 10_000; if (outcome === "timeout") return new Promise<never>(() => undefined); return outcome === "quarantined" ? { status: "quarantined", category: "slow" } : { status: outcome }; }) }; const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => clock, maxClockSkewMs: 0, attemptTimeoutMs: 5 }); const result = await delivery.deliver({}); if (outcome === "delivered") { expect(result).toEqual({ delivered: 1, pending: 0, quarantined: 0 }); await expect(stat(join(outbox.producerPath, "quarantine", `${job.id}.expired.json`))).rejects.toMatchObject({ code: "ENOENT" }); } else { expect(result).toEqual({ delivered: 0, pending: 0, quarantined: 1 }); expect(await readFile(join(outbox.producerPath, "quarantine", `${job.id}.expired.json`), "utf8")).toContain(job.auditHash); await expect(stat(join(outbox.producerPath, "quarantine", `${job.id}.json`))).rejects.toMatchObject({ code: "ENOENT" }); await expect(stat(join(outbox.producerPath, "control", `${job.id}.json`))).rejects.toMatchObject({ code: "ENOENT" }); } await expect(stat(job.file)).rejects.toMatchObject({ code: "ENOENT" }); expect(processor.process).toHaveBeenCalledTimes(1); }
      finally { await rm(homeDir, { recursive: true, force: true }); }
    }
  });

  it("rejects cross-host adoption even when Pi and Prime deliberately share an outbox root", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-cross-host-adopt-")); const shared = join(homeDir, "shared-agent");
    try { const piOutbox = await createOutbox({ host: "pi", homeDir, env: { PI_CODING_AGENT_DIR: shared }, nodeId: "node-cross-host-pi", producerUuid: producerId(180), machineId: "machine-cross-host-pi" }); const primeOutbox = await createOutbox({ host: "prime", homeDir, env: { PRIME_AGENT_CODING_AGENT_DIR: shared }, nodeId: "node-cross-host-prime", producerUuid: producerId(181), machineId: "machine-cross-host-prime" }); const piPolicyPending = { ...policy(), ownerHost: "pi" as const, id: "pending" }; const piPolicy = { ...piPolicyPending, id: processingPolicyHash(piPolicyPending) }; const piEpisodePending = { ...episode(piPolicy), ownerHost: "pi" as const, host: "pi" as const, contentHash: "pending" }; const piEpisode = { ...piEpisodePending, contentHash: canonicalRecordHash(piEpisodePending) }; await piOutbox.enqueue({ episodes: [piEpisode], policy: piPolicy }); await primeOutbox.enqueue({ episodes: [episode(policy(), "00000000-0000-5000-8000-000000000181")], policy: policy() }); await piOutbox.closeProducer(); await primeOutbox.closeProducer(); const primeProcessor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const primeDelivery = createOutboxDelivery({ outboxRoot: primeOutbox.root, producerPath: primeOutbox.producerPath, processor: primeProcessor, now: () => Date.parse("2029-01-03T00:00:00.000Z"), maxClockSkewMs: 0 }); await expect(primeDelivery.adopt(piOutbox.producerPath)).rejects.toThrow(/owner host/u); const piProcessor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const piDelivery = createOutboxDelivery({ outboxRoot: piOutbox.root, producerPath: piOutbox.producerPath, processor: piProcessor, now: () => Date.parse("2029-01-03T00:00:00.000Z"), maxClockSkewMs: 0 }); await expect(piDelivery.adopt(primeOutbox.producerPath)).rejects.toThrow(/owner host/u); expect(primeProcessor.process).not.toHaveBeenCalled(); expect(piProcessor.process).not.toHaveBeenCalled(); }
    finally { await rm(homeDir, { recursive: true, force: true }); }
  });


  it("fails closed on every invalid delivery clock value before processor or file mutation", async () => {
    for (const invalid of [Number.NaN, Number.POSITIVE_INFINITY, -1, Date.parse("2101-01-01T00:00:00.000Z")]) {
      const homeDir = await mkdtemp(join(tmpdir(), "task5-invalid-delivery-clock-"));
      try { const outbox = await createOutbox({ host: "prime", homeDir, nodeId: `node-clock-${190 + [Number.NaN, Number.POSITIVE_INFINITY, -1, Date.parse("2101-01-01T00:00:00.000Z")].findIndex((value) => Object.is(value, invalid))}`, producerUuid: producerId(190 + [Number.NaN, Number.POSITIVE_INFINITY, -1, Date.parse("2101-01-01T00:00:00.000Z")].findIndex((value) => Object.is(value, invalid))), machineId: "machine-invalid-clock" }); const current = policy(); const job = await outbox.enqueue({ episodes: [episode(current)], policy: current }); const before = await readFile(job.file, "utf8"); const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => invalid, maxClockSkewMs: 0 }); await expect(delivery.deliver({})).rejects.toThrow(/clock/u); await expect(delivery.adopt(outbox.producerPath)).rejects.toThrow(/clock/u); expect(processor.process).not.toHaveBeenCalled(); expect(await readFile(job.file, "utf8")).toBe(before); expect(activeReservationNames(await readdir(join(outbox.producerPath, "control")))).toEqual([]); expect(activeReservationNames(await readdir(join(outbox.producerPath, "quarantine")))).toEqual([]); }
      finally { await rm(homeDir, { recursive: true, force: true }); }
    }
  });


  it("validates the store clock before setup and every public mutation", async () => {
    const invalids = [Number.NaN, Number.POSITIVE_INFINITY, -1, Date.parse("2101-01-01T00:00:00.000Z")];
    for (let index = 0; index < invalids.length; index += 1) { const homeDir = await mkdtemp(join(tmpdir(), "task5-store-setup-clock-")); try { await expect(createOutbox({ host: "prime", homeDir, nodeId: `node-setup-clock-${index}`, producerUuid: producerId(200 + index), machineId: "machine-setup-clock", now: () => invalids[index]! })).rejects.toThrow(/clock/u); await expect(stat(join(homeDir, ".prime-agent", "pi-qdrant-memory-v2"))).rejects.toMatchObject({ code: "ENOENT" }); } finally { await rm(homeDir, { recursive: true, force: true }); } }
    for (let index = 0; index < invalids.length; index += 1) { const homeDir = await mkdtemp(join(tmpdir(), "task5-store-api-clock-")); let now = Date.parse("2029-01-01T00:00:00.000Z");
      try { const outbox = await createOutbox({ host: "prime", homeDir, nodeId: `node-store-api-clock-${index}`, producerUuid: producerId(204 + index), machineId: "machine-store-api-clock", now: () => now }); const current = policy(); const job = await outbox.enqueue({ episodes: [episode(current, `00000000-0000-5000-8000-${String(500 + index).padStart(12, "0")}`)], policy: current }); const stateFile = join(outbox.producerPath, "state.json"); const accepted = await readFile(job.file, "utf8"); const state = await readFile(stateFile, "utf8"); now = invalids[index]!; await expect(outbox.enqueue({ episodes: [episode(current, `00000000-0000-5000-8000-${String(600 + index).padStart(12, "0")}`)], policy: current })).rejects.toThrow(/clock/u); await expect(outbox.listPending()).rejects.toThrow(/clock/u); await expect(outbox.quarantine(job, "invalid_clock")).rejects.toThrow(/clock/u); await expect(outbox.heartbeat()).rejects.toThrow(/clock/u); await expect(outbox.closeProducer()).rejects.toThrow(/clock/u); await expect(outbox.outboxStatus()).rejects.toThrow(/clock/u); expect(await readFile(job.file, "utf8")).toBe(accepted); expect(await readFile(stateFile, "utf8")).toBe(state); expect(activeReservationNames(await readdir(join(outbox.producerPath, "control")))).toEqual([]); expect(activeReservationNames(await readdir(join(outbox.producerPath, "quarantine")))).toEqual([]); now = Date.parse("2029-01-01T00:00:01.000Z"); const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => now, maxClockSkewMs: 0 }); expect(await delivery.deliver({})).toEqual({ delivered: 1, pending: 0, quarantined: 0 }); expect(processor.process).toHaveBeenCalledTimes(1); }
      finally { await rm(homeDir, { recursive: true, force: true }); }
    }
  });

  it("spends maxJobs across quarantine expiry candidates and makes fair progress", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-total-budget-quarantine-")); let now = Date.parse("2029-01-01T00:00:00.000Z");
    try { const current = policy("2029-01-01T00:00:01.000Z"); const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-total-budget", producerUuid: producerId(210), machineId: "machine-total-budget", now: () => now }); const jobs = []; for (let index = 0; index < 3; index += 1) { const job = await outbox.enqueue({ episodes: [episode(current, `00000000-0000-5000-8000-${String(210 + index).padStart(12, "0")}`)], policy: current }); jobs.push(job); await outbox.quarantine(job, "accepted"); } now += 10_000; const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => now, maxClockSkewMs: 0 }); for (let remaining = 2; remaining >= 0; remaining -= 1) { expect(await delivery.deliver({ maxJobs: 1 })).toEqual({ delivered: 0, pending: 0, quarantined: 1 }); const full = (await readdir(join(outbox.producerPath, "quarantine"))).filter((name) => /^[-0-9a-f]{36}\.json$/u.test(name)); expect(full).toHaveLength(remaining); } expect(processor.process).not.toHaveBeenCalled(); }
    finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("does not sweep an aborted shutdown and bounds payload IO independently of backlog", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-total-budget-io-"));
    try { const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-total-budget-io", producerUuid: producerId(220), machineId: "machine-total-budget-io" }); const current = policy(); for (let index = 0; index < 30; index += 1) await outbox.enqueue({ episodes: [episode(current, `00000000-0000-5000-8000-${String(300 + index).padStart(12, "0")}`)], policy: current }); const readSpy = vi.fn(nodeFs.readFile); const lstatSpy = vi.fn(nodeFs.lstat); const renameSpy = vi.fn(nodeFs.rename); const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "pending" }) }; const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => Date.parse("2029-01-03T00:00:00.000Z"), maxClockSkewMs: 0, fs: { ...nodeFs, readFile: readSpy, lstat: lstatSpy, rename: renameSpy } }); const controller = new AbortController(); controller.abort(); expect(await delivery.shutdown({ signal: controller.signal, maxJobs: 1 })).toEqual({ delivered: 0, pending: 0, quarantined: 0 }); expect(readSpy).not.toHaveBeenCalled(); expect(lstatSpy).not.toHaveBeenCalled(); expect(renameSpy).not.toHaveBeenCalled(); expect(await delivery.deliver({ maxJobs: 1 })).toEqual({ delivered: 0, pending: 1, quarantined: 0 }); expect(processor.process).toHaveBeenCalledTimes(1); expect(readSpy.mock.calls.filter((call) => String(call[0]).includes(`${join(outbox.producerPath, "jobs")}/`)).length).toBeLessThanOrEqual(3); expect(lstatSpy.mock.calls.filter((call) => String(call[0]).includes(`${join(outbox.producerPath, "jobs")}/`)).length).toBeLessThanOrEqual(6); expect(renameSpy.mock.calls.length).toBeLessThan(3); }
    finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("clamps retry controls at the bounded clock ceiling without a retry storm", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-retry-clock-ceiling-")); let now = Date.parse("2100-12-31T23:59:59.989Z");
    try { const current = policy(null); const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-retry-clock-ceiling", producerUuid: producerId(230), machineId: "machine-retry-clock-ceiling", now: () => now }); const job = await outbox.enqueue({ episodes: [episode(current)], policy: current }); const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "pending" }) }; const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => now, maxClockSkewMs: 0, retryBaseMs: 100, retryMaxMs: 1_000 }); expect(await delivery.deliver({})).toEqual({ delivered: 0, pending: 1, quarantined: 0 }); const controlFile = join(outbox.producerPath, "control", `${job.id}.json`); expect(JSON.parse(await readFile(controlFile, "utf8")).nextAttemptAt).toBe(Date.parse("2100-12-31T23:59:59.999Z")); expect(await delivery.deliver({})).toEqual({ delivered: 0, pending: 1, quarantined: 0 }); now = Date.parse("2100-12-31T23:59:59.999Z"); expect(await delivery.deliver({})).toEqual({ delivered: 0, pending: 1, quarantined: 0 }); expect(processor.process).toHaveBeenCalledTimes(1); expect(JSON.parse(await readFile(controlFile, "utf8")).nextAttemptAt).toBe(now); }
    finally { await rm(homeDir, { recursive: true, force: true }); }
  });


  it("atomically fences every precommit publication window and recovers without payload-temp retention", async () => {
    for (const stage of ["before-temp", "during-write", "before-rename", "after-rename"] as const) {
      const homeDir = await mkdtemp(join(tmpdir(), `task5-fence-${stage}-`)); let reached!: () => void; let resume!: () => void; const paused = new Promise<void>((resolvePaused) => { reached = resolvePaused; }); const gate = new Promise<void>((resolveResume) => { resume = resolveResume; }); let pause = true;
      const fsA = { ...nodeFs,
        open: async (...args: Parameters<typeof open>) => { const path = String(args[0]); if (pause && stage === "before-temp" && path.includes("/jobs/") && /\.json\.tmp-[0-9]+-[a-f0-9]{32}$/u.test(path)) { pause = false; reached(); await gate; } const handle = await open(...args); if (pause && stage === "during-write" && path.includes("/jobs/") && /\.json\.tmp-[0-9]+-[a-f0-9]{32}$/u.test(path)) { pause = false; const write = handle.writeFile.bind(handle); return Object.assign(handle, { writeFile: async (...writeArgs: Parameters<typeof handle.writeFile>) => { const body = String(writeArgs[0]); await write(body.slice(0, Math.max(1, Math.floor(body.length / 2))), "utf8"); reached(); await gate; } }); } return handle; },
        rename: async (from: Parameters<typeof nodeFs.rename>[0], to: Parameters<typeof nodeFs.rename>[1]) => { const destination = String(to); const isJob = destination.split(/[\/]/u).at(-2) === "jobs" && destination.endsWith(".json"); if (pause && stage === "before-rename" && isJob) { pause = false; reached(); await gate; return nodeFs.rename(from, to); } if (pause && stage === "after-rename" && isJob) { await nodeFs.rename(from, to); pause = false; reached(); await gate; return; } return nodeFs.rename(from, to); },
      };
      try { let storeNow = Date.parse("2029-01-01T00:00:00.000Z"); const deadline = stage === "during-write" ? new Date(storeNow + 1_000).toISOString() : "2030-01-01T00:00:00.000Z"; const current = policy(deadline); const producerA = await createOutbox({ host: "prime", homeDir, nodeId: `node-fence-${stage}`, producerUuid: producerId(240 + ["before-temp", "during-write", "before-rename", "after-rename"].indexOf(stage)), machineId: `machine-fence-${stage}`, now: () => storeNow, fs: fsA }); const producerB = await createOutbox({ host: "prime", homeDir, nodeId: `node-fence-${stage}`, producerUuid: producerId(250 + ["before-temp", "during-write", "before-rename", "after-rename"].indexOf(stage)), machineId: `machine-fence-${stage}`, now: () => storeNow }); const accepting = producerA.enqueue({ episodes: [episode(current, `00000000-0000-5000-8000-${String(700 + ["before-temp", "during-write", "before-rename", "after-rename"].indexOf(stage)).padStart(12, "0")}`)], policy: current }); const outcome = accepting.then(() => ({ accepted: true }), (error: unknown) => ({ accepted: false, error })); await Promise.race([paused, outcome.then((value) => { throw value.error ?? new Error("enqueue completed before pause"); })]); const lock = await readAdmissionLock(join(producerA.root, "reservations")); expect(lock.policyId).toBe(current.id); expect(lock.deadline).toBe(deadline); const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const deliveryNow = stage === "during-write" ? storeNow + 2_000 : Date.parse("2029-01-03T00:00:00.000Z"); const delivery = createOutboxDelivery({ outboxRoot: producerA.root, producerPath: stage === "during-write" ? producerA.producerPath : producerB.producerPath, processor, now: () => deliveryNow, maxClockSkewMs: 0, heartbeatTimeoutMs: 100 }); if (stage === "during-write") await delivery.deliver({ maxJobs: 1 }); else await delivery.adopt(producerA.producerPath); const fenced = (await readdir(producerA.producerPath)).filter((name) => name.startsWith("jobs.fenced-")); expect(fenced).toHaveLength(1); expect((await readdir(join(producerA.producerPath, fenced[0]!))).filter((name) => /\.json\.tmp-/u.test(name))).toEqual([]); expect(activeReservationNames(await readdir(join(producerA.root, "reservations")))).toEqual([]); resume(); expect((await outcome).accepted).toBe(false); await expect(stat(join(producerA.producerPath, "jobs"))).rejects.toMatchObject({ code: "ENOENT" }); await expect(producerA.heartbeat()).rejects.toThrow(/fenced/u); const deliveryResult = await delivery.deliver({}); if (stage === "before-rename" || stage === "after-rename") expect(deliveryResult.delivered).toBe(1); else expect(processor.process).not.toHaveBeenCalled(); const audits = await Promise.all((await readdir(join(producerA.producerPath, "quarantine"))).map((name) => readFile(join(producerA.producerPath, "quarantine", name), "utf8"))); if (stage === "during-write") { expect(audits.join("\n")).toContain("partial_precommit"); expect(audits.join("\n")).not.toContain("destinationIds"); expect(audits.join("\n")).not.toContain("already [token redacted]"); } storeNow = deliveryNow; const next = await producerB.enqueue({ episodes: [episode(current, `00000000-0000-5000-8000-${String(800 + ["before-temp", "during-write", "before-rename", "after-rename"].indexOf(stage)).padStart(12, "0")}`)], policy: current }); expect(await stat(next.file)).toBeDefined(); }
      finally { resume?.(); await rm(homeDir, { recursive: true, force: true }); }
    }
  }, 20_000);

  it("recovers fence directory-sync and manifest-publication crash windows", async () => {
    for (const fault of ["directory-sync", "manifest-publication"] as const) { const homeDir = await mkdtemp(join(tmpdir(), `task5-fence-crash-${fault}-`)); let armed = false; let failed = false;
      try { const outbox = await createOutbox({ host: "prime", homeDir, nodeId: `node-fence-crash-${fault}`, producerUuid: producerId(260 + ["directory-sync", "manifest-publication"].indexOf(fault)), machineId: `machine-fence-crash-${fault}`, now: () => 1_000 }); await outbox.closeProducer(); const fs = { ...nodeFs, rename: async (from: Parameters<typeof nodeFs.rename>[0], to: Parameters<typeof nodeFs.rename>[1]) => { const destination = String(to); if (String(from).endsWith("/jobs") && destination.includes("/jobs.fenced-")) armed = true; if (!failed && fault === "manifest-publication" && destination.endsWith("/fence.json")) { await nodeFs.rename(from, to); failed = true; throw Object.assign(new Error("manifest publication crash"), { code: "EIO" }); } return nodeFs.rename(from, to); }, open: async (...args: Parameters<typeof open>) => { const handle = await open(...args); const path = String(args[0]); const sync = handle.sync.bind(handle); return Object.assign(handle, { sync: async () => { if (!failed && fault === "directory-sync" && armed && path === outbox.producerPath) { failed = true; throw Object.assign(new Error("fence directory sync crash"), { code: "EIO" }); } return sync(); } }); } }; const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const crashing = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => 100_000, maxClockSkewMs: 0, heartbeatTimeoutMs: 100, fs }); await expect(crashing.adopt(outbox.producerPath)).rejects.toThrow(/crash/u); const recovered = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => 100_000, maxClockSkewMs: 0, heartbeatTimeoutMs: 100 }); await expect(recovered.adopt(outbox.producerPath)).resolves.toBeUndefined(); expect((await readdir(outbox.producerPath)).filter((name) => name.startsWith("jobs.fenced-"))).toHaveLength(1); const manifest = JSON.parse(await readFile(join(outbox.producerPath, "fence.json"), "utf8")); expect(manifest.kind).toBe("producer_jobs_fence"); expect(manifest.jobsDir).toMatch(/^jobs\.fenced-[a-f0-9]{32}$/u); }
      finally { await rm(homeDir, { recursive: true, force: true }); }
    }
  });


  it("recovers an exact fsynced precommit temp after the writer process is killed", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-killed-precommit-")); const script = join(homeDir, "writer.mjs"); let child: ReturnType<typeof spawn> | undefined;
    try { const current = policy("2030-01-01T00:00:00.000Z"); const item = episode(current, "00000000-0000-5000-8000-000000000900"); const inputFile = join(homeDir, "input.json"); await nodeFs.writeFile(inputFile, JSON.stringify({ current, item }), { mode: 0o600 }); await nodeFs.writeFile(script, `import * as nodeFs from "node:fs/promises";
import { createOutbox } from ${JSON.stringify(new URL("../../dist/outbox/store.js", import.meta.url).href)};
const [homeDir,inputFile] = process.argv.slice(2);
const { current, item } = JSON.parse(await nodeFs.readFile(inputFile, "utf8"));
const fs = { ...nodeFs, rename: async (from, to) => { const destination = String(to); if (destination.split(/[\\/]/u).at(-2) === "jobs" && destination.endsWith(".json")) { process.stdout.write("PAUSED\\n"); await new Promise(() => undefined); } return nodeFs.rename(from, to); } };
const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-killed-precommit", producerUuid: "00000000-0000-4000-8000-000000000270", machineId: "machine-killed-precommit", now: () => Date.parse("2029-01-01T00:00:00.000Z"), fs });
await outbox.enqueue({ episodes: [item], policy: current });
`, { mode: 0o600 }); child = spawn(process.execPath, [script, homeDir, inputFile], { cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"] }); let stdout = ""; let stderr = ""; child.stderr!.on("data", (chunk) => { stderr += String(chunk); }); await new Promise<void>((resolveReady, rejectReady) => { const timer = setTimeout(() => rejectReady(new Error("child did not reach precommit pause")), 5_000); child!.stdout!.on("data", (chunk) => { stdout += String(chunk); if (stdout.includes("PAUSED")) { clearTimeout(timer); resolveReady(); } }); child!.once("error", rejectReady); child!.once("exit", (code) => { if (!stdout.includes("PAUSED")) rejectReady(new Error(`child exited before pause: ${code}: ${stderr}`)); }); }); child.kill("SIGKILL"); await once(child, "exit"); const producerPath = join(homeDir, ".prime", "agent", "pi-qdrant-memory", "outbox", "node-killed-precommit", "00000000-0000-4000-8000-000000000270"); const producerB = await createOutbox({ host: "prime", homeDir, nodeId: "node-killed-precommit", producerUuid: producerId(271), machineId: "machine-killed-precommit", now: () => Date.parse("2029-01-01T00:00:00.000Z") }); const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const delivery = createOutboxDelivery({ outboxRoot: producerB.root, producerPath: producerB.producerPath, processor, now: () => Date.parse("2029-01-03T00:00:00.000Z"), maxClockSkewMs: 0, heartbeatTimeoutMs: 100 }); await delivery.adopt(producerPath); const fenced = (await readdir(producerPath)).filter((name) => name.startsWith("jobs.fenced-")); expect(fenced).toHaveLength(1); expect((await readdir(join(producerPath, fenced[0]!))).filter((name) => name.includes(".json.tmp-"))).toEqual([]); expect(activeReservationNames(await readdir(join(producerB.root, "reservations")))).toEqual([]); expect(await delivery.deliver({})).toEqual({ delivered: 1, pending: 0, quarantined: 0 }); expect(processor.process).toHaveBeenCalledTimes(1); const next = await producerB.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000901")], policy: current }); expect(await stat(next.file)).toBeDefined(); }
    finally { if (child?.exitCode === null && child.signalCode === null) { child.kill("SIGKILL"); await once(child, "exit").catch(() => undefined); } await rm(homeDir, { recursive: true, force: true }); }
  }, 10_000);


  it("fences a stale killed null-deadline admission before any job temp is written", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-killed-null-lock-")); const script = join(homeDir, "writer.mjs"); let child: ReturnType<typeof spawn> | undefined;
    try {
      const current = policy(null); const item = episode(current, "00000000-0000-5000-8000-000000000910"); const inputFile = join(homeDir, "input.json"); await nodeFs.writeFile(inputFile, JSON.stringify({ current, item }), { mode: 0o600 });
      await nodeFs.writeFile(script, `import * as nodeFs from "node:fs/promises";
import { createOutbox } from ${JSON.stringify(new URL("../../dist/outbox/store.js", import.meta.url).href)};
const [homeDir,inputFile] = process.argv.slice(2);
const { current, item } = JSON.parse(await nodeFs.readFile(inputFile, "utf8"));
const fs = { ...nodeFs, open: async (...args) => { const path = String(args[0]); if (path.includes("/jobs/") && /\\.json\\.tmp-[0-9]+-[a-f0-9]{32}$/u.test(path)) { process.stdout.write("PAUSED\\n"); await new Promise(() => undefined); } return nodeFs.open(...args); } };
const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-killed-null-lock", producerUuid: "00000000-0000-4000-8000-000000000300", machineId: "machine-killed-null-lock", now: () => 1_000, fs });
await outbox.enqueue({ episodes: [item], policy: current });
`, { mode: 0o600 });
      child = spawn(process.execPath, [script, homeDir, inputFile], { cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"] }); let stdout = ""; let stderr = ""; child.stderr!.on("data", (chunk) => { stderr += String(chunk); });
      await new Promise<void>((resolveReady, rejectReady) => { const timer = setTimeout(() => rejectReady(new Error("child did not reach pre-temp pause")), 5_000); child!.stdout!.on("data", (chunk) => { stdout += String(chunk); if (stdout.includes("PAUSED")) { clearTimeout(timer); resolveReady(); } }); child!.once("error", rejectReady); child!.once("exit", (code) => { if (!stdout.includes("PAUSED")) rejectReady(new Error(`child exited before pause: ${code}: ${stderr}`)); }); });
      const root = join(homeDir, ".prime", "agent", "pi-qdrant-memory", "outbox"); const producerPath = join(root, "node-killed-null-lock", "00000000-0000-4000-8000-000000000300"); const reservations = join(root, "reservations"); expect(activeReservationNames(await readdir(join(producerPath, "jobs")))).toEqual([]); expect(await readdir(reservations)).toEqual(expect.arrayContaining([expect.stringMatching(ADMISSION_LOCK_NAME)]));
      child.kill("SIGKILL"); await once(child, "exit"); const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const delivery = createOutboxDelivery({ outboxRoot: root, producerPath, processor, now: () => 1_101, maxClockSkewMs: 0, heartbeatTimeoutMs: 100 });
      expect(await delivery.deliver({ maxJobs: 1 })).toEqual({ delivered: 0, pending: 0, quarantined: 1 }); expect(processor.process).not.toHaveBeenCalled(); expect(activeReservationNames(await readdir(reservations))).toEqual([]); await expect(stat(join(producerPath, "jobs"))).rejects.toMatchObject({ code: "ENOENT" }); const fenced = (await readdir(producerPath)).filter((name) => name.startsWith("jobs.fenced-")); expect(fenced).toHaveLength(1); expect(activeReservationNames(await readdir(join(producerPath, fenced[0]!)))).toEqual([]);
      const audits = await Promise.all((await readdir(join(producerPath, "quarantine"))).map((name) => readFile(join(producerPath, "quarantine", name), "utf8"))); expect(audits.join("\n")).toContain("aborted_precommit"); expect(audits.join("\n")).not.toContain("already [token redacted]"); expect(audits.join("\n")).not.toContain("destinationIds");
      const nextProducer = await createOutbox({ host: "prime", homeDir, nodeId: "node-killed-null-lock", producerUuid: producerId(301), machineId: "machine-killed-null-lock", now: () => 1_101 }); const accepted = await nextProducer.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000911")], policy: current }); expect(await stat(accepted.file)).toBeDefined();
    } finally { if (child?.exitCode === null && child.signalCode === null) { child.kill("SIGKILL"); await once(child, "exit").catch(() => undefined); } await rm(homeDir, { recursive: true, force: true }); }
  }, 10_000);

  it("fences stale or closed live null-deadline admissions and the old writer cannot publish", async () => {
    for (const mode of ["stale", "closed"] as const) {
      const homeDir = await mkdtemp(join(tmpdir(), `task5-live-null-lock-${mode}-`)); let reached!: () => void; let resume!: () => void; const paused = new Promise<void>((resolvePaused) => { reached = resolvePaused; }); const gate = new Promise<void>((resolveResume) => { resume = resolveResume; }); let pause = true;
      const writerFs = { ...nodeFs, open: async (...args: Parameters<typeof open>) => { const path = String(args[0]); if (pause && path.includes("/jobs/") && /\.json\.tmp-[0-9]+-[a-f0-9]{32}$/u.test(path)) { pause = false; reached(); await gate; } return open(...args); } };
      try {
        const current = policy(null); const stale = await createOutbox({ host: "prime", homeDir, nodeId: `node-live-null-lock-${mode}`, producerUuid: producerId(mode === "stale" ? 302 : 303), machineId: `machine-live-null-lock-${mode}`, now: () => 1_000, fs: writerFs }); const accepting = stale.enqueue({ episodes: [episode(current, mode === "stale" ? "00000000-0000-5000-8000-000000000912" : "00000000-0000-5000-8000-000000000913")], policy: current }); const outcome = accepting.then(() => true, () => false); await paused; if (mode === "closed") await writeProducerState(stale.producerPath, "closed", 1_000, 1_000);
        const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const delivery = createOutboxDelivery({ outboxRoot: stale.root, producerPath: stale.producerPath, processor, now: () => mode === "stale" ? 1_101 : 1_000, maxClockSkewMs: 0, heartbeatTimeoutMs: 100 }); expect(await delivery.deliver({ maxJobs: 1 })).toEqual({ delivered: 0, pending: 0, quarantined: 1 }); expect(processor.process).not.toHaveBeenCalled(); expect(activeReservationNames(await readdir(join(stale.root, "reservations")))).toEqual([]);
        const fenced = (await readdir(stale.producerPath)).find((name) => name.startsWith("jobs.fenced-")); expect(fenced).toBeDefined(); expect(activeReservationNames(await readdir(join(stale.producerPath, fenced!)))).toEqual([]); resume(); expect(await outcome).toBe(false); await expect(stat(join(stale.producerPath, "jobs"))).rejects.toMatchObject({ code: "ENOENT" }); expect(activeReservationNames(await readdir(join(stale.producerPath, fenced!)))).toEqual([]);
      } finally { resume?.(); await rm(homeDir, { recursive: true, force: true }); }
    }
  }, 10_000);

  it("preserves fresh and heartbeat-boundary live null-deadline admissions", async () => {
    for (const delta of [99, 100] as const) {
      const homeDir = await mkdtemp(join(tmpdir(), `task5-boundary-null-lock-${delta}-`)); let reached!: () => void; let resume!: () => void; const paused = new Promise<void>((resolvePaused) => { reached = resolvePaused; }); const gate = new Promise<void>((resolveResume) => { resume = resolveResume; }); let pause = true;
      const writerFs = { ...nodeFs, open: async (...args: Parameters<typeof open>) => { const path = String(args[0]); if (pause && path.includes("/jobs/") && /\.json\.tmp-[0-9]+-[a-f0-9]{32}$/u.test(path)) { pause = false; reached(); await gate; } return open(...args); } };
      try {
        const current = policy(null); const nodeId = delta === 99 ? "node-boundary-live-a" : "node-boundary-live-b"; const outbox = await createOutbox({ host: "prime", homeDir, nodeId, producerUuid: producerId(delta === 99 ? 304 : 305), machineId: `machine-boundary-live-${delta}`, now: () => 1_000, fs: writerFs }); const accepting = outbox.enqueue({ episodes: [episode(current, delta === 99 ? "00000000-0000-5000-8000-000000000914" : "00000000-0000-5000-8000-000000000915")], policy: current }); const outcome = accepting.then(() => true, () => false); await paused;
        const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => 1_000 + delta, maxClockSkewMs: 0, heartbeatTimeoutMs: 100 }); expect(await delivery.deliver({ maxJobs: 1 })).toEqual({ delivered: 0, pending: 0, quarantined: 0 }); expect(processor.process).not.toHaveBeenCalled(); expect((await readdir(outbox.producerPath)).filter((name) => name.startsWith("jobs.fenced-"))).toEqual([]); expect(await readdir(join(outbox.root, "reservations"))).toEqual(expect.arrayContaining([expect.stringMatching(ADMISSION_LOCK_NAME)]));
        resume(); expect(await outcome).toBe(true); expect((await readdir(outbox.producerPath)).filter((name) => name.startsWith("jobs.fenced-"))).toEqual([]); expect(activeReservationNames(await readdir(join(outbox.root, "reservations")))).toEqual([]); expect((await readdir(join(outbox.producerPath, "jobs"))).filter((name) => name.endsWith(".json"))).toHaveLength(1);
      } finally { resume?.(); await rm(homeDir, { recursive: true, force: true }); }
    }
  }, 10_000);

  it("includes clock skew when fencing stale locked null-deadline admissions", async () => {
    for (const delta of [150, 151] as const) {
      const homeDir = await mkdtemp(join(tmpdir(), `task5-skewed-null-lock-${delta}-`)); let reached!: () => void; let resume!: () => void; const paused = new Promise<void>((resolvePaused) => { reached = resolvePaused; }); const gate = new Promise<void>((resolveResume) => { resume = resolveResume; }); let pause = true;
      const writerFs = { ...nodeFs, open: async (...args: Parameters<typeof open>) => { const path = String(args[0]); if (pause && path.includes("/jobs/") && /\.json\.tmp-[0-9]+-[a-f0-9]{32}$/u.test(path)) { pause = false; reached(); await gate; } return open(...args); } };
      try {
        const current = policy(null); const nodeId = delta === 150 ? "node-skewed-null-lock-a" : "node-skewed-null-lock-b"; const outbox = await createOutbox({ host: "prime", homeDir, nodeId, producerUuid: producerId(delta === 150 ? 307 : 308), machineId: `machine-skewed-null-lock-${delta}`, now: () => 1_000, fs: writerFs }); const accepting = outbox.enqueue({ episodes: [episode(current, delta === 150 ? "00000000-0000-5000-8000-000000000917" : "00000000-0000-5000-8000-000000000918")], policy: current }); const outcome = accepting.then(() => true, () => false); await paused;
        const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => 1_000 + delta, maxClockSkewMs: 50, heartbeatTimeoutMs: 100 }); const result = await delivery.deliver({ maxJobs: 1 });
        if (delta === 150) { expect(result).toEqual({ delivered: 0, pending: 0, quarantined: 0 }); expect((await readdir(outbox.producerPath)).filter((name) => name.startsWith("jobs.fenced-"))).toEqual([]); expect(await readdir(join(outbox.root, "reservations"))).toEqual(expect.arrayContaining([expect.stringMatching(ADMISSION_LOCK_NAME)])); resume(); expect(await outcome).toBe(true); expect(processor.process).not.toHaveBeenCalled(); }
        else { expect(result).toEqual({ delivered: 0, pending: 0, quarantined: 1 }); const fenced = (await readdir(outbox.producerPath)).filter((name) => name.startsWith("jobs.fenced-")); expect(fenced).toHaveLength(1); expect(activeReservationNames(await readdir(join(outbox.root, "reservations")))).toEqual([]); resume(); expect(await outcome).toBe(false); expect(processor.process).not.toHaveBeenCalled(); }
      } finally { resume?.(); await rm(homeDir, { recursive: true, force: true }); }
    }
  }, 10_000);

  it("does not fence a locked producer when the delivery clock fails validation", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-invalid-clock-null-lock-")); let reached!: () => void; let resume!: () => void; const paused = new Promise<void>((resolvePaused) => { reached = resolvePaused; }); const gate = new Promise<void>((resolveResume) => { resume = resolveResume; }); let pause = true;
    const writerFs = { ...nodeFs, open: async (...args: Parameters<typeof open>) => { const path = String(args[0]); if (pause && path.includes("/jobs/") && /\.json\.tmp-[0-9]+-[a-f0-9]{32}$/u.test(path)) { pause = false; reached(); await gate; } return open(...args); } };
    try {
      const current = policy(null); const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-invalid-clock-null-lock", producerUuid: producerId(306), machineId: "machine-invalid-clock-null-lock", now: () => 1_000, fs: writerFs }); const accepting = outbox.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000916")], policy: current }); const outcome = accepting.then(() => true, () => false); await paused;
      const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor: { process: vi.fn() }, now: () => Number.NaN, maxClockSkewMs: 0, heartbeatTimeoutMs: 100 }); await expect(delivery.deliver({ maxJobs: 1 })).rejects.toThrow(/clock/u); expect((await readdir(outbox.producerPath)).filter((name) => name.startsWith("jobs.fenced-"))).toEqual([]); expect(await readdir(join(outbox.root, "reservations"))).toEqual(expect.arrayContaining([expect.stringMatching(ADMISSION_LOCK_NAME)])); expect(activeReservationNames(await readdir(join(outbox.producerPath, "jobs")))).toEqual([]);
      resume(); expect(await outcome).toBe(true); expect(activeReservationNames(await readdir(join(outbox.root, "reservations")))).toEqual([]);
    } finally { resume?.(); await rm(homeDir, { recursive: true, force: true }); }
  }, 10_000);


  it("fails closed when an incomplete fence has multiple candidate jobs directories", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-fence-ambiguity-"));
    try { const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-fence-ambiguity", producerUuid: producerId(272), machineId: "machine-fence-ambiguity", now: () => 1_000 }); await outbox.closeProducer(); await nodeFs.rename(join(outbox.producerPath, "jobs"), join(outbox.producerPath, "jobs.fenced-00000000000000000000000000000001")); await nodeFs.mkdir(join(outbox.producerPath, "jobs.fenced-00000000000000000000000000000002"), { mode: 0o700 }); const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => 100_000, maxClockSkewMs: 0, heartbeatTimeoutMs: 100 }); await expect(delivery.adopt(outbox.producerPath)).rejects.toThrow(/ambiguous|incomplete/u); await expect(stat(join(outbox.producerPath, "fence.json"))).rejects.toMatchObject({ code: "ENOENT" }); expect(processor.process).not.toHaveBeenCalled(); }
    finally { await rm(homeDir, { recursive: true, force: true }); }
  });


  it("re-enters fenced recovery for an exact temp left after proof cleanup", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-fenced-temp-reentry-")); let reached!: () => void; let resume!: () => void; const paused = new Promise<void>((resolvePaused) => { reached = resolvePaused; }); const gate = new Promise<void>((resolveResume) => { resume = resolveResume; }); let pause = true;
    const writerFs = { ...nodeFs, rename: async (from: Parameters<typeof nodeFs.rename>[0], to: Parameters<typeof nodeFs.rename>[1]) => { const destination = String(to); if (pause && destination.split(/[\/]/u).at(-2) === "jobs" && destination.endsWith(".json")) { pause = false; reached(); await gate; } return nodeFs.rename(from, to); } };
    try { const current = policy(); const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-fenced-temp-reentry", producerUuid: producerId(273), machineId: "machine-fenced-temp-reentry", now: () => 1_000, fs: writerFs }); const accepting = outbox.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000903")], policy: current }); const outcome = accepting.then(() => true, () => false); await paused; const jobsDir = join(outbox.producerPath, "jobs"); const tempName = (await readdir(jobsDir)).find((name) => name.includes(".json.tmp-")); expect(tempName).toBeDefined(); const jobId = tempName!.slice(0, 36); await nodeFs.link(join(jobsDir, tempName!), join(jobsDir, `${jobId}.json`)); const reservationsDir = join(outbox.root, "reservations"); let failed = false; const faultFs = { ...nodeFs, lstat: async (...args: Parameters<typeof nodeFs.lstat>) => { const path = String(args[0]); if (!failed && path.includes(".json.tmp-") && activeReservationNames(await nodeFs.readdir(reservationsDir)).length === 0) { failed = true; throw Object.assign(new Error("post-proof orphan sweep crash"), { code: "EIO" }); } return nodeFs.lstat(...args); } }; const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const crashing = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => 100_000, maxClockSkewMs: 0, heartbeatTimeoutMs: 100, fs: faultFs }); await expect(crashing.adopt(outbox.producerPath)).rejects.toThrow(/orphan sweep crash/u); expect(activeReservationNames(await readdir(reservationsDir))).toEqual([]); const fenced = (await readdir(outbox.producerPath)).find((name) => name.startsWith("jobs.fenced-")); expect(fenced).toBeDefined(); expect((await readdir(join(outbox.producerPath, fenced!))).some((name) => name.includes(".json.tmp-"))).toBe(true); resume(); expect(await outcome).toBe(false); const recovered = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => 100_000, maxClockSkewMs: 0, heartbeatTimeoutMs: 100 }); expect(await recovered.deliver({ maxJobs: 1 })).toEqual({ delivered: 0, pending: 0, quarantined: 1 }); expect((await readdir(join(outbox.producerPath, fenced!))).some((name) => name.includes(".json.tmp-"))).toBe(false); const audits = await Promise.all((await readdir(join(outbox.producerPath, "quarantine"))).map((name) => readFile(join(outbox.producerPath, "quarantine", name), "utf8"))); expect(audits.join("\n")).toContain("orphan_precommit"); expect(audits.join("\n")).not.toContain("already [token redacted]"); expect(processor.process).not.toHaveBeenCalled(); }
    finally { resume?.(); await rm(homeDir, { recursive: true, force: true }); }
  });


  it("fsyncs a recovered complete payload inode before publication and retains proof on sync failure", async () => {
    for (const failSync of [false, true]) { const homeDir = await mkdtemp(join(tmpdir(), `task5-recovery-inode-sync-${failSync}-`)); let reached!: () => void; let resume!: () => void; const paused = new Promise<void>((resolvePaused) => { reached = resolvePaused; }); const gate = new Promise<void>((resolveResume) => { resume = resolveResume; }); let pause = true;
      const writerFs = { ...nodeFs, open: async (...args: Parameters<typeof open>) => { const handle = await open(...args); const path = String(args[0]); if (pause && path.includes("/jobs/") && /\.json\.tmp-/u.test(path)) { pause = false; const write = handle.writeFile.bind(handle); return Object.assign(handle, { writeFile: async (...writeArgs: Parameters<typeof handle.writeFile>) => { await write(...writeArgs); reached(); await gate; } }); } return handle; } };
      try { const current = policy(); const producerA = await createOutbox({ host: "prime", homeDir, nodeId: `node-recovery-inode-sync-${failSync}`, producerUuid: producerId(failSync ? 275 : 274), machineId: `machine-recovery-inode-sync-${failSync}`, now: () => 1_000, fs: writerFs }); const producerB = await createOutbox({ host: "prime", homeDir, nodeId: `node-recovery-inode-sync-${failSync}`, producerUuid: producerId(failSync ? 277 : 276), machineId: `machine-recovery-inode-sync-${failSync}`, now: () => 1_000 }); const accepting = producerA.enqueue({ episodes: [episode(current, failSync ? "00000000-0000-5000-8000-000000000905" : "00000000-0000-5000-8000-000000000904")], policy: current }); const outcome = accepting.then(() => true, () => false); await paused; const reservation = await readAdmissionLock(join(producerA.root, "reservations")); const events: string[] = []; const recoveryFs = { ...nodeFs, open: async (...args: Parameters<typeof open>) => { const handle = await open(...args); const path = String(args[0]); const sync = handle.sync.bind(handle); if (path.includes(".json.tmp-")) return Object.assign(handle, { sync: async () => { events.push("file-sync"); if (failSync) throw Object.assign(new Error("recovered payload sync EIO"), { code: "EIO" }); return sync(); } }); if (path.includes("/jobs.fenced-") && String(args[1]) === "r") return Object.assign(handle, { sync: async () => { events.push("directory-sync"); return sync(); } }); if (path.includes("/jobs.fenced-") && path.endsWith(`${reservation.jobId}.json`)) { const read = handle.readFile.bind(handle); return Object.assign(handle, { readFile: async (...readArgs: Parameters<typeof handle.readFile>) => { const value = await read(...readArgs); events.push("readback"); return value; } }); } return handle; }, link: async (from: Parameters<typeof nodeFs.link>[0], to: Parameters<typeof nodeFs.link>[1]) => { if (String(from).includes(".json.tmp-")) events.push("link"); return nodeFs.link(from, to); }, readFile: async (...args: Parameters<typeof nodeFs.readFile>) => { const value = await nodeFs.readFile(...args); const path = String(args[0]); if (path.includes("/jobs.fenced-") && path.endsWith(`${reservation.jobId}.json`)) events.push("readback"); return value; }, rm: async (path: Parameters<typeof nodeFs.rm>[0], options?: Parameters<typeof nodeFs.rm>[1]) => { if (ADMISSION_LOCK_NAME.test(String(path).split(/[\/]/u).at(-1) ?? "")) events.push("proof-clear"); return options === undefined ? nodeFs.rm(path) : nodeFs.rm(path, options); } }; const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const delivery = createOutboxDelivery({ outboxRoot: producerB.root, producerPath: producerB.producerPath, processor, now: () => 100_000, maxClockSkewMs: 0, heartbeatTimeoutMs: 100, fs: recoveryFs }); if (failSync) { await expect(delivery.adopt(producerA.producerPath)).rejects.toThrow(/payload sync EIO/u); expect(events).toEqual(["file-sync"]); expect(await readdir(join(producerA.root, "reservations"))).toEqual(expect.arrayContaining([expect.stringMatching(ADMISSION_LOCK_NAME)])); const fenced = (await readdir(producerA.producerPath)).find((name) => name.startsWith("jobs.fenced-")); expect(fenced).toBeDefined(); expect((await readdir(join(producerA.producerPath, fenced!))).some((name) => name.includes(".json.tmp-"))).toBe(true); await expect(stat(join(producerA.producerPath, fenced!, `${reservation.jobId}.json`))).rejects.toMatchObject({ code: "ENOENT" }); const retry = createOutboxDelivery({ outboxRoot: producerB.root, producerPath: producerB.producerPath, processor, now: () => 100_000, maxClockSkewMs: 0, heartbeatTimeoutMs: 100 }); await retry.adopt(producerA.producerPath); }
        else { await delivery.adopt(producerA.producerPath); const fileSync = events.indexOf("file-sync"); const preparationDirectorySync = events.indexOf("directory-sync"); const link = events.indexOf("link"); const canonicalDirectorySync = events.indexOf("directory-sync", link + 1); const readback = events.indexOf("readback"); const proofClear = events.indexOf("proof-clear"); expect(fileSync).toBeGreaterThanOrEqual(0); expect(fileSync).toBeLessThan(preparationDirectorySync); expect(preparationDirectorySync).toBeLessThan(link); expect(link).toBeLessThan(canonicalDirectorySync); expect(canonicalDirectorySync).toBeLessThan(readback); expect(readback).toBeLessThan(proofClear); expect(activeReservationNames(await readdir(join(producerA.root, "reservations")))).toEqual([]); }
        resume(); expect(await outcome).toBe(false); expect(processor.process).not.toHaveBeenCalled(); }
      finally { resume?.(); await rm(homeDir, { recursive: true, force: true }); }
    }
  });

  it("includes configured clock skew in stale-adoption liveness grace", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-adopt-skew-grace-")); let now = 1_150;
    try { const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-adopt-skew-grace", producerUuid: producerId(278), machineId: "machine-adopt-skew-grace", now: () => 1_000 }); const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor, now: () => now, maxClockSkewMs: 50, heartbeatTimeoutMs: 100 }); await expect(delivery.adopt(outbox.producerPath)).rejects.toThrow(/still active/u); expect((await readdir(outbox.producerPath)).filter((name) => name.startsWith("jobs.fenced-"))).toEqual([]); now += 1; await expect(delivery.adopt(outbox.producerPath)).resolves.toBeUndefined(); expect((await readdir(outbox.producerPath)).filter((name) => name.startsWith("jobs.fenced-"))).toHaveLength(1); }
    finally { await rm(homeDir, { recursive: true, force: true }); }
  });


  it("retains proof after canonical rename until jobs-directory fsync and strict readback succeed", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-canonical-proof-order-")); let jobsSyncs = 0; let postSync = false; let canonicalFile = ""; const events: string[] = [];
    const fs = { ...nodeFs, rename: async (from: Parameters<typeof nodeFs.rename>[0], to: Parameters<typeof nodeFs.rename>[1]) => { const result = await nodeFs.rename(from, to); if (String(to).split(/[\/]/u).at(-2) === "jobs" && String(to).endsWith(".json")) events.push("rename"); return result; }, open: async (...args: Parameters<typeof open>) => { const handle = await open(...args); const path = String(args[0]); const sync = handle.sync.bind(handle); if (path.endsWith("/jobs") && String(args[1]) === "r") return Object.assign(handle, { sync: async () => { jobsSyncs += 1; events.push(`dir-sync-${jobsSyncs}`); if (jobsSyncs <= 2) throw Object.assign(new Error(`jobs dir fsync ${jobsSyncs} EIO`), { code: "EIO" }); await sync(); postSync = true; } }); if (path === canonicalFile) { const read = handle.readFile.bind(handle); return Object.assign(handle, { readFile: async (...readArgs: Parameters<typeof handle.readFile>) => { const value = await read(...readArgs); if (postSync) { events.push("post-sync-readback"); postSync = false; } return value; } }); } return handle; }, readFile: async (...args: Parameters<typeof nodeFs.readFile>) => { const value = await nodeFs.readFile(...args); if (postSync && String(args[0]) === canonicalFile) { events.push("post-sync-readback"); postSync = false; } return value; }, rm: async (path: Parameters<typeof nodeFs.rm>[0], options?: Parameters<typeof nodeFs.rm>[1]) => { if (ADMISSION_LOCK_NAME.test(String(path).split(/[\/]/u).at(-1) ?? "")) events.push("proof-clear"); return options === undefined ? nodeFs.rm(path) : nodeFs.rm(path, options); } };
    try { const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-canonical-proof-order", producerUuid: producerId(279), machineId: "machine-canonical-proof-order", now: () => Date.parse("2029-01-01T00:00:00.000Z"), fs }); const current = policy(); const enqueueInput = { episodes: [episode(current, "00000000-0000-5000-8000-000000000906")], policy: current }; await expect(outbox.enqueue(enqueueInput)).rejects.toThrow(/jobs dir fsync 1/u); canonicalFile = join(outbox.producerPath, "jobs", (await readdir(join(outbox.producerPath, "jobs"))).find((name) => /^[-0-9a-f]{36}\.json$/u.test(name))!); expect(await stat(canonicalFile)).toBeDefined(); expect(await readdir(join(outbox.root, "reservations"))).toEqual(expect.arrayContaining([expect.stringMatching(ADMISSION_LOCK_NAME)])); events.length = 0; await expect(outbox.enqueue(enqueueInput)).rejects.toThrow(/jobs dir fsync 2/u); expect(events).toEqual(["dir-sync-2"]); expect(await readdir(join(outbox.root, "reservations"))).toEqual(expect.arrayContaining([expect.stringMatching(ADMISSION_LOCK_NAME)])); events.length = 0; const recovered = await outbox.enqueue(enqueueInput); expect(recovered.file).toBe(canonicalFile); const sync = events.indexOf("dir-sync-3"); const readback = events.indexOf("post-sync-readback"); const clear = events.indexOf("proof-clear"); expect(sync).toBe(0); expect(sync).toBeLessThan(readback); expect(readback).toBeLessThan(clear); expect(activeReservationNames(await readdir(join(outbox.root, "reservations")))).toEqual([]); }
    finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("does not reclaim a closed producer precommit proof before fenced recovery", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-closed-precommit-proof-"));
    let reached!: () => void; let resume!: () => void;
    const paused = new Promise<void>((resolvePaused) => { reached = resolvePaused; });
    const gate = new Promise<void>((resolveResume) => { resume = resolveResume; });
    let pause = true;
    const writerFs = { ...nodeFs, open: async (...args: Parameters<typeof open>) => { const handle = await open(...args); const path = String(args[0]); if (pause && path.includes("/jobs/") && path.includes(".json.tmp-")) { pause = false; const write = handle.writeFile.bind(handle); return Object.assign(handle, { writeFile: async (...writeArgs: Parameters<typeof handle.writeFile>) => { await write(...writeArgs); reached(); await gate; } }); } return handle; } };
    let sawBlockedB = false; let markBlockedB!: () => void;
    const blockedB = new Promise<void>((resolveBlocked) => { markBlockedB = resolveBlocked; });
    const producerBFs = { ...nodeFs, readdir: async (path: Parameters<typeof nodeFs.readdir>[0], options?: Parameters<typeof nodeFs.readdir>[1]) => {
      const names = options === undefined ? await nodeFs.readdir(path) : await nodeFs.readdir(path, options);
      if (!sawBlockedB && String(path).endsWith("/reservations") && (names as string[]).some((name) => ADMISSION_LOCK_NAME.test(name))) { sawBlockedB = true; markBlockedB(); }
      return names;
    } };
    try {
      const producerA = await createOutbox({ host: "prime", homeDir, nodeId: "node-closed-precommit-proof", producerUuid: producerId(280), machineId: "machine-closed-precommit-proof", now: () => 1_000, fs: writerFs });
      const producerB = await createOutbox({ host: "prime", homeDir, nodeId: "node-closed-precommit-proof", producerUuid: producerId(281), machineId: "machine-closed-precommit-proof", now: () => 1_000, fs: producerBFs });
      const current = policy();
      const acceptingA = producerA.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000907")], policy: current });
      const outcomeA = acceptingA.then(() => true, () => false);
      await paused;
      const closed = { version: 1, state: "closed", heartbeatAt: 1_000, closedAt: 1_000, auditHash: "" };
      closed.auditHash = sha256Hex(canonicalStringify({ closedAt: closed.closedAt, heartbeatAt: closed.heartbeatAt, state: closed.state, version: closed.version }));
      await nodeFs.writeFile(join(producerA.producerPath, "state.json"), canonicalStringify(closed), { mode: 0o600 });
      let settledB = false;
      const acceptingB = producerB.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000908")], policy: current }).then((value) => ({ status: "fulfilled" as const, value }), (reason: unknown) => ({ status: "rejected" as const, reason }));
      void acceptingB.then(() => { settledB = true; });
      await blockedB;
      expect(settledB).toBe(false);
      const lockNameBefore = (await readdir(join(producerA.root, "reservations"))).find((name) => /^admission\.\d{16}\.lock$/u.test(name));
      expect(lockNameBefore).toBeDefined();
      const lockBefore = JSON.parse(await readFile(join(producerA.root, "reservations", lockNameBefore!), "utf8"));
      expect(lockBefore.producerUuid).toBe(producerA.producerUuid);
      const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) };
      const delivery = createOutboxDelivery({ outboxRoot: producerB.root, producerPath: producerB.producerPath, processor, now: () => 1_000, maxClockSkewMs: 0 });
      await delivery.adopt(producerA.producerPath);
      const outcomeB = await acceptingB;
      expect(outcomeB.status).toBe("fulfilled");
      if (outcomeB.status !== "fulfilled") throw outcomeB.reason;
      expect(await stat(outcomeB.value.file)).toBeDefined();
      resume();
      expect(await outcomeA).toBe(false);
      expect((await readdir(producerA.producerPath)).filter((name) => name.startsWith("jobs.fenced-"))).toHaveLength(1);
      expect(processor.process).not.toHaveBeenCalled();
    } finally { resume?.(); await rm(homeDir, { recursive: true, force: true }); }
  }, 10_000);

  it("never reuses a retired generation while stale recovery and a capped contender interleave", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-no-reuse-interleave-")); let pauseOwnerRemoval = true; let ownerPaused!: () => void; let releaseOwner!: () => void; const ownerPausedGate = new Promise<void>((resolve) => { ownerPaused = resolve; }); const releaseOwnerGate = new Promise<void>((resolve) => { releaseOwner = resolve; }); let recoveryPaused!: () => void; let releaseRecovery!: () => void; const recoveryPausedGate = new Promise<void>((resolve) => { recoveryPaused = resolve; }); const releaseRecoveryGate = new Promise<void>((resolve) => { releaseRecovery = resolve; }); let pauseRecoveryRemoval = true; const bRetirements: string[] = [];
    const ownerFs = { ...nodeFs, rm: async (path: Parameters<typeof nodeFs.rm>[0], options?: Parameters<typeof nodeFs.rm>[1]) => { if (pauseOwnerRemoval && ADMISSION_LOCK_NAME.test(String(path).split(/[\/]/u).at(-1) ?? "")) { pauseOwnerRemoval = false; ownerPaused(); await releaseOwnerGate; } return options === undefined ? nodeFs.rm(path) : nodeFs.rm(path, options); } };
    const recoveryFs = { ...nodeFs, rm: async (path: Parameters<typeof nodeFs.rm>[0], options?: Parameters<typeof nodeFs.rm>[1]) => { if (pauseRecoveryRemoval && ADMISSION_LOCK_NAME.test(String(path).split(/[\/]/u).at(-1) ?? "")) { pauseRecoveryRemoval = false; recoveryPaused(); await releaseRecoveryGate; } return options === undefined ? nodeFs.rm(path) : nodeFs.rm(path, options); } };
    const contenderFs = { ...nodeFs, link: async (existing: Parameters<typeof nodeFs.link>[0], target: Parameters<typeof nodeFs.link>[1]) => { if (ADMISSION_RETIREMENT_NAME.test(String(target).split(/[\/]/u).at(-1) ?? "")) bRetirements.push(String(target).split(/[\/]/u).at(-1)!); return nodeFs.link(existing, target); } };
    try {
      const current = policy(); const owner = await createOutbox({ host: "prime", homeDir, nodeId: "node-no-reuse-interleave", producerUuid: producerId(296), machineId: "machine-no-reuse-interleave", now: () => 1_000, maxJobs: 1, fs: ownerFs }); const contender = await createOutbox({ host: "prime", homeDir, nodeId: "node-no-reuse-interleave", producerUuid: producerId(297), machineId: "machine-no-reuse-interleave", now: () => 1_000, maxJobs: 1, fs: contenderFs });
      const ownerAttempt = owner.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000946")], policy: current }); await ownerPausedGate;
      const reservations = join(owner.root, "reservations"); expect(await readdir(reservations)).toEqual(expect.arrayContaining(["admission.0000000000000000.retired", "admission.0000000000000000.lock"]));
      await writeProducerState(owner.producerPath, "closed", 1_000, 1_000);
      const recovery = createOutboxDelivery({ outboxRoot: owner.root, producerPath: contender.producerPath, processor: { process: vi.fn() }, now: () => 100_000, maxClockSkewMs: 0, heartbeatTimeoutMs: 100, fs: recoveryFs }).adopt(owner.producerPath); await recoveryPausedGate;
      await expect(contender.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000947")], policy: current })).rejects.toThrow(/capacity/u);
      expect(bRetirements).toEqual(["admission.0000000000000001.retired"]); const whileRecoveryPaused = await readdir(reservations); expect(whileRecoveryPaused).toEqual(expect.arrayContaining(["admission.0000000000000000.retired", "admission.0000000000000001.retired"])); expect(whileRecoveryPaused).not.toContain("admission.0000000000000000.lock"); expect((await contender.outboxStatus()).captureAllowed).toBe(false); expect((await contender.outboxStatus()).jobs).toBe(1);
      releaseRecovery(); await recovery; releaseOwner(); expect(await ownerAttempt).toBeDefined(); expect(activeAdmissionLockNames(await readdir(reservations))).toEqual([]);
    } finally { releaseRecovery?.(); releaseOwner?.(); await rm(homeDir, { recursive: true, force: true }); }
  }, 10_000);

  it("resets the process admission cache when the reservations directory inode is replaced", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-admission-cache-replacement-"));
    try {
      const current = policy(); const first = await createOutbox({ host: "prime", homeDir, nodeId: "node-admission-cache-replacement", producerUuid: producerId(298), machineId: "machine-admission-cache-replacement" }); await first.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000948")], policy: current });
      const reservations = join(first.root, "reservations"); await nodeFs.rename(reservations, `${reservations}.replaced`); await nodeFs.mkdir(reservations, { mode: 0o700 }); const rootHandle = await open(first.root, "r"); try { await rootHandle.sync(); } finally { await rootHandle.close(); }
      const second = await createOutbox({ host: "prime", homeDir, nodeId: "node-admission-cache-replacement", producerUuid: producerId(299), machineId: "machine-admission-cache-replacement" }); await second.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000949")], policy: current });
      expect(await readdir(reservations)).toEqual(expect.arrayContaining(["admission.0000000000000000.retired"])); expect(await readdir(reservations)).not.toEqual(expect.arrayContaining(["admission.0000000000000001.retired"]));
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("advances to a generation-specific lock after a completed stale generation", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-completed-lock-generation-"));
    let failRetirement = true;
    const staleFs = { ...nodeFs, link: async (existingPath: Parameters<typeof nodeFs.link>[0], newPath: Parameters<typeof nodeFs.link>[1]) => { if (failRetirement && /admission\.\d{16}\.retired$/u.test(String(newPath))) { failRetirement = false; throw Object.assign(new Error("completed generation retirement fault"), { code: "EIO" }); } return nodeFs.link(existingPath, newPath); } };
    try {
      const stale = await createOutbox({ host: "prime", homeDir, nodeId: "node-completed-lock-generation", producerUuid: producerId(282), machineId: "machine-completed-lock-generation", now: () => 1_000, fs: staleFs });
      const current = policy();
      await expect(stale.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000909")], policy: current })).rejects.toThrow(/completed generation retirement fault/u);
      const before = await readdir(join(stale.root, "reservations"));
      expect(before).toEqual(expect.arrayContaining([expect.stringMatching(/^admission\.\d{16}\.lock$/u)]));
      expect(before.some((name) => /^admission\.\d{16}\.retired$/u.test(name))).toBe(false);
      const producer = await createOutbox({ host: "prime", homeDir, nodeId: "node-completed-lock-generation", producerUuid: producerId(283), machineId: "machine-completed-lock-generation", now: () => 1_001 });
      const accepted = await producer.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000919")], policy: current });
      expect(await stat(accepted.file)).toBeDefined();
      const after = await readdir(join(producer.root, "reservations"));
      expect(after).toEqual(expect.arrayContaining(["admission.0000000000000000.retired", "admission.0000000000000001.retired"]));
      expect(activeAdmissionLockNames(after)).toEqual([]);
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });


  it("keeps generation-specific admission locks safe across child-process reclaimers", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-child-generation-race-")); const script = join(homeDir, "racer.mjs"); const children: Array<ReturnType<typeof spawn>> = [];
    try {
      const current = policy(); let failRetirement = true;
      const staleFs = { ...nodeFs, link: async (existingPath: Parameters<typeof nodeFs.link>[0], newPath: Parameters<typeof nodeFs.link>[1]) => { if (failRetirement && /admission\.\d{16}\.retired$/u.test(String(newPath))) { failRetirement = false; throw Object.assign(new Error("stale retirement crash"), { code: "EIO" }); } return nodeFs.link(existingPath, newPath); } };
      const stale = await createOutbox({ host: "prime", homeDir, nodeId: "node-child-generation-race", producerUuid: producerId(284), machineId: "machine-child-generation-race", now: () => 1_000, fs: staleFs });
      await expect(stale.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000930")], policy: current })).rejects.toThrow(/stale retirement crash/u);
      const inputFile = join(homeDir, "input.json"); await nodeFs.writeFile(inputFile, JSON.stringify({ current }), { mode: 0o600 });
      await nodeFs.writeFile(script, `import * as nodeFs from "node:fs/promises";
import { createOutbox } from ${JSON.stringify(new URL("../../dist/outbox/store.js", import.meta.url).href)};
import { canonicalRecordHash } from ${JSON.stringify(new URL("../../dist/domain/records.js", import.meta.url).href)};
const [homeDir,inputFile,producerUuid,episodeId] = process.argv.slice(2);
const { current } = JSON.parse(await nodeFs.readFile(inputFile, "utf8"));
function episode(id) { const pending = { recordType: "episode", id, ownerHost: "prime", schemaRevision: 1, createdAt: "2029-01-01T00:00:00.000Z", privacyEpoch: 0, processingPolicyId: current.id, expiresAt: current.expiresAt, contentHash: "pending", sourceEntryId: "entry-1", host: "prime", projectId: "project-1", projectIdentityKind: "registered", sessionId: "session-1", turnId: "turn-1", agentRole: "root", depth: 0, eventKind: "user", eventAt: "2029-01-01T00:00:00.000Z", modelId: "model-local", embeddingDimension: 1024, originProvider: "provider-local", destinationId: "qdrant:local", status: "active", secretScan: "passed", text: "already [token redacted]" }; return { ...pending, contentHash: canonicalRecordHash(pending) }; }
const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-child-generation-race", producerUuid, machineId: "machine-child-generation-race", now: () => 1_001 });
const accepted = await outbox.enqueue({ episodes: [episode(episodeId)], policy: current });
process.stdout.write(JSON.stringify({ file: accepted.file }) + "\\n");
`, { mode: 0o600 });
      const runChild = (producerUuid: string, episodeId: string): { child: ReturnType<typeof spawn>; output: Promise<{ file: string }> } => { const child = spawn(process.execPath, [script, homeDir, inputFile, producerUuid, episodeId], { cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"] }); children.push(child); let stdout = ""; let stderr = ""; child.stdout!.on("data", (chunk) => { stdout += String(chunk); }); child.stderr!.on("data", (chunk) => { stderr += String(chunk); }); return { child, output: once(child, "exit").then(([code]) => { if (code !== 0) throw new Error(stderr); return JSON.parse(stdout.trim()) as { file: string }; }) }; };
      const left = runChild(producerId(285), "00000000-0000-5000-8000-000000000931"); const right = runChild(producerId(286), "00000000-0000-5000-8000-000000000932");
      const accepted = await Promise.all([left.output, right.output]); for (const item of accepted) expect(await stat(item.file)).toBeDefined();
      const reservations = await readdir(join(stale.root, "reservations")); expect(activeReservationNames(reservations)).toEqual([]); expect(activeAdmissionLockNames(reservations)).toEqual([]);
    } finally { for (const child of children) if (child.exitCode === null && child.signalCode === null) { child.kill("SIGKILL"); await once(child, "exit").catch(() => undefined); } await rm(homeDir, { recursive: true, force: true }); }
  }, 20_000);

  it("skips a generation retired between successful child-process link and post-link readback", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-post-link-retired-generation-")); const script = join(homeDir, "postlink.mjs"); let child: ReturnType<typeof spawn> | undefined;
    try {
      const current = policy(); const inputFile = join(homeDir, "input.json"); const readyFile = join(homeDir, "linked.ready"); const releaseFile = join(homeDir, "linked.release"); await nodeFs.writeFile(inputFile, JSON.stringify({ current }), { mode: 0o600 });
      await nodeFs.writeFile(script, `import * as nodeFs from "node:fs/promises";
import { setTimeout as delay } from "node:timers/promises";
import { createOutbox } from ${JSON.stringify(new URL("../../dist/outbox/store.js", import.meta.url).href)};
import { canonicalRecordHash } from ${JSON.stringify(new URL("../../dist/domain/records.js", import.meta.url).href)};
const [homeDir,inputFile,readyFile,releaseFile] = process.argv.slice(2);
const { current } = JSON.parse(await nodeFs.readFile(inputFile, "utf8"));
function episode(id) { const pending = { recordType: "episode", id, ownerHost: "prime", schemaRevision: 1, createdAt: "2029-01-01T00:00:00.000Z", privacyEpoch: 0, processingPolicyId: current.id, expiresAt: current.expiresAt, contentHash: "pending", sourceEntryId: "entry-1", host: "prime", projectId: "project-1", projectIdentityKind: "registered", sessionId: "session-1", turnId: "turn-1", agentRole: "root", depth: 0, eventKind: "user", eventAt: "2029-01-01T00:00:00.000Z", modelId: "model-local", embeddingDimension: 1024, originProvider: "provider-local", destinationId: "qdrant:local", status: "active", secretScan: "passed", text: "already [token redacted]" }; return { ...pending, contentHash: canonicalRecordHash(pending) }; }
let paused = false;
const fs = { ...nodeFs, link: async (existingPath, newPath) => { if (!paused && /admission\.0000000000000000\.lock$/u.test(String(newPath))) { paused = true; await nodeFs.writeFile(readyFile, "ready", { mode: 0o600 }); while (!(await nodeFs.stat(releaseFile).then(() => true, () => false))) await delay(5); } return nodeFs.link(existingPath, newPath); } };
const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-post-link-retired-generation", producerUuid: "00000000-0000-4000-8000-000000000287", machineId: "machine-post-link-retired-generation", now: () => 1_000, fs });
const accepted = await outbox.enqueue({ episodes: [episode("00000000-0000-5000-8000-000000000933")], policy: current });
process.stdout.write(JSON.stringify({ file: accepted.file }) + "\\n");
`, { mode: 0o600 });
      child = spawn(process.execPath, [script, homeDir, inputFile, readyFile, releaseFile], { cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"] }); let stdout = ""; let stderr = ""; child.stdout!.on("data", (chunk) => { stdout += String(chunk); }); child.stderr!.on("data", (chunk) => { stderr += String(chunk); });
      await new Promise<void>((resolveReady, rejectReady) => { const timer = setTimeout(() => rejectReady(new Error(`child did not pause before link: ${stderr}`)), 5_000); const check = async () => { if (await nodeFs.stat(readyFile).then(() => true, () => false)) { clearTimeout(timer); resolveReady(); } else setTimeout(check, 5); }; void check(); child!.once("exit", (code) => { if (code !== null) rejectReady(new Error(`child exited early ${code}: ${stderr}`)); }); });
      const reservations = join(homeDir, ".prime", "agent", "pi-qdrant-memory", "outbox", "reservations"); const oldReservation: Record<string, unknown> = { version: 1, reservationId: deterministicUuid("pi-qdrant-memory-v2:outbox-reservation", "node-post-link-retired-generation", producerId(288), "00000000-0000-5000-8000-000000000934"), jobId: "00000000-0000-5000-8000-000000000934", jobAuditHash: sha256Hex("old-post-link"), policyId: current.id, deadline: current.expiresAt, nodeId: "node-post-link-retired-generation", producerUuid: producerId(288), requestedBytes: 1, auditHash: "" }; oldReservation.auditHash = sha256Hex(canonicalStringify({ deadline: oldReservation.deadline, jobAuditHash: oldReservation.jobAuditHash, jobId: oldReservation.jobId, nodeId: oldReservation.nodeId, policyId: oldReservation.policyId, producerUuid: oldReservation.producerUuid, requestedBytes: oldReservation.requestedBytes, reservationId: oldReservation.reservationId, version: 1 })); const retirement: Record<string, unknown> = { version: 1, kind: "admission_lock_retired", generation: 0, reservation: oldReservation, auditHash: "" }; retirement.auditHash = sha256Hex(canonicalStringify({ generation: 0, kind: "admission_lock_retired", reservation: oldReservation, version: 1 })); await nodeFs.writeFile(join(reservations, "admission.0000000000000000.retired"), canonicalStringify(retirement), { mode: 0o600 }); const handle = await open(reservations, "r"); await handle.sync(); await handle.close();
      await nodeFs.writeFile(releaseFile, "go", { mode: 0o600 }); const [code] = await once(child, "exit"); expect({ code, stderr }).toMatchObject({ code: 0 }); const accepted = JSON.parse(stdout.trim()) as { file: string }; expect(await stat(accepted.file)).toBeDefined(); const names = await readdir(reservations); expect(activeAdmissionLockNames(names)).toEqual([]); expect(names).toEqual(expect.arrayContaining(["admission.0000000000000000.retired", "admission.0000000000000001.retired"]));
    } finally { if (child?.exitCode === null && child.signalCode === null) { child.kill("SIGKILL"); await once(child, "exit").catch(() => undefined); } await rm(homeDir, { recursive: true, force: true }); }
  }, 20_000);

  it("fails closed on legacy or gapped admission generation artifacts", async () => {
    for (const mode of ["legacy", "retirement-gap", "future-lock"] as const) {
      const homeDir = await mkdtemp(join(tmpdir(), `task5-admission-corruption-${mode}-`));
      try {
        const outbox = await createOutbox({ host: "prime", homeDir, nodeId: `node-admission-corruption-${mode}`, producerUuid: producerId(mode === "legacy" ? 290 : mode === "retirement-gap" ? 291 : 292), machineId: `machine-admission-corruption-${mode}` }); const current = policy(); const reservations = join(outbox.root, "reservations");
        const corrupt = reservationRecord(`node-admission-corruption-${mode}`, outbox.producerUuid, "00000000-0000-5000-8000-000000000940", current);
        if (mode === "legacy") await nodeFs.writeFile(join(reservations, "admission.lock"), canonicalStringify(corrupt), { mode: 0o600 });
        else if (mode === "retirement-gap") await nodeFs.writeFile(join(reservations, "admission.0000000000000001.retired"), canonicalStringify(retirementRecord(1, corrupt)), { mode: 0o600 });
        else await nodeFs.writeFile(join(reservations, "admission.0000000000000001.lock"), canonicalStringify(corrupt), { mode: 0o600 });
        const handle = await open(reservations, "r"); await handle.sync(); await handle.close();
        expect((await outbox.outboxStatus()).captureAllowed).toBe(false); await expect(outbox.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000941")], policy: current })).rejects.toThrow(/legacy|gap|cursor/u);
      } finally { await rm(homeDir, { recursive: true, force: true }); }
    }
  });

  it("does not advance to the next generation until retirement publication is directory-durable", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-retirement-sync-proof-")); let failRetirement = true;
    const staleFs = { ...nodeFs, link: async (existingPath: Parameters<typeof nodeFs.link>[0], newPath: Parameters<typeof nodeFs.link>[1]) => { if (failRetirement && /admission\.\d{16}\.retired$/u.test(String(newPath))) { failRetirement = false; throw Object.assign(new Error("seed retirement fault"), { code: "EIO" }); } return nodeFs.link(existingPath, newPath); } };
    try {
      const stale = await createOutbox({ host: "prime", homeDir, nodeId: "node-retirement-sync-proof", producerUuid: producerId(293), machineId: "machine-retirement-sync-proof", now: () => 1_000, fs: staleFs }); const current = policy(); await expect(stale.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000943")], policy: current })).rejects.toThrow(/seed retirement fault/u);
      let armed = false; let failed = false; const fs = { ...nodeFs, link: async (existingPath: Parameters<typeof nodeFs.link>[0], newPath: Parameters<typeof nodeFs.link>[1]) => { const result = await nodeFs.link(existingPath, newPath); if (/admission\.\d{16}\.retired$/u.test(String(newPath))) armed = true; return result; }, open: async (...args: Parameters<typeof open>) => { const handle = await open(...args); const path = String(args[0]); const flags = String(args[1]); return Object.assign(handle, { sync: async () => { if (armed && !failed && flags === "r" && path.endsWith("reservations")) { failed = true; throw Object.assign(new Error("retirement dir fsync fault"), { code: "EIO" }); } return Object.getPrototypeOf(handle).sync.call(handle); } }); } };
      const contender = await createOutbox({ host: "prime", homeDir, nodeId: "node-retirement-sync-proof", producerUuid: producerId(294), machineId: "machine-retirement-sync-proof", now: () => 1_001, fs }); await expect(contender.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000944")], policy: current })).rejects.toThrow(/retirement dir fsync fault/u);
      const names = await readdir(join(stale.root, "reservations")); expect(names).toContain("admission.0000000000000000.retired"); expect(names).not.toContain("admission.0000000000000001.lock");
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("does not let a previously retired reservation claim a later generation", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-retired-reservation-replay-")); let clock = Date.parse("2028-12-31T00:00:00.000Z");
    try {
      const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-retired-reservation-replay", producerUuid: producerId(295), machineId: "machine-retired-reservation-replay", now: () => clock }); const current = policy("2029-01-01T00:00:00.000Z"); const input = { episodes: [episode(current, "00000000-0000-5000-8000-000000000945")], policy: current };
      const accepted = await outbox.enqueue(input); clock = Date.parse("2029-01-02T00:00:00.000Z"); const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor: { process: vi.fn() }, now: () => clock, maxClockSkewMs: 0 }); expect(await delivery.deliver({})).toEqual({ delivered: 0, pending: 0, quarantined: 1 }); await expect(stat(accepted.file)).rejects.toMatchObject({ code: "ENOENT" });
      await expect(outbox.enqueue(input)).rejects.toThrow(/retired/u); expect(activeAdmissionLockNames(await readdir(join(outbox.root, "reservations")))).toEqual([]);
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("requires delivery directory fsync and strict reread before clearing ambiguous accepted proof", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-delivery-durable-proof-")); let armed = false; let failedCommitSync = false;
    const storeFs = { ...nodeFs, rename: async (from: Parameters<typeof nodeFs.rename>[0], to: Parameters<typeof nodeFs.rename>[1]) => { const result = await nodeFs.rename(from, to); if (String(to).includes("/jobs/") && String(to).endsWith(".json")) armed = true; return result; }, open: async (...args: Parameters<typeof open>) => { const handle = await open(...args); const path = String(args[0]); const flags = String(args[1]); return Object.assign(handle, { sync: async () => { if (armed && !failedCommitSync && flags === "r" && path.endsWith("/jobs")) { failedCommitSync = true; throw Object.assign(new Error("ambiguous jobs fsync"), { code: "EIO" }); } return Object.getPrototypeOf(handle).sync.call(handle); } }); } };
    try {
      const clock = Date.parse("2029-01-02T00:00:00.000Z"); const outbox = await createOutbox({ host: "prime", homeDir, nodeId: "node-delivery-durable-proof", producerUuid: producerId(120), machineId: "machine-delivery-durable-proof", now: () => clock, fs: storeFs }); const current = policy(); await expect(outbox.enqueue({ episodes: [episode(current)], policy: current })).rejects.toThrow(/ambiguous jobs fsync/u); const name = (await readdir(join(outbox.producerPath, "jobs"))).find((entry) => entry.endsWith(".json") && !entry.includes(".tmp-"))!; const jobFile = join(outbox.producerPath, "jobs", name); const events: string[] = []; let jobSyncSeen = false; let failAudit = true;
      const deliveryFs = { ...nodeFs, readFile: async (...args: Parameters<typeof nodeFs.readFile>) => { const result = await nodeFs.readFile(...args); if (String(args[0]) === jobFile && jobSyncSeen) events.push("post-sync-readback"); return result; }, rm: async (path: Parameters<typeof nodeFs.rm>[0], options?: Parameters<typeof nodeFs.rm>[1]) => { if (ADMISSION_LOCK_NAME.test(String(path).split(/[\/]/u).at(-1) ?? "")) events.push("proof-clear"); return options === undefined ? nodeFs.rm(path) : nodeFs.rm(path, options); }, rename: async (from: Parameters<typeof nodeFs.rename>[0], to: Parameters<typeof nodeFs.rename>[1]) => { if (failAudit && String(to).endsWith(".delivered.json")) { failAudit = false; throw Object.assign(new Error("delivered audit EIO"), { code: "EIO" }); } return nodeFs.rename(from, to); }, open: async (...args: Parameters<typeof open>) => { const handle = await open(...args); const path = String(args[0]); const flags = String(args[1]); const read = handle.readFile.bind(handle); return Object.assign(handle, { sync: async () => { if (flags === "r" && path.endsWith("/jobs")) { events.push("job-dir-fsync"); jobSyncSeen = true; } return Object.getPrototypeOf(handle).sync.call(handle); }, readFile: async (...readArgs: Parameters<typeof handle.readFile>) => { const value = await read(...readArgs); if (path === jobFile && jobSyncSeen) events.push("post-sync-readback"); return value; } }); } };
      const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor: { process: vi.fn().mockResolvedValue({ status: "delivered" }) }, now: () => clock, maxClockSkewMs: 0, fs: deliveryFs }); expect(await delivery.deliver({})).toEqual({ delivered: 0, pending: 1, quarantined: 0 }); const clear = events.indexOf("proof-clear"); expect(clear).toBeGreaterThan(-1); expect(events.slice(0, clear)).toContain("job-dir-fsync"); expect(events.slice(events.lastIndexOf("job-dir-fsync", clear - 1) + 1, clear)).toContain("post-sync-readback"); expect(activeReservationNames(await readdir(join(outbox.root, "reservations")))).toEqual([]); expect(await stat(jobFile)).toBeDefined();
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("retains fenced admission proof when the jobs directory disappears during durability sync", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-fenced-proof-disappears-")); let reached!: () => void; let resume!: () => void; const paused = new Promise<void>((resolve) => { reached = resolve; }); const gate = new Promise<void>((resolve) => { resume = resolve; }); let pause = true;
    const writerFs = { ...nodeFs, rename: async (from: Parameters<typeof nodeFs.rename>[0], to: Parameters<typeof nodeFs.rename>[1]) => { const result = await nodeFs.rename(from, to); if (pause && String(to).includes("/jobs/") && String(to).endsWith(".json")) { pause = false; reached(); await gate; } return result; } };
    try {
      const stale = await createOutbox({ host: "prime", homeDir, nodeId: "node-fenced-proof-disappears", producerUuid: producerId(121), machineId: "machine-fenced-proof-disappears", now: () => 1_000, fs: writerFs }); const current = policy(); const accepting = stale.enqueue({ episodes: [episode(current)], policy: current }); const acceptingResult = accepting.then(() => true, () => false); await paused; const currentProducer = await createOutbox({ host: "prime", homeDir, nodeId: "node-fenced-proof-disappears", producerUuid: producerId(122), machineId: "machine-fenced-proof-disappears", now: () => 100_000 }); let removed = false;
      const recoveryFs = { ...nodeFs, open: async (...args: Parameters<typeof open>) => { const handle = await open(...args); const path = String(args[0]); const flags = String(args[1]); return Object.assign(handle, { sync: async () => { if (!removed && flags === "r" && path.split(/[\\/]/u).at(-1)?.startsWith("jobs.fenced-")) { removed = true; await nodeFs.rm(path, { recursive: true, force: true }); } return Object.getPrototypeOf(handle).sync.call(handle); } }); } }; const delivery = createOutboxDelivery({ outboxRoot: stale.root, producerPath: currentProducer.producerPath, processor: { process: vi.fn() }, now: () => 100_000, maxClockSkewMs: 0, heartbeatTimeoutMs: 100, fs: recoveryFs }); await expect(delivery.adopt(stale.producerPath)).rejects.toThrow(); expect(await readdir(join(stale.root, "reservations"))).toEqual(expect.arrayContaining([expect.stringMatching(ADMISSION_LOCK_NAME)])); resume(); expect(await acceptingResult).toBe(false);
    } finally { resume?.(); await rm(homeDir, { recursive: true, force: true }); }
  });

  it("keeps a late closed reservation until fenced recovery durably proves abort", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-late-abort-proof-"));
    try {
      const stale = await createOutbox({ host: "prime", homeDir, nodeId: "node-late-abort-proof", producerUuid: producerId(123), machineId: "machine-late-abort-proof", now: () => 1_000 }); const currentProducer = await createOutbox({ host: "prime", homeDir, nodeId: "node-late-abort-proof", producerUuid: producerId(124), machineId: "machine-late-abort-proof", now: () => 100_000 }); await stale.closeProducer(); const delivery = createOutboxDelivery({ outboxRoot: stale.root, producerPath: currentProducer.producerPath, processor: { process: vi.fn() }, now: () => 100_000, maxClockSkewMs: 0, heartbeatTimeoutMs: 100 }); await delivery.adopt(stale.producerPath); const jobId = "00000000-0000-5000-8000-000000000123"; const reservationId = deterministicUuid("pi-qdrant-memory-v2:outbox-reservation", "node-late-abort-proof", producerId(123), jobId); const pending: Record<string, unknown> = { version: 1, reservationId, jobId, jobAuditHash: sha256Hex("late-job"), policyId: policy().id, deadline: policy().expiresAt, nodeId: "node-late-abort-proof", producerUuid: producerId(123), requestedBytes: 777, auditHash: "" }; const withoutAudit = { ...pending }; delete withoutAudit.auditHash; pending.auditHash = sha256Hex(canonicalStringify(withoutAudit)); const reservations = join(stale.root, "reservations"); await nodeFs.writeFile(join(reservations, `${reservationId}.json`), canonicalStringify(pending), { mode: 0o600 }); await nodeFs.writeFile(join(reservations, "admission.0000000000000000.lock"), canonicalStringify(pending), { mode: 0o600 }); const handle = await open(reservations, "r"); await handle.sync(); await handle.close(); expect(await readdir(reservations)).toEqual(expect.arrayContaining([expect.stringMatching(ADMISSION_LOCK_NAME), `${reservationId}.json`])); await delivery.adopt(stale.producerPath); expect(activeReservationNames(await readdir(reservations))).toEqual([]); const audit = JSON.parse(await readFile(join(stale.producerPath, "quarantine", `precommit-${reservationId}.json`), "utf8")) as Record<string, unknown>; expect(audit).toMatchObject({ kind: "aborted_precommit", reservationId, jobId, jobAuditHash: pending.jobAuditHash, policyId: pending.policyId, deadline: pending.deadline, nodeId: pending.nodeId, producerUuid: pending.producerUuid, sourceHash: null, byteLength: 0 }); expect(audit.fenceAuditHash).toMatch(/^[a-f0-9]{64}$/u);
    } finally { await rm(homeDir, { recursive: true, force: true }); }
  });

  it("retains admission proof across public and processor quarantine publication fsync faults", async () => {
    for (const mode of ["public", "processor"] as const) {
      const homeDir = await mkdtemp(join(tmpdir(), `task5-quarantine-proof-${mode}-`)); let failAdmission = true; let armQuarantine = false; let quarantineSyncs = 0;
      const faultFs = { ...nodeFs, rm: async (path: Parameters<typeof nodeFs.rm>[0], options?: Parameters<typeof nodeFs.rm>[1]) => { if (failAdmission && ADMISSION_LOCK_NAME.test(String(path).split(/[\/]/u).at(-1) ?? "")) { failAdmission = false; throw Object.assign(new Error("retain proof"), { code: "EIO" }); } return options === undefined ? nodeFs.rm(path) : nodeFs.rm(path, options); }, open: async (...args: Parameters<typeof open>) => { const handle = await open(...args); const path = String(args[0]); const flags = String(args[1]); return Object.assign(handle, { sync: async () => { if (armQuarantine && flags === "r" && path.endsWith("/quarantine")) { quarantineSyncs += 1; if (quarantineSyncs === 2) throw Object.assign(new Error("quarantine publication EIO"), { code: "EIO" }); } return Object.getPrototypeOf(handle).sync.call(handle); } }); } };
      try {
        const clock = Date.parse("2029-01-02T00:00:00.000Z"); const outbox = await createOutbox({ host: "prime", homeDir, nodeId: `node-quarantine-proof-${mode}`, producerUuid: producerId(mode === "public" ? 125 : 126), machineId: `machine-quarantine-proof-${mode}`, now: () => clock, fs: faultFs }); const current = policy(); await expect(outbox.enqueue({ episodes: [episode(current)], policy: current })).rejects.toThrow(/retain proof/u); const name = (await readdir(join(outbox.producerPath, "jobs"))).find((entry) => entry.endsWith(".json") && !entry.includes(".tmp-"))!; const file = join(outbox.producerPath, "jobs", name); const job = parseOutboxJob(JSON.parse(await readFile(file, "utf8")), { host: "prime", nodeId: `node-quarantine-proof-${mode}`, producerUuid: producerId(mode === "public" ? 125 : 126), homeDir }); armQuarantine = true;
        if (mode === "public") await expect(outbox.quarantine(job, "proof-test")).rejects.toThrow(/publication/u); else { const delivery = createOutboxDelivery({ outboxRoot: outbox.root, producerPath: outbox.producerPath, processor: { process: vi.fn().mockResolvedValue({ status: "quarantined", category: "proof-test" }) }, now: () => clock, maxClockSkewMs: 0, fs: faultFs }); expect(await delivery.deliver({})).toEqual({ delivered: 0, pending: 1, quarantined: 0 }); }
        const proofNames = await readdir(join(outbox.root, "reservations")); expect(proofNames).toEqual(expect.arrayContaining([expect.stringMatching(ADMISSION_RETIREMENT_NAME)])); expect(activeAdmissionLockNames(proofNames)).toEqual([]); expect(await stat(join(outbox.producerPath, "quarantine", name))).toBeDefined(); expect(await stat(file)).toBeDefined();
      } finally { await rm(homeDir, { recursive: true, force: true }); }
    }
  });


  it("never upgrades a durable partial-precommit decision after its temp later completes", async () => {
    const homeDir = await mkdtemp(join(tmpdir(), "task5-partial-terminal-decision-")); let reached!: () => void; let resume!: () => void; const partial = new Promise<void>((resolve) => { reached = resolve; }); const gate = new Promise<void>((resolve) => { resume = resolve; }); let pause = true;
    const writerFs = { ...nodeFs, open: async (...args: Parameters<typeof open>) => { const handle = await open(...args); const path = String(args[0]); if (pause && path.includes("/jobs/") && path.includes(".json.tmp-")) { pause = false; return Object.assign(handle, { writeFile: async (data: string | Uint8Array) => { const bytes = Buffer.from(data); const cut = Math.floor(bytes.length / 2); await handle.write(bytes.subarray(0, cut), 0, cut, 0); reached(); await gate; await handle.write(bytes.subarray(cut), 0, bytes.length - cut, cut); } }); } return handle; } };
    try {
      const stale = await createOutbox({ host: "prime", homeDir, nodeId: "node-partial-terminal-decision", producerUuid: producerId(127), machineId: "machine-partial-terminal-decision", now: () => 1_000, fs: writerFs }); const currentProducer = await createOutbox({ host: "prime", homeDir, nodeId: "node-partial-terminal-decision", producerUuid: producerId(128), machineId: "machine-partial-terminal-decision", now: () => 100_000 }); const current = policy(); const accepting = stale.enqueue({ episodes: [episode(current, "00000000-0000-5000-8000-000000000127")], policy: current }); const acceptedResult = accepting.then(() => true, () => false); await partial; const reservation = await readAdmissionLock(join(stale.root, "reservations")); let failTempRemove = true; const recoveryFs = { ...nodeFs, rm: async (path: Parameters<typeof nodeFs.rm>[0], options?: Parameters<typeof nodeFs.rm>[1]) => { if (failTempRemove && String(path).includes("/jobs.fenced-") && String(path).includes(".json.tmp-")) { failTempRemove = false; throw Object.assign(new Error("partial temp unlink EIO"), { code: "EIO" }); } return options === undefined ? nodeFs.rm(path) : nodeFs.rm(path, options); } }; const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const first = createOutboxDelivery({ outboxRoot: stale.root, producerPath: currentProducer.producerPath, processor, now: () => 100_000, maxClockSkewMs: 0, heartbeatTimeoutMs: 100, fs: recoveryFs }); await expect(first.adopt(stale.producerPath)).rejects.toThrow(/partial temp unlink/u); const auditFile = join(stale.producerPath, "quarantine", `precommit-${reservation.reservationId}.json`); const auditBefore = await readFile(auditFile, "utf8"); expect(auditBefore).toContain('"kind":"partial_precommit"'); resume(); expect(await acceptedResult).toBe(false); const second = createOutboxDelivery({ outboxRoot: stale.root, producerPath: currentProducer.producerPath, processor, now: () => 100_000, maxClockSkewMs: 0, heartbeatTimeoutMs: 100 }); await second.adopt(stale.producerPath); expect(await readFile(auditFile, "utf8")).toBe(auditBefore); const fenced = (await readdir(stale.producerPath)).find((name) => name.startsWith("jobs.fenced-"))!; expect((await readdir(join(stale.producerPath, fenced))).filter((name) => name.includes(reservation.jobId))).toEqual([]); await expect(stat(join(stale.producerPath, "quarantine", `${reservation.jobId}.json`))).rejects.toMatchObject({ code: "ENOENT" }); expect(activeReservationNames(await readdir(join(stale.root, "reservations")))).toEqual([]); expect(processor.process).not.toHaveBeenCalled();
    } finally { resume?.(); await rm(homeDir, { recursive: true, force: true }); }
  });


  it("uses one no-clobber precommit decision in delayed-partial versus complete adopter races", async () => {
    for (const winner of ["accepted", "partial"] as const) {
      const homeDir = await mkdtemp(join(tmpdir(), `task5-precommit-decision-race-${winner}-`)); let writerReached!: () => void; let resumeWriter!: () => void; const writerPartial = new Promise<void>((resolve) => { writerReached = resolve; }); const writerGate = new Promise<void>((resolve) => { resumeWriter = resolve; }); let pauseWriter = true; let linkReached!: () => void; let releaseLink!: () => void; const partialLink = new Promise<void>((resolve) => { linkReached = resolve; }); const linkGate = new Promise<void>((resolve) => { releaseLink = resolve; }); let rmReached!: () => void; let releaseRm!: () => void; const partialRm = new Promise<void>((resolve) => { rmReached = resolve; }); const rmGate = new Promise<void>((resolve) => { releaseRm = resolve; }); let blockPartialLink = winner === "accepted"; let blockPartialRm = winner === "partial"; const decisionKinds: string[] = [];
      const writerFs = { ...nodeFs, open: async (...args: Parameters<typeof open>) => { const handle = await open(...args); const path = String(args[0]); if (pauseWriter && path.includes("/jobs/") && path.includes(".json.tmp-")) { pauseWriter = false; return Object.assign(handle, { writeFile: async (data: string | Uint8Array) => { const bytes = Buffer.from(data); const cut = Math.floor(bytes.length / 2); await handle.write(bytes.subarray(0, cut), 0, cut, 0); writerReached(); await writerGate; await handle.write(bytes.subarray(cut), 0, bytes.length - cut, cut); } }); } return handle; } };
      try {
        const index = winner === "accepted" ? 129 : 131; const stale = await createOutbox({ host: "prime", homeDir, nodeId: `node-precommit-decision-race-${winner}`, producerUuid: producerId(index), machineId: `machine-precommit-decision-race-${winner}`, now: () => 1_000, fs: writerFs }); const currentProducer = await createOutbox({ host: "prime", homeDir, nodeId: `node-precommit-decision-race-${winner}`, producerUuid: producerId(index + 1), machineId: `machine-precommit-decision-race-${winner}`, now: () => 100_000 }); const current = policy(); const accepting = stale.enqueue({ episodes: [episode(current, winner === "accepted" ? "00000000-0000-5000-8000-000000000129" : "00000000-0000-5000-8000-000000000131")], policy: current }); const acceptedResult = accepting.then(() => true, () => false); await writerPartial; const reservation = await readAdmissionLock(join(stale.root, "reservations")); const decisionFile = join(stale.producerPath, "quarantine", `precommit-${reservation.reservationId}.json`);
        const raceFs = { ...nodeFs, link: async (from: Parameters<typeof nodeFs.link>[0], to: Parameters<typeof nodeFs.link>[1]) => { if (String(to) === decisionFile) { const kind = (JSON.parse(await readFile(String(from), "utf8")) as { kind: string }).kind; if (kind === "partial_precommit" && blockPartialLink) { blockPartialLink = false; linkReached(); await linkGate; } try { await nodeFs.link(from, to); decisionKinds.push(kind); return; } catch (error) { throw error; } } return nodeFs.link(from, to); }, rm: async (path: Parameters<typeof nodeFs.rm>[0], options?: Parameters<typeof nodeFs.rm>[1]) => { if (blockPartialRm && String(path).includes("/jobs.fenced-") && String(path).includes(".json.tmp-")) { blockPartialRm = false; rmReached(); await rmGate; } return options === undefined ? nodeFs.rm(path) : nodeFs.rm(path, options); } }; const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const first = createOutboxDelivery({ outboxRoot: stale.root, producerPath: currentProducer.producerPath, processor, now: () => 100_000, maxClockSkewMs: 0, heartbeatTimeoutMs: 100, fs: raceFs }); const firstAdopt = first.adopt(stale.producerPath); const firstResult = firstAdopt.then(() => true, () => false);
        if (winner === "accepted") { await partialLink; resumeWriter(); expect(await acceptedResult).toBe(false); const second = createOutboxDelivery({ outboxRoot: stale.root, producerPath: currentProducer.producerPath, processor, now: () => 100_000, maxClockSkewMs: 0, heartbeatTimeoutMs: 100, fs: raceFs }); await second.adopt(stale.producerPath); releaseLink(); expect(await firstResult).toBe(false); expect(decisionKinds).toEqual(["accepted_precommit"]); expect((JSON.parse(await readFile(decisionFile, "utf8")) as { kind: string }).kind).toBe("accepted_precommit"); expect(await second.deliver({})).toEqual({ delivered: 1, pending: 0, quarantined: 0 }); expect(processor.process).toHaveBeenCalledTimes(1); }
        else { await partialRm; resumeWriter(); expect(await acceptedResult).toBe(false); const second = createOutboxDelivery({ outboxRoot: stale.root, producerPath: currentProducer.producerPath, processor, now: () => 100_000, maxClockSkewMs: 0, heartbeatTimeoutMs: 100, fs: raceFs }); await second.adopt(stale.producerPath); releaseRm(); expect(await firstResult).toBe(true); expect(decisionKinds).toEqual(["partial_precommit"]); expect((JSON.parse(await readFile(decisionFile, "utf8")) as { kind: string }).kind).toBe("partial_precommit"); expect(await second.deliver({})).toEqual({ delivered: 0, pending: 0, quarantined: 0 }); expect(processor.process).not.toHaveBeenCalled(); }
        const fenced = (await readdir(stale.producerPath)).find((name) => name.startsWith("jobs.fenced-"))!; expect((await readdir(join(stale.producerPath, fenced))).some((name) => name.includes(".json.tmp-"))).toBe(false); expect(activeReservationNames(await readdir(join(stale.root, "reservations")))).toEqual([]);
      } finally { resumeWriter?.(); releaseLink?.(); releaseRm?.(); await rm(homeDir, { recursive: true, force: true }); }
    }
  }, 15_000);


  it("publishes accepted decisions only after the exact temp source is file-and-directory durable", async () => {
    for (const fault of ["file-sync", "directory-sync", "after-decision"] as const) {
      const homeDir = await mkdtemp(join(tmpdir(), `task5-accepted-decision-durability-${fault}-`)); let reached!: () => void; let resume!: () => void; const written = new Promise<void>((resolve) => { reached = resolve; }); const gate = new Promise<void>((resolve) => { resume = resolve; }); let pause = true;
      const writerFs = { ...nodeFs, open: async (...args: Parameters<typeof open>) => { const handle = await open(...args); const path = String(args[0]); if (pause && path.includes("/jobs/") && path.includes(".json.tmp-")) { pause = false; const write = handle.writeFile.bind(handle); return Object.assign(handle, { writeFile: async (...writeArgs: Parameters<typeof handle.writeFile>) => { await write(...writeArgs); reached(); await gate; } }); } return handle; } };
      try {
        const index = fault === "file-sync" ? 133 : fault === "directory-sync" ? 135 : 137; const stale = await createOutbox({ host: "prime", homeDir, nodeId: `node-accepted-decision-durability-${fault}`, producerUuid: producerId(index), machineId: `machine-accepted-decision-durability-${fault}`, now: () => 1_000, fs: writerFs }); const currentProducer = await createOutbox({ host: "prime", homeDir, nodeId: `node-accepted-decision-durability-${fault}`, producerUuid: producerId(index + 1), machineId: `machine-accepted-decision-durability-${fault}`, now: () => 100_000 }); const current = policy(); const accepting = stale.enqueue({ episodes: [episode(current, `00000000-0000-5000-8000-${index.toString(16).padStart(12, "0")}`)], policy: current }); const acceptingResult = accepting.then(() => true, () => false); await written; const reservation = await readAdmissionLock(join(stale.root, "reservations")); const decisionFile = join(stale.producerPath, "quarantine", `precommit-${reservation.reservationId}.json`); const events: string[] = []; let failed = false;
        const recoveryFs = { ...nodeFs, open: async (...args: Parameters<typeof open>) => { const handle = await open(...args); const path = String(args[0]); const flags = String(args[1]); const sync = handle.sync.bind(handle); if (path.includes("/jobs.fenced-") && path.includes(".json.tmp-")) return Object.assign(handle, { sync: async () => { events.push("file-sync"); if (!failed && fault === "file-sync") { failed = true; throw Object.assign(new Error("accepted source file sync EIO"), { code: "EIO" }); } return sync(); } }); if (path.split(/[\\/]/u).at(-1)?.startsWith("jobs.fenced-") && flags === "r") return Object.assign(handle, { sync: async () => { events.push("directory-sync"); if (!failed && fault === "directory-sync") { failed = true; throw Object.assign(new Error("accepted source directory sync EIO"), { code: "EIO" }); } return sync(); } }); return handle; }, link: async (from: Parameters<typeof nodeFs.link>[0], to: Parameters<typeof nodeFs.link>[1]) => { if (String(to) === decisionFile) { events.push("decision-link"); return nodeFs.link(from, to); } if (String(from).includes("/jobs.fenced-") && String(from).includes(".json.tmp-") && String(to).endsWith(`${reservation.jobId}.json`)) { events.push("canonical-link"); if (!failed && fault === "after-decision") { failed = true; throw Object.assign(new Error("post-decision canonical link EIO"), { code: "EIO" }); } } return nodeFs.link(from, to); } }; const processor: OutboxJobProcessor = { process: vi.fn().mockResolvedValue({ status: "delivered" }) }; const first = createOutboxDelivery({ outboxRoot: stale.root, producerPath: currentProducer.producerPath, processor, now: () => 100_000, maxClockSkewMs: 0, heartbeatTimeoutMs: 100, fs: recoveryFs }); await expect(first.adopt(stale.producerPath)).rejects.toThrow(/accepted source|post-decision/u); const fenced = (await readdir(stale.producerPath)).find((name) => name.startsWith("jobs.fenced-"))!; const tempName = (await readdir(join(stale.producerPath, fenced))).find((name) => name.includes(".json.tmp-"))!; expect(tempName).toBeDefined(); expect(await readdir(join(stale.root, "reservations"))).toEqual(expect.arrayContaining([expect.stringMatching(ADMISSION_LOCK_NAME)]));
        if (fault === "after-decision") { const decision = JSON.parse(await readFile(decisionFile, "utf8")) as { kind: string; sourceHash: string; byteLength: number }; const source = await readFile(join(stale.producerPath, fenced, tempName)); expect(decision).toMatchObject({ kind: "accepted_precommit", sourceHash: sha256Hex(source), byteLength: source.length }); expect(events.indexOf("file-sync")).toBeLessThan(events.indexOf("directory-sync")); expect(events.indexOf("directory-sync")).toBeLessThan(events.indexOf("decision-link")); expect(events.indexOf("decision-link")).toBeLessThan(events.indexOf("canonical-link")); } else await expect(stat(decisionFile)).rejects.toMatchObject({ code: "ENOENT" });
        resume(); expect(await acceptingResult).toBe(false); const retry = createOutboxDelivery({ outboxRoot: stale.root, producerPath: currentProducer.producerPath, processor, now: () => 100_000, maxClockSkewMs: 0, heartbeatTimeoutMs: 100 }); await retry.adopt(stale.producerPath); expect((JSON.parse(await readFile(decisionFile, "utf8")) as { kind: string }).kind).toBe("accepted_precommit"); expect(await stat(join(stale.producerPath, fenced, `${reservation.jobId}.json`))).toBeDefined(); expect(activeReservationNames(await readdir(join(stale.root, "reservations")))).toEqual([]); expect(processor.process).not.toHaveBeenCalled();
      } finally { resume?.(); await rm(homeDir, { recursive: true, force: true }); }
    }
  }, 15_000);


  it("recovers prepared-only admission artifacts after fencing and restores capacity", async () => {
    for (const stage of ["published", "temp-complete", "temp-partial"] as const) {
      const homeDir = await mkdtemp(join(tmpdir(), `task5-prepared-only-${stage}-`)); let reached!: () => void; let resume!: () => void; const paused = new Promise<void>((resolve) => { reached = resolve; }); const gate = new Promise<void>((resolve) => { resume = resolve; }); let pause = true;
      const writerFs = { ...nodeFs, link: async (from: Parameters<typeof nodeFs.link>[0], to: Parameters<typeof nodeFs.link>[1]) => { if (pause && stage === "published" && String(from).split(/[\\/]/u).at(-1)?.startsWith("prepare-") && String(to).includes("/reservations/") && String(to).endsWith(".json")) { pause = false; reached(); await gate; } return nodeFs.link(from, to); }, rename: async (from: Parameters<typeof nodeFs.rename>[0], to: Parameters<typeof nodeFs.rename>[1]) => { if (pause && stage === "temp-complete" && String(from).includes("/reservations/prepare-") && String(from).includes(".tmp-") && String(to).split(/[\\/]/u).at(-1)?.startsWith("prepare-")) { pause = false; reached(); await gate; } return nodeFs.rename(from, to); }, open: async (...args: Parameters<typeof open>) => { const handle = await open(...args); const path = String(args[0]); if (pause && stage === "temp-partial" && path.includes("/reservations/prepare-") && path.includes(".tmp-")) { pause = false; return Object.assign(handle, { writeFile: async (data: string | Uint8Array) => { const bytes = Buffer.from(data); const cut = Math.floor(bytes.length / 2); await handle.write(bytes.subarray(0, cut), 0, cut, 0); reached(); await gate; await handle.write(bytes.subarray(cut), 0, bytes.length - cut, cut); } }); } return handle; } };
      try {
        const index = stage === "published" ? 139 : stage === "temp-complete" ? 141 : 143; const stale = await createOutbox({ host: "prime", homeDir, nodeId: `node-prepared-only-${stage}`, producerUuid: producerId(index), machineId: `machine-prepared-only-${stage}`, now: () => 1_000, maxJobs: 1, fs: writerFs }); const currentProducer = await createOutbox({ host: "prime", homeDir, nodeId: `node-prepared-only-${stage}`, producerUuid: producerId(index + 1), machineId: `machine-prepared-only-${stage}`, now: () => 100_000, maxJobs: 1 }); const current = policy(); const accepting = stale.enqueue({ episodes: [episode(current, `00000000-0000-5000-8000-${index.toString(16).padStart(12, "0")}`)], policy: current }); const acceptingResult = accepting.then(() => true, () => false); await paused; const before = (await readdir(join(stale.root, "reservations"))).filter((name) => name.startsWith("prepare-")); expect(before).toHaveLength(1); const processor: OutboxJobProcessor = { process: vi.fn() }; const delivery = createOutboxDelivery({ outboxRoot: stale.root, producerPath: currentProducer.producerPath, processor, now: () => 100_000, maxClockSkewMs: 0, heartbeatTimeoutMs: 100 }); await delivery.adopt(stale.producerPath); await delivery.adopt(stale.producerPath); expect((await readdir(join(stale.root, "reservations"))).filter((name) => name.startsWith("prepare-"))).toEqual([]); const audits = await Promise.all((await readdir(join(stale.producerPath, "quarantine"))).map((name) => readFile(join(stale.producerPath, "quarantine", name), "utf8"))); expect(audits.join("\n")).toContain(stage === "temp-partial" ? "orphan_prepared_precommit" : "aborted_precommit"); expect(audits.join("\n")).not.toContain("already [token redacted]"); const accepted = await currentProducer.enqueue({ episodes: [episode(current, `00000000-0000-5000-8000-${(index + 1).toString(16).padStart(12, "0")}`)], policy: current }); expect(await stat(accepted.file)).toBeDefined(); resume(); expect(await acceptingResult).toBe(false); expect(processor.process).not.toHaveBeenCalled();
      } finally { resume?.(); await rm(homeDir, { recursive: true, force: true }); }
    }
  }, 15_000);

});
