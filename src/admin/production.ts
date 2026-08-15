import { link, lstat, mkdir, readFile, unlink, writeFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { dirname, join } from "node:path";
import { canonicalStringify } from "../domain/canonical.js";
import { createTombstone } from "../coordination/tombstones.js";
import { createQdrantCoordinationStore, jobIdFor, type ProductionCoordinationStore } from "../qdrant/write.js";
import type { QdrantClientOptions } from "../qdrant/client.js";
import { loadAdminProcessSecrets } from "./secrets.js";
import { readQdrantCurrentSelection, type ForgetDependencies, type ForgetPlan } from "./forget.js";
import { revokePrivacy, type PrivacyRevokeDependencies, type PrivacyRevokePlan } from "./privacy.js";
import type { RuntimeConfig } from "../types.js";
import type { ControlRecord, EpisodeRecord } from "../domain/records.js";
import { AdminPlanError } from "./errors.js";
import { intersectPolicies, type ProcessingPolicy } from "../domain/policy.js";

const PLAN_ID = /^[a-f0-9]{64}$/u;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/iu;

export interface StoredPlanDependencies {
  readTextFile?(path: string): Promise<string>;
  writeTextFile?(path: string, text: string): Promise<void>;
}
export interface StoredPlan {
  save(kind: "privacy" | "forget", plan: PrivacyRevokePlan | ForgetPlan): Promise<void>;
  load<T extends PrivacyRevokePlan | ForgetPlan>(kind: "privacy" | "forget", id: string): Promise<T>;
}
function planPath(config: RuntimeConfig, kind: "privacy" | "forget", id: string): string {
  if (!PLAN_ID.test(id)) throw new TypeError("Plan ID is invalid");
  return join(dirname(config.configPath), "plans", `${kind}-${id}.json`);
}
function sameLogicalPlan(left: unknown, right: PrivacyRevokePlan | ForgetPlan): boolean {
  if (typeof left !== "object" || left === null || Array.isArray(left)) return false;
  const { requestedAt: _leftRequestedAt, ...leftStable } = left as Record<string, unknown>;
  const { requestedAt: _rightRequestedAt, ...rightStable } = right;
  return canonicalStringify(leftStable) === canonicalStringify(rightStable);
}
/** XDG-local immutable plan store. Existing files are never replaced and all
 * symlinked plan paths fail closed. */
export function createStoredPlan(config: RuntimeConfig, dependencies: StoredPlanDependencies = {}): StoredPlan {
  const read = dependencies.readTextFile ?? ((path: string) => readFile(path, "utf8"));
  const write = dependencies.writeTextFile;
  return {
    async save(kind, plan) {
      const id = plan.id;
      const path = planPath(config, kind, id);
      const text = `${canonicalStringify(plan)}\n`;
      await mkdir(dirname(path), { recursive: true, mode: 0o700 });
      const acceptExisting = async (): Promise<boolean> => {
        const stat = await lstat(path);
        if (stat.isSymbolicLink() || !stat.isFile() || stat.size > 512 * 1024) throw new Error("Plan path is not a regular file");
        let existing: unknown;
        try { existing = JSON.parse(await read(path)); } catch { throw new Error("Plan file is invalid"); }
        if (!sameLogicalPlan(existing, plan)) throw new Error("Plan ID collision or immutable plan mismatch");
        return true;
      };
      try { if (await acceptExisting()) return; }
      catch (error: unknown) { if (!(typeof error === "object" && error !== null && (error as { code?: string }).code === "ENOENT")) throw error; }
      if (write !== undefined) { await write(path, text); return; }
      const temp = `${path}.${process.pid}.${randomUUID()}.tmp`;
      await writeFile(temp, text, { encoding: "utf8", mode: 0o600, flag: "wx" });
      try {
        await link(temp, path);
      } catch (error: unknown) {
        if (!(typeof error === "object" && error !== null && (error as { code?: string }).code === "EEXIST")) throw error;
        await acceptExisting();
      } finally {
        await unlink(temp).catch(() => undefined);
      }
    },
    async load<T extends PrivacyRevokePlan | ForgetPlan>(kind: "privacy" | "forget", id: string): Promise<T> {
      const path = planPath(config, kind, id);
      const stat = await lstat(path);
      if (stat.isSymbolicLink() || !stat.isFile() || stat.size > 512 * 1024) throw new Error("Plan file is invalid");
      const parsed: unknown = JSON.parse(await read(path));
      if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed) || (parsed as { id?: unknown }).id !== id) throw new Error("Plan file does not match requested ID");
      return parsed as T;
    },
  };
}

export function operatorQdrantOptions(config: RuntimeConfig, env: Record<string, string | undefined>): QdrantClientOptions {
  const adminKey = loadAdminProcessSecrets(env).destinationApiKey;
  if (adminKey === undefined) throw new Error("Human Qdrant admin key is required for this operation");
  return { baseUrl: config.qdrant.url, collection: config.qdrant.collection, ownerHost: config.host, apiKey: adminKey, timeoutMs: config.retrieval.timeoutMs, readConsistency: config.coordination.readConsistency, maxClockSkewMs: config.coordination.maxClockSkewMs };
}
export function productionStore(config: RuntimeConfig, env: Record<string, string | undefined>): ProductionCoordinationStore {
  return createQdrantCoordinationStore(operatorQdrantOptions(config, env));
}

export function productionPrivacyDependencies(config: RuntimeConfig, env: Record<string, string | undefined>, reconcile: () => Promise<void>): { store: ProductionCoordinationStore; deps: PrivacyRevokeDependencies } {
  const store = productionStore(config, env);
  const deps: PrivacyRevokeDependencies = {
    readControl: () => store.readControl(),
    beginDrain: input => store.beginPolicyDrain(input),
    waitForQuiescence: input => store.waitForOldLeasesToQuiesce({ retiredEpoch: input.retiredEpoch, maxLeaseMs: config.coordination.leaseMs, maxClockSkewMs: config.coordination.maxClockSkewMs, timeoutMs: Math.min(120000, config.memoryModel.timeoutMs + config.coordination.leaseMs), ...(input.signal === undefined ? {} : { signal: input.signal }) }),
    beginForgetBarrier: input => store.beginForgetBarrier({ now: input.now, ...(input.revokedDestinationIds === undefined ? {} : { revokedDestinationIds: input.revokedDestinationIds }) }),
    rereadControl: () => store.readControl(),
    invalidateGeneration: async () => undefined,
    reconcile: async () => {
      // Policy/forget barriers intentionally leave control draining until the
      // normal lifecycle reactivates it; never enqueue work a worker cannot
      // claim under that state.
      if ((await store.readControl()).state === "active") await reconcile();
    },
  };
  return { store, deps };
}

export function productionForgetDependencies(config: RuntimeConfig, env: Record<string, string | undefined>, reconcile: () => Promise<void>): { store: ProductionCoordinationStore; deps: ForgetDependencies } {
  const store = productionStore(config, env);
  const deps: ForgetDependencies = {
    readControl: () => store.readControl(),
    resolveCurrent: id => readQdrantCurrentSelection(config, id),
    createTombstones: async input => {
      const control = await store.readControl();
      if (input.privacyEpoch !== undefined && input.privacyEpoch !== control.privacyEpoch) throw new AdminPlanError("Forget plan privacy epoch is stale");
      const createdAt = new Date().toISOString();
      for (const targetId of input.targetIds) {
        await createTombstone(store, { ownerHost: config.host, scope: input.scope, targetId, ...(input.scope === "occurrence" && UUID.test(targetId) ? { targetKind: "episode" as const } : {}), provenanceIds: [...input.provenanceIds], createdAt, privacyEpoch: control.privacyEpoch, processingPolicyId: control.processingPolicyId });
      }
    },
    readTombstones: targetIds => store.readTombstones(targetIds),
    beginForgetBarrier: input => store.beginForgetBarrier({ now: input.now }),
    invalidateCurrentViews: async () => undefined,
    invalidateCoverage: async () => undefined,
    reconcile: async () => {
      if ((await store.readControl()).state === "active") await reconcile();
    },
    rereadBarrier: () => store.readControl(),
  };
  return { store, deps };
}

export interface ProductionOperationRequest { command: "curate" | "raptor" | "reconcile"; action: "enqueue" | "wait"; jobId?: string; }
const ADMIN_COMMANDS = new Set<ProductionOperationRequest["command"]>(["curate", "raptor", "reconcile"]);
const MAX_ADMIN_EPISODES = 65_536;
const MAX_ADMIN_PAGES = Math.ceil(MAX_ADMIN_EPISODES / 256);
async function pause(ms: number): Promise<void> { await new Promise(resolve => setTimeout(resolve, ms)); }
async function allAdminEpisodeIds(store: ProductionCoordinationStore, control: ControlRecord, max: number): Promise<string[]> {
  const ids: string[] = []; const seen = new Set<string>(); const cursors = new Set<string>(); let offset: string | undefined;
  for (let page = 0; page < MAX_ADMIN_PAGES; page += 1) {
    const slice = await store.scrollEpisodeIds(offset, Math.min(256, max - ids.length), control.privacyEpoch);
    if (!Array.isArray(slice.episodeIds) || slice.episodeIds.length > max - ids.length || slice.episodeIds.some(id => typeof id !== "string" || id.length === 0 || seen.has(id))) throw new Error("Episode discovery is ambiguous");
    for (const id of slice.episodeIds) { seen.add(id); ids.push(id); }
    if (slice.nextOffset === undefined) return ids.sort();
    if (slice.episodeIds.length === 0 || cursors.has(slice.nextOffset)) throw new Error("Episode discovery cursor is invalid");
    cursors.add(slice.nextOffset); offset = slice.nextOffset;
  }
  throw new Error("Episode discovery exceeded bounded pages");
}
async function readAdminEpisodes(store: ProductionCoordinationStore, ids: readonly string[], privacyEpoch: number): Promise<EpisodeRecord[]> {
  const episodes: EpisodeRecord[] = [];
  for (let index = 0; index < ids.length; index += 1024) episodes.push(...await store.readEpisodes(ids.slice(index, index + 1024), privacyEpoch));
  const byId = new Map(episodes.map(episode => [episode.id, episode]));
  if (byId.size !== ids.length || ids.some(id => byId.get(id)?.id !== id)) throw new Error("Episode discovery readback is incomplete");
  return ids.map(id => byId.get(id)!);
}
type AdminJobInput = Parameters<ProductionCoordinationStore["createJob"]>[0];
async function enqueueAdminJob(store: ProductionCoordinationStore, input: AdminJobInput): Promise<Awaited<ReturnType<ProductionCoordinationStore["createJob"]>>> {
  const id = jobIdFor(input);
  const existing = await store.readJob(id);
  if (existing !== null) {
    if (existing.ownerHost !== input.ownerHost || canonicalStringify(existing.membership) !== canonicalStringify(input.membership) || existing.policyId !== input.policyIntersectionId || existing.policyHash !== input.policyHash || existing.policyEpoch !== input.policyEpoch || existing.extractorRevision !== input.extractorRevision || existing.privacyEpoch !== input.privacyEpoch || existing.createdAt !== input.createdAt || existing.expiresAt !== input.expiresAt) throw new Error("Admin job identity collision");
    return existing;
  }
  return store.createJob(input);
}
/** Enqueue/wait uses only named immutable-job and lease reads; it never
 * manufactures a worker/lease authority in the human process. Curation and
 * reconcile use the normal curation worker identity; only RAPTOR has a
 * dedicated admin extractor consumed by the lifecycle drain. */
export async function productionOperation(config: RuntimeConfig, env: Record<string, string | undefined>, request: ProductionOperationRequest): Promise<Record<string, unknown>> {
  const store = productionStore(config, env);
  if (!ADMIN_COMMANDS.has(request.command)) throw new Error("Operation command is invalid");
  if (request.action === "wait") {
    if (request.jobId === undefined) throw new Error("A job ID is required for wait");
    const deadline = Date.now() + Math.min(120000, Math.max(1000, config.memoryModel.timeoutMs));
    while (Date.now() < deadline) {
      const job = await store.readJob(request.jobId);
      if (job === null) throw new Error("Requested job was not found");
      const lease = await store.readLease(request.jobId);
      if (lease?.state === "completed") return { ok: true, command: request.command, action: request.action, jobId: request.jobId, state: "completed" };
      if (lease?.state === "released") throw new Error("Job was released before terminal completion");
      await pause(100);
    }
    throw new Error("Timed out waiting for job completion");
  }
  const control = await store.readControl();
  if (control.state !== "active") throw new Error("Collection control is not active");
  const episodeIds = await allAdminEpisodeIds(store, control, MAX_ADMIN_EPISODES);
  if (episodeIds.length === 0) return { ok: true, command: request.command, action: request.action, queued: false, reason: "no eligible episodes", privacyEpoch: control.privacyEpoch };
  const episodes = await readAdminEpisodes(store, episodeIds, control.privacyEpoch);
  const policyIds = [...new Set([control.processingPolicyId, ...episodes.map(episode => episode.processingPolicyId)])].sort();
  const policyRecords = await store.readProcessingPolicies(policyIds);
  const policies = new Map(policyRecords.map(record => [record.id, record.policy]));
  const activeWorkerPolicy = policies.get(control.processingPolicyId);
  if (activeWorkerPolicy === undefined) throw new Error("Active worker policy is unavailable");
  const groups = new Map<string, EpisodeRecord[]>();
  for (const episode of episodes) { const group = groups.get(episode.processingPolicyId) ?? []; group.push(episode); groups.set(episode.processingPolicyId, group); }
  const jobs: Awaited<ReturnType<ProductionCoordinationStore["createJob"]>>[] = [];
  if (request.command === "raptor") {
    const producerPolicies: ProcessingPolicy[] = [];
    for (const policyId of [...groups.keys()].sort()) { const producer = policies.get(policyId); if (producer === undefined) throw new Error("Producer policy is unavailable"); producerPolicies.push(producer); }
    const intersection = intersectPolicies(producerPolicies, activeWorkerPolicy);
    if (intersection === null || intersection.destinationIds.llm === undefined) return { ok: true, command: request.command, action: request.action, queued: false, reason: "incompatible policy groups", privacyEpoch: control.privacyEpoch };
    const membership = Object.freeze(episodeIds.slice().sort());
    const createdAt = episodes.map(episode => episode.eventAt).sort()[0]!;
    jobs.push(await enqueueAdminJob(store, { ownerHost: config.host, membership, policyIntersectionId: intersection.id, policyHash: control.coordinationPolicyHash, policyEpoch: control.coordinationPolicyEpoch, extractorRevision: "admin-raptor-v1", privacyEpoch: control.privacyEpoch, createdAt, expiresAt: intersection.expiresAt }));
  } else {
    for (const [producerId, group] of [...groups.entries()].sort(([left], [right]) => left.localeCompare(right))) {
      const producer = policies.get(producerId); if (producer === undefined) throw new Error("Producer policy is unavailable");
      const intersection = intersectPolicies([producer], activeWorkerPolicy);
      if (intersection === null || intersection.destinationIds.llm === undefined) continue;
      const ordered = group.map(episode => episode.id).sort();
      for (let index = 0; index < ordered.length; index += 1024) {
        const membership = Object.freeze(ordered.slice(index, index + 1024));
        const first = group.find(episode => episode.id === membership[0]); if (first === undefined) throw new Error("Episode membership is incomplete");
        jobs.push(await enqueueAdminJob(store, { ownerHost: config.host, membership, policyIntersectionId: intersection.id, policyHash: control.coordinationPolicyHash, policyEpoch: control.coordinationPolicyEpoch, extractorRevision: "curation-v1", privacyEpoch: control.privacyEpoch, createdAt: first.createdAt, expiresAt: intersection.expiresAt }));
      }
    }
  }
  if (jobs.length === 0) return { ok: true, command: request.command, action: request.action, queued: false, reason: "incompatible policy groups", privacyEpoch: control.privacyEpoch };
  return { ok: true, command: request.command, action: request.action, queued: true, jobId: jobs[0]!.id, jobIds: jobs.map(job => job.id), membershipCount: jobs.reduce((count, job) => count + job.membership.length, 0), privacyEpoch: control.privacyEpoch };
}
