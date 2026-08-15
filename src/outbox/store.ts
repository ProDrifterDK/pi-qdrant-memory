import { createHash, randomBytes as cryptoRandomBytes } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import * as nodeFs from "node:fs/promises";
import { isAbsolute, join, relative, resolve, sep } from "node:path";
import type { HostId } from "../types.js";
import type { EpisodeRecord } from "../domain/records.js";
import { parsePersistedMemoryRecord } from "../domain/records.js";
import type { ProcessingPolicy } from "../domain/policy.js";
import { processingPolicyHash } from "../domain/policy.js";
import { canonicalStringify, deterministicUuid, sha256Hex } from "../domain/canonical.js";
import { redactAndScan } from "../security/redaction.js";
import { assertPseudonymousNodeId } from "../security/egress.js";
import { acquireAdmissionGeneration, activeAdmissionLocks, isAdmissionProtocolArtifact, retireOwnedAdmissionLock } from "./reservation-protocol.js";

export type OutboxFileSystem = Pick<typeof nodeFs,
  "chmod" | "link" | "lstat" | "mkdir" | "open" | "readFile" | "readdir" | "realpath" | "rename" | "rm" | "stat"
>;

export interface OutboxJob {
  readonly version: 1;
  readonly id: string;
  readonly ownerHost: HostId;
  readonly nodeId: string;
  readonly producerUuid: string;
  readonly createdAt: string;
  readonly deadline: string | null;
  readonly policyId: string;
  readonly policy: ProcessingPolicy;
  readonly episodeIds: readonly string[];
  readonly episodes: readonly EpisodeRecord[];
  readonly auditHash: string;
}
export interface StoredOutboxJob extends OutboxJob { readonly file: string; }
export interface EnqueueInput { episodes: readonly EpisodeRecord[]; policy: ProcessingPolicy; }
export interface OutboxStatus {
  readonly state: "active" | "closed";
  readonly nodeId: string;
  readonly producerUuid: string;
  readonly jobs: number;
  readonly bytes: number;
  readonly oldestCreatedAt: string | null;
  readonly failedAttempts: number;
  readonly heartbeatAt: number;
  readonly captureAllowed: boolean;
}
export interface Outbox {
  readonly root: string;
  readonly producerPath: string;
  readonly nodeId: string;
  readonly producerUuid: string;
  enqueue(input: EnqueueInput): Promise<StoredOutboxJob>;
  listPending(): Promise<StoredOutboxJob[]>;
  quarantine(job: StoredOutboxJob | OutboxJob | string, category: string): Promise<void>;
  heartbeat(): Promise<void>;
  closeProducer(): Promise<void>;
  outboxStatus(): Promise<OutboxStatus>;
}
export interface CreateOutboxInput {
  host: HostId;
  homeDir: string;
  env?: Record<string, string | undefined>;
  nodeId?: string;
  producerUuid?: string;
  machineId?: string;
  sharedFilesystem?: boolean;
  maxJobs?: number;
  maxBytes?: number;
  now?: () => number;
  randomBytes?: (size: number) => Uint8Array;
  fs?: Partial<OutboxFileSystem>;
  notifyFull?: (status: Readonly<Pick<OutboxStatus, "jobs" | "bytes" | "captureAllowed">>) => void;
}

interface ProducerIdentity {
  version: 1; ownerHost: HostId; nodeId: string; producerUuid: string; sharedFilesystem: boolean;
  explicitNodeId: boolean; machineAuditHash: string; createdAt: string; auditHash: string;
}
interface ProducerState { version: 1; state: "active" | "closed"; heartbeatAt: number; closedAt: number | null; auditHash: string; }
interface NodeIdentity { version: 1; nodeId: string; machineAuditHash: string; auditHash: string; }
interface CaptureReservation { version: 1; reservationId: string; jobId: string; jobAuditHash: string; policyId: string; deadline: string | null; nodeId: string; producerUuid: string; requestedBytes: number; auditHash: string; }
interface JobsFence { version: 1; kind: "producer_jobs_fence"; nodeId: string; producerUuid: string; jobsDir: string; auditHash: string; }

type PlainRecord = Record<string, unknown>;
const HOST_DEFAULTS: Record<HostId, string> = { pi: ".pi/agent", prime: ".prime/agent" };
const JOB_KEYS = ["version", "id", "ownerHost", "nodeId", "producerUuid", "createdAt", "deadline", "policyId", "policy", "episodeIds", "episodes", "auditHash"] as const;
const PRODUCER_KEYS = ["version", "ownerHost", "nodeId", "producerUuid", "sharedFilesystem", "explicitNodeId", "machineAuditHash", "createdAt", "auditHash"] as const;
const STATE_KEYS = ["version", "state", "heartbeatAt", "closedAt", "auditHash"] as const;
const NODE_KEYS = ["version", "nodeId", "machineAuditHash", "auditHash"] as const;
const FENCE_KEYS = ["version", "kind", "nodeId", "producerUuid", "jobsDir", "auditHash"] as const;
const RESERVATION_KEYS = ["version", "reservationId", "jobId", "jobAuditHash", "policyId", "deadline", "nodeId", "producerUuid", "requestedBytes", "auditHash"] as const;
const SAFE_COMPONENT = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/u;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;
const FENCED_DIR = /^jobs\.fenced-[a-f0-9]{32}$/u;
const PRODUCER_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;
const ULID = /^[0-9A-HJKMNP-TV-Z]{26}$/u;
const LOCAL_DESTINATION = /^local:[a-f0-9]{32}$/u;
const HASH = /^[0-9a-f]{64}$/u;
const ISO_DATE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/u;
const MAX_EPISODES = 1024;

function record(value: unknown): value is PlainRecord { return typeof value === "object" && value !== null && !Array.isArray(value); }
function exactKeys(value: PlainRecord, keys: readonly string[]): boolean {
  const actual = Object.keys(value).sort(); const expected = [...keys].sort();
  return actual.length === expected.length && actual.every((key, index) => key === expected[index]);
}
function errno(error: unknown, code: string): boolean { return record(error) && error.code === code; }
function assertHost(host: unknown): asserts host is HostId { if (host !== "pi" && host !== "prime") throw new TypeError("Outbox host must be pi or prime"); }
function assertSafeComponent(name: string, value: unknown): asserts value is string {
  if (typeof value !== "string" || !SAFE_COMPONENT.test(value) || value === "." || value === ".." || value === "local" || /(?:api[-_]?key|access[-_]?token|authorization|bearer|credential|password|secret|token)/iu.test(value)) {
    throw new TypeError(`${name} must be a bounded pseudonymous path component`);
  }
}
function assertProducerUuid(value: unknown): asserts value is string {
  if (typeof value !== "string" || !PRODUCER_UUID.test(value)) throw new TypeError("producerUuid must be a canonical UUIDv4");
}
function assertFiniteTime(name: string, value: unknown): asserts value is number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0 || value > Date.parse("2100-12-31T23:59:59.999Z")) throw new TypeError(`${name} must be a bounded millisecond timestamp`);
}
function iso(ms: number): string { assertFiniteTime("outbox clock", ms); return new Date(ms).toISOString(); }
function hashWithout<T extends PlainRecord>(value: T, key: keyof T): string { const copy = { ...value }; delete copy[key]; return sha256Hex(canonicalStringify(copy)); }
function assertInside(root: string, candidate: string): void {
  const rel = relative(root, candidate);
  if (rel === "" || rel.startsWith(`..${sep}`) || rel === ".." || isAbsolute(rel)) throw new Error("Outbox path must remain below its validated root");
}
function deepFreeze<T>(value: T): T {
  if (typeof value === "object" && value !== null && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const child of Object.values(value as Record<string, unknown>)) deepFreeze(child);
  }
  return value;
}
function cloneCanonical<T>(value: T): T { return JSON.parse(canonicalStringify(value)) as T; }
function jobFileValue(job: OutboxJob, file: string): StoredOutboxJob {
  return deepFreeze({ ...cloneCanonical(job), file });
}
function idOf(job: StoredOutboxJob | OutboxJob | string): string {
  const id = typeof job === "string" ? job : job.id;
  if (!UUID.test(id)) throw new TypeError("Outbox job ID is invalid");
  return id;
}
function validCategory(value: unknown): asserts value is string {
  if (typeof value !== "string" || value.length === 0 || value.length > 128 || !/^[A-Za-z0-9._:-]+$/u.test(value) || /(?:api[-_]?key|access[-_]?token|authorization|bearer|credential|password|secret|token)/iu.test(value)) throw new TypeError("Outbox category must be bounded and redacted");
}

async function noSymlinks(fs: OutboxFileSystem, path: string): Promise<boolean> {
  const absolute = resolve(path);
  if (!isAbsolute(absolute)) return false;
  const pieces = absolute.split(sep).filter(Boolean);
  let cursor = absolute.startsWith(sep) ? sep : "";
  for (const piece of pieces) {
    cursor = cursor === sep ? `${cursor}${piece}` : `${cursor}${sep}${piece}`;
    try { if ((await fs.lstat(cursor)).isSymbolicLink()) return false; }
    catch (error) { if (errno(error, "ENOENT")) break; return false; }
  }
  return true;
}
async function ensureDirectory(fs: OutboxFileSystem, path: string, requirePrivateFinal = true): Promise<void> {
  const absolute = resolve(path); if (!isAbsolute(absolute) || absolute === sep) throw new Error("Outbox directory path is unsafe");
  const pieces = absolute.split(sep).filter(Boolean); let cursor: string = sep;
  for (let index = 0; index < pieces.length; index += 1) {
    cursor = cursor === sep ? `${sep}${pieces[index]!}` : join(cursor, pieces[index]!); let created = false; let missing = false; let info;
    try { info = await fs.lstat(cursor); }
    catch (error) {
      if (!errno(error, "ENOENT")) throw error; missing = true;
      try { await fs.mkdir(cursor, { recursive: false, mode: 0o700 }); created = true; }
      catch (mkdirError) { if (!errno(mkdirError, "EEXIST")) throw mkdirError; }
      info = await fs.lstat(cursor);
      if (created) { await fs.chmod(cursor, 0o700); await syncDirectory(fs, resolve(cursor, "..")); info = await fs.lstat(cursor); }
    }
    if (!info.isDirectory() || info.isSymbolicLink()) throw new Error("Outbox directory path is unsafe");
    if ((missing || created || (requirePrivateFinal && index === pieces.length - 1)) && (info.mode & 0o077) !== 0) throw new Error("Outbox directory permissions are unsafe");
    if (!created) await syncDirectory(fs, resolve(cursor, ".."));
  }
}
async function syncDirectory(fs: OutboxFileSystem, path: string): Promise<void> {
  const handle = await fs.open(path, "r");
  try { await handle.sync(); } finally { await handle.close(); }
}
async function atomicWrite(fs: OutboxFileSystem, path: string, value: unknown, random: (size: number) => Uint8Array): Promise<number> {
  const parent = resolve(path, "..");
  if (!(await noSymlinks(fs, parent))) throw new Error("Outbox write path is unsafe");
  const body = canonicalStringify(value);
  const suffix = Buffer.from(random(16)).toString("hex");
  const temp = `${path}.tmp-${process.pid}-${suffix}`;
  assertInside(parent, temp);
  const handle = await fs.open(temp, "wx", 0o600);
  try { await handle.writeFile(body, "utf8"); await handle.sync(); }
  catch (error) { await handle.close().catch(() => undefined); await fs.rm(temp, { force: true }).catch(() => undefined); throw error; }
  await handle.close();
  try {
    await fs.chmod(temp, 0o600);
    await fs.rename(temp, path);
    await syncDirectory(fs, parent);
  } catch (error) { await fs.rm(temp, { force: true }).catch(() => undefined); throw error; }
  return Buffer.byteLength(body, "utf8");
}
async function exclusiveWrite(fs: OutboxFileSystem, path: string, value: unknown, random: (size: number) => Uint8Array): Promise<void> {
  const parent = resolve(path, ".."); if (!(await noSymlinks(fs, parent))) throw new Error("Outbox exclusive-write path is unsafe"); const body = canonicalStringify(value); const temp = `${path}.create-${process.pid}-${Buffer.from(random(16)).toString("hex")}`; assertInside(parent, temp); const handle = await fs.open(temp, "wx", 0o600);
  try { await handle.writeFile(body, "utf8"); await handle.sync(); }
  catch (error) { await handle.close().catch(() => undefined); await fs.rm(temp, { force: true }).catch(() => undefined); await syncDirectory(fs, parent).catch(() => undefined); throw error; }
  await handle.close();
  try { await fs.chmod(temp, 0o600); await fs.link(temp, path); await syncDirectory(fs, parent); }
  catch (error) { await fs.rm(temp, { force: true }).catch(() => undefined); await syncDirectory(fs, parent).catch(() => undefined); throw error; }
  await fs.rm(temp); await syncDirectory(fs, parent);
}

async function readSecureJson(fs: OutboxFileSystem, path: string): Promise<unknown> {
  const info = await fs.lstat(path);
  if (!info.isFile() || info.isSymbolicLink() || (info.mode & 0o077) !== 0) throw new Error("Outbox state file is unsafe");
  return JSON.parse(await fs.readFile(path, "utf8"));
}

async function installationSalt(fs: OutboxFileSystem, root: string, random: (size: number) => Uint8Array): Promise<string> {
  const path = join(root, "installation-salt");
  try {
    const current = await readSecureJson(fs, path);
    if (typeof current !== "string" || !HASH.test(current)) throw new Error("Outbox installation salt is malformed");
    return current;
  } catch (error) {
    if (!errno(error, "ENOENT")) throw error;
    const value = Buffer.from(random(32)).toString("hex");
    try { await exclusiveWrite(fs, path, value, random); return value; }
    catch (writeError) {
      if (!errno(writeError, "EEXIST")) throw writeError;
      const current = await readSecureJson(fs, path);
      if (typeof current !== "string" || !HASH.test(current)) throw new Error("Outbox installation salt is malformed");
      return current;
    }
  }
}
async function machineIdentity(fs: OutboxFileSystem, explicit: string | undefined): Promise<string | undefined> {
  const candidate = explicit ?? await fs.readFile("/etc/machine-id", "utf8").catch(() => undefined);
  if (candidate === undefined) return undefined;
  const value = candidate.trim();
  if (value.length === 0 || value.length > 512 || /[\r\n\0/\\]/u.test(value)) throw new TypeError("Machine identity is invalid");
  return value;
}
function randomUuid(random: (size: number) => Uint8Array): string {
  const bytes = Buffer.from(random(16));
  if (bytes.length !== 16) throw new Error("CSPRNG must return exactly 128 bits");
  bytes[6] = ((bytes[6] ?? 0) & 0x0f) | 0x40; bytes[8] = ((bytes[8] ?? 0) & 0x3f) | 0x80;
  const hex = bytes.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
function producerState(state: Omit<ProducerState, "auditHash">): ProducerState { const value = { ...state, auditHash: "" }; value.auditHash = hashWithout(value, "auditHash"); return value; }
function validateState(value: unknown): ProducerState {
  if (!record(value) || !exactKeys(value, STATE_KEYS) || value.version !== 1 || (value.state !== "active" && value.state !== "closed")) throw new Error("Outbox producer state is malformed");
  assertFiniteTime("heartbeatAt", value.heartbeatAt); if (value.closedAt !== null) assertFiniteTime("closedAt", value.closedAt);
  if ((value.state === "active" && value.closedAt !== null) || (value.state === "closed" && (value.closedAt === null || value.closedAt < value.heartbeatAt)) || typeof value.auditHash !== "string" || value.auditHash !== hashWithout(value, "auditHash")) throw new Error("Outbox producer state is malformed");
  return value as unknown as ProducerState;
}
function nodeIdentity(nodeId: string, machineAuditHash: string): NodeIdentity { const value: NodeIdentity = { version: 1, nodeId, machineAuditHash, auditHash: "" }; value.auditHash = hashWithout(value as unknown as PlainRecord, "auditHash"); return value; }
function validateNode(value: unknown, expected: NodeIdentity): void {
  if (!record(value) || !exactKeys(value, NODE_KEYS) || value.version !== 1 || value.nodeId !== expected.nodeId || value.machineAuditHash !== expected.machineAuditHash || value.auditHash !== hashWithout(value, "auditHash")) throw new Error("Duplicate or malformed shared-filesystem node identity");
}
function producerIdentity(input: Omit<ProducerIdentity, "auditHash">): ProducerIdentity { const value = { ...input, auditHash: "" }; value.auditHash = hashWithout(value, "auditHash"); return value; }
function validateProducer(value: unknown, expected?: Pick<ProducerIdentity, "ownerHost" | "nodeId" | "producerUuid">): ProducerIdentity {
  if (!record(value) || !exactKeys(value, PRODUCER_KEYS) || value.version !== 1) throw new Error("Outbox producer identity is malformed");
  assertHost(value.ownerHost); assertProducerUuid(value.producerUuid);
  if (typeof value.sharedFilesystem !== "boolean" || typeof value.explicitNodeId !== "boolean" || (value.sharedFilesystem && !value.explicitNodeId)) throw new Error("Outbox producer identity is malformed");
  try { assertPseudonymousNodeId(value.nodeId, { allowDerivedDigest: !value.explicitNodeId }); } catch { throw new Error("Outbox producer identity is malformed"); }
  if (typeof value.machineAuditHash !== "string" || !HASH.test(value.machineAuditHash) || typeof value.createdAt !== "string" || !ISO_DATE.test(value.createdAt) || new Date(value.createdAt).toISOString() !== value.createdAt || value.auditHash !== hashWithout(value, "auditHash")) throw new Error("Outbox producer identity is malformed");
  if (expected !== undefined && (value.ownerHost !== expected.ownerHost || value.nodeId !== expected.nodeId || value.producerUuid !== expected.producerUuid)) throw new Error("Outbox producer identity does not match its path");
  return value as unknown as ProducerIdentity;
}
function captureReservation(job: OutboxJob, requestedBytes: number): CaptureReservation {
  const reservationId = deterministicUuid("pi-qdrant-memory-v2:outbox-reservation", job.nodeId, job.producerUuid, job.id);
  const value: CaptureReservation = { version: 1, reservationId, jobId: job.id, jobAuditHash: job.auditHash, policyId: job.policyId, deadline: job.deadline, nodeId: job.nodeId, producerUuid: job.producerUuid, requestedBytes, auditHash: "" };
  value.auditHash = hashWithout(value as unknown as PlainRecord, "auditHash"); return value;
}
function validateReservation(value: unknown): CaptureReservation {
  if (!record(value) || !exactKeys(value, RESERVATION_KEYS) || value.version !== 1 || typeof value.reservationId !== "string" || !UUID.test(value.reservationId) || typeof value.jobId !== "string" || !UUID.test(value.jobId) || typeof value.jobAuditHash !== "string" || !HASH.test(value.jobAuditHash) || typeof value.policyId !== "string" || !HASH.test(value.policyId) || (value.deadline !== null && (typeof value.deadline !== "string" || !ISO_DATE.test(value.deadline) || !Number.isFinite(Date.parse(value.deadline)))) || typeof value.nodeId !== "string" || typeof value.producerUuid !== "string" || !PRODUCER_UUID.test(value.producerUuid) || !Number.isSafeInteger(value.requestedBytes) || (value.requestedBytes as number) < 1 || (value.requestedBytes as number) > 1_073_741_824 || value.auditHash !== hashWithout(value, "auditHash")) throw new Error("Outbox reservation is malformed");
  assertPseudonymousNodeId(value.nodeId, { allowDerivedDigest: true });
  const expectedId = deterministicUuid("pi-qdrant-memory-v2:outbox-reservation", value.nodeId, value.producerUuid, value.jobId); if (value.reservationId !== expectedId) throw new Error("Outbox reservation identity is malformed");
  return value as unknown as CaptureReservation;
}
function validateFence(value: unknown, nodeId: string, producerUuid: string): JobsFence { if (!record(value) || !exactKeys(value, FENCE_KEYS) || value.version !== 1 || value.kind !== "producer_jobs_fence" || value.nodeId !== nodeId || value.producerUuid !== producerUuid || typeof value.jobsDir !== "string" || !FENCED_DIR.test(value.jobsDir) || value.auditHash !== hashWithout(value, "auditHash")) throw new Error("Outbox producer fence is malformed"); return value as unknown as JobsFence; }
function assertExactRedacted(value: string, maxChars: number, homeDir: string): void {
  const checked = redactAndScan({ text: value, maxChars, homeDir });
  if (checked.dropped || checked.secretScan !== "passed" || checked.text !== value) throw new TypeError("Outbox accepts only exact final redacted material");
}
function assertHostRecordId(value: string, homeDir: string): void {
  if (UUID.test(value) || ULID.test(value) || /^[a-f0-9]{40}$/u.test(value)) return;
  assertExactRedacted(value, 512, homeDir);
}
function assertProjectId(value: string, homeDir: string): void {
  if (/^[a-f0-9]{64}$/u.test(value)) return;
  assertHostRecordId(value, homeDir);
}
function assertDestinationId(value: string, homeDir: string): void {
  if (LOCAL_DESTINATION.test(value)) return;
  assertExactRedacted(value, 512, homeDir);
}
function safePolicyMaterial(policy: ProcessingPolicy, homeDir: string): void {
  for (const value of [policy.originProvider, policy.residency, policy.dataUse, policy.policyRevision]) assertExactRedacted(value, 512, homeDir);
  for (const value of [policy.destinationIds.qdrant, policy.destinationIds.embedding, policy.destinationIds.llm]) if (value !== undefined) assertDestinationId(value, homeDir);
}
function safeEpisodeMaterial(episode: EpisodeRecord, homeDir: string): void {
  if (!UUID.test(episode.id)) throw new TypeError("Outbox episode ID must be a canonical UUID");
  for (const value of [episode.text, episode.toolArgs]) if (value !== undefined) assertExactRedacted(value, 16_000, homeDir);
  for (const value of [episode.originProvider, episode.toolName]) if (value !== undefined) assertExactRedacted(value, 512, homeDir);
  for (const value of [episode.sourceEntryId, episode.sessionId, episode.turnId, episode.modelId]) assertHostRecordId(value, homeDir);
  assertProjectId(episode.projectId, homeDir); assertDestinationId(episode.destinationId, homeDir);
  if (episode.errorFingerprint !== undefined && !/^[a-f0-9]{32}$/u.test(episode.errorFingerprint)) throw new TypeError("Outbox error fingerprint must be exact lower-hex32");
  if (episode.producerId !== undefined) assertProducerUuid(episode.producerId);
  if (episode.nodeId !== undefined) assertPseudonymousNodeId(episode.nodeId, { allowDerivedDigest: true });
  if (episode.vector !== undefined) throw new TypeError("Outbox episodes must not contain embeddings");
}
function outboxJob(input: { host: HostId; nodeId: string; producerUuid: string; now: number; policy: ProcessingPolicy; episodes: readonly EpisodeRecord[]; homeDir: string }): OutboxJob {
  if (!Array.isArray(input.episodes) || input.episodes.length === 0 || input.episodes.length > MAX_EPISODES) throw new TypeError("Outbox job must contain 1..1024 episodes");
  const policy = cloneCanonical(input.policy); const policyHash = processingPolicyHash(policy);
  if (policy.id !== policyHash || policy.ownerHost !== input.host) throw new TypeError("Outbox processing policy is invalid");
  safePolicyMaterial(policy, input.homeDir);
  const episodes = input.episodes.map((candidate) => {
    const parsed = parsePersistedMemoryRecord(cloneCanonical(candidate), { ownerHost: input.host });
    if (parsed.recordType !== "episode") throw new TypeError("Outbox accepts only episode records");
    if (parsed.processingPolicyId !== policy.id || parsed.expiresAt !== policy.expiresAt || parsed.ownerHost !== policy.ownerHost || parsed.originProvider !== policy.originProvider || parsed.destinationId !== policy.destinationIds.qdrant || parsed.secretScan !== "passed" || (parsed.producerId !== undefined && parsed.producerId !== input.producerUuid) || (parsed.nodeId !== undefined && parsed.nodeId !== input.nodeId)) throw new TypeError("Outbox episode policy or producer envelope is inconsistent");
    safeEpisodeMaterial(parsed, input.homeDir); return parsed;
  });
  if (new Set(episodes.map((item) => item.id)).size !== episodes.length) throw new TypeError("Outbox episode IDs must be unique");
  const episodeIds = episodes.map((item) => item.id);
  const id = deterministicUuid("pi-qdrant-memory-v2:outbox-job", input.host, episodeIds, policy.id);
  const createdAt = episodes.map((episode) => episode.createdAt).sort().at(-1)!;
  const value: OutboxJob = { version: 1, id, ownerHost: input.host, nodeId: input.nodeId, producerUuid: input.producerUuid, createdAt, deadline: policy.expiresAt, policyId: policy.id, policy, episodeIds, episodes, auditHash: "" };
  (value as { auditHash: string }).auditHash = hashWithout(value as unknown as PlainRecord, "auditHash");
  return deepFreeze(value);
}
export function parseOutboxJob(value: unknown, expected?: { host?: HostId; nodeId?: string; producerUuid?: string; homeDir?: string }): OutboxJob {
  if (!record(value) || !exactKeys(value, JOB_KEYS) || value.version !== 1 || typeof value.id !== "string" || !UUID.test(value.id)) throw new Error("Outbox job is malformed");
  assertHost(value.ownerHost); assertPseudonymousNodeId(value.nodeId, { allowDerivedDigest: true }); assertProducerUuid(value.producerUuid);
  if (typeof value.createdAt !== "string" || !ISO_DATE.test(value.createdAt) || !Number.isFinite(Date.parse(value.createdAt)) || new Date(value.createdAt).toISOString() !== value.createdAt || (value.deadline !== null && (typeof value.deadline !== "string" || !ISO_DATE.test(value.deadline) || new Date(value.deadline).toISOString() !== value.deadline))) throw new Error("Outbox job timestamps are malformed");
  if (!record(value.policy) || typeof value.policyId !== "string" || value.policyId !== value.policy.id || processingPolicyHash(value.policy as unknown as ProcessingPolicy) !== value.policyId || value.deadline !== value.policy.expiresAt) throw new Error("Outbox job policy is malformed");
  if (value.policy.ownerHost !== value.ownerHost) throw new Error("Outbox job policy owner linkage is invalid");
  safePolicyMaterial(value.policy as unknown as ProcessingPolicy, expected?.homeDir ?? "/");
  if (!Array.isArray(value.episodeIds) || !Array.isArray(value.episodes) || value.episodes.length === 0 || value.episodes.length > MAX_EPISODES || value.episodeIds.length !== value.episodes.length) throw new Error("Outbox job membership is malformed");
  const episodeIds = value.episodeIds; const rawEpisodes = value.episodes;
  const episodes = rawEpisodes.map((candidate, index) => {
    const parsed = parsePersistedMemoryRecord(candidate, { ownerHost: value.ownerHost as HostId });
    const policy = value.policy as unknown as ProcessingPolicy;
    if (parsed.recordType !== "episode" || parsed.id !== episodeIds[index] || parsed.ownerHost !== value.ownerHost || parsed.processingPolicyId !== value.policyId || parsed.expiresAt !== value.deadline || parsed.originProvider !== policy.originProvider || parsed.destinationId !== policy.destinationIds.qdrant || (parsed.producerId !== undefined && parsed.producerId !== value.producerUuid) || (parsed.nodeId !== undefined && parsed.nodeId !== value.nodeId) || parsed.vector !== undefined) throw new Error("Outbox job episode is malformed");
    safeEpisodeMaterial(parsed, expected?.homeDir ?? "/"); return parsed;
  });
  if (new Set(episodeIds).size !== episodeIds.length) throw new Error("Outbox job membership is malformed");
  const deterministic = deterministicUuid("pi-qdrant-memory-v2:outbox-job", value.ownerHost, episodeIds, value.policyId);
  if (value.id !== deterministic || typeof value.auditHash !== "string" || value.auditHash !== hashWithout(value, "auditHash")) throw new Error("Outbox job integrity check failed");
  if (expected?.host !== undefined && value.ownerHost !== expected.host || expected?.nodeId !== undefined && value.nodeId !== expected.nodeId || expected?.producerUuid !== undefined && value.producerUuid !== expected.producerUuid) throw new Error("Outbox job does not match its producer path");
  return deepFreeze({ ...(value as unknown as OutboxJob), episodes: deepFreeze(episodes) });
}

async function quarantineMalformed(fs: OutboxFileSystem, source: string, quarantineDir: string, random: (size: number) => Uint8Array, now: number, input: { bytes?: Uint8Array; unsafePath?: boolean }): Promise<void> {
  const unsafePath = input.unsafePath === true; const bytes = input.bytes;
  if (!unsafePath && bytes === undefined) throw new Error("Malformed quarantine requires securely-read bytes");
  const sourceHash = unsafePath ? sha256Hex(canonicalStringify({ kind: "unsafe_path", source })) : createHash("sha256").update(bytes!).digest("hex");
  const base: PlainRecord = { version: 1, kind: unsafePath ? "unsafe_path" : "malformed", sourceHash, byteLength: bytes?.length ?? 0, category: "malformed", quarantinedAt: iso(now), auditHash: "" };
  base.auditHash = hashWithout(base, "auditHash");
  const destination = join(quarantineDir, `malformed-${sourceHash}.json`);
  try { await atomicWrite(fs, destination, base, random); }
  catch (error) {
    if (!errno(error, "EEXIST")) throw error;
  }
  await fs.rm(source, { force: true }); await syncDirectory(fs, resolve(source, ".."));
}

interface PreparedOutboxIdentity {
  readonly fs: OutboxFileSystem;
  readonly random: (size: number) => Uint8Array;
  readonly clock: () => number;
  readonly setupNow: number;
  readonly sharedFilesystem: boolean;
  readonly root: string;
  readonly reservationsDir: string;
  readonly nodeId: string;
  readonly nodePath: string;
  readonly machineAuditHash: string;
}
async function prepareOutboxIdentity(input: CreateOutboxInput): Promise<PreparedOutboxIdentity> {
  assertHost(input.host);
  if (typeof input.homeDir !== "string" || !isAbsolute(input.homeDir)) throw new TypeError("Outbox home directory must be absolute");
  const sharedFilesystem = input.sharedFilesystem ?? false;
  if (input.nodeId !== undefined) assertPseudonymousNodeId(input.nodeId);
  if (sharedFilesystem && input.nodeId === undefined) throw new Error("An explicit unique node ID is required for a shared filesystem");
  const fs = { ...nodeFs, ...(input.fs ?? {}) } as OutboxFileSystem;
  const random = input.randomBytes ?? ((size: number) => cryptoRandomBytes(size));
  const now = input.now ?? Date.now; const clock = (): number => { const value = now(); assertFiniteTime("outbox clock", value); return value; }; const setupNow = clock();
  const env = input.env ?? {};
  const expectedName = input.host === "pi" ? "PI_CODING_AGENT_DIR" : "PRIME_AGENT_CODING_AGENT_DIR";
  const otherName = input.host === "pi" ? "PRIME_AGENT_CODING_AGENT_DIR" : "PI_CODING_AGENT_DIR";
  if (env[otherName] !== undefined && env[otherName]!.trim() !== "") throw new Error("Contradictory host agent roots are unsafe");
  const configured = env[expectedName];
  if (configured !== undefined && configured.trim() === "") throw new Error("Host agent root must not be empty");
  if (configured !== undefined && !isAbsolute(configured.trim())) throw new Error("Configured host agent root must be absolute");
  const agentRoot = resolve(configured === undefined ? join(input.homeDir, HOST_DEFAULTS[input.host]) : configured.trim());
  if (!isAbsolute(agentRoot) || agentRoot === resolve(sep) || !(await noSymlinks(fs, agentRoot))) throw new Error("Host agent root is unsafe");
  await ensureDirectory(fs, agentRoot, false);
  const pluginRoot = join(agentRoot, "pi-qdrant-memory"); const root = join(pluginRoot, "outbox"); const reservationsDir = join(root, "reservations");
  await ensureDirectory(fs, pluginRoot); await ensureDirectory(fs, root); await ensureDirectory(fs, reservationsDir);
  const salt = await installationSalt(fs, root, random);
  const machine = await machineIdentity(fs, input.machineId);
  if (input.nodeId === undefined && machine === undefined) throw new Error("A machine identity or explicit pseudonymous node ID is required");
  const nodeId = input.nodeId ?? `node-${sha256Hex(canonicalStringify({ machine, salt })).slice(0, 32)}`;
  assertPseudonymousNodeId(nodeId, { allowDerivedDigest: input.nodeId === undefined });
  const machineAuditHash = sha256Hex(canonicalStringify({ machine: machine ?? `explicit:${nodeId}`, salt }));
  const nodePath = join(root, nodeId); await ensureDirectory(fs, nodePath);
  const expectedNode = nodeIdentity(nodeId, machineAuditHash); const nodeFile = join(nodePath, "node.json");
  try { await exclusiveWrite(fs, nodeFile, expectedNode, random); }
  catch (error) { if (!errno(error, "EEXIST")) throw error; validateNode(await readSecureJson(fs, nodeFile), expectedNode); }
  return Object.freeze({ fs, random, clock, setupNow, sharedFilesystem, root, reservationsDir, nodeId, nodePath, machineAuditHash });
}

/** Resolve and persist the pseudonymous installation node identity without creating a producer. */
export async function resolveOutboxNodeId(input: Omit<CreateOutboxInput, "producerUuid" | "maxJobs" | "maxBytes" | "notifyFull">): Promise<string> {
  return (await prepareOutboxIdentity(input)).nodeId;
}

export class OutboxCapacityError extends Error { constructor() { super("Outbox capacity reached; new capture was not accepted"); this.name = "OutboxCapacityError"; } }
class OutboxAdmissionBusyError extends Error { constructor() { super("Outbox admission is busy"); this.name = "OutboxAdmissionBusyError"; } }

export async function createOutbox(input: CreateOutboxInput): Promise<Outbox> {
  assertHost(input.host);
  if (typeof input.homeDir !== "string" || !isAbsolute(input.homeDir)) throw new TypeError("Outbox home directory must be absolute");
  const maxJobs = input.maxJobs ?? 10_000; const maxBytes = input.maxBytes ?? 268_435_456;
  if (!Number.isSafeInteger(maxJobs) || maxJobs < 1 || maxJobs > 100_000) throw new TypeError("outbox.maxJobs must be between 1 and 100000");
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 1_048_576 || maxBytes > 1_073_741_824) throw new TypeError("outbox.maxBytes must be between 1 MiB and 1 GiB");
  if (input.producerUuid !== undefined) assertProducerUuid(input.producerUuid);
  const prepared = await prepareOutboxIdentity(input);
  const { fs, random, clock, setupNow, sharedFilesystem, root, reservationsDir, nodeId, nodePath, machineAuditHash } = prepared;
  const producerUuid = input.producerUuid ?? randomUuid(random); assertProducerUuid(producerUuid);
  const producerPath = join(nodePath, producerUuid); assertInside(root, producerPath);
  if (!(await noSymlinks(fs, producerPath))) throw new Error("Producer path is unsafe");
  await fs.mkdir(producerPath, { recursive: false, mode: 0o700 });
  await fs.chmod(producerPath, 0o700); await syncDirectory(fs, nodePath);
  const jobsDir = join(producerPath, "jobs"); const controlDir = join(producerPath, "control"); const quarantineDir = join(producerPath, "quarantine");
  await ensureDirectory(fs, jobsDir); await ensureDirectory(fs, controlDir); await ensureDirectory(fs, quarantineDir);
  const created = setupNow;
  const identity = producerIdentity({ version: 1, ownerHost: input.host, nodeId, producerUuid, sharedFilesystem, explicitNodeId: input.nodeId !== undefined, machineAuditHash, createdAt: iso(created) }); const producerFile = join(producerPath, "producer.json");
  try { await exclusiveWrite(fs, producerFile, identity, random); }
  catch (error) { try { await fs.lstat(producerFile); } catch (missing) { if (errno(missing, "ENOENT")) { await fs.rm(producerPath, { recursive: true, force: true }); await syncDirectory(fs, nodePath); } } throw error; }
  let state = producerState({ version: 1, state: "active", heartbeatAt: created, closedAt: null });
  const stateFile = join(producerPath, "state.json"); await atomicWrite(fs, stateFile, state, random);
  let serial = Promise.resolve();
  const serialized = async <T>(operation: () => Promise<T>): Promise<T> => {
    const previous = serial; let release!: () => void; serial = new Promise<void>((done) => { release = done; });
    await previous; try { return await operation(); } finally { release(); }
  };
  async function localPending(): Promise<StoredOutboxJob[]> {
    validateProducer(await readSecureJson(fs, join(producerPath, "producer.json")), { ownerHost: input.host, nodeId, producerUuid });
    const names = (await fs.readdir(jobsDir)).filter((name) => name.endsWith(".json")).sort();
    const result: StoredOutboxJob[] = [];
    for (const name of names) {
      const file = join(jobsDir, name); let info;
      try { info = await fs.lstat(file); } catch { continue; }
      if (info.isSymbolicLink()) { await quarantineMalformed(fs, file, quarantineDir, random, clock(), { unsafePath: true }).catch(() => undefined); continue; }
      if (!info.isFile() || (info.mode & 0o077) !== 0) continue;
      let bytes: Uint8Array; try { bytes = await fs.readFile(file); } catch { continue; }
      try {
        const job = parseOutboxJob(JSON.parse(Buffer.from(bytes).toString("utf8")), { host: input.host, nodeId, producerUuid, homeDir: input.homeDir });
        if (name !== `${job.id}.json`) throw new Error("job filename mismatch"); result.push(jobFileValue(job, file));
      } catch { await quarantineMalformed(fs, file, quarantineDir, random, clock(), { bytes }).catch(() => undefined); }
    }
    return result;
  }
  async function rootUsage(): Promise<{ jobs: number; bytes: number; reservations: CaptureReservation[]; unsafeReservations: boolean }> {
    let jobs = 0; let bytes = 0; const reservations: CaptureReservation[] = []; let unsafeReservations = false;
    for (const nodeName of await fs.readdir(root)) {
      if (!SAFE_COMPONENT.test(nodeName) || nodeName === "reservations") continue;
      const candidateNode = join(root, nodeName); let nodeInfo;
      try { nodeInfo = await fs.lstat(candidateNode); } catch (error) { if (errno(error, "ENOENT")) continue; throw error; }
      if (!nodeInfo.isDirectory() || nodeInfo.isSymbolicLink()) continue;
      for (const producerName of await fs.readdir(candidateNode)) {
        if (!PRODUCER_UUID.test(producerName)) continue; const candidateProducer = join(candidateNode, producerName); let producerInfo;
        try { producerInfo = await fs.lstat(candidateProducer); } catch (error) { if (errno(error, "ENOENT")) continue; throw error; }
        if (!producerInfo.isDirectory() || producerInfo.isSymbolicLink()) continue;
        const queueDirs = (await fs.readdir(candidateProducer)).filter((name) => name === "jobs" || name === "control" || name === "quarantine" || FENCED_DIR.test(name));
        for (const kind of queueDirs) {
          const dir = join(candidateProducer, kind); let dirInfo;
          try { dirInfo = await fs.lstat(dir); } catch (error) { if (errno(error, "ENOENT")) continue; throw error; }
          if (!dirInfo.isDirectory() || dirInfo.isSymbolicLink()) continue;
          for (const name of await fs.readdir(dir)) {
            try {
              const info = await fs.lstat(join(dir, name)); if (!info.isFile() || info.isSymbolicLink()) continue; bytes += info.size;
              const id = name.endsWith(".json") ? name.slice(0, -5) : ""; if ((kind === "jobs" || kind === "quarantine" || FENCED_DIR.test(kind)) && UUID.test(id)) jobs += 1;
            } catch (error) { if (!errno(error, "ENOENT")) throw error; }
          }
        }
      }
    }
    const reservationsInfo = await fs.lstat(reservationsDir); if (!reservationsInfo.isDirectory() || reservationsInfo.isSymbolicLink() || (reservationsInfo.mode & 0o077) !== 0) throw new Error("Outbox reservations directory is unsafe");
    try { await activeAdmissionLocks(fs, reservationsDir, validateReservation); } catch { unsafeReservations = true; }
    for (const name of await fs.readdir(reservationsDir)) {
      if (name === "admission.lock") { unsafeReservations = true; continue; }
      const file = join(reservationsDir, name); try { const info = await fs.lstat(file); if (!info.isFile() || info.isSymbolicLink()) { unsafeReservations = true; continue; } bytes += info.size; if (isAdmissionProtocolArtifact(name)) continue; if (name.startsWith("admission.") || name.startsWith(".admission.")) { unsafeReservations = true; continue; } if (!name.endsWith(".json")) continue; const reservation = validateReservation(await readSecureJson(fs, file)); if (name !== `${reservation.reservationId}.json`) throw new Error("reservation filename mismatch"); reservations.push(reservation); } catch { unsafeReservations = true; }
    }
    return { jobs, bytes, reservations, unsafeReservations };
  }
  function reservationMatchesJob(reservation: CaptureReservation, job: OutboxJob): boolean { return job.ownerHost === input.host && job.nodeId === reservation.nodeId && job.producerUuid === reservation.producerUuid && job.id === reservation.jobId && job.auditHash === reservation.jobAuditHash && job.policyId === reservation.policyId && job.deadline === reservation.deadline && Buffer.byteLength(canonicalStringify(job), "utf8") === reservation.requestedBytes; }
  async function proofDirectories(reservation: CaptureReservation): Promise<string[]> {
    const producer = join(root, reservation.nodeId, reservation.producerUuid); const identity = validateProducer(await readSecureJson(fs, join(producer, "producer.json")), { ownerHost: input.host, nodeId: reservation.nodeId, producerUuid: reservation.producerUuid }); validateNode(await readSecureJson(fs, join(root, reservation.nodeId, "node.json")), nodeIdentity(reservation.nodeId, identity.machineAuditHash)); const names = await fs.readdir(producer); const fenced = names.filter((name) => FENCED_DIR.test(name)).sort(); let jobs = join(producer, "jobs");
    try { const manifest = validateFence(await readSecureJson(fs, join(producer, "fence.json")), reservation.nodeId, reservation.producerUuid); if (fenced.length !== 1 || fenced[0] !== manifest.jobsDir) throw new Error("Outbox producer fence is ambiguous"); try { await fs.lstat(jobs); throw new Error("Outbox original jobs directory exists after fencing"); } catch (error) { if (!errno(error, "ENOENT")) throw error; } jobs = join(producer, manifest.jobsDir); }
    catch (error) { if (!errno(error, "ENOENT")) throw error; if (fenced.length !== 0) throw new Error("Outbox producer has an incomplete fence"); }
    for (const dir of [jobs, join(producer, "quarantine")]) { const info = await fs.lstat(dir); if (!info.isDirectory() || info.isSymbolicLink() || (info.mode & 0o077) !== 0 || await fs.realpath(dir) !== dir) throw new Error("Outbox proof directory is unsafe"); } return [jobs, join(producer, "quarantine")];
  }
  async function durableReservationJobProof(reservation: CaptureReservation): Promise<OutboxJob | undefined> {
    const readExact = async (file: string): Promise<OutboxJob> => { const info = await fs.lstat(file); if (!info.isFile() || info.isSymbolicLink() || (info.mode & 0o077) !== 0 || info.size !== reservation.requestedBytes) throw new Error("Outbox durable proof file is unsafe or has the wrong byte length"); let handle; let bytes: Uint8Array; try { handle = await fs.open(file, fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW); const opened = await handle.stat(); if (!opened.isFile() || opened.dev !== info.dev || opened.ino !== info.ino || opened.size !== reservation.requestedBytes || (opened.mode & 0o077) !== 0) throw new Error("Outbox durable proof inode changed"); bytes = await handle.readFile(); } finally { await handle?.close().catch(() => undefined); } const after = await fs.lstat(file); if (!after.isFile() || after.isSymbolicLink() || after.dev !== info.dev || after.ino !== info.ino || after.size !== reservation.requestedBytes || (after.mode & 0o077) !== 0 || bytes.length !== reservation.requestedBytes) throw new Error("Outbox durable proof path changed"); const text = Buffer.from(bytes).toString("utf8"); const job = parseOutboxJob(JSON.parse(text), { host: input.host, nodeId: reservation.nodeId, producerUuid: reservation.producerUuid, homeDir: input.homeDir }); if (text !== canonicalStringify(job) || !reservationMatchesJob(reservation, job)) throw new Error("Outbox durable proof does not exactly match reservation"); return job; };
    for (const dir of await proofDirectories(reservation)) { const identity = await fs.lstat(dir); if (!identity.isDirectory() || identity.isSymbolicLink() || (identity.mode & 0o077) !== 0 || await fs.realpath(dir) !== dir) throw new Error("Outbox durable proof directory is unsafe"); const file = join(dir, `${reservation.jobId}.json`); let first: OutboxJob; try { first = await readExact(file); } catch (error) { if (errno(error, "ENOENT")) continue; throw error; } await syncDirectory(fs, dir); const after = await fs.lstat(dir); if (!after.isDirectory() || after.isSymbolicLink() || (after.mode & 0o077) !== 0 || await fs.realpath(dir) !== dir || after.dev !== identity.dev || after.ino !== identity.ino) throw new Error("Outbox durable proof directory changed"); const second = await readExact(file); if (canonicalStringify(second) !== canonicalStringify(first)) throw new Error("Outbox durable proof readback mismatch"); return second; } return undefined;
  }
  async function removeReservation(reservation: CaptureReservation): Promise<void> { try { await fs.rm(join(reservationsDir, `${reservation.reservationId}.json`)); } catch (error) { if (!errno(error, "ENOENT")) throw error; } await syncDirectory(fs, reservationsDir); }
  async function cleanupReservations(reservations: readonly CaptureReservation[]): Promise<void> { for (const reservation of reservations) if (await durableReservationJobProof(reservation) !== undefined) await removeReservation(reservation); }
  async function acquireAdmissionLock(reservation: CaptureReservation): Promise<CaptureReservation> {
    const reservationFile = join(reservationsDir, `${reservation.reservationId}.json`);
    try { await acquireAdmissionGeneration({ fs, dir: reservationsDir, reservationFile, reservation, validateReservation, durableProof: async (existing) => await durableReservationJobProof(existing) !== undefined }); return reservation; }
    catch (error) { if (error instanceof Error && error.message === "Outbox admission is busy") throw new OutboxAdmissionBusyError(); throw error; }
  }
  async function finalizeAdmission(reservation: CaptureReservation, requireOwnership = true): Promise<void> {
    await retireOwnedAdmissionLock({ fs, dir: reservationsDir, reservation, validateReservation, requireOwnership });
    await removeReservation(reservation);
  }
  async function admissionLockOwnership(reservation: CaptureReservation): Promise<"own" | "other" | "absent" | "unknown"> { try { const locks = await activeAdmissionLocks(fs, reservationsDir, validateReservation); if (locks.some((lock) => canonicalStringify(lock.reservation) === canonicalStringify(reservation))) return "own"; return locks.length === 0 ? "absent" : "other"; } catch { return "unknown"; } }
  async function ensureReservation(reservation: CaptureReservation): Promise<void> {
    const file = join(reservationsDir, `${reservation.reservationId}.json`); const prepared = join(reservationsDir, `prepare-${sha256Hex(reservation.nodeId).slice(0, 16)}-${reservation.producerUuid}-${reservation.reservationId}`);
    try { await atomicWrite(fs, prepared, reservation, random); try { await fs.link(prepared, file); await syncDirectory(fs, reservationsDir); } catch (error) { if (!errno(error, "EEXIST")) throw error; const existing = validateReservation(await readSecureJson(fs, file)); if (canonicalStringify(existing) !== canonicalStringify(reservation)) throw new Error("Outbox reservation collision"); } }
    finally { await fs.rm(prepared, { force: true }).then(() => syncDirectory(fs, reservationsDir)).catch(() => undefined); }
  }
  async function assertProducerUnfenced(): Promise<void> { try { await fs.lstat(join(producerPath, "fence.json")); throw new Error("Outbox producer jobs namespace is fenced"); } catch (error) { if (!errno(error, "ENOENT")) throw error; } const info = await fs.lstat(jobsDir); if (!info.isDirectory() || info.isSymbolicLink() || (info.mode & 0o077) !== 0 || await fs.realpath(jobsDir) !== jobsDir) throw new Error("Outbox producer jobs namespace is fenced or unsafe"); }
  async function writeState(next: ProducerState): Promise<void> { const validated = validateState(next); await atomicWrite(fs, stateFile, validated, random); state = validateState(await readSecureJson(fs, stateFile)); }
  async function controlAttempts(id: string): Promise<number> {
    try { const value = await readSecureJson(fs, join(controlDir, `${id}.json`)); return record(value) && Number.isSafeInteger(value.attempts) && (value.attempts as number) >= 0 ? value.attempts as number : 0; } catch { return 0; }
  }
  const api: Outbox = {
    root, producerPath, nodeId, producerUuid,
    enqueue: (enqueueInput) => serialized(async () => {
      const acceptedAt = clock(); await assertProducerUnfenced(); state = validateState(await readSecureJson(fs, stateFile)); if (state.state !== "active") throw new Error("Outbox producer is closed");
      const job = outboxJob({ host: input.host, nodeId, producerUuid, now: acceptedAt, policy: enqueueInput.policy, episodes: enqueueInput.episodes, homeDir: input.homeDir });
      const file = join(jobsDir, `${job.id}.json`); const bodyBytes = Buffer.byteLength(canonicalStringify(job), "utf8"); const reservation = captureReservation(job, bodyBytes);
      let canonicalInfo; try { canonicalInfo = await fs.lstat(file); } catch (error) { if (!errno(error, "ENOENT")) throw error; }
      if (canonicalInfo !== undefined) {
        if (!canonicalInfo.isFile() || canonicalInfo.isSymbolicLink() || (canonicalInfo.mode & 0o077) !== 0) throw new Error("Existing outbox job path is unsafe"); const existing = parseOutboxJob(await readSecureJson(fs, file), { host: input.host, nodeId, producerUuid, homeDir: input.homeDir });
        if (canonicalStringify(existing) !== canonicalStringify(job)) throw new Error("Deterministic outbox job collision"); const reservationFile = join(reservationsDir, `${reservation.reservationId}.json`);
        try { const persisted = validateReservation(await readSecureJson(fs, reservationFile)); if (canonicalStringify(persisted) !== canonicalStringify(reservation)) throw new Error("Outbox reservation collision"); } catch (reservationError) { if (!errno(reservationError, "ENOENT")) throw reservationError; }
        const readback = await durableReservationJobProof(reservation); if (readback === undefined || canonicalStringify(readback) !== canonicalStringify(job)) throw new Error("Existing outbox job lacks durable completion proof"); await finalizeAdmission(reservation, false); return jobFileValue(readback, file);
      }
      try { await ensureReservation(reservation); }
      catch (error) { const ownership = await admissionLockOwnership(reservation); if (ownership === "absent" || ownership === "other") await removeReservation(reservation); throw error; }
      let ownLock: CaptureReservation; try { ownLock = await acquireAdmissionLock(reservation); }
      catch (error) { const ownership = await admissionLockOwnership(reservation); if (error instanceof OutboxAdmissionBusyError || ownership === "absent" || ownership === "other") await removeReservation(reservation); throw error; }
      let usage: Awaited<ReturnType<typeof rootUsage>>;
      try { usage = await rootUsage(); await cleanupReservations(usage.reservations.filter((item) => item.reservationId !== reservation.reservationId)); usage = await rootUsage(); }
      catch (error) { await finalizeAdmission(ownLock); throw error; }
      if (usage.unsafeReservations) { await finalizeAdmission(ownLock); throw new Error("Outbox reservation state is unsafe"); }
      let projectedJobs = usage.jobs; let projectedBytes = usage.bytes; const selected = new Set<string>();
      for (const candidate of [...usage.reservations].sort((left, right) => left.reservationId.localeCompare(right.reservationId))) {
        if (projectedJobs + 1 <= maxJobs && projectedBytes + candidate.requestedBytes <= maxBytes) { projectedJobs += 1; projectedBytes += candidate.requestedBytes; selected.add(candidate.reservationId); }
      }
      if (!selected.has(reservation.reservationId)) {
        await finalizeAdmission(ownLock); try { input.notifyFull?.({ jobs: usage.jobs, bytes: usage.bytes, captureAllowed: false }); } catch { /* notification is best effort */ } throw new OutboxCapacityError();
      }
      try { await atomicWrite(fs, file, job, random); }
      catch (writeError) { let publicationAbsent = false; try { try { await fs.lstat(file); } catch (error) { if (!errno(error, "ENOENT")) throw error; const prefix = `${job.id}.json.tmp-`; if ((await fs.readdir(jobsDir)).some((name) => name.startsWith(prefix))) throw new Error("Outbox job temp remains"); await syncDirectory(fs, jobsDir); try { await fs.lstat(file); throw new Error("Outbox canonical job appeared during absence proof"); } catch (recheckError) { if (!errno(recheckError, "ENOENT")) throw recheckError; } if ((await fs.readdir(jobsDir)).some((name) => name.startsWith(prefix))) throw new Error("Outbox job temp appeared during absence proof"); publicationAbsent = true; } } catch { /* visible or ambiguous publication retains admission proof */ } if (publicationAbsent) await finalizeAdmission(ownLock); throw writeError; }
      const readback = parseOutboxJob(await readSecureJson(fs, file), { host: input.host, nodeId, producerUuid, homeDir: input.homeDir }); if (canonicalStringify(readback) !== canonicalStringify(job)) throw new Error("Outbox job readback failed");
      await finalizeAdmission(ownLock); return jobFileValue(readback, file);
    }),
    listPending: () => serialized(async () => { clock(); await assertProducerUnfenced(); return localPending(); }),
    quarantine: (job, category) => serialized(async () => {
      clock(); await assertProducerUnfenced(); validCategory(category); const id = idOf(job); const source = join(jobsDir, `${id}.json`); const destination = join(quarantineDir, `${id}.json`); let current: OutboxJob;
      try { current = parseOutboxJob(await readSecureJson(fs, source), { host: input.host, nodeId, producerUuid, homeDir: input.homeDir }); }
      catch (error) { if (!errno(error, "ENOENT")) throw error; current = parseOutboxJob(await readSecureJson(fs, destination), { host: input.host, nodeId, producerUuid, homeDir: input.homeDir }); await syncDirectory(fs, jobsDir); }
      if (record(job) && typeof job.auditHash === "string" && job.auditHash !== current.auditHash) throw new Error("Quarantine job audit mismatch"); const currentReservation = captureReservation(current, Buffer.byteLength(canonicalStringify(current), "utf8"));
      const reasonFile = join(quarantineDir, `${id}.reason.json`); const reason: PlainRecord = { version: 1, jobId: id, category, auditHash: "" }; reason.auditHash = hashWithout(reason, "auditHash"); await atomicWrite(fs, reasonFile, reason, random);
      try {
        try { await fs.link(source, destination); }
        catch (error) { if (!errno(error, "EEXIST") && !errno(error, "ENOENT")) throw error; const existing = parseOutboxJob(await readSecureJson(fs, destination), { host: input.host, nodeId, producerUuid, homeDir: input.homeDir }); if (existing.auditHash !== current.auditHash) throw new Error("Quarantine collision"); }
        await syncDirectory(fs, quarantineDir); if (parseOutboxJob(await readSecureJson(fs, destination), { host: input.host, nodeId, producerUuid, homeDir: input.homeDir }).auditHash !== current.auditHash) throw new Error("Quarantine readback failed");
        try { await fs.rm(source); } catch (error) { if (!errno(error, "ENOENT")) throw error; } await syncDirectory(fs, jobsDir);
      } catch (error) { try { await readSecureJson(fs, destination); } catch (destinationError) { if (errno(destinationError, "ENOENT")) try { await fs.rm(reasonFile); await syncDirectory(fs, quarantineDir); } catch { /* bounded orphan cleanup is best effort */ } } throw error; }
      const quarantineProof = await durableReservationJobProof(currentReservation); if (quarantineProof === undefined || canonicalStringify(quarantineProof) !== canonicalStringify(current)) throw new Error("Quarantine lacks durable accepted proof"); await finalizeAdmission(currentReservation, false);
      await fs.rm(join(controlDir, `${id}.json`), { force: true }); await syncDirectory(fs, controlDir);
    }),
    heartbeat: () => serialized(async () => { const heartbeatNow = clock(); await assertProducerUnfenced(); state = validateState(await readSecureJson(fs, stateFile)); if (state.state !== "active") throw new Error("Outbox producer is closed"); await writeState(producerState({ version: 1, state: "active", heartbeatAt: Math.max(state.heartbeatAt, heartbeatNow), closedAt: null })); }),
    closeProducer: () => serialized(async () => { const closeNow = clock(); await assertProducerUnfenced(); state = validateState(await readSecureJson(fs, stateFile)); if (state.state === "closed") return; const closedAt = Math.max(state.heartbeatAt, closeNow); await writeState(producerState({ version: 1, state: "closed", heartbeatAt: state.heartbeatAt, closedAt })); }),
    outboxStatus: () => serialized(async () => {
      clock(); await assertProducerUnfenced(); state = validateState(await readSecureJson(fs, stateFile)); const localJobs = await localPending(); let failedAttempts = 0;
      for (const job of localJobs) failedAttempts += await controlAttempts(job.id);
      const usage = await rootUsage();
      return { state: state.state, nodeId, producerUuid, jobs: usage.jobs, bytes: usage.bytes, oldestCreatedAt: localJobs.map((job) => job.createdAt).sort()[0] ?? null, failedAttempts, heartbeatAt: state.heartbeatAt, captureAllowed: state.state === "active" && !usage.unsafeReservations && usage.reservations.length === 0 && usage.jobs < maxJobs && usage.bytes < maxBytes };
    }),
  };
  return Object.freeze(api);
}
