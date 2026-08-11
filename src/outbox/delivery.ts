import * as nodeFs from "node:fs/promises";
import { constants as fsConstants } from "node:fs";
import { isAbsolute, join, relative, resolve, sep } from "node:path";
import { createHash, randomBytes } from "node:crypto";
import { canonicalStringify, deterministicUuid, sha256Hex } from "../domain/canonical.js";
import { canonicalRecordHash, episodeSemanticProjection, type EpisodeRecord, type ProcessingPolicyRecord } from "../domain/records.js";
import { intersectPolicies, isPolicyExpired, processingPolicyHash, type ProcessingPolicy } from "../domain/policy.js";
import { redactAndScan } from "../security/redaction.js";
import type { AuthorizedDestination } from "../types.js";
import type { OutboxFileSystem, OutboxJob } from "./store.js";
import { parseOutboxJob } from "./store.js";
import { activeAdmissionLocks, isAdmissionProtocolArtifact, retireOwnedAdmissionLock } from "./reservation-protocol.js";
import { assertPseudonymousNodeId } from "../security/egress.js";
import { QdrantContentHashCollisionError } from "../domain/qdrant-errors.js";
import { expectedQdrantCollection } from "../qdrant/client.js";

export interface OutboxJobProcessor {
  process(job: OutboxJob, input: { signal?: AbortSignal }): Promise<{ status: "delivered" | "pending" | "quarantined"; category?: string }>;
}
export interface DeliveryResult { delivered: number; pending: number; quarantined: number; }
export interface DeliveryInput {
  outboxRoot: string;
  producerPath: string;
  processor: OutboxJobProcessor;
  now: () => number;
  maxClockSkewMs: number;
  retryBaseMs?: number;
  retryMaxMs?: number;
  heartbeatTimeoutMs?: number;
  attemptTimeoutMs?: number;
  fs?: Partial<OutboxFileSystem>;
}
export interface OutboxDelivery {
  deliver(input: { signal?: AbortSignal; maxJobs?: number }): Promise<DeliveryResult>;
  adopt(producerPath: string): Promise<void>;
  shutdown(input?: { signal?: AbortSignal; maxJobs?: number }): Promise<DeliveryResult>;
}

type PlainRecord = Record<string, unknown>;
interface ProducerIdentity {
  version: 1; ownerHost: "pi" | "prime"; nodeId: string; producerUuid: string; sharedFilesystem: boolean;
  explicitNodeId: boolean; machineAuditHash: string; createdAt: string; auditHash: string;
}
interface ProducerState { version: 1; state: "active" | "closed"; heartbeatAt: number; closedAt: number | null; auditHash: string; }
interface RetryControl { version: 1; jobId: string; attempts: number; nextAttemptAt: number; lastCategory: string; auditHash: string; }
interface ExpiryAudit { version: 1; kind: "expired"; jobId: string; payloadAuditHash: string; policyId: string; deadline: string; expiredAt: string; auditHash: string; }
interface DeliveredAudit { version: 1; kind: "delivered"; status: "delivered"; jobId: string; payloadAuditHash: string; deliveredAt: string; auditHash: string; }
interface ProducerFiles { path: string; nodeId: string; producerUuid: string; identity: ProducerIdentity; state: ProducerState; jobsDir: string; controlDir: string; quarantineDir: string; fenced: boolean; fencePublished: boolean; fenceAuditHash: string | null; }
interface JobsFence { version: 1; kind: "producer_jobs_fence"; nodeId: string; producerUuid: string; jobsDir: string; auditHash: string; }
interface CaptureReservation { version: 1; reservationId: string; jobId: string; jobAuditHash: string; policyId: string; deadline: string | null; nodeId: string; producerUuid: string; requestedBytes: number; auditHash: string; }

const SAFE_COMPONENT = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/u;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;
const PRODUCER_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;
const HASH = /^[0-9a-f]{64}$/u;
const ISO_DATE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/u;
const MAX_TIME = Date.parse("2100-12-31T23:59:59.999Z");
const PRODUCER_KEYS = ["version", "ownerHost", "nodeId", "producerUuid", "sharedFilesystem", "explicitNodeId", "machineAuditHash", "createdAt", "auditHash"] as const;
const STATE_KEYS = ["version", "state", "heartbeatAt", "closedAt", "auditHash"] as const;
const NODE_KEYS = ["version", "nodeId", "machineAuditHash", "auditHash"] as const;
const CONTROL_KEYS = ["version", "jobId", "attempts", "nextAttemptAt", "lastCategory", "auditHash"] as const;
const EXPIRY_KEYS = ["version", "kind", "jobId", "payloadAuditHash", "policyId", "deadline", "expiredAt", "auditHash"] as const;
const DELIVERED_KEYS = ["version", "kind", "status", "jobId", "payloadAuditHash", "deliveredAt", "auditHash"] as const;
const FENCE_KEYS = ["version", "kind", "nodeId", "producerUuid", "jobsDir", "auditHash"] as const;
const FENCED_DIR = /^jobs\.fenced-([a-f0-9]{32})$/u;
const RESERVATION_KEYS = ["version", "reservationId", "jobId", "jobAuditHash", "policyId", "deadline", "nodeId", "producerUuid", "requestedBytes", "auditHash"] as const;
const PRECOMMIT_KEYS = ["version", "kind", "jobId", "reservationId", "jobAuditHash", "policyId", "deadline", "nodeId", "producerUuid", "fenceAuditHash", "sourceHash", "byteLength", "auditHash"] as const;
const REASON_KEYS = ["version", "jobId", "category", "auditHash"] as const;

function record(value: unknown): value is PlainRecord { return typeof value === "object" && value !== null && !Array.isArray(value); }
function exactKeys(value: PlainRecord, expected: readonly string[]): boolean { const actual = Object.keys(value).sort(); const keys = [...expected].sort(); return actual.length === keys.length && actual.every((key, index) => key === keys[index]); }
function errno(error: unknown, code: string): boolean { return record(error) && error.code === code; }
function hashWithout(value: PlainRecord, key: string): string { const copy = { ...value }; delete copy[key]; return sha256Hex(canonicalStringify(copy)); }
function finiteTime(value: unknown): value is number { return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 && value <= MAX_TIME; }
function safeComponent(value: unknown): value is string { return typeof value === "string" && SAFE_COMPONENT.test(value) && value !== "." && value !== ".." && value !== "local" && !/(?:api[-_]?key|access[-_]?token|authorization|bearer|credential|password|secret|token)/iu.test(value); }
function safeCategory(value: unknown, fallback: string): string { return typeof value === "string" && value.length > 0 && value.length <= 128 && /^[A-Za-z0-9._:-]+$/u.test(value) && !/(?:api[-_]?key|access[-_]?token|authorization|bearer|credential|password|secret|token)/iu.test(value) ? value : fallback; }
function assertInside(root: string, candidate: string): void { const rel = relative(root, candidate); if (rel === "" || rel === ".." || rel.startsWith(`..${sep}`) || isAbsolute(rel)) throw new Error("Adopted producer path is outside the validated outbox root"); }
async function noSymlinks(fs: OutboxFileSystem, path: string): Promise<boolean> {
  const absolute = resolve(path); if (!isAbsolute(absolute)) return false;
  const pieces = absolute.split(sep).filter(Boolean); let cursor = absolute.startsWith(sep) ? sep : "";
  for (const piece of pieces) { cursor = cursor === sep ? `${cursor}${piece}` : `${cursor}${sep}${piece}`; try { if ((await fs.lstat(cursor)).isSymbolicLink()) return false; } catch (error) { if (errno(error, "ENOENT")) return false; return false; } }
  return true;
}
async function syncDirectory(fs: OutboxFileSystem, path: string): Promise<void> { const handle = await fs.open(path, "r"); try { await handle.sync(); } finally { await handle.close(); } }
async function privateDirectoryIdentity(fs: OutboxFileSystem, dir: string, parent: string): Promise<{ dev: number; ino: number }> { const info = await fs.lstat(dir); if (!info.isDirectory() || info.isSymbolicLink() || (info.mode & 0o077) !== 0 || await fs.realpath(dir) !== dir) throw new Error("Outbox proof directory is unsafe"); return { dev: info.dev, ino: info.ino }; }
async function atomicWrite(fs: OutboxFileSystem, path: string, value: unknown): Promise<void> {
  const parent = resolve(path, ".."); if (!(await noSymlinks(fs, parent))) throw new Error("Outbox control path is unsafe");
  const temp = `${path}.tmp-${process.pid}-${randomBytes(16).toString("hex")}`; assertInside(parent, temp);
  const handle = await fs.open(temp, "wx", 0o600);
  try { await handle.writeFile(canonicalStringify(value), "utf8"); await handle.sync(); } catch (error) { await handle.close().catch(() => undefined); await fs.rm(temp, { force: true }).catch(() => undefined); throw error; }
  await handle.close();
  try { await fs.chmod(temp, 0o600); await fs.rename(temp, path); await syncDirectory(fs, parent); } catch (error) { await fs.rm(temp, { force: true }).catch(() => undefined); throw error; }
}
async function readSecureJson(fs: OutboxFileSystem, path: string): Promise<unknown> {
  const info = await fs.lstat(path);
  if (!info.isFile() || info.isSymbolicLink() || (info.mode & 0o077) !== 0) throw new Error("Outbox state file is unsafe");
  return JSON.parse(await fs.readFile(path, "utf8"));
}
async function readExactCanonicalJson<T>(fs: OutboxFileSystem, path: string, validate: (value: unknown) => T): Promise<T> { const info = await fs.lstat(path); if (!info.isFile() || info.isSymbolicLink() || (info.mode & 0o077) !== 0 || info.size < 1 || info.size > 1_048_576) throw new Error("Outbox audit file is unsafe"); let handle; let bytes: Uint8Array; try { handle = await fs.open(path, fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW); const opened = await handle.stat(); if (!opened.isFile() || opened.dev !== info.dev || opened.ino !== info.ino || opened.size !== info.size || (opened.mode & 0o077) !== 0) throw new Error("Outbox audit inode changed"); bytes = await handle.readFile(); } finally { await handle?.close().catch(() => undefined); } const after = await fs.lstat(path); if (!after.isFile() || after.isSymbolicLink() || after.dev !== info.dev || after.ino !== info.ino || after.size !== info.size || bytes.length !== info.size || (after.mode & 0o077) !== 0) throw new Error("Outbox audit path changed"); const text = Buffer.from(bytes).toString("utf8"); const parsed = validate(JSON.parse(text)); if (text !== canonicalStringify(parsed)) throw new Error("Outbox audit bytes are not canonical"); return parsed; }
function validateProducer(value: unknown, nodeId: string, producerUuid: string): ProducerIdentity {
  if (!record(value) || !exactKeys(value, PRODUCER_KEYS) || value.version !== 1 || (value.ownerHost !== "pi" && value.ownerHost !== "prime") || value.nodeId !== nodeId || value.producerUuid !== producerUuid || !PRODUCER_UUID.test(producerUuid) || typeof value.sharedFilesystem !== "boolean" || typeof value.explicitNodeId !== "boolean" || (value.sharedFilesystem && !value.explicitNodeId)) throw new Error("Adopted producer identity is malformed or mismatched");
  try { assertPseudonymousNodeId(value.nodeId, { allowDerivedDigest: !value.explicitNodeId }); } catch { throw new Error("Adopted producer identity is malformed or mismatched"); }
  if (typeof value.machineAuditHash !== "string" || !HASH.test(value.machineAuditHash) || typeof value.createdAt !== "string" || !ISO_DATE.test(value.createdAt) || new Date(value.createdAt).toISOString() !== value.createdAt || value.auditHash !== hashWithout(value, "auditHash")) throw new Error("Adopted producer identity is malformed or mismatched");
  return value as unknown as ProducerIdentity;
}
function validateState(value: unknown): ProducerState {
  if (!record(value) || !exactKeys(value, STATE_KEYS) || value.version !== 1 || (value.state !== "active" && value.state !== "closed") || !finiteTime(value.heartbeatAt) || (value.closedAt !== null && !finiteTime(value.closedAt)) || (value.state === "active" && value.closedAt !== null) || (value.state === "closed" && (value.closedAt === null || value.closedAt < value.heartbeatAt)) || value.auditHash !== hashWithout(value, "auditHash")) throw new Error("Adopted producer state is malformed");
  return value as unknown as ProducerState;
}
function validateNode(value: unknown, identity: ProducerIdentity): void {
  if (!record(value) || !exactKeys(value, NODE_KEYS) || value.version !== 1 || value.nodeId !== identity.nodeId || value.machineAuditHash !== identity.machineAuditHash || value.auditHash !== hashWithout(value, "auditHash")) throw new Error("Adopted node identity is malformed or mismatched");
}
function retryControl(value: unknown, jobId: string): RetryControl {
  if (!record(value) || !exactKeys(value, CONTROL_KEYS) || value.version !== 1 || value.jobId !== jobId || !Number.isSafeInteger(value.attempts) || (value.attempts as number) < 1 || (value.attempts as number) > 1_000_000 || !finiteTime(value.nextAttemptAt) || typeof value.lastCategory !== "string" || safeCategory(value.lastCategory, "") !== value.lastCategory || value.auditHash !== hashWithout(value, "auditHash")) throw new Error("Outbox retry control is malformed");
  return value as unknown as RetryControl;
}
function reservation(value: unknown): CaptureReservation {
  if (!record(value) || !exactKeys(value, RESERVATION_KEYS) || value.version !== 1 || typeof value.reservationId !== "string" || !UUID.test(value.reservationId) || typeof value.jobId !== "string" || !UUID.test(value.jobId) || typeof value.jobAuditHash !== "string" || !HASH.test(value.jobAuditHash) || typeof value.policyId !== "string" || !HASH.test(value.policyId) || (value.deadline !== null && (typeof value.deadline !== "string" || !ISO_DATE.test(value.deadline) || !Number.isFinite(Date.parse(value.deadline)))) || typeof value.nodeId !== "string" || typeof value.producerUuid !== "string" || !PRODUCER_UUID.test(value.producerUuid) || !Number.isSafeInteger(value.requestedBytes) || (value.requestedBytes as number) < 1 || (value.requestedBytes as number) > 1_073_741_824 || value.auditHash !== hashWithout(value, "auditHash")) throw new Error("Outbox reservation is malformed"); assertPseudonymousNodeId(value.nodeId, { allowDerivedDigest: true }); if (value.reservationId !== deterministicUuid("pi-qdrant-memory-v2:outbox-reservation", value.nodeId, value.producerUuid, value.jobId)) throw new Error("Outbox reservation identity is malformed"); return value as unknown as CaptureReservation;
}
function makeControl(jobId: string, attempts: number, nextAttemptAt: number, lastCategory: string): RetryControl { const value: RetryControl = { version: 1, jobId, attempts, nextAttemptAt, lastCategory, auditHash: "" }; value.auditHash = hashWithout(value as unknown as PlainRecord, "auditHash"); return value; }
async function canonicalRoot(fs: OutboxFileSystem, root: string): Promise<string> {
  if (typeof root !== "string" || !isAbsolute(root)) throw new Error("Outbox root must be absolute and canonical");
  const resolved = resolve(root); if (root !== resolved) throw new Error("Outbox root must be absolute and canonical");
  if (!(await noSymlinks(fs, resolved))) throw new Error("Outbox root contains a symlink or is unavailable");
  const canonical = await fs.realpath(resolved); if (canonical !== resolved || !resolved.endsWith(`${sep}pi-qdrant-memory${sep}outbox`)) throw new Error("Outbox root is not a canonical validated host-agent outbox");
  const info = await fs.lstat(resolved); if (!info.isDirectory() || info.isSymbolicLink() || (info.mode & 0o077) !== 0) throw new Error("Outbox root permissions are unsafe");
  return resolved;
}
function validateFence(value: unknown, nodeId: string, producerUuid: string): JobsFence {
  if (!record(value) || !exactKeys(value, FENCE_KEYS) || value.version !== 1 || value.kind !== "producer_jobs_fence" || value.nodeId !== nodeId || value.producerUuid !== producerUuid || typeof value.jobsDir !== "string" || !FENCED_DIR.test(value.jobsDir) || value.auditHash !== hashWithout(value, "auditHash")) throw new Error("Producer jobs fence is malformed");
  return value as unknown as JobsFence;
}
function jobsFence(nodeId: string, producerUuid: string, jobsDir: string): JobsFence { const value: JobsFence = { version: 1, kind: "producer_jobs_fence", nodeId, producerUuid, jobsDir, auditHash: "" }; value.auditHash = hashWithout(value as unknown as PlainRecord, "auditHash"); return value; }
async function directoryState(fs: OutboxFileSystem, path: string): Promise<"absent" | "safe"> { try { const info = await fs.lstat(path); if (!info.isDirectory() || info.isSymbolicLink() || (info.mode & 0o077) !== 0 || await fs.realpath(path) !== path) throw new Error("Producer jobs directory is unsafe"); return "safe"; } catch (error) { if (errno(error, "ENOENT")) return "absent"; throw error; } }
async function producerFiles(fs: OutboxFileSystem, root: string, producerPath: string, allowIncompleteFence = false): Promise<ProducerFiles> {
  if (!isAbsolute(producerPath)) throw new Error("Producer path must be absolute and canonical"); const resolved = resolve(producerPath); if (producerPath !== resolved) throw new Error("Producer path must be absolute and canonical"); assertInside(root, resolved);
  const rel = relative(root, resolved); const pieces = rel.split(sep); if (pieces.length !== 2 || !safeComponent(pieces[0]) || typeof pieces[1] !== "string" || !PRODUCER_UUID.test(pieces[1])) throw new Error("Producer path is not a direct node/producer directory");
  if (!(await noSymlinks(fs, resolved)) || await fs.realpath(resolved) !== resolved) throw new Error("Producer path contains a symlink or is not canonical"); const info = await fs.lstat(resolved); if (!info.isDirectory() || info.isSymbolicLink() || (info.mode & 0o077) !== 0) throw new Error("Producer directory permissions are unsafe");
  const nodeId = pieces[0]; const producerUuid = pieces[1]; const nodeInfo = await fs.lstat(join(root, nodeId)); if (!nodeInfo.isDirectory() || nodeInfo.isSymbolicLink() || (nodeInfo.mode & 0o077) !== 0) throw new Error("Adopted node directory is unsafe");
  const identity = validateProducer(await readSecureJson(fs, join(resolved, "producer.json")), nodeId, producerUuid); validateNode(await readSecureJson(fs, join(root, nodeId, "node.json")), identity); const state = validateState(await readSecureJson(fs, join(resolved, "state.json")));
  for (const name of ["control", "quarantine"] as const) { if (await directoryState(fs, join(resolved, name)) !== "safe") throw new Error("Producer state directory is missing"); }
  const names = await fs.readdir(resolved); const fencedNames = names.filter((name) => FENCED_DIR.test(name)).sort(); let manifest: JobsFence | undefined;
  try { manifest = validateFence(await readSecureJson(fs, join(resolved, "fence.json")), nodeId, producerUuid); } catch (error) { if (!errno(error, "ENOENT")) throw error; }
  const original = await directoryState(fs, join(resolved, "jobs"));
  if (manifest !== undefined) { if (original !== "absent" || fencedNames.length !== 1 || fencedNames[0] !== manifest.jobsDir || await directoryState(fs, join(resolved, manifest.jobsDir)) !== "safe") throw new Error("Producer jobs fence namespace is ambiguous"); return { path: resolved, nodeId, producerUuid, identity, state, jobsDir: join(resolved, manifest.jobsDir), controlDir: join(resolved, "control"), quarantineDir: join(resolved, "quarantine"), fenced: true, fencePublished: true, fenceAuditHash: manifest.auditHash }; }
  if (fencedNames.length !== 0 || original === "absent") { let discovered = fencedNames; if (allowIncompleteFence && original === "absent" && discovered.length === 0) discovered = (await fs.readdir(resolved)).filter((name) => FENCED_DIR.test(name)).sort(); if (!allowIncompleteFence || original !== "absent" || discovered.length !== 1 || await directoryState(fs, join(resolved, discovered[0]!)) !== "safe") throw new Error("Producer has an incomplete or ambiguous jobs fence"); return { path: resolved, nodeId, producerUuid, identity, state, jobsDir: join(resolved, discovered[0]!), controlDir: join(resolved, "control"), quarantineDir: join(resolved, "quarantine"), fenced: true, fencePublished: false, fenceAuditHash: null }; }
  return { path: resolved, nodeId, producerUuid, identity, state, jobsDir: join(resolved, "jobs"), controlDir: join(resolved, "control"), quarantineDir: join(resolved, "quarantine"), fenced: false, fencePublished: false, fenceAuditHash: null };
}
async function fenceProducer(fs: OutboxFileSystem, root: string, initial: ProducerFiles): Promise<ProducerFiles> {
  if (initial.fenced && initial.fencePublished) return initial; let fencedName = initial.fenced ? initial.jobsDir.slice(initial.path.length + 1) : "";
  if (!initial.fenced) { fencedName = `jobs.fenced-${randomBytes(16).toString("hex")}`; try { await fs.rename(join(initial.path, "jobs"), join(initial.path, fencedName)); await syncDirectory(fs, initial.path); } catch (error) { if (!errno(error, "ENOENT")) throw error; const recovered = await producerFiles(fs, root, initial.path, true); if (!recovered.fenced) throw new Error("Producer jobs fence race did not converge"); fencedName = recovered.jobsDir.slice(recovered.path.length + 1); } }
  const value = jobsFence(initial.nodeId, initial.producerUuid, fencedName); const manifestFile = join(initial.path, "fence.json");
  try { const existing = validateFence(await readSecureJson(fs, manifestFile), initial.nodeId, initial.producerUuid); if (canonicalStringify(existing) !== canonicalStringify(value)) throw new Error("Producer jobs fence collision"); }
  catch (error) { if (!errno(error, "ENOENT")) throw error; await atomicWrite(fs, manifestFile, value); }
  await syncDirectory(fs, initial.path); const readback = validateFence(await readSecureJson(fs, manifestFile), initial.nodeId, initial.producerUuid); if (canonicalStringify(readback) !== canonicalStringify(value)) throw new Error("Producer jobs fence readback failed"); return producerFiles(fs, root, initial.path);
}
const JOB_TEMP = /^([0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})\.json\.tmp-([0-9]+)-([a-f0-9]{32})$/u;
type PrecommitDecisionKind = "accepted_precommit" | "partial_precommit" | "aborted_precommit";
function precommitDecision(value: CaptureReservation, producer: ProducerFiles, kind: PrecommitDecisionKind, bytes?: Uint8Array): PlainRecord { if (producer.fenceAuditHash === null || (kind !== "aborted_precommit" && bytes === undefined)) throw new Error("Precommit decision requires a published fence and exact source bytes"); const decision: PlainRecord = { version: 1, kind, jobId: value.jobId, reservationId: value.reservationId, jobAuditHash: value.jobAuditHash, policyId: value.policyId, deadline: value.deadline, nodeId: value.nodeId, producerUuid: value.producerUuid, fenceAuditHash: producer.fenceAuditHash, sourceHash: bytes === undefined ? null : createHash("sha256").update(bytes).digest("hex"), byteLength: bytes?.length ?? 0, auditHash: "" }; decision.auditHash = hashWithout(decision, "auditHash"); return decision; }
function validatePrecommitDecision(input: unknown, value: CaptureReservation, producer: ProducerFiles): PlainRecord { if (!record(input) || !exactKeys(input, PRECOMMIT_KEYS) || (input.kind !== "accepted_precommit" && input.kind !== "partial_precommit" && input.kind !== "aborted_precommit") || input.version !== 1 || input.jobId !== value.jobId || input.reservationId !== value.reservationId || input.jobAuditHash !== value.jobAuditHash || input.policyId !== value.policyId || input.deadline !== value.deadline || input.nodeId !== value.nodeId || input.producerUuid !== value.producerUuid || input.fenceAuditHash !== producer.fenceAuditHash || (input.kind === "aborted_precommit" ? (input.sourceHash !== null || input.byteLength !== 0) : (typeof input.sourceHash !== "string" || !HASH.test(input.sourceHash) || !Number.isSafeInteger(input.byteLength) || (input.byteLength as number) < 0 || (input.byteLength as number) > value.requestedBytes || (input.kind === "accepted_precommit" && input.byteLength !== value.requestedBytes))) || input.auditHash !== hashWithout(input, "auditHash")) throw new Error("Precommit decision is malformed or mismatched"); return input; }
async function readExactPrecommitDecision(fs: OutboxFileSystem, file: string, value: CaptureReservation, producer: ProducerFiles): Promise<PlainRecord> { const info = await fs.lstat(file); if (!info.isFile() || info.isSymbolicLink() || (info.mode & 0o077) !== 0 || info.size < 1 || info.size > 16_384) throw new Error("Precommit decision path is unsafe"); let handle; let bytes: Uint8Array; try { handle = await fs.open(file, fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW); const opened = await handle.stat(); if (!opened.isFile() || opened.dev !== info.dev || opened.ino !== info.ino || opened.size !== info.size || (opened.mode & 0o077) !== 0) throw new Error("Precommit decision inode changed"); bytes = await handle.readFile(); } finally { await handle?.close().catch(() => undefined); } const after = await fs.lstat(file); if (!after.isFile() || after.isSymbolicLink() || after.dev !== info.dev || after.ino !== info.ino || after.size !== info.size || bytes.length !== info.size || (after.mode & 0o077) !== 0) throw new Error("Precommit decision path changed"); const text = Buffer.from(bytes).toString("utf8"); const parsed = validatePrecommitDecision(JSON.parse(text), value, producer); if (text !== canonicalStringify(parsed)) throw new Error("Precommit decision bytes are not canonical"); return parsed; }
async function durablePrecommitDecision(fs: OutboxFileSystem, producer: ProducerFiles, value: CaptureReservation): Promise<PlainRecord | undefined> { const identity = await privateDirectoryIdentity(fs, producer.quarantineDir, producer.path); const file = join(producer.quarantineDir, `precommit-${value.reservationId}.json`); let first: PlainRecord; try { first = await readExactPrecommitDecision(fs, file, value, producer); } catch (error) { if (errno(error, "ENOENT")) return undefined; throw error; } await syncDirectory(fs, producer.quarantineDir); const after = await privateDirectoryIdentity(fs, producer.quarantineDir, producer.path); if (after.dev !== identity.dev || after.ino !== identity.ino) throw new Error("Precommit decision directory changed"); const second = await readExactPrecommitDecision(fs, file, value, producer); if (canonicalStringify(second) !== canonicalStringify(first)) throw new Error("Precommit decision readback changed"); return second; }
async function publishPrecommitDecision(fs: OutboxFileSystem, producer: ProducerFiles, value: CaptureReservation, kind: PrecommitDecisionKind, bytes?: Uint8Array): Promise<PlainRecord> { const expected = precommitDecision(value, producer, kind, bytes); const file = join(producer.quarantineDir, `precommit-${value.reservationId}.json`); const existing = await durablePrecommitDecision(fs, producer, value); if (existing !== undefined) return existing; const temp = join(producer.quarantineDir, `.precommit-${value.reservationId}.tmp-${process.pid}-${randomBytes(16).toString("hex")}`); const body = canonicalStringify(expected); const handle = await fs.open(temp, "wx", 0o600); try { await handle.writeFile(body, "utf8"); await handle.sync(); } catch (error) { await handle.close().catch(() => undefined); await fs.rm(temp, { force: true }).catch(() => undefined); throw error; } await handle.close(); await fs.chmod(temp, 0o600); try { await fs.link(temp, file); } catch (error) { if (!errno(error, "EEXIST")) throw error; } await syncDirectory(fs, producer.quarantineDir); const winner = await durablePrecommitDecision(fs, producer, value); if (winner === undefined) throw new Error("Precommit decision publication disappeared"); await durableRemove(fs, temp, producer.quarantineDir); return winner; }
async function provePrecommitAbsence(fs: OutboxFileSystem, producer: ProducerFiles, value: CaptureReservation): Promise<void> { if (!producer.fenced || !producer.fencePublished || producer.fenceAuditHash === null) throw new Error("Precommit absence requires a durable fence"); const jobsIdentity = await privateDirectoryIdentity(fs, producer.jobsDir, producer.path); const quarantineIdentity = await privateDirectoryIdentity(fs, producer.quarantineDir, producer.path); const files = [join(producer.jobsDir, `${value.jobId}.json`), join(producer.quarantineDir, `${value.jobId}.json`)]; const check = async (): Promise<void> => { for (const file of files) try { await fs.lstat(file); throw new Error("Accepted payload exists during non-acceptance proof"); } catch (error) { if (!errno(error, "ENOENT")) throw error; } if ((await fs.readdir(producer.jobsDir)).some((name) => JOB_TEMP.exec(name)?.[1] === value.jobId)) throw new Error("Precommit payload temp remains"); }; await check(); await syncDirectory(fs, producer.jobsDir); await syncDirectory(fs, producer.quarantineDir); const jobsAfter = await privateDirectoryIdentity(fs, producer.jobsDir, producer.path); const quarantineAfter = await privateDirectoryIdentity(fs, producer.quarantineDir, producer.path); if (jobsAfter.dev !== jobsIdentity.dev || jobsAfter.ino !== jobsIdentity.ino || quarantineAfter.dev !== quarantineIdentity.dev || quarantineAfter.ino !== quarantineIdentity.ino) throw new Error("Precommit proof directory changed"); await check(); }
async function durableNonAcceptanceProof(fs: OutboxFileSystem, producer: ProducerFiles, value: CaptureReservation): Promise<boolean> { const decision = await durablePrecommitDecision(fs, producer, value); if (decision === undefined || decision.kind === "accepted_precommit") return false; await provePrecommitAbsence(fs, producer, value); return true; }
async function durableAcceptedTerminalDecisionProof(fs: OutboxFileSystem, producer: ProducerFiles, value: CaptureReservation, decision: PlainRecord): Promise<boolean> { if (decision.kind !== "accepted_precommit") return false; const expiryFile = join(producer.quarantineDir, `${value.jobId}.expired.json`); const deliveredFile = join(producer.quarantineDir, `${value.jobId}.delivered.json`); let first: PlainRecord | undefined; let file = ""; try { const expiry = await readExactCanonicalJson(fs, expiryFile, (input) => { if (!record(input) || !exactKeys(input, EXPIRY_KEYS) || input.version !== 1 || input.kind !== "expired" || input.jobId !== value.jobId || input.payloadAuditHash !== value.jobAuditHash || input.policyId !== value.policyId || input.deadline !== value.deadline || input.expiredAt !== value.deadline || input.auditHash !== hashWithout(input, "auditHash")) throw new Error("Expiry decision proof is malformed or mismatched"); return input; }); first = expiry; file = expiryFile; } catch (error) { if (!errno(error, "ENOENT")) throw error; } if (first === undefined) try { const delivered = await readExactCanonicalJson(fs, deliveredFile, (input) => { const audit = validateDeliveredAudit(input); if (audit.jobId !== value.jobId || audit.payloadAuditHash !== value.jobAuditHash) throw new Error("Delivered decision proof is mismatched"); return audit as unknown as PlainRecord; }); first = delivered; file = deliveredFile; } catch (error) { if (!errno(error, "ENOENT")) throw error; } if (first === undefined) return false; await syncDirectory(fs, producer.quarantineDir); const second = file === expiryFile ? await readExactCanonicalJson(fs, file, (input) => { if (!record(input) || !exactKeys(input, EXPIRY_KEYS) || input.version !== 1 || input.kind !== "expired" || input.jobId !== value.jobId || input.payloadAuditHash !== value.jobAuditHash || input.policyId !== value.policyId || input.deadline !== value.deadline || input.expiredAt !== value.deadline || input.auditHash !== hashWithout(input, "auditHash")) throw new Error("Expiry decision proof readback is mismatched"); return input; }) : await readExactCanonicalJson(fs, file, (input) => validateDeliveredAudit(input) as unknown as PlainRecord); if (canonicalStringify(second) !== canonicalStringify(first)) throw new Error("Accepted terminal decision proof changed"); await provePrecommitAbsence(fs, producer, value); return true; }
function orphanPrecommitAudit(jobId: string, bytes: Uint8Array): PlainRecord { const audit: PlainRecord = { version: 1, kind: "orphan_precommit", jobId, sourceHash: createHash("sha256").update(bytes).digest("hex"), byteLength: bytes.length, auditHash: "" }; audit.auditHash = hashWithout(audit, "auditHash"); return audit; }
async function readSecureArtifactBytes(fs: OutboxFileSystem, file: string, maxBytes: number): Promise<Uint8Array> { const info = await fs.lstat(file); if (!info.isFile() || info.isSymbolicLink() || (info.mode & 0o077) !== 0 || info.size < 0 || info.size > maxBytes) throw new Error("Precommit artifact path is unsafe"); let handle; let bytes: Uint8Array; try { handle = await fs.open(file, fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW); const opened = await handle.stat(); if (!opened.isFile() || opened.dev !== info.dev || opened.ino !== info.ino || opened.size !== info.size || (opened.mode & 0o077) !== 0) throw new Error("Precommit artifact inode changed"); bytes = await handle.readFile(); } finally { await handle?.close().catch(() => undefined); } const after = await fs.lstat(file); if (!after.isFile() || after.isSymbolicLink() || after.dev !== info.dev || after.ino !== info.ino || after.size !== info.size || bytes.length !== info.size || (after.mode & 0o077) !== 0) throw new Error("Precommit artifact path changed"); return bytes; }
async function durablePreparedTempAudit(fs: OutboxFileSystem, producer: ProducerFiles, name: string, reservationId: string): Promise<PlainRecord | undefined> { const auditFile = join(producer.quarantineDir, `precommit-prepared-${sha256Hex(name).slice(0, 32)}.json`); const keys = ["version", "kind", "nodeId", "producerUuid", "reservationId", "fenceAuditHash", "artifactNameHash", "sourceHash", "byteLength", "auditHash"] as const; const validate = (input: unknown): PlainRecord => { if (!record(input) || !exactKeys(input, keys) || input.version !== 1 || input.kind !== "orphan_prepared_precommit" || input.nodeId !== producer.nodeId || input.producerUuid !== producer.producerUuid || input.reservationId !== reservationId || input.fenceAuditHash !== producer.fenceAuditHash || input.artifactNameHash !== sha256Hex(name) || typeof input.sourceHash !== "string" || !HASH.test(input.sourceHash) || !Number.isSafeInteger(input.byteLength) || (input.byteLength as number) < 0 || (input.byteLength as number) > 1_048_576 || input.auditHash !== hashWithout(input, "auditHash")) throw new Error("Prepared-temp audit is malformed or mismatched"); return input; }; let first: PlainRecord; try { first = await readExactCanonicalJson(fs, auditFile, validate); } catch (error) { if (errno(error, "ENOENT")) return undefined; throw error; } await syncDirectory(fs, producer.quarantineDir); const second = await readExactCanonicalJson(fs, auditFile, validate); if (canonicalStringify(first) !== canonicalStringify(second)) throw new Error("Prepared-temp audit readback changed"); return second; }
async function auditAndRemovePreparedTemp(fs: OutboxFileSystem, producer: ProducerFiles, reservationsDir: string, file: string, name: string, reservationId: string, bytes: Uint8Array): Promise<void> { if (producer.fenceAuditHash === null) throw new Error("Prepared-temp cleanup requires a durable fence"); let winner = await durablePreparedTempAudit(fs, producer, name, reservationId); if (winner === undefined) { const audit: PlainRecord = { version: 1, kind: "orphan_prepared_precommit", nodeId: producer.nodeId, producerUuid: producer.producerUuid, reservationId, fenceAuditHash: producer.fenceAuditHash, artifactNameHash: sha256Hex(name), sourceHash: createHash("sha256").update(bytes).digest("hex"), byteLength: bytes.length, auditHash: "" }; audit.auditHash = hashWithout(audit, "auditHash"); const auditFile = join(producer.quarantineDir, `precommit-prepared-${sha256Hex(name).slice(0, 32)}.json`); const temp = join(producer.quarantineDir, `.precommit-prepared-${sha256Hex(name).slice(0, 32)}.tmp-${process.pid}-${randomBytes(16).toString("hex")}`); const handle = await fs.open(temp, "wx", 0o600); try { await handle.writeFile(canonicalStringify(audit), "utf8"); await handle.sync(); } catch (error) { await handle.close().catch(() => undefined); await fs.rm(temp, { force: true }).catch(() => undefined); throw error; } await handle.close(); await fs.chmod(temp, 0o600); try { await fs.link(temp, auditFile); } catch (error) { if (!errno(error, "EEXIST")) throw error; } await syncDirectory(fs, producer.quarantineDir); winner = await durablePreparedTempAudit(fs, producer, name, reservationId); if (winner === undefined) throw new Error("Prepared-temp audit publication disappeared"); await durableRemove(fs, temp, producer.quarantineDir); } await durableRemove(fs, file, reservationsDir); }
function decisionMatchesBytes(decision: PlainRecord, bytes: Uint8Array): boolean { return decision.sourceHash === createHash("sha256").update(bytes).digest("hex") && decision.byteLength === bytes.length; }
async function convergeNonAcceptanceDecision(fs: OutboxFileSystem, root: string, producer: ProducerFiles, value: CaptureReservation): Promise<boolean> { const decision = await durablePrecommitDecision(fs, producer, value); if (decision === undefined || decision.kind === "accepted_precommit") return false; for (const name of await fs.readdir(producer.jobsDir)) if (JOB_TEMP.exec(name)?.[1] === value.jobId) await durableRemove(fs, join(producer.jobsDir, name), producer.jobsDir); const accepted = await durableReservationJobProof(fs, producer, value); if (accepted !== undefined) for (const dir of [producer.jobsDir, producer.quarantineDir]) { const file = join(dir, `${value.jobId}.json`); try { const copy = await readAcceptedJob(fs, file, producer); if (!reservationMatchesJob(value, producer, copy) || canonicalStringify(copy) !== canonicalStringify(accepted)) throw new Error("Audited precommit payload collision"); await durableRemove(fs, file, dir); } catch (error) { if (!errno(error, "ENOENT")) throw error; } } if (!(await durableNonAcceptanceProof(fs, producer, value))) throw new Error("Precommit non-acceptance did not converge"); await clearReservationProof(fs, root, producer, value); return true; }
async function clearReservationProof(fs: OutboxFileSystem, root: string, producer: ProducerFiles, value: CaptureReservation): Promise<void> {
  const dir = join(root, "reservations"); const reservationFile = join(dir, `${value.reservationId}.json`); const preparedPrefix = `prepare-${sha256Hex(producer.nodeId).slice(0, 16)}-${producer.producerUuid}-${value.reservationId}`;
  const decision = await durablePrecommitDecision(fs, producer, value); const accepted = decision?.kind === "partial_precommit" || decision?.kind === "aborted_precommit" ? undefined : await durableReservationJobProof(fs, producer, value);
  const authorized = decision?.kind === "partial_precommit" || decision?.kind === "aborted_precommit" ? await durableNonAcceptanceProof(fs, producer, value) : accepted !== undefined && (decision?.kind !== "accepted_precommit" || decisionMatchesBytes(decision, Buffer.from(canonicalStringify(accepted), "utf8"))) || decision?.kind === "accepted_precommit" && await durableAcceptedTerminalDecisionProof(fs, producer, value, decision);
  if (!authorized) {
    let matchingProof = false;
    for (const lock of await activeAdmissionLocks(fs, dir, reservation)) matchingProof ||= lock.reservation.reservationId === value.reservationId;
    try { const persisted = reservation(await readSecureJson(fs, reservationFile)); matchingProof ||= persisted.reservationId === value.reservationId; } catch (error) { if (!errno(error, "ENOENT")) throw error; }
    for (const name of await fs.readdir(dir)) if (name.startsWith(preparedPrefix)) { const prepared = reservation(await readSecureJson(fs, join(dir, name))); matchingProof ||= prepared.reservationId === value.reservationId; }
    if (matchingProof) throw new Error("Admission proof has no durable acceptance or non-acceptance authority"); return;
  }
  await retireOwnedAdmissionLock({ fs, dir, reservation: value, validateReservation: reservation });
  const file = join(dir, `${value.reservationId}.json`); try { const stored = reservation(await readSecureJson(fs, file)); if (canonicalStringify(stored) !== canonicalStringify(value)) throw new Error("Outbox reservation collision"); await durableRemove(fs, file, dir); } catch (error) { if (!errno(error, "ENOENT")) throw error; }
  const prefix = `prepare-${sha256Hex(producer.nodeId).slice(0, 16)}-${producer.producerUuid}-${value.reservationId}`; for (const name of await fs.readdir(dir)) if (name.startsWith(prefix)) { const prepared = reservation(await readSecureJson(fs, join(dir, name))); if (canonicalStringify(prepared) !== canonicalStringify(value)) throw new Error("Prepared reservation collision"); await durableRemove(fs, join(dir, name), dir); } await syncDirectory(fs, dir);
  await retireOwnedAdmissionLock({ fs, dir, reservation: value, validateReservation: reservation });
}
async function recoverFencedAdmissions(fs: OutboxFileSystem, root: string, producer: ProducerFiles, now: number): Promise<number> {
  let terminals = 0; if (!producer.fenced || !producer.fencePublished || producer.fenceAuditHash === null) throw new Error("Precommit recovery requires a durable jobs fence"); const reservationsDir = join(root, "reservations"); const values = new Map<string, CaptureReservation>();
  const mergeValue = (value: CaptureReservation, source: string): void => { if (value.nodeId !== producer.nodeId || value.producerUuid !== producer.producerUuid) return; const prior = values.get(value.reservationId); if (prior !== undefined && canonicalStringify(prior) !== canonicalStringify(value)) throw new Error(`Outbox reservation/${source} mismatch`); values.set(value.reservationId, value); }; const preparedOwnerPrefix = `prepare-${sha256Hex(producer.nodeId).slice(0, 16)}-${producer.producerUuid}-`;
  for (const name of await fs.readdir(reservationsDir)) { if (name === "admission.lock") throw new Error("Outbox legacy admission lock is unsafe"); if (name.startsWith("prepare-")) { if (!name.startsWith(preparedOwnerPrefix)) continue; const suffix = name.slice(preparedOwnerPrefix.length); const tempMatch = /^([0-9a-f-]{36})\.tmp-([0-9]+)-([a-f0-9]{32})$/u.exec(suffix); const reservationId = tempMatch?.[1] ?? suffix; if (!UUID.test(reservationId) || (tempMatch === null && suffix !== reservationId)) throw new Error("Prepared reservation filename is ambiguous"); const file = join(reservationsDir, name); if (tempMatch !== null && await durablePreparedTempAudit(fs, producer, name, reservationId) !== undefined) { await durableRemove(fs, file, reservationsDir); continue; } const bytes = await readSecureArtifactBytes(fs, file, 1_048_576); let value: CaptureReservation; try { const text = Buffer.from(bytes).toString("utf8"); value = reservation(JSON.parse(text)); if (text !== canonicalStringify(value)) throw new Error("Prepared reservation bytes are not canonical"); } catch (error) { if (tempMatch === null) throw error; await auditAndRemovePreparedTemp(fs, producer, reservationsDir, file, name, reservationId, bytes); continue; } if (value.nodeId !== producer.nodeId || value.producerUuid !== producer.producerUuid || value.reservationId !== reservationId || name !== `${preparedOwnerPrefix}${value.reservationId}${tempMatch === null ? "" : `.tmp-${tempMatch[2]}-${tempMatch[3]}`}`) throw new Error("Prepared reservation filename/content mismatch"); mergeValue(value, tempMatch === null ? "prepared" : "prepared-temp"); continue; } if (!name.endsWith(".json") || name.includes(".tmp-")) continue; const value = reservation(await readSecureJson(fs, join(reservationsDir, name))); if (name !== `${value.reservationId}.json`) throw new Error("Outbox reservation filename mismatch"); mergeValue(value, "persisted"); }
  for (const lock of await activeAdmissionLocks(fs, reservationsDir, reservation)) mergeValue(lock.reservation, "lock");
  const tempNames = (await fs.readdir(producer.jobsDir)).filter((name) => JOB_TEMP.test(name)); const claimed = new Set<string>();
  for (const value of [...values.values()].sort((left, right) => left.reservationId.localeCompare(right.reservationId))) {
    if (await convergeNonAcceptanceDecision(fs, root, producer, value)) { terminals += 1; continue; }
    const names = tempNames.filter((name) => JOB_TEMP.exec(name)?.[1] === value.jobId); if (names.length > 1) throw new Error("Ambiguous precommit payload temps"); let accepted = await durableReservationJobProof(fs, producer, value); let acceptedBytes: Uint8Array | undefined;
    if (accepted !== undefined) acceptedBytes = Buffer.from(canonicalStringify(accepted), "utf8");
    if (accepted === undefined && names.length === 1) { const name = names[0]!; claimed.add(name); const tempFile = join(producer.jobsDir, name); const canonicalFile = join(producer.jobsDir, `${value.jobId}.json`); let bytes: Uint8Array; let tempInfo;
      try { tempInfo = await fs.lstat(tempFile); if (!tempInfo.isFile() || tempInfo.isSymbolicLink() || (tempInfo.mode & 0o077) !== 0) throw new Error("Precommit payload temp is unsafe"); bytes = await fs.readFile(tempFile); } catch (error) { if (!errno(error, "ENOENT")) throw error; const winner = await publishPrecommitDecision(fs, producer, value, "aborted_precommit"); if (winner.kind === "accepted_precommit") throw new Error("Accepted precommit decision has no payload proof"); if (!(await convergeNonAcceptanceDecision(fs, root, producer, value))) throw new Error("Aborted precommit decision did not converge"); terminals += 1; continue; }
      let complete: OutboxJob | undefined; try { const text = Buffer.from(bytes).toString("utf8"); const job = parseOutboxJob(JSON.parse(text), { host: producer.identity.ownerHost, nodeId: producer.nodeId, producerUuid: producer.producerUuid, homeDir: "/" }); if (!reservationMatchesJob(value, producer, job) || bytes.length !== value.requestedBytes || canonicalStringify(job) !== text) throw new Error("Precommit job mismatch"); complete = job; } catch { /* securely-read incomplete or malformed payload is a non-acceptance candidate */ }
      if (complete === undefined) { const winner = await publishPrecommitDecision(fs, producer, value, "partial_precommit", bytes); if (winner.kind === "accepted_precommit") throw new Error("Accepted precommit decision is awaiting its exact complete payload"); if (!(await convergeNonAcceptanceDecision(fs, root, producer, value))) throw new Error("Partial precommit decision did not converge"); terminals += 1; continue; }
      let handle; try { handle = await fs.open(tempFile, fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW); const opened = await handle.stat(); if (!opened.isFile() || opened.dev !== tempInfo.dev || opened.ino !== tempInfo.ino || opened.size !== bytes.length || (opened.mode & 0o077) !== 0) throw new Error("Precommit payload inode changed"); await handle.sync(); } finally { await handle?.close().catch(() => undefined); } const afterSync = await fs.lstat(tempFile); if (!afterSync.isFile() || afterSync.isSymbolicLink() || afterSync.dev !== tempInfo.dev || afterSync.ino !== tempInfo.ino || afterSync.size !== bytes.length || (afterSync.mode & 0o077) !== 0) throw new Error("Precommit payload path changed after sync"); await syncDirectory(fs, producer.jobsDir); const afterDirectorySync = await fs.lstat(tempFile); if (!afterDirectorySync.isFile() || afterDirectorySync.isSymbolicLink() || afterDirectorySync.dev !== tempInfo.dev || afterDirectorySync.ino !== tempInfo.ino || afterDirectorySync.size !== bytes.length || (afterDirectorySync.mode & 0o077) !== 0) throw new Error("Precommit payload path changed after directory sync"); let rereadHandle; let reread: Uint8Array; try { rereadHandle = await fs.open(tempFile, fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW); const opened = await rereadHandle.stat(); if (!opened.isFile() || opened.dev !== tempInfo.dev || opened.ino !== tempInfo.ino || opened.size !== bytes.length || (opened.mode & 0o077) !== 0) throw new Error("Precommit payload durable inode changed"); reread = await rereadHandle.readFile(); } finally { await rereadHandle?.close().catch(() => undefined); } const afterReread = await fs.lstat(tempFile); if (!afterReread.isFile() || afterReread.isSymbolicLink() || afterReread.dev !== tempInfo.dev || afterReread.ino !== tempInfo.ino || afterReread.size !== bytes.length || (afterReread.mode & 0o077) !== 0 || Buffer.compare(Buffer.from(reread), Buffer.from(bytes)) !== 0) throw new Error("Precommit payload durable reread mismatch"); const winner = await publishPrecommitDecision(fs, producer, value, "accepted_precommit", reread); if (winner.kind !== "accepted_precommit") { if (!(await convergeNonAcceptanceDecision(fs, root, producer, value))) throw new Error("Non-acceptance decision did not converge"); terminals += 1; continue; } if (!decisionMatchesBytes(winner, reread)) throw new Error("Accepted precommit decision source mismatch"); try { await fs.link(tempFile, canonicalFile); await syncDirectory(fs, producer.jobsDir); } catch (error) { if (!errno(error, "EEXIST")) throw error; } accepted = await durableReservationJobProof(fs, producer, value); if (accepted === undefined || canonicalStringify(accepted) !== canonicalStringify(complete)) throw new Error("Recovered job durable readback mismatch"); acceptedBytes = bytes; await durableRemove(fs, tempFile, producer.jobsDir);
    }
    if (accepted === undefined && names.length === 0) { const winner = await publishPrecommitDecision(fs, producer, value, "aborted_precommit"); if (winner.kind !== "accepted_precommit") { if (!(await convergeNonAcceptanceDecision(fs, root, producer, value))) throw new Error("Aborted precommit decision did not converge"); terminals += 1; continue; } accepted = await durableReservationJobProof(fs, producer, value); if (accepted === undefined) { if (await durableAcceptedTerminalDecisionProof(fs, producer, value, winner)) { await clearReservationProof(fs, root, producer, value); continue; } throw new Error("Accepted precommit decision has no durable job"); } acceptedBytes = Buffer.from(canonicalStringify(accepted), "utf8"); }
    if (accepted === undefined || acceptedBytes === undefined) throw new Error("Accepted precommit recovery did not converge"); const decision = await publishPrecommitDecision(fs, producer, value, "accepted_precommit", acceptedBytes); if (decision.kind !== "accepted_precommit" || !decisionMatchesBytes(decision, acceptedBytes)) { if (await convergeNonAcceptanceDecision(fs, root, producer, value)) { terminals += 1; continue; } throw new Error("Precommit decision conflict"); }
    if (expired(accepted, now, 0)) { await expireAccepted(fs, root, producer, join(producer.jobsDir, `${value.jobId}.json`), producer.jobsDir, accepted); terminals += 1; } else { await clearReservationProof(fs, root, producer, value); }
    const retainedDecision = await durablePrecommitDecision(fs, producer, value); if (retainedDecision?.kind !== "accepted_precommit") throw new Error("Accepted precommit decision was not retained");
  }
  for (const name of tempNames) if (!claimed.has(name)) { const match = JOB_TEMP.exec(name)!; const file = join(producer.jobsDir, name); let bytes: Uint8Array; try { const info = await fs.lstat(file); if (!info.isFile() || info.isSymbolicLink() || (info.mode & 0o077) !== 0) throw new Error("Orphan precommit payload temp is unsafe"); bytes = await fs.readFile(file); } catch (error) { if (errno(error, "ENOENT")) continue; throw error; } terminals += 1; const audit = orphanPrecommitAudit(match[1]!, bytes); await atomicWrite(fs, join(producer.quarantineDir, `precommit-orphan-${sha256Hex(name).slice(0, 32)}.json`), audit); await durableRemove(fs, file, producer.jobsDir); } return terminals;
}
async function admissionLockForProducer(fs: OutboxFileSystem, root: string, producer: ProducerFiles): Promise<CaptureReservation | undefined> { for (const lock of await activeAdmissionLocks(fs, join(root, "reservations"), reservation)) if (lock.reservation.nodeId === producer.nodeId && lock.reservation.producerUuid === producer.producerUuid) return lock.reservation; return undefined; }
function producerInactive(state: ProducerState, operationNow: number, timeoutMs: number, skewMs: number): boolean { if (state.state === "closed") return true; if (operationNow <= state.heartbeatAt) return false; return operationNow - state.heartbeatAt > timeoutMs + skewMs; }
function expired(job: OutboxJob, now: number, skew: number): boolean { return job.deadline !== null && Date.parse(job.deadline) <= now + skew; }
async function malformedQuarantine(fs: OutboxFileSystem, file: string, producer: ProducerFiles, now: number, input: { bytes?: Uint8Array; unsafePath?: boolean }): Promise<void> {
  const unsafePath = input.unsafePath === true; const bytes = input.bytes; if (!unsafePath && bytes === undefined) throw new Error("Malformed quarantine requires securely-read bytes");
  const sourceHash = unsafePath ? sha256Hex(canonicalStringify({ kind: "unsafe_path", file })) : createHash("sha256").update(bytes!).digest("hex");
  const audit: PlainRecord = { version: 1, kind: unsafePath ? "unsafe_path" : "malformed", sourceHash, byteLength: bytes?.length ?? 0, category: "malformed", quarantinedAt: new Date(now).toISOString(), auditHash: "" }; audit.auditHash = hashWithout(audit, "auditHash");
  await atomicWrite(fs, join(producer.quarantineDir, `malformed-${sourceHash}.json`), audit);
  await fs.rm(file, { force: true }); await syncDirectory(fs, producer.jobsDir);
}
async function readAcceptedJob(fs: OutboxFileSystem, file: string, producer: ProducerFiles): Promise<OutboxJob> { return parseOutboxJob(await readSecureJson(fs, file), { host: producer.identity.ownerHost, nodeId: producer.nodeId, producerUuid: producer.producerUuid, homeDir: "/" }); }
async function promoteQuarantinePayload(fs: OutboxFileSystem, producer: ProducerFiles, source: string, destination: string, job: OutboxJob): Promise<void> {
  try { await fs.link(source, destination); }
  catch (error) { if (!errno(error, "EEXIST") && !errno(error, "ENOENT")) throw error; const existing = await readAcceptedJob(fs, destination, producer); if (existing.auditHash !== job.auditHash) throw new Error("Immutable quarantine collision"); }
  await syncDirectory(fs, producer.quarantineDir); const readback = await readAcceptedJob(fs, destination, producer); if (readback.auditHash !== job.auditHash) throw new Error("Immutable quarantine readback failed"); await durableRemove(fs, source, producer.jobsDir);
}
async function quarantineAccepted(fs: OutboxFileSystem, producer: ProducerFiles, file: string, job: OutboxJob, category: string): Promise<boolean> {
  const bounded = safeCategory(category, "processor_quarantined"); const destination = join(producer.quarantineDir, `${job.id}.json`); const reasonFile = join(producer.quarantineDir, `${job.id}.reason.json`);
  const reason: PlainRecord = { version: 1, jobId: job.id, category: bounded, auditHash: "" }; reason.auditHash = hashWithout(reason, "auditHash"); await atomicWrite(fs, reasonFile, reason);
  try { await promoteQuarantinePayload(fs, producer, file, destination, job); }
  catch (error) { try { await readAcceptedJob(fs, destination, producer); } catch (destinationError) { if (errno(destinationError, "ENOENT")) await durableRemove(fs, reasonFile, producer.quarantineDir).catch(() => undefined); } throw error; }
  await fs.rm(join(producer.controlDir, `${job.id}.json`), { force: true }).then(() => syncDirectory(fs, producer.controlDir)).catch(() => undefined); return true;
}
function expiryAudit(job: OutboxJob): ExpiryAudit {
  if (job.deadline === null) throw new Error("An indefinite job cannot expire");
  const value: ExpiryAudit = { version: 1, kind: "expired", jobId: job.id, payloadAuditHash: job.auditHash, policyId: job.policyId, deadline: job.deadline, expiredAt: job.deadline, auditHash: "" };
  value.auditHash = hashWithout(value as unknown as PlainRecord, "auditHash"); return value;
}
function validateExpiryAudit(value: unknown, job: OutboxJob): ExpiryAudit {
  const expected = expiryAudit(job);
  if (!record(value) || !exactKeys(value, EXPIRY_KEYS) || canonicalStringify(value) !== canonicalStringify(expected)) throw new Error("Expiry audit is malformed or mismatched");
  return value as unknown as ExpiryAudit;
}
function deliveredAudit(job: OutboxJob, deliveredAt: number): DeliveredAudit { const value: DeliveredAudit = { version: 1, kind: "delivered", status: "delivered", jobId: job.id, payloadAuditHash: job.auditHash, deliveredAt: new Date(deliveredAt).toISOString(), auditHash: "" }; value.auditHash = hashWithout(value as unknown as PlainRecord, "auditHash"); return value; }
function validateDeliveredAudit(value: unknown): DeliveredAudit {
  if (!record(value) || !exactKeys(value, DELIVERED_KEYS) || value.version !== 1 || value.kind !== "delivered" || value.status !== "delivered" || typeof value.jobId !== "string" || !UUID.test(value.jobId) || typeof value.payloadAuditHash !== "string" || !HASH.test(value.payloadAuditHash) || typeof value.deliveredAt !== "string" || !ISO_DATE.test(value.deliveredAt) || !Number.isFinite(Date.parse(value.deliveredAt)) || value.auditHash !== hashWithout(value, "auditHash")) throw new Error("Delivered audit is malformed"); return value as unknown as DeliveredAudit;
}
async function ensureDeliveredAudit(fs: OutboxFileSystem, producer: ProducerFiles, job: OutboxJob, at: number): Promise<string> {
  const file = join(producer.quarantineDir, `${job.id}.delivered.json`); const read = (): Promise<DeliveredAudit> => readExactCanonicalJson(fs, file, validateDeliveredAudit);
  try { const existing = await read(); if (existing.jobId !== job.id || existing.payloadAuditHash !== job.auditHash) throw new Error("Delivered audit does not match accepted job"); await syncDirectory(fs, producer.quarantineDir); const second = await read(); if (canonicalStringify(second) !== canonicalStringify(existing)) throw new Error("Delivered audit readback changed"); return file; } catch (error) { if (!errno(error, "ENOENT")) throw error; }
  const audit = deliveredAudit(job, at); await atomicWrite(fs, file, audit); const readback = await read(); if (canonicalStringify(readback) !== canonicalStringify(audit)) throw new Error("Delivered audit readback failed"); return file;
}
async function ensureExpiryAudit(fs: OutboxFileSystem, auditFile: string, job: OutboxJob, audit: ExpiryAudit): Promise<void> {
  const parent = resolve(auditFile, ".."); const read = (): Promise<ExpiryAudit> => readExactCanonicalJson(fs, auditFile, (value) => validateExpiryAudit(value, job)); let replace = false;
  try { const first = await read(); if (canonicalStringify(first) !== canonicalStringify(audit)) throw new Error("Expiry audit collision"); await syncDirectory(fs, parent); const second = await read(); if (canonicalStringify(second) !== canonicalStringify(first)) throw new Error("Expiry audit readback changed"); return; }
  catch (error) { if (errno(error, "ENOENT")) replace = true; else { let info; try { info = await fs.lstat(auditFile); } catch (inspectError) { if (errno(inspectError, "ENOENT")) replace = true; else throw error; } if (info !== undefined) { if (info.isSymbolicLink() || !info.isFile() || (info.mode & 0o077) !== 0) replace = true; else { let bytes: Uint8Array; try { bytes = await fs.readFile(auditFile); } catch { throw error; } try { validateExpiryAudit(JSON.parse(Buffer.from(bytes).toString("utf8")), job); } catch { replace = true; } if (!replace) throw error; } } } }
  if (replace) { await atomicWrite(fs, auditFile, audit); const readback = await read(); if (canonicalStringify(readback) !== canonicalStringify(audit)) throw new Error("Expiry audit readback failed"); }
}
async function expireAccepted(fs: OutboxFileSystem, root: string, producer: ProducerFiles, _file: string, _sourceDir: string, job: OutboxJob): Promise<boolean> {
  const auditFile = join(producer.quarantineDir, `${job.id}.expired.json`); const audit = expiryAudit(job); await ensureExpiryAudit(fs, auditFile, job, audit); await convergeExpiryTerminal(fs, root, producer, auditFile, job); return true;
}
type ControlRead = { kind: "missing" } | { kind: "valid"; control: RetryControl } | { kind: "defer" };
async function controlAudit(fs: OutboxFileSystem, producer: ProducerFiles, file: string, jobId: string, input: { bytes?: Uint8Array; unsafePath?: boolean }): Promise<void> {
  const sourceHash = input.unsafePath === true ? sha256Hex(canonicalStringify({ kind: "unsafe_control_path", file })) : createHash("sha256").update(input.bytes!).digest("hex");
  const audit: PlainRecord = { version: 1, kind: input.unsafePath === true ? "unsafe_control_path" : "malformed_control", jobId, sourceHash, byteLength: input.bytes?.length ?? 0, auditHash: "" }; audit.auditHash = hashWithout(audit, "auditHash");
  await atomicWrite(fs, join(producer.quarantineDir, `control-${jobId}-${sourceHash}.json`), audit); await fs.rm(file, { force: true }); await syncDirectory(fs, producer.controlDir);
}
async function readControl(fs: OutboxFileSystem, producer: ProducerFiles, jobId: string): Promise<ControlRead> {
  const file = join(producer.controlDir, `${jobId}.json`); let info;
  try { info = await fs.lstat(file); } catch (error) { return errno(error, "ENOENT") ? { kind: "missing" } : { kind: "defer" }; }
  if (info.isSymbolicLink()) { try { await controlAudit(fs, producer, file, jobId, { unsafePath: true }); } catch { /* leave/reset is best effort */ } return { kind: "defer" }; }
  if (!info.isFile() || (info.mode & 0o077) !== 0) return { kind: "defer" };
  let bytes: Uint8Array; try { bytes = await fs.readFile(file); } catch { return { kind: "defer" }; }
  try { return { kind: "valid", control: retryControl(JSON.parse(Buffer.from(bytes).toString("utf8")), jobId) }; }
  catch { try { await controlAudit(fs, producer, file, jobId, { bytes }); } catch { /* keep job active */ } return { kind: "defer" }; }
}
function retryDelay(jobId: string, attempts: number, base: number, maximum: number): number {
  const exponent = Math.min(30, Math.max(0, attempts - 1)); const bounded = Math.min(maximum, base * (2 ** exponent));
  const fraction = Number.parseInt(sha256Hex(canonicalStringify({ attempts, jobId })).slice(0, 8), 16) / 0xffffffff;
  return Math.max(1, Math.min(maximum, Math.floor(bounded * (0.5 + fraction * 0.5))));
}
async function pendingRetryEligible(fs: OutboxFileSystem, producer: ProducerFiles, job: OutboxJob): Promise<boolean> {
  let active: OutboxJob; try { active = await readAcceptedJob(fs, join(producer.jobsDir, `${job.id}.json`), producer); } catch (error) { if (errno(error, "ENOENT")) return false; throw error; } if (active.auditHash !== job.auditHash) throw new Error("Pending retry active proof mismatch");
  try { const quarantined = await readAcceptedJob(fs, join(producer.quarantineDir, `${job.id}.json`), producer); if (quarantined.auditHash !== job.auditHash) throw new Error("Pending retry quarantine mismatch"); return false; } catch (error) { if (!errno(error, "ENOENT")) throw error; }
  try { const delivered = validateDeliveredAudit(await readSecureJson(fs, join(producer.quarantineDir, `${job.id}.delivered.json`))); if (delivered.jobId !== job.id || delivered.payloadAuditHash !== job.auditHash) throw new Error("Pending retry delivered-audit mismatch"); return false; } catch (error) { if (!errno(error, "ENOENT")) throw error; }
  try { validateExpiryAudit(await readSecureJson(fs, join(producer.quarantineDir, `${job.id}.expired.json`)), job); return false; } catch (error) { if (!errno(error, "ENOENT")) throw error; }
  return true;
}
async function writePending(fs: OutboxFileSystem, producer: ProducerFiles, job: OutboxJob, previous: RetryControl | undefined, now: number, category: string, base: number, maximum: number): Promise<void> {
  try { if (!(await pendingRetryEligible(fs, producer, job))) return; } catch { return; }
  const attempts = (previous?.attempts ?? 0) + 1; const delay = retryDelay(job.id, attempts, base, maximum); const nextAttemptAt = Math.min(MAX_TIME, now + delay); if (!finiteTime(nextAttemptAt)) return; const file = join(producer.controlDir, `${job.id}.json`); await atomicWrite(fs, file, makeControl(job.id, attempts, nextAttemptAt, safeCategory(category, "processor_pending")));
  try { if (await pendingRetryEligible(fs, producer, job)) return; } catch { /* post-write ambiguity removes mutable retry state */ } await durableRemove(fs, file, producer.controlDir);
}
type ProcessOutcome = { kind: "delivered" | "pending" | "quarantined"; category: string };
async function invokeProcessor(processor: OutboxJobProcessor, job: OutboxJob, signal: AbortSignal | undefined, timeoutMs: number): Promise<ProcessOutcome> {
  if (signal?.aborted) return { kind: "pending", category: "aborted" };
  const controller = new AbortController(); let resolveAbort!: (value: symbol) => void;
  const aborted = new Promise<symbol>((resolveAborted) => { resolveAbort = resolveAborted; });
  const onAbort = (): void => { controller.abort(); resolveAbort(Symbol.for("outbox.aborted")); }; signal?.addEventListener("abort", onAbort, { once: true });
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<symbol>((resolveTimeout) => { timer = setTimeout(() => { controller.abort(); resolveTimeout(Symbol.for("outbox.timeout")); }, timeoutMs); });
  try {
    const call = Promise.resolve().then(() => processor.process(job, { signal: controller.signal })).catch(() => ({ status: "pending" as const, category: "processor_error" }));
    const result = await Promise.race([call, timeout, aborted]);
    if (typeof result === "symbol") return { kind: "pending", category: result === Symbol.for("outbox.aborted") ? "aborted" : "attempt_deadline" };
    if (!record(result) || !exactKeys(result, result.category === undefined ? ["status"] : ["status", "category"]) || !["delivered", "pending", "quarantined"].includes(String(result.status))) return { kind: "pending", category: "processor_result_invalid" };
    return { kind: result.status as ProcessOutcome["kind"], category: safeCategory(result.category, result.status === "quarantined" ? "processor_quarantined" : "processor_pending") };
  } finally { if (timer !== undefined) clearTimeout(timer); signal?.removeEventListener("abort", onAbort); }
}

function reservationMatchesJob(value: CaptureReservation, producer: ProducerFiles, job: OutboxJob): boolean { return job.ownerHost === producer.identity.ownerHost && value.nodeId === producer.nodeId && value.producerUuid === producer.producerUuid && value.jobId === job.id && value.jobAuditHash === job.auditHash && value.policyId === job.policyId && value.deadline === job.deadline && value.requestedBytes === Buffer.byteLength(canonicalStringify(job), "utf8"); }
function reservationForJob(producer: ProducerFiles, job: OutboxJob): CaptureReservation { const value: CaptureReservation = { version: 1, reservationId: deterministicUuid("pi-qdrant-memory-v2:outbox-reservation", producer.nodeId, producer.producerUuid, job.id), jobId: job.id, jobAuditHash: job.auditHash, policyId: job.policyId, deadline: job.deadline, nodeId: producer.nodeId, producerUuid: producer.producerUuid, requestedBytes: Buffer.byteLength(canonicalStringify(job), "utf8"), auditHash: "" }; value.auditHash = hashWithout(value as unknown as PlainRecord, "auditHash"); return value; }
async function durableRemove(fs: OutboxFileSystem, file: string, dir: string): Promise<void> { try { await fs.rm(file); } catch (error) { if (!errno(error, "ENOENT")) throw error; } await syncDirectory(fs, dir); }
async function durableReservationJobProof(fs: OutboxFileSystem, producer: ProducerFiles, value: CaptureReservation): Promise<OutboxJob | undefined> {
  if (value.nodeId !== producer.nodeId || value.producerUuid !== producer.producerUuid) throw new Error("Outbox reservation producer mismatch");
  const readExact = async (file: string): Promise<OutboxJob> => { const info = await fs.lstat(file); if (!info.isFile() || info.isSymbolicLink() || (info.mode & 0o077) !== 0 || info.size !== value.requestedBytes) throw new Error("Outbox durable job proof is unsafe or has the wrong byte length"); let handle; let bytes: Uint8Array; try { handle = await fs.open(file, fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW); const opened = await handle.stat(); if (!opened.isFile() || opened.dev !== info.dev || opened.ino !== info.ino || opened.size !== value.requestedBytes || (opened.mode & 0o077) !== 0) throw new Error("Outbox durable job proof inode changed"); bytes = await handle.readFile(); } finally { await handle?.close().catch(() => undefined); } const after = await fs.lstat(file); if (!after.isFile() || after.isSymbolicLink() || after.dev !== info.dev || after.ino !== info.ino || after.size !== value.requestedBytes || (after.mode & 0o077) !== 0 || bytes.length !== value.requestedBytes) throw new Error("Outbox durable job proof path changed"); const text = Buffer.from(bytes).toString("utf8"); const job = parseOutboxJob(JSON.parse(text), { host: producer.identity.ownerHost, nodeId: producer.nodeId, producerUuid: producer.producerUuid, homeDir: "/" }); if (text !== canonicalStringify(job) || !reservationMatchesJob(value, producer, job)) throw new Error("Outbox durable job proof does not exactly match reservation"); return job; };
  for (const dir of [producer.jobsDir, producer.quarantineDir]) { const identity = await privateDirectoryIdentity(fs, dir, producer.path); const file = join(dir, `${value.jobId}.json`); let first: OutboxJob; try { first = await readExact(file); } catch (error) { if (errno(error, "ENOENT")) continue; throw error; } await syncDirectory(fs, dir); const after = await privateDirectoryIdentity(fs, dir, producer.path); if (after.dev !== identity.dev || after.ino !== identity.ino) throw new Error("Outbox durable job proof directory changed"); const second = await readExact(file); if (canonicalStringify(second) !== canonicalStringify(first)) throw new Error("Outbox durable job proof readback mismatch"); return second; } return undefined;
}
async function finalizeJobAdmission(fs: OutboxFileSystem, root: string, producer: ProducerFiles, job: OutboxJob): Promise<void> {
  const expected = reservationForJob(producer, job); const decision = await durablePrecommitDecision(fs, producer, expected); if (decision?.kind === "partial_precommit" || decision?.kind === "aborted_precommit") throw new Error("Outbox admission is terminally non-accepted"); const proof = await durableReservationJobProof(fs, producer, expected); if (proof === undefined || canonicalStringify(proof) !== canonicalStringify(job) || (decision?.kind === "accepted_precommit" && !decisionMatchesBytes(decision, Buffer.from(canonicalStringify(proof), "utf8")))) throw new Error("Outbox admission has no durable immutable completion proof");
  const dir = join(root, "reservations"); const reservationId = expected.reservationId; const reservationFile = join(dir, `${reservationId}.json`);
  await retireOwnedAdmissionLock({ fs, dir, reservation: expected, validateReservation: reservation });
  try { const value = reservation(await readSecureJson(fs, reservationFile)); if (canonicalStringify(value) !== canonicalStringify(expected)) throw new Error("Outbox reservation does not match accepted job"); await durableRemove(fs, reservationFile, dir); }
  catch (error) { if (!errno(error, "ENOENT")) throw error; await syncDirectory(fs, dir); }
  const preparedPrefix = `prepare-${sha256Hex(producer.nodeId).slice(0, 16)}-${producer.producerUuid}-${reservationId}`; for (const name of await fs.readdir(dir)) if (name.startsWith(preparedPrefix)) { const value = reservation(await readSecureJson(fs, join(dir, name))); if (canonicalStringify(value) !== canonicalStringify(expected)) throw new Error("Prepared reservation does not match accepted job"); await durableRemove(fs, join(dir, name), dir); }
}
async function inspectTerminalCopy(fs: OutboxFileSystem, file: string, producer: ProducerFiles): Promise<OutboxJob | undefined> { try { return await readAcceptedJob(fs, file, producer); } catch (error) { if (errno(error, "ENOENT")) return undefined; throw error; } }
async function processorDeliveredProof(fs: OutboxFileSystem, producer: ProducerFiles, expected: OutboxJob): Promise<OutboxJob> { const proof = await durableReservationJobProof(fs, producer, reservationForJob(producer, expected)); if (proof === undefined || canonicalStringify(proof) !== canonicalStringify(expected)) throw new Error("Processor-delivered durable proof is absent or mismatched"); return proof; }
async function convergeDeliveredTerminal(fs: OutboxFileSystem, root: string, producer: ProducerFiles, auditFile: string, audit: DeliveredAudit): Promise<void> {
  const activeFile = join(producer.jobsDir, `${audit.jobId}.json`); const quarantineFile = join(producer.quarantineDir, `${audit.jobId}.json`); const matches = (job: OutboxJob): void => { if (job.id !== audit.jobId || job.auditHash !== audit.payloadAuditHash) throw new Error("Delivered audit payload mismatch"); };
  for (let attempt = 0; attempt < 4; attempt += 1) {
    const active = await inspectTerminalCopy(fs, activeFile, producer); if (active !== undefined) { matches(active); await finalizeJobAdmission(fs, root, producer, active); await durableRemove(fs, activeFile, producer.jobsDir); } else await syncDirectory(fs, producer.jobsDir);
    const quarantined = await inspectTerminalCopy(fs, quarantineFile, producer); if (quarantined !== undefined) { matches(quarantined); await finalizeJobAdmission(fs, root, producer, quarantined); await durableRemove(fs, quarantineFile, producer.quarantineDir); } else await syncDirectory(fs, producer.quarantineDir);
    const activeAgain = await inspectTerminalCopy(fs, activeFile, producer); const quarantineAgain = await inspectTerminalCopy(fs, quarantineFile, producer); if (activeAgain !== undefined || quarantineAgain !== undefined) { if (activeAgain !== undefined) matches(activeAgain); if (quarantineAgain !== undefined) matches(quarantineAgain); continue; }
    await syncDirectory(fs, producer.jobsDir); await syncDirectory(fs, producer.quarantineDir); await durableRemove(fs, join(producer.controlDir, `${audit.jobId}.json`), producer.controlDir); await durableRemove(fs, join(producer.quarantineDir, `${audit.jobId}.reason.json`), producer.quarantineDir);
    const finalActive = await inspectTerminalCopy(fs, activeFile, producer); const finalQuarantine = await inspectTerminalCopy(fs, quarantineFile, producer); if (finalActive !== undefined || finalQuarantine !== undefined) { if (finalActive !== undefined) matches(finalActive); if (finalQuarantine !== undefined) matches(finalQuarantine); continue; }
    await syncDirectory(fs, producer.jobsDir); await syncDirectory(fs, producer.quarantineDir); await durableRemove(fs, join(producer.quarantineDir, `${audit.jobId}.reason.json`), producer.quarantineDir); await durableRemove(fs, auditFile, producer.quarantineDir); return;
  }
  throw new Error("Delivered terminal convergence did not stabilize");
}
async function convergeExpiryTerminal(fs: OutboxFileSystem, root: string, producer: ProducerFiles, auditFile: string, job: OutboxJob): Promise<void> {
  validateExpiryAudit(await readSecureJson(fs, auditFile), job); const activeFile = join(producer.jobsDir, `${job.id}.json`); const quarantineFile = join(producer.quarantineDir, `${job.id}.json`); const matches = (copy: OutboxJob): void => { if (copy.id !== job.id || copy.auditHash !== job.auditHash) throw new Error("Expiry audit payload mismatch"); };
  for (let attempt = 0; attempt < 4; attempt += 1) {
    const active = await inspectTerminalCopy(fs, activeFile, producer); if (active !== undefined) { matches(active); await finalizeJobAdmission(fs, root, producer, active); await durableRemove(fs, activeFile, producer.jobsDir); } else await syncDirectory(fs, producer.jobsDir);
    const quarantined = await inspectTerminalCopy(fs, quarantineFile, producer); if (quarantined !== undefined) { matches(quarantined); await finalizeJobAdmission(fs, root, producer, quarantined); await durableRemove(fs, quarantineFile, producer.quarantineDir); } else await syncDirectory(fs, producer.quarantineDir);
    const activeAgain = await inspectTerminalCopy(fs, activeFile, producer); const quarantineAgain = await inspectTerminalCopy(fs, quarantineFile, producer); if (activeAgain !== undefined || quarantineAgain !== undefined) { if (activeAgain !== undefined) matches(activeAgain); if (quarantineAgain !== undefined) matches(quarantineAgain); continue; }
    await syncDirectory(fs, producer.jobsDir); await syncDirectory(fs, producer.quarantineDir); await durableRemove(fs, join(producer.controlDir, `${job.id}.json`), producer.controlDir); await durableRemove(fs, join(producer.quarantineDir, `${job.id}.reason.json`), producer.quarantineDir);
    const deliveredFile = join(producer.quarantineDir, `${job.id}.delivered.json`); try { const delivered = validateDeliveredAudit(await readSecureJson(fs, deliveredFile)); if (delivered.jobId !== job.id || delivered.payloadAuditHash !== job.auditHash) throw new Error("Expiry delivered-audit mismatch"); await durableRemove(fs, deliveredFile, producer.quarantineDir); } catch (error) { if (!errno(error, "ENOENT")) throw error; await syncDirectory(fs, producer.quarantineDir); }
    const finalActive = await inspectTerminalCopy(fs, activeFile, producer); const finalQuarantine = await inspectTerminalCopy(fs, quarantineFile, producer); if (finalActive !== undefined || finalQuarantine !== undefined) { if (finalActive !== undefined) matches(finalActive); if (finalQuarantine !== undefined) matches(finalQuarantine); continue; }
    await syncDirectory(fs, producer.jobsDir); await syncDirectory(fs, producer.quarantineDir); await durableRemove(fs, join(producer.quarantineDir, `${job.id}.reason.json`), producer.quarantineDir); validateExpiryAudit(await readSecureJson(fs, auditFile), job); return;
  }
  throw new Error("Expiry terminal convergence did not stabilize");
}
async function terminalPayloadAbsent(fs: OutboxFileSystem, producer: ProducerFiles, id: string): Promise<boolean> {
  for (const file of [join(producer.jobsDir, `${id}.json`), join(producer.quarantineDir, `${id}.json`), join(producer.quarantineDir, `${id}.delivered.json`)]) { try { await fs.lstat(file); return false; } catch (error) { if (!errno(error, "ENOENT")) return false; } } await syncDirectory(fs, producer.jobsDir); await syncDirectory(fs, producer.quarantineDir); return true;
}
async function activePayloadAbsent(fs: OutboxFileSystem, producer: ProducerFiles, id: string): Promise<boolean> { try { await fs.lstat(join(producer.jobsDir, `${id}.json`)); return false; } catch (error) { if (!errno(error, "ENOENT")) return false; } await syncDirectory(fs, producer.jobsDir); return true; }
async function cleanupOrphanTerminalSidecars(fs: OutboxFileSystem, producer: ProducerFiles): Promise<number> {
  let pending = 0;
  for (const name of await fs.readdir(producer.quarantineDir)) {
    if (!name.endsWith(".reason.json")) continue; const id = name.slice(0, -12); if (!UUID.test(id)) continue; const file = join(producer.quarantineDir, name);
    try { if (!(await terminalPayloadAbsent(fs, producer, id))) continue; const value = await readSecureJson(fs, file); if (!record(value) || !exactKeys(value, REASON_KEYS) || value.version !== 1 || value.jobId !== id || typeof value.category !== "string" || safeCategory(value.category, "") !== value.category || value.auditHash !== hashWithout(value, "auditHash")) throw new Error("Quarantine reason is malformed"); await durableRemove(fs, file, producer.quarantineDir); }
    catch { pending += 1; }
  }
  for (const name of await fs.readdir(producer.controlDir)) {
    if (!name.endsWith(".json")) continue; const id = name.slice(0, -5); if (!UUID.test(id)) continue; const file = join(producer.controlDir, name);
    try { if (!(await activePayloadAbsent(fs, producer, id))) continue; retryControl(await readSecureJson(fs, file), id); await durableRemove(fs, file, producer.controlDir); }
    catch { pending += 1; }
  }
  return pending;
}
async function cleanupOfflineReservations(fs: OutboxFileSystem, root: string, producer: ProducerFiles): Promise<void> {
  const dir = join(root, "reservations"); let names: string[]; try { names = await fs.readdir(dir); } catch { return; }
  const authority = async (value: CaptureReservation): Promise<boolean> => { if (value.nodeId !== producer.nodeId || value.producerUuid !== producer.producerUuid) return false; const decision = await durablePrecommitDecision(fs, producer, value); if (decision?.kind === "partial_precommit" || decision?.kind === "aborted_precommit") return durableNonAcceptanceProof(fs, producer, value); const job = await durableReservationJobProof(fs, producer, value); return job !== undefined && (decision?.kind !== "accepted_precommit" || decisionMatchesBytes(decision, Buffer.from(canonicalStringify(job), "utf8"))) || decision?.kind === "accepted_precommit" && await durableAcceptedTerminalDecisionProof(fs, producer, value, decision); };
  for (const lock of await activeAdmissionLocks(fs, dir, reservation)) { try { if (await authority(lock.reservation)) await retireOwnedAdmissionLock({ fs, dir, reservation: lock.reservation, validateReservation: reservation }); } catch { return; } }
  const preparedPrefix = `prepare-${sha256Hex(producer.nodeId).slice(0, 16)}-${producer.producerUuid}-`;
  for (const name of names) { if (isAdmissionProtocolArtifact(name)) continue; const file = join(dir, name); try { if (name.startsWith(preparedPrefix)) { const value = reservation(await readSecureJson(fs, file)); if (await authority(value)) await durableRemove(fs, file, dir); continue; } if (!name.endsWith(".json")) continue; const value = reservation(await readSecureJson(fs, file)); if (name === `${value.reservationId}.json` && await authority(value)) await durableRemove(fs, file, dir); } catch { /* unmatched/transient state remains fail-closed */ } }
}
export function createOutboxDelivery(input: DeliveryInput): OutboxDelivery {
  if (typeof input.processor?.process !== "function" || typeof input.now !== "function" || typeof input.producerPath !== "string") throw new TypeError("Outbox delivery requires a current producer path, injected processor, and clock");
  const clock = (): number => { const value = input.now(); if (!finiteTime(value)) throw new TypeError("Outbox delivery clock is invalid"); return value; };
  if (!Number.isSafeInteger(input.maxClockSkewMs) || input.maxClockSkewMs < 0 || input.maxClockSkewMs > 3_600_000) throw new TypeError("maxClockSkewMs is out of bounds");
  const retryBaseMs = input.retryBaseMs ?? 500; const retryMaxMs = input.retryMaxMs ?? 30_000;
  if (!Number.isSafeInteger(retryBaseMs) || retryBaseMs < 100 || retryBaseMs > 10_000 || !Number.isSafeInteger(retryMaxMs) || retryMaxMs < 1_000 || retryMaxMs > 300_000 || retryBaseMs > retryMaxMs) throw new TypeError("Outbox retry bounds are invalid");
  const heartbeatTimeoutMs = input.heartbeatTimeoutMs ?? 60_000; const attemptTimeoutMs = input.attemptTimeoutMs ?? 30_000;
  if (!Number.isSafeInteger(heartbeatTimeoutMs) || heartbeatTimeoutMs < 1 || heartbeatTimeoutMs > 86_400_000 || !Number.isSafeInteger(attemptTimeoutMs) || attemptTimeoutMs < 1 || attemptTimeoutMs > 120_000) throw new TypeError("Outbox delivery deadline bounds are invalid");
  const fs = { ...nodeFs, ...(input.fs ?? {}) } as OutboxFileSystem; const adopted = new Set<string>(); const active = new Set<string>();
  let rootPromise: Promise<string> | undefined; const root = (): Promise<string> => rootPromise ??= canonicalRoot(fs, input.outboxRoot);
  type ScannedJob = { kind: "job"; job: OutboxJob; file: string } | { kind: "pending" };
  async function jobsFor(producer: ProducerFiles): Promise<ScannedJob[]> {
    const result: ScannedJob[] = [];
    for (const name of (await fs.readdir(producer.jobsDir)).sort()) {
      if (name.includes(".tmp-") || !name.endsWith(".json")) continue; const file = join(producer.jobsDir, name); let info;
      try { info = await fs.lstat(file); } catch (error) { if (!errno(error, "ENOENT")) result.push({ kind: "pending" }); continue; }
      if (info.isSymbolicLink()) { await malformedQuarantine(fs, file, producer, clock(), { unsafePath: true }).catch(() => undefined); continue; }
      if (!info.isFile() || (info.mode & 0o077) !== 0) { result.push({ kind: "pending" }); continue; }
      let bytes: Uint8Array; try { bytes = await fs.readFile(file); } catch { result.push({ kind: "pending" }); continue; }
      try { const job = parseOutboxJob(JSON.parse(Buffer.from(bytes).toString("utf8")), { host: producer.identity.ownerHost, nodeId: producer.nodeId, producerUuid: producer.producerUuid, homeDir: "/" }); if (name !== `${job.id}.json`) throw new Error("job filename mismatch"); result.push({ kind: "job", job, file }); }
      catch { await malformedQuarantine(fs, file, producer, clock(), { bytes }).catch(() => undefined); }
    }
    return result;
  }
  type QuarantineScan = { jobs: Array<{ job: OutboxJob; file: string }>; pending: number };
  async function quarantinedPayloads(producer: ProducerFiles): Promise<QuarantineScan> {
    const result: QuarantineScan = { jobs: [], pending: 0 };
    for (const name of (await fs.readdir(producer.quarantineDir)).sort()) {
      if (!name.endsWith(".json")) continue; const id = name.slice(0, -5); if (!UUID.test(id)) continue; const file = join(producer.quarantineDir, name); let info;
      try { info = await fs.lstat(file); } catch (error) { if (!errno(error, "ENOENT")) result.pending += 1; continue; }
      if (!info.isFile() || info.isSymbolicLink() || (info.mode & 0o077) !== 0) { result.pending += 1; continue; }
      let bytes: Uint8Array; try { bytes = await fs.readFile(file); } catch { result.pending += 1; continue; }
      try { const job = parseOutboxJob(JSON.parse(Buffer.from(bytes).toString("utf8")), { host: producer.identity.ownerHost, nodeId: producer.nodeId, producerUuid: producer.producerUuid, homeDir: "/" }); if (job.id !== id) throw new Error("quarantine filename mismatch"); result.jobs.push({ job, file }); } catch { result.pending += 1; }
    }
    return result;
  }
  async function reconcileQuarantineCopies(producer: ProducerFiles): Promise<{ pending: number; blocked: Set<string> }> {
    const scan = await quarantinedPayloads(producer); const result = { pending: scan.pending, blocked: new Set<string>() };
    for (const item of scan.jobs) {
      const source = join(producer.jobsDir, `${item.job.id}.json`); try { await syncDirectory(fs, producer.quarantineDir); const active = await inspectTerminalCopy(fs, source, producer); if (active !== undefined) { result.blocked.add(item.job.id); if (active.auditHash !== item.job.auditHash) throw new Error("Quarantine payload mismatch"); await durableRemove(fs, source, producer.jobsDir); result.blocked.delete(item.job.id); } else await syncDirectory(fs, producer.jobsDir); }
      catch { result.pending += 1; result.blocked.add(item.job.id); }
    }
    return result;
  }
  async function quarantineExpiryPass(validatedRoot: string, producer: ProducerFiles): Promise<{ expired: number; pending: number }> {
    const scan = await quarantinedPayloads(producer); let count = 0; let pending = 0;
    for (const item of scan.jobs) if (expired(item.job, clock(), input.maxClockSkewMs)) { try { if (await expireAccepted(fs, validatedRoot, producer, item.file, producer.quarantineDir, item.job)) count += 1; else pending += 1; } catch { pending += 1; } }
    return { expired: count, pending };
  }
  async function reconcileDelivered(validatedRoot: string, producer: ProducerFiles): Promise<{ pending: number; blocked: Set<string> }> {
    const result = { pending: 0, blocked: new Set<string>() };
    for (const name of (await fs.readdir(producer.quarantineDir)).sort()) {
      if (!name.endsWith(".delivered.json")) continue; const id = name.slice(0, -15); if (!UUID.test(id)) continue; result.blocked.add(id); const auditFile = join(producer.quarantineDir, name);
      try { const audit = validateDeliveredAudit(await readSecureJson(fs, auditFile)); if (audit.jobId !== id) throw new Error("Delivered audit filename mismatch"); await convergeDeliveredTerminal(fs, validatedRoot, producer, auditFile, audit); result.blocked.delete(id); } catch { result.pending += 1; }
    }
    return result;
  }
  async function expiryPass(validatedRoot: string, producer: ProducerFiles): Promise<number> { let count = 0; for (const item of await jobsFor(producer)) if (item.kind === "job" && expired(item.job, clock(), input.maxClockSkewMs) && await expireAccepted(fs, validatedRoot, producer, item.file, producer.jobsDir, item.job)) count += 1; return count; }
  type ExactPayload = { kind: "absent" } | { kind: "pending" } | { kind: "job"; job: OutboxJob; file: string };
  async function exactPayload(producer: ProducerFiles, directory: string, id: string, malformed: boolean): Promise<ExactPayload> {
    const file = join(directory, `${id}.json`); let info;
    try { info = await fs.lstat(file); } catch (error) { return errno(error, "ENOENT") ? { kind: "absent" } : { kind: "pending" }; }
    if (info.isSymbolicLink()) { if (malformed) { try { await malformedQuarantine(fs, file, producer, clock(), { unsafePath: true }); return { kind: "absent" }; } catch { return { kind: "pending" }; } } return { kind: "pending" }; }
    if (!info.isFile() || (info.mode & 0o077) !== 0) return { kind: "pending" };
    let bytes: Uint8Array; try { bytes = await fs.readFile(file); } catch { return { kind: "pending" }; }
    try { const job = parseOutboxJob(JSON.parse(Buffer.from(bytes).toString("utf8")), { host: producer.identity.ownerHost, nodeId: producer.nodeId, producerUuid: producer.producerUuid, homeDir: "/" }); if (job.id !== id) throw new Error("payload filename mismatch"); return { kind: "job", job, file }; }
    catch { if (malformed) { try { await malformedQuarantine(fs, file, producer, clock(), { bytes }); return { kind: "absent" }; } catch { return { kind: "pending" }; } } return { kind: "pending" }; }
  }
  async function reconcileExactDelivered(validatedRoot: string, producer: ProducerFiles, id: string): Promise<"absent" | "converged" | "pending"> {
    const file = join(producer.quarantineDir, `${id}.delivered.json`); let value: unknown; try { value = await readSecureJson(fs, file); } catch (error) { return errno(error, "ENOENT") ? "absent" : "pending"; }
    try { const audit = validateDeliveredAudit(value); if (audit.jobId !== id) throw new Error("Delivered audit filename mismatch"); await convergeDeliveredTerminal(fs, validatedRoot, producer, file, audit); return "converged"; } catch { return "pending"; }
  }
  async function reconcileExactExpiry(validatedRoot: string, producer: ProducerFiles, job: OutboxJob): Promise<"absent" | "invalid" | "converged" | "pending"> {
    const file = join(producer.quarantineDir, `${job.id}.expired.json`); let info; try { info = await fs.lstat(file); } catch (error) { return errno(error, "ENOENT") ? "absent" : "pending"; }
    if (info.isSymbolicLink() || (info.isFile() && (info.mode & 0o077) !== 0)) return "invalid"; if (!info.isFile()) return "pending";
    let bytes: Uint8Array; try { bytes = await fs.readFile(file); } catch { return "pending"; } let value: unknown; try { value = JSON.parse(Buffer.from(bytes).toString("utf8")); validateExpiryAudit(value, job); } catch { return "invalid"; }
    try { await convergeExpiryTerminal(fs, validatedRoot, producer, file, job); return "converged"; } catch { return "pending"; }
  }
  async function cleanupExactSidecars(producer: ProducerFiles, id: string): Promise<number> {
    let pending = 0; const reasonFile = join(producer.quarantineDir, `${id}.reason.json`);
    try { if (await terminalPayloadAbsent(fs, producer, id)) { try { const value = await readSecureJson(fs, reasonFile); if (!record(value) || !exactKeys(value, REASON_KEYS) || value.version !== 1 || value.jobId !== id || typeof value.category !== "string" || safeCategory(value.category, "") !== value.category || value.auditHash !== hashWithout(value, "auditHash")) throw new Error("Quarantine reason is malformed"); await durableRemove(fs, reasonFile, producer.quarantineDir); } catch (error) { if (!errno(error, "ENOENT")) pending += 1; } } } catch { pending += 1; }
    const controlFile = join(producer.controlDir, `${id}.json`);
    try { if (await activePayloadAbsent(fs, producer, id)) { try { retryControl(await readSecureJson(fs, controlFile), id); await durableRemove(fs, controlFile, producer.controlDir); } catch (error) { if (!errno(error, "ENOENT")) pending += 1; } } } catch { pending += 1; }
    return pending;
  }
  async function candidateIds(producer: ProducerFiles): Promise<string[]> {
    const ids = new Set<string>();
    for (const name of await fs.readdir(producer.jobsDir)) { const temp = JOB_TEMP.exec(name); if (temp !== null) { ids.add(temp[1]!); continue; } if (name.includes(".tmp-") || !name.endsWith(".json")) continue; const id = name.slice(0, -5); if (UUID.test(id)) ids.add(id); }
    for (const name of await fs.readdir(producer.quarantineDir)) { let id: string | undefined; if (name.endsWith(".delivered.json")) id = name.slice(0, -15); else if (name.endsWith(".reason.json")) id = name.slice(0, -12); else if (name.endsWith(".json")) id = name.slice(0, -5); if (id !== undefined && UUID.test(id)) ids.add(id); }
    for (const name of await fs.readdir(producer.controlDir)) { if (!name.endsWith(".json")) continue; const id = name.slice(0, -5); if (UUID.test(id)) ids.add(id); }
    return [...ids].sort();
  }
  let deliveryCursor = 0;
  async function deliver(deliverInput: { signal?: AbortSignal; maxJobs?: number }): Promise<DeliveryResult> {
    const maxJobs = deliverInput.maxJobs ?? 10_000; if (!Number.isSafeInteger(maxJobs) || maxJobs < 1 || maxJobs > 10_000) throw new TypeError("maxJobs must be between 1 and 10000"); const operationNow = clock();
    const totals: DeliveryResult = { delivered: 0, pending: 0, quarantined: 0 }; if (deliverInput.signal?.aborted) return totals;
    const validatedRoot = await root(); let current = await producerFiles(fs, validatedRoot, input.producerPath, true); const ownerHost = current.identity.ownerHost; const paths = new Set<string>([current.path, ...adopted]); const producers: ProducerFiles[] = [current];
    for (const path of [...paths].sort()) if (path !== current.path) { const producer = await producerFiles(fs, validatedRoot, path, true); if (producer.identity.ownerHost !== ownerHost) throw new Error("Adopted producer owner host does not match current producer"); producers.push(producer); }
    const locks = new Map<string, CaptureReservation>(); let spent = 0;
    for (let index = 0; index < producers.length; index += 1) { let producer = producers[index]!; const lock = await admissionLockForProducer(fs, validatedRoot, producer); if (lock !== undefined) locks.set(producer.path, lock); const deadlineExpired = lock?.deadline !== null && lock?.deadline !== undefined && Date.parse(lock.deadline) <= operationNow + input.maxClockSkewMs; const inactiveLockedProducer = lock !== undefined && producerInactive(producer.state, operationNow, heartbeatTimeoutMs, input.maxClockSkewMs); const fencedTemp = producer.fenced && (await fs.readdir(producer.jobsDir)).some((name) => JOB_TEMP.test(name)); if ((!producer.fencePublished && producer.fenced) || deadlineExpired || inactiveLockedProducer || (producer.fenced && lock !== undefined) || fencedTemp) { if (spent >= maxJobs) continue; try { producer = await fenceProducer(fs, validatedRoot, producer); producers[index] = producer; if (index === 0) current = producer; totals.quarantined += await recoverFencedAdmissions(fs, validatedRoot, producer, operationNow); spent += 1; } catch { totals.pending += 1; return totals; } } }
    const remainingBudget = maxJobs - spent; if (remainingBudget === 0) return totals;
    const candidates: Array<{ producer: ProducerFiles; id: string }> = []; for (const producer of producers) { const ids = new Set(await candidateIds(producer)); const lock = locks.get(producer.path); if (lock !== undefined) ids.add(lock.jobId); for (const id of ids) candidates.push({ producer, id }); }
    candidates.sort((left, right) => `${left.producer.path}\0${left.id}`.localeCompare(`${right.producer.path}\0${right.id}`)); if (candidates.length === 0) { deliveryCursor = 0; return totals; }
    const start = deliveryCursor % candidates.length; const selected = Array.from({ length: Math.min(remainingBudget, candidates.length) }, (_, index) => candidates[(start + index) % candidates.length]!); deliveryCursor = (start + selected.length) % candidates.length;
    for (const { producer, id } of selected) {
      if (deliverInput.signal?.aborted) { totals.pending += 1; break; }
      let blocked = false; const quarantined = await exactPayload(producer, producer.quarantineDir, id, false);
      if (quarantined.kind === "pending") { totals.pending += 1; blocked = true; }
      else if (quarantined.kind === "job") {
        const source = join(producer.jobsDir, `${id}.json`);
        try { await syncDirectory(fs, producer.quarantineDir); const activeCopy = await inspectTerminalCopy(fs, source, producer); if (activeCopy !== undefined) { if (activeCopy.auditHash !== quarantined.job.auditHash) throw new Error("Quarantine payload mismatch"); await durableRemove(fs, source, producer.jobsDir); } else await syncDirectory(fs, producer.jobsDir); }
        catch { totals.pending += 1; blocked = true; }
        if (!blocked) { const expiry = await reconcileExactExpiry(validatedRoot, producer, quarantined.job); if (expiry === "pending") { totals.pending += 1; blocked = true; } else if (expiry === "converged") { totals.quarantined += 1; continue; } else if (expiry === "invalid" && !expired(quarantined.job, clock(), input.maxClockSkewMs)) { totals.pending += 1; blocked = true; } }
        if (!blocked && expired(quarantined.job, clock(), input.maxClockSkewMs)) { try { if (await expireAccepted(fs, validatedRoot, producer, quarantined.file, producer.quarantineDir, quarantined.job)) totals.quarantined += 1; else totals.pending += 1; } catch { totals.pending += 1; } continue; }
      }
      const delivered = await reconcileExactDelivered(validatedRoot, producer, id); if (delivered === "pending") { totals.pending += 1; blocked = true; }
      if (blocked || delivered === "converged") continue; if (quarantined.kind === "job") { try { await finalizeJobAdmission(fs, validatedRoot, producer, quarantined.job); totals.quarantined += 1; } catch { totals.pending += 1; } continue; }
      const item = await exactPayload(producer, producer.jobsDir, id, true); if (item.kind === "pending") { totals.pending += 1; continue; } if (item.kind === "absent") { totals.pending += await cleanupExactSidecars(producer, id); continue; }
      const expiry = await reconcileExactExpiry(validatedRoot, producer, item.job); if (expiry === "pending") { totals.pending += 1; continue; } if (expiry === "converged") { totals.quarantined += 1; continue; } if (expiry === "invalid" && !expired(item.job, clock(), input.maxClockSkewMs)) { totals.pending += 1; continue; }
      const key = `${producer.path}\0${id}`; if (active.has(key)) { totals.pending += 1; continue; }
      if (expired(item.job, clock(), input.maxClockSkewMs)) { try { if (await expireAccepted(fs, validatedRoot, producer, item.file, producer.jobsDir, item.job)) totals.quarantined += 1; else totals.pending += 1; } catch { totals.pending += 1; } continue; }
      const controlRead = await readControl(fs, producer, id); if (controlRead.kind === "defer") { totals.pending += 1; continue; } const control = controlRead.kind === "valid" ? controlRead.control : undefined;
      const attemptNow = clock(); if (control !== undefined && (control.nextAttemptAt > attemptNow || (control.nextAttemptAt === MAX_TIME && attemptNow === MAX_TIME))) { totals.pending += 1; continue; }
      if (deliverInput.signal?.aborted) { totals.pending += 1; break; } const boundaryNow = clock();
      if (expired(item.job, boundaryNow, input.maxClockSkewMs)) { try { if (await expireAccepted(fs, validatedRoot, producer, item.file, producer.jobsDir, item.job)) totals.quarantined += 1; else totals.pending += 1; } catch { totals.pending += 1; } continue; }
      active.add(key);
      try {
        const deadlineRemaining = item.job.deadline === null ? attemptTimeoutMs : Math.max(1, Date.parse(item.job.deadline) - boundaryNow - input.maxClockSkewMs);
        const outcome = await invokeProcessor(input.processor, item.job, deliverInput.signal, Math.min(attemptTimeoutMs, deadlineRemaining)); const completionNow = clock();
        if (outcome.kind !== "delivered" && expired(item.job, completionNow, input.maxClockSkewMs)) { try { if (await expireAccepted(fs, validatedRoot, producer, item.file, producer.jobsDir, item.job)) totals.quarantined += 1; else totals.pending += 1; } catch { totals.pending += 1; } continue; }
        if (outcome.kind === "delivered") {
          let accepted: OutboxJob; try { accepted = await processorDeliveredProof(fs, producer, item.job); } catch { totals.pending += 1; continue; }
          try { await finalizeJobAdmission(fs, validatedRoot, producer, accepted); } catch { totals.pending += 1; continue; }
          let deliveredAuditFile: string; try { deliveredAuditFile = await ensureDeliveredAudit(fs, producer, item.job, completionNow); } catch { totals.pending += 1; continue; }
          try { const audit = validateDeliveredAudit(await readSecureJson(fs, deliveredAuditFile)); await convergeDeliveredTerminal(fs, validatedRoot, producer, deliveredAuditFile, audit); } catch { totals.pending += 1; continue; } totals.delivered += 1;
        } else if (outcome.kind === "quarantined") { try { if (await quarantineAccepted(fs, producer, item.file, item.job, outcome.category)) { await finalizeJobAdmission(fs, validatedRoot, producer, item.job); totals.quarantined += 1; } else totals.pending += 1; } catch { totals.pending += 1; } }
        else { await writePending(fs, producer, item.job, control, completionNow, outcome.category, retryBaseMs, retryMaxMs).catch(() => undefined); totals.pending += 1; }
      } finally { active.delete(key); }
    }
    return totals;
  }
  function assertAdoptable(producer: ProducerFiles, now: number): void {
    if (!producerInactive(producer.state, now, heartbeatTimeoutMs, input.maxClockSkewMs)) throw new Error("Producer is still active and cannot be adopted");
  }
  async function adopt(path: string): Promise<void> {
    const validatedRoot = await root(); const adoptionNow = clock(); const current = await producerFiles(fs, validatedRoot, input.producerPath, true); let producer = await producerFiles(fs, validatedRoot, path, true); if (producer.identity.ownerHost !== current.identity.ownerHost) throw new Error("Adopted producer owner host does not match current producer"); if (!producer.fenced) assertAdoptable(producer, adoptionNow); producer = await fenceProducer(fs, validatedRoot, producer); await recoverFencedAdmissions(fs, validatedRoot, producer, adoptionNow);
    await reconcileQuarantineCopies(producer); await quarantineExpiryPass(validatedRoot, producer); await reconcileDelivered(validatedRoot, producer); await cleanupOrphanTerminalSidecars(fs, producer); await expiryPass(validatedRoot, producer);
    const refreshedCurrent = await producerFiles(fs, validatedRoot, input.producerPath); const refreshed = await producerFiles(fs, validatedRoot, producer.path); if (!refreshed.fenced || refreshed.identity.ownerHost !== refreshedCurrent.identity.ownerHost || refreshedCurrent.identity.ownerHost !== current.identity.ownerHost) throw new Error("Adopted producer owner host or fence changed"); await cleanupOfflineReservations(fs, validatedRoot, refreshed); adopted.add(refreshed.path);
  }
  return Object.freeze({ deliver, adopt, shutdown: async (shutdownInput = {}) => { try { return await deliver(shutdownInput); } catch { return { delivered: 0, pending: 0, quarantined: 0 }; } } });
}


/** The only control surface Task 7 needs; Task 8/13 must provide its bounded revocation snapshot. */
export interface IngestControlReader {
  read(): Promise<{
    state: "active" | "draining" | "retired";
    privacyEpoch: number;
    coordinationPolicyEpoch: number;
    policyHash: string;
    revokedDestinationIds: readonly string[];
  }>;
}
/** Opaque Qdrant capability created only by the validated destination factory. */
export interface BoundQdrantDestination {
  readonly destination: AuthorizedDestination;
  /** Immutable host/physical collection pairing of the opaque writer. */
  readonly ownerHost: "pi" | "prime";
  readonly collection: "pi_memory" | "prime_memory";
  /** Independently pinned control policy; never an episode processing-policy ID. */
  readonly coordination: { readonly policyHash: string; readonly policyEpoch: number };
  insertAndReadback(record: ProcessingPolicyRecord | EpisodeRecord): Promise<"inserted" | "existing">;
  retrieve<T extends ProcessingPolicyRecord | EpisodeRecord>(recordType: T["recordType"], id: string): Promise<T | null>;
}
/** Opaque BGE-M3-only capability created only by the validated destination factory. */
export interface BoundEmbeddingDestination {
  readonly destination: AuthorizedDestination;
  embed(input: { model: string; text: string; signal?: AbortSignal }): Promise<readonly number[]>;
}
export interface IngestInput {
  job: OutboxJob;
  now: number;
  localPolicy: ProcessingPolicy;
  qdrant: BoundQdrantDestination;
  embedding: BoundEmbeddingDestination;
  control: IngestControlReader;
  maxClockSkewMs: number;
}

type IngestCategory = "aborted" | "control_unavailable" | "policy_invalid" | "policy_unauthorized" | "expired" | "scanner_rejected" | "hash_collision" | "qdrant_failed" | "embedding_failed" | "embedding_invalid" | "episode_invalid";
type IngestAttempt = { result: DeliveryResult; category?: IngestCategory };
type ControlSnapshot = Awaited<ReturnType<IngestControlReader["read"]>>;
const INGEST_CONTROL_KEYS = ["state", "privacyEpoch", "coordinationPolicyEpoch", "policyHash", "revokedDestinationIds"] as const;

function ingestResult(kind: "delivered" | "pending" | "quarantined", count: number, category?: IngestCategory): IngestAttempt {
  return { result: { delivered: kind === "delivered" ? count : 0, pending: kind === "pending" ? count : 0, quarantined: kind === "quarantined" ? count : 0 }, ...(category === undefined ? {} : { category }) };
}
function exactDestination(left: AuthorizedDestination, right: AuthorizedDestination): boolean {
  return left.id === right.id && left.residency === right.residency && left.dataUse === right.dataUse;
}
function destinationFor(policy: ProcessingPolicy, lane: "qdrant" | "embedding"): AuthorizedDestination {
  return { id: policy.destinationIds[lane], residency: policy.residency, dataUse: policy.dataUse };
}
function exactRedactedDestinationId(value: unknown): value is string {
  if (typeof value !== "string" || value.length === 0 || value.length > 256 || !/^[A-Za-z0-9._:/-]+$/u.test(value)) return false;
  const checked = redactAndScan({ text: value, maxChars: 256, homeDir: "/" });
  return !checked.dropped && checked.secretScan === "passed" && checked.redactionStatus === "unchanged" && checked.text === value;
}
function validControlSnapshot(value: unknown, qdrant: BoundQdrantDestination): value is ControlSnapshot {
  if (typeof value !== "object" || value === null) return false;
  const snapshot = value as Record<string, unknown>; const revoked = snapshot.revokedDestinationIds;
  if (!exactKeys(snapshot, INGEST_CONTROL_KEYS) || snapshot.state !== "active" || !Number.isSafeInteger(snapshot.privacyEpoch) || (snapshot.privacyEpoch as number) < 0 || !Number.isSafeInteger(snapshot.coordinationPolicyEpoch) || (snapshot.coordinationPolicyEpoch as number) < 0 || typeof snapshot.policyHash !== "string" || snapshot.policyHash.length === 0 || snapshot.policyHash.length > 512 || !Array.isArray(revoked) || revoked.length > 1024 || revoked.some((id) => !exactRedactedDestinationId(id)) || new Set(revoked).size !== revoked.length) return false;
  return snapshot.policyHash === qdrant.coordination.policyHash && snapshot.coordinationPolicyEpoch === qdrant.coordination.policyEpoch;
}
function isRevoked(snapshot: ControlSnapshot, destination: AuthorizedDestination): boolean { return snapshot.revokedDestinationIds.includes(destination.id); }
function clockValue(clock: () => number): number { const value = clock(); if (!finiteTime(value)) throw new TypeError("Ingest clock is invalid"); return value; }
function isExpired(policy: ProcessingPolicy, now: number, skew: number): boolean { return isPolicyExpired(policy, now, skew); }
function episodeText(episode: EpisodeRecord): string { return episodeSemanticProjection(episode); }
function finalEpisodeMaterialIsSafe(episode: EpisodeRecord): boolean {
  const text = episodeText(episode); if (text.length === 0 || text.length > 16_000) return false;
  const checked = redactAndScan({ text, maxChars: 16_000, homeDir: "/" });
  return !checked.dropped && checked.secretScan === "passed" && checked.text === text;
}
function validVector(vector: readonly number[]): vector is readonly number[] { return vector.length === 1024 && vector.every((value) => typeof value === "number" && Number.isFinite(value)); }
function sameEpisodeReadback(readback: unknown, source: EpisodeRecord): boolean {
  if (!record(readback)) return false;
  try { const candidate = readback as unknown as EpisodeRecord; return candidate.recordType === "episode" && candidate.id === source.id && candidate.contentHash === source.contentHash && canonicalRecordHash(candidate) === candidate.contentHash && candidate.ownerHost === source.ownerHost && candidate.schemaRevision === source.schemaRevision && candidate.privacyEpoch === source.privacyEpoch && candidate.processingPolicyId === source.processingPolicyId && candidate.expiresAt === source.expiresAt && candidate.secretScan === "passed" && candidate.redactionStatus === source.redactionStatus && candidate.vector !== undefined && validVector(candidate.vector); } catch { return false; }
}
function verifiedDifferentCanonicalHash(readback: unknown, expected: ProcessingPolicyRecord | EpisodeRecord): boolean {
  if (!record(readback) || readback.recordType !== expected.recordType || readback.id !== expected.id || typeof readback.contentHash !== "string" || readback.contentHash === expected.contentHash) return false;
  try { return canonicalRecordHash(readback as unknown as ProcessingPolicyRecord | EpisodeRecord) === readback.contentHash; } catch { return false; }
}
function policyRecord(job: OutboxJob, privacyEpoch: number): ProcessingPolicyRecord {
  const pending: ProcessingPolicyRecord = {
    recordType: "processing_policy", id: job.policy.id, ownerHost: job.ownerHost, schemaRevision: 1, createdAt: job.createdAt,
    privacyEpoch, processingPolicyId: job.policy.id, expiresAt: job.policy.expiresAt, contentHash: "pending",
    policy: job.policy, canonicalHash: job.policy.id,
  };
  return { ...pending, contentHash: canonicalRecordHash(pending) };
}
function policyRecordReadbackIsExact(readback: unknown, expected: ProcessingPolicyRecord): boolean {
  if (!record(readback)) return false;
  try { const candidate = readback as unknown as ProcessingPolicyRecord; return candidate.recordType === "processing_policy" && candidate.id === expected.id && candidate.contentHash === expected.contentHash && canonicalRecordHash(candidate) === candidate.contentHash && candidate.ownerHost === expected.ownerHost && candidate.schemaRevision === expected.schemaRevision && candidate.privacyEpoch === expected.privacyEpoch && candidate.processingPolicyId === expected.processingPolicyId && candidate.expiresAt === expected.expiresAt && candidate.canonicalHash === expected.canonicalHash && canonicalStringify(candidate.policy) === canonicalStringify(expected.policy); } catch { return false; }
}
function staticIngestValidation(input: IngestInput, now: number): { effective: ProcessingPolicy } | IngestAttempt {
  const count = Array.isArray(input.job?.episodes) && input.job.episodes.length > 0 ? input.job.episodes.length : 1;
  if (!Number.isSafeInteger(input.maxClockSkewMs) || input.maxClockSkewMs < 0 || input.maxClockSkewMs > 3_600_000) return ingestResult("quarantined", count, "policy_invalid");
  try {
    if (input.job.policyId !== input.job.policy.id || input.job.deadline !== input.job.policy.expiresAt || processingPolicyHash(input.job.policy) !== input.job.policy.id || processingPolicyHash(input.localPolicy) !== input.localPolicy.id) return ingestResult("quarantined", count, "policy_invalid");
    const effective = intersectPolicies([input.job.policy], input.localPolicy);
    if (effective === null) return ingestResult("pending", count, "policy_unauthorized");
    if (!exactDestination(input.qdrant.destination, destinationFor(effective, "qdrant")) || !exactDestination(input.embedding.destination, destinationFor(effective, "embedding"))) return ingestResult("pending", count, "policy_unauthorized");
    if (input.qdrant.ownerHost !== input.job.ownerHost || input.qdrant.collection !== expectedQdrantCollection(input.job.ownerHost)) return ingestResult("pending", count, "policy_unauthorized");
    if (isExpired(effective, now, input.maxClockSkewMs)) return ingestResult("quarantined", count, "expired");
    for (const episode of input.job.episodes) {
      if (episode.recordType !== "episode" || episode.ownerHost !== input.job.ownerHost || episode.host !== input.job.ownerHost || episode.processingPolicyId !== input.job.policy.id || episode.expiresAt !== input.job.policy.expiresAt || episode.originProvider !== input.job.policy.originProvider || episode.destinationId !== input.job.policy.destinationIds.qdrant || episode.vector !== undefined || episode.secretScan !== "passed" || !finalEpisodeMaterialIsSafe(episode) || episode.contentHash !== canonicalRecordHash(episode)) return ingestResult("quarantined", count, episode.secretScan === "passed" ? "episode_invalid" : "scanner_rejected");
    }
    return { effective };
  } catch { return ingestResult("quarantined", count, "policy_invalid"); }
}
async function readStableControl(input: IngestInput): Promise<ControlSnapshot | undefined> {
  try { const snapshot = await input.control.read(); return validControlSnapshot(snapshot, input.qdrant) ? snapshot : undefined; } catch { return undefined; }
}
function controlAllows(snapshot: ControlSnapshot, input: IngestInput, privacyEpoch: number): boolean {
  return snapshot.privacyEpoch === privacyEpoch && !isRevoked(snapshot, input.qdrant.destination) && !isRevoked(snapshot, input.embedding.destination);
}
async function ingestAttempt(rawInput: IngestInput, clock: () => number, signal?: AbortSignal): Promise<IngestAttempt> {
  // This is a public direct-call seam as well as the Task 5 callback: parse
  // and clone the durable envelope before reading policy/control or egressing.
  let job: OutboxJob;
  try { job = parseOutboxJob(rawInput.job); } catch { return ingestResult("quarantined", 1, "episode_invalid"); }
  const input: IngestInput = { ...rawInput, job }; const count = job.episodes.length;
  let started: number; try { started = clockValue(clock); } catch { return ingestResult("pending", count, "control_unavailable"); }
  if (signal?.aborted) return ingestResult("pending", count, "aborted");
  const initial = staticIngestValidation(input, started); if ("result" in initial) return initial;
  const first = await readStableControl(input);
  if (signal?.aborted) return ingestResult("pending", count, "aborted");
  let beforePolicyNow: number; try { beforePolicyNow = clockValue(clock); } catch { return ingestResult("pending", count, "control_unavailable"); }
  // An embedding-only revocation is deliberately stricter: it suppresses the
  // policy point too, so no part of the job egresses after either revocation.
  if (first === undefined || !controlAllows(first, input, first.privacyEpoch)) return ingestResult("pending", count, "control_unavailable");
  if (job.episodes.some((episode) => episode.privacyEpoch !== first.privacyEpoch)) return ingestResult("pending", count, "control_unavailable");
  if (isExpired(initial.effective, beforePolicyNow, input.maxClockSkewMs)) return ingestResult("quarantined", count, "expired");
  if (signal?.aborted) return ingestResult("pending", count, "aborted");
  const policy = policyRecord(job, first.privacyEpoch);
  try { await input.qdrant.insertAndReadback(policy); }
  catch (error) { const collision = error instanceof QdrantContentHashCollisionError; return ingestResult(collision ? "quarantined" : "pending", count, collision ? "hash_collision" : "qdrant_failed"); }
  if (signal?.aborted) return ingestResult("pending", count, "aborted");
  let policyReadback: ProcessingPolicyRecord | null;
  try { policyReadback = await input.qdrant.retrieve("processing_policy", policy.id) as ProcessingPolicyRecord | null; }
  catch { return ingestResult("pending", count, "qdrant_failed"); }
  if (signal?.aborted) return ingestResult("pending", count, "aborted");
  if (!policyRecordReadbackIsExact(policyReadback, policy)) return ingestResult(verifiedDifferentCanonicalHash(policyReadback, policy) ? "quarantined" : "pending", count, verifiedDifferentCanonicalHash(policyReadback, policy) ? "hash_collision" : "qdrant_failed");
  for (const source of job.episodes) {
    if (signal?.aborted) return ingestResult("pending", count, "aborted");
    const beforeEmbed = await readStableControl(input);
    if (signal?.aborted) return ingestResult("pending", count, "aborted");
    let beforeNow: number; try { beforeNow = clockValue(clock); } catch { return ingestResult("pending", count, "control_unavailable"); }
    const beforeExpired = isExpired(initial.effective, beforeNow, input.maxClockSkewMs);
    if (beforeEmbed === undefined || !controlAllows(beforeEmbed, input, first.privacyEpoch) || beforeExpired || signal?.aborted) return ingestResult(beforeExpired ? "quarantined" : "pending", count, signal?.aborted ? "aborted" : beforeExpired ? "expired" : "control_unavailable");
    if (signal?.aborted) return ingestResult("pending", count, "aborted");
    let existing: EpisodeRecord | null;
    try { existing = await input.qdrant.retrieve("episode", source.id) as EpisodeRecord | null; }
    catch { return ingestResult("pending", count, "qdrant_failed"); }
    if (signal?.aborted) return ingestResult("pending", count, "aborted");
    if (existing !== null) {
      if (sameEpisodeReadback(existing, source)) {
        const finalExisting = await readStableControl(input);
        if (signal?.aborted) return ingestResult("pending", count, "aborted");
        let finalExistingNow: number; try { finalExistingNow = clockValue(clock); } catch { return ingestResult("pending", count, "control_unavailable"); }
        const existingExpired = isExpired(initial.effective, finalExistingNow, input.maxClockSkewMs);
        if (finalExisting === undefined || !controlAllows(finalExisting, input, first.privacyEpoch) || existingExpired) return ingestResult(existingExpired ? "quarantined" : "pending", count, existingExpired ? "expired" : "control_unavailable");
        continue;
      }
      return ingestResult(verifiedDifferentCanonicalHash(existing, source) ? "quarantined" : "pending", count, verifiedDifferentCanonicalHash(existing, source) ? "hash_collision" : "qdrant_failed");
    }
    // A null lookup is also an authorization boundary: it may have taken long
    // enough for revocation, state, privacy epoch, or expiry to change.
    const afterLookup = await readStableControl(input);
    if (signal?.aborted) return ingestResult("pending", count, "aborted");
    let afterLookupNow: number; try { afterLookupNow = clockValue(clock); } catch { return ingestResult("pending", count, "control_unavailable"); }
    const afterLookupExpired = isExpired(initial.effective, afterLookupNow, input.maxClockSkewMs);
    if (afterLookup === undefined || !controlAllows(afterLookup, input, first.privacyEpoch) || afterLookupExpired) return ingestResult(afterLookupExpired ? "quarantined" : "pending", count, afterLookupExpired ? "expired" : "control_unavailable");
    if (signal?.aborted) return ingestResult("pending", count, "aborted");
    let vector: readonly number[];
    try { vector = await input.embedding.embed({ model: "bge-m3", text: episodeText(source), ...(signal === undefined ? {} : { signal }) }); }
    catch { return ingestResult("pending", count, signal?.aborted ? "aborted" : "embedding_failed"); }
    if (signal?.aborted) return ingestResult("pending", count, "aborted");
    if (!validVector(vector)) return ingestResult("pending", count, "embedding_invalid");
    const final = await readStableControl(input);
    if (signal?.aborted) return ingestResult("pending", count, "aborted");
    let now: number; try { now = clockValue(clock); } catch { return ingestResult("pending", count, "control_unavailable"); }
    const finalExpired = isExpired(initial.effective, now, input.maxClockSkewMs);
    if (final === undefined || !controlAllows(final, input, first.privacyEpoch) || finalExpired) return ingestResult(finalExpired ? "quarantined" : "pending", count, finalExpired ? "expired" : "control_unavailable");
    const materialized: EpisodeRecord = { ...source, vector: [...vector] };
    if (signal?.aborted) return ingestResult("pending", count, "aborted");
    try { await input.qdrant.insertAndReadback(materialized); }
    catch (error) { const collision = error instanceof QdrantContentHashCollisionError; return ingestResult(collision ? "quarantined" : "pending", count, collision ? "hash_collision" : "qdrant_failed"); }
    if (signal?.aborted) return ingestResult("pending", count, "aborted");
    let episodeReadback: EpisodeRecord | null;
    try { episodeReadback = await input.qdrant.retrieve("episode", materialized.id) as EpisodeRecord | null; }
    catch { return ingestResult("pending", count, "qdrant_failed"); }
    if (signal?.aborted) return ingestResult("pending", count, "aborted");
    if (!sameEpisodeReadback(episodeReadback, materialized)) return ingestResult(verifiedDifferentCanonicalHash(episodeReadback, materialized) ? "quarantined" : "pending", count, verifiedDifferentCanonicalHash(episodeReadback, materialized) ? "hash_collision" : "qdrant_failed");
  }
  return ingestResult("delivered", count);
}
function safeIngestCount(value: unknown): number {
  return record(value) && Array.isArray(value.episodes) && value.episodes.length > 0 && value.episodes.length <= 1024 ? value.episodes.length : 1;
}

/**
 * Ingest a durable job without throwing into a host turn.  Its public `now`
 * value is fixed for deterministic direct callers; the production processor
 * below supplies its live clock so expiry is checked again after embedding.
 */
export async function ingestPendingJobs(input: IngestInput): Promise<DeliveryResult> { return (await ingestAttempt(input, () => input.now)).result; }

/** The sole production OutboxJobProcessor; Task 5 remains scheduling-only. */
export function createIngestProcessor(input: Omit<IngestInput, "job" | "now"> & { now: () => number }): OutboxJobProcessor {
  if (typeof input.now !== "function") throw new TypeError("Ingest processor requires a clock");
  return Object.freeze({
    process: async (job: OutboxJob, processInput: { signal?: AbortSignal }) => {
      const count = safeIngestCount(job);
      const attempt = await ingestAttempt({ ...input, job, now: 0 }, input.now, processInput.signal).catch(() => ingestResult("pending", count, "control_unavailable"));
      if (attempt.result.delivered === count) return { status: "delivered" as const };
      if (attempt.result.quarantined > 0) return { status: "quarantined" as const, category: attempt.category ?? "episode_invalid" };
      return { status: "pending" as const, category: attempt.category ?? "control_unavailable" };
    },
  });
}
