import { SessionManager } from "@earendil-works/pi-coding-agent";
import { types as nodeTypes } from "node:util";
import type { HostId } from "../types.js";
import { resolveAgentMarker, type AgentMarker } from "../capture/episode.js";
import { canonicalStringify, sha256Hex } from "../domain/canonical.js";
import { ProductionCoordinationStore } from "../qdrant/write.js";
import { runCurationCore, type CurationRunResult, type CurationWorkerInput } from "../curation/worker.js";

const ROOT_WORKER_ISSUER = Symbol("pi-qdrant-memory-v2.root-worker-issuer");
const SESSION_MANAGER_PROTOTYPE = SessionManager.prototype;
const SESSION_MANAGER_METHODS = Object.freeze({
  getHeader: Object.getOwnPropertyDescriptor(SESSION_MANAGER_PROTOTYPE, "getHeader")?.value,
  getBranch: Object.getOwnPropertyDescriptor(SESSION_MANAGER_PROTOTYPE, "getBranch")?.value,
  getEntries: Object.getOwnPropertyDescriptor(SESSION_MANAGER_PROTOTYPE, "getEntries")?.value,
  getSessionId: Object.getOwnPropertyDescriptor(SESSION_MANAGER_PROTOTYPE, "getSessionId")?.value,
});
const SECRET = /(?:api[-_]?key|access[-_]?token|authorization|bearer|credential|password|secret|token)/iu;

/**
 * Nominal root capability.  There is intentionally no public issuer or
 * runtime/factory adapter: only the high-level lifecycle operation below can
 * construct this class, and the lease kernel accepts only this private brand.
 */
export class RootWorkerContext {
  readonly #issuer: symbol;
  readonly #host: HostId;
  readonly #evidenceHash: string;
  readonly #clock: () => number;
  readonly #nodeId: string;
  readonly #leaseMs: number;
  readonly #maxClockSkewMs: number;
  #lastSample: number | null = null;

  constructor(host: HostId, evidenceHash: string, issuer: symbol, clock: (() => number) | undefined, nodeId: string | undefined, leaseMs: number | undefined, maxClockSkewMs: number | undefined) {
    if (issuer !== ROOT_WORKER_ISSUER) throw new TypeError("Root worker capability requires the module issuer");
    if (host !== "pi" && host !== "prime") throw new TypeError("Root worker host is invalid");
    if (typeof evidenceHash !== "string" || !/^[0-9a-f]{64}$/u.test(evidenceHash)) throw new TypeError("Root worker evidence is invalid");
    if (clock !== undefined && typeof clock !== "function") throw new TypeError("Root worker clock is invalid");
    if (typeof nodeId !== "string" || nodeId.length === 0 || nodeId.length > 512 || SECRET.test(nodeId)) throw new TypeError("Root worker node id is invalid");
    if (typeof leaseMs !== "number" || !Number.isSafeInteger(leaseMs) || leaseMs < 1 || leaseMs > 86_400_000) throw new TypeError("Root worker lease TTL is invalid");
    if (typeof maxClockSkewMs !== "number" || !Number.isSafeInteger(maxClockSkewMs) || maxClockSkewMs < 0 || maxClockSkewMs > 3_600_000) throw new TypeError("Root worker clock skew is invalid");
    const trustedLeaseMs = leaseMs; const trustedMaxClockSkewMs = maxClockSkewMs;
    this.#issuer = issuer;
    this.#host = host;
    this.#evidenceHash = evidenceHash;
    this.#clock = clock ?? (() => Date.now());
    this.#nodeId = nodeId;
    this.#leaseMs = trustedLeaseMs;
    this.#maxClockSkewMs = trustedMaxClockSkewMs;
    Object.freeze(this);
  }
  static isValid(value: unknown): value is RootWorkerContext {
    if (typeof value !== "object" || value === null || !(#issuer in value)) return false;
    return value instanceof RootWorkerContext && value.#issuer === ROOT_WORKER_ISSUER;
  }
  now(): number {
    const value = this.#clock();
    if (!Number.isSafeInteger(value) || value < 0) throw new TypeError("Root worker clock is invalid");
    if (this.#lastSample !== null && value < this.#lastSample) throw new TypeError("Root worker clock went backwards");
    this.#lastSample = value;
    return value;
  }
  get host(): HostId { return this.#host; }
  get evidenceHash(): string { return this.#evidenceHash; }
  get nodeId(): string { return this.#nodeId; }
  get leaseMs(): number { return this.#leaseMs; }
  get maxClockSkewMs(): number { return this.#maxClockSkewMs; }
}
Object.freeze(RootWorkerContext);
Object.freeze(RootWorkerContext.prototype);

// SessionManager is a runtime nominal class.  Do not monkey-patch its static
// factories: host contexts may be created before this module loads.  The
// checks below require the exact prototype methods and exercise the real
// internal session operations before any store/network access.
interface GenuineSessionManagerSnapshot {
  readonly header: Record<string, unknown> | null;
  /** Reads internal session state only after the root marker is proven. */
  readonly verifyRootState: () => string | null;
}
/** Validate nominal lifecycle and read only its marker header. Internal session
 * entries/session id are deliberately deferred until root work is allowed. */
function snapshotGenuineSessionManager(value: unknown): GenuineSessionManagerSnapshot | null {
  if (nodeTypes.isProxy(value) || !(value instanceof SessionManager)) return null;
  if (Object.getPrototypeOf(value) !== SESSION_MANAGER_PROTOTYPE) return null;
  if (Object.prototype.hasOwnProperty.call(value, "getHeader") || Object.prototype.hasOwnProperty.call(value, "getBranch")) return null;
  const prototype = SESSION_MANAGER_PROTOTYPE;
  const getHeader = SESSION_MANAGER_METHODS.getHeader;
  const getBranch = SESSION_MANAGER_METHODS.getBranch;
  const getEntries = SESSION_MANAGER_METHODS.getEntries;
  const getSessionId = SESSION_MANAGER_METHODS.getSessionId;
  if (typeof getHeader !== "function" || typeof getBranch !== "function" || typeof getEntries !== "function" || typeof getSessionId !== "function") return null;
  if (Object.getOwnPropertyDescriptor(prototype, "getHeader")?.value !== getHeader || Object.getOwnPropertyDescriptor(prototype, "getBranch")?.value !== getBranch || Object.getOwnPropertyDescriptor(prototype, "getEntries")?.value !== getEntries || Object.getOwnPropertyDescriptor(prototype, "getSessionId")?.value !== getSessionId) return null;
  try {
    const rawHeader = getHeader.call(value);
    if (!(rawHeader === null || typeof rawHeader === "object")) return null;
    const header = snapshotHeader(rawHeader);
    return Object.freeze({ header, verifyRootState: (): string | null => {
      try { const entries = getEntries.call(value); const sessionId = getSessionId.call(value); if (!Array.isArray(entries) || typeof sessionId !== "string" || sessionId.length === 0) return null; return sha256Hex(canonicalStringify({ sessionId, entryCount: entries.length })); } catch { return null; }
    } });
  } catch { return null; }
}
function snapshotEnvironment(input: Record<string, string | undefined>): Record<string, string | undefined> {
  if (nodeTypes.isProxy(input) || input === null || typeof input !== "object" || Array.isArray(input)) throw new TypeError("Root lifecycle environment is invalid");
  const result: Record<string, string | undefined> = {};
  // Marker resolution is intentionally capability-minimal: never enumerate or
  // touch unrelated environment properties (which may be secret-bearing or
  // accessor-backed). Read only the three documented marker keys, and only
  // through own data descriptors so hostile getters fail closed without being
  // invoked.
  for (const key of ["RLM_DEPTH", "PI_SUBAGENT_CHILD", "PI_SUBAGENT_DEPTH"] as const) {
    const descriptor = Object.getOwnPropertyDescriptor(input, key);
    if (descriptor === undefined) continue;
    if (!("value" in descriptor)) throw new TypeError("Root lifecycle environment is invalid");
    const value = descriptor.value;
    if (value !== undefined && typeof value !== "string") throw new TypeError("Root lifecycle environment is invalid");
    result[key] = value;
  }
  return Object.freeze(result);
}
function snapshotHeader(value: unknown): Record<string, unknown> | null {
  if (value === undefined || value === null) return null;
  if (nodeTypes.isProxy(value) || typeof value !== "object" || Array.isArray(value)) throw new TypeError("Root lifecycle header is invalid");
  // Project only the host marker fields. Never enumerate/JSON-clone the raw
  // header: arbitrary secret-bearing fields and accessors are outside the
  // authority contract. Required marker accessors fail closed; unknown keys
  // (including symbols) are ignored without being read.
  const projected: Record<string, unknown> = {};
  const depth = Object.getOwnPropertyDescriptor(value, "rlmDepth");
  if (depth !== undefined) {
    if (!("value" in depth)) throw new TypeError("Root lifecycle header is invalid");
    const raw = depth.value;
    const validNumber = typeof raw === "number" && Number.isSafeInteger(raw) && raw >= 0 && raw <= 1000;
    const validString = typeof raw === "string" && /^\d{1,4}$/u.test(raw) && Number(raw) <= 1000;
    projected.rlmDepth = validNumber ? raw : validString ? Number(raw) : "invalid";
  }
  const parent = Object.getOwnPropertyDescriptor(value, "parentSession");
  if (parent !== undefined) {
    if (!("value" in parent)) throw new TypeError("Root lifecycle header is invalid");
    const raw = parent.value;
    if (raw === undefined || raw === null) projected.parentSession = null;
    else if (nodeTypes.isProxy(raw)) throw new TypeError("Root lifecycle header is invalid");
    else if (typeof raw === "string" || (typeof raw === "object" && raw !== null)) projected.parentSession = "present";
    else projected.parentSession = 0; // invalid marker; forces child/fail-closed
  }
  return Object.freeze(projected);
}
/**
 * Own a canonical JSON value before any lifecycle value crosses an await.  In
 * particular, never use `[...value]` for membership/policy arrays: a sparse
 * array, accessor, nested Proxy, or later mutation must either fail closed or
 * be represented by one dense immutable snapshot.
 */
function ownedCanonicalSnapshot<T>(value: unknown, label: string): T {
  const seen = new Set<object>();
  const rejectProxyOrAccessors = (candidate: unknown): void => {
    if (candidate === null || typeof candidate !== "object") return;
    if (nodeTypes.isProxy(candidate)) throw new TypeError(`${label} contains a proxy`);
    if (seen.has(candidate)) throw new TypeError(`${label} is cyclic`);
    seen.add(candidate);
    try {
      const prototype = Object.getPrototypeOf(candidate);
      if (Array.isArray(candidate)) {
        if (prototype !== Array.prototype || Object.getOwnPropertySymbols(candidate).length > 0) throw new TypeError(`${label} array is invalid`);
        const names = Object.getOwnPropertyNames(candidate);
        if (names.length !== candidate.length + 1 || !names.includes("length")) throw new TypeError(`${label} array is sparse or has extra fields`);
      } else if (prototype !== Object.prototype && prototype !== null) throw new TypeError(`${label} object is invalid`);
      for (const name of Object.getOwnPropertyNames(candidate)) {
        if (Array.isArray(candidate) && name === "length") continue;
        const descriptor = Object.getOwnPropertyDescriptor(candidate, name);
        if (descriptor === undefined || !("value" in descriptor) || descriptor.enumerable !== true) throw new TypeError(`${label} contains an accessor`);
        rejectProxyOrAccessors(descriptor.value);
      }
    } finally { seen.delete(candidate); }
  };
  try { rejectProxyOrAccessors(value); } catch { throw new TypeError(`${label} is not canonical JSON`); }
  let serialized: string;
  try { serialized = canonicalStringify(value); } catch { throw new TypeError(`${label} is not canonical JSON`); }
  let clone: unknown;
  try { clone = JSON.parse(serialized) as unknown; } catch { throw new TypeError(`${label} is not canonical JSON`); }
  return clone as T;
}
function ownedDenseArray<T>(value: unknown, label: string, max = 1024): readonly T[] {
  const clone = ownedCanonicalSnapshot<unknown>(value, label);
  if (!Array.isArray(clone) || clone.length === 0 || clone.length > max) throw new TypeError(`${label} must be a bounded dense array`);
  return Object.freeze(clone.slice() as T[]);
}
function validLifecycleMarker(host: HostId, header: Record<string, unknown> | null, env: Record<string, string | undefined>): AgentMarker {
  return resolveAgentMarker({ host, header, env });
}

/**
 * The sole successful curation entry point. It consumes a genuine
 * SessionManager instance and returns only a result; RootWorkerContext never
 * crosses this boundary. A structural manager, subclass, proxy, or raw header
 * fails closed before store/network work.
 */
export type RootCurationLifecycleInput = Omit<CurationWorkerInput, "rootWorker"> & { membership: readonly string[]; env: Record<string, string | undefined> };
export async function runCurationFromLifecycle(sessionManager: SessionManager, input: RootCurationLifecycleInput): Promise<CurationRunResult> {
  // Validate/snapshot the nominal lifecycle first. Options (including a Proxy
  // or explosive getters) are untouched until the genuine manager passes.
  const managerSnapshot = snapshotGenuineSessionManager(sessionManager);
  if (managerSnapshot === null) return Object.freeze({ state: "child" });
  if (input === null || typeof input !== "object" || Array.isArray(input) || nodeTypes.isProxy(input)) return Object.freeze({ state: "child" });
  // Root/child resolution is an authority gate. Read only the host and
  // environment needed to resolve the marker; explosive store/membership/LLM
  // getters must remain untouched for genuine child/ambiguous lifecycles.
  const host = input.host;
  let env: Record<string, string | undefined>;
  try { env = snapshotEnvironment(input.env ?? {}); } catch { return Object.freeze({ state: "child" }); }
  if (host !== "pi" && host !== "prime") return Object.freeze({ state: "child" });
  const header = managerSnapshot.header;
  let marker: AgentMarker;
  try { marker = validLifecycleMarker(host, header, env); } catch { return Object.freeze({ state: "child" }); }
  if (!marker.rootWorkAllowed || !marker.valid || marker.role !== "root") return Object.freeze({ state: "child" });
  const lifecycleDigest = managerSnapshot.verifyRootState();
  if (lifecycleDigest === null) return Object.freeze({ state: "child" });
  const evidenceHash = sha256Hex(canonicalStringify({ host, marker, lifecycleDigest }));
  // Store capability/owner is the first caller-owned authority check.  Do not
  // touch membership, policies, embedding, or any other snapshot until a real
  // production store has passed its private brand check.
  const store = input.store;
  if (!ProductionCoordinationStore.isValid(store)) return Object.freeze({ state: "child" });
  if (host !== store.ownerHost) return Object.freeze({ state: "child" });
  // Only after the genuine root marker + store are proven may caller-owned
  // arrays/objects be canonically cloned. This also turns sparse arrays into a
  // dense immutable snapshot and rejects nested proxies/accessors.
  const membership = ownedDenseArray<string>(input.membership, "Curation membership");
  const producerPolicies = ownedDenseArray<CurationWorkerInput["producerPolicies"][number]>(input.producerPolicies, "Curation producer policies", 64);
  const workerPolicy = ownedCanonicalSnapshot<CurationWorkerInput["workerPolicy"]>(input.workerPolicy, "Curation worker policy");
  const nodeId = input.nodeId;
  const leaseMs = input.leaseMs;
  const maxClockSkewMs = input.maxClockSkewMs;
  const clock = input.clock;
  const extractorRevision = input.extractorRevision;
  const embedding = input.embedding;
  // Keep mutable fresh-call seams behind one lazy thunk. Core invokes this only
  // after a leased job has been selected; accepted recovery never invokes it.
  const freshOptionsProvider = () => {
    const llm = input.llm!;
    const maxOutputTokens = input.maxOutputTokens;
    const timeoutMs = input.timeoutMs;
    const scan = input.scan;
    return { llm, ...(maxOutputTokens === undefined ? {} : { maxOutputTokens }), ...(timeoutMs === undefined ? {} : { timeoutMs }), ...(scan === undefined ? {} : { scan }) };
  };
  let worker: RootWorkerContext;
  try { worker = new RootWorkerContext(host, evidenceHash, ROOT_WORKER_ISSUER, clock, nodeId, leaseMs, maxClockSkewMs); } catch { return Object.freeze({ state: "child" }); }
  // Explicitly copy the core inputs; never spread the lifecycle manager or
  // untrusted marker/header into a worker authority.  hostContext is ignored:
  // the worker creates a fresh empty context at the egress boundary.
  return runCurationCore(worker, {
    host, store, nodeId, leaseMs, maxClockSkewMs, ...(clock === undefined ? {} : { clock }),
    workerPolicy, extractorRevision, producerPolicies,
    embedding, freshOptionsProvider,
    membership,
  });
}
Object.freeze(runCurationFromLifecycle);
