import { SessionManager } from "@earendil-works/pi-coding-agent";
import { types as nodeTypes } from "node:util";
import { resolveAgentMarker } from "../capture/episode.js";
import { canonicalStringify, sha256Hex } from "../domain/canonical.js";
import { ProductionCoordinationStore } from "../qdrant/write.js";
import { runCurationCore } from "../curation/worker.js";
import { buildRaptorGeneration } from "../raptor/builder.js";
import { intersectPolicies } from "../domain/policy.js";
import { createJob } from "./jobs.js";
import { claimLease } from "./leases.js";
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
    #issuer;
    #host;
    #evidenceHash;
    #clock;
    #nodeId;
    #leaseMs;
    #maxClockSkewMs;
    #lastSample = null;
    constructor(host, evidenceHash, issuer, clock, nodeId, leaseMs, maxClockSkewMs) {
        if (issuer !== ROOT_WORKER_ISSUER)
            throw new TypeError("Root worker capability requires the module issuer");
        if (host !== "pi" && host !== "prime")
            throw new TypeError("Root worker host is invalid");
        if (typeof evidenceHash !== "string" || !/^[0-9a-f]{64}$/u.test(evidenceHash))
            throw new TypeError("Root worker evidence is invalid");
        if (clock !== undefined && typeof clock !== "function")
            throw new TypeError("Root worker clock is invalid");
        if (typeof nodeId !== "string" || nodeId.length === 0 || nodeId.length > 512 || SECRET.test(nodeId))
            throw new TypeError("Root worker node id is invalid");
        if (typeof leaseMs !== "number" || !Number.isSafeInteger(leaseMs) || leaseMs < 1 || leaseMs > 86_400_000)
            throw new TypeError("Root worker lease TTL is invalid");
        if (typeof maxClockSkewMs !== "number" || !Number.isSafeInteger(maxClockSkewMs) || maxClockSkewMs < 0 || maxClockSkewMs > 3_600_000)
            throw new TypeError("Root worker clock skew is invalid");
        const trustedLeaseMs = leaseMs;
        const trustedMaxClockSkewMs = maxClockSkewMs;
        this.#issuer = issuer;
        this.#host = host;
        this.#evidenceHash = evidenceHash;
        this.#clock = clock ?? (() => Date.now());
        this.#nodeId = nodeId;
        this.#leaseMs = trustedLeaseMs;
        this.#maxClockSkewMs = trustedMaxClockSkewMs;
        Object.freeze(this);
    }
    static isValid(value) {
        if (typeof value !== "object" || value === null || !(#issuer in value))
            return false;
        return value instanceof RootWorkerContext && value.#issuer === ROOT_WORKER_ISSUER;
    }
    now() {
        const value = this.#clock();
        if (!Number.isSafeInteger(value) || value < 0)
            throw new TypeError("Root worker clock is invalid");
        if (this.#lastSample !== null && value < this.#lastSample)
            throw new TypeError("Root worker clock went backwards");
        this.#lastSample = value;
        return value;
    }
    get host() { return this.#host; }
    get evidenceHash() { return this.#evidenceHash; }
    get nodeId() { return this.#nodeId; }
    get leaseMs() { return this.#leaseMs; }
    get maxClockSkewMs() { return this.#maxClockSkewMs; }
}
Object.freeze(RootWorkerContext);
Object.freeze(RootWorkerContext.prototype);
/** Validate nominal lifecycle and read only its marker header. Internal session
 * entries/session id are deliberately deferred until root work is allowed. */
function snapshotGenuineSessionManager(value) {
    if (nodeTypes.isProxy(value) || !(value instanceof SessionManager))
        return null;
    if (Object.getPrototypeOf(value) !== SESSION_MANAGER_PROTOTYPE)
        return null;
    if (Object.prototype.hasOwnProperty.call(value, "getHeader") || Object.prototype.hasOwnProperty.call(value, "getBranch") || Object.prototype.hasOwnProperty.call(value, "getEntries") || Object.prototype.hasOwnProperty.call(value, "getSessionId"))
        return null;
    const prototype = SESSION_MANAGER_PROTOTYPE;
    const getHeader = SESSION_MANAGER_METHODS.getHeader;
    const getBranch = SESSION_MANAGER_METHODS.getBranch;
    const getEntries = SESSION_MANAGER_METHODS.getEntries;
    const getSessionId = SESSION_MANAGER_METHODS.getSessionId;
    if (typeof getHeader !== "function" || typeof getBranch !== "function" || typeof getEntries !== "function" || typeof getSessionId !== "function")
        return null;
    if (Object.getOwnPropertyDescriptor(prototype, "getHeader")?.value !== getHeader || Object.getOwnPropertyDescriptor(prototype, "getBranch")?.value !== getBranch || Object.getOwnPropertyDescriptor(prototype, "getEntries")?.value !== getEntries || Object.getOwnPropertyDescriptor(prototype, "getSessionId")?.value !== getSessionId)
        return null;
    try {
        const rawHeader = getHeader.call(value);
        if (!(rawHeader === null || typeof rawHeader === "object"))
            return null;
        const header = snapshotHeader(rawHeader);
        return Object.freeze({ header, verifyRootState: () => {
                try {
                    const entries = getEntries.call(value);
                    const sessionId = getSessionId.call(value);
                    if (!Array.isArray(entries) || typeof sessionId !== "string" || sessionId.length === 0)
                        return null;
                    return sha256Hex(canonicalStringify({ sessionId, entryCount: entries.length }));
                }
                catch {
                    return null;
                }
            } });
    }
    catch {
        return null;
    }
}
function snapshotEnvironment(input) {
    if (nodeTypes.isProxy(input) || input === null || typeof input !== "object" || Array.isArray(input))
        throw new TypeError("Root lifecycle environment is invalid");
    const result = {};
    // Marker resolution is intentionally capability-minimal: never enumerate or
    // touch unrelated environment properties (which may be secret-bearing or
    // accessor-backed). Read only the three documented marker keys, and only
    // through own data descriptors so hostile getters fail closed without being
    // invoked.
    for (const key of ["RLM_DEPTH", "PI_SUBAGENT_CHILD", "PI_SUBAGENT_DEPTH"]) {
        const descriptor = Object.getOwnPropertyDescriptor(input, key);
        if (descriptor === undefined)
            continue;
        if (!("value" in descriptor))
            throw new TypeError("Root lifecycle environment is invalid");
        const value = descriptor.value;
        if (value !== undefined && typeof value !== "string")
            throw new TypeError("Root lifecycle environment is invalid");
        result[key] = value;
    }
    return Object.freeze(result);
}
function snapshotHeader(value) {
    if (value === undefined || value === null)
        return null;
    if (nodeTypes.isProxy(value) || typeof value !== "object" || Array.isArray(value))
        throw new TypeError("Root lifecycle header is invalid");
    // Project only the host marker fields. Never enumerate/JSON-clone the raw
    // header: arbitrary secret-bearing fields and accessors are outside the
    // authority contract. Required marker accessors fail closed; unknown keys
    // (including symbols) are ignored without being read.
    const projected = {};
    const depth = Object.getOwnPropertyDescriptor(value, "rlmDepth");
    if (depth !== undefined) {
        if (!("value" in depth))
            throw new TypeError("Root lifecycle header is invalid");
        const raw = depth.value;
        const validNumber = typeof raw === "number" && Number.isSafeInteger(raw) && raw >= 0 && raw <= 1000;
        const validString = typeof raw === "string" && /^\d{1,4}$/u.test(raw) && Number(raw) <= 1000;
        projected.rlmDepth = validNumber ? raw : validString ? Number(raw) : "invalid";
    }
    const parent = Object.getOwnPropertyDescriptor(value, "parentSession");
    if (parent !== undefined) {
        if (!("value" in parent))
            throw new TypeError("Root lifecycle header is invalid");
        const raw = parent.value;
        if (raw === undefined || raw === null)
            projected.parentSession = null;
        else if (nodeTypes.isProxy(raw))
            throw new TypeError("Root lifecycle header is invalid");
        else if (typeof raw === "string" || (typeof raw === "object" && raw !== null))
            projected.parentSession = "present";
        else
            projected.parentSession = 0; // invalid marker; forces child/fail-closed
    }
    return Object.freeze(projected);
}
/**
 * Own a canonical JSON value before any lifecycle value crosses an await.  In
 * particular, never use `[...value]` for membership/policy arrays: a sparse
 * array, accessor, nested Proxy, or later mutation must either fail closed or
 * be represented by one dense immutable snapshot.
 */
function ownedCanonicalSnapshot(value, label) {
    const seen = new Set();
    const rejectProxyOrAccessors = (candidate) => {
        if (candidate === null || typeof candidate !== "object")
            return;
        if (nodeTypes.isProxy(candidate))
            throw new TypeError(`${label} contains a proxy`);
        if (seen.has(candidate))
            throw new TypeError(`${label} is cyclic`);
        seen.add(candidate);
        try {
            const prototype = Object.getPrototypeOf(candidate);
            if (Array.isArray(candidate)) {
                if (prototype !== Array.prototype || Object.getOwnPropertySymbols(candidate).length > 0)
                    throw new TypeError(`${label} array is invalid`);
                const names = Object.getOwnPropertyNames(candidate);
                if (names.length !== candidate.length + 1 || !names.includes("length"))
                    throw new TypeError(`${label} array is sparse or has extra fields`);
            }
            else if (prototype !== Object.prototype && prototype !== null)
                throw new TypeError(`${label} object is invalid`);
            for (const name of Object.getOwnPropertyNames(candidate)) {
                if (Array.isArray(candidate) && name === "length")
                    continue;
                const descriptor = Object.getOwnPropertyDescriptor(candidate, name);
                if (descriptor === undefined || !("value" in descriptor) || descriptor.enumerable !== true)
                    throw new TypeError(`${label} contains an accessor`);
                rejectProxyOrAccessors(descriptor.value);
            }
        }
        finally {
            seen.delete(candidate);
        }
    };
    try {
        rejectProxyOrAccessors(value);
    }
    catch {
        throw new TypeError(`${label} is not canonical JSON`);
    }
    let serialized;
    try {
        serialized = canonicalStringify(value);
    }
    catch {
        throw new TypeError(`${label} is not canonical JSON`);
    }
    let clone;
    try {
        clone = JSON.parse(serialized);
    }
    catch {
        throw new TypeError(`${label} is not canonical JSON`);
    }
    return clone;
}
function ownedDenseArray(value, label, max = 1024) {
    const clone = ownedCanonicalSnapshot(value, label);
    if (!Array.isArray(clone) || clone.length === 0 || clone.length > max)
        throw new TypeError(`${label} must be a bounded dense array`);
    return Object.freeze(clone.slice());
}
function validLifecycleMarker(host, header, env) {
    return resolveAgentMarker({ host, header, env });
}
export async function runCurationFromLifecycle(sessionManager, input) {
    // Validate/snapshot the nominal lifecycle first. Options (including a Proxy
    // or explosive getters) are untouched until the genuine manager passes.
    const managerSnapshot = snapshotGenuineSessionManager(sessionManager);
    if (managerSnapshot === null)
        return Object.freeze({ state: "child" });
    if (input === null || typeof input !== "object" || Array.isArray(input) || nodeTypes.isProxy(input))
        return Object.freeze({ state: "child" });
    // Root/child resolution is an authority gate. Read only the host and
    // environment needed to resolve the marker; explosive store/membership/LLM
    // getters must remain untouched for genuine child/ambiguous lifecycles.
    const host = input.host;
    let env;
    try {
        env = snapshotEnvironment(input.env ?? {});
    }
    catch {
        return Object.freeze({ state: "child" });
    }
    if (host !== "pi" && host !== "prime")
        return Object.freeze({ state: "child" });
    const header = managerSnapshot.header;
    let marker;
    try {
        marker = validLifecycleMarker(host, header, env);
    }
    catch {
        return Object.freeze({ state: "child" });
    }
    if (!marker.rootWorkAllowed || !marker.valid || marker.role !== "root")
        return Object.freeze({ state: "child" });
    const lifecycleDigest = managerSnapshot.verifyRootState();
    if (lifecycleDigest === null)
        return Object.freeze({ state: "child" });
    const evidenceHash = sha256Hex(canonicalStringify({ host, marker, lifecycleDigest }));
    // Store capability/owner is the first caller-owned authority check.  Do not
    // touch membership, policies, embedding, or any other snapshot until a real
    // production store has passed its private brand check.
    const store = input.store;
    if (!ProductionCoordinationStore.isValid(store))
        return Object.freeze({ state: "child" });
    if (host !== store.ownerHost)
        return Object.freeze({ state: "child" });
    // Only after the genuine root marker + store are proven may caller-owned
    // arrays/objects be canonically cloned. This also turns sparse arrays into a
    // dense immutable snapshot and rejects nested proxies/accessors.
    const membership = ownedDenseArray(input.membership, "Curation membership");
    const producerPolicies = ownedDenseArray(input.producerPolicies, "Curation producer policies", 64);
    const workerPolicy = ownedCanonicalSnapshot(input.workerPolicy, "Curation worker policy");
    const nodeId = input.nodeId;
    const leaseMs = input.leaseMs;
    const maxClockSkewMs = input.maxClockSkewMs;
    const clock = input.clock;
    const signal = input.signal;
    const extractorRevision = input.extractorRevision;
    const embedding = input.embedding;
    // Keep mutable fresh-call seams behind one lazy thunk. Core invokes this only
    // after a leased job has been selected; accepted recovery never invokes it.
    const freshOptionsProvider = () => {
        const llm = input.llm;
        const maxOutputTokens = input.maxOutputTokens;
        const timeoutMs = input.timeoutMs;
        const scan = input.scan;
        return { llm, ...(maxOutputTokens === undefined ? {} : { maxOutputTokens }), ...(timeoutMs === undefined ? {} : { timeoutMs }), ...(scan === undefined ? {} : { scan }) };
    };
    let worker;
    try {
        worker = new RootWorkerContext(host, evidenceHash, ROOT_WORKER_ISSUER, clock, nodeId, leaseMs, maxClockSkewMs);
    }
    catch {
        return Object.freeze({ state: "child" });
    }
    // Explicitly copy the core inputs; never spread the lifecycle manager or
    // untrusted marker/header into a worker authority.  hostContext is ignored:
    // the worker creates a fresh empty context at the egress boundary.
    return runCurationCore(worker, {
        host, store, nodeId, leaseMs, maxClockSkewMs, ...(clock === undefined ? {} : { clock }), ...(signal === undefined ? {} : { signal }),
        workerPolicy, extractorRevision, producerPolicies,
        embedding, freshOptionsProvider,
        membership,
    });
}
Object.freeze(runCurationFromLifecycle);
/** The sole RAPTOR root entry point; the private root/lease capability never escapes. */
export async function runRaptorFromLifecycle(sessionManager, input) {
    const managerSnapshot = snapshotGenuineSessionManager(sessionManager);
    if (managerSnapshot === null)
        return Object.freeze({ state: "child" });
    if (input === null || typeof input !== "object" || Array.isArray(input) || nodeTypes.isProxy(input))
        return Object.freeze({ state: "child" });
    const host = input.host;
    let env;
    try {
        env = snapshotEnvironment(input.env ?? {});
    }
    catch {
        return Object.freeze({ state: "child" });
    }
    if (host !== "pi" && host !== "prime")
        return Object.freeze({ state: "child" });
    let marker;
    try {
        marker = validLifecycleMarker(host, managerSnapshot.header, env);
    }
    catch {
        return Object.freeze({ state: "child" });
    }
    if (!marker.rootWorkAllowed || !marker.valid || marker.role !== "root")
        return Object.freeze({ state: "child" });
    const lifecycleDigest = managerSnapshot.verifyRootState();
    if (lifecycleDigest === null)
        return Object.freeze({ state: "child" });
    const store = input.store;
    if (!ProductionCoordinationStore.isValid(store) || store.ownerHost !== host)
        return Object.freeze({ state: "child" });
    let leaves;
    let workerPolicy;
    try {
        leaves = ownedDenseArray(input.leaves, "RAPTOR lifecycle leaves", 65_536);
        workerPolicy = ownedCanonicalSnapshot(input.workerPolicy, "RAPTOR lifecycle worker policy");
    }
    catch {
        return Object.freeze({ state: "child" });
    }
    const producerPolicies = new Map();
    for (const leaf of leaves) {
        const prior = producerPolicies.get(leaf.policy.id);
        if (prior !== undefined && canonicalStringify(prior) !== canonicalStringify(leaf.policy))
            return Object.freeze({ state: "pending", reason: "incompatible_policy" });
        producerPolicies.set(leaf.policy.id, leaf.policy);
    }
    const policy = intersectPolicies([...producerPolicies.values()].sort((left, right) => left.id.localeCompare(right.id)), workerPolicy);
    if (policy === null || policy.destinationIds.llm === undefined)
        return Object.freeze({ state: "pending", reason: "incompatible_policy" });
    const membership = Object.freeze(leaves.map((leaf) => leaf.id).sort());
    if (membership.length === 0 || new Set(membership).size !== membership.length)
        return Object.freeze({ state: "pending", reason: "invalid_input" });
    const evidenceHash = sha256Hex(canonicalStringify({ host, marker, lifecycleDigest }));
    let worker;
    try {
        worker = new RootWorkerContext(host, evidenceHash, ROOT_WORKER_ISSUER, input.clock, input.nodeId, input.leaseMs, input.maxClockSkewMs);
    }
    catch {
        return Object.freeze({ state: "child" });
    }
    const control = await store.readControl();
    if (control.state !== "active")
        return Object.freeze({ state: "pending", reason: "authority_changed" });
    const storedEpisodes = [];
    try {
        for (let index = 0; index < membership.length; index += 1024)
            storedEpisodes.push(...await store.readEpisodes(membership.slice(index, index + 1024), control.privacyEpoch));
    }
    catch {
        return Object.freeze({ state: "pending", reason: "authority_changed" });
    }
    const storedById = new Map(storedEpisodes.map((episode) => [episode.id, episode]));
    const controlAfterSources = await store.readControl();
    if (storedById.size !== membership.length || membership.some((id) => storedById.get(id)?.privacyEpoch !== control.privacyEpoch) || controlAfterSources.contentHash !== control.contentHash)
        return Object.freeze({ state: "pending", reason: "authority_changed" });
    const createdAt = leaves.map((leaf) => leaf.eventAt).sort()[0];
    const job = input.jobId === undefined
        ? await createJob(store, { ownerHost: host, membership, policyIntersectionId: policy.id, policyHash: control.coordinationPolicyHash, policyEpoch: control.coordinationPolicyEpoch, extractorRevision: input.extractorRevision, privacyEpoch: control.privacyEpoch, createdAt, expiresAt: policy.expiresAt })
        : await (async () => {
            const existing = await store.readJob(input.jobId);
            if (existing === null || existing.ownerHost !== host || canonicalStringify(existing.membership) !== canonicalStringify(membership) || existing.policyId !== policy.id || existing.policyHash !== control.coordinationPolicyHash || existing.policyEpoch !== control.coordinationPolicyEpoch || existing.extractorRevision !== input.extractorRevision || existing.privacyEpoch !== control.privacyEpoch || existing.expiresAt !== policy.expiresAt || existing.createdAt !== createdAt)
                throw new TypeError("RAPTOR durable job identity is invalid");
            return existing;
        })();
    const authority = await claimLease(store, worker, { jobId: job.id, policyEpoch: control.coordinationPolicyEpoch, policyHash: control.coordinationPolicyHash, privacyEpoch: control.privacyEpoch });
    if (authority === null)
        return Object.freeze({ state: "pending", reason: "authority_changed" });
    const buildInput = { host, workerPolicy, leaves, llm: input.llm, embedding: input.embedding, modelId: input.modelId, homeDir: input.homeDir, seed: input.seed, maxLevels: input.maxLevels, summaryInputTokens: input.summaryInputTokens, umapDimensions: input.umapDimensions, localNeighbors: input.localNeighbors, gmmMaxClusters: input.gmmMaxClusters, membershipThreshold: input.membershipThreshold, ...(input.global === undefined ? {} : { global: input.global }), ...(input.scan === undefined ? {} : { scan: input.scan }), ...(input.signal === undefined ? {} : { signal: input.signal }), ...(input.reuseCandidates === undefined ? {} : { reuseCandidates: input.reuseCandidates }) };
    // The builder renews its lease while clustering/model/embedding work runs
    // and performs the fenced terminal transition with the latest authority.
    return buildRaptorGeneration(store, authority, buildInput);
}
Object.freeze(runRaptorFromLifecycle);
//# sourceMappingURL=root.js.map