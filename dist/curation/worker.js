import { SessionManager } from "@earendil-works/pi-coding-agent";
import { types as nodeTypes } from "node:util";
import { BoundEmbeddingDestination } from "../clients/embeddings.js";
import { RootWorkerContext } from "../coordination/root.js";
import { claimLease, readLease, releaseLease } from "../coordination/leases.js";
import { createJob, readActiveAcceptance, writeProposal, acceptProposal, readJob, completeJob, jobIdFor } from "../coordination/jobs.js";
import { readControl } from "../coordination/control.js";
import { jobExpired } from "../coordination/deadline.js";
import { LeaseAuthority, ProductionCoordinationStore } from "../qdrant/write.js";
import { coverageId } from "../domain/ids.js";
import { canonicalStringify } from "../domain/canonical.js";
import { canonicalRecordHash } from "../domain/records.js";
import { intersectPolicies, isPolicyExpired, processingPolicyHash, PROVIDER_AGNOSTIC_ORIGIN } from "../domain/policy.js";
import { resolveAgentMarker } from "../capture/episode.js";
import { completeMemory } from "./llm.js";
import { buildCurationPrompt, CURATION_PROMPT_REVISION } from "./prompt.js";
import { assertPersistableCurationResult, parseStrictCurationJson, validateCurationResult } from "./validate.js";
import { materializeCuration, deriveEffectiveOrder } from "./temporal.js";
import { projectCurationItem } from "./projection.js";
import { parseCurationProposalEnvelope, provenanceMatches } from "./provenance.js";
import { contentId, observationId, stateKey } from "../domain/ids.js";
export const CURATION_TURN_TRIGGER = 10;
export const CURATION_TOOL_TRIGGER = 15;
const MAX_MEMBERSHIP = 1024;
const DEFAULT_MAX_OUTPUT_TOKENS = 8_192;
const DEFAULT_TIMEOUT_MS = 120_000;
/** Canonical owned snapshot for caller options. Proxies/accessors/sparse
 * arrays fail closed without invoking a getter, and JSON arrays are dense. */
function ownedCanonicalSnapshot(value, label) {
    const seen = new Set();
    const inspect = (candidate) => {
        if (candidate === null || typeof candidate !== "object")
            return;
        if (nodeTypes.isProxy(candidate))
            throw new TypeError(`${label} contains a proxy`);
        if (seen.has(candidate))
            throw new TypeError(`${label} is cyclic`);
        seen.add(candidate);
        try {
            const prototype = Object.getPrototypeOf(candidate);
            if (Object.getOwnPropertySymbols(candidate).length > 0)
                throw new TypeError(`${label} contains symbols`);
            if (Array.isArray(candidate)) {
                if (prototype !== Array.prototype)
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
                inspect(descriptor.value);
            }
        }
        finally {
            seen.delete(candidate);
        }
    };
    try {
        inspect(value);
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
    try {
        return JSON.parse(serialized);
    }
    catch {
        throw new TypeError(`${label} is not canonical JSON`);
    }
}
function ownedDenseArray(value, label, max = MAX_MEMBERSHIP) {
    const clone = ownedCanonicalSnapshot(value, label);
    if (!Array.isArray(clone) || clone.length === 0 || clone.length > max)
        throw new TypeError(`${label} must be a bounded dense array`);
    return Object.freeze(clone.slice());
}
function ownedModelSnapshot(value) {
    if (value === null || typeof value !== "object" || Array.isArray(value) || nodeTypes.isProxy(value) || Object.getOwnPropertySymbols(value).length > 0)
        throw new TypeError("LLM model is invalid");
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null)
        throw new TypeError("LLM model is invalid");
    const normalized = {};
    for (const name of Object.getOwnPropertyNames(value)) {
        const descriptor = Object.getOwnPropertyDescriptor(value, name);
        if (descriptor === undefined || !("value" in descriptor) || descriptor.enumerable !== true)
            throw new TypeError("LLM model is invalid");
        if (descriptor.value !== undefined)
            normalized[name] = descriptor.value;
    }
    const model = ownedCanonicalSnapshot(normalized, "LLM model");
    return { canonical: canonicalStringify(model), model };
}
/** Read only a named own data descriptor; unknown options are never touched. */
function ownOption(options, key, required = true) {
    const descriptor = Object.getOwnPropertyDescriptor(options, key);
    if (descriptor === undefined) {
        if (required)
            throw new TypeError(`Curation option ${key} is missing`);
        return undefined;
    }
    if (!("value" in descriptor) || descriptor.enumerable !== true)
        throw new TypeError(`Curation option ${key} is not a data field`);
    return descriptor.value;
}
function isLifecycleManager(value) {
    if (nodeTypes.isProxy(value) || !(value instanceof SessionManager) || Object.getPrototypeOf(value) !== SessionManager.prototype || Object.prototype.hasOwnProperty.call(value, "getHeader") || Object.prototype.hasOwnProperty.call(value, "getBranch"))
        return false;
    return Object.getOwnPropertyDescriptor(SessionManager.prototype, "getHeader")?.value === SessionManager.prototype.getHeader && Object.getOwnPropertyDescriptor(SessionManager.prototype, "getBranch")?.value === SessionManager.prototype.getBranch;
}
/**
 * Trigger discovery is optimization ONLY: enqueue at root turn 10, tool
 * trigger 15, before compaction; shutdown only persists pending work and never
 * starts LLM curation. Root/child gating uses the SAME validated host-marker
 * resolution as capture: Prime resolves child from header.rlmDepth then
 * RLM_DEPTH; Pi resolves header.parentSession as the sole host child signal
 * (PI_SUBAGENT_CHILD/DEPTH are optional extension-wrapper markers validated
 * when present, never assumed). Invalid/contradictory markers disable root
 * curation; children may ingest/search but cannot claim.
 */
export function curationTrigger(input) {
    const host = input.host;
    const rootTurns = input.rootTurns;
    const toolCalls = input.toolCalls;
    if (!Number.isSafeInteger(rootTurns) || rootTurns < 0 || !Number.isSafeInteger(toolCalls) || toolCalls < 0)
        throw new TypeError("Curation trigger counters are invalid");
    if (!isLifecycleManager(input.sessionManager))
        return "child";
    let header;
    try {
        header = SessionManager.prototype.getHeader.call(input.sessionManager);
    }
    catch {
        return "child";
    }
    const marker = resolveAgentMarker({ host, header: header === null || (typeof header === "object" && !Array.isArray(header)) ? header : undefined, env: input.env ?? {} });
    if (!marker.rootWorkAllowed)
        return "child";
    if (input.shutdown === true)
        return "persist_only";
    if (input.beforeCompaction === true || rootTurns >= CURATION_TURN_TRIGGER || toolCalls >= CURATION_TOOL_TRIGGER)
        return "run";
    return "disabled";
}
/**
 * Copy only named destination/binding fields from an already isolated object.
 * Unknown keys are deliberately never enumerated (an API_KEY accessor on an
 * untrusted config object must remain untouched), and accessors/proxies fail
 * closed without invoking their traps/getters.
 */
function snapshotNamedDataFields(value, fields, label) {
    if (value === null || typeof value !== "object" || Array.isArray(value) || nodeTypes.isProxy(value))
        throw new TypeError(`${label} is invalid`);
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null)
        throw new TypeError(`${label} is invalid`);
    const snapshot = {};
    for (const field of fields) {
        const descriptor = Object.getOwnPropertyDescriptor(value, field);
        if (descriptor === undefined || !Object.prototype.hasOwnProperty.call(descriptor, "value") || descriptor.enumerable !== true)
            throw new TypeError(`${label} is invalid`);
        snapshot[field] = descriptor.value;
    }
    return Object.freeze(snapshot);
}
function snapshotDestination(value, label) {
    const snapshot = snapshotNamedDataFields(value, ["id", "residency", "dataUse"], label);
    if (typeof snapshot.id !== "string" || typeof snapshot.residency !== "string" || typeof snapshot.dataUse !== "string")
        throw new TypeError(`${label} is invalid`);
    return Object.freeze({ id: snapshot.id, residency: snapshot.residency, dataUse: snapshot.dataUse });
}
function snapshotModelBinding(value) {
    const snapshot = snapshotNamedDataFields(value, ["providerId", "modelId", "destinationId"], "LLM destination binding");
    if (typeof snapshot.providerId !== "string" || typeof snapshot.modelId !== "string" || typeof snapshot.destinationId !== "string")
        throw new TypeError("LLM destination binding is invalid");
    return Object.freeze({ providerId: snapshot.providerId, modelId: snapshot.modelId, destinationId: snapshot.destinationId });
}
function requireBoundedId(name, value) {
    if (typeof value !== "string" || value.length === 0 || value.length > 512 || /(?:api[-_]?key|access[-_]?token|authorization|bearer|credential|password|secret|token)/iu.test(value))
        throw new TypeError(`${name} must be a bounded redacted id`);
    return value;
}
function isoNow(factory) {
    const value = factory();
    if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/u.test(value) || !Number.isFinite(Date.parse(value)))
        throw new TypeError("Curation clock is invalid");
    return value;
}
/**
 * Deterministic coverage truth recovery: explicit membership MINUS episodes
 * already covered by this extractor revision under the exact policy identity.
 * Coverage IDs are policy-specific, so a policy migration re-curates.
 */
export async function filterUncoveredEpisodes(input) {
    const store = input.store;
    if (!ProductionCoordinationStore.isValid(store))
        throw new TypeError("Coverage filtering requires the branded production store");
    const membership = Object.freeze([...input.membership]);
    const extractorRevision = input.extractorRevision;
    const policyHash = input.policyHash;
    const policyEpoch = input.policyEpoch;
    const privacyEpoch = input.privacyEpoch;
    const policyIntersectionId = input.policyIntersectionId;
    if (!Array.isArray(membership) || membership.length === 0 || membership.length > MAX_MEMBERSHIP || new Set(membership).size !== membership.length)
        throw new TypeError("Membership must be explicit, unique and bounded");
    membership.forEach((id, index) => { requireBoundedId(`membership[${index}]`, id); if (index > 0 && membership[index - 1] >= id)
        throw new TypeError("Membership must be sorted"); });
    if (typeof extractorRevision !== "string" || extractorRevision.length === 0 || extractorRevision.length > 512 || !Number.isSafeInteger(policyEpoch) || policyEpoch < 0 || !Number.isSafeInteger(privacyEpoch) || privacyEpoch < 0 || typeof policyHash !== "string" || policyHash.length === 0 || policyHash.length > 512)
        throw new TypeError("Coverage filter identity is invalid");
    const covered = await store.readCoverage(membership.map((id) => coverageId({ ownerHost: store.ownerHost, episodeId: id, extractorRevision, coordinationPolicyHash: policyHash, coordinationPolicyEpoch: policyEpoch, policyIntersectionId, privacyEpoch })));
    const coveredSet = new Set(covered.map((entry) => entry.episodeId));
    return Object.freeze(membership.filter((id) => !coveredSet.has(id)));
}
function snapshotProcessingPolicy(input) {
    const id = input.id;
    const ownerHost = input.ownerHost;
    const destinationIds = input.destinationIds;
    const qdrant = destinationIds.qdrant;
    const embedding = destinationIds.embedding;
    const llm = destinationIds.llm;
    const originProvider = input.originProvider;
    const allowCrossProviderReplay = input.allowCrossProviderReplay;
    const expiresAt = input.expiresAt;
    const residency = input.residency;
    const dataUse = input.dataUse;
    const policyRevision = input.policyRevision;
    return Object.freeze({ id, ownerHost, destinationIds: Object.freeze({ qdrant, embedding, ...(llm === undefined ? {} : { llm }) }), originProvider, allowCrossProviderReplay, expiresAt, residency, dataUse, policyRevision });
}
function validateWorkerPolicies(workerPolicy, producerPolicies, host, now, skewMs) {
    if (processingPolicyHash(workerPolicy) !== workerPolicy.id || isPolicyExpired(workerPolicy, now, skewMs) || workerPolicy.ownerHost !== host)
        throw new TypeError("Worker policy is invalid");
    if (!Array.isArray(producerPolicies) || producerPolicies.length > 64 || new Set(producerPolicies.map((producer) => producer.id)).size !== producerPolicies.length)
        throw new TypeError("Producer policies are unbounded or ambiguous");
    for (const producer of producerPolicies) {
        if (processingPolicyHash(producer) !== producer.id || isPolicyExpired(producer, now, skewMs) || producer.ownerHost !== host)
            throw new TypeError("Producer policy is invalid");
    }
}
/**
 * Complete LLM egress barrier.  Each half intentionally reads the same
 * authority lanes in the same order: control -> job -> claim -> tombstones ->
 * fresh clock.  The second half is not a control-only check: a job/proposal
 * identity, claim fence, tombstone, or trusted clock mutation in the slow lane
 * invalidates the call before bytes can leave the process.
 */
async function assertLlmBarrier(store, authority, membership, destinationId, phase) {
    const error = (lane) => { throw new TypeError(`LLM ${phase} ${lane} barrier failed`); };
    const readHalf = async () => {
        // Keep this sequence explicit.  In particular, do not move tombstones
        // before job/claim or replace the job read with a captured earlier value.
        const control = await store.readControl();
        const job = await readJob(store, authority.jobId);
        const claim = await store.readLease(authority.jobId);
        const tombstones = await store.readTombstones(membership);
        let now;
        try {
            now = authority.now();
        }
        catch {
            return error("clock");
        }
        return { control, job, claim, tombstones, now };
    };
    const valid = (half) => {
        const { control, job, claim, tombstones, now } = half;
        if (control.state !== "active" || control.coordinationPolicyEpoch !== authority.coordinationPolicyEpoch || control.coordinationPolicyHash !== authority.coordinationPolicyHash || control.privacyEpoch !== authority.privacyEpoch || control.revokedDestinationIds.includes(destinationId))
            return false;
        if (job === null || job.id !== authority.jobId || job.ownerHost !== authority.ownerHost || job.policyId !== authority.processingPolicyId || job.processingPolicyId !== authority.processingPolicyId || job.policyHash !== authority.coordinationPolicyHash || job.policyEpoch !== authority.coordinationPolicyEpoch || job.coordinationPolicyHash !== authority.coordinationPolicyHash || job.coordinationPolicyEpoch !== authority.coordinationPolicyEpoch || job.privacyEpoch !== authority.privacyEpoch || job.membership.length !== membership.length || job.membership.some((id, index) => id !== membership[index]) || jobExpired(job, now, authority.maxClockSkewMs))
            return false;
        if (job.expiresAt !== null && claim !== null && Date.parse(claim.expiresAt) > Date.parse(job.expiresAt))
            return false;
        if (claim === null || !authority.matchesClaim(claim) || claim.state !== "leased" || Date.parse(claim.expiresAt) <= now)
            return false;
        if (tombstones.length > 0)
            return false;
        return true;
    };
    const first = await readHalf();
    if (!valid(first))
        return error("first");
    const second = await readHalf();
    if (!valid(second))
        return error("second");
    // No lane may change between the two snapshots.  Hashes alone are not
    // enough for the mutable job/control envelopes: compare every canonical
    // field after the branded store has parsed them.
    try {
        if (canonicalStringify(first.control) !== canonicalStringify(second.control) || canonicalStringify(first.job) !== canonicalStringify(second.job) || canonicalStringify(first.claim) !== canonicalStringify(second.claim) || canonicalStringify(first.tombstones) !== canonicalStringify(second.tombstones))
            return error("identity");
        return canonicalStringify({ control: second.control, job: second.job, claim: second.claim, tombstones: second.tombstones });
    }
    catch {
        return error("identity");
    }
}
async function assertPreLlmBarrier(store, authority, membership, llmDestinationId) {
    return assertLlmBarrier(store, authority, membership, llmDestinationId, "pre-egress");
}
async function assertPostLlmBarrier(store, authority, membership, llmDestinationId) {
    return assertLlmBarrier(store, authority, membership, llmDestinationId, "post-egress");
}
export async function runCurationCore(worker, input) {
    // Nominal capability is a separate argument: forged/proxy options cannot
    // fire a getter before the genuine root context is proven.
    if (!RootWorkerContext.isValid(worker))
        return Object.freeze({ state: "child" });
    if (input === null || typeof input !== "object" || Array.isArray(input) || nodeTypes.isProxy(input) || (Object.getPrototypeOf(input) !== Object.prototype && Object.getPrototypeOf(input) !== null))
        throw new TypeError("Curation options are invalid");
    // GLOBAL RULE: prove the two nominal capabilities BEFORE touching any
    // membership/policy arrays or caller-owned snapshots. ownOption inspects only
    // the named data descriptor, so a getter/symbol/unknown field is untouched.
    const store = ownOption(input, "store");
    if (!ProductionCoordinationStore.isValid(store))
        throw new TypeError("Curation requires the branded production store");
    const embedding = ownOption(input, "embedding");
    if (!BoundEmbeddingDestination.isValid(embedding))
        throw new TypeError("Curation requires the opaque bound embedding destination");
    // Snapshot known scalar/config fields once, then canonicalize structured
    // arrays/objects into dense immutable values before any await.
    const host = ownOption(input, "host");
    const nodeId = ownOption(input, "nodeId");
    const leaseMs = ownOption(input, "leaseMs");
    const maxClockSkewMs = ownOption(input, "maxClockSkewMs");
    const clock = ownOption(input, "clock", false);
    const signal = ownOption(input, "signal", false);
    if (signal !== undefined && !(signal instanceof AbortSignal))
        throw new TypeError("Curation signal is invalid");
    const workerPolicy = snapshotProcessingPolicy(ownedCanonicalSnapshot(ownOption(input, "workerPolicy"), "Curation worker policy"));
    const extractorRevision = ownOption(input, "extractorRevision");
    const producerPolicies = Object.freeze(ownedDenseArray(ownOption(input, "producerPolicies"), "Curation producer policies", 64).map((policy) => snapshotProcessingPolicy(policy)));
    // LLM/model configuration is intentionally lazy: accepted/released crash
    // recovery must materialize the durable proposal without touching caller LLM
    // getters, registry, or model object. These are populated only on the fresh
    // leased path immediately before pre-egress validation.
    let modelSnapshot;
    let modelCanonical = "";
    let modelStillBound = () => true;
    let modelRegistry;
    let llmDestination;
    let llmDestinationBinding;
    const deepFreeze = (value) => {
        if (value !== null && typeof value === "object" && !Object.isFrozen(value)) {
            for (const child of Object.values(value))
                deepFreeze(child);
            Object.freeze(value);
        }
        return value;
    };
    const membership = ownedDenseArray(ownOption(input, "membership"), "Curation membership");
    // Mutable fresh-call seams are intentionally read only after a leased job is
    // selected. Accepted/released recovery must not touch these properties.
    let maxOutputTokens;
    let timeoutMs;
    let scan;
    if (host !== store.ownerHost)
        throw new TypeError("Curation host does not match the store owner");
    const embeddingDestination = snapshotDestination(embedding.destination, "Embedding destination");
    const embeddingCoordination = Object.freeze({ policyHash: embedding.coordination.policyHash, policyEpoch: embedding.coordination.policyEpoch });
    if (!Array.isArray(membership) || membership.length === 0 || membership.length > MAX_MEMBERSHIP || new Set(membership).size !== membership.length)
        throw new TypeError("Membership must be explicit, unique and bounded");
    membership.forEach((id, index) => { requireBoundedId(`membership[${index}]`, id); if (index > 0 && membership[index - 1] >= id)
        throw new TypeError("Membership must be sorted"); });
    if (typeof extractorRevision !== "string" || extractorRevision.length === 0 || extractorRevision.length > 512)
        throw new TypeError("Extractor revision is invalid");
    // Root issuance occurs only inside runCurationFromLifecycle. Bind the
    // snapshotted configuration to the nominal worker before any store read.
    if (worker.host !== host || worker.nodeId !== nodeId || worker.leaseMs !== leaseMs || worker.maxClockSkewMs !== maxClockSkewMs)
        return Object.freeze({ state: "child" });
    if (signal?.aborted)
        return Object.freeze({ state: "pending", reason: "aborted" });
    const control = await readControl(store);
    if (signal?.aborted)
        return Object.freeze({ state: "pending", reason: "aborted" });
    if (control.state !== "active")
        return Object.freeze({ state: "pending", reason: "control-not-active" });
    if (control.revokedDestinationIds.includes(embeddingDestination.id))
        return Object.freeze({ state: "pending", reason: "embedding-destination-revoked" });
    const now = worker.now();
    validateWorkerPolicies(workerPolicy, producerPolicies, host, now, maxClockSkewMs);
    const episodes = await store.readEpisodes(membership, control.privacyEpoch).catch(() => []);
    if (signal?.aborted)
        return Object.freeze({ state: "pending", reason: "aborted" });
    if (episodes.length !== membership.length)
        return Object.freeze({ state: "pending", reason: "membership-missing" });
    const episodeById = new Map(episodes.map((episode) => [episode.id, episode]));
    const producerById = new Map(producerPolicies.map((policy) => [policy.id, policy]));
    // Split explicit jobs by compatible policy groups (producer x worker
    // intersection with an LLM destination); incompatible groups stay pending.
    const groups = new Map();
    for (const id of membership) {
        const producerId = episodeById.get(id).processingPolicyId;
        const group = groups.get(producerId) ?? [];
        group.push(id);
        groups.set(producerId, group);
    }
    const compatible = [];
    for (const [producerId, groupIds] of groups) {
        const producer = producerById.get(producerId);
        if (producer === undefined)
            continue;
        const intersection = intersectPolicies([producer], workerPolicy);
        if (intersection === null || intersection.destinationIds.llm === undefined)
            continue;
        // Bind the opaque embedding capability to the same source/local/active
        // policy intersection before any LLM egress; a mismatched destination is
        // pending work, never an invitation to substitute another endpoint.
        if (intersection.destinationIds.embedding !== embeddingDestination.id || embeddingDestination.residency !== intersection.residency || embeddingDestination.dataUse !== intersection.dataUse || embeddingCoordination.policyHash !== control.coordinationPolicyHash || embeddingCoordination.policyEpoch !== control.coordinationPolicyEpoch)
            continue;
        compatible.push({ producerId, intersection, episodes: [...groupIds].sort() });
    }
    if (compatible.length === 0)
        return Object.freeze({ state: "pending", reason: "no-compatible-policy-group" });
    // Snapshot coverage for every group first, then probe the exact job identity
    // that would be used (full membership or uncovered subset). This preserves
    // accepted/released resume priority even after a crash wrote partial coverage.
    let group;
    let existingJobId;
    let blockedAcceptance;
    const terminalGroups = new Set();
    // Crash-resume discovery: inspect the bounded authoritative job index for
    // compatible accepted/released-with-accept subsets, rather than enumerating
    // arbitrary subsets. Candidate ambiguity is resolved by deterministic job id.
    const discoveredJobs = [];
    // Crash-resume discovery is authoritative. A malformed, truncated, cyclic,
    // or failed scroll is not equivalent to "no accepted work": stop before any
    // plan/claim/LLM/embedding operation rather than falling back to a fresh job.
    try {
        let jobCursor;
        let discoveryPages = 0;
        const seenCursors = new Set();
        const seenJobIds = new Set();
        let lastJobId;
        do {
            if (signal?.aborted)
                return Object.freeze({ state: "pending", reason: "aborted" });
            if (++discoveryPages > 16)
                throw new TypeError("Curation job discovery exceeded bounded pages");
            if (jobCursor !== undefined) {
                if (!/^[A-Za-z0-9._:/-]{1,512}$/u.test(jobCursor) || seenCursors.has(jobCursor))
                    throw new TypeError("Curation job discovery cursor is cyclic or invalid");
                seenCursors.add(jobCursor);
            }
            const slice = await store.scrollJobs(jobCursor, 256);
            if (slice === null || typeof slice !== "object" || Array.isArray(slice) || !Array.isArray(slice.jobs) || slice.jobs.length > 256 || new Set(slice.jobs.map((job) => job.id)).size !== slice.jobs.length)
                throw new TypeError("Curation job discovery response is ambiguous");
            if (slice.nextOffset !== undefined && slice.jobs.length === 0)
                throw new TypeError("Curation job discovery is truncated");
            for (const candidateJob of slice.jobs) {
                const candidateId = candidateJob?.id;
                if (typeof candidateId !== "string" || !/^[A-Za-z0-9._:/-]{1,512}$/u.test(candidateId) || (lastJobId !== undefined && lastJobId >= candidateId) || seenJobIds.has(candidateId))
                    throw new TypeError("Curation job discovery jobs are unsorted or duplicated");
                lastJobId = candidateId;
                seenJobIds.add(candidateId);
                // Avoid up to 4096 irrelevant lease reads: only jobs with the exact
                // active policy/epoch/privacy/expiry and a non-empty subset of a
                // compatible full group can be resumable candidates.
                const compatibleCandidate = compatible.find((candidate) => {
                    const fullSet = new Set(candidate.episodes);
                    return candidateJob.ownerHost === host && candidateJob.policyId === candidate.intersection.id && candidateJob.policyHash === control.coordinationPolicyHash && candidateJob.policyEpoch === control.coordinationPolicyEpoch && candidateJob.privacyEpoch === control.privacyEpoch && candidateJob.extractorRevision === extractorRevision && candidateJob.expiresAt === candidate.intersection.expiresAt && Array.isArray(candidateJob.membership) && candidateJob.membership.length > 0 && candidateJob.membership.every((id) => fullSet.has(id));
                });
                if (compatibleCandidate === undefined)
                    continue;
                const candidateLease = await readLease(store, candidateId);
                if (candidateLease?.acceptedProposalId !== null && candidateLease?.acceptedManifestHash !== null && (candidateLease?.state === "accepted" || candidateLease?.state === "released"))
                    discoveredJobs.push({ job: candidateJob, lease: candidateLease });
            }
            const nextOffset = slice.nextOffset;
            if (nextOffset !== undefined && (typeof nextOffset !== "string" || !/^[A-Za-z0-9._:/-]{1,512}$/u.test(nextOffset) || (jobCursor !== undefined && nextOffset <= jobCursor) || seenCursors.has(nextOffset)))
                throw new TypeError("Curation job discovery next cursor is invalid");
            jobCursor = nextOffset;
        } while (jobCursor !== undefined);
    }
    catch {
        return Object.freeze({ state: "pending", reason: "job-discovery-unavailable" });
    }
    const plans = [];
    for (const candidate of compatible) {
        const full = [...candidate.episodes].sort();
        const fullSet = new Set(full);
        const discoveredCandidates = discoveredJobs.filter((entry) => entry.job !== null && entry.job.ownerHost === host && entry.job.policyId === candidate.intersection.id && entry.job.policyHash === control.coordinationPolicyHash && entry.job.policyEpoch === control.coordinationPolicyEpoch && entry.job.privacyEpoch === control.privacyEpoch && entry.job.extractorRevision === extractorRevision && entry.job.expiresAt === candidate.intersection.expiresAt && entry.job.membership.every((id) => fullSet.has(id))).sort((left, right) => left.job.id.localeCompare(right.job.id));
        const tombstones = await store.readTombstones(full);
        const tombstonedIds = new Set(tombstones.filter((tombstone) => tombstone.scope === "occurrence" && full.includes(tombstone.targetId)).map((tombstone) => tombstone.targetId));
        const allTombstoned = tombstonedIds.size === full.length;
        const uncovered = await filterUncoveredEpisodes({ store, membership: full, extractorRevision, policyHash: control.coordinationPolicyHash, policyEpoch: control.coordinationPolicyEpoch, privacyEpoch: control.privacyEpoch, policyIntersectionId: candidate.intersection.id });
        const memberships = [full, ...(uncovered.length > 0 && canonicalStringify(uncovered) !== canonicalStringify(full) ? [uncovered] : [])];
        const jobIds = memberships.map((membership) => ({ membership, id: jobIdFor({ ownerHost: host, membership, policyHash: control.coordinationPolicyHash, policyEpoch: control.coordinationPolicyEpoch, extractorRevision, policyIntersectionId: candidate.intersection.id, privacyEpoch: control.privacyEpoch }) }));
        let blockedAccepted = false;
        let blockedNonterminal = false;
        // Accepted output is an exact membership barrier. A tombstoned accepted
        // subset is not rewritten as a smaller job and must not starve another
        // clean accepted/actionable producer group.
        for (const discovered of discoveredCandidates) {
            const discoveredTombstones = await store.readTombstones(discovered.job.membership);
            if (discoveredTombstones.length !== 0) {
                blockedAccepted = true;
                if (blockedAcceptance === undefined)
                    blockedAcceptance = { candidate, membership: [...discovered.job.membership], id: discovered.job.id };
                continue;
            }
            if (group === undefined) {
                group = { ...candidate, episodes: [...discovered.job.membership] };
                existingJobId = discovered.job.id;
            }
            break;
        }
        const plan = { candidate, full, uncovered, allTombstoned, blockedAccepted, blockedNonterminal, jobIds };
        plans.push(plan);
        if (allTombstoned)
            continue;
        for (const probe of jobIds) {
            const existingJob = await readJob(store, probe.id);
            if (existingJob === null)
                continue;
            const existingLease = await readLease(store, probe.id);
            const hasAcceptedPair = existingLease?.acceptedProposalId !== null && existingLease?.acceptedManifestHash !== null && (existingLease?.state === "accepted" || existingLease?.state === "released");
            if (hasAcceptedPair) {
                const probeTombstones = await store.readTombstones(probe.membership);
                if (probeTombstones.length !== 0) {
                    blockedAccepted = true;
                    plan.blockedAccepted = true;
                    if (blockedAcceptance === undefined)
                        blockedAcceptance = { candidate, membership: [...probe.membership], id: probe.id };
                    continue;
                }
                if (group === undefined) {
                    group = { ...candidate, episodes: [...probe.membership] };
                    existingJobId = probe.id;
                }
                break;
            }
            // A released/leased job without an accepted pair is still the exact
            // durable work identity. Reuse it instead of insert_only-colliding when
            // a previous LLM failure is retried with a different clock.
            if (existingLease?.state === "leased" || existingLease?.state === "released" || existingLease === null) {
                // Defer non-accepted work until every producer group has been scanned;
                // a later clean accepted pair always has resume priority.
                break;
            }
            if (existingLease?.state === "completed") {
                const expected = probe.membership.map((episodeId) => coverageId({ ownerHost: host, episodeId, extractorRevision, coordinationPolicyHash: control.coordinationPolicyHash, coordinationPolicyEpoch: control.coordinationPolicyEpoch, policyIntersectionId: candidate.intersection.id, privacyEpoch: control.privacyEpoch }));
                const covered = await store.readCoverage(expected);
                if (covered.length === expected.length && covered.every((record) => expected.includes(record.id) && record.contentHash === canonicalRecordHash(record)))
                    terminalGroups.add(probe.id);
            }
        }
    }
    if (group === undefined) {
        // No accepted pair exists globally. Now choose the first exact durable
        // nonterminal job identity (or uncovered plan) in deterministic order.
        outer: for (const plan of plans) {
            for (const probe of plan.jobIds) {
                const existingJob = await readJob(store, probe.id);
                if (existingJob === null)
                    continue;
                const existingLease = await readLease(store, probe.id);
                if (existingLease?.state === "leased" || existingLease?.state === "released") {
                    const probeTombstones = await store.readTombstones(probe.membership);
                    if (probeTombstones.length !== 0) {
                        plan.blockedNonterminal = true;
                        continue;
                    }
                    group = { ...plan.candidate, episodes: [...probe.membership] };
                    existingJobId = probe.id;
                    break outer;
                }
            }
        }
    }
    if (group === undefined) {
        // No resumable acceptance exists: choose the first uncovered plan in
        // deterministic producer-group order, without starving later groups.
        for (const plan of plans) {
            if (plan.allTombstoned || plan.blockedAccepted || plan.blockedNonterminal || plan.uncovered.length === 0)
                continue;
            const selectedId = plan.jobIds.find((probe) => canonicalStringify(probe.membership) === canonicalStringify(plan.uncovered))?.id;
            if (selectedId !== undefined && terminalGroups.has(selectedId))
                continue;
            group = { ...plan.candidate, episodes: [...plan.uncovered] };
            break;
        }
    }
    if (group === undefined && blockedAcceptance !== undefined) {
        group = { ...blockedAcceptance.candidate, episodes: [...blockedAcceptance.membership] };
        existingJobId = blockedAcceptance.id;
    }
    if (group === undefined) {
        const terminalControl = await readControl(store);
        if (terminalControl.state !== "active" || terminalControl.coordinationPolicyEpoch !== control.coordinationPolicyEpoch || terminalControl.coordinationPolicyHash !== control.coordinationPolicyHash || terminalControl.privacyEpoch !== control.privacyEpoch)
            return Object.freeze({ state: "pending", reason: "control-changed-before-tombstone-terminal" });
        if (plans.some((plan) => plan.blockedNonterminal))
            return Object.freeze({ state: "pending", reason: "tombstoned-durable-job" });
        if (plans.length > 0 && plans.every((plan) => plan.allTombstoned))
            return Object.freeze({ state: "completed", reason: "all-tombstoned", observations: 0 });
        return Object.freeze({ state: "completed", reason: "already-covered", observations: 0 });
    }
    // Fence the explicit job identity itself against a control change that
    // occurred while coverage was being read; stale work may remain searchable
    // but must never proceed to a claim/LLM call under the old epoch.
    const beforeJob = await readControl(store);
    if (beforeJob.state !== "active" || beforeJob.coordinationPolicyEpoch !== control.coordinationPolicyEpoch || beforeJob.coordinationPolicyHash !== control.coordinationPolicyHash || beforeJob.privacyEpoch !== control.privacyEpoch || beforeJob.revokedDestinationIds.includes(embeddingDestination.id))
        return Object.freeze({ state: "pending", reason: "control-changed-before-job" });
    const jobIdValue = existingJobId ?? (await createJob(store, {
        ownerHost: host, membership: group.episodes, policyIntersectionId: group.intersection.id,
        policyHash: control.coordinationPolicyHash, policyEpoch: control.coordinationPolicyEpoch,
        extractorRevision, privacyEpoch: control.privacyEpoch, expiresAt: group.intersection.expiresAt,
        // Job identity is durable work metadata: bind its timestamp to the
        // canonical episode rather than a per-run mutable clock getter.
        createdAt: episodeById.get(group.episodes[0]).createdAt,
    })).id;
    if (signal?.aborted)
        return Object.freeze({ state: "pending", reason: "aborted", jobId: jobIdValue });
    const claim = await claimLease(store, worker, { jobId: jobIdValue, policyEpoch: control.coordinationPolicyEpoch, policyHash: control.coordinationPolicyHash, privacyEpoch: control.privacyEpoch });
    if (claim === null)
        return Object.freeze({ state: "no_claim", jobId: jobIdValue });
    let activeAuthority = claim;
    const fail = async (reason) => {
        // Accepted work is durable resumable state. Never downgrade it merely
        // because a derived write/readback failed; a later invocation must consume
        // the same proposal without issuing another completion.
        try {
            await releaseLease(store, activeAuthority);
        }
        catch { /* lease may be stolen/expired; job stays retryable */ }
        return Object.freeze({ state: "pending", reason, jobId: jobIdValue });
    };
    // Crash recovery: acceptance is durable before materialization. If another
    // invocation reclaims the still-accepted lease, consume that exact proposal
    // directly; never issue another LLM request or proposal.
    if (claim.state === "accepted") {
        try {
            const acceptance = await readActiveAcceptance(store, claim);
            if (acceptance === null)
                return await fail("accepted-output-barrier");
            const acceptedProposal = acceptance.proposal;
            const acceptedJob = acceptance.job;
            if (acceptedProposal.membership.length !== acceptedJob.membership.length || acceptedProposal.membership.some((id, index) => id !== acceptedJob.membership[index]))
                return await fail("accepted-output-binding");
            const acceptedEnvelope = parseCurationProposalEnvelope(acceptedProposal.content);
            if (acceptedEnvelope === null)
                return await fail("accepted-output-binding");
            const acceptedDestinationId = group.intersection.destinationIds.llm;
            const acceptedProviderValid = group.intersection.originProvider === PROVIDER_AGNOSTIC_ORIGIN || acceptedEnvelope.provenance.providerId === group.intersection.originProvider || group.intersection.allowCrossProviderReplay === true;
            if (acceptedDestinationId === undefined || !acceptedProviderValid || !provenanceMatches(acceptedEnvelope.provenance, { host, destinationId: acceptedDestinationId, policyId: acceptedJob.policyId, policyHash: acceptedJob.policyId, policyEpoch: claim.coordinationPolicyEpoch, promptRevision: CURATION_PROMPT_REVISION }) || acceptedProposal.createdAt !== acceptedEnvelope.provenance.invokedAt)
                return await fail("accepted-output-provenance");
            const acceptedEpisodes = await store.readEpisodes(acceptedProposal.membership, acceptedJob.privacyEpoch).catch(() => []);
            if (acceptedEpisodes.length !== acceptedProposal.membership.length)
                return await fail("accepted-membership-missing");
            const acceptedEpisodeById = new Map(acceptedEpisodes.map((episode) => [episode.id, episode]));
            if (acceptedJob.membership.length === 0 || acceptedEpisodeById.get(acceptedJob.membership[0])?.createdAt !== acceptedJob.createdAt)
                return await fail("accepted-job-created-at");
            const acceptedResult = validateCurationResult({ items: acceptedEnvelope.items }, { directUserEpisodeIds: new Set(acceptedEpisodes.filter((episode) => episode.eventKind === "user").map((episode) => episode.id)), knownEpisodeIds: new Set(acceptedEpisodes.map((episode) => episode.id)) });
            const outcome = await materializeCuration(claim, { store, result: acceptedResult, policy: group.intersection, embedding, extractorRevision, });
            const completed = await completeJob(store, claim);
            if (!completed)
                return await fail("job-completion-readback");
            return Object.freeze({ state: "completed", jobId: jobIdValue, observations: outcome.observations.length });
        }
        catch (error) {
            return await fail("accepted-materialization-failed");
        }
    }
    // Stable control -> tombstones -> fresh claim/clock -> control sandwich
    // immediately before LLM egress. A forgotten member or revocation cannot be
    // raced by a delayed preflight read.
    try {
        // Snapshot mutable fresh-call options only for a leased job. Accepted
        // resume returned above never executes this block or touches these getters.
        const fresh = input.freshOptionsProvider?.() ?? { llm: input.llmProvider?.() ?? input.llm, maxOutputTokens: input.maxOutputTokens, timeoutMs: input.timeoutMs, scan: input.scan };
        if (fresh.llm === undefined)
            throw new TypeError("LLM configuration is missing");
        maxOutputTokens = fresh.maxOutputTokens ?? DEFAULT_MAX_OUTPUT_TOKENS;
        timeoutMs = fresh.timeoutMs ?? DEFAULT_TIMEOUT_MS;
        scan = fresh.scan;
        if (scan !== undefined && typeof scan !== "function")
            throw new TypeError("Curation scanner is invalid");
        if (!Number.isSafeInteger(maxOutputTokens) || maxOutputTokens < 128 || maxOutputTokens > 8_192 || !Number.isSafeInteger(timeoutMs) || timeoutMs < 1_000 || timeoutMs > 120_000)
            throw new TypeError("LLM budgets are invalid");
        // Snapshot the mutable LLM surface only for a leased job. Accepted resume
        // returned above never executes this block.
        const llm = fresh.llm;
        if (llm === undefined)
            return await fail("llm-unavailable");
        const memoryModel = llm.memoryModel;
        const registry = llm.modelRegistry;
        const registryComplete = Reflect.get(registry, "complete");
        const registryAuth = Reflect.get(registry, "getApiKeyAndHeaders");
        modelRegistry = Object.freeze({
            ...(typeof registryComplete === "function" ? { complete: registryComplete.bind(registry) } : {}),
            ...(typeof registryAuth === "function" ? { getApiKeyAndHeaders: registryAuth.bind(registry) } : {}),
        });
        try {
            const ownedModel = ownedModelSnapshot(memoryModel);
            modelCanonical = ownedModel.canonical;
            modelSnapshot = deepFreeze(ownedModel.model);
        }
        catch {
            return await fail("LLM model snapshot is invalid");
        }
        modelStillBound = () => { try {
            return ownedModelSnapshot(memoryModel).canonical === modelCanonical;
        }
        catch {
            return false;
        } };
        if (!modelStillBound())
            return await fail("model-changed");
        // Snapshot each mutable seam exactly once. The helpers inspect only the
        // contractual own data descriptors, never enumerate unknown keys.
        const rawDestination = llm.llmDestination;
        const rawBinding = llm.llmDestinationBinding;
        llmDestination = snapshotDestination(rawDestination, "LLM destination");
        llmDestinationBinding = snapshotModelBinding(rawBinding);
        if (group.intersection.destinationIds.llm !== llmDestination.id || llmDestinationBinding.destinationId !== llmDestination.id)
            return await fail("llm-destination-mismatch");
        const preLlmBarrier = await assertPreLlmBarrier(store, claim, group.episodes, llmDestination.id);
        // Task 6 portable LLM bridge: bounded envelope egress only.
        const prompt = buildCurationPrompt({
            host, policyId: group.intersection.id, policyHash: claim.coordinationPolicyHash, policyEpoch: claim.coordinationPolicyEpoch,
            provider: { providerId: llmDestinationBinding.providerId, modelId: llmDestinationBinding.modelId, destinationId: llmDestination.id },
            membership: group.episodes, episodes: group.episodes.map((id) => episodeById.get(id)),
        });
        if (!modelStillBound())
            return await fail("model-changed");
        if (signal?.aborted)
            return await fail("aborted");
        const completion = await completeMemory({
            envelope: prompt.envelope, model: modelSnapshot, hostContext: { messages: [] },
            maxInputTokens: prompt.maxInputTokens, maxOutputTokens, timeoutMs, ...(signal === undefined ? {} : { signal }),
            memoryContext: {
                host, modelRegistry, memoryModel: modelSnapshot, policy: group.intersection, llmDestination,
                llmDestinationBinding, policyEpoch: claim.coordinationPolicyEpoch, policyHash: group.intersection.id,
                allowCrossProviderReplay: group.intersection.allowCrossProviderReplay,
            },
            promptRevision: prompt.promptRevision,
        });
        if (signal?.aborted)
            return await fail("aborted");
        if (completion.state !== "completed")
            return await fail(`llm-${completion.reason}`);
        if (!modelStillBound())
            return await fail("model-changed");
        const expectedDestinationId = group.intersection.destinationIds.llm;
        if (expectedDestinationId === undefined || expectedDestinationId !== llmDestination.id)
            return await fail("llm-destination-mismatch");
        const expectedProvenance = { host, providerId: llmDestinationBinding.providerId, modelId: llmDestinationBinding.modelId, destinationId: expectedDestinationId, policyId: group.intersection.id, policyHash: group.intersection.id, policyEpoch: claim.coordinationPolicyEpoch, promptRevision: CURATION_PROMPT_REVISION };
        if (!provenanceMatches(completion.provenance, expectedProvenance) || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/u.test(completion.provenance.invokedAt) || new Date(Date.parse(completion.provenance.invokedAt)).toISOString() !== completion.provenance.invokedAt)
            return await fail("llm-provenance");
        const parsed = parseStrictCurationJson(completion.text);
        const directUserEpisodeIds = new Set(episodes.filter((episode) => episode.eventKind === "user").map((episode) => episode.id));
        const result = assertPersistableCurationResult(validateCurationResult(parsed, { directUserEpisodeIds, knownEpisodeIds: new Set(group.episodes) }), scan);
        // Pre-accept collision proof: two items may differ in text/confidence while
        // deriving the same observation identity. Reject before proposal persistence
        // rather than creating an accepted output completion can never verify.
        const projectionKeys = new Map();
        for (const item of result.items) {
            const projection = projectCurationItem(host, control.coordinationPolicyHash, control.coordinationPolicyEpoch, item, episodeById);
            const prior = projectionKeys.get(projection.observationId);
            if (prior !== undefined)
                return await fail("duplicate-observation-projection");
            projectionKeys.set(projection.observationId, canonicalStringify({ ...projection, evidence: projection.evidence.map((episode) => episode.id).sort(), item }));
        }
        // Stable post-LLM control -> claim/clock -> tombstones -> control
        // sandwich. Destination revocation is checked here as well as before the
        // call, so a late completion can never be accepted.
        const postLlmBarrier = await assertPostLlmBarrier(store, claim, group.episodes, llmDestination.id);
        if (postLlmBarrier !== preLlmBarrier)
            return await fail("llm-authority-changed");
        const proposal = await writeProposal(store, claim, {
            membership: group.episodes,
            content: { schema: "curation_proposal_v1", items: [...result.items], provenance: completion.provenance },
            createdAt: completion.provenance.invokedAt,
        });
        const accepted = await acceptProposal(store, claim, { proposalId: proposal.id });
        if (accepted === null)
            return await fail("proposal-race");
        activeAuthority = accepted;
        const outcome = await materializeCuration(accepted, {
            store, result, policy: group.intersection, embedding,
            extractorRevision, ...(scan === undefined ? {} : { scan }),
        });
        // Explicit terminal completion is capability-gated and follows exact
        // immutable readback. A failed completion remains retryable; accepted
        // retries consume the proposal without another LLM call.
        const completed = await completeJob(store, accepted);
        if (!completed)
            return await fail("job-completion-readback");
        return Object.freeze({ state: "completed", jobId: jobIdValue, observations: outcome.observations.length });
    }
    catch (error) {
        const detail = error instanceof Error ? error.message : "";
        const reason = detail.toLowerCase().includes("direct user evidence") ? "direct user evidence" : detail.toLowerCase().includes("llm") ? "llm-failed" : "materialization-failed";
        return await fail(reason);
    }
}
//# sourceMappingURL=worker.js.map