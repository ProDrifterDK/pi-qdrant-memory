import { canonicalRecordHash, parseMemoryRecord } from "../domain/records.js";
import { canonicalStringify } from "../domain/canonical.js";
import { bindConfiguredDestination, canonicalEgressEndpoint } from "../security/egress.js";
import { physicalPointId, COLLECTION_CONTROL_ID, assertBootstrapControl, controlPayload, controlRecordFromPayload } from "./schema.js";
import { expectedQdrantCollection, readPolicy } from "./client.js";
import { QdrantContentHashCollisionError } from "../domain/qdrant-errors.js";
export { QdrantContentHashCollisionError, QDRANT_CONTENT_HASH_COLLISION } from "../domain/qdrant-errors.js";
const CONTROL_PATCH_KEYS = new Set(["version", "processingPolicyId", "activeGeneration", "activeBaseGeneration", "privacyEpoch", "coordinationPolicyEpoch", "coordinationPolicyHash", "state", "scanCursor", "lastForgetBarrier", "contentHash"]);
function fail(message) { throw new TypeError(message); }
function isRecord(value) { return typeof value === "object" && value !== null && !Array.isArray(value); }
function mapKey(key) {
    const names = { recordType: "record_type", ownerHost: "owner_host", schemaRevision: "schema_revision", createdAt: "created_at", privacyEpoch: "privacy_epoch", processingPolicyId: "processing_policy_id", expiresAt: "expires_at", contentHash: "content_hash", sourceEntryId: "source_entry_id", projectId: "project_id", projectIdentityKind: "project_identity_kind", sessionId: "session_id", turnId: "turn_id", agentRole: "agent_role", eventKind: "event_kind", eventAt: "event_at", modelId: "model_id", embeddingDimension: "embedding_dimension", originProvider: "origin_provider", destinationId: "destination_id", redactionStatus: "redaction_status", secretScan: "secret_scan", toolName: "tool_name", toolArgs: "tool_args", errorFingerprint: "error_fingerprint", producerId: "producer_id", nodeId: "node_id", coordinationPolicyHash: "coordination_policy_hash", coordinationPolicyEpoch: "coordination_policy_epoch", contentId: "content_id", observationId: "observation_id", effectiveAt: "effective_at", sourceEpisodeIds: "source_episode_ids", manifestHash: "manifest_hash", primaryEvidenceEpisodeId: "primary_evidence_episode_id", effectiveOrder: "effective_order", stateKey: "state_key", category: "category", scope: "scope", subject: "subject", predicate: "predicate", confidence: "confidence", generationId: "generation_id", clusterId: "cluster_id", membershipHash: "membership_hash", level: "level", memberIds: "member_ids", summary: "summary", promptRevision: "prompt_revision", algorithm: "algorithm", seed: "seed", jobId: "job_id", fencingToken: "fencing_token", temporalFrom: "temporal_from", temporalTo: "temporal_to", coveredProjects: "covered_projects", algorithmParameters: "algorithm_parameters", activeGeneration: "active_generation", activeBaseGeneration: "active_base_generation", state: "state", scanCursor: "scan_cursor", lastForgetBarrier: "last_forget_barrier", policy: "policy", canonicalHash: "canonical_hash", policyId: "policy_id", policyHash: "policy_hash", policyEpoch: "policy_epoch", membership: "membership", leaseExpiresAt: "lease_expires_at", leaseOwner: "lease_owner", acceptedProposalId: "accepted_proposal_id", acceptedManifestHash: "accepted_manifest_hash", episodeId: "episode_id", extractorRevision: "extractor_revision", sourceId: "source_id", targetId: "target_id", provenanceId: "provenance_id", resolution: "resolution", conflictManifestHash: "conflict_manifest_hash", value: "value" };
    return names[key] ?? key;
}
function recordPayload(record) { const parsed = parseMemoryRecord(record); const payload = {}; for (const [key, value] of Object.entries(parsed)) {
    if (key === "vector")
        continue;
    const mapped = mapKey(key);
    if (mapped === key && /[A-Z]/u.test(key))
        fail(`Unmapped record field: ${key}`);
    payload[mapped] = value;
} payload.status = payload.status ?? "active"; payload.secret_scan = payload.secret_scan ?? "passed"; return payload; }
function recordPoint(record) { const payload = record.recordType === "collection_control" ? controlPayload(record) : recordPayload(record); const point = { id: physicalPointId(record.recordType, record.id), payload }; if ("vector" in record && record.vector !== undefined)
    point.vector = { semantic: record.vector }; return point; }
function policyFor(client, recordType, purpose = "write_verification") { return readPolicy({ ownerHost: client.ownerHost, purpose, recordTypes: [recordType], maxClockSkewMs: client.maxClockSkewMs }); }
function contentHash(payload) { return payload.content_hash; }
function collision(expected, actual) { throw new Error(`content hash collision for ${expected}: ${String(actual)}`); }
function checkHash(payload, expected) { if (contentHash(payload) !== expected)
    collision(expected, contentHash(payload)); }
async function retrieveOne(client, id, policy, includeVector = false) { const points = await client.retrieve([id], policy, { includeVector }); return points.find((point) => point.id === id); }
/** Insert-only is at-least-once: preflight and postflight reads classify observed state; a concurrent race is inherently ambiguous. */
export async function insertOnly(client, record) {
    if (!(record.recordType === "collection_control" && record.version === 0))
        parseMemoryRecord(record);
    if (record.recordType === "collection_control" && record.version === 0)
        assertBootstrapControl(record, client.ownerHost);
    else if (record.contentHash !== canonicalRecordHash(record))
        fail("Memory record canonical hash mismatch");
    const point = recordPoint(record);
    const policy = policyFor(client, record.recordType === "collection_control" ? "collection_control" : record.recordType, record.recordType === "collection_control" ? "control" : "write_verification");
    const before = await retrieveOne(client, point.id, policy);
    let existing = false;
    if (before !== undefined) {
        checkHash(before.payload, record.contentHash);
        existing = true;
    }
    await client.upsertPoints([point], "insert_only");
    const after = await retrieveOne(client, point.id, policy);
    if (after === undefined)
        throw new Error(`insert-only write did not read back point ${point.id}`);
    checkHash(after.payload, record.contentHash);
    return existing ? "existing" : "inserted";
}
export async function insertInitialControl(client, control) { assertBootstrapControl(control, client.ownerHost); return insertOnly(client, control); }
function patchPayload(patch) { const result = {}; for (const key of Object.keys(patch)) {
    if (!CONTROL_PATCH_KEYS.has(key))
        fail(`CAS patch key is not mutable: ${key}`);
    const value = patch[key];
    if (key === "version" || key === "privacyEpoch" || key === "coordinationPolicyEpoch") {
        if (!Number.isSafeInteger(value) || value < 0)
            fail(`CAS patch field is invalid: ${key}`);
    }
    else if (key === "state") {
        if (!["active", "draining", "retired"].includes(String(value)))
            fail("CAS patch state is invalid");
    }
    else if (["activeGeneration", "activeBaseGeneration", "scanCursor", "lastForgetBarrier"].includes(key)) {
        if (value !== null && (typeof value !== "string" || value.length === 0))
            fail(`CAS patch field is invalid: ${key}`);
    }
    else if (key === "processingPolicyId") {
        if (typeof value !== "string" || value.length === 0 || value.length > 512)
            fail("CAS patch processing policy is invalid");
    }
    else if (key === "coordinationPolicyHash") {
        if (typeof value !== "string" || value.length === 0 || value.length > 512)
            fail("CAS patch policy hash is invalid");
    }
    else if (key === "contentHash") {
        if (typeof value !== "string" || !/^[a-f0-9]{64}$/u.test(value))
            fail("CAS patch content hash is invalid");
    }
    result[mapKey(key)] = value;
} return result; }
function ownerPrecondition(client, expectedVersion, expectedPrivacyEpoch, expectedEpoch, expectedState, expectedBaseGeneration) { return { kind: "collection-control-cas", ownerHost: client.ownerHost, recordType: "collection_control", expectedVersion, expectedPrivacyEpoch, expectedEpoch, expectedState, ...(expectedBaseGeneration === undefined ? {} : { expectedBaseGeneration }) }; }
function mergePayload(existing, patch) { return { ...existing, ...patch }; }
function deepEqual(left, right) { try {
    return canonicalStringify(left) === canonicalStringify(right);
}
catch {
    return false;
} }
export async function updateOnlyCas(client, input) {
    if (input.id !== COLLECTION_CONTROL_ID)
        fail("CAS control ID is invalid");
    if (!Number.isSafeInteger(input.expectedVersion) || input.expectedVersion < 0 || !Number.isSafeInteger(input.expectedEpoch) || input.expectedEpoch < 0)
        fail("CAS version/epoch is invalid");
    if (!isRecord(input.patch) || input.patch.version !== input.expectedVersion + 1)
        fail("CAS patch must advance version exactly once");
    const patch = patchPayload(input.patch);
    const policy = policyFor(client, "collection_control", "control");
    const current = await retrieveOne(client, COLLECTION_CONTROL_ID, policy, true);
    if (current === undefined)
        return false;
    const currentRecord = controlRecordFromPayload(current.payload, client.ownerHost);
    if (currentRecord.state === "retired" || currentRecord.version !== input.expectedVersion || currentRecord.coordinationPolicyEpoch !== input.expectedEpoch)
        return false;
    const suppliedHash = input.patch.contentHash;
    const candidate = { ...currentRecord, ...input.patch, contentHash: "pending" };
    parseMemoryRecord(candidate);
    const privacyDelta = candidate.privacyEpoch - currentRecord.privacyEpoch;
    const coordinationDelta = candidate.coordinationPolicyEpoch - currentRecord.coordinationPolicyEpoch;
    if (privacyDelta < 0 || privacyDelta > 1 || coordinationDelta < 0 || coordinationDelta > 1)
        return false;
    if (coordinationDelta === 0 && (candidate.coordinationPolicyHash !== currentRecord.coordinationPolicyHash || candidate.processingPolicyId !== currentRecord.processingPolicyId))
        return false;
    const computedHash = canonicalRecordHash(candidate);
    if (suppliedHash !== undefined && suppliedHash !== computedHash)
        fail("CAS patch content hash is inconsistent");
    candidate.contentHash = computedHash;
    const point = recordPoint(candidate);
    if (current.vector !== undefined)
        point.vector = current.vector;
    await client.upsertPoints([point], "update_only", ownerPrecondition(client, input.expectedVersion, currentRecord.privacyEpoch, input.expectedEpoch, currentRecord.state));
    const reread = await retrieveOne(client, COLLECTION_CONTROL_ID, policy, true);
    if (reread === undefined)
        return false;
    const rereadRecord = controlRecordFromPayload(reread.payload, client.ownerHost);
    return deepEqual(controlPayload(rereadRecord), controlPayload(candidate));
}
export async function publishControlCas(client, input) {
    if (!Number.isSafeInteger(input.expectedVersion) || input.expectedVersion < 0 || input.next.id !== COLLECTION_CONTROL_ID || input.next.version !== input.expectedVersion + 1 || input.next.ownerHost !== client.ownerHost)
        fail("Control CAS input is invalid");
    parseMemoryRecord(input.next);
    if (input.next.contentHash !== canonicalRecordHash(input.next))
        fail("Next control canonical hash mismatch");
    const policy = policyFor(client, "collection_control", "control");
    const current = await retrieveOne(client, COLLECTION_CONTROL_ID, policy, true);
    if (current === undefined)
        return false;
    const currentRecord = controlRecordFromPayload(current.payload, client.ownerHost);
    if (currentRecord.state !== "active" || input.next.state !== "active")
        return false;
    if (input.next.privacyEpoch !== currentRecord.privacyEpoch || input.next.coordinationPolicyEpoch !== currentRecord.coordinationPolicyEpoch || input.next.coordinationPolicyHash !== currentRecord.coordinationPolicyHash || input.next.processingPolicyId !== currentRecord.processingPolicyId)
        return false;
    const expectedPayload = controlPayload(input.next);
    if (currentRecord.version === input.next.version && deepEqual(controlPayload(currentRecord), expectedPayload))
        return true;
    if (currentRecord.version !== input.expectedVersion || currentRecord.activeBaseGeneration !== input.expectedBaseGeneration)
        return false;
    const point = recordPoint(input.next);
    if (current.vector !== undefined)
        point.vector = current.vector;
    await client.upsertPoints([point], "update_only", ownerPrecondition(client, input.expectedVersion, currentRecord.privacyEpoch, currentRecord.coordinationPolicyEpoch, currentRecord.state, input.expectedBaseGeneration));
    const reread = await retrieveOne(client, COLLECTION_CONTROL_ID, policy, true);
    if (reread === undefined)
        return false;
    const rereadRecord = controlRecordFromPayload(reread.payload, client.ownerHost);
    return deepEqual(controlPayload(rereadRecord), expectedPayload);
}
/** A nominal, endpoint-pinned writer capability; factories never accept a raw structural client. */
export class ValidatedQdrantSessionWriter {
    endpoint;
    ownerHost;
    collection;
    #writer;
    constructor(endpoint, client) {
        this.endpoint = endpoint;
        this.ownerHost = client.ownerHost;
        this.collection = client.collection;
        // Copy identity scalars and bind every operation once. Later reassignment
        // of methods/properties on a caller-owned fake/client cannot retarget this capability.
        this.#writer = Object.freeze({ endpoint, ownerHost: this.ownerHost, collection: this.collection, maxClockSkewMs: client.maxClockSkewMs, retrieve: client.retrieve.bind(client), upsertPoints: client.upsertPoints.bind(client) });
        Object.freeze(this);
    }
    writer() { return this.#writer; }
    static bind(input) {
        const endpoint = canonicalEgressEndpoint(input.endpoint);
        if (typeof input.client?.upsertPoints !== "function" || typeof input.client.retrieve !== "function" || (input.client.ownerHost !== "pi" && input.client.ownerHost !== "prime") || input.client.collection !== expectedQdrantCollection(input.client.ownerHost) || !Number.isFinite(input.client.maxClockSkewMs) || typeof input.client.endpoint !== "string" || canonicalEgressEndpoint(input.client.endpoint) !== endpoint)
            throw new TypeError("Qdrant writer endpoint/host/collection pairing is invalid");
        return new ValidatedQdrantSessionWriter(endpoint, Object.freeze(input.client));
    }
}
/** Explicit factory seam for endpoint-bound production writers and test fakes. */
export function bindQdrantSessionWriter(input) { return ValidatedQdrantSessionWriter.bind(input); }
function validCoordinationBinding(hash, epoch) {
    if (typeof hash !== "string" || hash.length === 0 || hash.length > 512 || !/^[A-Za-z0-9._:-]+$/u.test(hash) || !Number.isSafeInteger(epoch) || epoch < 0)
        throw new TypeError("Qdrant coordination binding is invalid");
}
function payloadValue(payload, camel, snake) { return Object.prototype.hasOwnProperty.call(payload, snake) ? payload[snake] : payload[camel]; }
function optionalPayload(payload, camel, snake) { return Object.prototype.hasOwnProperty.call(payload, snake) ? payload[snake] : payload[camel]; }
function sameCanonicalWirePayload(point, parsed) {
    try {
        return canonicalStringify(point.payload) === canonicalStringify(recordPayload(parsed));
    }
    catch {
        return false;
    }
}
function sameCanonicalWireVector(point, parsed) {
    const expected = parsed.recordType === "episode" && parsed.vector !== undefined ? { semantic: parsed.vector } : null;
    try {
        return canonicalStringify(point.vector ?? null) === canonicalStringify(expected);
    }
    catch {
        return false;
    }
}
async function boundRetrieve(client, recordType, id) {
    const policy = policyFor(client, recordType);
    const point = (await client.retrieve([physicalPointId(recordType, id)], policy, { includeVector: true })).find((candidate) => candidate.id === physicalPointId(recordType, id));
    if (point === undefined || point.payload.record_type !== recordType || point.payload.content_hash === undefined)
        return null;
    const common = {
        recordType, id, ownerHost: payloadValue(point.payload, "ownerHost", "owner_host"), schemaRevision: payloadValue(point.payload, "schemaRevision", "schema_revision"),
        createdAt: payloadValue(point.payload, "createdAt", "created_at"), privacyEpoch: payloadValue(point.payload, "privacyEpoch", "privacy_epoch"),
        processingPolicyId: payloadValue(point.payload, "processingPolicyId", "processing_policy_id"), expiresAt: payloadValue(point.payload, "expiresAt", "expires_at"), contentHash: payloadValue(point.payload, "contentHash", "content_hash"),
    };
    const value = recordType === "processing_policy" ? {
        ...common, policy: point.payload.policy, canonicalHash: payloadValue(point.payload, "canonicalHash", "canonical_hash"),
    } : {
        ...common, sourceEntryId: payloadValue(point.payload, "sourceEntryId", "source_entry_id"), host: point.payload.host,
        projectId: payloadValue(point.payload, "projectId", "project_id"), projectIdentityKind: payloadValue(point.payload, "projectIdentityKind", "project_identity_kind"),
        sessionId: payloadValue(point.payload, "sessionId", "session_id"), turnId: payloadValue(point.payload, "turnId", "turn_id"), agentRole: payloadValue(point.payload, "agentRole", "agent_role"),
        depth: point.payload.depth, eventKind: payloadValue(point.payload, "eventKind", "event_kind"), eventAt: payloadValue(point.payload, "eventAt", "event_at"),
        modelId: payloadValue(point.payload, "modelId", "model_id"), embeddingDimension: payloadValue(point.payload, "embeddingDimension", "embedding_dimension"),
        originProvider: payloadValue(point.payload, "originProvider", "origin_provider"), destinationId: payloadValue(point.payload, "destinationId", "destination_id"),
        status: point.payload.status, redactionStatus: payloadValue(point.payload, "redactionStatus", "redaction_status"), secretScan: payloadValue(point.payload, "secretScan", "secret_scan"),
        ...(optionalPayload(point.payload, "text", "text") === undefined ? {} : { text: optionalPayload(point.payload, "text", "text") }),
        ...(optionalPayload(point.payload, "toolName", "tool_name") === undefined ? {} : { toolName: optionalPayload(point.payload, "toolName", "tool_name") }),
        ...(optionalPayload(point.payload, "toolArgs", "tool_args") === undefined ? {} : { toolArgs: optionalPayload(point.payload, "toolArgs", "tool_args") }),
        ...(optionalPayload(point.payload, "errorFingerprint", "error_fingerprint") === undefined ? {} : { errorFingerprint: optionalPayload(point.payload, "errorFingerprint", "error_fingerprint") }),
        ...(optionalPayload(point.payload, "producerId", "producer_id") === undefined ? {} : { producerId: optionalPayload(point.payload, "producerId", "producer_id") }),
        ...(optionalPayload(point.payload, "nodeId", "node_id") === undefined ? {} : { nodeId: optionalPayload(point.payload, "nodeId", "node_id") }),
        ...(point.vector?.semantic === undefined ? {} : { vector: point.vector.semantic }),
    };
    try {
        const parsed = parseMemoryRecord(value);
        if (parsed.contentHash !== canonicalRecordHash(parsed) || !sameCanonicalWirePayload(point, parsed) || !sameCanonicalWireVector(point, parsed))
            return null;
        return parsed;
    }
    catch {
        return null;
    }
}
function isVerifiedBoundCollision(readback, record) {
    return readback !== null && readback.recordType === record.recordType && readback.id === record.id && readback.contentHash !== record.contentHash;
}
/** Create a closure that snapshots one canonical endpoint/client/destination pairing. */
export function createQdrantDestinationFactory(input) {
    validCoordinationBinding(input.coordinationPolicyHash, input.coordinationPolicyEpoch);
    const endpoint = canonicalEgressEndpoint(input.endpoint);
    if (!(input.client instanceof ValidatedQdrantSessionWriter) || input.client.endpoint !== endpoint || input.client.collection !== expectedQdrantCollection(input.client.ownerHost))
        throw new TypeError("Qdrant writer endpoint/host/collection pairing is invalid");
    const client = input.client.writer();
    const ownerHost = input.client.ownerHost;
    const collection = input.client.collection;
    if (client.endpoint !== endpoint || client.ownerHost !== ownerHost || client.collection !== collection || client.collection !== expectedQdrantCollection(client.ownerHost))
        throw new TypeError("Qdrant writer endpoint/host/collection pairing is invalid");
    const egressMode = input.egressMode;
    const nodeId = input.nodeId;
    const configuredIdentity = Object.freeze({ ...input.destination });
    const configured = bindConfiguredDestination({ endpoint, configuredDestination: configuredIdentity, requestedDestination: configuredIdentity, egressMode, ...(nodeId === undefined ? {} : { nodeId }) });
    const coordination = Object.freeze({ policyHash: input.coordinationPolicyHash, policyEpoch: input.coordinationPolicyEpoch });
    return Object.freeze({ bind: (requested) => {
            const destination = Object.freeze({ ...bindConfiguredDestination({ endpoint, configuredDestination: configured, requestedDestination: requested, egressMode, ...(nodeId === undefined ? {} : { nodeId }) }) });
            return Object.freeze({ destination, ownerHost, collection, coordination,
                insertAndReadback: async (record) => {
                    let result;
                    try {
                        result = await insertOnly(client, record);
                    }
                    catch {
                        let observed = null;
                        try {
                            observed = await boundRetrieve(client, record.recordType, record.id);
                        }
                        catch { /* ambiguous readback remains retryable */ }
                        if (isVerifiedBoundCollision(observed, record))
                            throw new QdrantContentHashCollisionError();
                        throw new Error("Qdrant insertion failed");
                    }
                    const readback = await boundRetrieve(client, record.recordType, record.id);
                    if (readback === null)
                        throw new Error("Qdrant insert/readback is unavailable");
                    if (isVerifiedBoundCollision(readback, record))
                        throw new QdrantContentHashCollisionError();
                    if (readback.contentHash !== record.contentHash)
                        throw new Error("Qdrant insert/readback is unavailable");
                    return result;
                },
                retrieve: async (recordType, id) => boundRetrieve(client, recordType, id),
            });
        } });
}
/** Bind an exact expected identity; callers cannot pass an independent allowlist. */
export function bindQdrantDestination(factory, destination) {
    if (typeof factory?.bind !== "function")
        throw new TypeError("Qdrant destination factory is invalid");
    return factory.bind(destination);
}
//# sourceMappingURL=write.js.map