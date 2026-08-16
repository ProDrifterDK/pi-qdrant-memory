import { canonicalStringify, deterministicUuid, sha256Hex } from "../domain/canonical.js";
import { canonicalRecordHash, parseMemoryRecord } from "../domain/records.js";
export const V2_COLLECTION_METADATA = {
    schema: "pi-qdrant-memory-v2",
    schema_revision: 1,
    dense_vector: "semantic",
    embedding_model: "bge-m3",
    embedding_dimension: 1024,
    // Qdrant 1.17 L2-normalizes Cosine vectors on write with CPU-dependent f32
    // accumulation, so exact vector readback (and thus the vector-bound content
    // hash) is impossible under Cosine. Every embedding is canonicalized at the
    // single embedding boundary (L2-normalized, shortest-f32 components), where
    // Dot on normalized vectors is exactly Cosine and round-trips byte-exact.
    distance: "Dot",
};
export const V2_CONTRACT_HASH = sha256Hex(canonicalStringify(V2_COLLECTION_METADATA));
export const REQUIRED_INDEXES = [
    ["record_type", "keyword"], ["owner_host", "keyword"], ["project_id", "keyword"], ["project_identity_kind", "keyword"], ["scope", "keyword"], ["status", "keyword"], ["resolution", "keyword"], ["state_key", "keyword"], ["content_id", "keyword"], ["observation_id", "keyword"], ["session_id", "keyword"], ["turn_id", "keyword"], ["agent_role", "keyword"], ["generation_id", "keyword"], ["job_id", "keyword"], ["category", "keyword"], ["tool_name", "keyword"], ["error_fingerprint", "keyword"], ["secret_scan", "keyword"], ["event_at", "datetime"], ["effective_at", "datetime"], ["created_at", "datetime"], ["lease_expires_at", "datetime"], ["expires_at", "datetime"], ["privacy_epoch", "integer"], ["coordination_policy_epoch", "integer"], ["version", "integer"], ["fencing_token", "integer"], ["level", "integer"], ["accepted_proposal_id", "keyword"], ["text", "text"],
];
export const COLLECTION_METADATA_ID = deterministicUuid("pi-qdrant-memory-v2", "collection_metadata");
export const COLLECTION_CONTROL_ID = deterministicUuid("pi-qdrant-memory-v2", "collection_control");
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/iu;
export function isPhysicalPointId(value) { return typeof value === "string" && UUID.test(value); }
/** Qdrant point IDs are UUIDs. Logical IDs remain in payload and are domain-mapped when needed. */
export function physicalPointId(recordType, logicalId) {
    if (typeof recordType !== "string" || recordType.length === 0 || typeof logicalId !== "string" || logicalId.length === 0)
        throw new TypeError("Point identity is invalid");
    return isPhysicalPointId(logicalId) ? logicalId : deterministicUuid("pi-qdrant-memory-v2:point", recordType, logicalId);
}
export function collectionMetadataPayload(ownerHost, contractHash = V2_CONTRACT_HASH) {
    if (ownerHost !== "pi" && ownerHost !== "prime")
        throw new TypeError("Metadata owner host is invalid");
    if (!/^[a-f0-9]{64}$/u.test(contractHash))
        throw new TypeError("Metadata contract hash is invalid");
    return { record_type: "collection_metadata", owner_host: ownerHost, schema: V2_COLLECTION_METADATA.schema, schema_revision: 1, dense_vector: "semantic", embedding_model: "bge-m3", embedding_dimension: 1024, distance: "Dot", contract_hash: contractHash, status: "active", secret_scan: "passed" };
}
export function collectionMetadataPoint(ownerHost, contractHash = V2_CONTRACT_HASH) { return { id: COLLECTION_METADATA_ID, payload: collectionMetadataPayload(ownerHost, contractHash), vector: {} }; }
/** Control payload is intentionally point-only; no Qdrant collection metadata bag is used. */
export function controlPayload(control) {
    return { record_type: "collection_control", id: control.id, owner_host: control.ownerHost, schema_revision: control.schemaRevision, created_at: control.createdAt, privacy_epoch: control.privacyEpoch, processing_policy_id: control.processingPolicyId, expires_at: control.expiresAt, content_hash: control.contentHash, version: control.version, active_generation: control.activeGeneration, active_base_generation: control.activeBaseGeneration, coordination_policy_epoch: control.coordinationPolicyEpoch, coordination_policy_hash: control.coordinationPolicyHash, state: control.state, status: "active", secret_scan: "passed", scan_cursor: control.scanCursor, last_forget_barrier: control.lastForgetBarrier, revoked_destination_ids: [...control.revokedDestinationIds] };
}
export function collectionControlPoint(control) {
    if (control.id !== COLLECTION_CONTROL_ID)
        throw new TypeError("Collection control ID is invalid");
    return { id: COLLECTION_CONTROL_ID, payload: controlPayload(control), vector: {} };
}
/** Strict bootstrap control validation shared by init, admin insertion and write helper. */
export function bootstrapControlHash(control) {
    const copy = { ...control };
    delete copy.contentHash;
    delete copy.createdAt;
    delete copy.vector;
    delete copy.producerId;
    delete copy.nodeId;
    return sha256Hex(canonicalStringify(copy));
}
export function assertBootstrapControl(control, ownerHost) {
    try {
        parseMemoryRecord({ ...control, version: 1 }, { ownerHost });
    }
    catch {
        throw new TypeError("Invalid version-0 bootstrap control");
    }
    if (control.id !== COLLECTION_CONTROL_ID || control.recordType !== "collection_control" || control.ownerHost !== ownerHost || control.schemaRevision !== 1 || control.version !== 0 || control.privacyEpoch !== 0 || control.activeGeneration !== null || control.activeBaseGeneration !== null || control.state !== "active" || control.scanCursor !== null || control.lastForgetBarrier !== null || control.expiresAt !== null || !Array.isArray(control.revokedDestinationIds) || control.revokedDestinationIds.length !== 0 || control.processingPolicyId !== V2_CONTRACT_HASH || control.coordinationPolicyHash !== V2_CONTRACT_HASH || !Number.isSafeInteger(control.coordinationPolicyEpoch) || control.coordinationPolicyEpoch !== 0 || control.contentHash !== bootstrapControlHash(control))
        throw new TypeError("Invalid version-0 bootstrap control");
}
function exactObject(value) { return typeof value === "object" && value !== null && !Array.isArray(value); }
const CONTROL_PAYLOAD_KEYS = Object.keys(controlPayload({ ownerHost: "pi", schemaRevision: 1, createdAt: "1970-01-01T00:00:00.000Z", privacyEpoch: 0, processingPolicyId: "placeholder", expiresAt: null, recordType: "collection_control", id: COLLECTION_CONTROL_ID, version: 1, activeGeneration: null, activeBaseGeneration: null, coordinationPolicyEpoch: 0, coordinationPolicyHash: "placeholder", state: "active", scanCursor: null, lastForgetBarrier: null, revokedDestinationIds: [], contentHash: "placeholder" })).sort();
function exactControlPayload(value) { const keys = Object.keys(value).sort(); return keys.length === CONTROL_PAYLOAD_KEYS.length && keys.every((key, index) => key === CONTROL_PAYLOAD_KEYS[index]); }
/** Convert and strictly validate a Qdrant control payload. Version 0 is accepted only through bootstrap validation. */
export function controlRecordFromPayload(value, ownerHost) {
    if (!exactObject(value) || !exactControlPayload(value) || value.record_type !== "collection_control" || value.id !== COLLECTION_CONTROL_ID || value.owner_host !== ownerHost || value.status !== "active" || value.secret_scan !== "passed")
        throw new TypeError("Invalid collection control payload");
    const control = { ownerHost, schemaRevision: value.schema_revision, createdAt: value.created_at, privacyEpoch: value.privacy_epoch, processingPolicyId: value.processing_policy_id, expiresAt: value.expires_at, recordType: "collection_control", id: COLLECTION_CONTROL_ID, version: value.version, activeGeneration: value.active_generation, activeBaseGeneration: value.active_base_generation, coordinationPolicyEpoch: value.coordination_policy_epoch, coordinationPolicyHash: value.coordination_policy_hash, state: value.state, scanCursor: value.scan_cursor, lastForgetBarrier: value.last_forget_barrier, revokedDestinationIds: [...value.revoked_destination_ids], contentHash: value.content_hash };
    if (control.version === 0) {
        parseMemoryRecord({ ...control, version: 1 }, { ownerHost });
        assertBootstrapControl(control, ownerHost);
        return control;
    }
    parseMemoryRecord(control, { ownerHost });
    if (control.contentHash !== canonicalRecordHash(control))
        throw new TypeError("Control payload canonical hash mismatch");
    return control;
}
export function isBootstrapControlPayload(value, control, ownerHost) {
    if (!exactObject(value) || value.owner_host !== ownerHost)
        return false;
    try {
        const expected = controlPayload(control);
        const keys = Object.keys(value).sort();
        const expectedKeys = Object.keys(expected).sort();
        return keys.length === expectedKeys.length && keys.every((key, index) => key === expectedKeys[index] && canonicalStringify(value[key]) === canonicalStringify(expected[key]));
    }
    catch {
        return false;
    }
}
export function isValidBootstrapControlPayload(value, ownerHost) { try {
    return controlRecordFromPayload(value, ownerHost).version === 0;
}
catch {
    return false;
} }
export function isCollectionMetadataPayload(value, ownerHost, contractHash = V2_CONTRACT_HASH) {
    if (!exactObject(value))
        return false;
    try {
        const expected = collectionMetadataPayload(ownerHost, contractHash);
        const keys = Object.keys(value).sort();
        const expectedKeys = Object.keys(expected).sort();
        return keys.length === expectedKeys.length && keys.every((key, index) => key === expectedKeys[index] && canonicalStringify(value[key]) === canonicalStringify(expected[key]));
    }
    catch {
        return false;
    }
}
export function collectionVectors() { return { semantic: { size: 1024, distance: "Dot" } }; }
//# sourceMappingURL=schema.js.map