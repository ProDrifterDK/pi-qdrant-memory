import { type ControlRecord } from "../domain/records.js";
import type { HostId } from "../types.js";
export declare const V2_COLLECTION_METADATA: {
    readonly schema: "pi-qdrant-memory-v2";
    readonly schema_revision: 1;
    readonly dense_vector: "semantic";
    readonly embedding_model: "bge-m3";
    readonly embedding_dimension: 1024;
    readonly distance: "Dot";
};
export declare const V2_CONTRACT_HASH: string;
export type PayloadIndexSchema = "keyword" | "integer" | "datetime" | "text";
export declare const REQUIRED_INDEXES: readonly [readonly ["record_type", "keyword"], readonly ["owner_host", "keyword"], readonly ["project_id", "keyword"], readonly ["project_identity_kind", "keyword"], readonly ["scope", "keyword"], readonly ["status", "keyword"], readonly ["resolution", "keyword"], readonly ["state_key", "keyword"], readonly ["content_id", "keyword"], readonly ["observation_id", "keyword"], readonly ["session_id", "keyword"], readonly ["turn_id", "keyword"], readonly ["agent_role", "keyword"], readonly ["generation_id", "keyword"], readonly ["job_id", "keyword"], readonly ["category", "keyword"], readonly ["tool_name", "keyword"], readonly ["error_fingerprint", "keyword"], readonly ["secret_scan", "keyword"], readonly ["event_at", "datetime"], readonly ["effective_at", "datetime"], readonly ["created_at", "datetime"], readonly ["lease_expires_at", "datetime"], readonly ["expires_at", "datetime"], readonly ["privacy_epoch", "integer"], readonly ["coordination_policy_epoch", "integer"], readonly ["version", "integer"], readonly ["fencing_token", "integer"], readonly ["level", "integer"], readonly ["accepted_proposal_id", "keyword"], readonly ["text", "text"]];
export declare const COLLECTION_METADATA_ID: string;
export declare const COLLECTION_CONTROL_ID: string;
export type PointRecordType = "episode" | "curated_memory" | "curated_current" | "conflict_manifest" | "raptor_summary" | "collection_control" | "processing_policy" | "job" | "lease" | "proposal" | "coverage" | "evidence_link" | "tombstone" | "collection_metadata";
export declare function isPhysicalPointId(value: unknown): value is string;
/** Qdrant point IDs are UUIDs. Logical IDs remain in payload and are domain-mapped when needed. */
export declare function physicalPointId(recordType: string, logicalId: string): string;
export interface CollectionMetadataPayload {
    record_type: "collection_metadata";
    owner_host: HostId;
    schema: typeof V2_COLLECTION_METADATA.schema;
    schema_revision: 1;
    dense_vector: typeof V2_COLLECTION_METADATA.dense_vector;
    embedding_model: typeof V2_COLLECTION_METADATA.embedding_model;
    embedding_dimension: 1024;
    distance: "Dot";
    contract_hash: string;
    status: "active";
    secret_scan: "passed";
}
export declare function collectionMetadataPayload(ownerHost: HostId, contractHash?: string): CollectionMetadataPayload;
export declare function collectionMetadataPoint(ownerHost: HostId, contractHash?: string): {
    id: string;
    payload: CollectionMetadataPayload;
    vector: Record<string, never>;
};
/** Control payload is intentionally point-only; no Qdrant collection metadata bag is used. */
export declare function controlPayload(control: ControlRecord): Record<string, unknown>;
export declare function collectionControlPoint(control: ControlRecord): {
    id: string;
    payload: Record<string, unknown>;
    vector: Record<string, never>;
};
/** Strict bootstrap control validation shared by init, admin insertion and write helper. */
export declare function bootstrapControlHash(control: ControlRecord): string;
export declare function assertBootstrapControl(control: ControlRecord, ownerHost: HostId): void;
/** Convert and strictly validate a Qdrant control payload. Version 0 is accepted only through bootstrap validation. */
export declare function controlRecordFromPayload(value: unknown, ownerHost: HostId): ControlRecord;
export declare function isBootstrapControlPayload(value: unknown, control: ControlRecord, ownerHost: HostId): boolean;
export declare function isValidBootstrapControlPayload(value: unknown, ownerHost: HostId): boolean;
export declare function isCollectionMetadataPayload(value: unknown, ownerHost: HostId, contractHash?: string): value is CollectionMetadataPayload;
export declare function collectionVectors(): {
    semantic: {
        size: 1024;
        distance: "Dot";
    };
};
