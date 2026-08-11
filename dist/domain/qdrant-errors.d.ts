/** Terminal only when a present exact-ID Qdrant record proves a different canonical hash. */
export declare const QDRANT_CONTENT_HASH_COLLISION: "qdrant_content_hash_collision";
export declare class QdrantContentHashCollisionError extends Error {
    readonly code: "qdrant_content_hash_collision";
    constructor(message?: string);
}
