/** Terminal only when a present exact-ID Qdrant record proves a different canonical hash. */
export declare const QDRANT_CONTENT_HASH_COLLISION: "qdrant_content_hash_collision";
export declare class QdrantContentHashCollisionError extends Error {
    readonly code: "qdrant_content_hash_collision";
    constructor(message?: string);
}
/**
 * Narrow INTERNAL classifier error: a present exact-ID Episode point whose
 * stored contentHash is valid ONLY under the former vector-excluding formula
 * (vector present, hash computed without it). Such a point is a verified
 * legacy-incompatible collision: the terminal content-hash-collision category,
 * never an ambiguous null that would loop pending forever. No migration/live
 * rewrite happens in Task 8.
 */
export declare const QDRANT_LEGACY_EPISODE_HASH: "qdrant_legacy_episode_hash";
export declare class QdrantLegacyEpisodeHashError extends Error {
    readonly code: "qdrant_legacy_episode_hash";
    constructor(message?: string);
}
