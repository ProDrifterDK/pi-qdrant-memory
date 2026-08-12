/** Terminal only when a present exact-ID Qdrant record proves a different canonical hash. */
export const QDRANT_CONTENT_HASH_COLLISION = "qdrant_content_hash_collision";
export class QdrantContentHashCollisionError extends Error {
    code = QDRANT_CONTENT_HASH_COLLISION;
    constructor(message = "Qdrant content-hash collision") { super(message); this.name = "QdrantContentHashCollisionError"; }
}
// Frozen class + prototype: Symbol.hasInstance monkeypatching cannot
// reclassify ambiguous failures as terminal (the instanceof checks used by the
// ingest processor and the bound destination stay nonforgeable).
Object.freeze(QdrantContentHashCollisionError);
Object.freeze(QdrantContentHashCollisionError.prototype);
/**
 * Narrow INTERNAL classifier error: a present exact-ID Episode point whose
 * stored contentHash is valid ONLY under the former vector-excluding formula
 * (vector present, hash computed without it). Such a point is a verified
 * legacy-incompatible collision: the terminal content-hash-collision category,
 * never an ambiguous null that would loop pending forever. No migration/live
 * rewrite happens in Task 8.
 */
export const QDRANT_LEGACY_EPISODE_HASH = "qdrant_legacy_episode_hash";
export class QdrantLegacyEpisodeHashError extends Error {
    code = QDRANT_LEGACY_EPISODE_HASH;
    constructor(message = "Qdrant legacy episode hash (vector-excluding formula)") { super(message); this.name = "QdrantLegacyEpisodeHashError"; }
}
// Frozen class + prototype (non-monkeypatchable brand predicate).
Object.freeze(QdrantLegacyEpisodeHashError);
Object.freeze(QdrantLegacyEpisodeHashError.prototype);
//# sourceMappingURL=qdrant-errors.js.map