/** Terminal only when a present exact-ID Qdrant record proves a different canonical hash. */
export const QDRANT_CONTENT_HASH_COLLISION = "qdrant_content_hash_collision";
export class QdrantContentHashCollisionError extends Error {
    code = QDRANT_CONTENT_HASH_COLLISION;
    constructor(message = "Qdrant content-hash collision") { super(message); this.name = "QdrantContentHashCollisionError"; }
}
//# sourceMappingURL=qdrant-errors.js.map