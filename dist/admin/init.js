/**
 * Task 1 deliberately exposes a destination-only contract shell. Network
 * initialization and collection mutation are owned by the Qdrant task.
 */
export async function initializeDestination(config, _deps = {}) {
    return {
        host: config.host,
        collection: config.qdrant.collection,
        ownerHost: config.host,
        schema: "pi-qdrant-memory-v2",
        schemaRevision: 1,
        vector: {
            name: "semantic",
            model: config.embeddings.model,
            dimension: config.embeddings.dimension,
            distance: "Cosine",
        },
        capture: {
            enabled: config.capture.enabled,
            episodeRetentionDays: config.capture.episodeRetentionDays,
        },
        initialized: false,
    };
}
//# sourceMappingURL=init.js.map