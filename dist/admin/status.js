/** Return the destination-only v2 status contract without touching services. */
export async function memoryStatus(config, _deps = {}) {
    return {
        host: config.host,
        configPath: config.configPath,
        enabled: config.enabled,
        autoRecall: config.autoRecall,
        destination: {
            endpoint: config.qdrant.url,
            collection: config.qdrant.collection,
            ownerHost: config.host,
            schema: "pi-qdrant-memory-v2",
            dimension: config.embeddings.dimension,
            distance: "Cosine",
            exists: false,
            healthy: false,
            keyConfigured: config.qdrant.apiKey !== undefined,
        },
        embeddings: {
            endpoint: config.embeddings.baseUrl,
            model: config.embeddings.model,
            dimension: config.embeddings.dimension,
            healthy: false,
            keyConfigured: config.embeddings.apiKey !== undefined,
        },
        capture: {
            enabled: config.capture.enabled,
            episodeRetentionDays: config.capture.episodeRetentionDays,
        },
        privacy: {
            egressMode: config.privacy.egressMode,
            qdrantDestinations: config.privacy.allowedQdrantDestinations.length,
            embeddingDestinations: config.privacy.allowedEmbeddingDestinations.length,
            llmDestinations: config.privacy.allowedLlmDestinations.length,
        },
        qdrant: { healthy: false, destinationHealthy: false, probed: false },
    };
}
//# sourceMappingURL=status.js.map