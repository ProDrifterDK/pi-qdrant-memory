import { MemoryClientError } from "../clients/http.js";
import { readPolicy } from "../qdrant/client.js";
import { statusCollectionInfo, statusHealth, statusRetrieve } from "./transport.js";
import { COLLECTION_METADATA_ID, isCollectionMetadataPayload } from "../qdrant/schema.js";
function isMissing(error) { return error instanceof MemoryClientError && error.category === "http" && error.status === 404; }
/** Probe only the configured host collection with its collection-scoped key. */
export async function memoryStatus(config, deps = {}) {
    const base = {
        host: config.host, configPath: config.configPath, enabled: config.enabled, autoRecall: config.autoRecall,
        destination: { endpoint: config.qdrant.url, collection: config.qdrant.collection, ownerHost: config.host, schema: "pi-qdrant-memory-v2", dimension: 1024, distance: "Cosine", exists: false, healthy: false, keyConfigured: config.qdrant.apiKey !== undefined },
        embeddings: { endpoint: config.embeddings.baseUrl, model: config.embeddings.model, dimension: config.embeddings.dimension, healthy: false, keyConfigured: config.embeddings.apiKey !== undefined },
        capture: { enabled: config.capture.enabled, episodeRetentionDays: config.capture.episodeRetentionDays },
        privacy: { egressMode: config.privacy.egressMode, qdrantDestinations: config.privacy.allowedQdrantDestinations.length, embeddingDestinations: config.privacy.allowedEmbeddingDestinations.length, llmDestinations: config.privacy.allowedLlmDestinations.length },
        qdrant: { healthy: false, destinationHealthy: false, probed: false },
    };
    if (deps.fetchImpl === undefined)
        return base;
    // The read transport is LEXICAL inside admin/transport.ts; status only
    // calls named status operations with validated options.
    const statusOptions = { baseUrl: config.qdrant.url, collection: config.qdrant.collection, ownerHost: config.host, ...(config.qdrant.apiKey === undefined ? {} : { apiKey: config.qdrant.apiKey }), timeoutMs: config.retrieval.timeoutMs, ...(deps.signal === undefined ? {} : { signal: deps.signal }), readConsistency: config.coordination.readConsistency, maxClockSkewMs: config.coordination.maxClockSkewMs };
    base.qdrant.probed = true;
    try {
        await statusHealth(statusOptions, deps.fetchImpl);
        base.qdrant.healthy = true;
    }
    catch { /* status is a bounded diagnostic, not an error channel */ }
    try {
        await statusCollectionInfo(statusOptions, deps.fetchImpl);
        base.destination.exists = true;
        const metadata = await statusRetrieve(statusOptions, deps.fetchImpl, [COLLECTION_METADATA_ID], readPolicy({ ownerHost: config.host, purpose: "metadata", recordTypes: ["collection_metadata"], maxClockSkewMs: config.coordination.maxClockSkewMs }));
        base.destination.healthy = metadata.length === 1 && isCollectionMetadataPayload(metadata[0].payload, config.host);
        base.qdrant.destinationHealthy = base.destination.healthy;
    }
    catch (error) {
        if (isMissing(error))
            base.destination.exists = false;
    }
    return base;
}
//# sourceMappingURL=status.js.map