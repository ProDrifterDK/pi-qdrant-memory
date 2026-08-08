import { EmbeddingsClient } from "../clients/embeddings.js";
import { MemoryClientError } from "../clients/http.js";
import { ReadonlyQdrantClient } from "../clients/qdrant-readonly.js";
import { AdminQdrantClient } from "./qdrant-admin.js";
function endpointOrigin(value) {
    try {
        return new URL(value).origin;
    }
    catch {
        throw new MemoryClientError("configuration", "Configured endpoint is invalid");
    }
}
function isMissing(error) {
    return error instanceof MemoryClientError && error.category === "http" && error.status === 404;
}
function makeInfoClient(url, apiKey, config, deps) {
    return new AdminQdrantClient({
        baseUrl: url,
        timeoutMs: config.retrieval.timeoutMs,
        ...(apiKey === undefined ? {} : { apiKey }),
        ...(deps.fetchImpl === undefined ? {} : { fetchImpl: deps.fetchImpl }),
    });
}
function makeHealthClient(url, collection, apiKey, config, deps) {
    return new ReadonlyQdrantClient({
        baseUrl: url,
        collection,
        timeoutMs: config.retrieval.timeoutMs,
        ...(apiKey === undefined ? {} : { apiKey }),
        ...(deps.fetchImpl === undefined ? {} : { fetchImpl: deps.fetchImpl }),
    });
}
async function health(client, signal) {
    try {
        await client.health(signal);
        return true;
    }
    catch {
        return false;
    }
}
function statusCollection(endpoint, collection, keyConfigured, info, healthy) {
    return {
        endpoint,
        collection,
        exists: info !== undefined,
        dimension: info?.dimension ?? null,
        distance: info?.distance ?? null,
        pointCount: info?.pointCount ?? null,
        healthy,
        keyConfigured,
    };
}
export async function memoryStatus(config, deps = {}) {
    const destinationEndpoint = endpointOrigin(config.qdrant.url);
    const sourceEndpoint = endpointOrigin(config.admin.source.url);
    const embeddingsEndpoint = endpointOrigin(config.embeddings.baseUrl);
    const destinationInfoClient = makeInfoClient(config.qdrant.url, config.admin.destinationApiKey, config, deps);
    const sourceInfoClient = makeInfoClient(config.admin.source.url, config.admin.source.apiKey, config, deps);
    const destinationHealthClient = makeHealthClient(config.qdrant.url, config.qdrant.collection, config.admin.destinationApiKey, config, deps);
    const sourceHealthClient = makeHealthClient(config.admin.source.url, config.admin.source.collection, config.admin.source.apiKey, config, deps);
    const embeddingsClient = new EmbeddingsClient({
        baseUrl: config.embeddings.baseUrl,
        model: config.embeddings.model,
        dimension: config.embeddings.dimension,
        queryPrefix: config.embeddings.queryPrefix,
        timeoutMs: config.retrieval.timeoutMs,
        ...(config.embeddings.apiKey === undefined ? {} : { apiKey: config.embeddings.apiKey }),
        ...(deps.fetchImpl === undefined ? {} : { fetchImpl: deps.fetchImpl }),
    });
    let destinationInfo;
    try {
        destinationInfo = await destinationInfoClient.collectionInfo(config.qdrant.collection, deps.signal);
    }
    catch (error) {
        if (!isMissing(error))
            throw error;
    }
    const sourceInfo = await sourceInfoClient.collectionInfo(config.admin.source.collection, deps.signal);
    const [destinationHealthy, sourceHealthy, embeddingsHealthy] = await Promise.all([
        health(destinationHealthClient, deps.signal),
        health(sourceHealthClient, deps.signal),
        embeddingsClient.embedQuery("pi-qdrant-memory health probe").then(() => true).catch(() => false),
    ]);
    const destination = statusCollection(destinationEndpoint, config.qdrant.collection, config.admin.destinationApiKey !== undefined, destinationInfo, destinationHealthy);
    const source = {
        endpoint: sourceEndpoint,
        collection: config.admin.source.collection,
        exists: true,
        dimension: sourceInfo.dimension,
        distance: sourceInfo.distance,
        pointCount: sourceInfo.pointCount,
        healthy: sourceHealthy,
        keyConfigured: config.admin.source.apiKey !== undefined,
    };
    return {
        destinationExists: destination.exists,
        destination,
        source,
        embeddings: {
            endpoint: embeddingsEndpoint,
            model: config.embeddings.model,
            dimension: config.embeddings.dimension,
            healthy: embeddingsHealthy,
            keyConfigured: config.embeddings.apiKey !== undefined,
        },
        qdrant: {
            healthy: destinationHealthy && sourceHealthy,
            destinationHealthy,
            sourceHealthy,
        },
    };
}
//# sourceMappingURL=status.js.map