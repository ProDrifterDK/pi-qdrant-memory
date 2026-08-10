import { MemoryClientError } from "../clients/http.js";
import { QdrantReadClient, readPolicy } from "../qdrant/client.js";
import { COLLECTION_METADATA_ID, isCollectionMetadataPayload } from "../qdrant/schema.js";
import type { RuntimeConfig } from "../types.js";

export interface MemoryStatus {
  host: RuntimeConfig["host"];
  configPath: string;
  enabled: boolean;
  autoRecall: boolean;
  destination: {
    endpoint: string;
    collection: string;
    ownerHost: RuntimeConfig["host"];
    schema: "pi-qdrant-memory-v2";
    dimension: 1024;
    distance: "Cosine";
    exists: boolean;
    healthy: boolean;
    keyConfigured: boolean;
  };
  embeddings: {
    endpoint: string;
    model: string;
    dimension: 1024;
    healthy: boolean;
    keyConfigured: boolean;
  };
  capture: {
    enabled: boolean;
    episodeRetentionDays: RuntimeConfig["capture"]["episodeRetentionDays"];
  };
  privacy: {
    egressMode: RuntimeConfig["privacy"]["egressMode"];
    qdrantDestinations: number;
    embeddingDestinations: number;
    llmDestinations: number;
  };
  qdrant: {
    healthy: boolean;
    destinationHealthy: boolean;
    probed: boolean;
  };
}

export interface MemoryStatusDependencies {
  signal?: AbortSignal;
  fetchImpl?: typeof fetch;
}
function isMissing(error: unknown): boolean { return error instanceof MemoryClientError && error.category === "http" && error.status === 404; }

/** Probe only the configured host collection with its collection-scoped key. */
export async function memoryStatus(config: RuntimeConfig, deps: MemoryStatusDependencies = {}): Promise<MemoryStatus> {
  const base: MemoryStatus = {
    host: config.host, configPath: config.configPath, enabled: config.enabled, autoRecall: config.autoRecall,
    destination: { endpoint: config.qdrant.url, collection: config.qdrant.collection, ownerHost: config.host, schema: "pi-qdrant-memory-v2", dimension: 1024, distance: "Cosine", exists: false, healthy: false, keyConfigured: config.qdrant.apiKey !== undefined },
    embeddings: { endpoint: config.embeddings.baseUrl, model: config.embeddings.model, dimension: config.embeddings.dimension, healthy: false, keyConfigured: config.embeddings.apiKey !== undefined },
    capture: { enabled: config.capture.enabled, episodeRetentionDays: config.capture.episodeRetentionDays },
    privacy: { egressMode: config.privacy.egressMode, qdrantDestinations: config.privacy.allowedQdrantDestinations.length, embeddingDestinations: config.privacy.allowedEmbeddingDestinations.length, llmDestinations: config.privacy.allowedLlmDestinations.length },
    qdrant: { healthy: false, destinationHealthy: false, probed: false },
  };
  if (deps.fetchImpl === undefined) return base;
  const client = new QdrantReadClient({ baseUrl: config.qdrant.url, collection: config.qdrant.collection, ownerHost: config.host, ...(config.qdrant.apiKey === undefined ? {} : { apiKey: config.qdrant.apiKey }), timeoutMs: config.retrieval.timeoutMs, fetchImpl: deps.fetchImpl, ...(deps.signal === undefined ? {} : { signal: deps.signal }), readConsistency: config.coordination.readConsistency, maxClockSkewMs: config.coordination.maxClockSkewMs });
  base.qdrant.probed = true;
  try { await client.health(); base.qdrant.healthy = true; } catch { /* status is a bounded diagnostic, not an error channel */ }
  try {
    await client.collectionInfo();
    base.destination.exists = true;
    const metadata = await client.retrieve([COLLECTION_METADATA_ID], readPolicy({ ownerHost: config.host, purpose: "metadata", recordTypes: ["collection_metadata"], maxClockSkewMs: config.coordination.maxClockSkewMs }));
    base.destination.healthy = metadata.length === 1 && isCollectionMetadataPayload(metadata[0]!.payload, config.host);
    base.qdrant.destinationHealthy = base.destination.healthy;
  } catch (error: unknown) {
    if (isMissing(error)) base.destination.exists = false;
  }
  return base;
}
