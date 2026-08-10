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
    probed: false;
  };
}

export interface MemoryStatusDependencies {
  signal?: AbortSignal;
}

/** Return the destination-only v2 status contract without touching services. */
export async function memoryStatus(
  config: RuntimeConfig,
  _deps: MemoryStatusDependencies = {},
): Promise<MemoryStatus> {
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
