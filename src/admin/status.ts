import { EmbeddingsClient } from "../clients/embeddings.js";
import { MemoryClientError } from "../clients/http.js";
import { ReadonlyQdrantClient } from "../clients/qdrant-readonly.js";
import type { RuntimeConfig } from "../types.js";
import { AdminQdrantClient, type AdminCollectionInfo } from "./qdrant-admin.js";

interface Dependencies {
  fetchImpl?: typeof fetch;
  signal?: AbortSignal;
}

export interface MemoryStatus {
  destinationExists: boolean;
  destination: {
    endpoint: string;
    collection: string;
    exists: boolean;
    dimension: number | null;
    distance: string | null;
    pointCount: number | null;
    healthy: boolean;
    keyConfigured: boolean;
  };
  source: {
    endpoint: string;
    collection: string;
    exists: boolean;
    dimension: number | null;
    distance: string | null;
    pointCount: number | null;
    healthy: boolean;
    keyConfigured: boolean;
  };
  embeddings: {
    endpoint: string;
    model: string;
    dimension: number;
    healthy: boolean;
    keyConfigured: boolean;
  };
  qdrant: {
    healthy: boolean;
    destinationHealthy: boolean;
    sourceHealthy: boolean;
  };
}

function endpointOrigin(value: string): string {
  try {
    return new URL(value).origin;
  } catch {
    throw new MemoryClientError("configuration", "Configured endpoint is invalid");
  }
}

function isMissing(error: unknown): boolean {
  return error instanceof MemoryClientError && error.category === "http" && error.status === 404;
}

function makeInfoClient(
  url: string,
  apiKey: string | undefined,
  config: RuntimeConfig,
  deps: Dependencies,
): AdminQdrantClient {
  return new AdminQdrantClient({
    baseUrl: url,
    timeoutMs: config.retrieval.timeoutMs,
    ...(apiKey === undefined ? {} : { apiKey }),
    ...(deps.fetchImpl === undefined ? {} : { fetchImpl: deps.fetchImpl }),
  });
}

function makeHealthClient(
  url: string,
  collection: string,
  apiKey: string | undefined,
  config: RuntimeConfig,
  deps: Dependencies,
): ReadonlyQdrantClient {
  return new ReadonlyQdrantClient({
    baseUrl: url,
    collection,
    timeoutMs: config.retrieval.timeoutMs,
    ...(apiKey === undefined ? {} : { apiKey }),
    ...(deps.fetchImpl === undefined ? {} : { fetchImpl: deps.fetchImpl }),
  });
}

async function health(client: ReadonlyQdrantClient, signal: AbortSignal | undefined): Promise<boolean> {
  try {
    await client.health(signal);
    return true;
  } catch {
    return false;
  }
}

function statusCollection(
  endpoint: string,
  collection: string,
  keyConfigured: boolean,
  info: AdminCollectionInfo | undefined,
  healthy: boolean,
): MemoryStatus["destination"] {
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

export async function memoryStatus(config: RuntimeConfig, deps: Dependencies = {}): Promise<MemoryStatus> {
  const destinationEndpoint = endpointOrigin(config.qdrant.url);
  const sourceEndpoint = endpointOrigin(config.admin.source.url);
  const embeddingsEndpoint = endpointOrigin(config.embeddings.baseUrl);
  const destinationInfoClient = makeInfoClient(
    config.qdrant.url,
    config.admin.destinationApiKey,
    config,
    deps,
  );
  const sourceInfoClient = makeInfoClient(
    config.admin.source.url,
    config.admin.source.apiKey,
    config,
    deps,
  );
  const destinationHealthClient = makeHealthClient(
    config.qdrant.url,
    config.qdrant.collection,
    config.admin.destinationApiKey,
    config,
    deps,
  );
  const sourceHealthClient = makeHealthClient(
    config.admin.source.url,
    config.admin.source.collection,
    config.admin.source.apiKey,
    config,
    deps,
  );
  const embeddingsClient = new EmbeddingsClient({
    baseUrl: config.embeddings.baseUrl,
    model: config.embeddings.model,
    dimension: config.embeddings.dimension,
    queryPrefix: config.embeddings.queryPrefix,
    timeoutMs: config.retrieval.timeoutMs,
    ...(config.embeddings.apiKey === undefined ? {} : { apiKey: config.embeddings.apiKey }),
    ...(deps.fetchImpl === undefined ? {} : { fetchImpl: deps.fetchImpl }),
  });

  let destinationInfo: AdminCollectionInfo | undefined;
  try {
    destinationInfo = await destinationInfoClient.collectionInfo(config.qdrant.collection, deps.signal);
  } catch (error: unknown) {
    if (!isMissing(error)) throw error;
  }
  const sourceInfo = await sourceInfoClient.collectionInfo(config.admin.source.collection, deps.signal);
  const [destinationHealthy, sourceHealthy, embeddingsHealthy] = await Promise.all([
    health(destinationHealthClient, deps.signal),
    health(sourceHealthClient, deps.signal),
    embeddingsClient.embedQuery("pi-qdrant-memory health probe").then(() => true).catch(() => false),
  ]);

  const destination = statusCollection(
    destinationEndpoint,
    config.qdrant.collection,
    config.admin.destinationApiKey !== undefined,
    destinationInfo,
    destinationHealthy,
  );
  const source: MemoryStatus["source"] = {
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
