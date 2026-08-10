import { describe, expect, it } from "vitest";
import { initializeDestination } from "../../src/admin/init.js";
import { memoryStatus } from "../../src/admin/status.js";
import type { RuntimeConfig } from "../../src/types.js";

function config(): RuntimeConfig {
  return {
    host: "pi", enabled: true, autoRecall: true, configPath: "/tmp/config.json",
    qdrant: { url: "http://destination", collection: "pi_memory", apiKey: "runtime-secret", replicationFactor: 1, writeConsistencyFactor: 1 },
    embeddings: { baseUrl: "http://embeddings/v1", model: "bge-m3", dimension: 1024, queryPrefix: "search_query: ", apiKey: "embedding-secret" },
    retrieval: { topK: 5, candidatesPerLane: 20, minScore: 0.35, projectBoost: 0.05, contextBudgetChars: 1200, toolResultBudgetChars: 8000, hardContextCharBudget: 16000, timeoutMs: 2500, rootScope: "project", childSearch: true },
    projects: { registrations: {} },
    capture: { enabled: false, projectAllowlist: [], projectDenylist: [], episodeRetentionDays: "indefinite", toolArgsChars: 2000, toolResultChars: 4000 },
    privacy: { egressMode: "local_only", allowedQdrantDestinations: [], allowedEmbeddingDestinations: [], allowedLlmDestinations: [], allowActiveModelFallback: false, allowCrossProviderReplay: false },
    coordination: { maxClockSkewMs: 300000, readConsistency: 1, leaseMs: 30000, reconcileIntervalMs: 900000 },
    outbox: { maxJobs: 10000, maxBytes: 268435456, retryBaseMs: 500, retryMaxMs: 30000, sharedFilesystem: false },
    curation: { turnTrigger: 10, toolTrigger: 15, maxInputTokens: 12000 },
    memoryModel: { timeoutMs: 30000, maxOutputTokens: 2048 },
    raptor: { rebuildEpisodeDelta: 64, maxLevels: 5, summaryInputTokens: 12000, umapDimensions: 10, localNeighbors: 10, gmmMaxClusters: 50, membershipThreshold: 0.1 },
  };
}

describe("destination-only v2 admin shell", () => {
  it("returns immutable destination contract details without a network call", async () => {
    await expect(initializeDestination(config())).resolves.toMatchObject({ host: "pi", ownerHost: "pi", collection: "pi_memory", schema: "pi-qdrant-memory-v2", schemaRevision: 1, vector: { name: "semantic", model: "bge-m3", dimension: 1024, distance: "Cosine" }, initialized: false });
  });

  it("reports destination and policy state without a second collection", async () => {
    const result = await memoryStatus(config());
    expect(result.destination).toMatchObject({ endpoint: "http://destination", collection: "pi_memory", ownerHost: "pi", schema: "pi-qdrant-memory-v2", exists: false, healthy: false, keyConfigured: true });
    expect(result).not.toHaveProperty("source");
    expect(result.capture.enabled).toBe(false);
    expect(result.privacy.egressMode).toBe("local_only");
    expect(result.qdrant.probed).toBe(false);
  });
});
