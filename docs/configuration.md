# Configuration reference

**Implementation status:** Task 1 provides a transitional loader and the complete v2 configuration shape. Capture, durable delivery, collection initialization, policy enforcement, curation, RAPTOR, privacy barriers, and forget operations remain locked target behavior for later tasks.

Configuration is read only from:

```text
${XDG_CONFIG_HOME:-$HOME/.config}/pi-qdrant-memory/config.json
```

Only an absent file is ignored. Invalid JSON, unknown file fields, file credentials, retired fields, URL credentials, unknown environment settings, and invalid values fail closed. Repository files are never read as configuration or authorization.

## Precedence and host collections

The precedence is allowlisted environment (for operational overrides), active host section, shared section, then compiled default. `autoRecall` accepts its allowlisted environment override; `enabled` is host > shared > compiled default because it has no environment suffix. The default Qdrant collection is `pi_memory` for Pi and `prime_memory` for Prime. The loader currently returns the fixed embedding dimension 1024; any other effective file or environment value is rejected.

The following shape is the **approved target configuration contract**. Task 1 validates and returns this shape, but does not activate the target capture/write behavior:

```json
{
  "enabled": true,
  "autoRecall": true,
  "qdrant": { "url": "http://127.0.0.1:6333", "collection": "pi_memory", "replicationFactor": 1, "writeConsistencyFactor": 1 },
  "embeddings": { "baseUrl": "http://127.0.0.1:8080/v1", "model": "bge-m3", "dimension": 1024, "queryPrefix": "search_query: " },
  "retrieval": { "topK": 5, "candidatesPerLane": 20, "minScore": 0.35, "projectBoost": 0.05, "contextBudgetChars": 1200, "toolResultBudgetChars": 8000, "hardContextCharBudget": 16000, "timeoutMs": 2500, "rootScope": "project", "childSearch": true },
  "projects": { "registrations": {} },
  "capture": { "enabled": false, "projectAllowlist": [], "projectDenylist": [], "episodeRetentionDays": "indefinite", "toolArgsChars": 2000, "toolResultChars": 4000 },
  "privacy": { "egressMode": "local_only", "allowedQdrantDestinations": [], "allowedEmbeddingDestinations": [], "allowedLlmDestinations": [], "allowActiveModelFallback": false, "allowCrossProviderReplay": false },
  "coordination": { "maxClockSkewMs": 300000, "readConsistency": 1, "leaseMs": 30000, "reconcileIntervalMs": 900000 },
  "outbox": { "maxJobs": 10000, "maxBytes": 268435456, "retryBaseMs": 500, "retryMaxMs": 30000, "sharedFilesystem": false },
  "curation": { "turnTrigger": 10, "toolTrigger": 15, "maxInputTokens": 12000 },
  "memoryModel": { "timeoutMs": 30000, "maxOutputTokens": 2048 },
  "raptor": { "rebuildEpisodeDelta": 64, "maxLevels": 5, "summaryInputTokens": 12000, "umapDimensions": 10, "localNeighbors": 10, "gmmMaxClusters": 50, "membershipThreshold": 0.1 }
}
```

## Environment names

Operational suffixes are the documented endpoint, model, retrieval, boolean, and fixed-dimension settings under `PI_QDRANT_MEMORY_`: `QDRANT_URL`, `QDRANT_COLLECTION`, `EMBEDDING_BASE_URL`, `EMBEDDING_MODEL`, `EMBEDDING_DIMENSION`, `AUTO_RECALL`, `TOP_K`, `CANDIDATES_PER_LANE`, `MIN_SCORE`, `PROJECT_BOOST`, `CONTEXT_BUDGET_CHARS`, `TOOL_RESULT_BUDGET_CHARS`, and `TIMEOUT_MS`. The dimension value must be exactly `1024`; the hard context ceiling is exactly `16000`.

The exact credential names are `PI_QDRANT_MEMORY_QDRANT_API_KEY`, `PI_QDRANT_MEMORY_EMBEDDING_API_KEY`, and `PI_QDRANT_MEMORY_ADMIN_QDRANT_API_KEY`. Runtime configuration consumes only the first two. The administrative value is consumed only by human CLI code and is absent from `RuntimeConfig`.

## Later target controls

The target contract requires explicit retention and egress disclosure before capture, operator project registration, redaction/scanning before disk or network, policy-bound destinations, durable at-least-once delivery, immutable records, and human-only privacy operations. Task 1 only validates their transitional fields and does not perform these operations.
