# Configuration reference

Configuration is read only from:

```text
${XDG_CONFIG_HOME:-$HOME/.config}/pi-qdrant-memory/config.json
```

Only a missing file is ignored. Invalid JSON, unknown fields, file credentials, URL credentials, retired fields, unknown `PI_QDRANT_MEMORY_` names, and invalid values fail closed. The precedence order is an allowlisted operational environment override, active-host section (`pi` or `prime`), shared root section, then compiled default.

## Host isolation

The defaults are `pi_memory` for Pi and `prime_memory` for Prime. If both hosts are enabled, resolving to the same endpoint and collection is rejected. Host sections may override shared fields:

```json
{
  "pi": { "qdrant": { "url": "http://127.0.0.1:6333", "collection": "pi_memory" } },
  "prime": { "qdrant": { "url": "http://127.0.0.1:6333", "collection": "prime_memory" } }
}
```

Set `PI_QDRANT_MEMORY_HOST` to `pi` or `prime` for human CLI commands. It selects a host; it is not part of `RuntimeConfig` and does not override extension host detection.

## Safe shared baseline

Capture remains disabled until retention and egress are explicitly present. The host-sensitive collection is intentionally omitted: the loader supplies `pi_memory` for Pi and `prime_memory` for Prime. Optional fields are described below the JSON.

```json
{
  "enabled": true,
  "autoRecall": true,
  "qdrant": {
    "url": "http://127.0.0.1:6333",
    "replicationFactor": 1,
    "writeConsistencyFactor": 1
  },
  "embeddings": {
    "baseUrl": "http://127.0.0.1:8080/v1",
    "model": "bge-m3",
    "dimension": 1024,
    "queryPrefix": "search_query: "
  },
  "retrieval": {
    "topK": 5,
    "candidatesPerLane": 20,
    "minScore": 0.35,
    "projectBoost": 0.05,
    "contextBudgetChars": 1200,
    "toolResultBudgetChars": 8000,
    "hardContextCharBudget": 16000,
    "timeoutMs": 2500,
    "rootScope": "project",
    "childSearch": true
  },
  "projects": { "registrations": {} },
  "capture": {
    "enabled": false,
    "projectAllowlist": [],
    "projectDenylist": [],
    "episodeRetentionDays": "indefinite",
    "toolArgsChars": 2000,
    "toolResultChars": 4000
  },
  "privacy": {
    "egressMode": "local_only",
    "allowedQdrantDestinations": [],
    "allowedEmbeddingDestinations": [],
    "allowedLlmDestinations": [],
    "allowActiveModelFallback": false,
    "allowCrossProviderReplay": false
  },
  "coordination": {
    "maxClockSkewMs": 300000,
    "readConsistency": 1,
    "leaseMs": 30000,
    "reconcileIntervalMs": 900000
  },
  "outbox": {
    "maxJobs": 10000,
    "maxBytes": 268435456,
    "retryBaseMs": 500,
    "retryMaxMs": 30000,
    "sharedFilesystem": false
  },
  "curation": {
    "turnTrigger": 10,
    "toolTrigger": 15,
    "maxInputTokens": 12000
  },
  "memoryModel": {
    "timeoutMs": 30000,
    "maxOutputTokens": 2048
  },
  "raptor": {
    "rebuildEpisodeDelta": 64,
    "maxLevels": 5,
    "summaryInputTokens": 12000,
    "umapDimensions": 10,
    "localNeighbors": 10,
    "gmmMaxClusters": 50,
    "membershipThreshold": 0.1
  }
}
```

Optional `outbox.nodeId` is required when `outbox.sharedFilesystem` is true and must be a unique pseudonymous node component. Optional `memoryModel.modelId` pins the generation model; BGE-M3 is rejected because it is embedding-only. Optional `raptor.seed` is a uint32; otherwise the seed is derived deterministically.

`coordination.readConsistency` accepts a positive integer, `majority`, `quorum`, or `all`. Replicated collections require majority-safe read and write settings.

## Capture activation

To enable capture, explicitly set:

- `capture.enabled: true`;
- `capture.episodeRetentionDays` to a positive integer or `indefinite`;
- `privacy.egressMode` to `local_only` or `allowlist`; and
- project allow/deny rules appropriate for the host.

Then run `init` with matching `--retention`, `--egress`, and `--confirm` arguments. A mismatch fails closed.

`projectAllowlist` and `projectDenylist` contain registered project aliases. Register and unregister projects through the human CLI instead of hand-authoring fingerprints:

```bash
pi-qdrant-memory project register --path /absolute/project --alias project-id --confirm
pi-qdrant-memory project unregister --alias project-id --confirm
```

Registrations are host-sensitive. Their canonical paths may not overlap, aliases are bounded, and a fingerprint mismatch degrades to local-only identity rather than inheriting authority.

## Destination policies

Each allowlist entry has this shape:

```json
{ "id": "destination-id", "residency": "local", "dataUse": "memory" }
```

`allowlist` mode requires at least one Qdrant and embedding destination. Capture also requires an authorized LLM destination selected by `memoryModel.modelId`, or the active model only when `allowActiveModelFallback` is true. Qdrant, embedding, and LLM destinations must agree on residency and data-use labels. `allowCrossProviderReplay` is false by default.

`local_only` accepts only loopback/private destination bindings derived from the configured endpoints. Model tools cannot supply endpoint, collection, credential, or destination arguments.

## Environment allowlist

Operational names:

- `PI_QDRANT_MEMORY_QDRANT_URL`
- `PI_QDRANT_MEMORY_QDRANT_COLLECTION`
- `PI_QDRANT_MEMORY_EMBEDDING_BASE_URL`
- `PI_QDRANT_MEMORY_EMBEDDING_MODEL`
- `PI_QDRANT_MEMORY_EMBEDDING_DIMENSION` (must be `1024`)
- `PI_QDRANT_MEMORY_AUTO_RECALL`
- `PI_QDRANT_MEMORY_TOP_K`
- `PI_QDRANT_MEMORY_CANDIDATES_PER_LANE`
- `PI_QDRANT_MEMORY_MIN_SCORE`
- `PI_QDRANT_MEMORY_PROJECT_BOOST`
- `PI_QDRANT_MEMORY_CONTEXT_BUDGET_CHARS`
- `PI_QDRANT_MEMORY_TOOL_RESULT_BUDGET_CHARS`
- `PI_QDRANT_MEMORY_TIMEOUT_MS`

Credential names:

- `PI_QDRANT_MEMORY_QDRANT_API_KEY`: runtime collection-scoped Qdrant credential.
- `PI_QDRANT_MEMORY_EMBEDDING_API_KEY`: runtime embedding credential.
- `PI_QDRANT_MEMORY_ADMIN_QDRANT_API_KEY`: human CLI administrative credential; never enters `RuntimeConfig`.

Credentials must not appear in the config, repository, URLs, status output, plans, logs, or model arguments.

## Retrieval and lifecycle

The default scope is registered-project only. `project_and_global` is available only to registered root sessions; children remain project-only. `childSearch` controls explicit child searches, not automatic recall. `autoRecall` may fail open without mutating the transcript.

Outbox capacity, retries, retention, expiry, policy revocation, privacy epochs, and tombstones are enforcement boundaries. Increasing capacity or retry windows does not weaken record validation. Shared filesystems require a stable unique node ID and private directory/file permissions.
