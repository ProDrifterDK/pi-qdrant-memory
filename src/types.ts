export type HostId = "prime" | "pi";
export type RetentionDays = number | "indefinite";

export interface AuthorizedDestination {
  id: string;
  residency: string;
  dataUse: string;
}

export interface CollectionMetadataContract {
  ownerHost: HostId;
  schema: "pi-qdrant-memory-v2";
  schemaRevision: 1;
  dimension: number;
  distance: "Cosine";
  model: string;
}

export interface OutboxConfig {
  maxJobs: number;
  maxBytes: number;
  retryBaseMs: number;
  retryMaxMs: number;
  nodeId?: string;
  sharedFilesystem: boolean;
}

export interface RetrievalConfig {
  topK: number;
  candidatesPerLane: number;
  minScore: number;
  projectBoost: number;
  contextBudgetChars: number;
  toolResultBudgetChars: number;
  hardContextCharBudget: 16000;
  timeoutMs: number;
  rootScope: "project" | "project_and_global";
  childSearch: boolean;
}

/** Dedicated generation-model limits. BGE-M3 belongs exclusively to embeddings. */
export interface MemoryModelConfig {
  modelId?: string;
  timeoutMs: number;
  maxOutputTokens: number;
}

export interface RuntimeConfig {
  host: HostId;
  configPath: string;
  enabled: boolean;
  autoRecall: boolean;
  qdrant: {
    url: string;
    collection: string;
    apiKey?: string;
    replicationFactor: number;
    writeConsistencyFactor: number;
  };
  embeddings: {
    baseUrl: string;
    model: string;
    dimension: 1024;
    queryPrefix: string;
    apiKey?: string;
  };
  retrieval: RetrievalConfig;
  projects: {
    registrations: Record<string, { canonicalPath: string; fingerprint: string; alias: string }>;
  };
  capture: {
    enabled: boolean;
    projectAllowlist: string[];
    projectDenylist: string[];
    episodeRetentionDays: RetentionDays;
    toolArgsChars: number;
    toolResultChars: number;
  };
  privacy: {
    egressMode: "local_only" | "allowlist";
    allowedQdrantDestinations: AuthorizedDestination[];
    allowedEmbeddingDestinations: AuthorizedDestination[];
    allowedLlmDestinations: AuthorizedDestination[];
    allowActiveModelFallback: boolean;
    allowCrossProviderReplay: boolean;
  };
  coordination: {
    maxClockSkewMs: number;
    readConsistency: number | "majority" | "quorum" | "all";
    leaseMs: number;
    reconcileIntervalMs: number;
  };
  outbox: OutboxConfig;
  curation: {
    turnTrigger: number;
    toolTrigger: number;
    maxInputTokens: number;
  };
  memoryModel: MemoryModelConfig;
  raptor: {
    rebuildEpisodeDelta: number;
    maxLevels: number;
    summaryInputTokens: number;
    umapDimensions: number;
    localNeighbors: number;
    gmmMaxClusters: number;
    membershipThreshold: number;
    seed?: number;
  };
}

export interface ConfigLoadDependencies {
  env: Record<string, string | undefined>;
  homeDir: string;
  xdgConfigHome?: string;
  readTextFile(path: string): Promise<string>;
}


/** Structural-redaction and final-scanner states shared by all egress lanes. */
export type RedactionStatus = "unchanged" | "redacted" | "dropped";
export type SecretScanStatus = "passed" | "rejected" | "error";
export interface RedactedEgressMaterial {
  readonly text: string;
  readonly redactionStatus: Exclude<RedactionStatus, "dropped">;
  readonly secretScan: "passed";
  readonly dropped: false;
  readonly contentHash: string;
}
