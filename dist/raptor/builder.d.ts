import type { BoundEmbeddingDestination } from "../clients/embeddings.js";
import type { BoundLlmDestination } from "../curation/llm.js";
import { type ProcessingPolicy } from "../domain/policy.js";
import { type RaptorSummaryRecord } from "../domain/records.js";
import { ProductionCoordinationStore, LeaseAuthority } from "../qdrant/write.js";
import { type SecretScanner } from "../security/redaction.js";
import type { HostId } from "../types.js";
import { type RaptorManifest } from "./manifest.js";
export declare const RAPTOR_ALGORITHM_REVISION = "raptor-umap140-diag-gmm-v1";
export declare const RAPTOR_PROMPT_REVISION = "raptor-summary-v2";
export interface RaptorLeafInput {
    readonly id: string;
    readonly text: string;
    readonly vector: readonly number[];
    readonly tokens: number;
    readonly projectId: string;
    readonly eventAt: string;
    readonly policy: ProcessingPolicy;
}
export interface RaptorBuildInput {
    readonly host: HostId;
    readonly workerPolicy: ProcessingPolicy;
    readonly leaves: readonly RaptorLeafInput[];
    readonly llm: BoundLlmDestination;
    readonly embedding: BoundEmbeddingDestination;
    readonly modelId: string;
    readonly homeDir: string;
    readonly seed: string | number;
    readonly maxLevels: number;
    readonly summaryInputTokens: number;
    readonly umapDimensions: number;
    readonly localNeighbors: number;
    readonly gmmMaxClusters: number;
    readonly membershipThreshold: number;
    readonly global?: boolean;
    readonly scan?: SecretScanner;
    readonly signal?: AbortSignal;
    readonly reuseCandidates?: readonly RaptorSummaryRecord[];
}
export type RaptorBuildResult = {
    readonly state: "completed";
    readonly generationId: string;
    readonly manifest: RaptorManifest;
    readonly summaries: readonly RaptorSummaryRecord[];
    readonly reused: number;
} | {
    readonly state: "pending";
    readonly reason: "invalid_input" | "incompatible_policy" | "authority_changed" | "summary_failed" | "scanner" | "embedding_failed" | "write_failed" | "publication_lost" | "cancelled" | "clustering_failed";
} | {
    readonly state: "empty";
    readonly reason: "no_eligible_leaves";
};
export interface RaptorPolicyGroup {
    readonly policy: ProcessingPolicy;
    readonly leaves: readonly RaptorLeafInput[];
}
/** Split producer leaves by a real all-source/worker intersection; incompatible leaves remain pending. */
export declare function groupRaptorLeavesByPolicy(leaves: readonly RaptorLeafInput[], workerPolicy: ProcessingPolicy): {
    readonly groups: readonly RaptorPolicyGroup[];
    readonly pendingIds: readonly string[];
};
/** Root-only deterministic generation build. All writes/publication are nominal store+lease operations. */
export declare function buildRaptorGeneration(store: ProductionCoordinationStore, authority: LeaseAuthority, input: RaptorBuildInput): Promise<RaptorBuildResult>;
