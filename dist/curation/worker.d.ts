import type { Model, Api, Context } from "@earendil-works/pi-ai";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { BoundEmbeddingDestination } from "../clients/embeddings.js";
import { RootWorkerContext } from "../coordination/root.js";
import { ProductionCoordinationStore } from "../qdrant/write.js";
import type { SecretScanner } from "../security/redaction.js";
import { type ProcessingPolicy } from "../domain/policy.js";
import type { AuthorizedDestination, HostId } from "../types.js";
import { type LlmDestinationModelBinding, type ModelRegistryLike } from "./llm.js";
export declare const CURATION_TURN_TRIGGER = 10;
export declare const CURATION_TOOL_TRIGGER = 15;
export type CurationTrigger = "run" | "persist_only" | "child" | "disabled";
export interface CurationTriggerInput {
    host: HostId;
    sessionManager: SessionManager;
    env: Record<string, string | undefined>;
    rootTurns: number;
    toolCalls: number;
    beforeCompaction: boolean;
    shutdown: boolean;
}
/**
 * Trigger discovery is optimization ONLY: enqueue at root turn 10, tool
 * trigger 15, before compaction; shutdown only persists pending work and never
 * starts LLM curation. Root/child gating uses the SAME validated host-marker
 * resolution as capture: Prime resolves child from header.rlmDepth then
 * RLM_DEPTH; Pi resolves header.parentSession as the sole host child signal
 * (PI_SUBAGENT_CHILD/DEPTH are optional extension-wrapper markers validated
 * when present, never assumed). Invalid/contradictory markers disable root
 * curation; children may ingest/search but cannot claim.
 */
export declare function curationTrigger(input: CurationTriggerInput): CurationTrigger;
/**
 * Deterministic coverage truth recovery: explicit membership MINUS episodes
 * already covered by this extractor revision under the exact policy identity.
 * Coverage IDs are policy-specific, so a policy migration re-curates.
 */
export declare function filterUncoveredEpisodes(input: {
    store: ProductionCoordinationStore;
    membership: readonly string[];
    extractorRevision: string;
    policyHash: string;
    policyEpoch: number;
    privacyEpoch: number;
    policyIntersectionId: string;
}): Promise<readonly string[]>;
export interface CurationWorkerInput {
    host: HostId;
    store: ProductionCoordinationStore;
    nodeId: string;
    leaseMs: number;
    maxClockSkewMs: number;
    clock?: () => number;
    signal?: AbortSignal;
    workerPolicy: ProcessingPolicy;
    extractorRevision: string;
    producerPolicies: readonly ProcessingPolicy[];
    embedding: BoundEmbeddingDestination;
    llm?: {
        memoryModel: Model<Api>;
        modelRegistry: ModelRegistryLike;
        llmDestination: AuthorizedDestination;
        llmDestinationBinding: LlmDestinationModelBinding;
    };
    /** Internal lifecycle thunk: defers reading caller-owned `llm` until a fresh leased path. */
    llmProvider?: () => {
        memoryModel: Model<Api>;
        modelRegistry: ModelRegistryLike;
        llmDestination: AuthorizedDestination;
        llmDestinationBinding: LlmDestinationModelBinding;
    };
    /** Lazy fresh-only options; lifecycle root keeps getters behind this thunk. */
    freshOptionsProvider?: () => {
        llm: {
            memoryModel: Model<Api>;
            modelRegistry: ModelRegistryLike;
            llmDestination: AuthorizedDestination;
            llmDestinationBinding: LlmDestinationModelBinding;
        };
        maxOutputTokens?: number;
        timeoutMs?: number;
        scan?: SecretScanner;
    };
    hostContext?: Context;
    maxOutputTokens?: number;
    timeoutMs?: number;
    /** Optional final scanner used only to further restrict curated egress. */
    scan?: SecretScanner;
    createdAt?: () => string;
}
export type CurationRunState = "completed" | "pending" | "child" | "no_claim";
export interface CurationRunResult {
    readonly state: CurationRunState;
    readonly reason?: string;
    readonly jobId?: string;
    readonly observations?: number;
}
/**
 * One curation cycle for the explicit sorted membership: at most ONE effective
 * claim per host/batch. Root/child gating is fail-closed; jobs are split by
 * compatible policy groups (producer x worker intersection with an LLM
 * destination), incompatible groups stay pending. Control privacy/coordination
 * epochs are reread before LLM egress, before proposal acceptance and inside
 * every materialization write. A failed LLM call or validation leaves a
 * retryable job and the episodes searchable.
 */
export type CurationWorkerOptions = CurationWorkerInput & {
    membership: readonly string[];
};
export declare function runCurationCore(worker: RootWorkerContext, input: CurationWorkerOptions): Promise<CurationRunResult>;
