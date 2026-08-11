import type { Api, Context, Model } from "@earendil-works/pi-ai";
import type { AuthorizedDestination, HostId } from "../types.js";
import { type ProcessingPolicy } from "../domain/policy.js";
export type ResolvedRequestAuthLike = {
    ok: true;
    apiKey?: string;
    headers?: Record<string, string | null>;
} | {
    ok: false;
    error: string;
};
export interface MemoryCompletionOptions {
    signal?: AbortSignal;
    maxOutputTokens: number;
    temperature: number;
}
export interface ModelRegistryLike {
    getApiKeyAndHeaders?: (model: Model<Api>) => Promise<ResolvedRequestAuthLike>;
    complete?: unknown;
}
/** A host-verified exact mapping from the selected registry model to one policy destination. */
export interface LlmDestinationModelBinding {
    readonly providerId: string;
    readonly modelId: string;
    readonly destinationId: string;
}
export interface MemoryCompletionContext {
    host: HostId;
    modelRegistry: ModelRegistryLike;
    memoryModel?: Model<Api>;
    activeModel?: Model<Api>;
    activeProviderId?: string;
    sessionProviderId?: string;
    allowActiveModelFallback?: boolean;
    allowCrossProviderReplay?: boolean;
    policy: ProcessingPolicy;
    /** The producer/worker intersection's authorized LLM destination. */
    llmDestination: AuthorizedDestination;
    /** Required trusted resolver evidence; allowlist membership alone is never enough. */
    llmDestinationBinding: LlmDestinationModelBinding;
    policyEpoch?: number;
    /** Optional supplied policy digest is accepted only when it equals the computed digest. */
    policyHash?: string;
}
export interface AiNamespaceLike {
    completeSimple?: unknown;
}
export interface CompletionProvenance {
    readonly host: HostId;
    readonly providerId: string;
    readonly modelId: string;
    readonly destinationId: string;
    readonly policyId: string;
    readonly policyEpoch: number;
    readonly policyHash: string;
    readonly promptRevision: string;
    readonly invokedAt: string;
}
export type MemoryCompletionPendingReason = "invalid_input" | "no_model" | "unsupported_model" | "fallback_disabled" | "cross_provider_disabled" | "policy" | "no_completion_method" | "auth" | "cancelled" | "timeout" | "invalid_response" | "output_limit" | "failed";
export type MemoryCompletionResult = {
    readonly state: "completed";
    readonly text: string;
    readonly provenance: CompletionProvenance;
} | {
    readonly state: "pending";
    readonly reason: MemoryCompletionPendingReason;
};
export interface BoundLlmDestination {
    readonly destination: AuthorizedDestination;
    complete(input: {
        envelope: string;
        signal?: AbortSignal;
    }): Promise<string>;
}
export interface CompleteMemoryInput {
    envelope: string;
    /** Must be the exact registry model selected by memoryModel or active fallback. */
    model: Model<Api>;
    /** Deliberately not forwarded: egress always receives a fresh envelope-only Context. */
    hostContext: Context;
    maxInputTokens: number;
    maxOutputTokens: number;
    timeoutMs: number;
    signal?: AbortSignal;
    memoryContext: MemoryCompletionContext;
    promptRevision: string;
    aiNamespace?: AiNamespaceLike;
}
/** Returns a new record; nullable host defaults are never forwarded to Prime. */
export declare function sanitizeAuthHeaders(headers?: Record<string, string | null>): Record<string, string>;
/** Prefers Pi's registry; Prime fallback is authenticated, bounded, and never aborts a host turn. */
export declare function completeMemory(input: CompleteMemoryInput): Promise<MemoryCompletionResult>;
