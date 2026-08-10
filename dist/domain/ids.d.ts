import type { HostId } from "../types.js";
export interface StateKeyInput {
    host: HostId;
    scope: string;
    projectId?: string | null;
    category: string;
    subject: string;
    predicate: string;
}
/** Logical state identity; it is independent of the current value and owner. */
export declare function stateKey(input: StateKeyInput): string;
/** Reusable value identity under one coordination policy. */
export declare function contentId(policyHash: string, logicalStateKey: string, canonicalValue: unknown): string;
export type EffectiveOrder = `session:${number}` | readonly [string, string, string];
/** Validate the two causal-order encodings permitted by §8.2. */
export declare function validateEffectiveOrder(value: unknown): asserts value is EffectiveOrder;
/** Insert-only occurrence identity. effectiveOrder may be a causal tuple. */
export declare function observationId(policyEpoch: number, logicalContentId: string, primaryEvidenceEpisodeId: string, effectiveOrder: unknown): string;
export declare function evidenceLinkId(observation: string, episode: string, extractorRevision: string | number): string;
export interface EpisodeIdentityInput {
    host: HostId;
    sessionId: string;
    messageId: string;
    part?: string | number;
}
export declare function episodeId(input: EpisodeIdentityInput): string;
export declare function episodeId(host: HostId, sessionId: string, messageId: string, part?: string | number): string;
export declare function jobId(ownerHost: HostId, membership: readonly string[], policyHash: string, extractorRevision: string): string;
export declare function jobId(input: {
    ownerHost: HostId;
    membership: readonly string[];
    policyHash: string;
    extractorRevision: string;
}): string;
export declare function manifestHash(memberIds: readonly string[]): string;
export declare function tombstoneId(scope: "occurrence" | "content" | "state", targetId: string, provenanceId?: string): string;
