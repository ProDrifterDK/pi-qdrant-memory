import { type EpisodeRecord } from "../domain/records.js";
import type { HostId } from "../types.js";
export declare const CURATION_PROMPT_REVISION = "curation-prompt-v1";
export declare const CURATION_MAX_INPUT_TOKENS = 65536;
export declare const UNTRUSTED_OPEN = "<untrusted-data>";
export declare const UNTRUSTED_CLOSE = "</untrusted-data>";
export interface CurationPromptProvider {
    providerId: string;
    modelId: string;
    destinationId: string;
}
export interface CurationPromptInput {
    host: HostId;
    policyId: string;
    policyHash: string;
    policyEpoch: number;
    provider: CurationPromptProvider;
    promptRevision?: string;
    membership: readonly string[];
    episodes: readonly EpisodeRecord[];
    maxInputTokens?: number;
}
export interface CurationPrompt {
    readonly envelope: string;
    readonly promptRevision: string;
    readonly maxInputTokens: number;
    /** Exact UTF-8 bytes of the envelope passed to the Task 6 bridge. */
    readonly envelopeBytes: number;
    readonly policyProvenance: Readonly<{
        host: HostId;
        policyId: string;
        policyHash: string;
        policyEpoch: number;
        providerId: string;
        modelId: string;
        destinationId: string;
    }>;
    readonly membership: readonly string[];
}
/**
 * Build the bounded untrusted curation envelope. The envelope is the ONLY
 * egress payload: explicit sorted membership + bounded redacted episode
 * fields inside explicit `<untrusted-data>` fences, a policy/provider
 * provenance header and a frozen prompt revision. It NEVER contains system or
 * developer instructions, injected memory, tool access declarations, vectors,
 * keys, host infrastructure or unredacted payload. The exact envelope bytes
 * are budgeted against maxInputTokens BEFORE the caller may egress.
 */
export declare function buildCurationPrompt(input: CurationPromptInput): CurationPrompt;
