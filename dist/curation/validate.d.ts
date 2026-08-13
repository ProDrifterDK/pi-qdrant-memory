import { type SecretScanner } from "../security/redaction.js";
export declare const CURATION_CATEGORIES: readonly ["preference", "correction", "convention", "fact", "failure", "learning"];
export declare const CURATION_SCOPES: readonly ["project", "session", "host", "global"];
export type CurationCategory = typeof CURATION_CATEGORIES[number];
export type CurationScope = typeof CURATION_SCOPES[number];
export interface CurationItem {
    readonly category: CurationCategory;
    readonly scope: CurationScope;
    readonly subject: string;
    readonly predicate: string;
    value?: unknown;
    text?: string;
    readonly evidence: readonly string[];
    confidence?: number;
}
export interface CurationResult {
    readonly items: readonly CurationItem[];
}
export interface CurationValidationContext {
    /** Episodes whose eventKind is a direct user event; only they may evidence preferences/corrections. */
    readonly directUserEpisodeIds: ReadonlySet<string>;
    /** Every episode id known to exist in the explicit membership. */
    readonly knownEpisodeIds: ReadonlySet<string>;
    readonly maxItems?: number;
    readonly maxEvidence?: number;
}
/**
 * Strict curation-result validation over an OWNED canonical clone. Unknown
 * fields/categories/scopes, non-plain or accessor-bearing input, unbounded
 * lists/strings/values and duplicate/foreign evidence ids are rejected.
 * Standing preferences/corrections require at least one direct-user episode
 * in evidence; a tool output that invents a standing instruction is rejected.
 */
export declare function validateCurationResult(input: unknown, ctx: CurationValidationContext): CurationResult;
/**
 * Validate that a validated result is safe to persist in an accepted proposal.
 * The entire canonical item (including nested value objects and their keys) is
 * structurally redacted and passed through the mandatory built-in scanner. Any
 * required transformation is rejected rather than persisting the raw result;
 * this keeps proposal/content identity and later materialization deterministic.
 */
export declare function assertPersistableCurationResult(result: CurationResult, scan?: SecretScanner): CurationResult;
/**
 * JSON-only parsing with fences/prefix/suffix rejection, prototype-key
 * rejection and duplicate-key rejection. The returned value is a plain
 * caller-independent clone.
 */
export declare function parseStrictCurationJson(text: string): unknown;
