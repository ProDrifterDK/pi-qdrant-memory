import type { MemoryCandidate } from "./retrieval/search.js";
export declare const MEMORY_CONTEXT_CUSTOM_TYPE = "pi-qdrant-memory-context";
export interface FormattedMemoryHit {
    hit: MemoryCandidate;
    text: string;
}
export interface FormattedMemoryContext {
    text: string;
    hits: FormattedMemoryHit[];
}
/**
 * Neutralize both delimiters and delimiter-like variants in attacker-controlled
 * fields. The exact closing marker uses a backslash so the excerpt remains
 * readable while never becoming the envelope's closing tag.
 */
export declare function escapeMemoryField(value: string): string;
export declare function formatMemoryProvenance(value: unknown, fallback?: string): string;
export declare function formatMemoryContext(hits: readonly MemoryCandidate[], requestedBudget: number): string;
/** Internal companion used by the explicit tool so details mirror formatting. */
export declare function formatMemoryContextResult(hits: readonly MemoryCandidate[], requestedBudget: number): FormattedMemoryContext;
