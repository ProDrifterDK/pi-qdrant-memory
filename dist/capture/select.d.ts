import type { PersistedEntry } from "./episode.js";
import type { RedactionStatus } from "../types.js";
export type { PersistedEntry } from "./episode.js";
export type SelectedEventKind = "user" | "assistant" | "tool_call" | "tool_result" | "tool_error";
export interface SelectedCaptureEntry {
    sourceEntryId: string;
    messageId: string;
    partIdentity: string | number;
    eventKind: SelectedEventKind;
    text?: string;
    /** Typed structural-redaction provenance from selector normalization. */
    textRedactionStatus?: RedactionStatus;
    toolName?: string;
    toolArgs?: string;
    toolArgsRedactionStatus?: RedactionStatus;
    errorFingerprint?: string;
    status?: string;
    code?: number;
    eventAt?: string | number;
    turnId?: string;
    /** Durable branch entry/part order within the persisted session. */
    sessionSequence?: number;
}
/** Normalize only final, persisted entries. Event arrays supplied by host callbacks are not accepted here. */
export declare function selectPersistedEntries(entries: readonly PersistedEntry[], options?: {
    toolArgsChars?: number;
    toolResultChars?: number;
    homeDir?: string;
    sequenceOffset?: number;
}): SelectedCaptureEntry[];
