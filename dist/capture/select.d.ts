import type { PersistedEntry } from "./episode.js";
export type { PersistedEntry } from "./episode.js";
export type SelectedEventKind = "user" | "assistant" | "tool_call" | "tool_result" | "tool_error";
export interface SelectedCaptureEntry {
    sourceEntryId: string;
    messageId: string;
    partIdentity: string | number;
    eventKind: SelectedEventKind;
    text?: string;
    toolName?: string;
    toolArgs?: string;
    errorFingerprint?: string;
    status?: string;
    code?: number;
    eventAt?: string | number;
    turnId?: string;
}
/** Normalize only final, persisted entries. Event arrays supplied by host callbacks are not accepted here. */
export declare function selectPersistedEntries(entries: readonly PersistedEntry[], options?: {
    toolArgsChars?: number;
    toolResultChars?: number;
    homeDir?: string;
}): SelectedCaptureEntry[];
