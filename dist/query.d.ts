export declare function isNaturalLanguagePrompt(prompt: string): boolean;
export declare function userTextFromMessage(message: unknown): string | undefined;
export declare function priorUserPromptsFromBranch(entries: readonly unknown[]): string[];
export declare function buildEffectiveQuery(current: string, priorUserPrompts: readonly string[]): string | undefined;
export interface RetrievalWindow {
    after?: string;
    before?: string;
}
/** Strict, model-safe RFC3339 retrieval window. Offsets are accepted but canonicalized to UTC. */
export declare function parseRetrievalWindow(after?: string, before?: string): RetrievalWindow;
