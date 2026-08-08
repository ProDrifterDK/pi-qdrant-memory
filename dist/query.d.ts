export declare function isNaturalLanguagePrompt(prompt: string): boolean;
export declare function userTextFromMessage(message: unknown): string | undefined;
export declare function priorUserPromptsFromBranch(entries: readonly unknown[]): string[];
export declare function buildEffectiveQuery(current: string, priorUserPrompts: readonly string[]): string | undefined;
