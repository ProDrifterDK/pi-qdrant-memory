import type { ExtensionContext, ToolDefinition } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import type { MemoryCandidate, MemorySearchResult } from "./retrieval/search.js";
export interface ExplicitSearchService {
    search(query: string, limit: number, ctx: ExtensionContext, signal?: AbortSignal): Promise<MemorySearchResult>;
}
export interface MemorySearchDetails {
    hitCount: number;
    hits: Array<Pick<MemoryCandidate, "id" | "text" | "rawScore" | "adjustedScore" | "lane" | "projectLabel" | "sourceType" | "sourceSystem" | "createdAt">>;
}
declare const memorySearchParameters: Type.TObject<{
    query: Type.TString;
    limit: Type.TOptional<Type.TInteger>;
}>;
export declare function createMemorySearchTool(input: {
    service: ExplicitSearchService;
    defaultLimit: number;
    toolResultBudgetChars: number;
    hardContextCharBudget: number;
}): ToolDefinition<typeof memorySearchParameters, MemorySearchDetails>;
export {};
