import type { ExtensionContext, ToolDefinition } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import type { MemoryCandidate, MemorySearchResult } from "./retrieval/search.js";
export declare const MEMORY_SEARCH_MODES: readonly ["all", "current", "historical", "episodes", "curated", "raptor"];
export type MemorySearchMode = typeof MEMORY_SEARCH_MODES[number];
export interface ExplicitMemorySearchInput {
    query: string;
    limit: number;
    mode: MemorySearchMode;
    after?: string;
    before?: string;
}
export interface ExplicitSearchService {
    search(input: ExplicitMemorySearchInput, ctx: ExtensionContext, signal?: AbortSignal): Promise<MemorySearchResult>;
}
export interface MemorySearchDetails {
    hitCount: number;
    hits: Array<Pick<MemoryCandidate, "id" | "text" | "rawScore" | "adjustedScore" | "lane" | "projectLabel" | "sourceType" | "sourceSystem" | "createdAt">>;
}
export declare const memorySearchParameters: Type.TObject<{
    query: Type.TString;
    limit: Type.TOptional<Type.TInteger>;
    mode: Type.TOptional<Type.TUnion<Type.TLiteral<"all" | "raptor" | "current" | "historical" | "episodes" | "curated">[]>>;
    after: Type.TOptional<Type.TString>;
    before: Type.TOptional<Type.TString>;
}>;
export declare function createMemorySearchTool(input: {
    service: ExplicitSearchService;
    defaultLimit: number;
    toolResultBudgetChars: number;
    hardContextCharBudget: number;
}): ToolDefinition<typeof memorySearchParameters, MemorySearchDetails>;
