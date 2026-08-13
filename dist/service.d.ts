import type { ContextEvent, ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { RecallCache } from "./cache.js";
import { type MemoryErrorCategory } from "./clients/http.js";
import type { ProjectIdentity } from "./project.js";
import type { MemoryReadStore, MemoryRetriever, MemorySearchResult } from "./retrieval/search.js";
import type { ExplicitSearchService } from "./tool.js";
import type { AuthorizedDestination, HostId, RuntimeConfig } from "./types.js";
import type { ExplicitMemorySearchInput } from "./tool.js";
type AgentMessage = ContextEvent["messages"][number];
type WarningCategory = MemoryErrorCategory | "format" | "internal" | "host";
export interface MemoryWarning {
    category: WarningCategory;
    message: string;
}
export type MemoryWarningSink = (warning: MemoryWarning, ctx: ExtensionContext) => void;
export interface MemoryServiceDependencies {
    host: HostId;
    config: RuntimeConfig;
    retriever: Pick<MemoryRetriever, "search">;
    projectResolver(cwd: string): Promise<ProjectIdentity>;
    cache: RecallCache<MemorySearchResult>;
    warningSink: MemoryWarningSink;
    modelDestination(ctx: ExtensionContext): AuthorizedDestination | undefined;
    isChild(ctx: ExtensionContext): boolean;
    qdrant?: Pick<MemoryReadStore, "health" | "collectionInfo">;
    embeddingHealth?(signal?: AbortSignal): Promise<void>;
}
/** Coordinates scoped retrieval, promise reuse, fail-open injection, and health warnings. */
export declare class MemoryService implements ExplicitSearchService {
    private readonly dependencies;
    private readonly configRevision;
    private readonly warned;
    constructor(dependencies: MemoryServiceDependencies);
    search(input: ExplicitMemorySearchInput, ctx: ExtensionContext, signal?: AbortSignal): Promise<MemorySearchResult>;
    prefetch(prompt: string, ctx: ExtensionContext): void;
    inject(messages: AgentMessage[], ctx: ExtensionContext): Promise<AgentMessage[]>;
    checkHealth(ctx: ExtensionContext): Promise<void>;
    clear(): void;
    private recall;
    private warn;
}
export {};
