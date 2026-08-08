import type { EmbeddingsClient } from "../clients/embeddings.js";
import type { QdrantSearchHit, ReadonlyQdrantClient } from "../clients/qdrant-readonly.js";
import type { HostId, RetrievalConfig } from "../types.js";
import type { ProjectIdentity } from "../project.js";
export interface MemoryCandidate {
    id: string;
    text: string;
    rawScore: number;
    adjustedScore: number;
    lane: "project" | "host";
    projectId?: string;
    projectLabel?: string;
    sourceType: string;
    sourceSystem: string;
    createdAt?: string;
}
export interface MemorySearchResult {
    query: string;
    hits: MemoryCandidate[];
}
type Lane = MemoryCandidate["lane"];
export declare function parseMemoryHit(hit: QdrantSearchHit, input: {
    expectedHost: HostId;
    expectedProjectId: string;
    lane: Lane;
}): MemoryCandidate | undefined;
export declare class MemoryRetriever {
    private readonly dependencies;
    constructor(dependencies: {
        embeddings: EmbeddingsClient;
        qdrant: ReadonlyQdrantClient;
        config: RetrievalConfig;
    });
    search(input: {
        query: string;
        host: HostId;
        project: ProjectIdentity;
        limit?: number;
        signal?: AbortSignal;
    }): Promise<MemorySearchResult>;
}
export {};
