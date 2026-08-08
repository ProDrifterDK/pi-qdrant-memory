import type { MemoryCandidate } from "./search.js";
export declare function mergeCandidates(input: {
    project: MemoryCandidate[];
    host: MemoryCandidate[];
    minScore: number;
    projectBoost: number;
    limit: number;
}): MemoryCandidate[];
