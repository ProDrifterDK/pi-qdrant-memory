import type { MemoryCandidate } from "./search.js";
/** Deterministic reciprocal-rank fusion across incomparable internal lane scores. */
export declare function mergeCandidates(input: {
    lanes: readonly (readonly MemoryCandidate[])[];
    limit: number;
    projectBoost: number;
}): MemoryCandidate[];
