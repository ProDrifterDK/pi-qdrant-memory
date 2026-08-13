export type UmapScope = "global" | "local";
export interface UmapReductionOptions {
    readonly seed: string | number;
    readonly scope: UmapScope;
    readonly dimensions: number;
    readonly neighbors: number;
}
export interface UmapReduction {
    readonly embedding: readonly (readonly number[])[];
    readonly parameters: {
        readonly scope: UmapScope;
        readonly seed: string;
        readonly nComponents: number;
        readonly nNeighbors: number;
    };
}
export declare function reduceUmapDetailed(input: readonly (readonly number[])[], options: UmapReductionOptions): UmapReduction;
export declare function reduceUmap(input: readonly (readonly number[])[], options: UmapReductionOptions): readonly (readonly number[])[];
export declare function reduceUmapPair(vectors: readonly (readonly number[])[], input: {
    seed: string | number;
    dimensions: number;
    globalNeighbors: number;
    localNeighbors: number;
}): {
    readonly global: UmapReduction;
    readonly local: UmapReduction;
};
