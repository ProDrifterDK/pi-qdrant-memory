export interface DiagonalGmmFit {
    readonly components: number;
    readonly dimensions: number;
    readonly logLikelihood: number;
    readonly parameterCount: number;
    readonly bic: number;
    readonly iterations: number;
    readonly weights: readonly number[];
    readonly means: readonly (readonly number[])[];
    readonly variances: readonly (readonly number[])[];
    readonly memberships: readonly (readonly number[])[];
}
export interface DiagonalGmmSelection {
    readonly fit: DiagonalGmmFit;
    readonly assignments: readonly (readonly number[])[];
    readonly clusters: readonly (readonly number[])[];
}
export declare function fitDiagonalGmm(input: readonly (readonly number[])[], options: {
    seed: string | number;
    components: number;
    maxIterations?: number;
    tolerance?: number;
}): DiagonalGmmFit;
export declare function selectDiagonalGmm(input: readonly (readonly number[])[], options: {
    seed: string | number;
    maxClusters: number;
    membershipThreshold?: number;
}): DiagonalGmmSelection;
