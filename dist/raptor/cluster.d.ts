export interface ClusterLeaf {
    readonly id: string;
    readonly vector: readonly number[];
    readonly tokens: number;
}
export interface ClusterNode {
    readonly id: string;
    readonly level: number;
    readonly leafIds: readonly string[];
    readonly vector: readonly number[];
    readonly tokens: number;
    readonly summary: boolean;
}
export interface ClusterEdge {
    readonly parentId: string;
    readonly childId: string;
}
export interface ClusterDag {
    readonly leafIds: readonly string[];
    readonly roots: readonly string[];
    readonly nodes: readonly ClusterNode[];
    readonly edges: readonly ClusterEdge[];
}
export interface ClusterDagOptions {
    readonly seed: string | number;
    readonly maxLevels: number;
    readonly tokenBudget: number;
    readonly umapDimensions?: number;
    readonly globalNeighbors?: number;
    readonly localNeighbors?: number;
    readonly gmmMaxClusters?: number;
    readonly membershipThreshold?: number;
}
/** Stable ID-sorted, token-greedy fallback. Oversized singletons remain flat. */
export declare function stableTokenPartition<T extends {
    readonly id: string;
    readonly tokens: number;
}>(input: readonly T[], tokenBudget: number): readonly (readonly T[])[];
/** Build a deterministic soft-membership DAG. Every edge is exactly level+1. */
export declare function buildClusterDag(input: readonly ClusterLeaf[], inputOptions: ClusterDagOptions): ClusterDag;
export declare function evidenceClosure(dag: ClusterDag, rootIds?: readonly string[]): readonly string[];
