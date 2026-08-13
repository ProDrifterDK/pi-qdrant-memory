import { types as nodeTypes } from "node:util";
import { UMAP } from "umap-js";
import { Xoshiro128StarStar } from "./random.js";

export type UmapScope = "global" | "local";
export interface UmapReductionOptions { readonly seed: string | number; readonly scope: UmapScope; readonly dimensions: number; readonly neighbors: number; }
export interface UmapReduction {
  readonly embedding: readonly (readonly number[])[];
  readonly parameters: { readonly scope: UmapScope; readonly seed: string; readonly nComponents: number; readonly nNeighbors: number };
}

function ownDenseArray(value: readonly unknown[], label: string, max: number): readonly unknown[] {
  if (!Array.isArray(value) || nodeTypes.isProxy(value) || Object.getPrototypeOf(value) !== Array.prototype || Object.getOwnPropertySymbols(value).length !== 0 || value.length > max || Object.getOwnPropertyNames(value).length !== value.length + 1) throw new TypeError(`${label} must be a bounded dense plain array`);
  const result: unknown[] = []; for (let index = 0; index < value.length; index += 1) { const descriptor = Object.getOwnPropertyDescriptor(value, String(index)); if (descriptor === undefined || !("value" in descriptor) || descriptor.enumerable !== true) throw new TypeError(`${label} contains an accessor or hole`); result.push(descriptor.value); } return result;
}
function denseMatrix(input: readonly (readonly number[])[]): number[][] {
  const rows = ownDenseArray(input, "UMAP input", 65_536); if (rows.length === 0) return [];
  const first = rows[0]; if (!Array.isArray(first) || nodeTypes.isProxy(first)) throw new TypeError("UMAP row is invalid"); const dimension = first.length;
  if (!Number.isSafeInteger(dimension) || dimension < 1 || dimension > 4096) throw new TypeError("UMAP dimension is invalid");
  return rows.map((candidate) => { if (!Array.isArray(candidate)) throw new TypeError("UMAP row is invalid"); const row = ownDenseArray(candidate, "UMAP row", 4096); if (row.length !== dimension || row.some((value) => typeof value !== "number" || !Number.isFinite(value))) throw new TypeError("UMAP input must be a dense finite matrix"); return row as number[]; });
}
function boundedOptions(count: number, inputDimension: number, input: UmapReductionOptions): UmapReduction["parameters"] {
  if (input.scope !== "global" && input.scope !== "local") throw new TypeError("UMAP scope is invalid");
  if (!Number.isSafeInteger(input.dimensions) || input.dimensions < 1 || input.dimensions > 64) throw new TypeError("UMAP dimensions must be in 1..64");
  if (!Number.isSafeInteger(input.neighbors) || input.neighbors < 1 || input.neighbors > 65_536) throw new TypeError("UMAP neighbors are invalid");
  const nComponents = count <= 2 ? 1 : Math.max(1, Math.min(input.dimensions, count - 2, inputDimension));
  const nNeighbors = count <= 2 ? Math.max(1, count - 1) : Math.max(2, Math.min(input.neighbors, count - 1));
  const seed = String(input.seed);
  if (seed.length === 0 || seed.length > 4096) throw new TypeError("UMAP seed is invalid");
  return Object.freeze({ scope: input.scope, seed, nComponents, nNeighbors });
}
function removeZeroVariance(matrix: number[][]): { matrix: number[][]; allEqual: boolean } {
  if (matrix.length === 0) return { matrix, allEqual: true };
  const dimension = matrix[0]!.length;
  const means = Array.from({ length: dimension }, (_, column) => matrix.reduce((sum, row) => sum + row[column]!, 0) / matrix.length);
  const variances = means.map((mean, column) => matrix.reduce((sum, row) => sum + (row[column]! - mean) ** 2, 0) / matrix.length);
  const active = variances.map((variance, index) => variance > 1e-12 ? index : -1).filter((index) => index >= 0);
  if (active.length === 0) return { matrix: matrix.map(() => [0]), allEqual: true };
  return { matrix: matrix.map((row) => active.map((column) => row[column]!)), allEqual: false };
}

export function reduceUmapDetailed(input: readonly (readonly number[])[], options: UmapReductionOptions): UmapReduction {
  const owned = denseMatrix(input);
  const parameters = boundedOptions(owned.length, owned[0]?.length ?? 1, options);
  if (owned.length === 0) return Object.freeze({ embedding: Object.freeze([]), parameters });
  if (owned.length === 1) return Object.freeze({ embedding: Object.freeze([Object.freeze([0])]), parameters });
  const normalized = removeZeroVariance(owned);
  if (normalized.allEqual) return Object.freeze({ embedding: Object.freeze(owned.map(() => Object.freeze(Array.from({ length: parameters.nComponents }, () => 0)))), parameters });
  if (owned.length === 2) {
    const distance = Math.sqrt(normalized.matrix[0]!.reduce((sum, value, index) => sum + (value - normalized.matrix[1]![index]!) ** 2, 0));
    const embedding = distance === 0 ? [[0], [0]] : [[-0.5], [0.5]];
    return Object.freeze({ embedding: Object.freeze(embedding.map((row) => Object.freeze(row))), parameters });
  }
  const rng = new Xoshiro128StarStar(`${parameters.seed}:${parameters.scope}`);
  const reducer = new UMAP({ nComponents: parameters.nComponents, nNeighbors: parameters.nNeighbors, random: rng.random });
  const result = reducer.fit(normalized.matrix.map((row) => [...row]));
  if (!Array.isArray(result) || result.length !== owned.length || result.some((row) => !Array.isArray(row) || row.length !== parameters.nComponents || row.some((value) => typeof value !== "number" || !Number.isFinite(value)))) throw new TypeError("UMAP produced an invalid embedding");
  return Object.freeze({ embedding: Object.freeze(result.map((row) => Object.freeze([...row]))), parameters });
}
export function reduceUmap(input: readonly (readonly number[])[], options: UmapReductionOptions): readonly (readonly number[])[] { return reduceUmapDetailed(input, options).embedding; }
export function reduceUmapPair(vectors: readonly (readonly number[])[], input: { seed: string | number; dimensions: number; globalNeighbors: number; localNeighbors: number }): { readonly global: UmapReduction; readonly local: UmapReduction } {
  const global = reduceUmapDetailed(vectors, { seed: input.seed, scope: "global", dimensions: input.dimensions, neighbors: input.globalNeighbors });
  const local = reduceUmapDetailed(vectors, { seed: input.seed, scope: "local", dimensions: input.dimensions, neighbors: input.localNeighbors });
  return Object.freeze({ global, local });
}
