import { types as nodeTypes } from "node:util";
import { canonicalStringify, sha256Hex } from "../domain/canonical.js";
import { selectDiagonalGmm } from "./gmm.js";
import { reduceUmapDetailed } from "./umap.js";

export interface ClusterLeaf { readonly id: string; readonly vector: readonly number[]; readonly tokens: number; }
export interface ClusterNode { readonly id: string; readonly level: number; readonly leafIds: readonly string[]; readonly vector: readonly number[]; readonly tokens: number; readonly summary: boolean; }
export interface ClusterEdge { readonly parentId: string; readonly childId: string; }
export interface ClusterDag { readonly leafIds: readonly string[]; readonly roots: readonly string[]; readonly nodes: readonly ClusterNode[]; readonly edges: readonly ClusterEdge[]; }
export interface ClusterDagOptions { readonly seed: string | number; readonly maxLevels: number; readonly tokenBudget: number; readonly umapDimensions?: number; readonly globalNeighbors?: number; readonly localNeighbors?: number; readonly gmmMaxClusters?: number; readonly membershipThreshold?: number; }

function own(value: object, key: string): unknown { if (nodeTypes.isProxy(value)) throw new TypeError("RAPTOR cluster proxy is forbidden"); const descriptor = Object.getOwnPropertyDescriptor(value, key); if (descriptor === undefined || !("value" in descriptor) || descriptor.enumerable !== true) throw new TypeError(`RAPTOR cluster ${key} must be own data`); return descriptor.value; }
function dense(value: readonly unknown[], label: string, max: number): readonly unknown[] { if (!Array.isArray(value) || nodeTypes.isProxy(value) || Object.getPrototypeOf(value) !== Array.prototype || Object.getOwnPropertySymbols(value).length !== 0 || value.length > max || Object.getOwnPropertyNames(value).length !== value.length + 1) throw new TypeError(`${label} must be a dense plain array`); const result: unknown[]=[]; for(let index=0;index<value.length;index+=1){const descriptor=Object.getOwnPropertyDescriptor(value,String(index));if(descriptor===undefined||!("value" in descriptor)||descriptor.enumerable!==true)throw new TypeError(`${label} contains an accessor or hole`);result.push(descriptor.value);}return result; }
function snapshotLeaves(input: readonly ClusterLeaf[]): ClusterLeaf[] {
  const values = dense(input, "RAPTOR leaves", 65_536); const ids = new Set<string>(); let dimension: number | undefined; const result: ClusterLeaf[]=[];
  for (const candidate of values) {
    if (typeof candidate !== "object" || candidate === null || nodeTypes.isProxy(candidate) || Object.getPrototypeOf(candidate) !== Object.prototype || Object.getOwnPropertySymbols(candidate).length !== 0) throw new TypeError("RAPTOR leaf is invalid");
    const id = own(candidate,"id"); const vectorValue=own(candidate,"vector"); const tokens=own(candidate,"tokens");
    if (typeof id !== "string" || id.length === 0 || id.length > 512 || ids.has(id)) throw new TypeError("RAPTOR leaf identity is invalid"); ids.add(id);
    if (!Array.isArray(vectorValue)) throw new TypeError("RAPTOR leaf vector is invalid"); const vector=dense(vectorValue,"RAPTOR leaf vector",4096); if(vector.length<1||vector.some((value)=>typeof value!=="number"||!Number.isFinite(value)))throw new TypeError("RAPTOR leaf vector is invalid");
    dimension ??= vector.length; if (vector.length !== dimension) throw new TypeError("RAPTOR leaf dimensions differ");
    if (!Number.isSafeInteger(tokens) || (tokens as number) < 1 || (tokens as number) > 1_000_000) throw new TypeError("RAPTOR leaf token estimate is invalid");
    result.push(Object.freeze({id,vector:Object.freeze(vector as number[]),tokens:tokens as number}));
  }
  return result.sort((left,right)=>left.id.localeCompare(right.id));
}

/** Stable ID-sorted, token-greedy fallback. Oversized singletons remain flat. */
export function stableTokenPartition<T extends { readonly id: string; readonly tokens: number }>(input: readonly T[], tokenBudget: number): readonly (readonly T[])[] {
  if (!Array.isArray(input) || !Number.isSafeInteger(tokenBudget) || tokenBudget < 1 || tokenBudget > 10_000_000) throw new TypeError("RAPTOR token budget is invalid");
  const sorted = [...input].sort((left, right) => left.id.localeCompare(right.id));
  const groups: T[][] = []; let current: T[] = []; let used = 0;
  for (const item of sorted) {
    if (typeof item.id !== "string" || item.id.length === 0 || !Number.isSafeInteger(item.tokens) || item.tokens < 1) throw new TypeError("RAPTOR partition item is invalid");
    if (current.length > 0 && used + item.tokens > tokenBudget) { groups.push(current); current = []; used = 0; }
    current.push(item); used += item.tokens;
    if (item.tokens >= tokenBudget) { groups.push(current); current = []; used = 0; }
  }
  if (current.length > 0) groups.push(current);
  return Object.freeze(groups.map((group) => Object.freeze(group)));
}
function meanVector(nodes: readonly ClusterNode[]): readonly number[] {
  const dimension = nodes[0]?.vector.length ?? 0;
  return Object.freeze(Array.from({ length: dimension }, (_, index) => nodes.reduce((sum, node) => sum + node.vector[index]!, 0) / nodes.length));
}
function summaryNode(children: readonly ClusterNode[], level: number, seed: string): ClusterNode {
  const leafIds = [...new Set(children.flatMap((node) => node.leafIds))].sort();
  const id = sha256Hex(canonicalStringify({ domain: "raptor-cluster-v1", seed, level, leafIds }));
  return Object.freeze({ id, level, leafIds: Object.freeze(leafIds), vector: meanVector(children), tokens: children.reduce((sum, node) => sum + node.tokens, 0), summary: true });
}
function fallbackGroups(nodes: readonly ClusterNode[], budget: number): readonly (readonly ClusterNode[])[] { return stableTokenPartition(nodes, budget); }
function learnedGroups(nodes: readonly ClusterNode[], options: Required<Pick<ClusterDagOptions, "umapDimensions" | "globalNeighbors" | "localNeighbors" | "gmmMaxClusters" | "membershipThreshold">> & { seed: string }): readonly (readonly ClusterNode[])[] | null {
  if (nodes.length < 3) return null;
  try {
    const globalEmbedding = reduceUmapDetailed(nodes.map((node) => node.vector), { seed: options.seed, scope: "global", dimensions: options.umapDimensions, neighbors: options.globalNeighbors });
    const global = selectDiagonalGmm(globalEmbedding.embedding, { seed: `${options.seed}:global`, maxClusters: Math.min(options.gmmMaxClusters, nodes.length - 1), membershipThreshold: options.membershipThreshold });
    const refined = new Map<string, readonly ClusterNode[]>();
    for (const globalMembers of global.clusters) {
      const candidates = globalMembers.map((index) => nodes[index]!).sort((a, b) => a.id.localeCompare(b.id));
      if (candidates.length < 3) { refined.set(candidates.map((node) => node.id).join("\0"), Object.freeze(candidates)); continue; }
      try {
        const localSeed = `${options.seed}:local:${candidates.map((node) => node.id).join(",")}`;
        const localEmbedding = reduceUmapDetailed(candidates.map((candidate) => candidate.vector), { seed: localSeed, scope: "local", dimensions: options.umapDimensions, neighbors: options.localNeighbors });
        const local = selectDiagonalGmm(localEmbedding.embedding, { seed: localSeed, maxClusters: Math.min(options.gmmMaxClusters, candidates.length - 1), membershipThreshold: options.membershipThreshold });
        for (const membership of local.clusters) {
          const group = membership.map((index) => candidates[index]!).sort((a, b) => a.id.localeCompare(b.id));
          if (group.length > 0) refined.set(group.map((node) => node.id).join("\0"), Object.freeze(group));
        }
      } catch { refined.set(candidates.map((node) => node.id).join("\0"), Object.freeze(candidates)); }
    }
    const learnedCandidates = [...refined.values()];
    // A single full-membership cluster delegates to the bounded fallback,
    // which creates one parent when it fits instead of vacuous singleton carry.
    if (learnedCandidates.some((group) => group.length === nodes.length)) return null;
    const groups = learnedCandidates.filter((group) => group.length > 0 && group.length < nodes.length);
    const covered = new Set(groups.flatMap((group) => group.map((node) => node.id)));
    for (const node of nodes) if (!covered.has(node.id)) groups.push(Object.freeze([node]));
    return groups.length > 1 ? Object.freeze(groups) : null;
  } catch { return null; }
}

/** Build a deterministic soft-membership DAG. Every edge is exactly level+1. */
export function buildClusterDag(input: readonly ClusterLeaf[], inputOptions: ClusterDagOptions): ClusterDag {
  const leaves = snapshotLeaves(input); const seed = String(inputOptions.seed);
  const maxLevels = inputOptions.maxLevels; const tokenBudget = inputOptions.tokenBudget;
  if (seed.length === 0 || seed.length > 4096 || !Number.isSafeInteger(maxLevels) || maxLevels < 1 || maxLevels > 10 || !Number.isSafeInteger(tokenBudget) || tokenBudget < 1 || tokenBudget > 10_000_000) throw new TypeError("RAPTOR DAG options are invalid");
  const learned = { seed, umapDimensions: inputOptions.umapDimensions ?? 10, globalNeighbors: inputOptions.globalNeighbors ?? 15, localNeighbors: inputOptions.localNeighbors ?? 10, gmmMaxClusters: inputOptions.gmmMaxClusters ?? 50, membershipThreshold: inputOptions.membershipThreshold ?? 0.1 };
  const leafNodes: ClusterNode[] = leaves.map((leaf) => Object.freeze({ id: leaf.id, level: 0, leafIds: Object.freeze([leaf.id]), vector: leaf.vector, tokens: leaf.tokens, summary: false }));
  if (leafNodes.length === 0) return Object.freeze({ leafIds: Object.freeze([]), roots: Object.freeze([]), nodes: Object.freeze([]), edges: Object.freeze([]) });
  if (leafNodes.length === 1) return Object.freeze({ leafIds: Object.freeze([leafNodes[0]!.id]), roots: Object.freeze([leafNodes[0]!.id]), nodes: Object.freeze(leafNodes), edges: Object.freeze([]) });
  const all = new Map(leafNodes.map((node) => [node.id, node])); const edges = new Map<string, ClusterEdge>(); let frontier: readonly ClusterNode[] = leafNodes;
  for (let attempt = 1; attempt <= maxLevels && frontier.length > 1; attempt += 1) {
    const frontierLevels = new Set(frontier.map((node) => node.level));
    // Carried over-budget roots intentionally terminate their branch; never
    // combine them later with a deeper branch because that would skip levels.
    if (frontierLevels.size !== 1) break;
    const level = frontier[0]!.level + 1; if (level > maxLevels) break;
    let groups: readonly (readonly ClusterNode[])[] | null;
    if (frontier.length === 2) groups = frontier[0]!.tokens + frontier[1]!.tokens <= tokenBudget ? Object.freeze([Object.freeze([...frontier])]) : null;
    else groups = learnedGroups(frontier, { ...learned, seed: `${seed}:level:${level}` });
    if (groups === null) groups = fallbackGroups(frontier, tokenBudget);
    // A learned cluster that exceeds the summary budget is reclustered by the
    // deterministic token-aware partition before any parent/prompt is built.
    const boundedGroups: Array<readonly ClusterNode[]> = [];
    for (const group of groups) {
      if (group.reduce((sum, node) => sum + node.tokens, 0) <= tokenBudget) boundedGroups.push(group);
      else boundedGroups.push(...fallbackGroups(group, tokenBudget));
    }
    groups = Object.freeze(boundedGroups);
    if (groups.length === frontier.length && groups.every((group) => group.length === 1)) break;
    // Summaries are created only when every child is from the current frontier,
    // so every edge is exactly level-1 -> level and carried nodes never mix.
    const next = new Map<string, ClusterNode>();
    for (const group of groups) {
      if (group.length === 1) { next.set(group[0]!.id, group[0]!); continue; }
      const parent = summaryNode(group, level, seed); next.set(parent.id, parent); all.set(parent.id, parent);
      for (const child of group) edges.set(`${parent.id}\0${child.id}`, Object.freeze({ parentId: parent.id, childId: child.id }));
    }
    const nextFrontier = [...next.values()].sort((a, b) => a.id.localeCompare(b.id));
    if (nextFrontier.length >= frontier.length && nextFrontier.every((node, index) => node.id === frontier[index]?.id)) break;
    frontier = Object.freeze(nextFrontier);
  }
  const nodes = [...all.values()].sort((a, b) => a.level - b.level || a.id.localeCompare(b.id));
  const levelById = new Map(nodes.map((node) => [node.id, node.level]));
  for (const edge of edges.values()) if (levelById.get(edge.parentId) !== (levelById.get(edge.childId) ?? -2) + 1) throw new TypeError("RAPTOR DAG edge skips a level");
  return Object.freeze({ leafIds: Object.freeze(leaves.map((leaf) => leaf.id)), roots: Object.freeze([...frontier].map((node) => node.id).sort()), nodes: Object.freeze(nodes), edges: Object.freeze([...edges.values()].sort((a, b) => a.parentId.localeCompare(b.parentId) || a.childId.localeCompare(b.childId))) });
}

export function evidenceClosure(dag: ClusterDag, rootIds: readonly string[] = dag.roots): readonly string[] {
  const children = new Map<string, string[]>(); for (const edge of dag.edges) { const list = children.get(edge.parentId) ?? []; list.push(edge.childId); children.set(edge.parentId, list); }
  const leaves = new Set(dag.leafIds); const result = new Set<string>(); const seen = new Set<string>(); const stack = [...rootIds];
  while (stack.length > 0) { const id = stack.pop()!; if (seen.has(id)) continue; seen.add(id); if (leaves.has(id)) result.add(id); for (const child of children.get(id) ?? []) stack.push(child); }
  return Object.freeze([...result].sort());
}
