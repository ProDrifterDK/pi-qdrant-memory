import { types as nodeTypes } from "node:util";
import { Worker, isMainThread, parentPort, workerData } from "node:worker_threads";
import { canonicalStringify, sha256Hex } from "../domain/canonical.js";
import { selectDiagonalGmm } from "./gmm.js";
import { reduceUmapDetailed } from "./umap.js";

export interface ClusterLeaf { readonly id: string; readonly vector: readonly number[]; readonly tokens: number; }
export interface ClusterNode { readonly id: string; readonly level: number; readonly leafIds: readonly string[]; readonly vector: readonly number[]; readonly tokens: number; readonly summary: boolean; }
export interface ClusterEdge { readonly parentId: string; readonly childId: string; }
export interface ClusterDag { readonly leafIds: readonly string[]; readonly roots: readonly string[]; readonly nodes: readonly ClusterNode[]; readonly edges: readonly ClusterEdge[]; }
export interface ClusterDagOptions { readonly seed: string | number; readonly maxLevels: number; readonly tokenBudget: number; readonly umapDimensions?: number; readonly globalNeighbors?: number; readonly localNeighbors?: number; readonly gmmMaxClusters?: number; readonly membershipThreshold?: number; }
export interface ClusterDagExecutionOptions { readonly signal?: AbortSignal; readonly timeoutMs: number; }

const CLUSTER_VECTOR_LIMIT = 4_096;
const CLUSTER_LEAF_LIMIT = 65_536;
const CLUSTER_PREPARE_CHUNK = 64;

function clusterWorkerUrl(): URL {
  // Vitest executes source TS while the release executes generated JS. Tests
  // deliberately use the staged generated worker so both paths exercise the
  // same isolated production kernel.
  return import.meta.url.endsWith(".ts")
    ? new URL("../../dist/raptor/cluster.js", import.meta.url)
    : new URL(import.meta.url);
}
function yieldToHost(): Promise<void> { return new Promise((resolve) => setImmediate(resolve)); }
function clusterAbortReason(signal: AbortSignal | undefined, deadline: number): Error | undefined {
  if (signal?.aborted) return new Error("RAPTOR clustering cancelled");
  if (Date.now() >= deadline) return new Error("RAPTOR clustering deadline exceeded");
  return undefined;
}

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
    const smallest = [...frontier].map((node) => node.tokens).sort((left, right) => left - right);
    // If even the two smallest nodes cannot share one summary prompt, every
    // valid partition is singleton. Avoid quadratic UMAP/GMM work and retain
    // the complete flat evidence frontier.
    if (smallest[0]! + smallest[1]! > tokenBudget) groups = null;
    else if (frontier.length === 2) groups = Object.freeze([Object.freeze([...frontier])]);
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

function snapshotClusterOptions(input: ClusterDagOptions): ClusterDagOptions {
  if (input === null || typeof input !== "object" || nodeTypes.isProxy(input) || (Object.getPrototypeOf(input) !== Object.prototype && Object.getPrototypeOf(input) !== null) || Object.getOwnPropertySymbols(input).length !== 0) throw new TypeError("RAPTOR DAG options are invalid");
  const allowed = new Set(["seed", "maxLevels", "tokenBudget", "umapDimensions", "globalNeighbors", "localNeighbors", "gmmMaxClusters", "membershipThreshold"]);
  if (Object.keys(input).some((key) => !allowed.has(key))) throw new TypeError("RAPTOR DAG options are invalid");
  const required = (key: "seed" | "maxLevels" | "tokenBudget"): unknown => own(input, key);
  const optional = (key: "umapDimensions" | "globalNeighbors" | "localNeighbors" | "gmmMaxClusters" | "membershipThreshold"): unknown => {
    const descriptor = Object.getOwnPropertyDescriptor(input, key);
    if (descriptor === undefined) return undefined;
    if (!("value" in descriptor) || descriptor.enumerable !== true) throw new TypeError("RAPTOR DAG options are invalid");
    return descriptor.value;
  };
  const seed = required("seed") as string | number;
  const maxLevels = required("maxLevels") as number;
  const tokenBudget = required("tokenBudget") as number;
  const umapDimensions = optional("umapDimensions") as number | undefined;
  const globalNeighbors = optional("globalNeighbors") as number | undefined;
  const localNeighbors = optional("localNeighbors") as number | undefined;
  const gmmMaxClusters = optional("gmmMaxClusters") as number | undefined;
  const membershipThreshold = optional("membershipThreshold") as number | undefined;
  return Object.freeze({ seed, maxLevels, tokenBudget,
    ...(umapDimensions === undefined ? {} : { umapDimensions }),
    ...(globalNeighbors === undefined ? {} : { globalNeighbors }),
    ...(localNeighbors === undefined ? {} : { localNeighbors }),
    ...(gmmMaxClusters === undefined ? {} : { gmmMaxClusters }),
    ...(membershipThreshold === undefined ? {} : { membershipThreshold }),
  });
}

/**
 * Execute the CPU-bound UMAP/GMM kernel outside the host event loop. Input
 * flattening yields cooperatively and the worker is terminated on the exact
 * abort/deadline boundary, leaving the prior generation active and retryable.
 */
export async function buildClusterDagOffThread(input: readonly ClusterLeaf[], inputOptions: ClusterDagOptions, execution: ClusterDagExecutionOptions): Promise<ClusterDag> {
  if (execution === null || typeof execution !== "object" || nodeTypes.isProxy(execution) || (Object.getPrototypeOf(execution) !== Object.prototype && Object.getPrototypeOf(execution) !== null) || Object.getOwnPropertySymbols(execution).length !== 0 || Object.keys(execution).some((key) => key !== "timeoutMs" && key !== "signal")) throw new TypeError("RAPTOR cluster execution options are invalid");
  const timeoutDescriptor = Object.getOwnPropertyDescriptor(execution, "timeoutMs");
  const signalDescriptor = Object.getOwnPropertyDescriptor(execution, "signal");
  if (timeoutDescriptor === undefined || !("value" in timeoutDescriptor) || timeoutDescriptor.enumerable !== true || (signalDescriptor !== undefined && (!("value" in signalDescriptor) || signalDescriptor.enumerable !== true))) throw new TypeError("RAPTOR cluster execution options are invalid");
  const timeoutMs = timeoutDescriptor.value;
  const signal = signalDescriptor?.value as AbortSignal | undefined;
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1_000 || timeoutMs > 120_000 || (signal !== undefined && (!(signal instanceof AbortSignal) || nodeTypes.isProxy(signal)))) throw new TypeError("RAPTOR cluster execution options are invalid");
  const options = snapshotClusterOptions(inputOptions);
  const values = dense(input, "RAPTOR leaves", CLUSTER_LEAF_LIMIT);
  if (values.length === 0) return buildClusterDag([], options);
  const deadline = Date.now() + timeoutMs;
  const firstAbort = clusterAbortReason(signal, deadline); if (firstAbort !== undefined) throw firstAbort;
  const ids: string[] = []; const tokens: number[] = []; const seen = new Set<string>(); let dimension: number | undefined; let flat: Float64Array | undefined;
  for (let leafIndex = 0; leafIndex < values.length; leafIndex += 1) {
    const candidate = values[leafIndex];
    if (typeof candidate !== "object" || candidate === null || nodeTypes.isProxy(candidate) || Object.getPrototypeOf(candidate) !== Object.prototype || Object.getOwnPropertySymbols(candidate).length !== 0) throw new TypeError("RAPTOR leaf is invalid");
    const id = own(candidate, "id"); const vectorValue = own(candidate, "vector"); const tokenValue = own(candidate, "tokens");
    if (typeof id !== "string" || id.length === 0 || id.length > 512 || seen.has(id) || !Array.isArray(vectorValue) || nodeTypes.isProxy(vectorValue) || Object.getPrototypeOf(vectorValue) !== Array.prototype || Object.getOwnPropertySymbols(vectorValue).length !== 0 || vectorValue.length < 1 || vectorValue.length > CLUSTER_VECTOR_LIMIT || !Number.isSafeInteger(tokenValue) || (tokenValue as number) < 1 || (tokenValue as number) > 1_000_000) throw new TypeError("RAPTOR leaf is invalid");
    seen.add(id); dimension ??= vectorValue.length;
    if (vectorValue.length !== dimension || Object.getOwnPropertyNames(vectorValue).length !== vectorValue.length + 1) throw new TypeError("RAPTOR leaf dimensions differ");
    if (flat === undefined) flat = new Float64Array(values.length * dimension);
    for (let vectorIndex = 0; vectorIndex < dimension; vectorIndex += 1) {
      const descriptor = Object.getOwnPropertyDescriptor(vectorValue, String(vectorIndex));
      if (descriptor === undefined || !("value" in descriptor) || descriptor.enumerable !== true || typeof descriptor.value !== "number" || !Number.isFinite(descriptor.value)) throw new TypeError("RAPTOR leaf vector is invalid");
      flat[leafIndex * dimension + vectorIndex] = descriptor.value;
    }
    ids.push(id); tokens.push(tokenValue as number);
    if ((leafIndex + 1) % CLUSTER_PREPARE_CHUNK === 0) { await yieldToHost(); const stopped = clusterAbortReason(signal, deadline); if (stopped !== undefined) throw stopped; }
  }
  const vectors = flat!;
  const remaining = deadline - Date.now(); if (remaining <= 0) throw new Error("RAPTOR clustering deadline exceeded");
  const vectorBuffer = vectors.buffer as ArrayBuffer;
  const worker = new Worker(clusterWorkerUrl(), { workerData: { kind: "raptor_cluster_v1", ids, tokens, dimension: dimension!, vectors: vectorBuffer, options }, transferList: [vectorBuffer] });
  return await new Promise<ClusterDag>((resolve, reject) => {
    let settled = false;
    const finish = (error: Error | undefined, dag?: ClusterDag): void => {
      if (settled) return; settled = true; clearTimeout(timer); signal?.removeEventListener("abort", onAbort);
      if (error !== undefined) { void worker.terminate(); reject(error); } else resolve(dag!);
    };
    const onAbort = (): void => finish(new Error("RAPTOR clustering cancelled"));
    const timer = setTimeout(() => finish(new Error("RAPTOR clustering deadline exceeded")), remaining); timer.unref?.();
    signal?.addEventListener("abort", onAbort, { once: true });
    worker.once("message", (message: unknown) => {
      if (typeof message !== "object" || message === null || !Object.prototype.hasOwnProperty.call(message, "ok")) { finish(new Error("RAPTOR clustering worker response is invalid")); return; }
      const payload = message as { ok?: unknown; dag?: ClusterDag };
      if (payload.ok !== true || payload.dag === undefined) { finish(new Error("RAPTOR clustering worker failed")); return; }
      finish(undefined, payload.dag);
    });
    worker.once("error", () => finish(new Error("RAPTOR clustering worker failed")));
    worker.once("exit", (code) => { if (code !== 0) finish(new Error("RAPTOR clustering worker exited")); });
  });
}

function runDedicatedClusterWorker(): void {
  if (isMainThread || parentPort === null || typeof workerData !== "object" || workerData === null || (workerData as { kind?: unknown }).kind !== "raptor_cluster_v1") return;
  try {
    const input = workerData as { readonly ids: readonly string[]; readonly tokens: readonly number[]; readonly dimension: number; readonly vectors: ArrayBuffer; readonly options: ClusterDagOptions };
    if (!Array.isArray(input.ids) || !Array.isArray(input.tokens) || input.ids.length !== input.tokens.length || !Number.isSafeInteger(input.dimension) || input.dimension < 1 || !(input.vectors instanceof ArrayBuffer)) throw new Error("RAPTOR clustering worker input is invalid");
    const flat = new Float64Array(input.vectors); if (flat.length !== input.ids.length * input.dimension) throw new Error("RAPTOR clustering worker vector shape is invalid");
    const leaves: ClusterLeaf[] = input.ids.map((id, index) => ({ id, tokens: input.tokens[index]!, vector: Array.from(flat.subarray(index * input.dimension, (index + 1) * input.dimension)) }));
    parentPort.postMessage({ ok: true, dag: buildClusterDag(leaves, input.options) });
  } catch { parentPort.postMessage({ ok: false }); }
}
runDedicatedClusterWorker();

export function evidenceClosure(dag: ClusterDag, rootIds: readonly string[] = dag.roots): readonly string[] {
  const children = new Map<string, string[]>(); for (const edge of dag.edges) { const list = children.get(edge.parentId) ?? []; list.push(edge.childId); children.set(edge.parentId, list); }
  const leaves = new Set(dag.leafIds); const result = new Set<string>(); const seen = new Set<string>(); const stack = [...rootIds];
  while (stack.length > 0) { const id = stack.pop()!; if (seen.has(id)) continue; seen.add(id); if (leaves.has(id)) result.add(id); for (const child of children.get(id) ?? []) stack.push(child); }
  return Object.freeze([...result].sort());
}
