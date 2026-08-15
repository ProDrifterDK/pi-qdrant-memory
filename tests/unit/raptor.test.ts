import { readFile } from "node:fs/promises";
import { describe, expect, it } from "vitest";
import { seedWords, Xoshiro128StarStar } from "../../src/raptor/random.js";
import { reduceUmap, reduceUmapPair } from "../../src/raptor/umap.js";
import { fitDiagonalGmm, selectDiagonalGmm } from "../../src/raptor/gmm.js";
import { buildClusterDag, buildClusterDagOffThread, evidenceClosure, stableTokenPartition } from "../../src/raptor/cluster.js";
import { groupRaptorLeavesByPolicy } from "../../src/raptor/builder.js";
import { processingPolicyHash, type ProcessingPolicy } from "../../src/domain/policy.js";

const vector = (x: number, y: number): number[] => [x, y];

describe("Task 10 deterministic RAPTOR core", () => {
  it("freezes SHA-256 seed expansion and xoshiro128** replay", () => {
    expect(seedWords("pi_memory:raptor-v1")).toEqual([0x088d91d3, 0x827b3396, 0xf52bc71f, 0xaabb4f96]);
    const rng = new Xoshiro128StarStar("pi_memory:raptor-v1");
    expect(Array.from({ length: 8 }, () => rng.nextUint32())).toEqual([3557339510, 4117261111, 868946786, 2504596727, 3853782219, 3922864389, 4055278877, 2875680613]);
    expect(new Xoshiro128StarStar("7").nextFloat()).toBeCloseTo(0.9147565984167159, 15);
    const bounded = new Xoshiro128StarStar("bounded");
    expect(Array.from({ length: 200 }, () => bounded.nextInt(7)).every((value) => value >= 0 && value < 7)).toBe(true);
  });

  it("handles UMAP base cases, equal vectors, bounds and deterministic global/local seeds", () => {
    expect(reduceUmap([], { seed: "s", scope: "global", dimensions: 10, neighbors: 10 })).toEqual([]);
    expect(reduceUmap([[4, 4]], { seed: "s", scope: "global", dimensions: 10, neighbors: 10 })).toEqual([[0]]);
    expect(reduceUmap([[4, 4], [4, 4]], { seed: "s", scope: "global", dimensions: 10, neighbors: 10 })).toEqual([[0], [0]]);
    const vectors = [vector(0, 0), vector(1, 1), vector(8, 8), vector(9, 9)];
    const first = reduceUmapPair(vectors, { seed: "manifest-hash", dimensions: 10, globalNeighbors: 99, localNeighbors: 10 });
    const second = reduceUmapPair(vectors, { seed: "manifest-hash", dimensions: 10, globalNeighbors: 99, localNeighbors: 10 });
    expect(first).toEqual(second);
    expect(first.global.parameters).toMatchObject({ nComponents: 2, nNeighbors: 3, scope: "global" });
    expect(first.local.parameters).toMatchObject({ nComponents: 2, nNeighbors: 3, scope: "local" });
    expect(first.global.embedding).toHaveLength(4);
    expect(first.global.embedding.every((row) => row.length === 2 && row.every(Number.isFinite))).toBe(true);
  });

  it("matches the frozen diagonal GMM BIC formula and soft membership contract", () => {
    const fit = fitDiagonalGmm([[0, 0], [1, 1], [9, 9]], { seed: "7", components: 2 });
    expect(fit.components).toBe(2);
    expect(fit.parameterCount).toBe(9);
    expect(fit.bic).toBeCloseTo(-2 * fit.logLikelihood + fit.parameterCount * Math.log(3), 10);
    expect(fit.memberships.every((row) => Math.abs(row.reduce((a, b) => a + b, 0) - 1) < 1e-9)).toBe(true);
    const selected = selectDiagonalGmm([[0, 0], [1, 1], [9, 9]], { seed: "7", maxClusters: 2, membershipThreshold: 0.1 });
    expect(selected.fit.components).toBe(2);
    expect(selected.clusters.flat().sort()).toEqual([0, 1, 2]);
    expect(selected.assignments.every((row) => row.length >= 1)).toBe(true);
  });

  it("falls back deterministically and builds an acyclic level-increasing soft DAG", () => {
    const leaves = [
      { id: "a", vector: vector(0, 0), tokens: 4 },
      { id: "b", vector: vector(0, 0), tokens: 4 },
      { id: "c", vector: vector(0, 0), tokens: 4 },
      { id: "d", vector: vector(0, 0), tokens: 4 },
    ];
    expect(stableTokenPartition(leaves, 8).map((group) => group.map((item) => item.id))).toEqual([["a", "b"], ["c", "d"]]);
    expect(buildClusterDag([], { seed: "s", maxLevels: 5, tokenBudget: 8 })).toMatchObject({ roots: [], nodes: [], edges: [] });
    const one = buildClusterDag([leaves[0]!], { seed: "s", maxLevels: 5, tokenBudget: 8 });
    expect(one.roots).toEqual(["a"]);
    const two = buildClusterDag(leaves.slice(0, 2), { seed: "s", maxLevels: 5, tokenBudget: 8 });
    expect(two.roots).toHaveLength(1);
    const dag = buildClusterDag(leaves, { seed: "s", maxLevels: 5, tokenBudget: 8 });
    const levels = new Map(dag.nodes.map((node) => [node.id, node.level]));
    expect(dag.edges.every((edge) => levels.get(edge.parentId)! === levels.get(edge.childId)! + 1)).toBe(true);
    expect(new Set(dag.edges.map((edge) => `${edge.parentId}->${edge.childId}`)).size).toBe(dag.edges.length);
    expect(new Set(dag.leafIds)).toEqual(new Set(leaves.map((leaf) => leaf.id)));
  });


  it("freezes same-seed edge Jaccard, leaf closure and the flat recall quality floor", () => {
    const corpus = Array.from({ length: 12 }, (_, index) => ({ id: `doc-${String(index).padStart(2, "0")}`, vector: [index < 6 ? index / 10 : 8 + index / 10, index < 6 ? index / 10 : 8 + index / 10], tokens: 2 }));
    const a = buildClusterDag(corpus, { seed: "quality-v1", maxLevels: 5, tokenBudget: 12, umapDimensions: 2, globalNeighbors: 6, localNeighbors: 4, gmmMaxClusters: 4, membershipThreshold: 0.1 });
    const b = buildClusterDag([...corpus].reverse(), { seed: "quality-v1", maxLevels: 5, tokenBudget: 12, umapDimensions: 2, globalNeighbors: 6, localNeighbors: 4, gmmMaxClusters: 4, membershipThreshold: 0.1 });
    const edgesA = new Set(a.edges.map((edge) => `${edge.parentId}->${edge.childId}`)); const edgesB = new Set(b.edges.map((edge) => `${edge.parentId}->${edge.childId}`));
    const union = new Set([...edgesA, ...edgesB]); const intersection = [...edgesA].filter((edge) => edgesB.has(edge));
    expect(union.size === 0 ? 1 : intersection.length / union.size).toBe(1);
    expect(evidenceClosure(a)).toEqual(corpus.map((item) => item.id).sort());
    const distance = (vector: readonly number[], query: readonly number[]) => vector.reduce((sum, value, i) => sum + (value - query[i]!) ** 2, 0);
    const relevant = new Set(["doc-00", "doc-01", "doc-02", "doc-03", "doc-04"]); const query = [0, 0];
    const flat = [...corpus].sort((x, y) => distance(x.vector, query) - distance(y.vector, query)).slice(0, 5).filter((item) => relevant.has(item.id)).length / relevant.size;
    const nodes = new Map(a.nodes.map((node) => [node.id, node]));
    const selectedRoots = [...a.roots].sort((left, right) => distance(nodes.get(left)!.vector, query) - distance(nodes.get(right)!.vector, query)).slice(0, Math.ceil(a.roots.length / 2));
    const descendedEvidence = new Set(evidenceClosure(a, selectedRoots));
    const hierarchical = corpus.filter((item) => descendedEvidence.has(item.id)).sort((x, y) => distance(x.vector, query) - distance(y.vector, query)).slice(0, 5).filter((item) => relevant.has(item.id)).length / relevant.size;
    expect(descendedEvidence.size).toBeLessThan(corpus.length);
    expect([...relevant].filter((id) => descendedEvidence.has(id)).length / relevant.size).toBeGreaterThanOrEqual(0.95);
    expect(hierarchical).toBeGreaterThanOrEqual(flat - 0.02);
  });

  it("separates incompatible producer-policy groups and never invents a shared destination", () => {
    const makePolicy = (embedding: string): ProcessingPolicy => { const pending = { id: "pending", ownerHost: "pi" as const, destinationIds: { qdrant: "qdrant:pi", embedding, llm: "llm:dedicated" }, originProvider: "provider", allowCrossProviderReplay: false, expiresAt: null, residency: "local", dataUse: "memory", policyRevision: "r1" }; return { ...pending, id: processingPolicyHash(pending) }; };
    const worker = makePolicy("embed:one"); const one = makePolicy("embed:one"); const two = makePolicy("embed:two");
    const leaf = (id: string, policy: ProcessingPolicy) => ({ id, text: "safe memory", vector: Array.from({ length: 1024 }, () => 0), tokens: 2, projectId: "project", eventAt: "2026-08-10T00:00:00.000Z", policy });
    const grouped = groupRaptorLeavesByPolicy([leaf("a", one), leaf("b", two)], worker);
    expect(grouped.groups).toHaveLength(1); expect(grouped.groups[0]!.leaves.map((item) => item.id)).toEqual(["a"]); expect(grouped.pendingIds).toEqual(["b"]);
  });

  it("never poisons mixed oversized-token corpora and keeps all edges level-adjacent", () => {
    for (let round = 0; round < 64; round += 1) {
      const corpus = Array.from({ length: 9 }, (_, index) => ({ id: `m-${round}-${index}`, vector: [index + round / 100, (index % 3) + round / 100], tokens: index === round % 9 ? 1000 : 2 }));
      const dag = buildClusterDag(corpus, { seed: `mixed-${round}`, maxLevels: 5, tokenBudget: 12, umapDimensions: 2, globalNeighbors: 4, localNeighbors: 3, gmmMaxClusters: 4, membershipThreshold: 0.1 });
      const levels = new Map(dag.nodes.map((node) => [node.id, node.level])); expect(dag.edges.every((edge) => levels.get(edge.parentId)! === levels.get(edge.childId)! + 1)).toBe(true); expect(evidenceClosure(dag)).toEqual(corpus.map((item) => item.id).sort());
    }
  });

  it("keeps N=2 flat when the summary budget cannot hold the pair", () => {
    const dag = buildClusterDag([{ id: "a", vector: [0, 0], tokens: 5 }, { id: "b", vector: [1, 1], tokens: 5 }], { seed: "s", maxLevels: 3, tokenBudget: 8 });
    expect(dag.roots).toEqual(["a", "b"]); expect(dag.edges).toEqual([]);
  });

  it("rejects proxies, accessors and sparse matrices without invoking attacker code", () => {
    let calls = 0; const proxyRow = new Proxy([0, 1], { get() { calls += 1; throw new Error("invoked"); } });
    expect(() => reduceUmap([proxyRow], { seed: "s", scope: "global", dimensions: 2, neighbors: 2 })).toThrow(/plain|proxy|row/i); expect(calls).toBe(0);
    expect(() => fitDiagonalGmm([proxyRow, [1, 2]], { seed: "s", components: 1 })).toThrow(/plain|proxy|row/i); expect(calls).toBe(0);
    const accessorLeaf = Object.create(Object.prototype) as Record<string, unknown>; Object.defineProperty(accessorLeaf, "id", { enumerable: true, get() { calls += 1; throw new Error("invoked"); } }); Object.defineProperty(accessorLeaf, "vector", { enumerable: true, value: [0, 0] }); Object.defineProperty(accessorLeaf, "tokens", { enumerable: true, value: 1 });
    expect(() => buildClusterDag([accessorLeaf as never], { seed: "s", maxLevels: 2, tokenBudget: 4 })).toThrow(/own data/i); expect(calls).toBe(0);
    const sparse = Array(2) as number[][]; sparse[0] = [0, 0]; expect(() => reduceUmap(sparse, { seed: "s", scope: "global", dimensions: 2, neighbors: 2 })).toThrow(/dense|sparse/i);
  });

  it("keeps the production worker kernel deterministic for varied high-dimensional vectors", async () => {
    const leaves = Array.from({ length: 41 }, (_, index) => ({ id: `leaf-${String(index).padStart(3, "0")}`, tokens: 4 + (index % 5), vector: Array.from({ length: 24 }, (_unused, dimension) => Math.sin((index + 1) * (dimension + 3)) + Math.cos(index * 0.17 + dimension * 0.31)) }));
    const options = { seed: "worker-varied", maxLevels: 3, tokenBudget: 40, umapDimensions: 10, globalNeighbors: 7, localNeighbors: 5, gmmMaxClusters: 8, membershipThreshold: 0.15 } as const;
    const expected = buildClusterDag(leaves, options); const actual = await buildClusterDagOffThread(leaves, options, { timeoutMs: 120_000 });
    expect(actual).toEqual(expected); expect(evidenceClosure(actual)).toHaveLength(41);
  });

  it("terminates an in-flight clustering worker on abort without blocking the host event loop", async () => {
    const leaves = Array.from({ length: 63 }, (_, index) => ({ id: `abort-${String(index).padStart(3, "0")}`, tokens: 8, vector: Array.from({ length: 128 }, (_unused, dimension) => Math.sin(index * 1.7 + dimension * 0.013) + Math.cos(index * dimension * 0.007)) }));
    const controller = new AbortController(); let hostTurnRan = false;
    const pending = buildClusterDagOffThread(leaves, { seed: "worker-abort", maxLevels: 4, tokenBudget: 64, umapDimensions: 10, globalNeighbors: 8, localNeighbors: 6, gmmMaxClusters: 50, membershipThreshold: 0.1 }, { signal: controller.signal, timeoutMs: 120_000 });
    setImmediate(() => { hostTurnRan = true; controller.abort(); });
    await expect(pending).rejects.toThrow(/cancelled/u); expect(hostTurnRan).toBe(true);
  });

  it("preserves the exact umap-js 1.4.0 Apache tarball notice", async () => {
    const pinned = await readFile(new URL("../../node_modules/umap-js/LICENSE", import.meta.url), "utf8");
    const copied = await readFile(new URL("../../src/vendor/umap-license-apache-2.0.txt", import.meta.url), "utf8");
    expect(copied).toBe(pinned);
    expect(copied).toContain("Apache License");
    expect(copied).toContain("Version 2.0, January 2004");
  });
});
