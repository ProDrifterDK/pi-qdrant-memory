import { describe, expect, it, vi } from "vitest";
import { projectFilter, hostFilter } from "../../src/retrieval/filters.js";
import { mergeCandidates } from "../../src/retrieval/merge.js";
import type { MemoryCandidate } from "../../src/retrieval/search.js";
import { MemoryRetriever } from "../../src/retrieval/search.js";
import type { RetrievalConfig } from "../../src/types.js";

const config: RetrievalConfig = {
  topK: 5,
  candidatesPerLane: 20,
  minScore: 0.35,
  projectBoost: 0.05,
  contextBudgetChars: 1200,
  toolResultBudgetChars: 8000,
  hardContextCharBudget: 16000,
  timeoutMs: 2500,
};

function candidate(id: string | number, rawScore: number, lane: "project" | "host" = "project"): MemoryCandidate {
  return {
    id,
    text: `text-${id}`,
    rawScore,
    adjustedScore: rawScore,
    lane,
    sourceType: "conversation",
    sourceSystem: "hermes",
  };
}

function payload(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    host: "prime",
    status: "active",
    secret_scan: "passed",
    text: "safe memory",
    source_type: "conversation",
    source_system: "hermes",
    ...overrides,
  };
}

describe("retrieval filters", () => {
  it("constructs positive allowlist filters that the caller cannot weaken", () => {
    expect(projectFilter("prime", "project-1")).toEqual({ must: [
      { key: "host", match: { value: "prime" } },
      { key: "status", match: { value: "active" } },
      { key: "secret_scan", match: { value: "passed" } },
      { key: "project_id", match: { value: "project-1" } },
    ] });
    expect(hostFilter("prime", "project-1").must_not).toEqual([
      { key: "project_id", match: { value: "project-1" } },
    ]);
  });
});

describe("candidate merging", () => {
  it("boosts project hits only after raw thresholding", () => {
    const result = mergeCandidates({
      project: [candidate("project-low", 0.34), candidate("project-ok", 0.36)],
      host: [candidate("host-best", 0.40, "host")],
      minScore: 0.35,
      projectBoost: 0.05,
      limit: 5,
    });
    expect(result.map(item => item.id)).toEqual(["project-ok", "host-best"]);
    expect(result[0]?.adjustedScore).toBeCloseTo(0.41);
  });

  it("deduplicates by normalized ID, keeps the higher adjusted score, and ties by ID", () => {
    const result = mergeCandidates({
      project: [candidate("2", 0.4), candidate("same", 0.5)],
      host: [candidate(2, 0.45, "host"), candidate("same", 0.46, "host"), candidate("1", 0.45, "host")],
      minScore: 0.35,
      projectBoost: 0,
      limit: 99,
    });
    expect(result.map(item => item.id)).toEqual(["same", "1", "2"]);
    expect(result.find(item => item.id === "same")?.lane).toBe("project");
  });

  it("clamps the result limit to 1 through 10", () => {
    const many = Array.from({ length: 12 }, (_, index) => candidate(String(index), 1 - index / 100, "host"));
    expect(mergeCandidates({ project: [], host: many, minScore: 0, projectBoost: 0, limit: 100 })).toHaveLength(10);
    expect(mergeCandidates({ project: [], host: many, minScore: 0, projectBoost: 0, limit: 0 })).toHaveLength(1);
  });
});

describe("MemoryRetriever", () => {
  it("embeds once and searches exactly two lanes with mandatory filters", async () => {
    const embedding = vi.fn(async () => [1, 0, 0]);
    const searches = vi.fn(async (input: { vector: number[]; limit: number; filter: unknown }) => {
      expect(input.vector).toEqual([1, 0, 0]);
      expect(input.limit).toBe(20);
      return (input.filter as { must: Array<{ key: string }> }).must.some(condition => condition.key === "project_id")
        ? [{ id: 1, score: 0.9, payload: payload({ project_id: "project-1", project_label: "repo" }) }]
        : [
          { id: "another-project", score: 0.85, payload: payload({ project_id: "project-2", project_label: "other" }) },
          { id: "global", score: 0.8, payload: payload({}) },
        ];
    });
    const retriever = new MemoryRetriever({
      embeddings: { embedQuery: embedding } as never,
      qdrant: { search: searches } as never,
      config,
    });

    await expect(retriever.search({ query: "normalized query", host: "prime", project: { id: "project-1", label: "repo" } })).resolves.toEqual({
      query: "normalized query",
      hits: [
        expect.objectContaining({ id: "1", lane: "project", projectId: "project-1" }),
        expect.objectContaining({ id: "another-project", lane: "host", projectId: "project-2" }),
        expect.objectContaining({ id: "global", lane: "host", adjustedScore: 0.8 }),
      ],
    });
    expect(embedding).toHaveBeenCalledTimes(1);
    expect(searches).toHaveBeenCalledTimes(2);
    expect(searches.mock.calls.map(call => call[0]?.limit)).toEqual([20, 20]);
    for (const [input] of searches.mock.calls) {
      const filter = input?.filter as { must: Array<unknown>; must_not?: Array<unknown> };
      expect(filter.must).toEqual(expect.arrayContaining([
        { key: "host", match: { value: "prime" } },
        { key: "status", match: { value: "active" } },
        { key: "secret_scan", match: { value: "passed" } },
      ]));
    }
  });

  it("rejects unsafe payloads and malformed provenance without stringifying objects", async () => {
    const unsafe = [
      { id: 1, score: 0.99, payload: payload({ host: "pi" }) },
      { id: 2, score: 0.98, payload: payload({ status: undefined }) },
      { id: 3, score: 0.97, payload: payload({ secret_scan: undefined }) },
      { id: 4, score: 0.96, payload: payload({ status: "stale" }) },
      { id: 5, score: 0.95, payload: payload({ text: "   " }) },
      { id: 6, score: 0.94, payload: payload({ source_type: { evil: "object" } }) },
      { id: 7, score: 0.93, payload: payload({ source_system: { evil: "object" } }) },
      { id: 8, score: 0.92, payload: payload({ project_id: { evil: "object" } }) },
    ];
    const searches = vi.fn(async (input: { filter: unknown }) =>
      (input.filter as { must: Array<{ key: string }> }).must.some(condition => condition.key === "project_id")
        ? unsafe
        : [
          { id: "ok", score: 0.9, payload: payload({ source_type: "tool", project_id: undefined }) },
          { id: "another", score: 0.89, payload: payload({ project_id: "project-2", project_label: "other" }) },
        ],
    );
    const retriever = new MemoryRetriever({
      embeddings: { embedQuery: vi.fn(async () => [1]) } as never,
      qdrant: { search: searches } as never,
      config: { ...config, minScore: 0 },
    });
    const result = await retriever.search({ query: "q", host: "prime", project: { id: "project-1", label: "repo" } });
    expect(result.hits.map(hit => hit.id)).toEqual(["ok", "another"]);
    expect(result.hits.some(hit => hit.id === "[object Object]")).toBe(false);
  });

  it("uses the explicit limit, clamps it, and passes the abort signal to both searches", async () => {
    const signal = new AbortController().signal;
    const searches = vi.fn(async (_input: unknown) => []);
    const retriever = new MemoryRetriever({
      embeddings: { embedQuery: vi.fn(async (_query: string, received?: AbortSignal) => { expect(received).toBe(signal); return [1]; }) } as never,
      qdrant: { search: searches } as never,
      config,
    });
    await retriever.search({ query: "q", host: "prime", project: { id: "project-1", label: "repo" }, limit: 99, signal });
    expect(searches).toHaveBeenCalledTimes(2);
    for (const [input] of searches.mock.calls) expect(input?.signal).toBe(signal);
  });
});
