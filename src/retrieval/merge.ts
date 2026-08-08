import type { MemoryCandidate } from "./search.js";

function clampLimit(value: number): number {
  if (Number.isNaN(value)) return 1;
  if (value === Number.POSITIVE_INFINITY) return 10;
  if (value === Number.NEGATIVE_INFINITY) return 1;
  return Math.min(10, Math.max(1, Math.trunc(value)));
}

function normalizedId(value: unknown): string | undefined {
  if (typeof value === "string" && value.length > 0) return value;
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) return String(value);
  return undefined;
}

export function mergeCandidates(input: {
  project: MemoryCandidate[];
  host: MemoryCandidate[];
  minScore: number;
  projectBoost: number;
  limit: number;
}): MemoryCandidate[] {
  const byId = new Map<string, MemoryCandidate>();
  for (const [lane, candidates] of [["project", input.project], ["host", input.host]] as const) {
    for (const candidate of candidates) {
      if (!Number.isFinite(candidate.rawScore) || candidate.rawScore < input.minScore) continue;
      const id = normalizedId(candidate.id);
      if (id === undefined) continue;
      const adjustedScore = lane === "project" ? candidate.rawScore + input.projectBoost : candidate.rawScore;
      const normalized: MemoryCandidate = { ...candidate, id, lane, adjustedScore };
      const previous = byId.get(id);
      if (previous === undefined || normalized.adjustedScore > previous.adjustedScore) {
        byId.set(id, normalized);
      }
    }
  }
  return [...byId.values()]
    .sort((left, right) => {
      const scoreOrder = right.adjustedScore - left.adjustedScore;
      if (scoreOrder !== 0) return scoreOrder;
      return left.id < right.id ? -1 : left.id > right.id ? 1 : 0;
    })
    .slice(0, clampLimit(input.limit));
}
