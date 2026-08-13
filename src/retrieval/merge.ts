import type { MemoryCandidate } from "./search.js";

const RRF_K = 60;
function clampLimit(value: number): number { return Number.isFinite(value) ? Math.min(10, Math.max(1, Math.trunc(value))) : value === Number.POSITIVE_INFINITY ? 10 : 1; }
function key(candidate: MemoryCandidate): string {
  if (candidate.lane === "historical" && candidate.observationId !== undefined) return `observation:${candidate.observationId}`;
  if (candidate.contentId !== undefined) return `content:${candidate.contentId}`;
  if (candidate.evidenceIds.length > 0) return `evidence:${[...candidate.evidenceIds].sort().join("\u0000")}`;
  return `id:${candidate.id}`;
}

/** Deterministic reciprocal-rank fusion across incomparable internal lane scores. */
export function mergeCandidates(input: { lanes: readonly (readonly MemoryCandidate[])[]; limit: number; projectBoost: number }): MemoryCandidate[] {
  const fused = new Map<string, MemoryCandidate>();
  for (const lane of input.lanes) {
    const ranked = [...lane].filter((candidate) => Number.isFinite(candidate.rawScore)).sort((left, right) => right.rawScore - left.rawScore || (left.id < right.id ? -1 : left.id > right.id ? 1 : 0));
    for (let index = 0; index < ranked.length; index += 1) {
      const candidate = ranked[index]!;
      const score = 1 / (RRF_K + index + 1) + (candidate.projectId !== undefined ? input.projectBoost : 0);
      const prior = fused.get(candidate.id);
      if (prior === undefined) fused.set(candidate.id, { ...candidate, adjustedScore: score, evidenceIds: [...candidate.evidenceIds] });
      else fused.set(candidate.id, { ...prior, adjustedScore: prior.adjustedScore + score });
    }
  }
  const byEvidence = new Map<string, MemoryCandidate>();
  for (const candidate of [...fused.values()].sort((a, b) => b.adjustedScore - a.adjustedScore || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0))) {
    const candidateKey = key(candidate);
    const prior = byEvidence.get(candidateKey);
    if (prior === undefined || candidate.adjustedScore > prior.adjustedScore || (candidate.adjustedScore === prior.adjustedScore && candidate.id < prior.id)) byEvidence.set(candidateKey, candidate);
  }
  return [...byEvidence.values()].sort((a, b) => b.adjustedScore - a.adjustedScore || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0)).slice(0, clampLimit(input.limit));
}
