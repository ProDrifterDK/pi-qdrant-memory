import { canonicalStringify } from "../domain/canonical.js";
import { parseMemoryRecord, type ControlRecord, type CuratedCurrentRecord, type CuratedMemoryRecord, type EpisodeRecord, type MemoryRecord, type ProcessingPolicyRecord, type RaptorSummaryRecord, type TombstoneRecord } from "../domain/records.js";
import { isPolicyExpired } from "../domain/policy.js";
import type { AuthorizedDestination, HostId, RetrievalConfig, RuntimeConfig } from "../types.js";
import type { ProjectIdentity } from "../project.js";
import { BoundEmbeddingDestination } from "../clients/embeddings.js";
import { laneFilter, GuardedLaneFilter, type FilterCondition, type RetrievalLane } from "./filters.js";
import { mergeCandidates } from "./merge.js";
import type { MemorySearchMode } from "../tool.js";
import { fetchJson, fetchOk, MemoryClientError } from "../clients/http.js";
import { tombstoneId } from "../domain/ids.js";
import { expectedQdrantCollection, physicalPointIdFor, type QdrantClientOptions } from "../qdrant/client.js";
import { COLLECTION_CONTROL_ID } from "../qdrant/schema.js";
import { recordFromPayload } from "../qdrant/write.js";
import { bindConfiguredDestination } from "../security/egress.js";

export type CandidateLane = "current" | "historical" | "episodes" | "curated" | "raptor" | "exact";
export interface MemoryCandidate {
  id: string; text: string; rawScore: number; adjustedScore: number; lane: CandidateLane;
  recordType: "episode" | "curated_memory" | "curated_current";
  projectId?: string; scope?: string; projectLabel?: string; sourceType: string; sourceSystem: string; createdAt?: string; expiresAt?: string;
  validFrom?: string; validTo?: string; policyEpoch?: number;
  evidenceIds: string[]; contentId?: string; observationId?: string; stateKey?: string;
  processingPolicyId: string; authorizationPolicyIds?: string[]; privacyEpoch: number;
}
export interface MemorySearchResult { query: string; hits: MemoryCandidate[]; }
export interface RankedMemoryRecord { record: MemoryRecord; score: number; }
export interface MemoryRecordRef { recordType: MemoryRecord["recordType"]; id: string; }
export interface MemoryReadRequest { lane: RetrievalLane; filter: GuardedLaneFilter; limit: number; vector?: readonly number[]; signal?: AbortSignal; }
export interface ExactReadRequest { query: string; filter: GuardedLaneFilter; limit: number; signal?: AbortSignal; }
export interface MemoryReadStore {
  readonly destination: AuthorizedDestination;
  readControl(): Promise<ControlRecord>;
  search(input: MemoryReadRequest): Promise<RankedMemoryRecord[]>;
  exact(input: ExactReadRequest): Promise<RankedMemoryRecord[]>;
  retrieve(refs: readonly MemoryRecordRef[]): Promise<MemoryRecord[]>;
  retrieveEvidence(ids: readonly string[]): Promise<MemoryRecord[]>;
  readPolicies(ids: readonly string[]): Promise<ProcessingPolicyRecord[]>;
  readTombstones(targetIds: readonly string[]): Promise<TombstoneRecord[]>;
  health(signal?: AbortSignal): Promise<void>;
  collectionInfo(signal?: AbortSignal): Promise<{ dimension: number; distance: string }>;
}

export interface MemoryRetrieverInput {
  query: string; host: HostId; project: ProjectIdentity; isChild: boolean; modelDestination: AuthorizedDestination;
  mode?: MemorySearchMode; after?: string; before?: string; limit?: number; signal?: AbortSignal;
}

function clampLimit(value: number): number { return Number.isFinite(value) ? Math.min(10, Math.max(1, Math.trunc(value))) : 1; }
function exactText(record: MemoryRecord): string {
  if (record.recordType === "episode") return [record.text, record.toolName, record.toolArgs, record.errorFingerprint].filter((value): value is string => typeof value === "string").join(" ");
  if (record.recordType === "curated_memory") return [record.text, record.subject, record.predicate, typeof record.value === "string" ? record.value : undefined].filter((value): value is string => typeof value === "string").join(" ");
  if (record.recordType === "curated_current" && record.resolution === "resolved") return record.text;
  return "";
}
function matchesExact(query: string, record: MemoryRecord): boolean {
  const haystack = exactText(record).normalize("NFKC").toLocaleLowerCase("en-US");
  const terms = query.normalize("NFKC").toLocaleLowerCase("en-US").split(/[^\p{L}\p{N}._:/-]+/u).filter((term) => term.length > 0).slice(0, 32);
  return terms.length > 0 && terms.every((term) => haystack.includes(term));
}
function safeEpisodeText(record: EpisodeRecord): string {
  if (record.text !== undefined && record.text.trim().length > 0) return record.text;
  if (record.toolName !== undefined) return record.eventKind === "tool_error" ? `Tool error: ${record.toolName}` : `Tool: ${record.toolName}`;
  return `Memory event: ${record.eventKind}`;
}
function candidate(record: EpisodeRecord | CuratedMemoryRecord | CuratedCurrentRecord, score: number, lane: CandidateLane, projectLabel?: string): MemoryCandidate | null {
  if (record.recordType === "curated_current" && record.resolution !== "resolved") return null;
  const text = record.recordType === "episode" ? safeEpisodeText(record) : record.text ?? "";
  if (text.trim().length === 0 || text.length > 16_000 || !Number.isFinite(score)) return null;
  const evidenceIds = record.recordType === "episode" ? [record.id] : [...(record.sourceEpisodeIds ?? [])];
  if (evidenceIds.length === 0 || evidenceIds.length > 1024 || new Set(evidenceIds).size !== evidenceIds.length) return null;
  const result: MemoryCandidate = { id: record.id, text, rawScore: score, adjustedScore: score, lane, recordType: record.recordType, sourceType: record.recordType === "episode" ? record.eventKind : record.recordType, sourceSystem: record.ownerHost, createdAt: record.createdAt, evidenceIds, processingPolicyId: record.processingPolicyId, privacyEpoch: record.privacyEpoch };
  if (record.expiresAt !== null) result.expiresAt = record.expiresAt;
  if (record.recordType === "episode") { result.projectId = record.projectId; result.scope = "project"; }
  else { if (record.projectId !== undefined) result.projectId = record.projectId; if (record.scope !== undefined) result.scope = record.scope; }
  if (projectLabel !== undefined && record.projectId !== undefined) result.projectLabel = projectLabel;
  if (record.recordType !== "episode") {
    if (record.contentId !== undefined) result.contentId = record.contentId;
    if (record.observationId !== undefined) result.observationId = record.observationId;
    if (record.stateKey !== undefined) result.stateKey = record.stateKey;
    if (record.recordType === "curated_memory") result.validFrom = record.effectiveAt;
    result.policyEpoch = record.coordinationPolicyEpoch;
  }
  return result;
}

/** Chronological historical intervals within one policy/state stream. Only adjacent equal content is collapsed, so A→B→A remains visible. */
export function historicalIntervals(input: readonly MemoryCandidate[]): MemoryCandidate[] {
  const groups = new Map<string, MemoryCandidate[]>();
  for (const candidate of input) {
    if (candidate.lane !== "historical" || candidate.recordType !== "curated_memory" || candidate.validFrom === undefined || candidate.stateKey === undefined || candidate.contentId === undefined || candidate.policyEpoch === undefined) continue;
    const key = canonicalStringify([candidate.policyEpoch, candidate.stateKey, candidate.scope ?? null, candidate.projectId ?? null]); const group = groups.get(key) ?? []; group.push(candidate); groups.set(key, group);
  }
  const result: MemoryCandidate[] = [];
  for (const group of groups.values()) {
    const sorted = [...group].sort((left, right) => left.validFrom!.localeCompare(right.validFrom!) || left.id.localeCompare(right.id)); const collapsed: MemoryCandidate[] = [];
    for (const value of sorted) { const prior = collapsed.at(-1); if (prior?.contentId === value.contentId) continue; collapsed.push({ ...value, evidenceIds: [...value.evidenceIds] }); }
    for (let index = 0; index < collapsed.length; index += 1) { const next = collapsed[index + 1]; if (next?.validFrom !== undefined) collapsed[index] = { ...collapsed[index]!, validTo: next.validFrom }; }
    result.push(...collapsed);
  }
  return result.sort((left, right) => left.policyEpoch! - right.policyEpoch! || left.stateKey!.localeCompare(right.stateKey!) || left.validFrom!.localeCompare(right.validFrom!) || left.id.localeCompare(right.id));
}

function tombstoneTargets(candidate: MemoryCandidate): string[] {
  return [...new Set([candidate.id, ...candidate.evidenceIds, candidate.contentId, candidate.observationId, candidate.stateKey].filter((value): value is string => typeof value === "string" && value.length > 0))].sort();
}
function sameControl(left: ControlRecord, right: ControlRecord): boolean { return canonicalStringify(left) === canonicalStringify(right); }
function isExpired(expiresAt: string | null | undefined, now: number, skew: number): boolean { return expiresAt !== null && expiresAt !== undefined && Date.parse(expiresAt) <= now + skew; }
function validProject(project: ProjectIdentity): boolean { return typeof project.id === "string" && project.id.length > 0 && project.id.length <= 512 && (project.identityKind === "registered" || project.identityKind === "local_only") && project.registrationValid !== false; }

/** Guarded hybrid retrieval. Every result passes a final stable-control, policy and tombstone barrier. */
export class MemoryRetriever {
  constructor(private readonly dependencies: { reader: MemoryReadStore; config: RetrievalConfig; embedding?: BoundEmbeddingDestination; embeddingDestination?: AuthorizedDestination; resolveEmbedding?: (control: ControlRecord, signal?: AbortSignal) => Promise<{ embedding: BoundEmbeddingDestination; destination: AuthorizedDestination } | undefined>; queryPrefix?: string; maxClockSkewMs?: number; now?: () => number }) {}

  async search(input: MemoryRetrieverInput): Promise<MemorySearchResult> {
    const query = input.query.trim();
    if (query.length === 0 || query.length > 4000 || !validProject(input.project) || (input.host !== "pi" && input.host !== "prime")) return { query, hits: [] };
    if (input.isChild && !this.dependencies.config.childSearch) return { query, hits: [] };
    try {
      const now = (this.dependencies.now ?? Date.now)();
      const control = parseMemoryRecord(await this.dependencies.reader.readControl()) as ControlRecord;
      if (control.recordType !== "collection_control" || control.ownerHost !== input.host || control.state !== "active") return { query, hits: [] };
      const readerDestination = this.dependencies.reader.destination;
      if (control.revokedDestinationIds.includes(readerDestination.id) || control.revokedDestinationIds.includes(input.modelDestination.id)) return { query, hits: [] };
      const preQueryControl = parseMemoryRecord(await this.dependencies.reader.readControl()); if (preQueryControl.recordType !== "collection_control" || !sameControl(control, preQueryControl)) return { query, hits: [] };
      const mode = input.mode ?? "all";
      const skew = this.dependencies.maxClockSkewMs ?? 0;
      const exactRecordTypes = mode === "episodes" ? ["episode"] as const : mode === "curated" ? ["curated_memory", "curated_current"] as const : ["episode", "curated_memory", "curated_current"] as const;
      const scopedFilter = (lane: RetrievalLane, global = false): GuardedLaneFilter => laneFilter({ ownerHost: input.host, lane, projectId: input.project.id, global, now, maxClockSkewMs: skew, privacyEpoch: control.privacyEpoch, coordinationPolicyEpoch: control.coordinationPolicyEpoch, ...(lane === "exact" ? { exactRecordTypes } : {}), ...(lane === "raptor" && control.activeGeneration !== null ? { activeGeneration: control.activeGeneration } : {}), ...(input.after === undefined ? {} : { after: input.after }), ...(input.before === undefined ? {} : { before: input.before }) });
      const laneLimit = this.dependencies.config.candidatesPerLane;
      const exactEnabled = mode !== "raptor" && mode !== "current" && mode !== "historical";
      const globalEnabled = !input.isChild && input.project.identityKind === "registered" && this.dependencies.config.rootScope === "project_and_global";
      const candidateInScope = (value: MemoryCandidate): boolean => value.scope === "project" ? value.projectId === input.project.id : value.scope === "global" ? globalEnabled : false;
      const exactHits = exactEnabled ? await this.dependencies.reader.exact({ query, filter: scopedFilter("exact"), limit: laneLimit, ...(input.signal === undefined ? {} : { signal: input.signal }) }) : [];
      const globalExactHits = exactEnabled && globalEnabled ? await this.dependencies.reader.exact({ query, filter: scopedFilter("exact", true), limit: laneLimit, ...(input.signal === undefined ? {} : { signal: input.signal }) }) : [];
      const exactCandidates: MemoryCandidate[] = [];
      for (const hit of [...exactHits, ...globalExactHits]) {
        const record = parseMemoryRecord(hit.record);
        if (!matchesExact(query, record) || (record.recordType !== "episode" && record.recordType !== "curated_memory" && record.recordType !== "curated_current") || mode === "episodes" && record.recordType !== "episode" || mode === "curated" && record.recordType === "episode") continue;
        const value = candidate(record, hit.score, "exact", input.project.label);
        if (value !== null && value.privacyEpoch === control.privacyEpoch && !isExpired(value.expiresAt, now, skew) && candidateInScope(value)) exactCandidates.push(value);
      }
      let denseVector: readonly number[] | undefined;
      let embedding = this.dependencies.embedding; let embeddingDestination = this.dependencies.embeddingDestination;
      if ((embedding === undefined || embeddingDestination === undefined) && this.dependencies.resolveEmbedding !== undefined) {
        try { const resolved = await this.dependencies.resolveEmbedding(control, input.signal); embedding = resolved?.embedding; embeddingDestination = resolved?.destination; } catch { embedding = undefined; embeddingDestination = undefined; }
      }
      if (embedding !== undefined && embeddingDestination !== undefined && BoundEmbeddingDestination.isValid(embedding) && embedding.destination.id === embeddingDestination.id && embedding.destination.residency === embeddingDestination.residency && embedding.destination.dataUse === embeddingDestination.dataUse && embedding.coordination.policyHash === control.coordinationPolicyHash && embedding.coordination.policyEpoch === control.coordinationPolicyEpoch && !control.revokedDestinationIds.includes(embeddingDestination.id)) {
        // Query embedding is itself policy-bound egress. Authorize it from the
        // active control policy or an exact, already-local candidate before
        // sending query text to the embedding destination.
        const preflightIds = [...new Set([control.processingPolicyId, ...exactCandidates.flatMap((value) => [value.processingPolicyId, ...(value.authorizationPolicyIds ?? [])])])].sort();
        const preflightRecords: ProcessingPolicyRecord[] = [];
        try { for (let index = 0; index < preflightIds.length; index += 1024) preflightRecords.push(...await this.dependencies.reader.readPolicies(preflightIds.slice(index, index + 1024))); }
        catch { preflightRecords.length = 0; }
        const preflightById = new Map(preflightRecords.map((raw) => [raw.id, raw]));
        const preflightAuthorized = preflightById.size === preflightIds.length && preflightIds.every((id) => {
          const raw = preflightById.get(id); if (raw === undefined) return false;
          const record = parseMemoryRecord(raw); if (record.recordType !== "processing_policy") return false;
          const policy = record.policy;
          return record.ownerHost === input.host && record.privacyEpoch === control.privacyEpoch && policy.ownerHost === input.host && policy.id === record.id && record.id === id && policy.destinationIds.qdrant === readerDestination.id && policy.destinationIds.embedding === embeddingDestination.id && policy.destinationIds.llm === input.modelDestination.id && policy.residency === readerDestination.residency && policy.dataUse === readerDestination.dataUse && policy.residency === embeddingDestination.residency && policy.dataUse === embeddingDestination.dataUse && policy.residency === input.modelDestination.residency && policy.dataUse === input.modelDestination.dataUse && !isPolicyExpired(policy, now, skew) && !control.revokedDestinationIds.includes(readerDestination.id) && !control.revokedDestinationIds.includes(embeddingDestination.id) && !control.revokedDestinationIds.includes(input.modelDestination.id);
        });
        if (preflightAuthorized) {
          try { denseVector = await embedding.embed({ model: "bge-m3", text: `${this.dependencies.queryPrefix ?? "search_query: "}${query}`, ...(input.signal === undefined ? {} : { signal: input.signal }) }); } catch { denseVector = undefined; }
          if (denseVector !== undefined) { const afterEmbedding = parseMemoryRecord(await this.dependencies.reader.readControl()) as ControlRecord; if (!sameControl(control, afterEmbedding)) return { query, hits: [] }; }
        }
      }
      const denseLanes: RetrievalLane[] = mode === "all" ? ["current", "historical", "episodes", "curated", "raptor"] : mode === "curated" ? ["current", "historical"] : [mode];
      const denseCandidates: MemoryCandidate[][] = []; const raptorSeeds: { record: RaptorSummaryRecord; score: number }[] = [];
      if (denseVector !== undefined) for (const lane of denseLanes) {
        if (lane === "raptor" && control.activeGeneration === null) continue;
        const hits = await this.dependencies.reader.search({ lane, filter: scopedFilter(lane), limit: laneLimit, vector: denseVector, ...(input.signal === undefined ? {} : { signal: input.signal }) });
        const canGlobalLane = globalEnabled && (lane === "current" || lane === "historical" || lane === "curated");
        const globalHits = canGlobalLane ? await this.dependencies.reader.search({ lane, filter: scopedFilter(lane, true), limit: laneLimit, vector: denseVector, ...(input.signal === undefined ? {} : { signal: input.signal }) }) : [];
        const laneCandidates: MemoryCandidate[] = [];
        for (const hit of [...hits, ...globalHits]) {
          if (!Number.isFinite(hit.score) || hit.score < this.dependencies.config.minScore) continue;
          const record = parseMemoryRecord(hit.record);
          if (record.recordType !== "episode" && record.recordType !== "curated_current" && record.recordType !== "curated_memory" && record.recordType !== "raptor_summary") continue;
          const typeMatches = lane === "episodes" ? record.recordType === "episode" : lane === "current" ? record.recordType === "curated_current" : lane === "historical" ? record.recordType === "curated_memory" : lane === "curated" ? record.recordType === "curated_memory" || record.recordType === "curated_current" : lane === "raptor" ? record.recordType === "raptor_summary" : false;
          if (!typeMatches || record.ownerHost !== input.host || record.privacyEpoch !== control.privacyEpoch || isExpired(record.expiresAt, now, skew)) continue;
          if (record.recordType === "raptor_summary") {
            if (lane === "raptor" && control.activeGeneration !== null && record.generationId === control.activeGeneration && record.coordinationPolicyEpoch === control.coordinationPolicyEpoch && record.coordinationPolicyHash === control.coordinationPolicyHash && record.memberIds !== undefined && record.memberIds.length > 0 && record.memberIds.length <= 1024 && record.coveredProjects.includes(input.project.id)) raptorSeeds.push({ record, score: hit.score });
            continue;
          }
          if (record.recordType !== "episode" && (record.coordinationPolicyEpoch !== control.coordinationPolicyEpoch || record.coordinationPolicyHash !== control.coordinationPolicyHash)) continue;
          const value = candidate(record, hit.score, lane as CandidateLane, input.project.label);
          if (value !== null && candidateInScope(value)) laneCandidates.push(value);
        }
        denseCandidates.push(lane === "historical" ? historicalIntervals(laneCandidates) : laneCandidates);
      }
      if (raptorSeeds.length > 0) {
        const pending = new Map<string, { score: number; policyIds: Set<string> }>();
        const queue = (id: string, score: number, policyIds: Iterable<string>): void => { const prior = pending.get(id); const ids = new Set([...(prior?.policyIds ?? []), ...policyIds]); pending.set(id, { score: Math.max(prior?.score ?? 0, score), policyIds: ids }); };
        for (const seed of raptorSeeds) for (const id of seed.record.memberIds ?? []) queue(id, seed.score, [seed.record.processingPolicyId]);
        const visited = new Set<string>(); const raptorEvidence: MemoryCandidate[] = [];
        for (let depth = 0; pending.size > 0 && depth < 10; depth += 1) {
          const batch = [...pending.entries()].filter(([id]) => !visited.has(id)).sort(([left], [right]) => left.localeCompare(right)).slice(0, 1024); pending.clear();
          if (batch.length === 0 || visited.size + batch.length > 8192) break;
          const ids = batch.map(([id]) => id); const records = await this.dependencies.reader.retrieveEvidence(ids);
          if (records.length !== ids.length || new Set(records.map((record) => record.id)).size !== records.length || records.some((record) => !ids.includes(record.id))) throw new TypeError("RAPTOR evidence descent is incomplete or ambiguous");
          const routeById = new Map(batch);
          for (const raw of records) {
            const record = parseMemoryRecord(raw); visited.add(record.id); const route = routeById.get(record.id); const score = route?.score ?? 0; const routePolicies = route?.policyIds ?? new Set<string>();
            if (record.ownerHost !== input.host || record.privacyEpoch !== control.privacyEpoch || isExpired(record.expiresAt, now, skew)) throw new TypeError("RAPTOR evidence crosses an authority boundary");
            if (record.recordType === "raptor_summary") {
              if (record.generationId !== control.activeGeneration || record.coordinationPolicyEpoch !== control.coordinationPolicyEpoch || record.coordinationPolicyHash !== control.coordinationPolicyHash || record.memberIds === undefined || record.memberIds.length === 0 || record.memberIds.length > 1024 || !record.coveredProjects.includes(input.project.id)) throw new TypeError("RAPTOR summary is not active");
              for (const id of record.memberIds) if (!visited.has(id)) queue(id, score, [...routePolicies, record.processingPolicyId]);
              continue;
            }
            if (record.recordType !== "episode" && record.recordType !== "curated_memory" && record.recordType !== "curated_current") throw new TypeError("RAPTOR leaf is not concrete memory evidence");
            if (record.recordType === "episode" && (record.projectId !== input.project.id || record.status !== "active" || record.secretScan !== "passed")) throw new TypeError("RAPTOR evidence is outside the project");
            if (record.recordType !== "episode" && (record.coordinationPolicyEpoch !== control.coordinationPolicyEpoch || record.coordinationPolicyHash !== control.coordinationPolicyHash)) throw new TypeError("RAPTOR derived evidence uses a stale policy");
            const value = candidate(record, score, "raptor", input.project.label); if (value !== null && candidateInScope(value)) { value.authorizationPolicyIds = [...new Set([record.processingPolicyId, ...routePolicies])].sort(); raptorEvidence.push(value); }
          }
        }
        if (pending.size > 0) throw new TypeError("RAPTOR evidence descent exceeded its bound");
        denseCandidates.push(raptorEvidence);
      }
      let candidates = mergeCandidates({ lanes: [exactCandidates, ...denseCandidates], limit: input.limit ?? this.dependencies.config.topK, projectBoost: this.dependencies.config.projectBoost });
      if (candidates.length === 0) return { query, hits: [] };
      const derivedCandidates = candidates.filter((value) => value.recordType === "curated_current" || value.recordType === "curated_memory");
      const evidenceIds = [...new Set(derivedCandidates.flatMap((value) => value.evidenceIds))].sort();
      const evidenceById = new Map<string, EpisodeRecord>();
      const evidenceScopes = new Map<string, MemoryCandidate[]>(); for (const value of derivedCandidates) for (const id of value.evidenceIds) { const refs = evidenceScopes.get(id) ?? []; refs.push(value); evidenceScopes.set(id, refs); }
      if (evidenceIds.length > 0) {
        const evidenceRecords: MemoryRecord[] = [];
        for (let index = 0; index < evidenceIds.length; index += 1024) {
          const chunk = evidenceIds.slice(index, index + 1024);
          evidenceRecords.push(...await this.dependencies.reader.retrieve(chunk.map((id) => ({ recordType: "episode", id }))));
        }
        if (new Set(evidenceRecords.map((record) => `${record.recordType}:${record.id}`)).size !== evidenceRecords.length) return { query, hits: [] };
        for (const raw of evidenceRecords) {
          const record = parseMemoryRecord(raw); if (record.recordType !== "episode") return { query, hits: [] }; const refs = evidenceScopes.get(record.id) ?? [];
          const scopeAllowed = refs.length > 0 && refs.every((value) => value.scope === "global" ? globalEnabled && record.projectIdentityKind === "registered" : value.scope === "project" && value.projectId === input.project.id && record.projectId === input.project.id);
          if (!evidenceIds.includes(record.id) || record.ownerHost !== input.host || !scopeAllowed || record.privacyEpoch !== control.privacyEpoch || record.status !== "active" || record.secretScan !== "passed" || isExpired(record.expiresAt, now, skew)) return { query, hits: [] };
          evidenceById.set(record.id, record);
        }
        candidates = candidates.filter((value) => value.recordType !== "curated_current" && value.recordType !== "curated_memory" || value.evidenceIds.length > 0 && value.evidenceIds.every((id) => evidenceById.has(id)));
      } else if (derivedCandidates.length > 0) candidates = candidates.filter((value) => value.recordType !== "curated_current" && value.recordType !== "curated_memory");
      const policyIds = [...new Set([...candidates.flatMap((value) => [value.processingPolicyId, ...(value.authorizationPolicyIds ?? [])]), ...[...evidenceById.values()].map((value) => value.processingPolicyId)])].sort();
      const policyRecords: ProcessingPolicyRecord[] = [];
      for (let index = 0; index < policyIds.length; index += 1024) policyRecords.push(...await this.dependencies.reader.readPolicies(policyIds.slice(index, index + 1024)));
      if (policyRecords.length !== policyIds.length) return { query, hits: [] };
      const policies = new Map<string, ProcessingPolicyRecord>();
      for (const raw of policyRecords) {
        const parsed = parseMemoryRecord(raw);
        if (parsed.recordType !== "processing_policy" || !policyIds.includes(parsed.id) || policies.has(parsed.id)) return { query, hits: [] };
        policies.set(parsed.id, parsed);
      }
      const destinationAuthorized = (processingPolicyId: string): boolean => {
        const record = policies.get(processingPolicyId); const policy = record?.policy;
        return record !== undefined && policy !== undefined && record.ownerHost === input.host && record.privacyEpoch === control.privacyEpoch && policy.ownerHost === input.host && policy.id === processingPolicyId && policy.destinationIds.qdrant === readerDestination.id && policy.destinationIds.llm === input.modelDestination.id && (denseVector === undefined || embeddingDestination !== undefined && policy.destinationIds.embedding === embeddingDestination.id && policy.residency === embeddingDestination.residency && policy.dataUse === embeddingDestination.dataUse && !control.revokedDestinationIds.includes(embeddingDestination.id)) && policy.residency === readerDestination.residency && policy.dataUse === readerDestination.dataUse && policy.residency === input.modelDestination.residency && policy.dataUse === input.modelDestination.dataUse && !isPolicyExpired(policy, now, skew) && !control.revokedDestinationIds.includes(input.modelDestination.id);
      };
      const authorized = candidates.filter((value) => !isExpired(value.expiresAt, now, skew) && [value.processingPolicyId, ...(value.authorizationPolicyIds ?? [])].every(destinationAuthorized) && value.evidenceIds.every((id) => { const evidence = evidenceById.get(id); return evidence === undefined ? value.recordType === "episode" && id === value.id : destinationAuthorized(evidence.processingPolicyId); }));
      if (authorized.length === 0) return { query, hits: [] };
      const targets = [...new Set(authorized.flatMap(tombstoneTargets))].sort();
      const tombstones: TombstoneRecord[] = [];
      for (let index = 0; index < targets.length; index += 8192) tombstones.push(...await this.dependencies.reader.readTombstones(targets.slice(index, index + 8192)));
      if (tombstones.length > 0) return { query, hits: [] };
      const finalControl = parseMemoryRecord(await this.dependencies.reader.readControl()) as ControlRecord;
      if (!sameControl(control, finalControl)) return { query, hits: [] };
      const limited = authorized.slice(0, clampLimit(input.limit ?? this.dependencies.config.topK));
      return { query, hits: mode === "historical" ? limited.sort((left, right) => (left.validFrom ?? "").localeCompare(right.validFrom ?? "") || left.id.localeCompare(right.id)) : limited };
    } catch { return { query, hits: [] }; }
  }
}

type JsonRecord = Record<string, unknown>;
interface WirePoint { id: string; payload: JsonRecord; vector?: { semantic: number[] }; score?: number; }
export interface GuardedMemoryReadStoreOptions extends QdrantClientOptions { destination: AuthorizedDestination; egressMode: RuntimeConfig["privacy"]["egressMode"]; nodeId?: string; }
interface ReaderOptions { endpoint: string; collection: "pi_memory" | "prime_memory"; ownerHost: "pi" | "prime"; destination: AuthorizedDestination; apiKey?: string; timeoutMs: number; readConsistency?: number | "majority" | "quorum" | "all"; maxClockSkewMs: number; fetchImpl?: typeof fetch; }
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/iu;
const STORE_ISSUER = Symbol("pi-qdrant-memory-v2.guarded-memory-reader");
function invalid(message: string): never { throw new MemoryClientError("invalid-response", message); }
function isRecord(value: unknown): value is JsonRecord { return typeof value === "object" && value !== null && !Array.isArray(value); }
function endpoint(value: string): string {
  let parsed: URL; try { parsed = new URL(value); } catch { throw new MemoryClientError("configuration", "Qdrant endpoint is invalid"); }
  if (!['http:', 'https:'].includes(parsed.protocol) || parsed.username !== "" || parsed.password !== "" || parsed.search !== "" || parsed.hash !== "") throw new MemoryClientError("configuration", "Qdrant endpoint is invalid");
  return parsed.toString().replace(/\/+$/u, "");
}
function snapshotOptions(input: GuardedMemoryReadStoreOptions): ReaderOptions {
  const baseUrl = input.baseUrl; const collection = input.collection; const ownerHost = input.ownerHost; const apiKey = input.apiKey; const timeoutMs = input.timeoutMs; const readConsistency = input.readConsistency; const maxClockSkewMs = input.maxClockSkewMs ?? 0; const fetchImpl = input.fetchImpl;
  if ((ownerHost !== "pi" && ownerHost !== "prime") || collection !== expectedQdrantCollection(ownerHost) || !Number.isFinite(timeoutMs) || timeoutMs <= 0 || !Number.isFinite(maxClockSkewMs) || maxClockSkewMs < 0) throw new MemoryClientError("configuration", "Qdrant reader configuration is invalid");
  if (apiKey !== undefined && (typeof apiKey !== "string" || apiKey.length === 0)) throw new MemoryClientError("configuration", "Qdrant reader configuration is invalid");
  const inputDestination = input.destination; const configuredDestination = Object.freeze({ id: inputDestination.id, residency: inputDestination.residency, dataUse: inputDestination.dataUse }); const egressMode = input.egressMode; const nodeId = input.nodeId; const canonicalEndpoint = endpoint(baseUrl);
  const destination = bindConfiguredDestination({ endpoint: canonicalEndpoint, configuredDestination, requestedDestination: configuredDestination, egressMode, ...(nodeId === undefined ? {} : { nodeId }) });
  const result: ReaderOptions = { endpoint: canonicalEndpoint, collection, ownerHost, destination, timeoutMs, maxClockSkewMs };
  if (apiKey !== undefined) result.apiKey = apiKey; if (readConsistency !== undefined) result.readConsistency = readConsistency; if (fetchImpl !== undefined) result.fetchImpl = fetchImpl;
  return Object.freeze(result);
}
function headers(options: ReaderOptions): Record<string, string> { const value: Record<string, string> = { "content-type": "application/json" }; if (options.apiKey !== undefined) value["api-key"] = options.apiKey; return value; }
function requestOptions(options: ReaderOptions, signal?: AbortSignal) { return { timeoutMs: options.timeoutMs, ...(signal === undefined ? {} : { signal }), ...(options.fetchImpl === undefined ? {} : { fetchImpl: options.fetchImpl }) }; }
function url(options: ReaderOptions, suffix: string): string {
  const value = new URL(`${options.endpoint}/collections/${encodeURIComponent(options.collection)}${suffix}`);
  if (options.readConsistency !== undefined) value.searchParams.set("consistency", String(options.readConsistency));
  return value.toString();
}
function envelope(value: unknown): unknown { if (!isRecord(value) || !("result" in value) || (value.status !== undefined && value.status !== "ok")) invalid("Qdrant response envelope is invalid"); return value.result; }
function vector(value: unknown): number[] | undefined {
  if (value === undefined) return undefined;
  if (!isRecord(value)) invalid("Qdrant semantic vector is invalid");
  const keys = Object.keys(value); if (keys.length === 0) return undefined;
  if (keys.length !== 1 || keys[0] !== "semantic" || !Array.isArray(value.semantic) || value.semantic.length !== 1024 || !value.semantic.every((part) => typeof part === "number" && Number.isFinite(part))) invalid("Qdrant semantic vector is invalid");
  return value.semantic.map(part => Math.fround(part)) as number[];
}
function point(value: unknown): WirePoint {
  if (!isRecord(value) || typeof value.id !== "string" || !UUID.test(value.id) || !isRecord(value.payload)) invalid("Qdrant point is invalid");
  const semantic = vector(value.vector);
  const result: WirePoint = { id: value.id, payload: value.payload }; if (semantic !== undefined) result.vector = { semantic };
  if (value.score !== undefined) { if (typeof value.score !== "number" || !Number.isFinite(value.score)) invalid("Qdrant score is invalid"); result.score = value.score; }
  return result;
}
function mandatory(filter: GuardedLaneFilter, ownerHost: "pi" | "prime"): void {
  if (!GuardedLaneFilter.isValid(filter) || !isRecord(filter) || !Array.isArray(filter.must) || !Array.isArray(filter.must_not) || !Array.isArray(filter.should)) throw new MemoryClientError("configuration", "Guarded Qdrant filter is invalid");
  const has = (key: string, expected: unknown): boolean => filter.must.some((condition) => "key" in condition && condition.key === key && "match" in condition && "value" in condition.match && condition.match.value === expected);
  if (!has("owner_host", ownerHost) || !has("status", "active") || !has("secret_scan", "passed") || !filter.must.some((condition) => "key" in condition && condition.key === "record_type") || !filter.must.some((condition) => "key" in condition && condition.key === "privacy_epoch") || !filter.should.some((condition) => "is_null" in condition && condition.is_null.key === "expires_at") || !filter.should.some((condition) => "key" in condition && condition.key === "expires_at" && "range" in condition && typeof condition.range.gt === "string")) throw new MemoryClientError("configuration", "Guarded Qdrant filter is incomplete");
}
function parseBound(pointValue: WirePoint, ownerHost: "pi" | "prime"): MemoryRecord {
  const record = recordFromPayload(pointValue.payload, ownerHost, pointValue.vector?.semantic);
  const expected = record.recordType === "collection_control" ? record.id : physicalPointIdFor(record.recordType, record.id);
  if (expected !== pointValue.id) invalid("Qdrant point identity is mismatched");
  return record;
}
function recordTypeConditions(filter: GuardedLaneFilter): string[] {
  for (const condition of filter.must) if ("key" in condition && condition.key === "record_type" && "match" in condition) {
    if ("value" in condition.match && typeof condition.match.value === "string") return [condition.match.value];
    if ("any" in condition.match && Array.isArray(condition.match.any) && condition.match.any.every((part) => typeof part === "string")) return [...condition.match.any] as string[];
  }
  return [];
}
function matchesCondition(payload: JsonRecord, condition: FilterCondition): boolean {
  if ("is_null" in condition) return payload[condition.is_null.key] === undefined || payload[condition.is_null.key] === null;
  const value = payload[condition.key];
  if ("match" in condition) {
    if ("value" in condition.match) return Array.isArray(value) ? value.includes(condition.match.value) : value === condition.match.value;
    const allowed = "any" in condition.match ? condition.match.any : [];
    return Array.isArray(value) ? value.some((part) => allowed.includes(part as never)) : allowed.includes(value as never);
  }
  if ((typeof value !== "string" && typeof value !== "number") || (typeof value === "number" && !Number.isFinite(value))) return false;
  const range = condition.range;
  if (range.gt !== undefined && !(value > range.gt)) return false;
  if (range.gte !== undefined && !(value >= range.gte)) return false;
  if (range.lte !== undefined && !(value <= range.lte)) return false;
  return true;
}
function payloadMatchesFilter(payload: JsonRecord, filter: GuardedLaneFilter): boolean {
  return filter.must.every((condition) => matchesCondition(payload, condition)) && !filter.must_not.some((condition) => matchesCondition(payload, condition)) && (filter.should.length === 0 || filter.should.some((condition) => matchesCondition(payload, condition)));
}

export class GuardedMemoryReadStore implements MemoryReadStore {
  readonly #options: ReaderOptions; readonly #issuer: symbol; readonly destination: AuthorizedDestination;
  constructor(options: GuardedMemoryReadStoreOptions, issuer: symbol) { if (issuer !== STORE_ISSUER) throw new TypeError("Guarded reader requires module issuer"); this.#issuer = issuer; this.#options = snapshotOptions(options); this.destination = Object.freeze({ ...this.#options.destination }); Object.freeze(this); }
  static isValid(value: unknown): value is GuardedMemoryReadStore { return typeof value === "object" && value !== null && #issuer in value && value instanceof GuardedMemoryReadStore && value.#issuer === STORE_ISSUER; }
  async health(signal?: AbortSignal): Promise<void> { await fetchOk(`${this.#options.endpoint}/healthz`, { method: "GET", headers: this.#options.apiKey === undefined ? {} : { "api-key": this.#options.apiKey } }, requestOptions(this.#options, signal)); }
  async collectionInfo(signal?: AbortSignal): Promise<{ dimension: number; distance: string }> {
    const raw = await fetchJson<unknown>(url(this.#options, ""), { method: "GET", headers: this.#options.apiKey === undefined ? {} : { "api-key": this.#options.apiKey } }, requestOptions(this.#options, signal));
    const result = envelope(raw); if (!isRecord(result) || !isRecord(result.config) || !isRecord(result.config.params) || !isRecord(result.config.params.vectors) || !isRecord(result.config.params.vectors.semantic) || result.config.params.vectors.semantic.size !== 1024 || result.config.params.vectors.semantic.distance !== "Dot") invalid("Qdrant collection is incompatible");
    return { dimension: 1024, distance: "Dot" };
  }
  async search(input: MemoryReadRequest): Promise<RankedMemoryRecord[]> {
    mandatory(input.filter, this.#options.ownerHost);
    if (!Array.isArray(input.vector) || input.vector.length !== 1024 || !input.vector.every((part) => typeof part === "number" && Number.isFinite(part)) || !Number.isSafeInteger(input.limit) || input.limit < 1 || input.limit > 1024) throw new MemoryClientError("configuration", "Dense retrieval request is invalid");
    const raw = await fetchJson<unknown>(url(this.#options, "/points/search"), { method: "POST", headers: headers(this.#options), body: JSON.stringify({ vector: { name: "semantic", vector: [...input.vector] }, limit: input.limit, filter: input.filter, with_payload: true, with_vector: true }) }, requestOptions(this.#options, input.signal));
    const result = envelope(raw); if (!Array.isArray(result)) invalid("Qdrant search result is invalid");
    const seen = new Set<string>(); return result.map(point).map((value) => { if (seen.has(value.id) || value.score === undefined) invalid("Qdrant search result is ambiguous"); seen.add(value.id); const record = parseBound(value, this.#options.ownerHost); if (!payloadMatchesFilter(value.payload, input.filter)) invalid("Qdrant search result violates its filter"); return { record, score: value.score }; });
  }
  async exact(input: ExactReadRequest): Promise<RankedMemoryRecord[]> {
    mandatory(input.filter, this.#options.ownerHost);
    if (typeof input.query !== "string" || input.query.length === 0 || input.query.length > 4000 || !Number.isSafeInteger(input.limit) || input.limit < 1 || input.limit > 1024) throw new MemoryClientError("configuration", "Exact retrieval request is invalid");
    const found: RankedMemoryRecord[] = []; let offset: string | null = null;
    for (let page = 0; page < 4; page += 1) {
      const terms = [...new Set(input.query.normalize("NFKC").toLocaleLowerCase("en-US").split(/[^\p{L}\p{N}._:/-]+/u).filter((term) => term.length > 0).slice(0, 32))];
      const exactConditions = [{ key: "text", match: { text: input.query } }, ...terms.flatMap((term) => [{ key: "tool_name", match: { value: term } }, { key: "error_fingerprint", match: { value: term } }])];
      const guardedFilter = { must: input.filter.must, must_not: input.filter.must_not, should: input.filter.should, min_should: { conditions: exactConditions, min_count: 1 } };
      const pageLimit = 256;
      const raw = await fetchJson<unknown>(url(this.#options, "/points/scroll"), { method: "POST", headers: headers(this.#options), body: JSON.stringify({ offset, limit: pageLimit, filter: guardedFilter, with_payload: true, with_vector: true }) }, requestOptions(this.#options, input.signal));
      const result = envelope(raw); if (!isRecord(result) || !Array.isArray(result.points)) invalid("Qdrant scroll result is invalid");
      const points = result.points.map(point); const ids = points.map((value) => value.id); if (new Set(ids).size !== ids.length) invalid("Qdrant scroll result is ambiguous");
      for (const value of points) { const record = parseBound(value, this.#options.ownerHost); if (!payloadMatchesFilter(value.payload, input.filter)) invalid("Qdrant scroll result violates its filter"); if (matchesExact(input.query, record)) found.push({ record, score: 1 }); }
      if (found.length >= input.limit) return found.slice(0, input.limit);
      const next = result.next_page_offset; if (next === null || next === undefined) return found; if (typeof next !== "string" || !UUID.test(next) || next === offset) invalid("Qdrant scroll cursor is invalid"); offset = next;
    }
    return found.slice(0, input.limit);
  }
  async #retrieve(refs: readonly MemoryRecordRef[], signal?: AbortSignal): Promise<MemoryRecord[]> {
    if (!Array.isArray(refs) || refs.length === 0 || refs.length > 1024) throw new MemoryClientError("configuration", "Retrieve references are invalid");
    const ids = refs.map((ref) => ref.recordType === "collection_control" ? ref.id : physicalPointIdFor(ref.recordType, ref.id)); if (new Set(ids).size !== ids.length) throw new MemoryClientError("configuration", "Retrieve references are ambiguous");
    const init: RequestInit = { method: "POST", headers: headers(this.#options), body: JSON.stringify({ ids, with_payload: true, with_vector: true }) };
    const raw = await fetchJson<unknown>(url(this.#options, "/points"), init, requestOptions(this.#options, signal));
    const result = envelope(raw); if (!Array.isArray(result)) invalid("Qdrant retrieve result is invalid");
    const points = result.map(point); if (new Set(points.map((value) => value.id)).size !== points.length || points.some((value) => !ids.includes(value.id))) invalid("Qdrant retrieve result is ambiguous");
    return points.map((value) => parseBound(value, this.#options.ownerHost));
  }
  async retrieve(refs: readonly MemoryRecordRef[]): Promise<MemoryRecord[]> { return refs.length === 0 ? [] : this.#retrieve(refs); }
  async retrieveEvidence(ids: readonly string[]): Promise<MemoryRecord[]> {
    if (!Array.isArray(ids) || ids.length === 0 || ids.length > 1024 || new Set(ids).size !== ids.length || ids.some((id) => typeof id !== "string" || id.length === 0 || id.length > 512)) throw new MemoryClientError("configuration", "Evidence IDs are invalid");
    const types = ["episode", "curated_memory", "curated_current", "raptor_summary"] as const;
    const physicalIds = [...new Set(ids.flatMap((id) => types.map((recordType) => physicalPointIdFor(recordType, id))))];
    const init: RequestInit = { method: "POST", headers: headers(this.#options), body: JSON.stringify({ ids: physicalIds, with_payload: true, with_vector: true }) };
    const raw = await fetchJson<unknown>(url(this.#options, "/points"), init, requestOptions(this.#options));
    const result = envelope(raw); if (!Array.isArray(result)) invalid("Qdrant evidence result is invalid");
    const points = result.map(point); if (new Set(points.map((value) => value.id)).size !== points.length || points.some((value) => !physicalIds.includes(value.id))) invalid("Qdrant evidence result is ambiguous");
    const records = points.map((value) => parseBound(value, this.#options.ownerHost));
    if (records.some((record) => !ids.includes(record.id) || !types.includes(record.recordType as typeof types[number])) || new Set(records.map((record) => record.id)).size !== records.length) invalid("Qdrant evidence result is invalid");
    return records;
  }
  async readControl(): Promise<ControlRecord> {
    const id = COLLECTION_CONTROL_ID; const records = await this.#retrieve([{ recordType: "collection_control", id }]);
    if (records.length !== 1 || records[0]!.recordType !== "collection_control" || records[0]!.id !== id) invalid("Collection control is missing or ambiguous"); return records[0] as ControlRecord;
  }
  async readPolicies(ids: readonly string[]): Promise<ProcessingPolicyRecord[]> {
    if (!Array.isArray(ids) || ids.length === 0 || ids.length > 1024 || new Set(ids).size !== ids.length) throw new MemoryClientError("configuration", "Policy IDs are invalid");
    const records = await this.#retrieve(ids.map((id) => ({ recordType: "processing_policy" as const, id })));
    if (records.some((record) => record.recordType !== "processing_policy" || !ids.includes(record.id))) invalid("Processing policy result is invalid"); return records as ProcessingPolicyRecord[];
  }
  async readTombstones(targetIds: readonly string[]): Promise<TombstoneRecord[]> {
    if (!Array.isArray(targetIds) || targetIds.length === 0 || targetIds.length > 8192 || new Set(targetIds).size !== targetIds.length) throw new MemoryClientError("configuration", "Tombstone targets are invalid");
    const found: TombstoneRecord[] = [];
    for (let index = 0; index < targetIds.length; index += 1024) {
      const targets = targetIds.slice(index, index + 1024); const ids = targets.map((target) => tombstoneId(this.#options.ownerHost, target));
      const records = await this.#retrieve(ids.map((id) => ({ recordType: "tombstone" as const, id })));
      for (const record of records) { if (record.recordType !== "tombstone" || !targets.includes(record.targetId)) invalid("Tombstone result is invalid"); found.push(record); }
    }
    return found;
  }
}
Object.freeze(GuardedMemoryReadStore); Object.freeze(GuardedMemoryReadStore.prototype);
export function createGuardedMemoryReadStore(options: GuardedMemoryReadStoreOptions): GuardedMemoryReadStore { return new GuardedMemoryReadStore(options, STORE_ISSUER); }
