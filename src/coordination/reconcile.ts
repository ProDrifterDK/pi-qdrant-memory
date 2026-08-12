import { canonicalRecordHash, isPersistedMemoryRecord, type CoverageRecord, type EpisodeRecord } from "../domain/records.js";
import { coverageId } from "../domain/ids.js";
import { ProductionCoordinationStore } from "../qdrant/write.js";
import type { HostId } from "../types.js";

export interface MarkCoverageInput {
  ownerHost: HostId;
  episodeId: string;
  extractorRevision: string;
  policyEpoch: number;
  policyHash: string;
  privacyEpoch: number;
  createdAt: string;
  processingPolicyId: string;
}
/**
 * Coverage truth is deterministic coverage IDs + batch retrieve; the scan
 * cursor is optimization only. The identity is policy-specific: owner +
 * episode + extractor + active coordination hash/epoch + processing-policy
 * intersection + privacy epoch, so pre-forget coverage can never suppress
 * post-forget work.
 */
export async function markCoverage(store: ProductionCoordinationStore, input: MarkCoverageInput): Promise<CoverageRecord> {
  if (!ProductionCoordinationStore.isValid(store)) throw new TypeError("Coverage mark requires a genuine production store");
  return store.markCoverage(input);
}
export interface EpisodeSlice { episodes: readonly EpisodeRecord[]; nextOffset?: string; }
export interface FindMissingInput {
  store: ProductionCoordinationStore;
  /** ID-offset bounded slice scan of persisted episodes; never a timestamp cursor. */
  listEpisodes(offset: string | undefined, limit: number): Promise<EpisodeSlice>;
  extractorRevision: string;
  policyEpoch: number;
  policyHash: string;
  policyIntersectionId: string;
  privacyEpoch: number;
  batchSize?: number;
  /** Turn bound: maximum number of slices scanned per call (default 4). */
  maxSlices?: number;
  /** Turn bound: maximum number of missing results returned per call (default 1024). */
  maxMissing?: number;
  offset?: string;
  signal?: AbortSignal;
}
export interface FindMissingResult { missing: EpisodeRecord[]; scanned: number; nextOffset?: string; truncated?: boolean; }
function validateSlice(slice: EpisodeSlice, batchSize: number, cursor: string | undefined, ownerHost: HostId): EpisodeRecord[] {
  if (slice === null || typeof slice !== "object" || !Array.isArray(slice.episodes) || slice.episodes.length > batchSize) throw new TypeError("Reconcile slice episodes are invalid");
  if (slice.nextOffset !== undefined && (typeof slice.nextOffset !== "string" || slice.nextOffset.length === 0 || slice.nextOffset.length > 512)) throw new TypeError("Reconcile slice cursor is invalid");
  // Bounded OVERLAP is permitted: a page may begin with strictly sorted,
  // unique episode IDs at/before the current ID cursor (overlap, so an episode
  // inserted behind the saved cursor is discovered) followed by newer IDs.
  // Strict ordering/uniqueness is enforced WITHIN each page only; the cursor
  // is optimization, never causal truth.
  let previous = "";
  let lastId: string | undefined;
  const pageIds = new Set<string>();
  for (const episode of slice.episodes) {
    if (episode === null || typeof episode !== "object" || episode.recordType !== "episode" || !isPersistedMemoryRecord(episode, { vectorDimension: 1024 })) throw new TypeError("Reconcile slice contains an invalid episode");
    // A scanned episode owned by a different host is rejected BEFORE any
    // coverage read/return (the store is the sole owner authority).
    if (episode.ownerHost !== ownerHost) throw new TypeError("Reconcile slice contains a foreign episode");
    if (episode.id <= previous || pageIds.has(episode.id)) throw new TypeError("Reconcile slice episodes must be strictly sorted and unique within the page");
    pageIds.add(episode.id);
    previous = episode.id;
    lastId = episode.id;
  }
  // Exclusive ID-offset contract: the slice cursor (when present) must be the
  // exact last processed episode id; cursor leaps are rejected.
  if (lastId !== undefined && slice.nextOffset !== undefined && slice.nextOffset !== lastId) throw new TypeError("Reconcile slice cursor must be the last processed episode id");
  // A resume cursor must STRICTLY ADVANCE beyond the input cursor: repeated or
  // regressing next cursors (including a pure-overlap page claiming a cursor)
  // are rejected so the sweep can never loop.
  if (slice.nextOffset !== undefined && cursor !== undefined && !(slice.nextOffset > cursor)) throw new TypeError("Reconcile slice cursor must advance beyond the input cursor");
  return [...slice.episodes];
}
/** Scan episodes in bounded slices, batch-retrieve their coverage IDs, enqueue missing/late episodes. */
export async function findMissingEpisodes(input: FindMissingInput): Promise<FindMissingResult> {
  // GLOBAL RULE: snapshot the untrusted store and every input field EXACTLY
  // ONCE into locals; the brand check fires on the local, and every later read
  // (ownerHost/readCoverage) uses ONLY the local. A getter swap real->fake can
  // never suppress missing work or relabel identity.
  const store = input.store;
  if (!ProductionCoordinationStore.isValid(store)) throw new TypeError("Coverage scan requires a genuine production store");
  const listEpisodes = input.listEpisodes;
  const extractorRevision = input.extractorRevision;
  const policyEpoch = input.policyEpoch;
  const policyHash = input.policyHash;
  const policyIntersectionId = input.policyIntersectionId;
  const privacyEpoch = input.privacyEpoch;
  const batchSize = input.batchSize ?? 64;
  const maxSlices = input.maxSlices ?? 4;
  const maxMissing = input.maxMissing ?? 1024;
  const offset = input.offset;
  const signal = input.signal;
  if (!Number.isSafeInteger(batchSize) || batchSize < 1 || batchSize > 1024) throw new TypeError("Reconcile batch size is invalid");
  if (!Number.isSafeInteger(maxSlices) || maxSlices < 1 || maxSlices > 1024) throw new TypeError("Reconcile slice bound is invalid");
  if (!Number.isSafeInteger(maxMissing) || maxMissing < 1 || maxMissing > 1024) throw new TypeError("Reconcile missing bound is invalid");
  if (typeof listEpisodes !== "function") throw new TypeError("Reconcile requires an episode scanner");
  if (typeof extractorRevision !== "string" || extractorRevision.length === 0 || extractorRevision.length > 512) throw new TypeError("Reconcile extractor revision is invalid");
  if (!Number.isSafeInteger(policyEpoch) || policyEpoch < 0 || !Number.isSafeInteger(privacyEpoch) || privacyEpoch < 0) throw new TypeError("Reconcile policy/privacy epochs are invalid");
  if (typeof policyHash !== "string" || policyHash.length === 0 || policyHash.length > 512 || typeof policyIntersectionId !== "string" || policyIntersectionId.length === 0 || policyIntersectionId.length > 512) throw new TypeError("Reconcile policy identity is invalid");
  if (offset !== undefined && (typeof offset !== "string" || offset.length === 0 || offset.length > 512)) throw new TypeError("Reconcile cursor is invalid");
  let cursor = offset;
  const missing: EpisodeRecord[] = [];
  let scanned = 0;
  // Per-call dedup: an overlap ID re-appearing in a later page of the SAME call
  // is never coverage-checked or reported twice.
  const processed = new Set<string>();
  // The last processed episode strictly AFTER the call's input cursor: the
  // forward resume position (never an overlap ID).
  let lastForward: string | undefined;
  for (let sliceIndex = 0; sliceIndex < maxSlices; sliceIndex += 1) {
    if (signal?.aborted) throw new TypeError("Reconcile scan aborted");
    let slice: EpisodeSlice;
    try { slice = await listEpisodes(cursor, batchSize); } catch { throw new TypeError("Reconcile episode scan failed"); }
    const episodes = validateSlice(slice, batchSize, cursor, store.ownerHost);
    if (episodes.length === 0) {
      if (slice.nextOffset !== undefined) throw new TypeError("Reconcile empty slice cannot carry a resume cursor");
      // EOF: an explicit completed sweep, never a cursor.
      return { missing, scanned, truncated: false };
    }
    const ids = episodes.map((entry) => coverageId({ ownerHost: store.ownerHost, episodeId: entry.id, extractorRevision: extractorRevision, coordinationPolicyHash: policyHash, coordinationPolicyEpoch: policyEpoch, policyIntersectionId: policyIntersectionId, privacyEpoch: privacyEpoch }));
    const covered = new Set((await store.readCoverage(ids)).map((entry) => entry.episodeId));
    for (const episode of episodes) {
      if (processed.has(episode.id)) continue; // overlap duplicate within this call
      processed.add(episode.id);
      scanned += 1;
      const overlap = cursor !== undefined && episode.id <= cursor;
      if (overlap && episode.id > (lastForward ?? "")) { /* overlap cannot advance forward */ }
      if (!overlap && (lastForward === undefined || episode.id > lastForward)) lastForward = episode.id;
      if (!covered.has(episode.id)) missing.push(episode);
      if (missing.length >= maxMissing) {
        if (overlap) {
          // maxMissing reached INSIDE overlap: never replace the resume cursor
          // with an older ID — retain/retry the prior forward cursor (the last
          // forward ID processed, or the input cursor when nothing forward was
          // processed yet). Tail members are never skipped or leapt over.
          const resumeOffset = lastForward ?? cursor;
          return resumeOffset === undefined ? { missing, scanned, truncated: true } : { missing, scanned, nextOffset: resumeOffset, truncated: true };
        }
        // maxMissing reached AFTER forward progress: resume exactly at the last
        // processed forward ID; tail members are NOT leapt over.
        return { missing, scanned, nextOffset: episode.id, truncated: true };
      }
    }
    cursor = slice.nextOffset;
    if (cursor === undefined) return { missing, scanned, truncated: false };
  }
  return { missing, scanned, nextOffset: cursor!, truncated: true };
}
/**
 * One bounded slice of the full operator sweep: externally resumable via the
 * returned ID-offset cursor, abortable, and safe to re-run with overlap.
 */
/**
 * One bounded slice of the sweep, EXTERNALLY RESUMABLE: the caller's `offset`
 * (and any overlap state) is passed straight through to the scanner, so
 * one-slice periodic calls actually resume. EOF (no nextOffset) completes the
 * sweep; the next normal periodic invocation with `offset: undefined` begins a
 * fresh full cycle, which — together with bounded overlap — guarantees an
 * episode inserted behind a saved cursor is eventually coverage-checked.
 */
export async function reconcileCoverage(input: Omit<FindMissingInput, "maxSlices">): Promise<FindMissingResult> {
  // Snapshot the untrusted store + scalars EXACTLY ONCE and pass a PLAIN
  // snapshot object to findMissingEpisodes (never the caller object).
  const store = input.store;
  if (!ProductionCoordinationStore.isValid(store)) throw new TypeError("Coverage reconcile requires a genuine production store");
  const listEpisodes = input.listEpisodes;
  const extractorRevision = input.extractorRevision;
  const policyEpoch = input.policyEpoch;
  const policyHash = input.policyHash;
  const policyIntersectionId = input.policyIntersectionId;
  const privacyEpoch = input.privacyEpoch;
  const batchSize = input.batchSize;
  const maxMissing = input.maxMissing;
  const offset = input.offset;
  const signal = input.signal;
  const snapshot: FindMissingInput = { store, listEpisodes, extractorRevision, policyEpoch, policyHash, policyIntersectionId, privacyEpoch, maxSlices: 1 };
  if (batchSize !== undefined) snapshot.batchSize = batchSize;
  if (maxMissing !== undefined) snapshot.maxMissing = maxMissing;
  if (offset !== undefined) snapshot.offset = offset;
  if (signal !== undefined) snapshot.signal = signal;
  return findMissingEpisodes(Object.freeze(snapshot));
}
