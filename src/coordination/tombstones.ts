import { canonicalRecordHash, type MemoryRecord, type TombstoneRecord } from "../domain/records.js";
import { ProductionCoordinationStore } from "../qdrant/write.js";
import { isContentTarget, isOccurrenceTarget, isStateTarget, isTombstoneTarget, tombstoneId, type TombstoneScope } from "../domain/ids.js";
import type { HostId } from "../types.js";
import type { IngestTombstoneReader } from "../outbox/delivery.js";

export interface CreateTombstoneInput {
  ownerHost: HostId;
  scope: TombstoneScope;
  /** Runtime-domain-verifiable target: tagged state/content/occurrence ID, or episode UUID. */
  targetId: string;
  /** Bare-UUID occurrence targets require an explicit episode selector and exact store lookup. */
  targetKind?: "episode";
  /** Source/provenance IDs that must also become occurrence tombstones (provenance closure). */
  provenanceIds?: readonly string[];
  createdAt: string;
  privacyEpoch: number;
  processingPolicyId: string;
}
function tombstoneRecord(input: CreateTombstoneInput, targetId: string, scope: TombstoneScope): TombstoneRecord {
  const record: TombstoneRecord = {
    ownerHost: input.ownerHost, schemaRevision: 1, createdAt: input.createdAt, privacyEpoch: input.privacyEpoch,
    processingPolicyId: input.processingPolicyId, expiresAt: null, recordType: "tombstone",
    id: tombstoneId(input.ownerHost, targetId), scope, targetId, contentHash: "pending",
  };
  return { ...record, contentHash: canonicalRecordHash(record) } as TombstoneRecord;
}
/**
 * Insert immutable tombstones (strong ordering/wait, reread) for a target and
 * its provenance source IDs. Mismatched scope/target types fail closed; bare
 * UUID occurrence targets are verified as episodes through an explicit
 * selector and an exact store lookup. Provenance source tombstones are
 * provenance-independent, so the same source converges across forgotten
 * targets (no provenanceId-dependent collision).
 */
export async function createTombstone(store: ProductionCoordinationStore, input: CreateTombstoneInput): Promise<TombstoneRecord[]> {
  if (!ProductionCoordinationStore.isValid(store)) throw new TypeError("Tombstone creation requires a genuine production store");
  return store.createTombstone(input);
}
export async function readTombstones(store: ProductionCoordinationStore, targetIds: readonly string[]): Promise<TombstoneRecord[]> {
  if (!ProductionCoordinationStore.isValid(store)) throw new TypeError("Tombstone read requires a genuine production store");
  if (!Array.isArray(targetIds) || targetIds.length === 0 || targetIds.length > 1024 || targetIds.some((id) => typeof id !== "string" || id.length === 0 || id.length > 512)) throw new TypeError("Tombstone target IDs are invalid");
  return store.readTombstones(targetIds);
}

export interface TombstoneTarget { scope: TombstoneScope; targetId: string; }
/** Enumerate occurrence/content/state targets plus provenance closure for a record. */
export function tombstoneTargets(record: MemoryRecord): TombstoneTarget[] {
  const targets: TombstoneTarget[] = [];
  switch (record.recordType) {
    case "episode":
      targets.push({ scope: "occurrence", targetId: record.id });
      break;
    case "curated_memory":
      targets.push({ scope: "occurrence", targetId: record.observationId }, { scope: "content", targetId: record.contentId });
      if (record.stateKey !== undefined) targets.push({ scope: "state", targetId: record.stateKey });
      if (record.primaryEvidenceEpisodeId !== undefined) targets.push({ scope: "occurrence", targetId: record.primaryEvidenceEpisodeId });
      for (const sourceId of record.sourceEpisodeIds ?? []) targets.push({ scope: "occurrence", targetId: sourceId });
      for (const sourceId of record.provenance ?? []) targets.push({ scope: "occurrence", targetId: sourceId });
      break;
    case "curated_current":
      if (record.resolution === "resolved") { targets.push({ scope: "occurrence", targetId: record.observationId }, { scope: "content", targetId: record.contentId }); }
      targets.push({ scope: "state", targetId: record.stateKey });
      for (const sourceId of record.sourceEpisodeIds ?? []) targets.push({ scope: "occurrence", targetId: sourceId });
      break;
    case "raptor_summary":
      // RAPTOR member IDs may be child summaries; they are never treated as
      // episode leaves here — the visibility function resolves them recursively
      // into verified episode targets via the manifest resolver.
      break;
    default:
      break;
  }
  return targets;
}

/**
 * Fail-closed final visibility. The tombstone batch is read by this function
 * (bounded target expansion + batch-read seam), never trusted from a caller's
 * incomplete list. Occurrence tombstones hide the exact occurrence PERMANENTLY;
 * content/state tombstones block current AND future recurrence.
 *
 * Provenance authority: until Task 10 installs a verifiable content-addressed
 * persisted manifest resolver, ANY manifest-bearing or manifest-only derived
 * record is invisible fail-closed (no caller-supplied structural resolver is
 * ever trusted as visibility authority). RAPTOR `memberIds` count as episode
 * provenance only when EVERY member is directly an exact persisted episode
 * UUID verified through store.readEpisodes; a single child-summary ID makes
 * the record invisible. Curated direct `sourceEpisodeIds` remain
 * exact-verified; `primaryEvidence`/observation ids alone are not closure.
 */
export async function isVisibleAfterTombstoneCheck(store: ProductionCoordinationStore, record: MemoryRecord): Promise<boolean> {
  // Nominal production authority: ONLY the branded production store can mint
  // visibility. A structural Pick/fake {readEpisodes, readTombstones} — even
  // one returning same-owner minimal IDs and empty tombstones — fails closed
  // false BEFORE any read; there is no test issuer or monkeypatchable brand
  // bypass.
  if (!ProductionCoordinationStore.isValid(store)) return false;
  try {
    if (record.contentHash !== canonicalRecordHash(record)) return false;
  } catch { return false; }
  const targets: TombstoneTarget[] = [];
  if (record.recordType === "curated_memory" || record.recordType === "curated_current" || record.recordType === "raptor_summary") {
    const manifestHash = record.recordType === "raptor_summary" ? record.manifestHash : record.recordType === "curated_memory" ? record.manifestHash : record.recordType === "curated_current" ? record.conflictManifestHash : undefined;
    // Fail closed for Task 10: any manifest (alone or alongside direct IDs) is invisible.
    if (manifestHash !== undefined) return false;
    const directSourceIds = record.recordType === "curated_memory"
      ? (record.sourceEpisodeIds ?? [])
      : record.recordType === "curated_current"
        ? (record.sourceEpisodeIds ?? [])
        : (record.memberIds ?? []);
    if (directSourceIds.length === 0) return false;
    // RAPTOR direct members are episode provenance only when every member is an
    // exact persisted episode UUID; curated sources are exact-verified too.
    if (!directSourceIds.every((id) => EPISODE_UUID_RE.test(id))) return false;
    for (const sourceId of directSourceIds) targets.push({ scope: "occurrence", targetId: sourceId });
  }
  targets.push(...tombstoneTargets(record));
  // Every generated domain target must itself be a VALID tombstone target:
  // raw/colliding contentId, stateKey or observation/occurrence selectors can
  // never be tombstoned, so such a record fails closed (invisible) BEFORE any
  // tombstone read or true return. Bare UUIDs additionally require the exact
  // store.readEpisodes owner verification below.
  for (const target of targets) {
    if (!isTombstoneTarget(target.scope, target.targetId)) return false;
    if (target.scope === "occurrence" && !isOccurrenceTarget(target.targetId)) return false;
  }
  const unique = new Set(targets.map((target) => `${target.scope}:${target.targetId}`));
  if (unique.size > 1024) return false;
  const ids = [...unique].map((key) => key.slice(key.indexOf(":") + 1));
  // Exact verified episode closure: every bare-UUID occurrence target must be an
  // exact canonical persisted episode of the pinned owner; arbitrary/ghost ids
  // fail closed. Direct-source and tombstone-target expansion may both name the
  // same episode, so the target list is deduplicated before the strict read.
  // Unsupported/non-retrievable MemoryRecord kinds yield ZERO targets: they
  // are invisible (false) BEFORE any readTombstones([]) call — a zero-target
  // kind must never throw or reach the tombstone reader.
  if (ids.length === 0) return false;
  const episodeTargets = [...new Set(targets.filter((target) => target.scope === "occurrence" && !String(target.targetId).startsWith("occurrence:")).map((target) => target.targetId))];
  try {
    if (episodeTargets.length > 0) {
      const episodes = await store.readEpisodes(episodeTargets);
      const byId = new Map(episodes.map((entry) => [entry.id, entry]));
      for (const targetId of episodeTargets) {
        const episode = byId.get(targetId);
        if (episode === undefined || episode.ownerHost !== record.ownerHost) return false;
      }
    }
    const tombstones = await store.readTombstones(ids);
    for (const target of targets) if (tombstoned(tombstones, target)) return false;
    return true;
  } catch {
    // Memory unavailable (reader failure, invalid batch identity, network):
    // invisible fail-open for the host turn — never propagate a retrieval-
    // breaking exception or return authority.
    return false;
  }
}
function tombstoned(tombstones: readonly TombstoneRecord[], target: TombstoneTarget): boolean {
  for (const tombstone of tombstones) {
    if (tombstone.scope !== target.scope || tombstone.targetId !== target.targetId) continue;
    return true; // occurrence/content/state tombstones hide permanently
  }
  return false;
}

const EPISODE_UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/iu;

/** Module-private unexported issuer: readers are constructed only through the production factory. */
const INGEST_TOMBSTONE_READER_ISSUER = Symbol("pi-qdrant-memory-v2.ingest-tombstone-reader-issuer");

/**
 * Nominal, frozen, bound tombstone barrier reader. The owner is tied to the
 * PRODUCTION store's pinned owner host; structural `{ownerHost, readTombstones}`
 * fakes and any non-branded store are rejected. The readTombstoned operation
 * validates bounded episode IDs, batch-reads the exact
 * H(owner,"tombstone",episode) points and returns only occurrence-scoped
 * target IDs. There is no public mint and no test binder in dist.
 */
export class BoundIngestTombstoneReader implements IngestTombstoneReader {
  readonly #issuer: symbol;
  readonly #store: ProductionCoordinationStore;
  readonly #ownerHost: HostId;
  /** Public constructor is unusable without the module-private issuer symbol. */
  constructor(store: ProductionCoordinationStore, ownerHost: HostId, issuer: symbol) {
    if (issuer !== INGEST_TOMBSTONE_READER_ISSUER) throw new TypeError("Tombstone reader requires the module issuer");
    if (ownerHost !== "pi" && ownerHost !== "prime") throw new TypeError("Tombstone reader owner is invalid");
    if (!ProductionCoordinationStore.isValid(store) || store.ownerHost !== ownerHost) throw new TypeError("Tombstone reader requires the branded production store of its owner");
    this.#issuer = issuer;
    this.#store = store;
    this.#ownerHost = ownerHost;
    Object.freeze(this);
  }
  /** Exposed validating operation only; issuance stays module-private. */
  static isValid(value: unknown): value is BoundIngestTombstoneReader {
    if (typeof value !== "object" || value === null || !(#issuer in value)) return false;
    return value instanceof BoundIngestTombstoneReader && value.#issuer === INGEST_TOMBSTONE_READER_ISSUER;
  }
  get ownerHost(): HostId { return this.#ownerHost; }
  async readTombstoned(episodeIds: readonly string[]): Promise<readonly string[]> {
    if (!Array.isArray(episodeIds) || episodeIds.length === 0 || episodeIds.length > 1024 || episodeIds.some((id) => typeof id !== "string" || id.length === 0 || id.length > 512)) throw new TypeError("Tombstone episode IDs are invalid");
    const tombstones = await this.#store.readTombstones(episodeIds);
    return tombstones.filter((entry) => entry.scope === "occurrence").map((entry) => entry.targetId);
  }
}
Object.freeze(BoundIngestTombstoneReader);
Object.freeze(BoundIngestTombstoneReader.prototype);

/** Production Task 8 tombstone barrier reader: requires the branded production store of the owner. */
export function createIngestTombstoneReader(store: ProductionCoordinationStore, ownerHost: HostId): BoundIngestTombstoneReader {
  return new BoundIngestTombstoneReader(store, ownerHost, INGEST_TOMBSTONE_READER_ISSUER);
}
