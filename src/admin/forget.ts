import { canonicalStringify, sha256Hex } from "../domain/canonical.js";
import { isContentTarget, isOccurrenceTarget, isStateTarget, type TombstoneScope } from "../domain/ids.js";
import { parseMemoryRecord, type CuratedCurrentRecord } from "../domain/records.js";
import { physicalPointIdFor, readPolicy, type QdrantClientOptions } from "../qdrant/client.js";
import { statusRetrieve } from "./transport.js";
import type { HostId, RuntimeConfig } from "../types.js";
import { AdminPlanError } from "./errors.js";

const MAX_TARGETS = 1024;
const SECRET = /(?:api[-_]?key|access[-_]?token|authorization|bearer|credential|password|secret|token)/iu;

export type ForgetScope = TombstoneScope;
export interface ForgetSelection {
  episodeId?: string;
  observationId?: string;
  curatedCurrentId?: string;
  contentId?: string;
  stateKey?: string;
  targetId?: string;
}
export interface CurrentSelection {
  id: string;
  observationId?: string;
  contentId?: string;
  stateKey?: string;
  sourceEpisodeIds?: readonly string[];
  evidenceEpisodeIds?: readonly string[];
  manifestHash?: string;
}
export interface ForgetPlanDependencies {
  resolveCurrent?(id: string): Promise<CurrentSelection | null>;
  readCurrent?(id: string): Promise<CurrentSelection | null>;
  resolveRecord?(selection: ForgetSelection): Promise<Partial<CurrentSelection> & { episodeIds?: readonly string[] } | null>;
}
export interface ForgetPlanInput extends ForgetPlanDependencies {
  ownerHost?: HostId;
  selection: ForgetSelection;
  /** Optional already-reread current view seam for embedders/tests. */
  current?: CurrentSelection;
  scope?: ForgetScope;
  requestedAt?: string | Date;
}
export interface ForgetTargetClosure {
  readonly episodes: readonly string[];
  readonly observations: readonly string[];
  readonly currentViews: readonly string[];
  readonly evidence: readonly string[];
  readonly generations: readonly string[];
  readonly manifests: readonly string[];
  readonly outboxProposals: readonly string[];
}
export interface ForgetPlan {
  readonly id: string;
  readonly operation: "forget";
  readonly ownerHost?: HostId;
  readonly scope: ForgetScope;
  readonly targets: readonly string[];
  readonly closure: ForgetTargetClosure;
  readonly selection: ForgetSelection;
  readonly recurrenceBlocked: boolean;
  readonly requestedAt: string;
  readonly requiresHumanApproval: true;
  readonly logicalBarrier: "tombstone_and_epoch";
  readonly physicalDeletion: "eventual_no_backup_claim";
}
export interface ForgetDependencies extends ForgetPlanDependencies {
  readControl?(): Promise<{ ownerHost: HostId; privacyEpoch: number; activeGeneration: string | null }>;
  beginForgetBarrier?(input: { now: number; expectedEpoch?: number; signal?: AbortSignal }): Promise<{ privacyEpoch: number; activeGeneration: string | null } | unknown>;
  createTombstones?(input: { scope: ForgetScope; targetIds: readonly string[]; provenanceIds: readonly string[]; privacyEpoch?: number; signal?: AbortSignal }): Promise<unknown>;
  readTombstones?(targetIds: readonly string[]): Promise<readonly unknown[]>;
  invalidateCurrentViews?(): Promise<void>;
  invalidateCoverage?(): Promise<void>;
  quarantineOutbox?(): Promise<void>;
  reconcile?(): Promise<void>;
  rereadBarrier?(): Promise<{ privacyEpoch: number; activeGeneration: string | null } | unknown>;
}
export interface RunForgetInput {
  plan: ForgetPlan;
  approvedPlanId: string;
  signal?: AbortSignal;
  now?: number;
}
export interface RunForgetResult {
  readonly ok: true;
  readonly planId: string;
  readonly scope: ForgetScope;
  readonly logicalInvisible: true;
  readonly recurrenceBlocked: boolean;
  readonly physicalDeletion: "eventual_no_backup_claim";
  readonly tombstonesWritten: boolean;
  readonly barrierConfirmed: boolean;
}

const CURRENT_WIRE_FIELDS: Readonly<Record<string, string>> = { owner_host: "ownerHost", schema_revision: "schemaRevision", created_at: "createdAt", privacy_epoch: "privacyEpoch", processing_policy_id: "processingPolicyId", expires_at: "expiresAt", record_type: "recordType", id: "id", content_hash: "contentHash", coordination_policy_hash: "coordinationPolicyHash", coordination_policy_epoch: "coordinationPolicyEpoch", version: "version", state_key: "stateKey", effective_order: "effectiveOrder", source_episode_ids: "sourceEpisodeIds", project_id: "projectId", scope: "scope", resolution: "resolution", content_id: "contentId", observation_id: "observationId", text: "text", conflict_manifest_hash: "conflictManifestHash" };
function currentFromWire(point: { id: string; payload: Record<string, unknown>; vector?: { semantic: number[] } }, ownerHost: HostId): CurrentSelection | null {
  const record: Record<string, unknown> = {};
  for (const [key, target] of Object.entries(CURRENT_WIRE_FIELDS)) if (point.payload[key] !== undefined) record[target] = point.payload[key];
  if (point.vector !== undefined) record.vector = point.vector.semantic;
  try {
    const parsed = parseMemoryRecord(record, { ownerHost, vectorDimension: 1024 });
    if (parsed.recordType !== "curated_current" || parsed.ownerHost !== ownerHost || parsed.id !== record.id) return null;
    const current = parsed as CuratedCurrentRecord;
    return { id: current.id, ...(current.resolution === "resolved" ? { observationId: current.observationId, contentId: current.contentId } : {}), stateKey: current.stateKey, ...(current.sourceEpisodeIds === undefined ? {} : { sourceEpisodeIds: current.sourceEpisodeIds }), ...(current.conflictManifestHash === undefined ? {} : { manifestHash: current.conflictManifestHash }) };
  } catch { return null; }
}
/** Read one current view through the named read-only admin transport. */
export async function readQdrantCurrentSelection(config: RuntimeConfig, id: string, fetchImpl: typeof fetch = globalThis.fetch): Promise<CurrentSelection | null> {
  const physicalId = physicalPointIdFor("curated_current", id);
  const options: QdrantClientOptions = { baseUrl: config.qdrant.url, collection: config.qdrant.collection, ownerHost: config.host, ...(config.qdrant.apiKey === undefined ? {} : { apiKey: config.qdrant.apiKey }), timeoutMs: config.retrieval.timeoutMs, readConsistency: config.coordination.readConsistency, maxClockSkewMs: config.coordination.maxClockSkewMs };
  const policy = readPolicy({ ownerHost: config.host, purpose: "query", recordTypes: ["curated_current"], maxClockSkewMs: config.coordination.maxClockSkewMs });
  const points = await statusRetrieve(options, fetchImpl, [physicalId], policy, true);
  if (points.length !== 1 || points[0]!.id !== physicalId) return null;
  return currentFromWire(points[0]!, config.host);
}

function safeId(value: unknown, name: string, max = 512): string {
  if (typeof value !== "string" || value.length === 0 || value.length > max || /[\u0000-\u001f\u007f-\u009f]/u.test(value) || SECRET.test(value)) throw new TypeError(`${name} is invalid or not redacted`);
  return value;
}
function safeOptionalId(value: string | undefined, name: string): string | undefined { return value === undefined ? undefined : safeId(value, name); }
function host(value: HostId | undefined): HostId | undefined { if (value !== undefined && value !== "pi" && value !== "prime") throw new TypeError("Forget owner host is invalid"); return value; }
function timestamp(value: string | Date | undefined): string { const parsed = value === undefined ? new Date() : value instanceof Date ? new Date(value.getTime()) : new Date(value); if (!Number.isFinite(parsed.getTime())) throw new TypeError("Forget request time is invalid"); return parsed.toISOString(); }
function scopeValue(value: ForgetScope | undefined): ForgetScope { if (value === undefined) return "occurrence"; if (value !== "occurrence" && value !== "content" && value !== "state") throw new TypeError("Forget scope is invalid"); return value; }
function uniqueSorted(values: readonly string[]): string[] { const result = [...new Set(values)].sort(); if (result.length > MAX_TARGETS) throw new TypeError("Forget target closure is unbounded"); return result; }
function selectionIds(selection: ForgetSelection): string[] {
  if (selection === null || typeof selection !== "object") throw new TypeError("Forget selection is invalid");
  const values = [selection.episodeId, selection.observationId, selection.curatedCurrentId, selection.contentId, selection.stateKey, selection.targetId].filter((value): value is string => value !== undefined).map((value, index) => safeId(value, `selection[${index}]`));
  if (values.length !== 1) throw new TypeError("Forget selection must identify exactly one target");
  return values;
}
function emptyClosure(): ForgetTargetClosure { return { episodes: [], observations: [], currentViews: [], evidence: [], generations: [], manifests: [], outboxProposals: [] }; }
function closureFromSelection(selection: ForgetSelection, resolved: Partial<CurrentSelection> & { episodeIds?: readonly string[] } | null, scope: ForgetScope): { targets: string[]; closure: ForgetTargetClosure } {
  const episodes: string[] = [];
  const observations: string[] = [];
  const currentViews: string[] = [];
  const evidence: string[] = [];
  const generations: string[] = [];
  const manifests: string[] = [];
  const outboxProposals: string[] = [];
  const explicit = selection.episodeId ?? selection.observationId ?? selection.curatedCurrentId ?? selection.contentId ?? selection.stateKey ?? selection.targetId!;
  const observation = resolved?.observationId ?? selection.observationId;
  const content = resolved?.contentId ?? selection.contentId;
  const state = resolved?.stateKey ?? selection.stateKey;
  const sourceEpisodes = [...(resolved?.sourceEpisodeIds ?? []), ...(resolved?.evidenceEpisodeIds ?? []), ...(resolved?.episodeIds ?? [])].map((value, index) => safeId(value, `closure.episode[${index}]`));
  if (selection.curatedCurrentId !== undefined) currentViews.push(safeId(selection.curatedCurrentId, "selection.curatedCurrentId"));
  if (scope === "occurrence") {
    const target = observation ?? (selection.episodeId ?? explicit);
    observations.push(safeId(target, "occurrence target"));
    episodes.push(...sourceEpisodes);
    if (selection.episodeId !== undefined) episodes.push(safeId(selection.episodeId, "selection.episodeId"));
    if (resolved?.manifestHash !== undefined) manifests.push(safeId(resolved.manifestHash, "closure.manifestHash"));
  } else if (scope === "content") {
    if (content === undefined) throw new AdminPlanError("Content forget requires a content_id or a resolvable current view");
    observations.push(safeId(content, "content target"));
    episodes.push(...sourceEpisodes);
    if (resolved?.manifestHash !== undefined) manifests.push(safeId(resolved.manifestHash, "closure.manifestHash"));
  } else {
    if (state === undefined) throw new AdminPlanError("State forget requires a state_key or a resolvable current view");
    currentViews.push(safeId(state, "state target"));
    episodes.push(...sourceEpisodes);
  }
  // Evidence/provenance closure is intentionally represented separately from
  // the primary target so occurrence forget never silently becomes recurrence
  // blocking. IDs are opaque and redacted; payloads are never copied.
  evidence.push(...sourceEpisodes);
  const closure: ForgetTargetClosure = { episodes: uniqueSorted(episodes), observations: uniqueSorted(observations), currentViews: uniqueSorted(currentViews), evidence: uniqueSorted(evidence), generations: uniqueSorted(generations), manifests: uniqueSorted(manifests), outboxProposals: uniqueSorted(outboxProposals) };
  const primary = scope === "occurrence" ? closure.observations : scope === "content" ? [safeId(content!, "content target")] : [safeId(state!, "state target")];
  return { targets: uniqueSorted(primary), closure };
}

/** Plan a redacted target/provenance closure. A current selection is resolved
 * before planning, so approval always names the observation/content/state that
 * will be barriered rather than a mutable UI alias. */
function validateSelectorScope(selection: ForgetSelection, scope: ForgetScope): void {
  if (selection.contentId !== undefined && scope !== "content") throw new AdminPlanError("content_id requires content forget scope");
  if (selection.stateKey !== undefined && scope !== "state") throw new AdminPlanError("state_key requires state forget scope");
  if (selection.observationId !== undefined && scope !== "occurrence") throw new AdminPlanError("observation_id requires occurrence forget scope");
  if (selection.episodeId !== undefined && scope !== "occurrence") throw new AdminPlanError("episode_id requires occurrence forget scope");
  if (selection.targetId !== undefined && (selection.targetId.startsWith("content:") && scope !== "content" || selection.targetId.startsWith("state:") && scope !== "state" || selection.targetId.startsWith("occurrence:") && scope !== "occurrence")) throw new AdminPlanError("forget target does not match scope");
}

export async function planForget(input: ForgetPlanInput): Promise<ForgetPlan> {
  const selection = input.selection;
  selectionIds(selection);
  const scope = scopeValue(input.scope);
  validateSelectorScope(selection, scope);
  const ownerHost = host(input.ownerHost);
  let resolved: (Partial<CurrentSelection> & { episodeIds?: readonly string[] }) | null = null;
  if (selection.curatedCurrentId !== undefined) {
    if (input.current !== undefined) {
      if (input.current.id !== selection.curatedCurrentId) throw new AdminPlanError("Current selection does not match the reread view");
      resolved = input.current;
    } else {
      const resolver = input.resolveCurrent ?? input.readCurrent;
      if (resolver === undefined) throw new AdminPlanError("Current forget selection requires a durable current resolver");
      const value = await resolver(selection.curatedCurrentId);
      if (value === null || value.id !== selection.curatedCurrentId) throw new AdminPlanError("Current selection could not be resolved");
      resolved = value;
    }
  } else if (input.resolveRecord !== undefined) {
    resolved = await input.resolveRecord(selection);
  }
  const { targets, closure } = closureFromSelection(selection, resolved, scope);
  const requestedAt = timestamp(input.requestedAt);
  const planBody = { closure, logicalBarrier: "tombstone_and_epoch" as const, operation: "forget" as const, ...(ownerHost === undefined ? {} : { ownerHost }), physicalDeletion: "eventual_no_backup_claim" as const, recurrenceBlocked: scope !== "occurrence", requestedAt, scope, selection, targets, requiresHumanApproval: true as const };
  const { requestedAt: _requestedAt, ...stablePlanBody } = planBody;
  const id = sha256Hex(canonicalStringify(stablePlanBody));
  return Object.freeze({ ...planBody, id });
}
export const createForgetPlan = planForget;

function checkPlan(plan: ForgetPlan): void {
  const { id, ...body } = plan;
  const { requestedAt: _requestedAt, ...stableBody } = body;
  if (id !== sha256Hex(canonicalStringify(stableBody)) || plan.operation !== "forget" || plan.requiresHumanApproval !== true || plan.logicalBarrier !== "tombstone_and_epoch" || plan.physicalDeletion !== "eventual_no_backup_claim") throw new AdminPlanError("Forget plan is invalid");
  scopeValue(plan.scope); timestamp(plan.requestedAt); host(plan.ownerHost); selectionIds(plan.selection);
  if (plan.recurrenceBlocked !== (plan.scope !== "occurrence")) throw new AdminPlanError("Forget plan recurrence semantics are invalid");
  if (!Array.isArray(plan.targets) || plan.targets.length === 0 || plan.targets.length > MAX_TARGETS) throw new AdminPlanError("Forget plan targets are invalid");
  plan.targets.forEach((value, index) => safeId(value, `plan.targets[${index}]`));
}

/** Apply a plan only after exact human approval and a reread barrier. */
export async function runForget(input: RunForgetInput, deps: ForgetDependencies = {}): Promise<RunForgetResult> {
  checkPlan(input.plan);
  if (input.approvedPlanId !== input.plan.id) throw new AdminPlanError("Forget approval does not match the plan");
  const now = input.now ?? Date.now(); if (!Number.isFinite(now)) throw new TypeError("Forget clock is invalid");
  const before = deps.readControl === undefined ? undefined : await deps.readControl();
  if (input.plan.ownerHost !== undefined && before !== undefined && before.ownerHost !== input.plan.ownerHost) throw new AdminPlanError("Forget plan owner does not match control");
  const initialEpoch = before?.privacyEpoch;
  let expectedEpoch = initialEpoch;
  let barrierConfirmed = false;
  if (deps.createTombstones === undefined || deps.beginForgetBarrier === undefined || deps.readTombstones === undefined) throw new Error("Forget requires durable tombstone and control-barrier capabilities");
  const provenanceIds = uniqueSorted([...input.plan.closure.episodes, ...input.plan.closure.evidence]);
  await deps.createTombstones({ scope: input.plan.scope, targetIds: input.plan.targets, provenanceIds, ...(expectedEpoch === undefined ? {} : { privacyEpoch: expectedEpoch }), ...(input.signal === undefined ? {} : { signal: input.signal }) });
  if (deps.beginForgetBarrier !== undefined) {
    const result = await deps.beginForgetBarrier({ now, ...(expectedEpoch === undefined ? {} : { expectedEpoch }), ...(input.signal === undefined ? {} : { signal: input.signal }) });
    if (typeof result === "object" && result !== null && "privacyEpoch" in result && typeof (result as { privacyEpoch?: unknown }).privacyEpoch === "number") expectedEpoch = (result as { privacyEpoch: number }).privacyEpoch;
  }
  if (deps.invalidateCurrentViews !== undefined) await deps.invalidateCurrentViews();
  if (deps.invalidateCoverage !== undefined) await deps.invalidateCoverage();
  if (deps.quarantineOutbox !== undefined) await deps.quarantineOutbox();
  if (deps.reconcile !== undefined) await deps.reconcile();
  const after = deps.rereadBarrier === undefined ? await (deps.readControl === undefined ? Promise.resolve(undefined) : deps.readControl()) : await deps.rereadBarrier();
  const afterEpoch = typeof after === "object" && after !== null && "privacyEpoch" in after && typeof (after as { privacyEpoch?: unknown }).privacyEpoch === "number" ? (after as { privacyEpoch: number }).privacyEpoch : undefined;
  const expectedAfterEpoch = initialEpoch === undefined ? expectedEpoch : initialEpoch + 1;
  const generationCleared = typeof after === "object" && after !== null && "activeGeneration" in after ? (after as { activeGeneration?: unknown }).activeGeneration === null : false;
  const epochAdvanced = expectedAfterEpoch === undefined ? afterEpoch !== undefined : afterEpoch === expectedAfterEpoch;
  const tombstones = await deps.readTombstones(input.plan.targets);
  const returnedTargets = new Set(tombstones.map((value) => typeof value === "object" && value !== null && "targetId" in value && typeof (value as { targetId?: unknown }).targetId === "string" ? (value as { targetId: string }).targetId : ""));
  barrierConfirmed = generationCleared && epochAdvanced && input.plan.targets.every((target) => returnedTargets.has(target));
  if (!barrierConfirmed) throw new Error("Forget logical barrier was not confirmed");
  return { ok: true, planId: input.plan.id, scope: input.plan.scope, logicalInvisible: true, recurrenceBlocked: input.plan.recurrenceBlocked, physicalDeletion: "eventual_no_backup_claim", tombstonesWritten: true, barrierConfirmed: true };
}
export const applyForget = runForget;
export const forget = runForget;

/** Small helper for callers that need to validate domain-tagged IDs before
 * invoking a durable store. Plain IDs are accepted in plans because operator
 * selectors may be aliases; production adapters should map them to tagged
 * tombstone targets before writing. */
export function isForgetTarget(scope: ForgetScope, value: string): boolean {
  return scope === "occurrence" ? isOccurrenceTarget(value) : scope === "content" ? isContentTarget(value) : isStateTarget(value);
}
