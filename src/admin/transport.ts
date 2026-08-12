import { fetchJson, fetchOk, MemoryClientError } from "../clients/http.js";
import { canonicalStringify } from "../domain/canonical.js";
import { assertBootstrapControl, collectionControlPoint, collectionMetadataPoint, collectionVectors, isPhysicalPointId, type PayloadIndexSchema, type PointRecordType } from "../qdrant/schema.js";
import type { ControlRecord } from "../domain/records.js";
import type { HostId } from "../types.js";
import { expectedQdrantCollection, readPolicy, validatePurpose, type HostScopedQdrantCollection, type PointId, type PreparedPoint, type QdrantClientOptions, type QdrantCollectionInfo, type QdrantPoint, type QdrantReadClient, type QdrantReadPolicy, type QdrantScrollResult, type QdrantSearchHit, type ReadOptions, type TypedUpdatePrecondition } from "../qdrant/client.js";

/**
 * @internal
 * Safe ADMIN transport ownership. The raw REST admin/read transport is
 * LEXICAL in this module (never exported): no constructor, factory, writer or
 * client object can be obtained from any package module. The module exposes
 * ONLY named admin operations (functions taking validated options); the
 * human-admin CLIs (admin/cli.ts, init, status) and the admin tests are the
 * only callers. The write session transport lives in qdrant/write.ts, the
 * read-only surface in qdrant/client.ts; neither exposes constructors either.
 */
type Consistency = number | "majority" | "quorum" | "all";
type JsonRecord = Record<string, unknown>;
/** Lexical (non-exported) admin write-capability type; never appears in the public d.ts. */
interface QdrantAdminClient extends QdrantReadClient { createCollection(): Promise<void>; createPayloadIndex(field: string, schema: PayloadIndexSchema): Promise<void>; deletePoints(ids: readonly PointId[]): Promise<void>; }
function isRecord(value: unknown): value is JsonRecord { return typeof value === "object" && value !== null && !Array.isArray(value); }
function failInput(message: string): never { throw new MemoryClientError("configuration", message); }
function failResponse(message: string): never { throw new MemoryClientError("invalid-response", message); }
function validId(value: unknown): value is PointId { return isPhysicalPointId(value); }
function validateId(value: unknown): asserts value is PointId { if (!validId(value)) failInput("Point ID must be a UUID"); }
function validateResponseId(value: unknown): asserts value is PointId { if (!validId(value)) failResponse("Qdrant point ID is invalid"); }
function validateCollection(value: unknown): asserts value is string { if (typeof value !== "string" || !/^[A-Za-z0-9][A-Za-z0-9_.-]{0,254}$/u.test(value)) failInput("Collection name is invalid"); }
function baseUrl(value: string): string { let parsed: URL; try { parsed = new URL(value); } catch { return failInput("Qdrant endpoint is invalid"); } if (!["http:", "https:"].includes(parsed.protocol) || parsed.username !== "" || parsed.password !== "" || parsed.search !== "" || parsed.hash !== "") failInput("Qdrant endpoint is invalid"); return parsed.toString().replace(/\/+$/u, ""); }
function validateTimeout(value: unknown): void { if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) failInput("Request timeout is invalid"); }
function headers(key: string | undefined, json = false): Record<string, string> { const result: Record<string, string> = {}; if (json) result["content-type"] = "application/json"; if (key !== undefined) result["api-key"] = key; return result; }
function requestOptions(options: QdrantClientOptions, fetchImpl: typeof fetch): { timeoutMs: number; signal?: AbortSignal; fetchImpl: typeof fetch } { const result: { timeoutMs: number; signal?: AbortSignal; fetchImpl: typeof fetch } = { timeoutMs: options.timeoutMs, fetchImpl }; if (options.signal !== undefined) result.signal = options.signal; return result; }
function collectionPath(options: QdrantClientOptions, suffix = ""): string { return `${options.baseUrl}/collections/${encodeURIComponent(options.collection)}${suffix}`; }
function consistency(url: string, value: Consistency | undefined): string { if (value === undefined) return url; const parsed = new URL(url); parsed.searchParams.set("consistency", String(value)); return parsed.toString(); }
function validQdrantVersion(value: string): boolean { const match = /^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/u.exec(value); if (match === null) return false; const [major, minor, patch] = value.split("+")[0]!.split(".").map((part) => Number(part)); return [major, minor, patch].every((part) => Number.isSafeInteger(part)); }
function pointWriteUrl(options: QdrantClientOptions): string { const parsed = new URL(collectionPath(options, "/points")); parsed.searchParams.set("wait", "true"); parsed.searchParams.set("ordering", "strong"); return parsed.toString(); }
function isFiniteVector(value: unknown): value is number[] { return Array.isArray(value) && value.length === 1024 && value.every((part) => typeof part === "number" && Number.isFinite(part)); }
function validatePayload(value: unknown, response = false): JsonRecord { if (!isRecord(value)) { if (response) failResponse("Qdrant payload must be an object"); failInput("Qdrant payload must be an object"); } try { canonicalStringify(value); } catch { if (response) failResponse("Qdrant payload must be finite canonical JSON"); failInput("Qdrant payload must be finite canonical JSON"); } return value; }
function normalizePoint(value: PreparedPoint): PreparedPoint { if (!isRecord(value) || !validId(value.id)) failInput("Prepared point ID must be a UUID"); const payload = validatePayload(value.payload); const point: PreparedPoint = { id: value.id, payload }; if (value.vector !== undefined) { if (!isRecord(value.vector) || Object.keys(value.vector).length !== 1 || !isFiniteVector(value.vector.semantic)) failInput("Prepared point must contain one finite semantic vector"); point.vector = { semantic: [...value.vector.semantic] }; } return point; }
function envelope(value: unknown): unknown { if (!isRecord(value) || !("result" in value)) failResponse("Qdrant JSON envelope is invalid"); if (value.status !== undefined && value.status !== "ok") failResponse("Qdrant envelope status is invalid"); return value.result; }
function updateEnvelope(value: unknown): void { const result = envelope(value); if (result === true) return; if (!isRecord(result) || !["acknowledged", "completed", "ok"].includes(String(result.status))) failResponse("Qdrant update did not complete"); if (result.operation_id !== undefined && result.operation_id !== null && (!Number.isSafeInteger(result.operation_id) || Number(result.operation_id) < 0)) failResponse("Qdrant operation ID is invalid"); }
function validatePolicy(policy: QdrantReadPolicy, configuredOwner: HostId): void { if (!isRecord(policy) || policy.ownerHost !== configuredOwner || (policy.ownerHost !== "pi" && policy.ownerHost !== "prime") || policy.requireStatus !== "active" || policy.requireSecretScan !== "passed" || !Number.isFinite(policy.now) || !Number.isFinite(policy.maxClockSkewMs) || policy.maxClockSkewMs < 0 || !Array.isArray(policy.recordTypes) || policy.recordTypes.length === 0 || policy.recordTypes.some((type) => !["episode", "curated_memory", "curated_current", "raptor_summary", "collection_control", "processing_policy", "job", "lease", "proposal", "coverage", "evidence_link", "tombstone", "collection_metadata"].includes(type))) failInput("Read policy is invalid"); try { validatePurpose(policy.purpose, policy.recordTypes); } catch { failInput("Read policy purpose is invalid"); } if (policy.projectId !== undefined && (typeof policy.projectId !== "string" || policy.projectId.length === 0) || policy.processingPolicyId !== undefined && (typeof policy.processingPolicyId !== "string" || policy.processingPolicyId.length === 0)) failInput("Read policy scope is invalid"); }
/** Coordination points are control-state, not memory; their envelope expiry is lease/claim state. */
const COORDINATION_POINT_TYPES = new Set(["collection_control", "processing_policy", "job", "lease", "proposal", "coverage", "evidence_link", "tombstone", "collection_metadata"]);
function validatePayloadForPolicy(payload: JsonRecord, policy: QdrantReadPolicy): void {
  if (payload.owner_host !== policy.ownerHost) failResponse("Qdrant point owner is missing or foreign");
  if (typeof payload.record_type !== "string" || !policy.recordTypes.includes(payload.record_type as PointRecordType)) failResponse("Qdrant point record type is outside the read policy");
  if (payload.status !== policy.requireStatus || payload.secret_scan !== policy.requireSecretScan) failResponse("Qdrant point status/secret policy is invalid");
  const expiry = payload.expires_at; if (policy.purpose !== "metadata" && !COORDINATION_POINT_TYPES.has(payload.record_type) && expiry !== null && (typeof expiry !== "string" || !Number.isFinite(Date.parse(expiry)) || Date.parse(expiry) <= policy.now + policy.maxClockSkewMs)) failResponse("Qdrant point is expired or has an invalid expiry");
  if (payload.record_type === "tombstone" && policy.purpose !== "internal" && policy.purpose !== "write_verification") failResponse("Qdrant tombstones are not readable memory points");
  if (policy.projectId !== undefined && payload.project_id !== policy.projectId) failResponse("Qdrant point project policy mismatch");
  if (policy.processingPolicyId !== undefined && payload.processing_policy_id !== policy.processingPolicyId) failResponse("Qdrant processing policy mismatch");
}
function point(value: unknown, policy: QdrantReadPolicy, includeVector: boolean): QdrantPoint { if (!isRecord(value) || !validId(value.id)) failResponse("Qdrant point ID is invalid"); const payload = validatePayload(value.payload, true); validatePayloadForPolicy(payload, policy); let vector: { semantic: number[] } | undefined; if (value.vector !== undefined) { if (!isRecord(value.vector) || Object.keys(value.vector).length !== 1 || !isFiniteVector(value.vector.semantic)) failResponse("Qdrant named vector is invalid"); if (includeVector) vector = { semantic: [...value.vector.semantic] }; } return vector === undefined ? { id: value.id, payload } : { id: value.id, payload, vector }; }
function responsePoints(value: unknown, policy: QdrantReadPolicy, includeVector: boolean): QdrantPoint[] { if (!Array.isArray(value)) failResponse("Qdrant points result is invalid"); return value.map((item) => point(item, policy, includeVector)); }
type WireKey = "owner_host" | "record_type" | "status" | "secret_scan" | "expires_at" | "project_id" | "processing_policy_id" | "version" | "fencing_token" | "coordination_policy_epoch" | "coordination_policy_hash" | "privacy_epoch" | "state" | "active_base_generation" | "job_id" | "owner_id" | "id" | "accepted_proposal_id" | "accepted_manifest_hash" | "created_at" | "content_hash";
type WireCondition = { key: WireKey; match?: { value?: string | number | boolean; any?: string[] }; range?: { gt?: string; lte?: string } } | { is_null: { key: WireKey } };
type WireFilter = { must: WireCondition[]; must_not: WireCondition[]; should: WireCondition[] };
function serverFilter(policy: QdrantReadPolicy): WireFilter { validatePolicy(policy, policy.ownerHost); const must: WireCondition[] = [{ key: "owner_host", match: { value: policy.ownerHost } }, { key: "status", match: { value: "active" } }, { key: "secret_scan", match: { value: "passed" } }]; if (policy.recordTypes.length === 1) must.push({ key: "record_type", match: { value: policy.recordTypes[0]! } }); else must.push({ key: "record_type", match: { any: [...policy.recordTypes] } }); if (policy.projectId !== undefined) must.push({ key: "project_id", match: { value: policy.projectId } }); if (policy.processingPolicyId !== undefined) must.push({ key: "processing_policy_id", match: { value: policy.processingPolicyId } }); return { must, must_not: policy.purpose === "internal" || policy.purpose === "write_verification" ? [] : [{ key: "record_type", match: { value: "tombstone" } }], should: policy.purpose === "internal" && policy.recordTypes.every((type) => COORDINATION_POINT_TYPES.has(type)) ? [] : [{ is_null: { key: "expires_at" } }, { key: "expires_at", range: { gt: new Date(policy.now + policy.maxClockSkewMs).toISOString() } }] }; }
function responseCollection(value: unknown): QdrantCollectionInfo { const result = envelope(value); if (!isRecord(result) || !isRecord(result.config) || !isRecord(result.config.params) || !isRecord(result.config.params.vectors)) failResponse("Collection configuration is invalid"); const vectors = result.config.params.vectors; if (!isRecord(vectors) || Object.keys(vectors).length !== 1 || !isRecord(vectors.semantic) || vectors.semantic.size !== 1024 || vectors.semantic.distance !== "Cosine") failResponse("Collection must have exactly semantic 1024/Cosine vector"); let pointsCount: number | null = null; if (result.points_count !== undefined && result.points_count !== null) { if (!Number.isSafeInteger(result.points_count) || Number(result.points_count) < 0) failResponse("Collection point count is invalid"); pointsCount = result.points_count as number; } let payloadSchema: JsonRecord | undefined; if (result.payload_schema !== undefined) { if (!isRecord(result.payload_schema)) failResponse("Collection payload schema is invalid"); payloadSchema = {}; for (const [field, value] of Object.entries(result.payload_schema)) { if (!isRecord(value) || typeof value.data_type !== "string" || !["keyword", "integer", "datetime", "text"].includes(value.data_type)) failResponse("Collection payload schema entry is invalid"); payloadSchema[field] = value; } } const status = typeof result.status === "string" ? result.status : undefined; return { ...(status === undefined ? {} : { status }), dimension: 1024, distance: "Cosine", vectors: { semantic: { size: 1024, distance: "Cosine" } }, pointsCount, ...(payloadSchema === undefined ? {} : { payloadSchema }), raw: value }; }
function freezeOptions(input: QdrantClientOptions): QdrantClientOptions {
  // GLOBAL RULE: snapshot every field EXACTLY ONCE into a plain frozen object;
  // validate/use ONLY the snapshot (no spread of the caller object).
  const baseUrlValue = input.baseUrl;
  const collection = input.collection;
  const ownerHost = input.ownerHost;
  const apiKey = input.apiKey;
  const timeoutMs = input.timeoutMs;
  const fetchImpl = input.fetchImpl;
  const readConsistency = input.readConsistency;
  const maxClockSkewMs = input.maxClockSkewMs;
  const signal = input.signal;
  const replicationFactor = input.replicationFactor;
  const writeConsistencyFactor = input.writeConsistencyFactor;
  const snapshot: QdrantClientOptions = { baseUrl: baseUrlValue, collection, ownerHost, timeoutMs };
  if (apiKey !== undefined) snapshot.apiKey = apiKey;
  if (fetchImpl !== undefined) snapshot.fetchImpl = fetchImpl;
  if (readConsistency !== undefined) snapshot.readConsistency = readConsistency;
  if (maxClockSkewMs !== undefined) snapshot.maxClockSkewMs = maxClockSkewMs;
  if (signal !== undefined) snapshot.signal = signal;
  if (replicationFactor !== undefined) snapshot.replicationFactor = replicationFactor;
  if (writeConsistencyFactor !== undefined) snapshot.writeConsistencyFactor = writeConsistencyFactor;
  const endpoint = baseUrl(snapshot.baseUrl);
  validateCollection(snapshot.collection);
  if (snapshot.ownerHost !== "pi" && snapshot.ownerHost !== "prime") failInput("Owner host is invalid");
  if (snapshot.collection !== expectedQdrantCollection(snapshot.ownerHost)) failInput("Qdrant collection does not match owner host");
  if (snapshot.apiKey !== undefined && (typeof snapshot.apiKey !== "string" || snapshot.apiKey.trim() === "")) failInput("Qdrant API key is invalid");
  validateTimeout(snapshot.timeoutMs);
  if (snapshot.maxClockSkewMs !== undefined && (!Number.isFinite(snapshot.maxClockSkewMs) || snapshot.maxClockSkewMs < 0)) failInput("Clock skew is invalid");
  return Object.freeze({ ...snapshot, baseUrl: endpoint, maxClockSkewMs: snapshot.maxClockSkewMs ?? 0 });
}

/**
 * Module-private per-instance state: frozen options, the transport function
 * CAPTURED at construction (never the dynamic global fetch), and the injected
 * flag. No `.options` property is ever emitted; WeakMaps keep apiKey/fetchImpl
 * undiscoverable via Object.keys/property names/symbols/inspection.
 */
interface RestClientState { options: QdrantClientOptions; fetchImpl: typeof fetch; injectedFetch: boolean; }
const REST_STATE = new WeakMap<object, RestClientState>();
/** Opaque per-instance frozen transport token: an empty identity object, never {options}. */
const REST_TOKEN = new WeakMap<object, object>();

function hasTransportToken(value: unknown): value is RestQdrantReadClient { return typeof value === "object" && value !== null && REST_STATE.has(value as object); }
function transportTokenOf(value: RestQdrantReadClient): object | undefined { return REST_TOKEN.get(value); }
function restState(value: RestQdrantReadClient): RestClientState { const state = REST_STATE.get(value); if (state === undefined) throw new TypeError("REST client state is missing"); return state; }

class RestQdrantReadClient implements QdrantReadClient {
  readonly endpoint: string; readonly ownerHost: HostId; readonly collection: HostScopedQdrantCollection; readonly maxClockSkewMs: number;
  constructor(options: QdrantClientOptions) {
    const frozen = freezeOptions(options);
    REST_STATE.set(this, { options: frozen, fetchImpl: options.fetchImpl !== undefined ? options.fetchImpl : (typeof globalThis !== "undefined" ? globalThis.fetch : fetch).bind(globalThis), injectedFetch: options.fetchImpl !== undefined });
    REST_TOKEN.set(this, Object.freeze({}));
    this.endpoint = frozen.baseUrl; this.ownerHost = frozen.ownerHost; this.collection = expectedQdrantCollection(this.ownerHost); this.maxClockSkewMs = frozen.maxClockSkewMs ?? 0; Object.freeze(this);
  }

  async health(): Promise<unknown> { const response = await fetchOk(`${restState(this).options.baseUrl}/healthz`, { method: "GET", headers: headers(restState(this).options.apiKey) }, requestOptions(restState(this).options, restState(this).fetchImpl)); const text = await response.text(); if (text.trim() === "healthz check passed") return text; let parsed: unknown; try { parsed = JSON.parse(text) as unknown; } catch { throw new MemoryClientError("invalid-json", "Health response was not valid JSON"); } if (!isRecord(parsed) || !("result" in parsed)) failResponse("Health response is invalid"); const result = envelope(parsed); if (!isRecord(result) || result.status !== "ok") failResponse("Health response is invalid"); return parsed; }
  async collectionInfo(): Promise<QdrantCollectionInfo> { return responseCollection(await fetchJson<unknown>(consistency(collectionPath(restState(this).options), restState(this).options.readConsistency), { method: "GET", headers: headers(restState(this).options.apiKey) }, requestOptions(restState(this).options, restState(this).fetchImpl))); }
  async retrieve(ids: readonly PointId[], policy: QdrantReadPolicy, options: ReadOptions = {}): Promise<QdrantPoint[]> { validatePolicy(policy, this.ownerHost); if (!Array.isArray(ids) || ids.length === 0 || ids.length > 1024 || ids.some((id) => !validId(id))) failInput("Retrieve IDs are invalid"); const response = await fetchJson<unknown>(consistency(collectionPath(restState(this).options, "/points/retrieve"), restState(this).options.readConsistency), { method: "POST", headers: headers(restState(this).options.apiKey, true), body: JSON.stringify({ ids, with_payload: true, with_vector: options.includeVector === true }) }, requestOptions(restState(this).options, restState(this).fetchImpl)); return responsePoints(envelope(response), policy, options.includeVector === true); }
  async scroll(input: { policy: QdrantReadPolicy; offset?: PointId; limit?: number }): Promise<QdrantScrollResult> { validatePolicy(input.policy, this.ownerHost); if (input.offset !== undefined && !validId(input.offset)) failInput("Scroll offset is invalid"); const limit = input.limit ?? 256; if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1024) failInput("Scroll limit is invalid"); const response = await fetchJson<unknown>(consistency(collectionPath(restState(this).options, "/points/scroll"), restState(this).options.readConsistency), { method: "POST", headers: headers(restState(this).options.apiKey, true), body: JSON.stringify({ offset: input.offset ?? null, limit, with_payload: true, with_vector: false, filter: serverFilter(input.policy) }) }, requestOptions(restState(this).options, restState(this).fetchImpl)); const result = envelope(response); if (!isRecord(result) || !Array.isArray(result.points)) failResponse("Scroll response is invalid"); const next = result.next_page_offset; if (next !== undefined && next !== null) validateResponseId(next); return next === undefined || next === null ? { points: responsePoints(result.points, input.policy, false) } : { points: responsePoints(result.points, input.policy, false), nextOffset: next }; }
  async search(input: { vector: readonly number[]; limit: number; policy: QdrantReadPolicy }): Promise<QdrantSearchHit[]> { validatePolicy(input.policy, this.ownerHost); if (!isFiniteVector(input.vector)) failInput("Search vector must contain finite 1024-dimensional numbers"); if (!Number.isSafeInteger(input.limit) || input.limit < 1 || input.limit > 1024) failInput("Search limit is invalid"); const response = await fetchJson<unknown>(consistency(collectionPath(restState(this).options, "/points/search"), restState(this).options.readConsistency), { method: "POST", headers: headers(restState(this).options.apiKey, true), body: JSON.stringify({ vector: { name: "semantic", vector: [...input.vector] }, limit: input.limit, filter: serverFilter(input.policy), with_payload: true, with_vector: false }) }, requestOptions(restState(this).options, restState(this).fetchImpl)); const result = envelope(response); if (!Array.isArray(result)) failResponse("Search response is invalid"); return result.map((value) => { if (!isRecord(value) || !validId(value.id) || typeof value.score !== "number" || !Number.isFinite(value.score)) failResponse("Search hit is invalid"); const payload = validatePayload(value.payload, true); validatePayloadForPolicy(payload, input.policy); return { id: value.id, score: value.score, payload }; }); }
  async count(policy: QdrantReadPolicy): Promise<number> { validatePolicy(policy, this.ownerHost); const response = await fetchJson<unknown>(consistency(collectionPath(restState(this).options, "/points/count"), restState(this).options.readConsistency), { method: "POST", headers: headers(restState(this).options.apiKey, true), body: JSON.stringify({ exact: true, filter: serverFilter(policy) }) }, requestOptions(restState(this).options, restState(this).fetchImpl)); const result = envelope(response); if (!isRecord(result) || !Number.isSafeInteger(result.count) || Number(result.count) < 0) failResponse("Count response is invalid"); return result.count as number; }
}
/** Module-private unexported issuer: real REST writers are branded at construction. */
const REST_WRITER_ISSUER = Symbol("pi-qdrant-memory-v2.rest-writer-issuer");

class RestQdrantSessionWriter extends RestQdrantReadClient {
  readonly #issuer: symbol;
  constructor(options: QdrantClientOptions) {
    super(options);
    this.#issuer = REST_WRITER_ISSUER;
    Object.freeze(this);
  }
  /** Real-brand check: only genuine REST writers pass — exact prototype (subclasses fail), private issuer and transport token. */
  static isValid(value: unknown): value is RestQdrantSessionWriter {
    if (typeof value !== "object" || value === null || !(#issuer in value) || !hasTransportToken(value)) return false;
    return Object.getPrototypeOf(value) === RestQdrantSessionWriter.prototype && value instanceof RestQdrantSessionWriter && value.#issuer === REST_WRITER_ISSUER;
  }
  async upsertPoints(points: readonly PreparedPoint[], mode: "insert_only" | "update_only", precondition?: TypedUpdatePrecondition): Promise<void> { if (!Array.isArray(points) || points.length === 0 || points.length > 1024 || points.some((point) => !isRecord(point) || !isPhysicalPointId(point.id))) failInput("Prepared points are invalid"); if (mode !== "insert_only" && mode !== "update_only") failInput("Upsert mode is invalid"); if (mode === "update_only" && precondition === undefined) failInput("Update-only precondition is required"); if (precondition !== undefined) validatePrecondition(precondition, this.ownerHost); const normalized = points.map(normalizePoint); for (const point of normalized) if (point.payload.owner_host !== this.ownerHost) failInput("Point owner does not match configured owner"); const body: JsonRecord = { points: normalized, update_mode: mode }; if (precondition !== undefined) body.update_filter = wirePrecondition(precondition); const response = await fetchJson<unknown>(pointWriteUrl(restState(this).options), { method: "PUT", headers: headers(restState(this).options.apiKey, true), body: JSON.stringify(body) }, requestOptions(restState(this).options, restState(this).fetchImpl)); updateEnvelope(response); }
}
function validBoundedText(value: unknown, max = 512): value is string { return typeof value === "string" && value.length > 0 && value.length <= max; }
function validatePrecondition(value: TypedUpdatePrecondition, owner: HostId): void {
  if (!isRecord(value) || value.ownerHost !== owner) failInput("Closed update precondition is invalid");
  if (value.kind === "collection-control-cas") {
    if (value.recordType !== "collection_control" || !["active", "draining", "retired"].includes(value.expectedState) || !Number.isSafeInteger(value.expectedVersion) || value.expectedVersion < 0 || !Number.isSafeInteger(value.expectedEpoch) || value.expectedEpoch < 0 || !Number.isSafeInteger(value.expectedPrivacyEpoch) || value.expectedPrivacyEpoch < 0 || (value.expectedBaseGeneration !== undefined && value.expectedBaseGeneration !== null && !validBoundedText(value.expectedBaseGeneration))) failInput("Closed control precondition is invalid");
    return;
  }
  if (value.kind === "lease-cas") {
    if (value.recordType !== "lease" || !validBoundedText(value.jobId) || !Number.isSafeInteger(value.expectedVersion) || value.expectedVersion < 0 || !Number.isSafeInteger(value.expectedFencingToken) || value.expectedFencingToken < 0 || !Number.isSafeInteger(value.expectedPolicyEpoch) || value.expectedPolicyEpoch < 0 || !validBoundedText(value.expectedPolicyHash) || !Number.isSafeInteger(value.expectedPrivacyEpoch) || value.expectedPrivacyEpoch < 0 || (value.expectedState !== "leased" && value.expectedState !== "accepted" && value.expectedState !== "released") || !validBoundedText(value.expectedOwner) || (value.expectedAcceptedProposalId !== null && !validBoundedText(value.expectedAcceptedProposalId)) || (value.expectedAcceptedManifestHash !== null && !validBoundedText(value.expectedAcceptedManifestHash)) || (value.expectedAcceptedProposalId === null) !== (value.expectedAcceptedManifestHash === null) || !validBoundedText(value.expectedProcessingPolicyId) || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/u.test(value.expectedCreatedAt) || !/^[0-9a-f]{64}$/u.test(value.expectedContentHash) || (value.expiresBefore !== undefined && (!Number.isSafeInteger(value.expiresBefore) || value.expiresBefore < 0)) || (value.expiresAfter !== undefined && (!Number.isSafeInteger(value.expiresAfter) || value.expiresAfter < 0))) failInput("Closed lease precondition is invalid");
    return;
  }
  failInput("Closed update precondition is invalid");
}
function wirePrecondition(value: TypedUpdatePrecondition): WireFilter {
  if (value.kind === "collection-control-cas") {
    const must: WireCondition[] = [{ key: "owner_host", match: { value: value.ownerHost } }, { key: "record_type", match: { value: "collection_control" } }, { key: "version", match: { value: value.expectedVersion } }, { key: "privacy_epoch", match: { value: value.expectedPrivacyEpoch } }, { key: "coordination_policy_epoch", match: { value: value.expectedEpoch } }, { key: "state", match: { value: value.expectedState } } as WireCondition]; if (value.expectedBaseGeneration === null) must.push({ is_null: { key: "active_base_generation" } }); else if (value.expectedBaseGeneration !== undefined) must.push({ key: "active_base_generation", match: { value: value.expectedBaseGeneration } }); return { must, must_not: [], should: [] };
  }
  const must: WireCondition[] = [{ key: "owner_host", match: { value: value.ownerHost } }, { key: "record_type", match: { value: "lease" } }, { key: "job_id", match: { value: value.jobId } }, { key: "version", match: { value: value.expectedVersion } }, { key: "fencing_token", match: { value: value.expectedFencingToken } }, { key: "coordination_policy_epoch", match: { value: value.expectedPolicyEpoch } }, { key: "coordination_policy_hash", match: { value: value.expectedPolicyHash } }, { key: "privacy_epoch", match: { value: value.expectedPrivacyEpoch } }, { key: "state", match: { value: value.expectedState } }, { key: "owner_id", match: { value: value.expectedOwner } }, { key: "processing_policy_id", match: { value: value.expectedProcessingPolicyId } }, { key: "created_at", match: { value: value.expectedCreatedAt } }, { key: "content_hash", match: { value: value.expectedContentHash } }]; if (value.expectedAcceptedProposalId === null) { must.push({ is_null: { key: "accepted_proposal_id" } }, { is_null: { key: "accepted_manifest_hash" } }); } else { must.push({ key: "accepted_proposal_id", match: { value: value.expectedAcceptedProposalId } }, { key: "accepted_manifest_hash", match: { value: value.expectedAcceptedManifestHash as string } }); } if (value.expiresBefore !== undefined) must.push({ key: "expires_at", range: { lte: new Date(value.expiresBefore).toISOString() } }); if (value.expiresAfter !== undefined) must.push({ key: "expires_at", range: { gt: new Date(value.expiresAfter).toISOString() } }); return { must, must_not: [], should: [] };
}
class RestQdrantAdminClient extends RestQdrantReadClient {
  constructor(options: QdrantClientOptions & { apiKey: string }) { super({ ...options, apiKey: options.apiKey }); if (options.apiKey.trim() === "") failInput("Administrative API key is required"); }
  async createCollection(): Promise<void> { const response = await fetchJson<unknown>(collectionPath(restState(this).options), { method: "PUT", headers: headers(restState(this).options.apiKey, true), body: JSON.stringify({ vectors: collectionVectors(), ...(restState(this).options.replicationFactor === undefined ? {} : { replication_factor: restState(this).options.replicationFactor }), ...(restState(this).options.writeConsistencyFactor === undefined ? {} : { write_consistency_factor: restState(this).options.writeConsistencyFactor }) }) }, requestOptions(restState(this).options, restState(this).fetchImpl)); const result = envelope(response); if (result !== true && !(isRecord(result) && ["acknowledged", "completed", "ok"].includes(String(result.status)))) failResponse("Collection creation response is invalid"); }
  async createPayloadIndex(field: string, schema: PayloadIndexSchema): Promise<void> { if (!/^[a-z][a-z0-9_]{0,127}$/u.test(field) || !["keyword", "integer", "datetime", "text"].includes(schema)) failInput("Payload index declaration is invalid"); const url = new URL(collectionPath(restState(this).options, "/index")); url.searchParams.set("wait", "true"); const response = await fetchJson<unknown>(url.toString(), { method: "PUT", headers: headers(restState(this).options.apiKey, true), body: JSON.stringify({ field_name: field, field_schema: schema }) }, requestOptions(restState(this).options, restState(this).fetchImpl)); updateEnvelope(response); }
  async deletePoints(ids: readonly PointId[]): Promise<void> { if (!Array.isArray(ids) || ids.length === 0 || ids.length > 1024 || ids.some((id) => !validId(id))) failInput("Delete IDs are invalid"); const url = new URL(collectionPath(restState(this).options, "/points/delete")); url.searchParams.set("wait", "true"); const response = await fetchJson<unknown>(url.toString(), { method: "POST", headers: headers(restState(this).options.apiKey, true), body: JSON.stringify({ points: ids }) }, requestOptions(restState(this).options, restState(this).fetchImpl)); updateEnvelope(response); }
  async insertMetadataPoint(owner: HostId): Promise<void> { if (owner !== this.ownerHost) failInput("Metadata owner mismatch"); const response = await fetchJson<unknown>(pointWriteUrl(restState(this).options), { method: "PUT", headers: headers(restState(this).options.apiKey, true), body: JSON.stringify({ points: [collectionMetadataPoint(owner)], update_mode: "insert_only" }) }, requestOptions(restState(this).options, restState(this).fetchImpl)); updateEnvelope(response); }
  async insertInitialControlPoint(control: ControlRecord): Promise<void> { assertBootstrapControl(control, this.ownerHost); const response = await fetchJson<unknown>(pointWriteUrl(restState(this).options), { method: "PUT", headers: headers(restState(this).options.apiKey, true), body: JSON.stringify({ points: [collectionControlPoint(control)], update_mode: "insert_only" }) }, requestOptions(restState(this).options, restState(this).fetchImpl)); updateEnvelope(response); }
  async serverInfo(): Promise<{ version: string }> { const response = await fetchJson<unknown>(`${restState(this).options.baseUrl}/`, { method: "GET", headers: headers(restState(this).options.apiKey) }, requestOptions(restState(this).options, restState(this).fetchImpl)); if (!isRecord(response) || typeof response.version !== "string" || !validQdrantVersion(response.version)) failResponse("Qdrant root version is invalid"); const [major, minor, patch] = response.version.split("+")[0]!.split(".").map((part) => Number(part)); if (major! < 1 || (major === 1 && (minor! < 17 || (minor === 17 && patch! < 0)))) failResponse("Qdrant version must be at least 1.17.0"); return { version: response.version }; }
}
Object.freeze(RestQdrantReadClient);
Object.freeze(RestQdrantReadClient.prototype);
Object.freeze(RestQdrantSessionWriter);
Object.freeze(RestQdrantSessionWriter.prototype);
/** Real-brand check for the REST writer class (avoids the value/interface name clash). */
function isRestQdrantSessionWriter(value: unknown): value is RestQdrantSessionWriter { return RestQdrantSessionWriter.isValid(value); }
/** Unpatchable per-instance transport accessor: returns the frozen token or undefined for non-genuine writers. */
function restTransportOf(value: unknown): object | undefined {
  return RestQdrantSessionWriter.isValid(value) ? transportTokenOf(value) : undefined;
}
/**
 * Production-bound writer: the exact real brand AND no injected transport.
 * The injected flag is private per-instance state, never inferred from exposed
 * options. Writers constructed with `fetchImpl` (test seams) are NOT
 * production-bound; production paths stub global fetch instead.
 */
function isProductionRestQdrantSessionWriter(value: unknown): value is RestQdrantSessionWriter {
  if (!RestQdrantSessionWriter.isValid(value)) return false;
  const state = REST_STATE.get(value as object);
  return state !== undefined && state.injectedFetch === false;
}
function adminClientFor(options: QdrantClientOptions & { apiKey: string }, fetchImpl: typeof fetch): RestQdrantAdminClient {
  return new RestQdrantAdminClient({ ...options, apiKey: options.apiKey, fetchImpl });
}
function readClientFor(options: QdrantClientOptions, fetchImpl: typeof fetch): RestQdrantReadClient {
  return new RestQdrantReadClient({ ...options, fetchImpl });
}
/** @internal */ export async function adminServerInfo(options: QdrantClientOptions & { apiKey: string }, fetchImpl: typeof fetch): Promise<{ version: string }> { return adminClientFor(options, fetchImpl).serverInfo(); }
/** @internal */ export async function adminCollectionInfo(options: QdrantClientOptions & { apiKey: string }, fetchImpl: typeof fetch): Promise<QdrantCollectionInfo> { return adminClientFor(options, fetchImpl).collectionInfo(); }
/** @internal */ export async function adminCreateCollection(options: QdrantClientOptions & { apiKey: string }, fetchImpl: typeof fetch): Promise<void> { return adminClientFor(options, fetchImpl).createCollection(); }
/** @internal */ export async function adminCreatePayloadIndex(options: QdrantClientOptions & { apiKey: string }, fetchImpl: typeof fetch, field: string, schema: PayloadIndexSchema): Promise<void> { return adminClientFor(options, fetchImpl).createPayloadIndex(field, schema); }
/** @internal */ export async function adminInsertMetadataPoint(options: QdrantClientOptions & { apiKey: string }, fetchImpl: typeof fetch, owner: HostId): Promise<void> { return adminClientFor(options, fetchImpl).insertMetadataPoint(owner); }
/** @internal */ export async function adminInsertInitialControlPoint(options: QdrantClientOptions & { apiKey: string }, fetchImpl: typeof fetch, control: ControlRecord): Promise<void> { return adminClientFor(options, fetchImpl).insertInitialControlPoint(control); }
/** @internal */ export async function adminRetrieve(options: QdrantClientOptions & { apiKey: string }, fetchImpl: typeof fetch, ids: readonly string[], policy: QdrantReadPolicy, includeVector = false): Promise<QdrantPoint[]> { return adminClientFor(options, fetchImpl).retrieve(ids, policy, { includeVector }); }
/** @internal */ export async function adminHealth(options: QdrantClientOptions & { apiKey: string }, fetchImpl: typeof fetch): Promise<unknown> { return adminClientFor(options, fetchImpl).health(); }
/** @internal */ export async function statusCollectionInfo(options: QdrantClientOptions, fetchImpl: typeof fetch): Promise<QdrantCollectionInfo> { return readClientFor(options, fetchImpl).collectionInfo(); }
/** @internal */ export async function statusHealth(options: QdrantClientOptions, fetchImpl: typeof fetch): Promise<unknown> { return readClientFor(options, fetchImpl).health(); }
/** @internal */ export async function statusRetrieve(options: QdrantClientOptions, fetchImpl: typeof fetch, ids: readonly string[], policy: QdrantReadPolicy): Promise<QdrantPoint[]> { return readClientFor(options, fetchImpl).retrieve(ids, policy); }
