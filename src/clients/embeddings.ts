import { fetchJson, MemoryClientError } from "./http.js";
import { bindConfiguredDestination, canonicalEgressEndpoint } from "../security/egress.js";
import type { AuthorizedDestination, RuntimeConfig } from "../types.js";

export interface EmbeddingsClientOptions {
  baseUrl: string;
  model: string;
  dimension: number;
  queryPrefix: string;
  apiKey?: string;
  timeoutMs: number;
  fetchImpl?: typeof fetch;
}

export interface DocumentEmbeddingClient {
  /** Canonical endpoint pinned at client construction, never caller options. */
  readonly endpoint: string;
  embedDocument(input: { model: string; text: string; signal?: AbortSignal }): Promise<readonly number[]>;
}
type JsonRecord = Record<string, unknown>;

function isRecord(value: unknown): value is JsonRecord { return typeof value === "object" && value !== null && !Array.isArray(value); }
function invalidResponse(message: string): MemoryClientError { return new MemoryClientError("invalid-response", message); }

/** Module-private unexported issuer: real embedding clients are branded at construction. */
const EMBEDDINGS_CLIENT_ISSUER = Symbol("pi-qdrant-memory-v2.embeddings-client-issuer");

/** Module-private per-instance state: frozen options, captured transport, injected flag (WeakMap, undiscoverable). */
interface EmbeddingsState { options: Readonly<EmbeddingsClientOptions>; fetchImpl: typeof fetch; injectedFetch: boolean; }
const EMBEDDINGS_STATE = new WeakMap<object, EmbeddingsState>();
/** Opaque per-instance frozen transport token for embeddings (never {options}). */
const EMBEDDINGS_TOKEN = new WeakMap<object, object>();

export class EmbeddingsClient implements DocumentEmbeddingClient {
  readonly endpoint: string; readonly #issuer: symbol;
  constructor(options: EmbeddingsClientOptions) {
    // GLOBAL RULE: snapshot every untrusted option field EXACTLY ONCE into a
    // plain frozen object; validate/use ONLY the snapshot, never the caller
    // object (no spread, no getter re-read).
    const baseUrl = options.baseUrl;
    const model = options.model;
    const dimension = options.dimension;
    const queryPrefix = options.queryPrefix;
    const apiKey = options.apiKey;
    const timeoutMs = options.timeoutMs;
    const fetchImpl = options.fetchImpl;
    const snapshotOptions: EmbeddingsClientOptions = { baseUrl, model, dimension, queryPrefix, timeoutMs };
    if (apiKey !== undefined) snapshotOptions.apiKey = apiKey;
    if (fetchImpl !== undefined) snapshotOptions.fetchImpl = fetchImpl;
    const endpoint = canonicalEgressEndpoint(snapshotOptions.baseUrl);
    if (typeof snapshotOptions.model !== "string" || snapshotOptions.model.length === 0 || !Number.isInteger(snapshotOptions.dimension) || snapshotOptions.dimension <= 0 || typeof snapshotOptions.queryPrefix !== "string" || !Number.isFinite(snapshotOptions.timeoutMs) || snapshotOptions.timeoutMs <= 0) throw new MemoryClientError("configuration", "Embedding client options are invalid");
    const frozen = Object.freeze({ ...snapshotOptions, baseUrl: endpoint });
    const capturedFetch = frozen.fetchImpl !== undefined ? frozen.fetchImpl : (typeof globalThis !== "undefined" ? globalThis.fetch : fetch).bind(globalThis);
    EMBEDDINGS_STATE.set(this, { options: frozen, fetchImpl: capturedFetch, injectedFetch: frozen.fetchImpl !== undefined });
    EMBEDDINGS_TOKEN.set(this, Object.freeze({}));
    this.endpoint = endpoint; this.#issuer = EMBEDDINGS_CLIENT_ISSUER; Object.freeze(this);
  }
  /** Real-brand check: only genuine EmbeddingsClient instances pass; forged prototypes and structural objects fail. */
  static isValid(value: unknown): value is EmbeddingsClient {
    if (typeof value !== "object" || value === null || !(#issuer in value) || !EMBEDDINGS_STATE.has(value as object)) return false;
    return Object.getPrototypeOf(value) === EmbeddingsClient.prototype && value instanceof EmbeddingsClient && value.#issuer === EMBEDDINGS_CLIENT_ISSUER;
  }
  /** Production-bound: exact brand and NO injected transport (fetchImpl test seams fail); flag is private state. */
  static isProductionBound(value: unknown): value is EmbeddingsClient {
    if (!EmbeddingsClient.isValid(value)) return false;
    const state = EMBEDDINGS_STATE.get(value as object);
    return state !== undefined && state.injectedFetch === false;
  }
  /** Opaque per-instance frozen transport identity (empty object; never options). */
  get transport(): object { const token = EMBEDDINGS_TOKEN.get(this); if (token === undefined) throw new TypeError("Embedding transport token is missing"); return token; }

  private async request(input: string, model: string, signal?: AbortSignal): Promise<number[]> {
    if (!Number.isInteger(EMBEDDINGS_STATE.get(this)!.options.dimension) || EMBEDDINGS_STATE.get(this)!.options.dimension <= 0) throw new MemoryClientError("configuration", "Embedding dimension must be a positive integer");
    const headers: Record<string, string> = { "content-type": "application/json" };
    if (EMBEDDINGS_STATE.get(this)!.options.apiKey !== undefined) headers.authorization = `Bearer ${EMBEDDINGS_STATE.get(this)!.options.apiKey}`;
    const transportOptions: { timeoutMs: number; signal?: AbortSignal; fetchImpl?: typeof fetch } = { timeoutMs: EMBEDDINGS_STATE.get(this)!.options.timeoutMs };
    if (signal !== undefined) transportOptions.signal = signal;
    if (EMBEDDINGS_STATE.get(this)!.fetchImpl !== undefined) transportOptions.fetchImpl = EMBEDDINGS_STATE.get(this)!.fetchImpl;
    const result = await fetchJson<unknown>(`${EMBEDDINGS_STATE.get(this)!.options.baseUrl.replace(/\/+$/, "")}/embeddings`, { method: "POST", headers, body: JSON.stringify({ model, input }) }, transportOptions);
    if (!isRecord(result) || !Array.isArray(result.data)) throw invalidResponse("Embedding response has an invalid data field");
    const first = result.data[0];
    if (!isRecord(first) || !Array.isArray(first.embedding)) throw invalidResponse("Embedding response has no embedding vector");
    if (first.embedding.length !== EMBEDDINGS_STATE.get(this)!.options.dimension) throw invalidResponse("Embedding vector has an unexpected dimension");
    if (!first.embedding.every((value) => typeof value === "number" && Number.isFinite(value))) throw invalidResponse("Embedding vector contains an invalid component");
    return first.embedding as number[];
  }

  async embedQuery(query: string, signal?: AbortSignal): Promise<number[]> { return this.request(`${EMBEDDINGS_STATE.get(this)!.options.queryPrefix}${query}`, EMBEDDINGS_STATE.get(this)!.options.model, signal); }
  /** Task 7 document embeddings deliberately omit queryPrefix and accept BGE-M3 only. */
  async embedDocument(input: { model: string; text: string; signal?: AbortSignal }): Promise<readonly number[]> {
    if (input.model !== "bge-m3" || EMBEDDINGS_STATE.get(this)!.options.model !== "bge-m3" || EMBEDDINGS_STATE.get(this)!.options.dimension !== 1024) throw new MemoryClientError("configuration", "Task 7 requires BGE-M3 with 1024 dimensions");
    if (typeof input.text !== "string" || input.text.length === 0 || input.text.length > 16_000) throw new MemoryClientError("configuration", "Document embedding text is invalid");
    return this.request(input.text, "bge-m3", input.signal);
  }
}

/** Module-private unexported issuer: validated embedding clients are constructed only through `bind`. */
const VALIDATED_EMBEDDING_ISSUER = Symbol("pi-qdrant-memory-v2.validated-embedding-issuer");

/**
 * Nominal endpoint/client pairing; only the REAL EmbeddingsClient brand may be
 * bound — arbitrary structural objects (including real-write/fake-read mixes)
 * fail closed. The emitted-JS constructor requires the module issuer.
 */
export class ValidatedEmbeddingDocumentClient {
  readonly endpoint: string; readonly #issuer: symbol; readonly #embedDocument: DocumentEmbeddingClient["embedDocument"];
  /** Public constructor is unusable without the module-private issuer symbol. */
  constructor(endpoint: string, client: DocumentEmbeddingClient, issuer: symbol) {
    if (issuer !== VALIDATED_EMBEDDING_ISSUER) throw new TypeError("Embedding client requires the module issuer");
    this.#issuer = issuer;
    this.endpoint = endpoint; this.#embedDocument = client.embedDocument.bind(client); Object.freeze(this);
  }
  /** Exposed validating operation only; issuance stays module-private. */
  static isValid(value: unknown): value is ValidatedEmbeddingDocumentClient {
    if (typeof value !== "object" || value === null || !(#issuer in value)) return false;
    return value instanceof ValidatedEmbeddingDocumentClient && value.#issuer === VALIDATED_EMBEDDING_ISSUER;
  }
  embedDocument(input: { model: string; text: string; signal?: AbortSignal }): Promise<readonly number[]> { return this.#embedDocument(input); }
  static bind(input: { endpoint: string; client: DocumentEmbeddingClient }): ValidatedEmbeddingDocumentClient {
    // Snapshot the untrusted endpoint + client EXACTLY ONCE; every check and
    // construction below uses only the locals (no getter re-read).
    const client = input.client;
    const endpoint = canonicalEgressEndpoint(input.endpoint);
    // Only the PRODUCTION-bound real embedding client may be bound: exact brand,
    // exact prototype and NO injected transport (fetchImpl test seams fail).
    if (!EmbeddingsClient.isProductionBound(client) || canonicalEgressEndpoint(client.endpoint) !== endpoint) throw new TypeError("Embedding document client endpoint pairing is invalid");
    if (!Object.isFrozen(client)) Object.freeze(client);
    return new ValidatedEmbeddingDocumentClient(endpoint, client, VALIDATED_EMBEDDING_ISSUER);
  }
}
Object.freeze(ValidatedEmbeddingDocumentClient);
Object.freeze(ValidatedEmbeddingDocumentClient.prototype);
Object.freeze(EmbeddingsClient);
Object.freeze(EmbeddingsClient.prototype);
/** Explicit validated seam used by production clients and endpoint-pinned fakes. */
export function bindEmbeddingDocumentClient(input: { endpoint: string; client: DocumentEmbeddingClient }): ValidatedEmbeddingDocumentClient { return ValidatedEmbeddingDocumentClient.bind(input); }
export interface EmbeddingDestinationFactoryInput {
  endpoint: string;
  destination: AuthorizedDestination;
  client: ValidatedEmbeddingDocumentClient;
  egressMode: RuntimeConfig["privacy"]["egressMode"];
  coordinationPolicyHash: string;
  coordinationPolicyEpoch: number;
  nodeId?: string;
}
export interface EmbeddingDestinationFactory { bind(destination: AuthorizedDestination): BoundEmbeddingDestination; }
/** Module-private unexported issuer: bound embedding destinations are constructed only through the factory. */
const BOUND_EMBEDDING_DESTINATION_ISSUER = Symbol("pi-qdrant-memory-v2.bound-embedding-destination-issuer");

/**
 * Nominal, frozen, privately branded bound embedding destination. It snapshots
 * endpoint/destination/coordination identity and binds embed once; forged
 * prototypes and monkeypatched statics fail the brand check.
 */
export class BoundEmbeddingDestination {
  readonly #issuer: symbol;
  readonly endpoint: string;
  readonly destination: AuthorizedDestination;
  readonly coordination: { readonly policyHash: string; readonly policyEpoch: number };
  readonly #embed: (input: { model: string; text: string; signal?: AbortSignal }) => Promise<readonly number[]>;
  /** Public constructor is unusable without the module-private issuer symbol. */
  constructor(input: { endpoint: string; destination: AuthorizedDestination; coordination: { policyHash: string; policyEpoch: number }; embed: (input: { model: string; text: string; signal?: AbortSignal }) => Promise<readonly number[]>; }, issuer: symbol) {
    if (issuer !== BOUND_EMBEDDING_DESTINATION_ISSUER) throw new TypeError("Embedding destination requires the module issuer");
    this.#issuer = issuer;
    this.endpoint = input.endpoint;
    this.destination = Object.freeze({ ...input.destination });
    this.coordination = Object.freeze({ policyHash: input.coordination.policyHash, policyEpoch: input.coordination.policyEpoch });
    this.#embed = input.embed;
    Object.freeze(this);
  }
  /** Exposed validating operation only; issuance stays module-private. */
  static isValid(value: unknown): value is BoundEmbeddingDestination {
    if (typeof value !== "object" || value === null || !(#issuer in value)) return false;
    return value instanceof BoundEmbeddingDestination && value.#issuer === BOUND_EMBEDDING_DESTINATION_ISSUER;
  }
  embed(input: { model: string; text: string; signal?: AbortSignal }): Promise<readonly number[]> { return this.#embed(input); }
}
Object.freeze(BoundEmbeddingDestination);
Object.freeze(BoundEmbeddingDestination.prototype);

/** Captures immutable endpoint/client/destination snapshots; a later caller mutation cannot retarget egress. */
export function createEmbeddingDestinationFactory(input: EmbeddingDestinationFactoryInput): EmbeddingDestinationFactory {
  // Snapshot every untrusted field EXACTLY ONCE into plain frozen objects.
  const client = input.client;
  const inputDestination = input.destination;
  const destinationId = inputDestination.id;
  const destinationResidency = inputDestination.residency;
  const destinationDataUse = inputDestination.dataUse;
  const destination = Object.freeze({ id: destinationId, residency: destinationResidency, dataUse: destinationDataUse });
  const egressMode = input.egressMode;
  const nodeId = input.nodeId;
  const coordinationPolicyHash = input.coordinationPolicyHash;
  const coordinationPolicyEpoch = input.coordinationPolicyEpoch;
  const endpoint = canonicalEgressEndpoint(input.endpoint);
  if (!ValidatedEmbeddingDocumentClient.isValid(client) || client.endpoint !== endpoint) throw new TypeError("Embedding document client endpoint pairing is invalid");
  if (typeof coordinationPolicyHash !== "string" || coordinationPolicyHash.length === 0 || coordinationPolicyHash.length > 512 || !Number.isSafeInteger(coordinationPolicyEpoch) || coordinationPolicyEpoch < 0) throw new TypeError("Embedding coordination binding is invalid");
  const embedDocument = client.embedDocument.bind(client);
  const configuredIdentity = Object.freeze({ ...destination });
  const coordination = Object.freeze({ policyHash: coordinationPolicyHash, policyEpoch: coordinationPolicyEpoch });
  const configured = bindConfiguredDestination({ endpoint, configuredDestination: configuredIdentity, requestedDestination: configuredIdentity, egressMode, ...(nodeId === undefined ? {} : { nodeId }) });
  return Object.freeze({ bind: (requested: AuthorizedDestination): BoundEmbeddingDestination => {
    const requestedId = requested.id;
    const requestedResidency = requested.residency;
    const requestedDataUse = requested.dataUse;
    const requestedSnapshot = Object.freeze({ id: requestedId, residency: requestedResidency, dataUse: requestedDataUse });
    const destination = Object.freeze({ ...bindConfiguredDestination({ endpoint, configuredDestination: configured, requestedDestination: requestedSnapshot, egressMode, ...(nodeId === undefined ? {} : { nodeId }) }) });
    return new BoundEmbeddingDestination({
      endpoint, destination, coordination,
      embed: async ({ model, text, signal }: { model: string; text: string; signal?: AbortSignal }): Promise<readonly number[]> => {
        if (model !== "bge-m3" || signal?.aborted) throw new Error(model !== "bge-m3" ? "Only BGE-M3 document embeddings are allowed" : "Embedding aborted");
        const vector = await embedDocument({ model: "bge-m3", text, ...(signal === undefined ? {} : { signal }) });
        if (!Array.isArray(vector) || vector.length !== 1024 || !vector.every((value) => typeof value === "number" && Number.isFinite(value))) throw new Error("Embedding vector must have 1024 finite components");
        return Object.freeze([...vector]);
      },
    }, BOUND_EMBEDDING_DESTINATION_ISSUER);
  } });
}

export function bindEmbeddingDestination(factory: EmbeddingDestinationFactory, destination: AuthorizedDestination): BoundEmbeddingDestination {
  // Snapshot the bound function EXACTLY ONCE (never read the getter twice).
  const bindFn = factory?.bind;
  if (typeof bindFn !== "function") throw new TypeError("Embedding destination factory is invalid");
  const destId = destination.id;
  const destResidency = destination.residency;
  const destDataUse = destination.dataUse;
  const dest = Object.freeze({ id: destId, residency: destResidency, dataUse: destDataUse });
  return bindFn(dest);
}
