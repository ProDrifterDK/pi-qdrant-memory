import { fetchJson, MemoryClientError } from "./http.js";
import { bindConfiguredDestination, canonicalEgressEndpoint } from "../security/egress.js";
import type { AuthorizedDestination, RuntimeConfig } from "../types.js";
import type { BoundEmbeddingDestination } from "../outbox/delivery.js";

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

export class EmbeddingsClient implements DocumentEmbeddingClient {
  readonly endpoint: string; private readonly options: Readonly<EmbeddingsClientOptions>;
  constructor(options: EmbeddingsClientOptions) {
    const endpoint = canonicalEgressEndpoint(options.baseUrl);
    if (typeof options.model !== "string" || options.model.length === 0 || !Number.isInteger(options.dimension) || options.dimension <= 0 || typeof options.queryPrefix !== "string" || !Number.isFinite(options.timeoutMs) || options.timeoutMs <= 0) throw new MemoryClientError("configuration", "Embedding client options are invalid");
    this.options = Object.freeze({ ...options, baseUrl: endpoint }); this.endpoint = endpoint; Object.freeze(this);
  }

  private async request(input: string, model: string, signal?: AbortSignal): Promise<number[]> {
    if (!Number.isInteger(this.options.dimension) || this.options.dimension <= 0) throw new MemoryClientError("configuration", "Embedding dimension must be a positive integer");
    const headers: Record<string, string> = { "content-type": "application/json" };
    if (this.options.apiKey !== undefined) headers.authorization = `Bearer ${this.options.apiKey}`;
    const transportOptions: { timeoutMs: number; signal?: AbortSignal; fetchImpl?: typeof fetch } = { timeoutMs: this.options.timeoutMs };
    if (signal !== undefined) transportOptions.signal = signal;
    if (this.options.fetchImpl !== undefined) transportOptions.fetchImpl = this.options.fetchImpl;
    const result = await fetchJson<unknown>(`${this.options.baseUrl.replace(/\/+$/, "")}/embeddings`, { method: "POST", headers, body: JSON.stringify({ model, input }) }, transportOptions);
    if (!isRecord(result) || !Array.isArray(result.data)) throw invalidResponse("Embedding response has an invalid data field");
    const first = result.data[0];
    if (!isRecord(first) || !Array.isArray(first.embedding)) throw invalidResponse("Embedding response has no embedding vector");
    if (first.embedding.length !== this.options.dimension) throw invalidResponse("Embedding vector has an unexpected dimension");
    if (!first.embedding.every((value) => typeof value === "number" && Number.isFinite(value))) throw invalidResponse("Embedding vector contains an invalid component");
    return first.embedding as number[];
  }

  async embedQuery(query: string, signal?: AbortSignal): Promise<number[]> { return this.request(`${this.options.queryPrefix}${query}`, this.options.model, signal); }
  /** Task 7 document embeddings deliberately omit queryPrefix and accept BGE-M3 only. */
  async embedDocument(input: { model: string; text: string; signal?: AbortSignal }): Promise<readonly number[]> {
    if (input.model !== "bge-m3" || this.options.model !== "bge-m3" || this.options.dimension !== 1024) throw new MemoryClientError("configuration", "Task 7 requires BGE-M3 with 1024 dimensions");
    if (typeof input.text !== "string" || input.text.length === 0 || input.text.length > 16_000) throw new MemoryClientError("configuration", "Document embedding text is invalid");
    return this.request(input.text, "bge-m3", input.signal);
  }
}

/** Nominal endpoint/client pairing; raw structural embedding clients cannot enter a factory. */
export class ValidatedEmbeddingDocumentClient {
  readonly endpoint: string; readonly #embedDocument: DocumentEmbeddingClient["embedDocument"];
  private constructor(endpoint: string, client: DocumentEmbeddingClient) { this.endpoint = endpoint; this.#embedDocument = client.embedDocument.bind(client); Object.freeze(this); }
  embedDocument(input: { model: string; text: string; signal?: AbortSignal }): Promise<readonly number[]> { return this.#embedDocument(input); }
  static bind(input: { endpoint: string; client: DocumentEmbeddingClient }): ValidatedEmbeddingDocumentClient {
    const endpoint = canonicalEgressEndpoint(input.endpoint);
    if (typeof input.client?.embedDocument !== "function" || typeof input.client.endpoint !== "string" || canonicalEgressEndpoint(input.client.endpoint) !== endpoint) throw new TypeError("Embedding document client endpoint pairing is invalid");
    return new ValidatedEmbeddingDocumentClient(endpoint, Object.freeze(input.client));
  }
}
/** Explicit validated seam used by production clients and endpoint-pinned fakes. */
export function bindEmbeddingDocumentClient(input: { endpoint: string; client: DocumentEmbeddingClient }): ValidatedEmbeddingDocumentClient { return ValidatedEmbeddingDocumentClient.bind(input); }
export interface EmbeddingDestinationFactoryInput {
  endpoint: string;
  destination: AuthorizedDestination;
  client: ValidatedEmbeddingDocumentClient;
  egressMode: RuntimeConfig["privacy"]["egressMode"];
  nodeId?: string;
}
export interface EmbeddingDestinationFactory { bind(destination: AuthorizedDestination): BoundEmbeddingDestination; }
/** Captures immutable endpoint/client/destination snapshots; a later caller mutation cannot retarget egress. */
export function createEmbeddingDestinationFactory(input: EmbeddingDestinationFactoryInput): EmbeddingDestinationFactory {
  const endpoint = canonicalEgressEndpoint(input.endpoint); if (!(input.client instanceof ValidatedEmbeddingDocumentClient) || input.client.endpoint !== endpoint) throw new TypeError("Embedding document client endpoint pairing is invalid");
  const embedDocument = input.client.embedDocument.bind(input.client); const egressMode = input.egressMode; const nodeId = input.nodeId; const configuredIdentity = Object.freeze({ ...input.destination });
  const configured = bindConfiguredDestination({ endpoint, configuredDestination: configuredIdentity, requestedDestination: configuredIdentity, egressMode, ...(nodeId === undefined ? {} : { nodeId }) });
  return Object.freeze({ bind: (requested: AuthorizedDestination): BoundEmbeddingDestination => {
    const destination = Object.freeze({ ...bindConfiguredDestination({ endpoint, configuredDestination: configured, requestedDestination: requested, egressMode, ...(nodeId === undefined ? {} : { nodeId }) }) });
    return Object.freeze({ destination, embed: async ({ model, text, signal }: { model: string; text: string; signal?: AbortSignal }): Promise<readonly number[]> => {
      if (model !== "bge-m3" || signal?.aborted) throw new Error(model !== "bge-m3" ? "Only BGE-M3 document embeddings are allowed" : "Embedding aborted");
      const vector = await embedDocument({ model: "bge-m3", text, ...(signal === undefined ? {} : { signal }) });
      if (!Array.isArray(vector) || vector.length !== 1024 || !vector.every((value) => typeof value === "number" && Number.isFinite(value))) throw new Error("Embedding vector must have 1024 finite components");
      return Object.freeze([...vector]);
    } });
  } });
}
export function bindEmbeddingDestination(factory: EmbeddingDestinationFactory, destination: AuthorizedDestination): BoundEmbeddingDestination {
  if (typeof factory?.bind !== "function") throw new TypeError("Embedding destination factory is invalid"); return factory.bind(destination);
}
