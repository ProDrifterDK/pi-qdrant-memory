import { fetchJson, fetchOk, MemoryClientError } from "./http.js";

export interface QdrantFilter {
  must: Array<{ key: string; match: { value: string } }>;
  must_not?: Array<{ key: string; match: { value: string } }>;
}

export interface QdrantSearchHit {
  id: string | number;
  score: number;
  payload: Record<string, unknown>;
}

interface ReadonlyQdrantClientOptions {
  baseUrl: string;
  collection: string;
  apiKey?: string;
  timeoutMs: number;
  fetchImpl?: typeof fetch;
}

type JsonRecord = Record<string, unknown>;
type RequestOptions = { timeoutMs: number; signal?: AbortSignal; fetchImpl?: typeof fetch };

function isRecord(value: unknown): value is JsonRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function invalidResponse(message: string): MemoryClientError {
  return new MemoryClientError("invalid-response", message);
}

function invalidInput(message: string): MemoryClientError {
  return new MemoryClientError("configuration", message);
}

function requestOptions(
  timeoutMs: number,
  signal: AbortSignal | undefined,
  fetchImpl: typeof fetch | undefined,
): RequestOptions {
  const options: RequestOptions = { timeoutMs };
  if (signal !== undefined) options.signal = signal;
  if (fetchImpl !== undefined) options.fetchImpl = fetchImpl;
  return options;
}

function headers(apiKey: string | undefined, json: boolean): Record<string, string> {
  const result: Record<string, string> = {};
  if (json) result["content-type"] = "application/json";
  if (apiKey !== undefined) result["api-key"] = apiKey;
  return result;
}

function collectionUrl(baseUrl: string, collection: string, suffix: string): string {
  return `${baseUrl.replace(/\/+$/, "")}/collections/${encodeURIComponent(collection)}${suffix}`;
}

function isFilterCondition(value: unknown): value is { key: string; match: { value: string } } {
  if (!isRecord(value) || typeof value.key !== "string" || !isRecord(value.match)) return false;
  return typeof value.match.value === "string";
}

function validateFilter(filter: QdrantFilter): void {
  if (!isRecord(filter) || !Array.isArray(filter.must) || !filter.must.every(isFilterCondition)) {
    throw invalidInput("Search filter is invalid");
  }
  if (filter.must_not !== undefined && (!Array.isArray(filter.must_not) || !filter.must_not.every(isFilterCondition))) {
    throw invalidInput("Search filter is invalid");
  }
}

export class ReadonlyQdrantClient {
  constructor(private readonly options: ReadonlyQdrantClientOptions) {}

  async health(signal?: AbortSignal): Promise<void> {
    await fetchOk(
      `${this.options.baseUrl.replace(/\/+$/, "")}/healthz`,
      { method: "GET", headers: headers(this.options.apiKey, false) },
      requestOptions(this.options.timeoutMs, signal, this.options.fetchImpl),
    );
  }

  async collectionInfo(signal?: AbortSignal): Promise<{ dimension: number; distance: string }> {
    const response = await fetchJson<unknown>(
      collectionUrl(this.options.baseUrl, this.options.collection, ""),
      { method: "GET", headers: headers(this.options.apiKey, false) },
      requestOptions(this.options.timeoutMs, signal, this.options.fetchImpl),
    );
    if (!isRecord(response) || !isRecord(response.result)) {
      throw invalidResponse("Collection response has an invalid result");
    }
    const config = response.result.config;
    if (!isRecord(config) || !isRecord(config.params)) {
      throw invalidResponse("Collection response has an invalid configuration");
    }
    const vectors = config.params.vectors;
    if (!isRecord(vectors)) {
      throw invalidResponse("Collection does not have one dense vector configuration");
    }
    const { size, distance } = vectors;
    if (typeof size !== "number" || !Number.isSafeInteger(size) || size <= 0 || typeof distance !== "string" || distance.length === 0) {
      throw invalidResponse("Collection does not have one valid dense vector configuration");
    }
    const dimension = size as number;
    return { dimension, distance };
  }

  async search(input: {
    vector: number[];
    limit: number;
    filter: QdrantFilter;
    signal?: AbortSignal;
  }): Promise<QdrantSearchHit[]> {
    if (!Array.isArray(input.vector) || input.vector.length === 0 || !input.vector.every((value) => typeof value === "number" && Number.isFinite(value))) {
      throw invalidInput("Search vector must contain finite numbers");
    }
    if (!Number.isSafeInteger(input.limit) || input.limit < 1) {
      throw invalidInput("Search limit must be a positive integer");
    }
    validateFilter(input.filter);

    const result = await fetchJson<unknown>(
      collectionUrl(this.options.baseUrl, this.options.collection, "/points/search"),
      {
        method: "POST",
        headers: headers(this.options.apiKey, true),
        body: JSON.stringify({
          vector: input.vector,
          limit: input.limit,
          filter: input.filter,
          with_payload: true,
          with_vector: false,
        }),
      },
      requestOptions(this.options.timeoutMs, input.signal, this.options.fetchImpl),
    );
    if (!isRecord(result) || !Array.isArray(result.result)) {
      throw invalidResponse("Search response has an invalid result");
    }

    return result.result.map((value): QdrantSearchHit => {
      if (!isRecord(value)) throw invalidResponse("Search response has an invalid hit");
      const id = value.id;
      const validId =
        (typeof id === "string" && id.length > 0) ||
        (typeof id === "number" && Number.isSafeInteger(id) && id >= 0);
      if (!validId || typeof value.score !== "number" || !Number.isFinite(value.score) || !isRecord(value.payload)) {
        throw invalidResponse("Search response has an invalid hit");
      }
      return { id, score: value.score, payload: value.payload };
    });
  }
}
