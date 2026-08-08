import { fetchJson, MemoryClientError } from "../clients/http.js";

export interface AdminPoint {
  id: string | number;
  vector: number[];
  payload: Record<string, unknown>;
}

export interface AdminCollectionInfo {
  dimension: number;
  distance: string;
  pointCount: number | null;
  payloadSchema?: Record<string, unknown>;
}

export interface AdminQdrantClientOptions {
  baseUrl: string;
  apiKey?: string;
  timeoutMs: number;
  fetchImpl?: typeof fetch;
}

type JsonRecord = Record<string, unknown>;
type RequestOptions = { timeoutMs: number; signal?: AbortSignal; fetchImpl?: typeof fetch };
type PointId = string | number;
type Offset = string | number;

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

function validateCollection(collection: string): void {
  if (typeof collection !== "string" || collection.length === 0) {
    throw invalidInput("Collection name is invalid");
  }
}

function validPointId(value: unknown): value is PointId {
  return (
    (typeof value === "string" && value.length > 0) ||
    (typeof value === "number" && Number.isSafeInteger(value) && value >= 0)
  );
}

function validOffset(value: unknown): value is Offset {
  return validPointId(value);
}

function validateOffset(value: unknown): asserts value is Offset {
  if (!validOffset(value)) throw invalidInput("Pagination offset is invalid");
}

function validateDenseVector(value: unknown): number[] {
  if (
    !Array.isArray(value) ||
    value.length === 0 ||
    !value.every((component) => typeof component === "number" && Number.isFinite(component))
  ) {
    throw invalidInput("Point vector must contain finite numbers");
  }
  return value as number[];
}

function validatePayload(value: unknown): Record<string, unknown> {
  if (!isRecord(value)) throw invalidInput("Point payload must be an object");
  return value;
}

function validatePoint(value: unknown): AdminPoint {
  if (!isRecord(value) || !validPointId(value.id)) {
    throw invalidInput("Point is invalid");
  }
  return {
    id: value.id,
    vector: validateDenseVector(value.vector),
    payload: validatePayload(value.payload),
  };
}

function validateDimension(dimension: number): void {
  if (!Number.isSafeInteger(dimension) || dimension <= 0) {
    throw invalidInput("Collection dimension is invalid");
  }
}

function validateDistance(distance: string): void {
  if (distance !== "Cosine") throw invalidInput("Collection distance is invalid");
}

function jsonBody(value: unknown): string {
  try {
    return JSON.stringify(value);
  } catch {
    throw invalidInput("Request body is invalid");
  }
}

function resultEnvelope(value: unknown): unknown {
  if (!isRecord(value) || !("result" in value)) {
    throw invalidResponse("Qdrant response has an invalid result");
  }
  if (value.status !== undefined && value.status !== "ok") {
    throw invalidResponse("Qdrant response has an invalid status");
  }
  return value.result;
}

function updateResult(value: unknown): void {
  const result = resultEnvelope(value);
  if (!isRecord(result) || result.status !== "completed") {
    throw invalidResponse("Qdrant update did not complete");
  }
  const operationId = result.operation_id;
  if (
    operationId !== undefined &&
    operationId !== null &&
    (typeof operationId !== "number" || !Number.isSafeInteger(operationId) || operationId < 0)
  ) {
    throw invalidResponse("Qdrant update has an invalid operation id");
  }
}

function parsePayloadSchema(value: unknown): Record<string, unknown> | undefined {
  if (value === undefined || value === null) return undefined;
  if (!isRecord(value)) throw invalidResponse("Collection response has an invalid payload schema");
  return value;
}

export class AdminQdrantClient {
  constructor(private readonly options: AdminQdrantClientOptions) {}

  async collectionInfo(collection: string, signal?: AbortSignal): Promise<AdminCollectionInfo> {
    validateCollection(collection);
    const response = await fetchJson<unknown>(
      collectionUrl(this.options.baseUrl, collection, ""),
      { method: "GET", headers: headers(this.options.apiKey, false) },
      requestOptions(this.options.timeoutMs, signal, this.options.fetchImpl),
    );
    const result = resultEnvelope(response);
    if (!isRecord(result) || !isRecord(result.config) || !isRecord(result.config.params)) {
      throw invalidResponse("Collection response has an invalid configuration");
    }
    const vectors = result.config.params.vectors;
    if (!isRecord(vectors)) {
      throw invalidResponse("Collection does not have one dense vector configuration");
    }
    const { size, distance } = vectors;
    if (
      typeof size !== "number" ||
      !Number.isSafeInteger(size) ||
      size <= 0 ||
      typeof distance !== "string" ||
      distance.length === 0
    ) {
      throw invalidResponse("Collection does not have one valid dense vector configuration");
    }
    const pointsCount = result.points_count;
    if (
      pointsCount !== undefined &&
      pointsCount !== null &&
      (typeof pointsCount !== "number" || !Number.isSafeInteger(pointsCount) || pointsCount < 0)
    ) {
      throw invalidResponse("Collection response has an invalid point count");
    }
    const info: AdminCollectionInfo = {
      dimension: size,
      distance,
      pointCount: pointsCount === undefined ? null : pointsCount,
    };
    const payloadSchema = parsePayloadSchema(result.payload_schema);
    if (payloadSchema !== undefined) info.payloadSchema = payloadSchema;
    return info;
  }

  async createCollection(
    collection: string,
    dimension: number,
    distance: "Cosine",
    signal?: AbortSignal,
  ): Promise<void> {
    validateCollection(collection);
    validateDimension(dimension);
    validateDistance(distance);
    const response = await fetchJson<unknown>(
      collectionUrl(this.options.baseUrl, collection, ""),
      {
        method: "PUT",
        headers: headers(this.options.apiKey, true),
        body: jsonBody({ vectors: { size: dimension, distance } }),
      },
      requestOptions(this.options.timeoutMs, signal, this.options.fetchImpl),
    );
    if (resultEnvelope(response) !== true) {
      throw invalidResponse("Collection creation response is invalid");
    }
  }

  async createKeywordIndex(collection: string, field: string, signal?: AbortSignal): Promise<void> {
    validateCollection(collection);
    if (typeof field !== "string" || field.length === 0) {
      throw invalidInput("Index field is invalid");
    }
    const response = await fetchJson<unknown>(
      collectionUrl(this.options.baseUrl, collection, "/index?wait=true"),
      {
        method: "PUT",
        headers: headers(this.options.apiKey, true),
        body: jsonBody({ field_name: field, field_schema: "keyword" }),
      },
      requestOptions(this.options.timeoutMs, signal, this.options.fetchImpl),
    );
    updateResult(response);
  }

  async scroll(
    collection: string,
    offset?: string | number,
    limit = 256,
    signal?: AbortSignal,
  ): Promise<{ points: AdminPoint[]; nextOffset?: string | number }> {
    validateCollection(collection);
    if (!Number.isSafeInteger(limit) || limit <= 0) throw invalidInput("Scroll limit is invalid");
    if (offset !== undefined) validateOffset(offset);
    const response = await fetchJson<unknown>(
      collectionUrl(this.options.baseUrl, collection, "/points/scroll"),
      {
        method: "POST",
        headers: headers(this.options.apiKey, true),
        body: jsonBody({ offset: offset ?? null, limit, with_payload: true, with_vector: true }),
      },
      requestOptions(this.options.timeoutMs, signal, this.options.fetchImpl),
    );
    const result = resultEnvelope(response);
    if (!isRecord(result) || !Array.isArray(result.points)) {
      throw invalidResponse("Scroll response has an invalid result");
    }
    const points = result.points.map((point) => {
      try {
        return validatePoint(point);
      } catch (error: unknown) {
        if (error instanceof MemoryClientError && error.category === "configuration") {
          throw invalidResponse("Scroll response has an invalid point");
        }
        throw error;
      }
    });
    const rawOffset = result.next_page_offset;
    if (rawOffset === undefined || rawOffset === null) return { points };
    if (!validOffset(rawOffset)) throw invalidResponse("Scroll response has an invalid pagination offset");
    return { points, nextOffset: rawOffset };
  }

  async upsert(collection: string, points: readonly AdminPoint[], signal?: AbortSignal): Promise<void> {
    validateCollection(collection);
    if (!Array.isArray(points)) throw invalidInput("Points are invalid");
    const validPoints = points.map((point) => validatePoint(point));
    const response = await fetchJson<unknown>(
      collectionUrl(this.options.baseUrl, collection, "/points?wait=true"),
      {
        method: "PUT",
        headers: headers(this.options.apiKey, true),
        body: jsonBody({ points: validPoints }),
      },
      requestOptions(this.options.timeoutMs, signal, this.options.fetchImpl),
    );
    updateResult(response);
  }
}
