import { MemoryClientError } from "../clients/http.js";
import type { RuntimeConfig } from "../types.js";
import { AdminQdrantClient, type AdminCollectionInfo } from "./qdrant-admin.js";

const KEYWORD_INDEX_FIELDS = ["host", "project_id", "status", "secret_scan", "source_type"] as const;

export interface InitializeDestinationResult {
  created: boolean;
  collection: string;
  dimension: number;
  distance: "Cosine";
}

interface Dependencies {
  fetchImpl?: typeof fetch;
  signal?: AbortSignal;
}

function isHttpStatus(error: unknown, status: number): error is MemoryClientError {
  return error instanceof MemoryClientError && error.category === "http" && error.status === status;
}

function destinationClient(config: RuntimeConfig, deps: Dependencies): AdminQdrantClient {
  return new AdminQdrantClient({
    baseUrl: config.qdrant.url,
    timeoutMs: config.retrieval.timeoutMs,
    ...(config.admin.destinationApiKey === undefined ? {} : { apiKey: config.admin.destinationApiKey }),
    ...(deps.fetchImpl === undefined ? {} : { fetchImpl: deps.fetchImpl }),
  });
}

function validateDestinationInfo(info: AdminCollectionInfo, dimension: number): void {
  if (
    info.dimension !== dimension ||
    typeof info.distance !== "string" ||
    info.distance.toLowerCase() !== "cosine"
  ) {
    throw new MemoryClientError("configuration", "Destination collection does not match the configured contract");
  }
}

function fieldSchemaIsKeyword(
  payloadSchema: Record<string, unknown> | undefined,
  field: string,
): boolean | undefined {
  if (payloadSchema === undefined || !Object.prototype.hasOwnProperty.call(payloadSchema, field)) return undefined;
  const definition = payloadSchema[field];
  if (
    typeof definition === "object" &&
    definition !== null &&
    !Array.isArray(definition) &&
    (definition as Record<string, unknown>).data_type === "keyword"
  ) {
    return true;
  }
  return false;
}

function validateKnownIndexes(info: AdminCollectionInfo): void {
  for (const field of KEYWORD_INDEX_FIELDS) {
    if (fieldSchemaIsKeyword(info.payloadSchema, field) === false) {
      throw new MemoryClientError("configuration", "Destination payload index has an incompatible schema");
    }
  }
}

async function createIndexIdempotently(
  client: AdminQdrantClient,
  collection: string,
  field: string,
  dimension: number,
  signal: AbortSignal | undefined,
): Promise<void> {
  try {
    await client.createKeywordIndex(collection, field, signal);
    return;
  } catch (error: unknown) {
    if (!isHttpStatus(error, 409)) throw error;
    let reread: AdminCollectionInfo;
    try {
      reread = await client.collectionInfo(collection, signal);
    } catch {
      throw error;
    }
    validateDestinationInfo(reread, dimension);
    if (fieldSchemaIsKeyword(reread.payloadSchema, field) !== true) throw error;
  }
}

export async function initializeDestination(
  config: RuntimeConfig,
  deps: Dependencies = {},
): Promise<InitializeDestinationResult> {
  const client = destinationClient(config, deps);
  const collection = config.qdrant.collection;
  const dimension = config.embeddings.dimension;
  let created = false;
  let existing: AdminCollectionInfo | undefined;

  try {
    existing = await client.collectionInfo(collection, deps.signal);
  } catch (error: unknown) {
    if (!isHttpStatus(error, 404)) throw error;
    try {
      await client.createCollection(collection, dimension, "Cosine", deps.signal);
      created = true;
    } catch (createError: unknown) {
      if (!isHttpStatus(createError, 409)) throw createError;
      // A concurrent creator is safe only if the resulting collection is exact.
      existing = await client.collectionInfo(collection, deps.signal);
      validateDestinationInfo(existing, dimension);
    }
  }

  if (existing !== undefined) {
    validateDestinationInfo(existing, dimension);
    validateKnownIndexes(existing);
  }

  for (const field of KEYWORD_INDEX_FIELDS) {
    if (existing !== undefined && fieldSchemaIsKeyword(existing.payloadSchema, field) === true) continue;
    await createIndexIdempotently(client, collection, field, dimension, deps.signal);
  }

  return { created, collection, dimension, distance: "Cosine" };
}
