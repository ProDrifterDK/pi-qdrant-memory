import { createHash } from "node:crypto";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";

export interface EmbeddingRequestSnapshot {
  readonly method: "POST";
  readonly path: "/v1/embeddings";
  readonly contentType: "application/json";
  readonly model: string;
  readonly input: string;
}

export interface EmbeddingStub {
  readonly baseUrl: string;
  readonly requests: readonly EmbeddingRequestSnapshot[];
  close(): Promise<void>;
}

type JsonRecord = Record<string, unknown>;

function isRecord(value: unknown): value is JsonRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function json(response: ServerResponse, status: number, body: unknown): void {
  const encoded = JSON.stringify(body);
  response.writeHead(status, {
    "content-type": "application/json",
    "content-length": Buffer.byteLength(encoded),
    connection: "close",
  });
  response.end(encoded);
}

function error(response: ServerResponse, status: number, code: string): void {
  json(response, status, { error: { code } });
}

async function readBody(request: IncomingMessage): Promise<string> {
  const chunks: Buffer[] = [];
  let length = 0;
  for await (const chunk of request) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as Uint8Array);
    length += buffer.length;
    if (length > 64 * 1024) throw new Error("request body too large");
    chunks.push(buffer);
  }
  return Buffer.concat(chunks).toString("utf8");
}

function normalizedEmbedding(model: string, input: string): number[] {
  const digest = createHash("sha256").update(model + input).digest();
  const vector = Array.from(digest.subarray(0, 4), byte => (byte - 127.5) / 127.5);
  const norm = Math.hypot(...vector) || 1;
  return vector.map(value => value / norm);
}

async function handle(
  request: IncomingMessage,
  response: ServerResponse,
  requests: EmbeddingRequestSnapshot[],
): Promise<void> {
  const path = new URL(request.url ?? "/", "http://127.0.0.1").pathname;
  if (path !== "/v1/embeddings") {
    error(response, 404, "not_found");
    return;
  }
  if (request.method !== "POST") {
    error(response, 405, "method_not_allowed");
    return;
  }

  const contentType = request.headers["content-type"]?.split(";", 1)[0]?.trim().toLowerCase();
  if (contentType !== "application/json") {
    error(response, 415, "unsupported_media_type");
    return;
  }

  let body: unknown;
  try {
    body = JSON.parse(await readBody(request));
  } catch {
    error(response, 400, "invalid_request");
    return;
  }
  if (
    !isRecord(body) ||
    typeof body.model !== "string" ||
    body.model.trim().length === 0 ||
    typeof body.input !== "string" ||
    body.input.trim().length === 0
  ) {
    error(response, 400, "invalid_request");
    return;
  }

  const model = body.model;
  const input = body.input;
  const snapshot = Object.freeze({
    method: "POST" as const,
    path: "/v1/embeddings" as const,
    contentType: "application/json" as const,
    model,
    input,
  });
  requests.push(snapshot);
  json(response, 200, {
    object: "list",
    data: [{ object: "embedding", embedding: normalizedEmbedding(model, input), index: 0 }],
    model,
    usage: { prompt_tokens: 0, total_tokens: 0 },
  });
}

export async function startEmbeddingStub(): Promise<EmbeddingStub> {
  const requests: EmbeddingRequestSnapshot[] = [];
  const server = createServer((request, response) => {
    void handle(request, response, requests).catch(() => {
      if (!response.headersSent) error(response, 400, "invalid_request");
      else response.destroy();
    });
  });

  await new Promise<void>((resolve, reject) => {
    const onError = (serverError: Error): void => {
      server.off("listening", onListening);
      reject(serverError);
    };
    const onListening = (): void => {
      server.off("error", onError);
      resolve();
    };
    server.once("error", onError);
    server.once("listening", onListening);
    server.listen(0, "127.0.0.1");
  });

  const address = server.address();
  if (address === null || typeof address === "string") {
    server.close();
    throw new Error("embedding stub did not bind a TCP address");
  }
  const port = (address as AddressInfo).port;
  let closePromise: Promise<void> | undefined;
  const close = (): Promise<void> => {
    closePromise ??= new Promise<void>((resolve, reject) => {
      server.close((closeError) => {
        if (closeError !== undefined) reject(closeError);
        else resolve();
      });
      server.closeIdleConnections();
      server.closeAllConnections();
    });
    return closePromise;
  };

  return {
    baseUrl: `http://127.0.0.1:${port}/v1`,
    requests,
    close,
  };
}
