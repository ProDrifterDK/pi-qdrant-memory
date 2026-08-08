export type MemoryErrorCategory =
  | "timeout"
  | "cancelled"
  | "network"
  | "http"
  | "invalid-json"
  | "invalid-response"
  | "configuration";

export class MemoryClientError extends Error {
  constructor(
    readonly category: MemoryErrorCategory,
    message: string,
    readonly status?: number,
  ) {
    super(message);
    this.name = "MemoryClientError";
  }
}

interface FetchOptions {
  timeoutMs: number;
  signal?: AbortSignal;
  fetchImpl?: typeof fetch;
}

function configurationError(message: string): MemoryClientError {
  return new MemoryClientError("configuration", message);
}

export async function fetchOk(
  url: string,
  init: RequestInit,
  options: FetchOptions,
): Promise<Response> {
  if (!Number.isFinite(options.timeoutMs) || options.timeoutMs <= 0) {
    throw configurationError("Request timeout must be a positive finite number");
  }

  const fetchImpl = options.fetchImpl ?? fetch;
  const hostSignal = options.signal;
  if (hostSignal?.aborted) {
    throw new MemoryClientError("cancelled", "Request was cancelled");
  }

  const controller = new AbortController();
  let abortReason: "cancelled" | "timeout" | undefined;
  const abortFromHost = (): void => {
    if (abortReason === undefined) abortReason = "cancelled";
    controller.abort();
  };
  hostSignal?.addEventListener("abort", abortFromHost, { once: true });
  const timer = setTimeout(() => {
    if (abortReason === undefined) abortReason = "timeout";
    controller.abort();
  }, options.timeoutMs);

  try {
    let response: Response;
    try {
      response = await fetchImpl(url, { ...init, signal: controller.signal });
    } catch {
      if (abortReason === "timeout") {
        throw new MemoryClientError("timeout", "Request timed out");
      }
      if (abortReason === "cancelled") {
        throw new MemoryClientError("cancelled", "Request was cancelled");
      }
      throw new MemoryClientError("network", "Request failed");
    }

    if (abortReason === "timeout") {
      throw new MemoryClientError("timeout", "Request timed out");
    }
    if (abortReason === "cancelled") {
      throw new MemoryClientError("cancelled", "Request was cancelled");
    }
    if (!response.ok) {
      throw new MemoryClientError("http", "Request returned an unsuccessful status", response.status);
    }
    return response;
  } finally {
    clearTimeout(timer);
    hostSignal?.removeEventListener("abort", abortFromHost);
  }
}

export async function fetchJson<T>(
  url: string,
  init: RequestInit,
  options: FetchOptions,
): Promise<T> {
  const response = await fetchOk(url, init, options);
  try {
    return (await response.json()) as T;
  } catch {
    throw new MemoryClientError("invalid-json", "Response was not valid JSON");
  }
}
