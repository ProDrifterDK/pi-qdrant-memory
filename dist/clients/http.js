export class MemoryClientError extends Error {
    category;
    status;
    constructor(category, message, status) {
        super(message);
        this.category = category;
        this.status = status;
        this.name = "MemoryClientError";
    }
}
function configurationError(message) {
    return new MemoryClientError("configuration", message);
}
function cancelStream(stream) {
    if (stream === null || stream === undefined)
        return;
    try {
        void stream.cancel().catch(() => undefined);
    }
    catch {
        // The request is already being aborted; cancellation is best effort.
    }
}
export async function fetchOk(url, init, options) {
    if (!Number.isFinite(options.timeoutMs) || options.timeoutMs <= 0) {
        throw configurationError("Request timeout must be a positive finite number");
    }
    const fetchImpl = options.fetchImpl ?? fetch;
    const hostSignal = options.signal;
    if (hostSignal?.aborted) {
        throw new MemoryClientError("cancelled", "Request was cancelled");
    }
    const controller = new AbortController();
    let abortReason;
    let cancelBody;
    const abortFromHost = () => {
        if (abortReason === undefined)
            abortReason = "cancelled";
        controller.abort();
        cancelBody?.();
    };
    hostSignal?.addEventListener("abort", abortFromHost, { once: true });
    const timer = setTimeout(() => {
        if (abortReason === undefined)
            abortReason = "timeout";
        controller.abort();
        cancelBody?.();
    }, options.timeoutMs);
    try {
        let response;
        try {
            response = await fetchImpl(url, { ...init, signal: controller.signal });
        }
        catch {
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
            controller.abort();
            throw new MemoryClientError("http", "Request returned an unsuccessful status", response.status);
        }
        // Drain a clone before returning so the same timeout budget covers the body.
        // The original response remains readable by fetchJson or a plain-text caller.
        try {
            const bodyClone = response.clone();
            if (bodyClone.body === null || bodyClone.body === undefined) {
                await bodyClone.arrayBuffer();
            }
            else {
                const reader = bodyClone.body.getReader();
                cancelBody = () => {
                    try {
                        void reader.cancel().catch(() => undefined);
                    }
                    catch {
                        // The body may have completed between the abort and cancellation.
                    }
                    cancelStream(response.body);
                };
                try {
                    while (true) {
                        const chunk = await reader.read();
                        if (chunk.done)
                            break;
                    }
                }
                finally {
                    cancelBody = undefined;
                    reader.releaseLock();
                }
            }
        }
        catch {
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
        return response;
    }
    finally {
        clearTimeout(timer);
        hostSignal?.removeEventListener("abort", abortFromHost);
    }
}
export async function fetchJson(url, init, options) {
    const response = await fetchOk(url, init, options);
    try {
        return (await response.json());
    }
    catch {
        throw new MemoryClientError("invalid-json", "Response was not valid JSON");
    }
}
//# sourceMappingURL=http.js.map