import { fetchJson, MemoryClientError } from "./http.js";
function isRecord(value) {
    return typeof value === "object" && value !== null && !Array.isArray(value);
}
function invalidResponse(message) {
    return new MemoryClientError("invalid-response", message);
}
export class EmbeddingsClient {
    options;
    constructor(options) {
        this.options = options;
    }
    async embedQuery(query, signal) {
        if (!Number.isInteger(this.options.dimension) || this.options.dimension <= 0) {
            throw new MemoryClientError("configuration", "Embedding dimension must be a positive integer");
        }
        const headers = { "content-type": "application/json" };
        if (this.options.apiKey !== undefined) {
            headers.authorization = `Bearer ${this.options.apiKey}`;
        }
        const transportOptions = {
            timeoutMs: this.options.timeoutMs,
        };
        if (signal !== undefined)
            transportOptions.signal = signal;
        if (this.options.fetchImpl !== undefined)
            transportOptions.fetchImpl = this.options.fetchImpl;
        const result = await fetchJson(`${this.options.baseUrl.replace(/\/+$/, "")}/embeddings`, {
            method: "POST",
            headers,
            body: JSON.stringify({
                model: this.options.model,
                input: `${this.options.queryPrefix}${query}`,
            }),
        }, transportOptions);
        if (!isRecord(result) || !Array.isArray(result.data)) {
            throw invalidResponse("Embedding response has an invalid data field");
        }
        const first = result.data[0];
        if (!isRecord(first) || !Array.isArray(first.embedding)) {
            throw invalidResponse("Embedding response has no embedding vector");
        }
        if (first.embedding.length !== this.options.dimension) {
            throw invalidResponse("Embedding vector has an unexpected dimension");
        }
        if (!first.embedding.every((value) => typeof value === "number" && Number.isFinite(value))) {
            throw invalidResponse("Embedding vector contains an invalid component");
        }
        return first.embedding;
    }
}
//# sourceMappingURL=embeddings.js.map