import { fetchJson, fetchOk, MemoryClientError } from "./http.js";
function isRecord(value) {
    return typeof value === "object" && value !== null && !Array.isArray(value);
}
function invalidResponse(message) {
    return new MemoryClientError("invalid-response", message);
}
function invalidInput(message) {
    return new MemoryClientError("configuration", message);
}
function requestOptions(timeoutMs, signal, fetchImpl) {
    const options = { timeoutMs };
    if (signal !== undefined)
        options.signal = signal;
    if (fetchImpl !== undefined)
        options.fetchImpl = fetchImpl;
    return options;
}
function headers(apiKey, json) {
    const result = {};
    if (json)
        result["content-type"] = "application/json";
    if (apiKey !== undefined)
        result["api-key"] = apiKey;
    return result;
}
function collectionUrl(baseUrl, collection, suffix) {
    return `${baseUrl.replace(/\/+$/, "")}/collections/${encodeURIComponent(collection)}${suffix}`;
}
function isFilterCondition(value) {
    if (!isRecord(value) || typeof value.key !== "string" || !isRecord(value.match))
        return false;
    return typeof value.match.value === "string";
}
function validateFilter(filter) {
    if (!isRecord(filter) || !Array.isArray(filter.must) || !filter.must.every(isFilterCondition)) {
        throw invalidInput("Search filter is invalid");
    }
    if (filter.must_not !== undefined && (!Array.isArray(filter.must_not) || !filter.must_not.every(isFilterCondition))) {
        throw invalidInput("Search filter is invalid");
    }
}
export class ReadonlyQdrantClient {
    options;
    constructor(options) {
        this.options = options;
    }
    async health(signal) {
        await fetchOk(`${this.options.baseUrl.replace(/\/+$/, "")}/healthz`, { method: "GET", headers: headers(this.options.apiKey, false) }, requestOptions(this.options.timeoutMs, signal, this.options.fetchImpl));
    }
    async collectionInfo(signal) {
        const response = await fetchJson(collectionUrl(this.options.baseUrl, this.options.collection, ""), { method: "GET", headers: headers(this.options.apiKey, false) }, requestOptions(this.options.timeoutMs, signal, this.options.fetchImpl));
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
        const dimension = size;
        return { dimension, distance };
    }
    async search(input) {
        if (!Array.isArray(input.vector) || input.vector.length === 0 || !input.vector.every((value) => typeof value === "number" && Number.isFinite(value))) {
            throw invalidInput("Search vector must contain finite numbers");
        }
        if (!Number.isSafeInteger(input.limit) || input.limit < 1) {
            throw invalidInput("Search limit must be a positive integer");
        }
        validateFilter(input.filter);
        const result = await fetchJson(collectionUrl(this.options.baseUrl, this.options.collection, "/points/search"), {
            method: "POST",
            headers: headers(this.options.apiKey, true),
            body: JSON.stringify({
                vector: input.vector,
                limit: input.limit,
                filter: input.filter,
                with_payload: true,
                with_vector: false,
            }),
        }, requestOptions(this.options.timeoutMs, input.signal, this.options.fetchImpl));
        if (!isRecord(result) || !Array.isArray(result.result)) {
            throw invalidResponse("Search response has an invalid result");
        }
        return result.result.map((value) => {
            if (!isRecord(value))
                throw invalidResponse("Search response has an invalid hit");
            const id = value.id;
            const validId = (typeof id === "string" && id.length > 0) ||
                (typeof id === "number" && Number.isSafeInteger(id) && id >= 0);
            if (!validId || typeof value.score !== "number" || !Number.isFinite(value.score) || !isRecord(value.payload)) {
                throw invalidResponse("Search response has an invalid hit");
            }
            return { id, score: value.score, payload: value.payload };
        });
    }
}
//# sourceMappingURL=qdrant-readonly.js.map