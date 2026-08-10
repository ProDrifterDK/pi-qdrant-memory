import { createHash } from "node:crypto";
import { canonicalStringify } from "../domain/canonical.js";
const NODE_ID = /^[A-Za-z0-9._-]{1,128}$/u;
function validNode(nodeId) { if (typeof nodeId !== "string" || !NODE_ID.test(nodeId) || nodeId === "local")
    throw new TypeError("A bounded pseudonymous node ID is required"); }
function normalizeEndpoint(endpoint) {
    if (endpoint.startsWith("unix:")) {
        if (endpoint.length > 4096)
            throw new TypeError("Unix endpoint is unbounded");
        return endpoint;
    }
    let url;
    try {
        url = new URL(endpoint);
    }
    catch {
        throw new TypeError("Endpoint must be a URL");
    }
    if (url.protocol !== "http:" && url.protocol !== "https:")
        throw new TypeError("Endpoint must use http(s)");
    if (url.username !== "" || url.password !== "" || url.search !== "" || url.hash !== "")
        throw new TypeError("Endpoint must not contain credentials or query metadata");
    return `${url.protocol}//${url.hostname}${url.port === "" ? "" : `:${url.port}`}${url.pathname.replace(/\/+$/u, "")}`;
}
function isLoopback(endpoint) {
    if (endpoint.startsWith("unix:"))
        return true;
    try {
        const url = new URL(endpoint);
        return (url.hostname === "127.0.0.1" || url.hostname === "localhost" || url.hostname === "[::1]" || url.hostname === "::1") && (url.protocol === "http:" || url.protocol === "https:");
    }
    catch {
        return false;
    }
}
function validLabels(labels) {
    if (typeof labels.residency !== "string" || labels.residency.length === 0 || labels.residency.length > 128 || typeof labels.dataUse !== "string" || labels.dataUse.length === 0 || labels.dataUse.length > 128 || !/^[A-Za-z0-9._:/ -]+$/u.test(labels.residency) || !/^[A-Za-z0-9._:/ -]+$/u.test(labels.dataUse) || /(?:api[-_]?key|token|secret|password)/iu.test(`${labels.residency} ${labels.dataUse}`))
        throw new TypeError("Egress labels are required and redacted");
}
export function localDestinationId(endpoint, nodeId, labels = { residency: "local", dataUse: "memory" }) {
    validNode(nodeId);
    validLabels(labels);
    const normalized = normalizeEndpoint(endpoint);
    if (!isLoopback(normalized))
        throw new Error("local_only egress requires a loopback or Unix-socket endpoint");
    const digest = createHash("sha256").update(canonicalStringify({ dataUse: labels.dataUse, endpoint: normalized, nodeId, residency: labels.residency }), "utf8").digest("hex").slice(0, 32);
    return `local:${digest}`;
}
export function destinationForEndpoint(endpoint, nodeId, labels = { residency: "local", dataUse: "memory" }) {
    validNode(nodeId);
    validLabels(labels);
    const normalized = normalizeEndpoint(endpoint);
    if (!isLoopback(normalized))
        throw new Error("local_only egress requires a loopback or Unix-socket endpoint");
    return { id: localDestinationId(normalized, nodeId, labels), residency: labels.residency, dataUse: labels.dataUse, endpoint: normalized, nodeId };
}
export function isDestinationAllowed(mode, destination, allowlist, options = {}) {
    if (mode === "local_only") {
        if (!("endpoint" in destination) || !("nodeId" in destination) || !isLoopback(destination.endpoint))
            return false;
        if (options.nodeId !== undefined && destination.nodeId !== options.nodeId)
            return false;
        try {
            validNode(destination.nodeId);
            return localDestinationId(destination.endpoint, destination.nodeId, { residency: destination.residency, dataUse: destination.dataUse }) === destination.id && (options.residency === undefined || options.residency === destination.residency) && (options.dataUse === undefined || options.dataUse === destination.dataUse);
        }
        catch {
            return false;
        }
    }
    return allowlist.some((allowed) => allowed.id === destination.id && allowed.residency === destination.residency && allowed.dataUse === destination.dataUse);
}
export function canEgress(input) { return isDestinationAllowed(input.mode, input.destination, input.allowlist, input.options); }
export function assertEgressAllowed(input) { if (!canEgress(input))
    throw new Error("Egress destination is not authorized by the processing policy"); }
//# sourceMappingURL=egress.js.map