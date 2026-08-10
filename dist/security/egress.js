import { createHash } from "node:crypto";
import { redactAndScan } from "./redaction.js";
import { canonicalStringify } from "../domain/canonical.js";
const NODE_ID = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/u;
export function assertPseudonymousNodeId(nodeId, options = {}) {
    if (typeof nodeId !== "string" || !NODE_ID.test(nodeId) || nodeId === "local" || nodeId === "." || nodeId === "..")
        throw new TypeError("A bounded pseudonymous node ID is required");
    // Only a caller that actually performed machine-id + installation-salt
    // derivation may opt into the fixed one-way digest representation.
    if (options.allowDerivedDigest === true && /^node-[a-f0-9]{32}$/u.test(nodeId))
        return;
    const checked = redactAndScan({ text: nodeId, maxChars: 128, homeDir: "/" });
    if (checked.dropped || checked.secretScan !== "passed" || checked.redactionStatus !== "unchanged" || checked.text !== nodeId)
        throw new TypeError("A bounded pseudonymous node ID is required");
}
function validNode(nodeId) { assertPseudonymousNodeId(nodeId, { allowDerivedDigest: true }); }
function scanDecodedPath(path) {
    let decoded;
    try {
        decoded = decodeURIComponent(path);
    }
    catch {
        throw new TypeError("Endpoint path encoding is invalid");
    }
    const checked = redactAndScan({ text: decoded, maxChars: 4096, homeDir: "/" });
    if (checked.dropped || checked.secretScan !== "passed" || checked.redactionStatus !== "unchanged" || checked.text !== decoded)
        throw new TypeError("Endpoint contains unsafe material");
}
function normalizeEndpoint(endpoint) {
    if (typeof endpoint !== "string" || endpoint.length > 4096)
        throw new TypeError("Endpoint is unbounded");
    const checked = redactAndScan({ text: endpoint, maxChars: 4096, homeDir: "/" });
    if (checked.dropped || checked.secretScan !== "passed" || checked.redactionStatus !== "unchanged" || checked.text !== endpoint)
        throw new TypeError("Endpoint contains unsafe material");
    if (endpoint.startsWith("unix:")) {
        scanDecodedPath(endpoint.slice("unix:".length));
        return endpoint;
    }
    let url;
    try {
        url = new URL(endpoint);
    }
    catch {
        throw new TypeError("Endpoint must be a URL");
    }
    scanDecodedPath(url.pathname);
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
function hasFinalShape(value) {
    return value !== undefined && typeof value.text === "string" && typeof value.contentHash === "string" && typeof value.redactionStatus === "string" && typeof value.secretScan === "string" && typeof value.dropped === "boolean" && value.text.length > 0 && value.dropped === false && value.redactionStatus !== "dropped" && value.secretScan === "passed";
}
function isCanonicalEgressMaterial(value, options) {
    if (!hasFinalShape(value) || !Number.isSafeInteger(options.maxChars) || options.maxChars < 0 || typeof options.homeDir !== "string")
        return false;
    const checked = redactAndScan({ text: value.text, maxChars: options.maxChars, homeDir: options.homeDir });
    return checked.secretScan === "passed" && checked.dropped === false && checked.text === value.text && checked.contentHash === value.contentHash && (value.redactionStatus === "unchanged" || value.redactionStatus === "redacted");
}
export function isFinalEgressMaterial(value, options) {
    return options !== undefined && isCanonicalEgressMaterial(value, options);
}
export function assertFinalEgressMaterial(value, options) {
    if (!isCanonicalEgressMaterial(value, options))
        throw new Error("Only final redacted material with a passed secret scan may egress");
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
export function canEgress(input) {
    if (!isCanonicalEgressMaterial(input.material, input.payload))
        return false;
    return isDestinationAllowed(input.mode, input.destination, input.allowlist, input.options);
}
export function assertEgressAllowed(input) { if (!canEgress(input))
    throw new Error("Egress destination or final payload is not authorized by the processing policy"); }
//# sourceMappingURL=egress.js.map