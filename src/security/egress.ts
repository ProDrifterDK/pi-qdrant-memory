import { createHash } from "node:crypto";
import type { AuthorizedDestination, RuntimeConfig } from "../types.js";
import { canonicalStringify } from "../domain/canonical.js";

export interface EgressDestination extends AuthorizedDestination {
  endpoint: string;
  nodeId: string;
}
const NODE_ID = /^[A-Za-z0-9._-]{1,128}$/u;
function validNode(nodeId: unknown): asserts nodeId is string { if (typeof nodeId !== "string" || !NODE_ID.test(nodeId) || nodeId === "local") throw new TypeError("A bounded pseudonymous node ID is required"); }
function normalizeEndpoint(endpoint: string): string {
  if (endpoint.startsWith("unix:")) { if (endpoint.length > 4096) throw new TypeError("Unix endpoint is unbounded"); return endpoint; }
  let url: URL; try { url = new URL(endpoint); } catch { throw new TypeError("Endpoint must be a URL"); }
  if (url.protocol !== "http:" && url.protocol !== "https:") throw new TypeError("Endpoint must use http(s)");
  if (url.username !== "" || url.password !== "" || url.search !== "" || url.hash !== "") throw new TypeError("Endpoint must not contain credentials or query metadata");
  return `${url.protocol}//${url.hostname}${url.port === "" ? "" : `:${url.port}`}${url.pathname.replace(/\/+$/u, "")}`;
}
function isLoopback(endpoint: string): boolean {
  if (endpoint.startsWith("unix:")) return true;
  try { const url = new URL(endpoint); return (url.hostname === "127.0.0.1" || url.hostname === "localhost" || url.hostname === "[::1]" || url.hostname === "::1") && (url.protocol === "http:" || url.protocol === "https:"); } catch { return false; }
}
function validLabels(labels: Pick<AuthorizedDestination, "residency" | "dataUse">): void {
  if (typeof labels.residency !== "string" || labels.residency.length === 0 || labels.residency.length > 128 || typeof labels.dataUse !== "string" || labels.dataUse.length === 0 || labels.dataUse.length > 128 || !/^[A-Za-z0-9._:/ -]+$/u.test(labels.residency) || !/^[A-Za-z0-9._:/ -]+$/u.test(labels.dataUse) || /(?:api[-_]?key|token|secret|password)/iu.test(`${labels.residency} ${labels.dataUse}`)) throw new TypeError("Egress labels are required and redacted");
}
export function localDestinationId(endpoint: string, nodeId: string, labels: Pick<AuthorizedDestination, "residency" | "dataUse"> = { residency: "local", dataUse: "memory" }): string {
  validNode(nodeId); validLabels(labels); const normalized = normalizeEndpoint(endpoint); if (!isLoopback(normalized)) throw new Error("local_only egress requires a loopback or Unix-socket endpoint");
  const digest = createHash("sha256").update(canonicalStringify({ dataUse: labels.dataUse, endpoint: normalized, nodeId, residency: labels.residency }), "utf8").digest("hex").slice(0, 32);
  return `local:${digest}`;
}
export function destinationForEndpoint(endpoint: string, nodeId: string, labels: Pick<AuthorizedDestination, "residency" | "dataUse"> = { residency: "local", dataUse: "memory" }): EgressDestination {
  validNode(nodeId); validLabels(labels); const normalized = normalizeEndpoint(endpoint); if (!isLoopback(normalized)) throw new Error("local_only egress requires a loopback or Unix-socket endpoint");
  return { id: localDestinationId(normalized, nodeId, labels), residency: labels.residency, dataUse: labels.dataUse, endpoint: normalized, nodeId };
}
export interface EgressCheckOptions { nodeId?: string; residency?: string; dataUse?: string }
export function isDestinationAllowed(mode: RuntimeConfig["privacy"]["egressMode"], destination: AuthorizedDestination | EgressDestination, allowlist: readonly AuthorizedDestination[], options: EgressCheckOptions = {}): boolean {
  if (mode === "local_only") {
    if (!("endpoint" in destination) || !("nodeId" in destination) || !isLoopback(destination.endpoint)) return false;
    if (options.nodeId !== undefined && destination.nodeId !== options.nodeId) return false;
    try { validNode(destination.nodeId); return localDestinationId(destination.endpoint, destination.nodeId, { residency: destination.residency, dataUse: destination.dataUse }) === destination.id && (options.residency === undefined || options.residency === destination.residency) && (options.dataUse === undefined || options.dataUse === destination.dataUse); } catch { return false; }
  }
  return allowlist.some((allowed) => allowed.id === destination.id && allowed.residency === destination.residency && allowed.dataUse === destination.dataUse);
}
export function canEgress(input: { mode: RuntimeConfig["privacy"]["egressMode"]; destination: AuthorizedDestination | EgressDestination; allowlist: readonly AuthorizedDestination[]; options?: EgressCheckOptions }): boolean { return isDestinationAllowed(input.mode, input.destination, input.allowlist, input.options); }
export function assertEgressAllowed(input: Parameters<typeof canEgress>[0]): void { if (!canEgress(input)) throw new Error("Egress destination is not authorized by the processing policy"); }
