import type { QdrantFilter } from "../clients/qdrant-readonly.js";
import type { HostId } from "../types.js";

function condition(key: string, value: string): { key: string; match: { value: string } } {
  return { key, match: { value } };
}

function freezeFilter(filter: QdrantFilter): QdrantFilter {
  for (const condition of filter.must) {
    Object.freeze(condition.match);
    Object.freeze(condition);
  }
  if (filter.must_not !== undefined) {
    for (const condition of filter.must_not) {
      Object.freeze(condition.match);
      Object.freeze(condition);
    }
    Object.freeze(filter.must_not);
  }
  Object.freeze(filter.must);
  return Object.freeze(filter);
}

function baseFilter(host: HostId): QdrantFilter {
  return {
    must: [
      condition("host", host),
      condition("status", "active"),
      condition("secret_scan", "passed"),
    ],
  };
}

/** Construct the mandatory current-project lane filter. */
export function projectFilter(host: HostId, projectId: string): QdrantFilter {
  if ((host !== "prime" && host !== "pi") || typeof projectId !== "string" || projectId.length === 0) {
    throw new Error("Invalid retrieval scope");
  }
  const filter = baseFilter(host);
  filter.must.push(condition("project_id", projectId));
  return freezeFilter(filter);
}

/** Construct the mandatory same-host, outside-current-project lane filter. */
export function hostFilter(host: HostId, projectId: string): QdrantFilter {
  if ((host !== "prime" && host !== "pi") || typeof projectId !== "string" || projectId.length === 0) {
    throw new Error("Invalid retrieval scope");
  }
  const filter = baseFilter(host);
  filter.must_not = [condition("project_id", projectId)];
  return freezeFilter(filter);
}
