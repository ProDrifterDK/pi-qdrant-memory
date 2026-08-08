import type { QdrantFilter } from "../clients/qdrant-readonly.js";
import type { HostId } from "../types.js";
/** Construct the mandatory current-project lane filter. */
export declare function projectFilter(host: HostId, projectId: string): QdrantFilter;
/** Construct the mandatory same-host, outside-current-project lane filter. */
export declare function hostFilter(host: HostId, projectId: string): QdrantFilter;
