import { resolveAgentMarker, type AgentMarker } from "./capture/episode.js";
import type { CollectionMetadataContract, HostId } from "./types.js";

export type HostDetectionResult =
  | { ok: true; host: HostId }
  | { ok: false; reason: "unknown" | "conflict" | "invalid-explicit-host" };

const hostFromExplicit = (explicit: string): HostId | undefined => {
  if (explicit === "prime" || explicit === "pi") return explicit;
  return undefined;
};

const hasMarker = (value: string | undefined): boolean =>
  typeof value === "string" && value.trim().length > 0;

const argvBasename = (value: string): string => {
  const parts = value.split(/[\\/]/);
  return parts[parts.length - 1] ?? "";
};

export function detectHost(input: {
  explicit?: string;
  env: Record<string, string | undefined>;
  argv: readonly string[];
}): HostDetectionResult {
  if (input.explicit !== undefined) {
    const host = hostFromExplicit(input.explicit);
    return host === undefined
      ? { ok: false, reason: "invalid-explicit-host" }
      : { ok: true, host };
  }

  const hosts = new Set<HostId>();
  if (hasMarker(input.env.PRIME_AGENT_CODING_AGENT_DIR)) hosts.add("prime");
  if (hasMarker(input.env.PI_CODING_AGENT_DIR)) hosts.add("pi");

  for (const arg of input.argv) {
    const basename = argvBasename(arg);
    if (basename === "prime-agent") hosts.add("prime");
    if (basename === "pi") hosts.add("pi");
  }

  if (hosts.size === 0) return { ok: false, reason: "unknown" };
  if (hosts.size > 1) return { ok: false, reason: "conflict" };
  return { ok: true, host: [...hosts][0]! };
}

function parsePersistedDepth(value: unknown): number {
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) return value;
  throw new Error("RLM depth must be a non-negative integer");
}

function parseEnvironmentDepth(value: string): number {
  const trimmed = value.trim();
  if (/^\d+$/.test(trimmed)) {
    const depth = Number(trimmed);
    if (Number.isSafeInteger(depth) && depth >= 0) return depth;
  }
  throw new Error("RLM depth must be a non-negative integer");
}

export function resolvePrimeRlmDepth(
  header: unknown,
  env: Record<string, string | undefined>,
): number {
  const record = typeof header === "object" && header !== null ? (header as Record<string, unknown>) : undefined;
  if (record !== undefined && record.rlmDepth !== undefined) {
    return parsePersistedDepth(record.rlmDepth);
  }

  if (env.RLM_DEPTH !== undefined) return parseEnvironmentDepth(env.RLM_DEPTH);
  return 0;
}



/**
 * Resolve the exact host lifecycle marker through the hardened capture parser.
 * Invalid or contradictory metadata is always represented as an ineligible
 * child, so callers cannot accidentally turn ambiguity into root authority.
 */
export function resolveHostAgentMarker(
  host: HostId,
  header: unknown,
  env: Record<string, string | undefined>,
): AgentMarker {
  return resolveAgentMarker({ host, header, env });
}

/** Fail-closed compatibility hook used before a host accepts a destination. */
export function validateCollectionMetadata(
  expectedHost: HostId,
  metadata: Partial<CollectionMetadataContract>,
  expectedModel?: string,
  expectedDimension = 1024,
): asserts metadata is CollectionMetadataContract {
  if (metadata.ownerHost !== expectedHost) throw new Error("Collection owner host mismatch");
  if (metadata.schema !== "pi-qdrant-memory-v2" || metadata.schemaRevision !== 1) throw new Error("Collection schema mismatch");
  if (metadata.dimension !== expectedDimension || metadata.distance !== "Dot") throw new Error("Collection vector metadata mismatch");
  if (expectedModel !== undefined && metadata.model !== expectedModel) throw new Error("Collection model mismatch");
}

export const assertCollectionMetadata = validateCollectionMetadata;
