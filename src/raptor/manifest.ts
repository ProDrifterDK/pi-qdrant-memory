import { canonicalStringify, sha256Hex } from "../domain/canonical.js";
import type { HostId } from "../types.js";

const MAX_LEAVES = 65_536;
const MAX_CHUNK = 1024;
const HEX = /^[a-f0-9]{64}$/u;
export interface RaptorManifestChunk { readonly id: string; readonly index: number; readonly memberIds: readonly string[]; readonly contentHash: string; }
export interface RaptorManifestRoot {
  readonly id: string; readonly ownerHost: HostId; readonly membershipHash: string; readonly merkleRoot: string; readonly chunkIds: readonly string[]; readonly leafCount: number; readonly chunkSize: number;
  readonly policyId: string; readonly policyHash: string; readonly policyEpoch: number; readonly privacyEpoch: number; readonly algorithm: string; readonly promptRevision: string; readonly modelId: string; readonly seed: string;
}
export interface RaptorManifest { readonly root: RaptorManifestRoot; readonly chunks: readonly RaptorManifestChunk[]; }
export interface BuildManifestInput { readonly ownerHost: HostId; readonly leafIds: readonly string[]; readonly chunkSize: number; readonly policyId: string; readonly policyHash: string; readonly policyEpoch: number; readonly privacyEpoch: number; readonly algorithm: string; readonly promptRevision: string; readonly modelId: string; readonly seed: string | number; }

function boundedText(name: string, value: unknown, max = 512): string { if (typeof value !== "string" || value.length === 0 || value.length > max) throw new TypeError(`RAPTOR manifest ${name} is invalid`); return value; }
function merkle(hashes: readonly string[]): string {
  if (hashes.length === 0) return sha256Hex(canonicalStringify({ domain: "raptor-merkle-empty-v1" }));
  let level = [...hashes];
  while (level.length > 1) { const next: string[] = []; for (let index = 0; index < level.length; index += 2) next.push(sha256Hex(canonicalStringify({ domain: "raptor-merkle-node-v1", left: level[index]!, right: level[index + 1] ?? level[index]! }))); level = next; }
  return level[0]!;
}
function manifestIdentity(input: Omit<RaptorManifestRoot, "id">): string { return sha256Hex(canonicalStringify({ domain: "raptor-manifest-root-v1", ...input })); }

export function buildManifest(input: BuildManifestInput): RaptorManifest {
  const ownerHost = input.ownerHost; if (ownerHost !== "pi" && ownerHost !== "prime") throw new TypeError("RAPTOR manifest owner is invalid");
  const raw = input.leafIds; if (!Array.isArray(raw) || raw.length > MAX_LEAVES) throw new TypeError("RAPTOR manifest membership is unbounded");
  const leafIds = raw.map((id) => boundedText("leaf ID", id)).sort();
  if (new Set(leafIds).size !== leafIds.length) throw new TypeError("RAPTOR manifest membership is ambiguous");
  const chunkSize = input.chunkSize; if (!Number.isSafeInteger(chunkSize) || chunkSize < 1 || chunkSize > MAX_CHUNK) throw new TypeError("RAPTOR manifest chunk size is invalid");
  const policyId = boundedText("policy ID", input.policyId); const policyHash = boundedText("policy hash", input.policyHash); const algorithm = boundedText("algorithm", input.algorithm); const promptRevision = boundedText("prompt revision", input.promptRevision); const modelId = boundedText("model ID", input.modelId); const seed = boundedText("seed", String(input.seed), 4096);
  const policyEpoch = input.policyEpoch; const privacyEpoch = input.privacyEpoch; if (!Number.isSafeInteger(policyEpoch) || policyEpoch < 0 || !Number.isSafeInteger(privacyEpoch) || privacyEpoch < 0) throw new TypeError("RAPTOR manifest epoch is invalid");
  const chunks: RaptorManifestChunk[] = [];
  for (let offset = 0; offset < leafIds.length; offset += chunkSize) {
    const memberIds = Object.freeze(leafIds.slice(offset, offset + chunkSize)); const index = chunks.length;
    const contentHash = sha256Hex(canonicalStringify({ domain: "raptor-manifest-chunk-v1", index, memberIds })); const id = contentHash;
    chunks.push(Object.freeze({ id, index, memberIds, contentHash }));
  }
  const membershipHash = sha256Hex(canonicalStringify({ domain: "raptor-membership-v1", leafIds })); const merkleRoot = merkle(chunks.map((chunk) => chunk.contentHash)); const chunkIds = Object.freeze(chunks.map((chunk) => chunk.id));
  const base = Object.freeze({ ownerHost, membershipHash, merkleRoot, chunkIds, leafCount: leafIds.length, chunkSize, policyId, policyHash, policyEpoch, privacyEpoch, algorithm, promptRevision, modelId, seed });
  const root: RaptorManifestRoot = Object.freeze({ id: manifestIdentity(base), ...base });
  return Object.freeze({ root, chunks: Object.freeze(chunks) });
}

export function verifyManifest(input: RaptorManifest): boolean {
  try {
    if (typeof input !== "object" || input === null || !Array.isArray(input.chunks)) return false;
    const root = input.root; if (root === undefined || !HEX.test(root.id) || !HEX.test(root.membershipHash) || !HEX.test(root.merkleRoot) || root.chunkIds.length !== input.chunks.length) return false;
    const leafIds: string[] = [];
    for (let index = 0; index < input.chunks.length; index += 1) {
      const chunk = input.chunks[index]!; if (chunk.index !== index || chunk.id !== chunk.contentHash || !HEX.test(chunk.contentHash) || chunk.memberIds.length < 1 || chunk.memberIds.length > root.chunkSize) return false;
      if (sha256Hex(canonicalStringify({ domain: "raptor-manifest-chunk-v1", index, memberIds: [...chunk.memberIds] })) !== chunk.contentHash || root.chunkIds[index] !== chunk.id) return false;
      leafIds.push(...chunk.memberIds);
    }
    if (leafIds.length !== root.leafCount || new Set(leafIds).size !== leafIds.length || leafIds.some((id, index) => index > 0 && leafIds[index - 1]! >= id)) return false;
    if (sha256Hex(canonicalStringify({ domain: "raptor-membership-v1", leafIds })) !== root.membershipHash || merkle(input.chunks.map((chunk) => chunk.contentHash)) !== root.merkleRoot) return false;
    const { id: _id, ...base } = root; return manifestIdentity(base) === root.id;
  } catch { return false; }
}
