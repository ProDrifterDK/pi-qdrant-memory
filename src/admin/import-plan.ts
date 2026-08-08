import type { HostId } from "../types.js";
import { projectIdentityFromStoredPath } from "../project.js";
import { canonicalStringify, deterministicPointId, sha256Hex } from "./canonical.js";
import { validateHermesPoint } from "./hermes-contract.js";
import type { AdminPoint } from "./qdrant-admin.js";
import { containsSecret } from "./secret-scan.js";

export const MAX_TAG_COUNT = 32;
export const MAX_TAG_LENGTH = 256;
export const MAX_TAG_TOTAL_LENGTH = 4096;
export const MAX_SOURCE_TYPE_LENGTH = 128;
export const MAX_PROJECT_LABEL_LENGTH = 255;
export const MAX_SOURCE_POINT_ID_LENGTH = 256;
const MAX_COLLECTION_LENGTH = 255;
const MAX_MODEL_LENGTH = 256;
const TRANSFORM_VERSION = 1 as const;
const GLOBAL_PROJECT_LABEL = "global";

const RELEVANT_PAYLOAD_FIELDS = [
  "text",
  "model",
  "project_path",
  "source_type",
  "created_at",
  "tags",
  "fact_status",
  "stale",
  "requires_review",
  "consolidation_quarantined",
  "raptor_excluded",
  "raptor_forgotten",
] as const;

export interface ImportPlan {
  planId: string;
  transformVersion: 1;
  targetHost: HostId;
  sourceCollection: string;
  destinationCollection: string;
  accepted: AdminPoint[];
  rejected: Record<string, number>;
  report: {
    accepted: number;
    rejected: number;
    bySourceType: Record<string, number>;
    byProjectLabel: Record<string, number>;
  };
}

export type NormalizedHermesPoint =
  | {
      accepted: true;
      point: AdminPoint;
      projectLabel?: string;
      sourceType: string;
      model?: string;
    }
  | { accepted: false; reason: string };

interface NormalizeInput {
  point: AdminPoint;
  targetHost: HostId;
  sourceCollection: string;
  configuredModel: string;
}

interface BuildImportPlanInput {
  points: readonly AdminPoint[];
  targetHost: HostId;
  sourceIdentity: string;
  sourceCollection: string;
  sourceDimension: number;
  sourceDistance: string;
  destinationCollection: string;
  destinationDimension: number;
  destinationDistance: string;
  configuredModel: string;
  declaredSourceModel?: string;
}

interface Selection {
  sourceId: string | number;
  sourceVector: number[];
  relevantPayload: Record<string, unknown>;
  normalized: AdminPoint;
  projectLabel?: string;
  sourceType: string;
  model?: string;
}

function hasOwn(value: Record<string, unknown>, key: string): boolean {
  return Object.prototype.hasOwnProperty.call(value, key);
}

function invalidPlan(message: string): never {
  throw new Error(`Cannot build Hermes import plan: ${message}`);
}

function validHost(host: HostId): boolean {
  return host === "prime" || host === "pi";
}

function validateBoundedConfigString(
  value: string,
  name: string,
  maxLength: number,
  scan = false,
): void {
  if (typeof value !== "string" || value.trim().length === 0 || value.length > maxLength) {
    invalidPlan(`${name} is invalid`);
  }
  if (scan && containsSecret(value)) invalidPlan(`${name} is invalid`);
}

function validateNormalizeContract(input: NormalizeInput): void {
  if (!validHost(input.targetHost)) invalidPlan("target host is invalid");
  validateBoundedConfigString(input.sourceCollection, "source collection", MAX_COLLECTION_LENGTH, true);
  validateBoundedConfigString(input.configuredModel, "configured model", MAX_MODEL_LENGTH);
}

function tagsWithinBounds(tags: readonly string[]): boolean {
  if (tags.length > MAX_TAG_COUNT) return false;
  let totalLength = 0;
  for (const tag of tags) {
    if (tag.length > MAX_TAG_LENGTH) return false;
    totalLength += tag.length;
    if (totalLength > MAX_TAG_TOTAL_LENGTH) return false;
  }
  return true;
}

function relevantPayload(payload: Record<string, unknown>): Record<string, unknown> {
  const output: Record<string, unknown> = {};
  for (const field of RELEVANT_PAYLOAD_FIELDS) {
    if (!hasOwn(payload, field)) continue;
    const value = payload[field];
    output[field] = Array.isArray(value) ? [...value] : value;
  }
  return output;
}

function scannedValues(input: {
  text: string;
  targetHost: HostId;
  sourceCollection: string;
  sourceId: string | number;
  sourceType: string;
  projectId?: string;
  projectLabel?: string;
  createdAt?: string;
  tags: readonly string[];
}): string[] {
  const values = [
    input.text,
    input.targetHost,
    "hermes",
    input.sourceCollection,
    String(input.sourceId),
    input.sourceType,
    "active",
    "passed",
    ...input.tags,
  ];
  if (input.projectId !== undefined) values.push(input.projectId);
  if (input.projectLabel !== undefined) values.push(input.projectLabel);
  if (input.createdAt !== undefined) values.push(input.createdAt);
  return values;
}

export function normalizeHermesPoint(input: NormalizeInput): NormalizedHermesPoint {
  validateNormalizeContract(input);
  const validation = validateHermesPoint(input.point);
  if (!validation.eligible) return { accepted: false, reason: validation.reason };

  const payload = validation.point.payload;
  const text = payload.text as string;
  const rawSourceType = hasOwn(payload, "source_type") ? payload.source_type as string : "";
  const sourceType = rawSourceType.trim().length === 0 ? "unknown" : rawSourceType;
  const tags = hasOwn(payload, "tags") ? [...payload.tags as string[]] : [];
  const createdAt = hasOwn(payload, "created_at") ? payload.created_at as string : undefined;

  let projectId: string | undefined;
  let projectLabel: string | undefined;
  if (hasOwn(payload, "project_path") && (payload.project_path as string).length > 0) {
    const identity = projectIdentityFromStoredPath(payload.project_path as string);
    projectId = identity.id;
    projectLabel = identity.label;
  }

  if (scannedValues({
    text,
    targetHost: input.targetHost,
    sourceCollection: input.sourceCollection,
    sourceId: validation.point.id,
    sourceType,
    ...(projectId === undefined ? {} : { projectId }),
    ...(projectLabel === undefined ? {} : { projectLabel }),
    ...(createdAt === undefined ? {} : { createdAt }),
    tags,
  }).some((value) => containsSecret(value))) {
    return { accepted: false, reason: "secret" };
  }

  if (!tagsWithinBounds(tags)) return { accepted: false, reason: "tag-bounds" };
  if (sourceType.length > MAX_SOURCE_TYPE_LENGTH) {
    return { accepted: false, reason: "source-type-bounds" };
  }
  if (projectLabel !== undefined && (projectLabel.length === 0 || projectLabel.length > MAX_PROJECT_LABEL_LENGTH)) {
    return { accepted: false, reason: "project-label-bounds" };
  }
  if (typeof validation.point.id === "string" && validation.point.id.length > MAX_SOURCE_POINT_ID_LENGTH) {
    return { accepted: false, reason: "source-point-id-bounds" };
  }
  if (validation.model !== undefined && validation.model !== input.configuredModel) {
    invalidPlan("source model does not match the configured model");
  }

  const destinationPayload: Record<string, unknown> = {
    text,
    host: input.targetHost,
    source_type: sourceType,
    source_system: "hermes",
    source_collection: input.sourceCollection,
    source_point_id: validation.point.id,
    status: "active",
    secret_scan: "passed",
  };
  if (projectId !== undefined) destinationPayload.project_id = projectId;
  if (projectLabel !== undefined) destinationPayload.project_label = projectLabel;
  if (createdAt !== undefined) destinationPayload.created_at = createdAt;
  if (hasOwn(payload, "tags")) destinationPayload.tags = tags;

  const normalizedPoint: AdminPoint = {
    id: deterministicPointId(input.targetHost, input.sourceCollection, validation.point.id),
    vector: [...validation.point.vector],
    payload: destinationPayload,
  };
  const base = {
    accepted: true as const,
    point: normalizedPoint,
    sourceType,
  };
  return {
    ...base,
    ...(projectLabel === undefined ? {} : { projectLabel }),
    ...(validation.model === undefined ? {} : { model: validation.model }),
  };
}

function validateBuildInput(input: BuildImportPlanInput): void {
  if (!Array.isArray(input.points)) invalidPlan("points are invalid");
  if (!validHost(input.targetHost)) invalidPlan("target host is invalid");
  validateBoundedConfigString(input.sourceIdentity, "source identity", 4096);
  validateBoundedConfigString(input.sourceCollection, "source collection", MAX_COLLECTION_LENGTH, true);
  validateBoundedConfigString(input.destinationCollection, "destination collection", MAX_COLLECTION_LENGTH, true);
  validateBoundedConfigString(input.sourceDistance, "source distance", 64);
  validateBoundedConfigString(input.destinationDistance, "destination distance", 64);
  validateBoundedConfigString(input.configuredModel, "configured model", MAX_MODEL_LENGTH);
  if (input.declaredSourceModel !== undefined) {
    validateBoundedConfigString(input.declaredSourceModel, "declared source model", MAX_MODEL_LENGTH);
  }
  if (!Number.isSafeInteger(input.sourceDimension) || input.sourceDimension <= 0) {
    invalidPlan("source dimension is invalid");
  }
  if (!Number.isSafeInteger(input.destinationDimension) || input.destinationDimension <= 0) {
    invalidPlan("destination dimension is invalid");
  }
  if (input.sourceDimension !== input.destinationDimension) invalidPlan("dimensions are incompatible");
  if (input.sourceDistance.toLowerCase() !== input.destinationDistance.toLowerCase()) {
    invalidPlan("distances are incompatible");
  }
}

function increment(counts: Map<string, number>, key: string): void {
  counts.set(key, (counts.get(key) ?? 0) + 1);
}

function compareStrings(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

function sortedRecord(counts: Map<string, number>): Record<string, number> {
  return Object.fromEntries([...counts.entries()].sort(([left], [right]) => compareStrings(left, right)));
}

function selectionKey(selection: Selection): string {
  return canonicalStringify(selection.sourceId);
}

function clonePoint(point: AdminPoint): AdminPoint {
  const payload: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(point.payload)) {
    payload[key] = Array.isArray(value) ? [...value] : value;
  }
  return { id: point.id, vector: [...point.vector], payload };
}

export function buildImportPlan(input: BuildImportPlanInput): ImportPlan {
  validateBuildInput(input);
  const rejectedCounts = new Map<string, number>();
  const selections: Selection[] = [];

  for (const sourcePoint of input.points) {
    const validation = validateHermesPoint(sourcePoint);
    if (validation.eligible && validation.point.vector.length !== input.sourceDimension) {
      invalidPlan("point vector dimension is incompatible");
    }

    const normalized = normalizeHermesPoint({
      point: sourcePoint,
      targetHost: input.targetHost,
      sourceCollection: input.sourceCollection,
      configuredModel: input.configuredModel,
    });
    if (!normalized.accepted) {
      increment(rejectedCounts, normalized.reason);
      continue;
    }

    selections.push({
      sourceId: sourcePoint.id,
      sourceVector: [...sourcePoint.vector],
      relevantPayload: relevantPayload(sourcePoint.payload),
      normalized: clonePoint(normalized.point),
      sourceType: normalized.sourceType,
      ...(normalized.projectLabel === undefined ? {} : { projectLabel: normalized.projectLabel }),
      ...(normalized.model === undefined ? {} : { model: normalized.model }),
    });
  }

  selections.sort((left, right) => compareStrings(selectionKey(left), selectionKey(right)));
  for (let index = 1; index < selections.length; index += 1) {
    const previous = selections[index - 1];
    const current = selections[index];
    if (previous !== undefined && current !== undefined && selectionKey(previous) === selectionKey(current)) {
      invalidPlan("duplicate selected source point id");
    }
  }

  const selectedModels = new Set(
    selections
      .map((selection) => selection.model)
      .filter((model): model is string => model !== undefined),
  );
  for (const model of selectedModels) {
    if (model !== input.configuredModel) invalidPlan("source model does not match the configured model");
  }
  if (selectedModels.size === 0) {
    if (input.declaredSourceModel === undefined) invalidPlan("declared source model is required");
    if (input.declaredSourceModel !== input.configuredModel) {
      invalidPlan("declared source model does not match the configured model");
    }
  }

  const manifest = {
    transformVersion: TRANSFORM_VERSION,
    targetHost: input.targetHost,
    source: {
      identity: input.sourceIdentity,
      collection: input.sourceCollection,
      dimension: input.sourceDimension,
      distance: input.sourceDistance,
    },
    destination: {
      collection: input.destinationCollection,
      dimension: input.destinationDimension,
      distance: input.destinationDistance,
    },
    modelInputs: {
      configured: input.configuredModel,
      declared: input.declaredSourceModel ?? null,
    },
    selections: selections.map((selection) => ({
      sourceId: selection.sourceId,
      vector: [...selection.sourceVector],
      relevantPayload: selection.relevantPayload,
      destination: clonePoint(selection.normalized),
    })),
  };
  const planId = sha256Hex(canonicalStringify(manifest));

  const accepted = selections.map((selection) => {
    const point = clonePoint(selection.normalized);
    point.payload.import_run_id = planId;
    return point;
  });
  const sourceTypeCounts = new Map<string, number>();
  const projectLabelCounts = new Map<string, number>();
  for (const selection of selections) {
    increment(sourceTypeCounts, selection.sourceType);
    increment(projectLabelCounts, selection.projectLabel ?? GLOBAL_PROJECT_LABEL);
  }

  const rejected = sortedRecord(rejectedCounts);
  return {
    planId,
    transformVersion: TRANSFORM_VERSION,
    targetHost: input.targetHost,
    sourceCollection: input.sourceCollection,
    destinationCollection: input.destinationCollection,
    accepted,
    rejected,
    report: {
      accepted: accepted.length,
      rejected: [...rejectedCounts.values()].reduce((total, count) => total + count, 0),
      bySourceType: sortedRecord(sourceTypeCounts),
      byProjectLabel: sortedRecord(projectLabelCounts),
    },
  };
}
