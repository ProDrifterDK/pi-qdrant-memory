import { isAbsolute } from "node:path";
import type { AdminPoint } from "./qdrant-admin.js";

export type HermesRejectionReason =
  | "id"
  | "vector"
  | "text"
  | "model"
  | "project-path"
  | "source-type"
  | "created-at"
  | "tags"
  | "fact-status"
  | "stale"
  | "review-required"
  | "quarantined"
  | "raptor-excluded"
  | "raptor-forgotten";

export type HermesValidation =
  | { eligible: true; point: AdminPoint; model?: string }
  | { eligible: false; reason: HermesRejectionReason };

type JsonRecord = Record<string, unknown>;

function isRecord(value: unknown): value is JsonRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasOwn(value: JsonRecord, key: string): boolean {
  return Object.prototype.hasOwnProperty.call(value, key);
}

function validId(value: unknown): value is string | number {
  return (
    (typeof value === "string" && value.length > 0) ||
    (typeof value === "number" && Number.isSafeInteger(value) && value >= 0)
  );
}

function validOrdinaryDenseArray<T>(
  value: unknown,
  validElement: (element: unknown) => element is T,
): value is T[] {
  if (!Array.isArray(value) || Object.getPrototypeOf(value) !== Array.prototype) return false;
  if (Object.getOwnPropertySymbols(value).length > 0) return false;
  if (Object.getOwnPropertyNames(value).length !== value.length + 1) return false;
  for (let index = 0; index < value.length; index += 1) {
    const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
    if (
      descriptor === undefined ||
      !("value" in descriptor) ||
      !descriptor.enumerable ||
      !validElement(descriptor.value)
    ) return false;
  }
  return true;
}

function validDenseVector(value: unknown): value is number[] {
  return (
    validOrdinaryDenseArray(
      value,
      (component): component is number => typeof component === "number" && Number.isFinite(component),
    ) && value.length > 0
  );
}

function validTags(value: unknown): value is string[] {
  return validOrdinaryDenseArray(value, (tag): tag is string => typeof tag === "string");
}

function leapYear(year: number): boolean {
  return year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
}

function daysInMonth(year: number, month: number): number {
  const days = [31, leapYear(year) ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  return days[month - 1] ?? 0;
}

/** Strict RFC-3339-shaped ISO timestamp validation without Date normalization. */
function validIsoTimestamp(value: string): boolean {
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})$/.exec(value);
  if (match === null) return false;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const hour = Number(match[4]);
  const minute = Number(match[5]);
  const second = Number(match[6]);
  if (
    month < 1 ||
    month > 12 ||
    day < 1 ||
    day > daysInMonth(year, month) ||
    hour > 23 ||
    minute > 59 ||
    second > 59
  ) return false;
  const zone = match[8];
  if (zone === undefined) return false;
  if (zone !== "Z") {
    const zoneHour = Number(zone.slice(1, 3));
    const zoneMinute = Number(zone.slice(4, 6));
    if (zoneHour > 23 || zoneMinute > 59) return false;
  }
  return Number.isFinite(Date.parse(value));
}

function invalidSafetyFlag(
  payload: JsonRecord,
  field: string,
): boolean {
  if (!hasOwn(payload, field)) return false;
  return payload[field] !== false;
}

export function validateHermesPoint(point: AdminPoint): HermesValidation {
  const candidate = point as unknown as JsonRecord;
  if (!isRecord(candidate) || !validId(candidate.id)) return { eligible: false, reason: "id" };
  if (!validDenseVector(candidate.vector)) return { eligible: false, reason: "vector" };
  if (!isRecord(candidate.payload)) return { eligible: false, reason: "text" };

  const payload = candidate.payload;
  if (!hasOwn(payload, "text") || typeof payload.text !== "string" || payload.text.trim().length === 0) {
    return { eligible: false, reason: "text" };
  }

  let model: string | undefined;
  if (hasOwn(payload, "model")) {
    if (typeof payload.model !== "string") return { eligible: false, reason: "model" };
    if (payload.model.length > 0) model = payload.model;
  }

  if (hasOwn(payload, "project_path")) {
    if (typeof payload.project_path !== "string") return { eligible: false, reason: "project-path" };
    if (payload.project_path.length > 0 && !isAbsolute(payload.project_path)) {
      return { eligible: false, reason: "project-path" };
    }
  }

  if (hasOwn(payload, "source_type") && typeof payload.source_type !== "string") {
    return { eligible: false, reason: "source-type" };
  }

  if (hasOwn(payload, "created_at")) {
    if (typeof payload.created_at !== "string" || !validIsoTimestamp(payload.created_at)) {
      return { eligible: false, reason: "created-at" };
    }
  }

  if (hasOwn(payload, "tags")) {
    if (!validTags(payload.tags)) return { eligible: false, reason: "tags" };
  }

  if (hasOwn(payload, "fact_status")) {
    if (payload.fact_status !== "" && payload.fact_status !== "active") {
      return { eligible: false, reason: "fact-status" };
    }
  }

  if (invalidSafetyFlag(payload, "stale")) return { eligible: false, reason: "stale" };
  if (invalidSafetyFlag(payload, "requires_review")) return { eligible: false, reason: "review-required" };
  if (invalidSafetyFlag(payload, "consolidation_quarantined")) {
    return { eligible: false, reason: "quarantined" };
  }
  if (invalidSafetyFlag(payload, "raptor_excluded")) {
    return { eligible: false, reason: "raptor-excluded" };
  }
  if (invalidSafetyFlag(payload, "raptor_forgotten")) {
    return { eligible: false, reason: "raptor-forgotten" };
  }

  const validPoint = point as AdminPoint;
  return model === undefined
    ? { eligible: true, point: validPoint }
    : { eligible: true, point: validPoint, model };
}
