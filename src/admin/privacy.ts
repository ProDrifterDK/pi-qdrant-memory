import { canonicalStringify, sha256Hex } from "../domain/canonical.js";
import type { ControlRecord } from "../domain/records.js";
import type { HostId } from "../types.js";
import { AdminPlanError } from "./errors.js";

const MAX_DESTINATIONS = 256;
const SECRET = /(?:api[-_]?key|access[-_]?token|authorization|bearer|credential|password|secret|token)/iu;

export interface PrivacyRevokePlan {
  readonly id: string;
  readonly operation: "privacy_revoke";
  readonly ownerHost: HostId;
  readonly fromPrivacyEpoch: number;
  readonly toPrivacyEpoch: number;
  readonly destinationIds: readonly string[];
  readonly requestedAt: string;
  readonly reason: string;
  readonly drainsWorkers: true;
  readonly invalidatesGeneration: true;
  readonly inFlightCallsCannotBeRevoked: true;
}

export interface PrivacyRevokePlanInput {
  ownerHost: HostId;
  /** A control snapshot is preferred; scalar fields are a test/CLI seam. */
  control?: Pick<ControlRecord, "ownerHost" | "privacyEpoch" | "activeGeneration" | "state">;
  currentPrivacyEpoch?: number;
  destinationIds?: readonly string[];
  requestedAt?: string | Date;
  reason?: string;
}

export type PrivacyControlSnapshot = Pick<ControlRecord, "ownerHost" | "privacyEpoch" | "activeGeneration" | "state" | "revokedDestinationIds">;
export interface PrivacyRevokeDependencies {
  readControl?(): Promise<PrivacyControlSnapshot>;
  beginDrain?(input: { now: number }): Promise<unknown>;
  waitForQuiescence?(input: { retiredEpoch: number; signal?: AbortSignal }): Promise<unknown>;
  /** The only write seam. Implementations must CAS the collection control. */
  advancePrivacyEpoch?(input: { expectedEpoch: number; nextEpoch: number; invalidateGeneration: true; revokedDestinationIds: readonly string[]; reason: string; signal?: AbortSignal }): Promise<unknown>;
  /** Equivalent named seam for a genuine coordination store. */
  beginForgetBarrier?(input: { now: number; expectedEpoch: number; revokedDestinationIds?: readonly string[]; signal?: AbortSignal }): Promise<unknown>;
  recordRevocations?(input: { destinationIds: readonly string[]; privacyEpoch: number; reason: string; signal?: AbortSignal }): Promise<void>;
  rereadControl?(): Promise<PrivacyControlSnapshot>;
  invalidateGeneration?(): Promise<void>;
  reconcile?(): Promise<void>;
}

export interface PrivacyRevokeResult {
  readonly ok: true;
  readonly planId: string;
  readonly previousPrivacyEpoch: number;
  readonly privacyEpoch: number;
  readonly generationInvalidated: true;
  readonly logicalInvisible: true;
  readonly inFlightCallsCannotBeRevoked: true;
  readonly reconciled: boolean;
}

function validHost(value: unknown): asserts value is HostId {
  if (value !== "pi" && value !== "prime") throw new TypeError("Privacy owner host is invalid");
}
function validEpoch(value: unknown, name: string): asserts value is number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) throw new TypeError(`${name} is invalid`);
}
function safeText(value: unknown, name: string, max: number): string {
  if (typeof value !== "string" || value.length === 0 || value.length > max || /[\u0000-\u001f\u007f-\u009f]/u.test(value) || SECRET.test(value)) throw new TypeError(`${name} is invalid or not redacted`);
  return value;
}
function instant(value: string | Date | undefined): string {
  const parsed = value === undefined ? new Date() : value instanceof Date ? new Date(value.getTime()) : new Date(value);
  if (!Number.isFinite(parsed.getTime())) throw new TypeError("Privacy request time is invalid");
  return parsed.toISOString();
}
function normalizedDestinations(value: readonly string[] | undefined): string[] {
  const entries = value === undefined ? [] : [...value];
  if (entries.length > MAX_DESTINATIONS) throw new TypeError("Privacy destinations are unbounded");
  const result = entries.map((entry, index) => safeText(entry, `destinationIds[${index}]`, 256));
  result.sort();
  for (let index = 1; index < result.length; index += 1) if (result[index] === result[index - 1]) throw new TypeError("Privacy destinations must be unique");
  return result;
}
function controlValues(input: PrivacyRevokePlanInput): { ownerHost: HostId; epoch: number } {
  validHost(input.ownerHost);
  const ownerHost = input.control?.ownerHost ?? input.ownerHost;
  validHost(ownerHost);
  if (ownerHost !== input.ownerHost) throw new TypeError("Privacy control owner mismatch");
  const epoch = input.control?.privacyEpoch ?? input.currentPrivacyEpoch ?? 0;
  validEpoch(epoch, "currentPrivacyEpoch");
  return { ownerHost, epoch };
}

/** Create a deterministic, redacted collection privacy-revocation plan. */
export function planPrivacyRevoke(input: PrivacyRevokePlanInput): PrivacyRevokePlan {
  const { ownerHost, epoch } = controlValues(input);
  const requestedAt = instant(input.requestedAt);
  const reason = safeText(input.reason ?? "operator privacy revocation", "reason", 512);
  const destinationIds = normalizedDestinations(input.destinationIds);
  const body = { destinationIds, fromPrivacyEpoch: epoch, invalidatesGeneration: true, inFlightCallsCannotBeRevoked: true, operation: "privacy_revoke", ownerHost, reason, requestedAt, toPrivacyEpoch: epoch + 1 } as const;
  // The approval token is content-addressed, not timestamp-addressed. The
  // human may print a plan in one process and approve it in another; the
  // persisted control epoch/destination set still makes stale plans fail.
  const { requestedAt: _requestedAt, ...stableBody } = body;
  const id = sha256Hex(canonicalStringify(stableBody));
  return Object.freeze({ ...body, id, drainsWorkers: true });
}
export const createPrivacyRevokePlan = planPrivacyRevoke;
export const planRevoke = planPrivacyRevoke;

function planMatches(plan: PrivacyRevokePlan): void {
  const { id, drainsWorkers: _drainsWorkers, requestedAt: _requestedAt, ...body } = plan;
  if (id !== sha256Hex(canonicalStringify(body)) || plan.operation !== "privacy_revoke" || plan.drainsWorkers !== true || plan.invalidatesGeneration !== true || plan.inFlightCallsCannotBeRevoked !== true) throw new AdminPlanError("Privacy revoke plan is invalid");
  validHost(plan.ownerHost); validEpoch(plan.fromPrivacyEpoch, "plan.fromPrivacyEpoch"); validEpoch(plan.toPrivacyEpoch, "plan.toPrivacyEpoch");
  if (plan.toPrivacyEpoch !== plan.fromPrivacyEpoch + 1) throw new AdminPlanError("Privacy revoke plan epoch is invalid");
  normalizedDestinations(plan.destinationIds);
  safeText(plan.reason, "plan.reason", 512);
  instant(plan.requestedAt);
}

export interface RevokePrivacyInput {
  plan: PrivacyRevokePlan;
  approvedPlanId: string;
  signal?: AbortSignal;
  now?: number;
}

/** Apply one exact plan. Missing write capabilities fail closed rather than
 * claiming a privacy barrier that was never CASed. */
export async function revokePrivacy(input: RevokePrivacyInput, deps: PrivacyRevokeDependencies = {}): Promise<PrivacyRevokeResult> {
  planMatches(input.plan);
  if (input.approvedPlanId !== input.plan.id) throw new AdminPlanError("Privacy revoke approval does not match the plan");
  const now = input.now ?? Date.now();
  if (!Number.isFinite(now)) throw new TypeError("Privacy revoke clock is invalid");
  const before = deps.readControl === undefined ? undefined : await deps.readControl();
  if (before !== undefined && (before.ownerHost !== input.plan.ownerHost || before.privacyEpoch !== input.plan.fromPrivacyEpoch)) throw new AdminPlanError("Privacy revoke plan is stale");
  let retiredEpoch = input.plan.fromPrivacyEpoch;
  if (deps.beginDrain !== undefined) {
    const drained = await deps.beginDrain({ now });
    if (typeof drained === "object" && drained !== null && "coordinationPolicyEpoch" in drained && typeof (drained as { coordinationPolicyEpoch?: unknown }).coordinationPolicyEpoch === "number") retiredEpoch = (drained as { coordinationPolicyEpoch: number }).coordinationPolicyEpoch;
  }
  if (deps.waitForQuiescence !== undefined) await deps.waitForQuiescence({ retiredEpoch, ...(input.signal === undefined ? {} : { signal: input.signal }) });
  if (deps.beginForgetBarrier !== undefined) {
    await deps.beginForgetBarrier({ now, expectedEpoch: input.plan.fromPrivacyEpoch, revokedDestinationIds: input.plan.destinationIds, ...(input.signal === undefined ? {} : { signal: input.signal }) });
  } else if (deps.advancePrivacyEpoch !== undefined) {
    await deps.advancePrivacyEpoch({ expectedEpoch: input.plan.fromPrivacyEpoch, nextEpoch: input.plan.toPrivacyEpoch, invalidateGeneration: true, revokedDestinationIds: input.plan.destinationIds, reason: input.plan.reason, ...(input.signal === undefined ? {} : { signal: input.signal }) });
  } else {
    throw new Error("Privacy revoke requires a durable control CAS capability");
  }
  if (deps.recordRevocations !== undefined) await deps.recordRevocations({ destinationIds: input.plan.destinationIds, privacyEpoch: input.plan.toPrivacyEpoch, reason: input.plan.reason, ...(input.signal === undefined ? {} : { signal: input.signal }) });
  if (deps.invalidateGeneration !== undefined) await deps.invalidateGeneration();
  if (deps.reconcile !== undefined) await deps.reconcile();
  const reread = deps.rereadControl ?? deps.readControl;
  if (reread === undefined) throw new Error("Privacy revoke requires a durable post-write reread");
  const after = await reread();
  if (after.ownerHost !== input.plan.ownerHost || after.privacyEpoch !== input.plan.toPrivacyEpoch || after.activeGeneration !== null || input.plan.destinationIds.some((id) => !after.revokedDestinationIds.includes(id))) throw new Error("Privacy revoke barrier was not confirmed");
  return { ok: true, planId: input.plan.id, previousPrivacyEpoch: input.plan.fromPrivacyEpoch, privacyEpoch: input.plan.toPrivacyEpoch, generationInvalidated: true, logicalInvisible: true, inFlightCallsCannotBeRevoked: true, reconciled: deps.reconcile !== undefined };
}
export const applyPrivacyRevoke = revokePrivacy;
export const privacyRevoke = revokePrivacy;
