import type { JobRecord } from "../domain/records.js";

/**
 * Central job-deadline semantics (cycle-free internal helper shared by the
 * lease and proposal authority paths). A nullable job deadline must already be
 * parser-valid; `jobExpired(job, now, skew)` is TRUE iff the deadline is
 * non-null and `Date.parse(job.expiresAt) <= now + skew` with safe, bounded,
 * overflow-fail-closed addition (a non-null invalid Date.parse fails closed).
 * Indefinite (null) deadlines remain live. Retention uses FUTURE skew; the
 * exact-owner lease expiry fix is unchanged.
 */
export function jobExpired(job: Pick<JobRecord, "expiresAt">, now: number, maxClockSkewMs: number): boolean {
  if (!Number.isSafeInteger(now) || (now as number) < 0 || !Number.isSafeInteger(maxClockSkewMs) || (maxClockSkewMs as number) < 0 || (maxClockSkewMs as number) > 3_600_000) throw new TypeError("Job deadline inputs are invalid");
  if (job.expiresAt === null) return false;
  const parsed = Date.parse(job.expiresAt);
  if (!Number.isFinite(parsed)) throw new TypeError("Job deadline is invalid");
  const cut = (now as number) + (maxClockSkewMs as number);
  if (!Number.isSafeInteger(cut)) throw new TypeError("Job deadline comparison overflowed");
  return parsed <= cut;
}
