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
export declare function jobExpired(job: Pick<JobRecord, "expiresAt">, now: number, maxClockSkewMs: number): boolean;
