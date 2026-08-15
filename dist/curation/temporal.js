import { canonicalStringify, sha256Hex } from "../domain/canonical.js";
import { contentId, curatedCurrentId, evidenceLinkId, observationId, stateKey, validateEffectiveOrder, MAX_SESSION_SEQUENCE } from "../domain/ids.js";
import { canonicalRecordHash } from "../domain/records.js";
import { isPolicyExpired, processingPolicyHash } from "../domain/policy.js";
import { ProductionCoordinationStore, LeaseAuthority } from "../qdrant/write.js";
import { BoundEmbeddingDestination } from "../clients/embeddings.js";
import { gateCuratedEgressText } from "../security/egress.js";
import { readActiveAcceptance, readJob } from "../coordination/jobs.js";
import { jobExpired } from "../coordination/deadline.js";
import { conflictManifestId } from "../domain/ids.js";
import { assertPersistableCurationResult, validateCurationResult } from "./validate.js";
import { projectCurationItem, projectConflictAggregate, projectEffectiveOrder, projectCurationText, compareProjectionOrders, effectiveOrderTuple } from "./projection.js";
import { parseCurationProposalEnvelope, provenanceMatches } from "./provenance.js";
import { CURATION_PROMPT_REVISION } from "./prompt.js";
import { types as nodeTypes } from "node:util";
const MAX_CONFLICT_MEMBERS = 1024;
export const compareEffectiveOrders = compareProjectionOrders;
export const deriveEffectiveOrder = projectEffectiveOrder;
export function derivedCuratedText(item) {
    return projectCurationText(item);
}
function ownData(value, key, required = true) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (descriptor === undefined) {
        if (required)
            throw new TypeError(`Curation input ${key} is missing`);
        return undefined;
    }
    if (!("value" in descriptor) || descriptor.enumerable !== true)
        throw new TypeError(`Curation input ${key} must be an own data field`);
    return descriptor.value;
}
function ownedJsonSnapshot(value) {
    const active = new Set();
    const clone = (candidate) => {
        if (candidate === null || typeof candidate === "string" || typeof candidate === "boolean")
            return candidate;
        if (typeof candidate === "number") {
            if (!Number.isFinite(candidate))
                throw new TypeError("non-finite");
            return candidate;
        }
        if (typeof candidate !== "object" || nodeTypes.isProxy(candidate) || active.has(candidate))
            throw new TypeError("non-canonical");
        active.add(candidate);
        try {
            if (Object.getOwnPropertySymbols(candidate).length > 0)
                throw new TypeError("symbol");
            if (Array.isArray(candidate)) {
                if (Object.getPrototypeOf(candidate) !== Array.prototype)
                    throw new TypeError("array");
                const lengthDescriptor = Object.getOwnPropertyDescriptor(candidate, "length");
                if (lengthDescriptor === undefined || !("value" in lengthDescriptor) || !Number.isSafeInteger(lengthDescriptor.value) || lengthDescriptor.value < 0 || lengthDescriptor.value > 4096 || Object.getOwnPropertyNames(candidate).length !== lengthDescriptor.value + 1)
                    throw new TypeError("array");
                const result = [];
                for (let index = 0; index < lengthDescriptor.value; index += 1) {
                    const descriptor = Object.getOwnPropertyDescriptor(candidate, String(index));
                    if (descriptor === undefined || !("value" in descriptor) || descriptor.enumerable !== true)
                        throw new TypeError("array");
                    result.push(clone(descriptor.value));
                }
                return result;
            }
            const prototype = Object.getPrototypeOf(candidate);
            if (prototype !== Object.prototype && prototype !== null)
                throw new TypeError("object");
            const result = {};
            for (const name of Object.getOwnPropertyNames(candidate)) {
                const descriptor = Object.getOwnPropertyDescriptor(candidate, name);
                if (descriptor === undefined || !("value" in descriptor) || descriptor.enumerable !== true)
                    throw new TypeError("object");
                Object.defineProperty(result, name, { value: clone(descriptor.value), enumerable: true, writable: true, configurable: true });
            }
            return result;
        }
        finally {
            active.delete(candidate);
        }
    };
    try {
        return JSON.parse(canonicalStringify(clone(value)));
    }
    catch {
        throw new TypeError("Curation input is not canonical JSON");
    }
}
function requireId(name, value) {
    if (typeof value !== "string" || value.length === 0 || value.length > 512 || /(?:api[-_]?key|access[-_]?token|authorization|bearer|credential|password|secret|token)/iu.test(value))
        throw new TypeError(`${name} must be a bounded redacted id`);
    return value;
}
/**
 * Fresh liveness barrier immediately before every visible derived write.  The
 * accepted authority, active control epochs/revocations, membership tombstone
 * set, and current accepted proposal are all reread; a delayed embedding or a
 * lease/policy change therefore leaves only retryable immutable work.
 */
async function readTombstonesChunked(store, ids) {
    const found = [];
    for (let index = 0; index < ids.length; index += 1024)
        found.push(...await store.readTombstones(ids.slice(index, index + 1024)));
    return found;
}
async function assertMaterializationLive(store, authority, embeddingDestination, membership, derivedTargets, expectedJob, expectedProposal) {
    const membershipSet = new Set(membership);
    const derived = [...new Set(derivedTargets)].filter((target) => !membershipSet.has(target)).sort();
    if (derived.some((target) => typeof target !== "string" || target.length === 0 || target.length > 512) || derived.length > 4096)
        throw new TypeError("Derived tombstone target set is unbounded");
    const acceptance = await readActiveAcceptance(store, authority);
    if (acceptance === null || canonicalStringify(acceptance.job) !== canonicalStringify(expectedJob) || canonicalStringify(acceptance.proposal) !== canonicalStringify(expectedProposal))
        throw new TypeError("Materialization acceptance is stale after the control/lease/proposal barrier");
    const control = await store.readControl();
    if (control.state !== "active" || control.coordinationPolicyEpoch !== authority.coordinationPolicyEpoch || control.coordinationPolicyHash !== authority.coordinationPolicyHash || control.privacyEpoch !== authority.privacyEpoch)
        throw new TypeError("Materialization control identity changed");
    if (control.revokedDestinationIds.includes(embeddingDestination.id))
        throw new TypeError("Materialization embedding destination is revoked");
    const tombstones = await store.readTombstones(membership);
    const derivedTombstones = derived.length > 0 ? await readTombstonesChunked(store, derived) : [];
    if (tombstones.length > 0 || derivedTombstones.length > 0)
        throw new TypeError("Materialization membership is tombstoned");
    // Finish the sandwich after the slow tombstone read. Re-read the exact job
    // and tombstone set again before final control/claim/clock so a mutation in
    // either penultimate lane cannot pass the barrier.
    const finalJob = await readJob(store, authority.jobId);
    const finalTombstones = await store.readTombstones(membership);
    const finalDerivedTombstones = derived.length > 0 ? await readTombstonesChunked(store, derived) : [];
    const finalControl = await store.readControl();
    const finalClaim = await store.readLease(authority.jobId);
    const finalNow = authority.now();
    if (finalTombstones.length > 0 || finalDerivedTombstones.length > 0 || finalJob === null || jobExpired(finalJob, finalNow, authority.maxClockSkewMs) || finalClaim === null || !authority.matchesClaim(finalClaim) || finalClaim.state !== "accepted" || Date.parse(finalClaim.expiresAt) <= finalNow || finalControl.state !== "active" || finalControl.coordinationPolicyEpoch !== authority.coordinationPolicyEpoch || finalControl.coordinationPolicyHash !== authority.coordinationPolicyHash || finalControl.privacyEpoch !== authority.privacyEpoch || finalControl.revokedDestinationIds.includes(embeddingDestination.id))
        throw new TypeError("Materialization final liveness barrier failed");
}
function decideCurrent(current, observation, maxClockSkewMs) {
    if (current === null)
        return "insert";
    const comparison = compareEffectiveOrders(current.effectiveOrder, observation.effectiveOrder, maxClockSkewMs);
    // A current envelope may migrate across processing-policy intersections only
    // for a strictly later causal observation. Equal/within-skew or late arrivals
    // cannot create a hybrid current/manifest and remain retryable.
    if (current.processingPolicyId !== observation.processingPolicyId || current.expiresAt !== observation.expiresAt) {
        if (comparison === "before") { /* strict later observation may migrate the envelope */ }
        else if (comparison === "after")
            return "late";
        else
            throw new TypeError("Cross-policy current transition is not strictly causal");
    }
    if (current.resolution === "resolved") {
        // Same logical content converges only when the persisted current is not
        // causally behind this arrival; a strictly later arrival may refresh the
        // representative current without creating a conflict.
        if (current.contentId === observation.contentId) {
            if (comparison === "before")
                return "update";
            return "converge";
        }
        if (comparison === "within_skew")
            return "conflict";
        if (comparison === "equal")
            return "converge";
        if (comparison === "before")
            return "update";
        return "late";
    }
    // Conflict current: a later-dated observation resolves the conflicted view;
    // an equal-content observation converges; within-skew grows the manifest.
    if (comparison === "within_skew")
        return "conflict_grow";
    if (comparison === "equal")
        return "converge";
    if (comparison === "before")
        return "update";
    return "late";
}
/**
 * STRICT immutable causal order for history folding: same-session sequences
 * first (ascending), then cross-session tuples by (event_at, episode_id,
 * content_id). The clock-skew window is a CURRENT-decision concept only —
 * immutable observations never reorder, so folding never consults it.
 */
function strictHistoryOrder(left, right) {
    const comparison = compareEffectiveOrders(left.effectiveOrder, right.effectiveOrder, 0);
    if (comparison === "before")
        return -1;
    if (comparison === "after")
        return 1;
    // `equal`/`within_skew` are deterministic ties for immutable history. Use
    // the same fallback event/episode/content tuple as OCC, never arrival order.
    const tuple = (value) => {
        return effectiveOrderTuple(value);
    };
    const a = tuple(left.effectiveOrder);
    const b = tuple(right.effectiveOrder);
    if (a !== null && b !== null) {
        for (let index = 0; index < 3; index += 1)
            if (a[index] !== b[index])
                return a[index] < b[index] ? -1 : 1;
    }
    return left.id < right.id ? -1 : left.id > right.id ? 1 : 0;
}
/**
 * A->B->A history folding over immutable observations: observations are
 * ordered by strict causal order and CONSECUTIVE equal canonical content folds
 * into ONE interval, so A->B->A preserves TWO A intervals. Every observation
 * belongs to exactly one segment (no superseded state reuse or cycle) and the
 * derived valid_from/valid_to come from observation eventAt without mutating
 * them. Semantic near-duplicates are best-effort only and never folded here.
 */
export function foldHistorySegments(observations, maxClockSkewMs) {
    if (!Number.isSafeInteger(maxClockSkewMs) || maxClockSkewMs < 0)
        throw new TypeError("Clock skew must be a non-negative integer");
    if (!Array.isArray(observations))
        throw new TypeError("Observations must be a bounded list");
    const ordered = [...observations].sort(strictHistoryOrder);
    const segments = [];
    for (const observation of ordered) {
        const last = segments[segments.length - 1];
        if (last !== undefined && last.contentId === observation.contentId && last.stateKey === observation.stateKey) {
            const observationIds = [...last.observationIds, observation.id];
            const validFrom = observation.eventAt < last.validFrom ? observation.eventAt : last.validFrom;
            const validTo = null;
            // The observations are strict-history ordered, so the latest equal
            // content is the canonical primary evidence for the folded segment.
            const primaryEvidenceEpisodeId = observation.primaryEvidenceEpisodeId;
            segments[segments.length - 1] = Object.freeze({ ...last, observationIds: Object.freeze(observationIds), primaryEvidenceEpisodeId, validFrom, validTo });
            continue;
        }
        segments.push(Object.freeze({
            stateKey: observation.stateKey,
            contentId: observation.contentId,
            observationIds: Object.freeze([observation.id]),
            primaryEvidenceEpisodeId: observation.primaryEvidenceEpisodeId,
            validFrom: observation.eventAt,
            validTo: null,
            ...(observation.category === undefined ? {} : { category: observation.category }),
            ...(observation.scope === undefined ? {} : { scope: observation.scope }),
            ...(observation.subject === undefined ? {} : { subject: observation.subject }),
            ...(observation.predicate === undefined ? {} : { predicate: observation.predicate }),
            ...(observation.value === undefined ? {} : { value: observation.value }),
            ...(observation.text === undefined ? {} : { text: observation.text }),
        }));
    }
    // Intervals close at the next changed segment; the final segment is
    // explicitly open (`validTo: null`). Immutable observations are untouched.
    const closed = segments.map((segment, index) => Object.freeze({ ...segment, validTo: index + 1 < segments.length ? segments[index + 1].validFrom : null }));
    return Object.freeze(closed);
}
export async function materializeCuration(authority, input) {
    // Capability first: a forged authority must fail before any caller-owned
    // result/policy/destination/getter or store/network operation is touched.
    if (!LeaseAuthority.isValid(authority))
        throw new TypeError("Materialization requires a genuine lease authority");
    if (input === null || typeof input !== "object" || Array.isArray(input) || nodeTypes.isProxy(input) || (Object.getPrototypeOf(input) !== Object.prototype && Object.getPrototypeOf(input) !== null))
        throw new TypeError("Materialization input is invalid");
    const store = ownData(input, "store");
    if (!ProductionCoordinationStore.isValid(store))
        throw new TypeError("Materialization requires the branded production store");
    if (!authority.matchesStore(store))
        throw new TypeError("Materialization authority does not match the store");
    if (authority.state !== "accepted")
        throw new TypeError("Materialization requires an ACCEPTED lease authority");
    const embedding = ownData(input, "embedding");
    if (!BoundEmbeddingDestination.isValid(embedding))
        throw new TypeError("Materialization requires the opaque bound embedding destination");
    // Only after all nominal capabilities are validated are caller-owned values
    // snapshotted; no original object is retained across awaits.
    const resultInput = ownedJsonSnapshot(ownData(input, "result"));
    const policy = ownedJsonSnapshot(ownData(input, "policy"));
    const extractorRevision = ownData(input, "extractorRevision");
    const scan = ownData(input, "scan", false);
    if (typeof extractorRevision !== "string" || extractorRevision.length === 0 || extractorRevision.length > 512)
        throw new TypeError("Extractor revision is invalid");
    const host = store.ownerHost;
    const maxClockSkewMs = authority.maxClockSkewMs;
    const now = authority.now();
    // Policy intersection validation (content-addressed + live + host + exact
    // embedding capability + exact coordination identity).
    const policyHash = processingPolicyHash(policy);
    if (policyHash !== policy.id || isPolicyExpired(policy, now, maxClockSkewMs) || policy.ownerHost !== host)
        throw new TypeError("Active policy intersection is invalid");
    const rawEmbeddingDestination = embedding.destination;
    const embeddingDestination = Object.freeze({ id: rawEmbeddingDestination.id, residency: rawEmbeddingDestination.residency, dataUse: rawEmbeddingDestination.dataUse });
    const rawEmbeddingCoordination = embedding.coordination;
    const embeddingCoordination = Object.freeze({ policyHash: rawEmbeddingCoordination.policyHash, policyEpoch: rawEmbeddingCoordination.policyEpoch });
    const policyEmbedding = policy.destinationIds.embedding;
    if (policyEmbedding === undefined || embeddingDestination.id !== policyEmbedding || embeddingDestination.residency !== policy.residency || embeddingDestination.dataUse !== policy.dataUse)
        throw new TypeError("Embedding destination does not match the policy intersection");
    if (embeddingCoordination.policyHash !== authority.coordinationPolicyHash || embeddingCoordination.policyEpoch !== authority.coordinationPolicyEpoch)
        throw new TypeError("Embedding coordination identity does not match the authority");
    if (policy.id !== authority.processingPolicyId)
        throw new TypeError("Policy intersection does not match the authority");
    // Post-LLM acceptance barrier: fresh control/claim/proposal/job/tombstone
    // reads + fresh clocks; a late/stale LLM response is physically harmless.
    const acceptance = await readActiveAcceptance(store, authority);
    if (acceptance === null)
        throw new TypeError("Active acceptance is no longer valid before materialization");
    const proposal = acceptance.proposal;
    const job = acceptance.job;
    const membership = proposal.membership;
    // AUTHORITATIVE evidence readback: the store re-reads the membership
    // episodes (a caller can never forge direct-user evidence).
    const storedEpisodes = await store.readEpisodes(membership, authority.privacyEpoch).catch(() => []);
    if (storedEpisodes.length !== membership.length)
        throw new TypeError("Membership episodes are missing");
    const episodeById = new Map(storedEpisodes.map((episode) => [episode.id, episode]));
    if (job.membership.length === 0 || episodeById.get(job.membership[0])?.createdAt !== job.createdAt)
        throw new TypeError("Accepted job createdAt is not bound to its canonical episode");
    const directUserEpisodeIds = new Set(storedEpisodes.filter((episode) => episode.eventKind === "user").map((episode) => episode.id));
    const knownEpisodeIds = new Set(storedEpisodes.map((episode) => episode.id));
    // Strict result validation over the authoritative evidence context.
    const validationContext = { directUserEpisodeIds, knownEpisodeIds };
    const result = assertPersistableCurationResult(validateCurationResult(resultInput, validationContext), scan);
    // Every accepted proposal must carry the exact strict envelope. There is no
    // legacy summary-only bypass: a stale completion can never be paired with a
    // different accepted proposal.
    const proposalEnvelope = parseCurationProposalEnvelope(proposal.content);
    const expectedDestinationId = policy.destinationIds.llm;
    if (proposalEnvelope === null || expectedDestinationId === undefined || !provenanceMatches(proposalEnvelope.provenance, { host, destinationId: expectedDestinationId, policyId: policy.id, policyHash: policy.id, policyEpoch: authority.coordinationPolicyEpoch, promptRevision: CURATION_PROMPT_REVISION }) || proposal.createdAt !== proposalEnvelope.provenance.invokedAt)
        throw new TypeError("accepted-output-provenance");
    const proposedResult = assertPersistableCurationResult(validateCurationResult({ items: proposalEnvelope.items }, validationContext), scan);
    if (canonicalStringify(proposedResult) !== canonicalStringify(result))
        throw new TypeError("Curation result does not match the accepted proposal");
    // Compute every pure projection before any embedding or derived write. This
    // makes duplicate observations/scope ambiguity fail closed without partial
    // materialization.
    const projections = result.items.map((item) => projectCurationItem(host, authority.coordinationPolicyHash, authority.coordinationPolicyEpoch, item, episodeById));
    if (new Set(projections.map((projection) => projection.observationId)).size !== projections.length)
        throw new TypeError("duplicate-observation-projection");
    const derivedTargets = projections.flatMap((projection) => [projection.observationId, projection.contentId, projection.stateKey]).sort();
    // Fresh control snapshot BEFORE embedding (destination revocation check).
    const controlBefore = await store.readControl();
    if (controlBefore.state !== "active")
        throw new TypeError("Control is not active before embedding");
    if (controlBefore.revokedDestinationIds.includes(embeddingDestination.id))
        throw new TypeError("Embedding destination is revoked before embedding");
    if (controlBefore.coordinationPolicyEpoch !== authority.coordinationPolicyEpoch || controlBefore.coordinationPolicyHash !== authority.coordinationPolicyHash || controlBefore.privacyEpoch !== authority.privacyEpoch)
        throw new TypeError("Control policy identity changed before embedding");
    const observations = [];
    const evidenceLinks = [];
    const currents = [];
    const conflicts = [];
    for (let projectionIndex = 0; projectionIndex < result.items.length; projectionIndex += 1) {
        const item = result.items[projectionIndex];
        const projection = projections[projectionIndex];
        const stateKeyValue = projection.stateKey;
        const contentIdValue = projection.contentId;
        const evidence = [...projection.evidence];
        const effectiveOrder = projection.effectiveOrder;
        const primary = projection.primary;
        const observationIdValue = projection.observationId;
        // Partial retries must read and verify an immutable observation before any
        // scan/embedding egress. Existing exact observations are reused verbatim.
        const existingObservation = await store.readObservation(authority, observationIdValue);
        const createdAt = existingObservation?.createdAt ?? primary.eventAt;
        let material;
        if (existingObservation !== null) {
            // Recompute the deterministic structural projection before accepting a
            // partial retry. Persisted text/vector metadata are not trusted merely
            // because the content-addressed point id exists.
            const expectedText = projection.text;
            const expectedValueOwn = item.value !== undefined;
            const existingValueOwn = Object.prototype.hasOwnProperty.call(existingObservation, "value");
            const sortedEvidence = [...item.evidence].sort();
            if (existingObservation.id !== observationIdValue || existingObservation.createdAt !== primary.eventAt || existingObservation.ownerHost !== host || existingObservation.processingPolicyId !== policy.id || existingObservation.coordinationPolicyEpoch !== authority.coordinationPolicyEpoch || existingObservation.coordinationPolicyHash !== authority.coordinationPolicyHash || existingObservation.privacyEpoch !== authority.privacyEpoch || existingObservation.expiresAt !== policy.expiresAt || existingObservation.contentId !== contentIdValue || existingObservation.stateKey !== stateKeyValue || existingObservation.eventAt !== primary.eventAt || existingObservation.effectiveAt !== primary.eventAt || existingObservation.primaryEvidenceEpisodeId !== primary.id || canonicalStringify(existingObservation.effectiveOrder) !== canonicalStringify(effectiveOrder) || existingObservation.category !== item.category || existingObservation.scope !== item.scope || existingObservation.subject !== item.subject || existingObservation.predicate !== item.predicate || existingValueOwn !== expectedValueOwn || (expectedValueOwn && canonicalStringify(existingObservation.value) !== canonicalStringify(item.value)) || existingObservation.text !== expectedText || canonicalStringify(existingObservation.provenance ?? []) !== canonicalStringify(sortedEvidence) || canonicalStringify(existingObservation.sourceEpisodeIds ?? []) !== canonicalStringify(sortedEvidence) || existingObservation.confidence !== item.confidence || existingObservation.contentHash !== canonicalRecordHash(existingObservation) || !Array.isArray(existingObservation.vector) || existingObservation.vector.length !== 1024 || !existingObservation.vector.every((value) => typeof value === "number" && Number.isFinite(value)))
                throw new TypeError("Existing observation does not exactly match the accepted result");
            observations.push(existingObservation);
            // Reused observations already passed the scanner; use persisted text only
            // for deterministic current projection, never for embedding egress.
            material = { text: existingObservation.text ?? "", redactionStatus: "unchanged", secretScan: "passed", dropped: false, contentHash: sha256Hex(existingObservation.text ?? "") };
        }
        else {
            // Structural redaction + final secret scan BEFORE any egress: only
            // secret_scan="passed" text may reach the embedding endpoint.
            material = gateCuratedEgressText(projection.text, { maxChars: 16_000, homeDir: "/", ...(scan === undefined ? {} : { scan }) });
            let vector;
            // Full fresh pre-embedding control/claim/tombstone/control barrier for
            // EVERY item (not only the first item in the batch).
            await assertMaterializationLive(store, authority, embeddingDestination, membership, derivedTargets, job, proposal);
            try {
                vector = await embedding.embed({ model: "bge-m3", text: material.text });
            }
            catch (error) {
                throw new TypeError("Derived embedding failed (retryable, no current write)");
            }
            if (!Array.isArray(vector) || vector.length !== 1024 || !vector.every((value) => typeof value === "number" && Number.isFinite(value)))
                throw new TypeError("Embedding must yield exactly 1024 finite components");
            // This is the mandatory post-embedding barrier: a destination revocation,
            // tombstone, lease steal or policy epoch change during the call prevents
            // even the immutable observation from becoming visible.
            await assertMaterializationLive(store, authority, embeddingDestination, membership, derivedTargets, job, proposal);
            const observation = {
                recordType: "curated_memory", id: observationIdValue, ownerHost: host, schemaRevision: 1,
                createdAt, privacyEpoch: authority.privacyEpoch, processingPolicyId: policy.id, expiresAt: policy.expiresAt,
                contentHash: "pending", contentId: contentIdValue, observationId: observationIdValue,
                eventAt: primary.eventAt, effectiveAt: primary.eventAt, sourceEpisodeIds: [...item.evidence].sort(),
                primaryEvidenceEpisodeId: primary.id, effectiveOrder, stateKey: stateKeyValue,
                category: item.category, scope: item.scope, ...(projection.projectId === undefined ? {} : { projectId: projection.projectId }), subject: item.subject, predicate: item.predicate,
                ...(item.value === undefined ? {} : { value: item.value }), text: material.text,
                provenance: [...item.evidence].sort(), ...(item.confidence === undefined ? {} : { confidence: item.confidence }),
                coordinationPolicyHash: authority.coordinationPolicyHash, coordinationPolicyEpoch: authority.coordinationPolicyEpoch,
                vector: [...vector],
            };
            observation.contentHash = canonicalRecordHash(observation);
            // Immutable observation insert (insert-only converges on partial retries).
            await assertMaterializationLive(store, authority, embeddingDestination, membership, derivedTargets, job, proposal);
            const storedObservation = await store.insertObservation(authority, { record: observation });
            observations.push(storedObservation);
        }
        const storedObservation = existingObservation ?? observations[observations.length - 1];
        // Immutable evidence links.
        for (const evidenceEpisode of evidence) {
            const link = {
                recordType: "evidence_link", id: "pending", ownerHost: host, schemaRevision: 1,
                createdAt, privacyEpoch: authority.privacyEpoch, processingPolicyId: policy.id, expiresAt: policy.expiresAt,
                contentHash: "pending", sourceId: observationIdValue, targetId: evidenceEpisode.id,
                jobId: authority.jobId, extractorRevision,
                coordinationPolicyHash: authority.coordinationPolicyHash, coordinationPolicyEpoch: authority.coordinationPolicyEpoch,
            };
            link.id = evidenceLinkId(observationIdValue, evidenceEpisode.id, extractorRevision);
            link.contentHash = canonicalRecordHash(link);
            await assertMaterializationLive(store, authority, embeddingDestination, membership, derivedTargets, job, proposal);
            const storedLink = await store.insertEvidenceLink(authority, { record: link });
            evidenceLinks.push(storedLink);
        }
        // Current OCC decision (real read + CAS; never a fake store). Read and
        // validate the entire persisted projection before deciding converge/grow:
        // a canonical hash alone cannot prove that policy, expiry, timestamps,
        // source closure, or a conflict envelope still denotes this job.
        const currentId = curatedCurrentId(host, stateKeyValue, authority.coordinationPolicyEpoch);
        await assertMaterializationLive(store, authority, embeddingDestination, membership, derivedTargets, job, proposal);
        const current = await store.readCurrent(authority, currentId);
        let preloadedConflictManifest = null;
        const preloadedConflictMembers = [];
        let priorConflictAggregate = null;
        let alreadyMemberConflict = false;
        if (current !== null) {
            if (current.id !== currentId || current.ownerHost !== host || current.schemaRevision !== 1 || !Number.isSafeInteger(current.version) || current.version < 1 || typeof current.processingPolicyId !== "string" || current.processingPolicyId.length === 0 || (current.expiresAt !== null && typeof current.expiresAt !== "string") || current.privacyEpoch !== authority.privacyEpoch || current.coordinationPolicyEpoch !== authority.coordinationPolicyEpoch || current.coordinationPolicyHash !== authority.coordinationPolicyHash || current.stateKey !== stateKeyValue || current.sourceEpisodeIds === undefined || current.sourceEpisodeIds.length === 0 || canonicalStringify(current.sourceEpisodeIds) !== canonicalStringify([...current.sourceEpisodeIds].sort()) || current.contentHash !== canonicalRecordHash(current))
                throw new TypeError("Existing current envelope mismatch");
            try {
                validateEffectiveOrder(current.effectiveOrder);
            }
            catch {
                throw new TypeError("Existing current order is invalid");
            }
            if (current.resolution === "resolved") {
                if (current.observationId === undefined || current.contentId === undefined || current.text === undefined || current.vector === undefined || current.vector.length !== 1024 || !current.vector.every((value) => typeof value === "number" && Number.isFinite(value)))
                    throw new TypeError("Existing resolved current is incomplete");
                const pointed = await store.readObservation(authority, current.observationId);
                if (pointed === null || pointed.id !== pointed.observationId || pointed.ownerHost !== host || pointed.schemaRevision !== 1 || pointed.processingPolicyId !== current.processingPolicyId || pointed.expiresAt !== current.expiresAt || pointed.privacyEpoch !== authority.privacyEpoch || pointed.coordinationPolicyEpoch !== authority.coordinationPolicyEpoch || pointed.coordinationPolicyHash !== authority.coordinationPolicyHash || pointed.contentHash !== canonicalRecordHash(pointed) || pointed.contentId !== current.contentId || pointed.observationId !== current.observationId || pointed.stateKey !== current.stateKey || pointed.createdAt !== pointed.eventAt || current.createdAt !== pointed.eventAt || pointed.eventAt !== pointed.effectiveAt || pointed.text !== current.text || canonicalStringify(pointed.effectiveOrder) !== canonicalStringify(current.effectiveOrder) || canonicalStringify(pointed.sourceEpisodeIds ?? []) !== canonicalStringify(current.sourceEpisodeIds) || pointed.vector === undefined || pointed.vector.length !== 1024 || pointed.vector.some((value, index) => current.vector[index] !== value))
                    throw new TypeError("Existing current projection mismatch");
            }
            else {
                if (current.conflictManifestHash === undefined || current.vector !== undefined || current.contentId !== undefined || current.observationId !== undefined || current.text !== undefined)
                    throw new TypeError("Existing conflict current is incomplete");
                preloadedConflictManifest = await store.readConflictManifest(authority, current.conflictManifestHash);
                if (preloadedConflictManifest === null || preloadedConflictManifest.ownerHost !== host || preloadedConflictManifest.schemaRevision !== 1 || preloadedConflictManifest.processingPolicyId !== current.processingPolicyId || preloadedConflictManifest.expiresAt !== current.expiresAt || preloadedConflictManifest.privacyEpoch !== authority.privacyEpoch || preloadedConflictManifest.coordinationPolicyEpoch !== authority.coordinationPolicyEpoch || preloadedConflictManifest.coordinationPolicyHash !== authority.coordinationPolicyHash || preloadedConflictManifest.stateKey !== stateKeyValue || preloadedConflictManifest.members.length < 2 || preloadedConflictManifest.members.some((id, index) => index > 0 && preloadedConflictManifest.members[index - 1] >= id) || preloadedConflictManifest.id !== conflictManifestId(preloadedConflictManifest.coordinationPolicyHash, preloadedConflictManifest.stateKey, preloadedConflictManifest.members) || preloadedConflictManifest.contentHash !== canonicalRecordHash(preloadedConflictManifest) || current.conflictManifestHash !== preloadedConflictManifest.id)
                    throw new TypeError("Existing conflict manifest is invalid");
                // Conflict manifests retain occurrence members, but repeated occurrences
                // of one logical content are convergence, not new alternatives. Read
                // every canonical member before deciding whether a CAS/growth is needed.
                for (const memberId of preloadedConflictManifest.members) {
                    const member = await store.readObservation(authority, memberId);
                    if (member === null || member.id !== member.observationId || member.createdAt !== member.eventAt || member.effectiveAt !== member.eventAt || member.contentHash !== canonicalRecordHash(member) || member.ownerHost !== host || member.schemaRevision !== 1 || member.processingPolicyId !== current.processingPolicyId || member.expiresAt !== current.expiresAt || member.privacyEpoch !== authority.privacyEpoch || member.coordinationPolicyEpoch !== authority.coordinationPolicyEpoch || member.coordinationPolicyHash !== authority.coordinationPolicyHash || member.stateKey !== stateKeyValue || member.contentId === undefined || member.sourceEpisodeIds === undefined || member.sourceEpisodeIds.length === 0 || member.provenance === undefined || canonicalStringify(member.sourceEpisodeIds) !== canonicalStringify([...member.sourceEpisodeIds].sort()) || canonicalStringify(member.provenance) !== canonicalStringify([...member.provenance].sort()) || !Array.isArray(member.vector) || member.vector.length !== 1024 || !member.vector.every((value) => typeof value === "number" && Number.isFinite(value)))
                        throw new TypeError("Existing conflict member is invalid");
                    preloadedConflictMembers.push(member);
                    if (member.contentId === storedObservation.contentId)
                        alreadyMemberConflict = true;
                }
                try {
                    priorConflictAggregate = projectConflictAggregate(preloadedConflictMembers);
                }
                catch {
                    throw new TypeError("Existing conflict aggregate is invalid");
                }
                if (priorConflictAggregate.members.length !== preloadedConflictManifest.members.length || canonicalStringify(preloadedConflictManifest.members) !== canonicalStringify(priorConflictAggregate.members) || preloadedConflictManifest.createdAt !== priorConflictAggregate.createdAt || canonicalStringify(current.sourceEpisodeIds) !== canonicalStringify(priorConflictAggregate.sourceEpisodeIds) || current.createdAt !== priorConflictAggregate.createdAt || canonicalStringify(current.effectiveOrder) !== canonicalStringify(priorConflictAggregate.effectiveOrder))
                    throw new TypeError("Existing conflict projection mismatch");
            }
        }
        let decision = decideCurrent(current, storedObservation, maxClockSkewMs);
        // Same-content arrivals must still pass the complete persisted conflict
        // validation above; only then may they converge without a write.
        if (alreadyMemberConflict)
            decision = "converge";
        if (decision === "insert" || decision === "update" || decision === "conflict" || decision === "conflict_grow") {
            const currentCreatedAt = storedObservation.eventAt;
            const nextBase = {
                recordType: "curated_current", id: currentId, ownerHost: host, schemaRevision: 1,
                createdAt: currentCreatedAt, privacyEpoch: authority.privacyEpoch, processingPolicyId: policy.id,
                expiresAt: policy.expiresAt, contentHash: "pending", version: (current?.version ?? 0) + 1,
                stateKey: stateKeyValue, effectiveOrder: storedObservation.effectiveOrder,
                scope: projection.scope, ...(projection.projectId === undefined ? {} : { projectId: projection.projectId }),
                sourceEpisodeIds: [...storedObservation.sourceEpisodeIds ?? []],
                coordinationPolicyHash: authority.coordinationPolicyHash, coordinationPolicyEpoch: authority.coordinationPolicyEpoch,
            };
            let next;
            if (decision === "conflict" || decision === "conflict_grow") {
                // Content-addressed conflict manifest by CAS: no winner is chosen.
                const existingMembers = preloadedConflictManifest !== null
                    ? preloadedConflictManifest.members
                    : current.resolution === "resolved" ? [current.observationId] : [];
                const members = [...new Set([...existingMembers, storedObservation.id])].sort();
                if (members.length < 2)
                    throw new TypeError("Conflict manifest requires at least two observations");
                // Never write coverage for an observation whose conflict manifest cannot
                // be represented. The accepted job remains retryable/quarantined.
                if (members.length > MAX_CONFLICT_MEMBERS)
                    throw new TypeError("Conflict manifest member cap exceeded");
                const aggregateMembers = [...preloadedConflictMembers.filter((member) => members.includes(member.id)), storedObservation];
                if (current?.resolution === "resolved" && current.observationId !== undefined && !aggregateMembers.some((member) => member.id === current.observationId)) {
                    const pointed = await store.readObservation(authority, current.observationId);
                    if (pointed === null)
                        throw new TypeError("Existing conflict observation is missing");
                    aggregateMembers.push(pointed);
                }
                const aggregate = projectConflictAggregate(aggregateMembers);
                if (aggregate.members.length !== members.length)
                    throw new TypeError("Conflict aggregate membership is incomplete");
                const manifest = {
                    recordType: "conflict_manifest", id: "pending", ownerHost: host, schemaRevision: 1,
                    createdAt: aggregate.createdAt, privacyEpoch: authority.privacyEpoch, processingPolicyId: policy.id,
                    expiresAt: policy.expiresAt, contentHash: "pending", stateKey: stateKeyValue, members: [...aggregate.members],
                    coordinationPolicyHash: authority.coordinationPolicyHash, coordinationPolicyEpoch: authority.coordinationPolicyEpoch,
                };
                manifest.id = conflictManifestId(manifest.coordinationPolicyHash, manifest.stateKey, manifest.members);
                manifest.contentHash = canonicalRecordHash(manifest);
                await assertMaterializationLive(store, authority, embeddingDestination, membership, derivedTargets, job, proposal);
                const storedManifest = await store.insertConflictManifest(authority, { record: manifest });
                conflicts.push(storedManifest);
                next = { ...nextBase, createdAt: aggregate.createdAt, sourceEpisodeIds: [...aggregate.sourceEpisodeIds], resolution: "conflict", conflictManifestHash: storedManifest.id, effectiveOrder: aggregate.effectiveOrder };
            }
            else {
                next = {
                    ...nextBase, resolution: "resolved", contentId: storedObservation.contentId, observationId: storedObservation.id,
                    text: storedObservation.text, vector: Object.freeze([...(storedObservation.vector ?? [])]),
                };
            }
            next.contentHash = canonicalRecordHash(next);
            await assertMaterializationLive(store, authority, embeddingDestination, membership, derivedTargets, job, proposal);
            const storedCurrent = await store.upsertCuratedCurrent(authority, { record: next, expectedVersion: current?.version ?? null });
            if (storedCurrent === null)
                throw new TypeError("Curated current CAS failed (retryable)");
            currents.push(storedCurrent);
        }
    }
    // Coverage for every curated episode/extractor revision AFTER all immutable
    // observation/evidence/current readbacks succeeded; accepted empty results
    // still write coverage. Overlapping memberships fold to one logical segment
    // (insert-only convergence) while stale outputs stay retired/invisible.
    const coverage = [];
    for (const episode of storedEpisodes) {
        await assertMaterializationLive(store, authority, embeddingDestination, membership, derivedTargets, job, proposal);
        const covered = await store.markCoverage(authority, {
            ownerHost: host, episodeId: episode.id, extractorRevision,
            policyHash: authority.coordinationPolicyHash, policyEpoch: authority.coordinationPolicyEpoch,
            privacyEpoch: authority.privacyEpoch, createdAt: episode.createdAt,
            processingPolicyId: policy.id,
        });
        coverage.push(covered);
    }
    // Coverage is itself a visible derived write: prove the accepted authority
    // remains live after the final insert before returning terminal materialization.
    await assertMaterializationLive(store, authority, embeddingDestination, membership, derivedTargets, job, proposal);
    const outcome = { observations: Object.freeze(observations), evidenceLinks: Object.freeze(evidenceLinks), currents: Object.freeze(currents), conflicts: Object.freeze(conflicts), coverage: Object.freeze(coverage) };
    return Object.freeze(outcome);
}
/** Deterministic content-addressed evidence digest for audit trails. */
export function evidenceDigest(evidence) {
    if (!Array.isArray(evidence) || evidence.length === 0 || evidence.length > 1024 || evidence.some((id) => typeof id !== "string"))
        throw new TypeError("Evidence is invalid");
    const sorted = [...new Set(evidence)].sort();
    return sha256Hex(canonicalStringify(sorted));
}
//# sourceMappingURL=temporal.js.map