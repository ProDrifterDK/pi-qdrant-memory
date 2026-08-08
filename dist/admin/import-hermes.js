import { timingSafeEqual } from "node:crypto";
import { buildImportPlan } from "./import-plan.js";
const SCROLL_PAGE_SIZE = 256;
const UPSERT_BATCH_SIZE = 64;
const PLAN_ID_PATTERN = /^[a-f0-9]{64}$/;
export class ImportValidationError extends Error {
    constructor(message) {
        super(message);
        this.name = "ImportValidationError";
    }
}
export class ImportInfrastructureError extends Error {
    constructor(message) {
        super(message);
        this.name = "ImportInfrastructureError";
    }
}
export class ImportApprovalMismatchError extends ImportValidationError {
    constructor() {
        super("source changed; run dry-run again");
        this.name = "ImportApprovalMismatchError";
    }
}
function invalid(message) {
    throw new ImportValidationError(message);
}
function validString(value, maximum) {
    return typeof value === "string" && value.trim().length > 0 && value.length <= maximum;
}
function validateOptions(options) {
    if (options.targetHost !== "prime" && options.targetHost !== "pi") {
        invalid("target host is invalid");
    }
    if (!validString(options.sourceIdentity, 4096))
        invalid("source identity is invalid");
    if (!validString(options.sourceCollection, 255))
        invalid("source collection is invalid");
    if (!validString(options.destinationCollection, 255)) {
        invalid("destination collection is invalid");
    }
    if (!validString(options.configuredModel, 256))
        invalid("configured model is invalid");
    if (options.declaredSourceModel !== undefined &&
        !validString(options.declaredSourceModel, 256)) {
        invalid("declared source model is invalid");
    }
    if (!Number.isSafeInteger(options.configuredDimension) ||
        options.configuredDimension <= 0 ||
        options.configuredDimension > 65_536) {
        invalid("configured dimension is invalid");
    }
}
function validateCollectionContracts(configuredDimension, source, destination) {
    if (source.dimension !== configuredDimension ||
        destination.dimension !== configuredDimension) {
        invalid("collection dimension is incompatible");
    }
    if (typeof source.distance !== "string" ||
        typeof destination.distance !== "string" ||
        source.distance.toLowerCase() !== "cosine" ||
        destination.distance.toLowerCase() !== "cosine") {
        invalid("collection distance is incompatible");
    }
}
async function collectionMetadata(role, collection, client, signal) {
    try {
        return await client.collectionInfo(collection, signal);
    }
    catch {
        throw new ImportInfrastructureError(`${role} collection metadata unavailable`);
    }
}
function offsetKey(offset) {
    return `${typeof offset}:${String(offset)}`;
}
function validOffset(offset) {
    return ((typeof offset === "string" && offset.length > 0) ||
        (typeof offset === "number" && Number.isSafeInteger(offset) && offset >= 0));
}
async function readAllSourcePoints(options, source) {
    const points = [];
    const seenOffsets = new Set();
    let offset;
    while (true) {
        let page;
        try {
            page = await source.scroll(options.sourceCollection, offset, SCROLL_PAGE_SIZE, options.signal);
        }
        catch {
            throw new ImportInfrastructureError("source collection read unavailable");
        }
        if (typeof page !== "object" ||
            page === null ||
            !Array.isArray(page.points)) {
            throw new ImportInfrastructureError("source collection read unavailable");
        }
        points.push(...page.points);
        const nextOffset = page.nextOffset;
        if (nextOffset === undefined)
            break;
        if (!validOffset(nextOffset)) {
            throw new ImportInfrastructureError("source pagination offset is invalid");
        }
        const key = offsetKey(nextOffset);
        if (seenOffsets.has(key)) {
            invalid("source pagination offset repeated or cyclic");
        }
        seenOffsets.add(key);
        offset = nextOffset;
    }
    return points;
}
export async function planHermesImport(options, clients) {
    validateOptions(options);
    const sourceInfo = await collectionMetadata("source", options.sourceCollection, clients.source, options.signal);
    const destinationInfo = await collectionMetadata("destination", options.destinationCollection, clients.destination, options.signal);
    validateCollectionContracts(options.configuredDimension, sourceInfo, destinationInfo);
    const points = await readAllSourcePoints(options, clients.source);
    try {
        return buildImportPlan({
            points,
            targetHost: options.targetHost,
            sourceIdentity: options.sourceIdentity,
            sourceCollection: options.sourceCollection,
            sourceDimension: sourceInfo.dimension,
            sourceDistance: sourceInfo.distance,
            destinationCollection: options.destinationCollection,
            destinationDimension: destinationInfo.dimension,
            destinationDistance: destinationInfo.distance,
            configuredModel: options.configuredModel,
            ...(options.declaredSourceModel === undefined
                ? {}
                : { declaredSourceModel: options.declaredSourceModel }),
        });
    }
    catch {
        throw new ImportValidationError("source content is incompatible");
    }
}
function decodePlanId(value) {
    if (!PLAN_ID_PATTERN.test(value))
        return undefined;
    const decoded = Buffer.from(value, "hex");
    return decoded.length === 32 ? decoded : undefined;
}
export async function applyHermesImport(options, clients) {
    const approvedDigest = decodePlanId(options.approvedPlanId);
    if (approvedDigest === undefined) {
        throw new ImportValidationError("approval plan ID is invalid");
    }
    const plan = await planHermesImport(options, clients);
    const computedDigest = decodePlanId(plan.planId);
    if (computedDigest === undefined ||
        computedDigest.length !== approvedDigest.length ||
        !timingSafeEqual(computedDigest, approvedDigest)) {
        throw new ImportApprovalMismatchError();
    }
    let batches = 0;
    for (let index = 0; index < plan.accepted.length; index += UPSERT_BATCH_SIZE) {
        const batch = plan.accepted.slice(index, index + UPSERT_BATCH_SIZE);
        if (batch.length === 0)
            continue;
        await clients.destination.upsert(options.destinationCollection, batch, options.signal);
        batches += 1;
    }
    return { planId: plan.planId, upserted: plan.accepted.length, batches };
}
//# sourceMappingURL=import-hermes.js.map