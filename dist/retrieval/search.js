import { hostFilter, projectFilter } from "./filters.js";
import { mergeCandidates } from "./merge.js";
function isRecord(value) {
    return typeof value === "object" && value !== null && !Array.isArray(value);
}
function hasOwn(value, key) {
    return Object.prototype.hasOwnProperty.call(value, key);
}
function nonEmptyString(value) {
    return typeof value === "string" && value.trim().length > 0;
}
function optionalString(payload, key) {
    if (!hasOwn(payload, key) || payload[key] === undefined)
        return undefined;
    const value = payload[key];
    return nonEmptyString(value) ? value : null;
}
function validOptionalProvenance(payload, key) {
    if (!hasOwn(payload, key) || payload[key] === undefined)
        return true;
    const value = payload[key];
    if (key === "source_point_id") {
        return ((typeof value === "string" && value.length > 0) ||
            (typeof value === "number" && Number.isSafeInteger(value) && value >= 0));
    }
    return nonEmptyString(value);
}
function parseHit(hit, expectedHost, expectedProjectId, lane) {
    const { id, score, payload } = hit;
    if (!isRecord(payload))
        return undefined;
    const normalizedId = typeof id === "string" && id.length > 0
        ? id
        : typeof id === "number" && Number.isSafeInteger(id) && id >= 0
            ? String(id)
            : undefined;
    if (normalizedId === undefined || !Number.isFinite(score))
        return undefined;
    if (payload.host !== expectedHost ||
        payload.status !== "active" ||
        payload.secret_scan !== "passed" ||
        typeof payload.text !== "string" ||
        payload.text.trim().length === 0)
        return undefined;
    const sourceType = optionalString(payload, "source_type");
    const sourceSystem = optionalString(payload, "source_system");
    const projectId = optionalString(payload, "project_id");
    const projectLabel = optionalString(payload, "project_label");
    const createdAt = optionalString(payload, "created_at");
    if (sourceType === null ||
        sourceSystem === null ||
        projectId === null ||
        projectLabel === null ||
        createdAt === null ||
        sourceType === undefined ||
        sourceSystem === undefined ||
        !validOptionalProvenance(payload, "source_collection") ||
        !validOptionalProvenance(payload, "source_point_id"))
        return undefined;
    if (lane === "project" && projectId !== expectedProjectId)
        return undefined;
    if (lane === "host" && projectId === expectedProjectId)
        return undefined;
    const candidate = {
        id: normalizedId,
        text: payload.text,
        rawScore: score,
        adjustedScore: score,
        lane,
        sourceType,
        sourceSystem,
    };
    if (projectId !== undefined)
        candidate.projectId = projectId;
    if (projectLabel !== undefined)
        candidate.projectLabel = projectLabel;
    if (createdAt !== undefined)
        candidate.createdAt = createdAt;
    return candidate;
}
export function parseMemoryHit(hit, input) {
    return parseHit(hit, input.expectedHost, input.expectedProjectId, input.lane);
}
function searchInput(vector, filter, limit, signal) {
    const input = {
        vector,
        limit,
        filter,
    };
    if (signal !== undefined)
        input.signal = signal;
    return input;
}
function clampResultLimit(value) {
    if (Number.isNaN(value))
        return 1;
    if (value === Number.POSITIVE_INFINITY)
        return 10;
    if (value === Number.NEGATIVE_INFINITY)
        return 1;
    return Math.min(10, Math.max(1, Math.trunc(value)));
}
export class MemoryRetriever {
    dependencies;
    constructor(dependencies) {
        this.dependencies = dependencies;
    }
    async search(input) {
        const vector = await this.dependencies.embeddings.embedQuery(input.query, input.signal);
        const laneLimit = this.dependencies.config.candidatesPerLane;
        const [projectHits, hostHits] = await Promise.all([
            this.dependencies.qdrant.search(searchInput(vector, projectFilter(input.host, input.project.id), laneLimit, input.signal)),
            this.dependencies.qdrant.search(searchInput(vector, hostFilter(input.host, input.project.id), laneLimit, input.signal)),
        ]);
        const projectCandidates = projectHits
            .map(hit => parseHit(hit, input.host, input.project.id, "project"))
            .filter((candidate) => candidate !== undefined);
        const hostCandidates = hostHits
            .map(hit => parseHit(hit, input.host, input.project.id, "host"))
            .filter((candidate) => candidate !== undefined);
        const limit = input.limit === undefined ? this.dependencies.config.topK : clampResultLimit(input.limit);
        return {
            query: input.query,
            hits: mergeCandidates({
                project: projectCandidates,
                host: hostCandidates,
                minScore: this.dependencies.config.minScore,
                projectBoost: this.dependencies.config.projectBoost,
                limit,
            }),
        };
    }
}
//# sourceMappingURL=search.js.map