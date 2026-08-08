function condition(key, value) {
    return { key, match: { value } };
}
function freezeFilter(filter) {
    for (const condition of filter.must) {
        Object.freeze(condition.match);
        Object.freeze(condition);
    }
    if (filter.must_not !== undefined) {
        for (const condition of filter.must_not) {
            Object.freeze(condition.match);
            Object.freeze(condition);
        }
        Object.freeze(filter.must_not);
    }
    Object.freeze(filter.must);
    return Object.freeze(filter);
}
function baseFilter(host) {
    return {
        must: [
            condition("host", host),
            condition("status", "active"),
            condition("secret_scan", "passed"),
        ],
    };
}
/** Construct the mandatory current-project lane filter. */
export function projectFilter(host, projectId) {
    if ((host !== "prime" && host !== "pi") || typeof projectId !== "string" || projectId.length === 0) {
        throw new Error("Invalid retrieval scope");
    }
    const filter = baseFilter(host);
    filter.must.push(condition("project_id", projectId));
    return freezeFilter(filter);
}
/** Construct the mandatory same-host, outside-current-project lane filter. */
export function hostFilter(host, projectId) {
    if ((host !== "prime" && host !== "pi") || typeof projectId !== "string" || projectId.length === 0) {
        throw new Error("Invalid retrieval scope");
    }
    const filter = baseFilter(host);
    filter.must_not = [condition("project_id", projectId)];
    return freezeFilter(filter);
}
//# sourceMappingURL=filters.js.map