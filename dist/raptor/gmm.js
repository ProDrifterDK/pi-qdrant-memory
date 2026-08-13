import { types as nodeTypes } from "node:util";
import { Xoshiro128StarStar } from "./random.js";
const VARIANCE_FLOOR = 1e-6;
const MAX_ITERATIONS = 100;
const DEFAULT_TOLERANCE = 1e-4;
function dense(value, label, max) {
    if (!Array.isArray(value) || nodeTypes.isProxy(value) || Object.getPrototypeOf(value) !== Array.prototype || Object.getOwnPropertySymbols(value).length !== 0 || value.length > max || Object.getOwnPropertyNames(value).length !== value.length + 1)
        throw new TypeError(`${label} must be a bounded dense plain array`);
    const output = [];
    for (let index = 0; index < value.length; index += 1) {
        const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
        if (descriptor === undefined || !("value" in descriptor) || descriptor.enumerable !== true)
            throw new TypeError(`${label} contains an accessor or hole`);
        output.push(descriptor.value);
    }
    return output;
}
function matrixSnapshot(input) {
    const rows = dense(input, "GMM input", 65_536);
    if (rows.length < 1)
        throw new TypeError("GMM input is empty");
    const first = rows[0];
    if (!Array.isArray(first) || nodeTypes.isProxy(first))
        throw new TypeError("GMM row is invalid");
    const dimensions = first.length;
    if (!Number.isSafeInteger(dimensions) || dimensions < 1 || dimensions > 64)
        throw new TypeError("GMM dimensions are invalid");
    return rows.map((candidate) => { if (!Array.isArray(candidate))
        throw new TypeError("GMM row is invalid"); const row = dense(candidate, "GMM row", 64); if (row.length !== dimensions || row.some((value) => typeof value !== "number" || !Number.isFinite(value)))
        throw new TypeError("GMM input must be a dense finite matrix"); return row; });
}
function standardize(matrix) {
    const dimensions = matrix[0].length;
    const means = Array.from({ length: dimensions }, (_, column) => matrix.reduce((sum, row) => sum + row[column], 0) / matrix.length);
    const variances = means.map((mean, column) => matrix.reduce((sum, row) => sum + (row[column] - mean) ** 2, 0) / matrix.length);
    const active = variances.map((variance, index) => variance > 1e-12 ? index : -1).filter((index) => index >= 0);
    if (active.length === 0)
        throw new TypeError("GMM input has no varying dimensions");
    return matrix.map((row) => active.map((column) => (row[column] - means[column]) / Math.sqrt(variances[column])));
}
function squaredDistance(left, right) { return left.reduce((sum, value, index) => sum + (value - right[index]) ** 2, 0); }
function initialMeans(matrix, components, seed) {
    const rng = new Xoshiro128StarStar(`${seed}:gmm:${components}`);
    const means = [[...matrix[rng.nextInt(matrix.length)]]];
    while (means.length < components) {
        const distances = matrix.map((row) => Math.min(...means.map((mean) => squaredDistance(row, mean))));
        const total = distances.reduce((sum, value) => sum + value, 0);
        let selected;
        if (!Number.isFinite(total) || total <= 0) {
            selected = matrix.findIndex((row) => !means.some((mean) => squaredDistance(row, mean) === 0));
            if (selected < 0)
                throw new TypeError("GMM initialization made no progress");
        }
        else {
            const target = rng.nextFloat() * total;
            let cumulative = 0;
            selected = distances.length - 1;
            for (let index = 0; index < distances.length; index += 1) {
                cumulative += distances[index];
                if (target < cumulative) {
                    selected = index;
                    break;
                }
            }
        }
        means.push([...matrix[selected]]);
    }
    return means;
}
function logProbability(row, weight, mean, variance) {
    if (!(weight > 0) || !Number.isFinite(weight))
        return Number.NEGATIVE_INFINITY;
    let value = Math.log(weight);
    for (let dimension = 0; dimension < row.length; dimension += 1)
        value += -0.5 * (Math.log(2 * Math.PI * variance[dimension]) + (row[dimension] - mean[dimension]) ** 2 / variance[dimension]);
    return value;
}
function expectation(matrix, weights, means, variances) {
    let logLikelihood = 0;
    const memberships = [];
    for (const row of matrix) {
        const logs = weights.map((weight, index) => logProbability(row, weight, means[index], variances[index]));
        const maximum = Math.max(...logs);
        if (!Number.isFinite(maximum))
            throw new TypeError("GMM likelihood is non-finite");
        const denominator = logs.reduce((sum, value) => sum + Math.exp(value - maximum), 0);
        const logNormalizer = maximum + Math.log(denominator);
        logLikelihood += logNormalizer;
        memberships.push(logs.map((value) => Math.exp(value - logNormalizer)));
    }
    if (!Number.isFinite(logLikelihood))
        throw new TypeError("GMM likelihood is non-finite");
    return { memberships, logLikelihood };
}
export function fitDiagonalGmm(input, options) {
    const raw = matrixSnapshot(input);
    const matrix = standardize(raw);
    const count = matrix.length;
    const dimensions = matrix[0].length;
    const components = options.components;
    const maxIterations = options.maxIterations ?? MAX_ITERATIONS;
    const tolerance = options.tolerance ?? DEFAULT_TOLERANCE;
    if (!Number.isSafeInteger(components) || components < 1 || components > Math.min(50, count))
        throw new TypeError("GMM component count is invalid");
    if (!Number.isSafeInteger(maxIterations) || maxIterations < 1 || maxIterations > MAX_ITERATIONS || !(tolerance > 0) || !Number.isFinite(tolerance))
        throw new TypeError("GMM convergence options are invalid");
    let means = initialMeans(matrix, components, String(options.seed));
    const globalVariance = Array.from({ length: dimensions }, (_, dimension) => Math.max(VARIANCE_FLOOR, matrix.reduce((sum, row) => sum + row[dimension] ** 2, 0) / count));
    let variances = Array.from({ length: components }, () => [...globalVariance]);
    let weights = Array.from({ length: components }, () => 1 / components);
    let previous = Number.NEGATIVE_INFINITY;
    let memberships = [];
    let logLikelihood = Number.NEGATIVE_INFINITY;
    let iterations = 0;
    for (let iteration = 1; iteration <= maxIterations; iteration += 1) {
        const expected = expectation(matrix, weights, means, variances);
        memberships = expected.memberships;
        logLikelihood = expected.logLikelihood;
        iterations = iteration;
        if (iteration > 1 && Math.abs(logLikelihood - previous) <= tolerance)
            break;
        previous = logLikelihood;
        const totals = Array.from({ length: components }, (_, component) => memberships.reduce((sum, row) => sum + row[component], 0));
        if (totals.some((total) => !(total > 1e-12) || !Number.isFinite(total)))
            throw new TypeError("GMM component collapsed");
        weights = totals.map((total) => total / count);
        means = totals.map((total, component) => Array.from({ length: dimensions }, (_, dimension) => memberships.reduce((sum, row, index) => sum + row[component] * matrix[index][dimension], 0) / total));
        variances = totals.map((total, component) => Array.from({ length: dimensions }, (_, dimension) => Math.max(VARIANCE_FLOOR, memberships.reduce((sum, row, index) => sum + row[component] * (matrix[index][dimension] - means[component][dimension]) ** 2, 0) / total)));
        if ([...weights, ...means.flat(), ...variances.flat()].some((value) => !Number.isFinite(value)))
            throw new TypeError("GMM fit is non-finite");
    }
    const finalExpected = expectation(matrix, weights, means, variances);
    memberships = finalExpected.memberships;
    logLikelihood = finalExpected.logLikelihood;
    const parameterCount = components * (2 * dimensions) + (components - 1);
    const bic = -2 * logLikelihood + parameterCount * Math.log(count);
    if (!Number.isFinite(bic))
        throw new TypeError("GMM BIC is non-finite");
    return Object.freeze({ components, dimensions, logLikelihood, parameterCount, bic, iterations, weights: Object.freeze([...weights]), means: Object.freeze(means.map((row) => Object.freeze([...row]))), variances: Object.freeze(variances.map((row) => Object.freeze([...row]))), memberships: Object.freeze(memberships.map((row) => Object.freeze([...row]))) });
}
export function selectDiagonalGmm(input, options) {
    const count = input.length;
    const maxClusters = Math.min(options.maxClusters, 50, Math.max(1, count - 1));
    const threshold = options.membershipThreshold ?? 0.1;
    if (!Number.isSafeInteger(options.maxClusters) || options.maxClusters < 1 || !(threshold >= 0 && threshold <= 1) || !Number.isFinite(threshold))
        throw new TypeError("GMM selection options are invalid");
    const fits = [];
    for (let components = 1; components <= maxClusters; components += 1) {
        try {
            fits.push(fitDiagonalGmm(input, { seed: options.seed, components }));
        }
        catch { /* invalid/singular candidates are excluded from BIC */ }
    }
    if (fits.length === 0)
        throw new TypeError("No finite GMM fit exists");
    const fit = fits.reduce((best, candidate) => candidate.bic < best.bic ? candidate : best);
    const assignments = fit.memberships.map((row) => {
        const maximum = row.indexOf(Math.max(...row));
        const selected = row.map((probability, index) => probability >= threshold || index === maximum ? index : -1).filter((index) => index >= 0);
        return Object.freeze(selected);
    });
    const unique = new Map();
    for (let component = 0; component < fit.components; component += 1) {
        const members = assignments.map((row, index) => row.includes(component) ? index : -1).filter((index) => index >= 0);
        if (members.length > 0)
            unique.set(members.join(","), Object.freeze(members));
    }
    return Object.freeze({ fit, assignments: Object.freeze(assignments), clusters: Object.freeze([...unique.values()]) });
}
//# sourceMappingURL=gmm.js.map