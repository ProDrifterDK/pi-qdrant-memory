import { randomBytes } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import { join } from "node:path";
import { canonicalStringify, sha256Hex } from "../domain/canonical.js";
const RETIREMENT_KEYS = ["version", "kind", "generation", "reservation", "auditHash"];
export const ADMISSION_GENERATION_LIMIT = 1_000_000;
export const ADMISSION_LOCK = /^admission\.([0-9]{16})\.lock$/u;
export const ADMISSION_RETIREMENT = /^admission\.([0-9]{16})\.retired$/u;
const ADMISSION_RETIREMENT_TEMP = /^\.admission\.([0-9]{16})\.retired\.tmp-[0-9]+-[a-f0-9]{32}$/u;
const directoryProtocolCaches = new Map();
function record(value) { return typeof value === "object" && value !== null && !Array.isArray(value); }
function exactKeys(value, keys) { const actual = Object.keys(value).sort(); const expected = [...keys].sort(); return actual.length === expected.length && actual.every((key, index) => key === expected[index]); }
function errno(error, code) { return record(error) && error.code === code; }
function hashWithout(value, key) { const copy = { ...value }; delete copy[key]; return sha256Hex(canonicalStringify(copy)); }
function sameReservation(left, right) { return canonicalStringify(left) === canonicalStringify(right); }
function generationFrom(match) { return Number.parseInt(match[1], 10); }
function assertReservationsDirectory(info) { if (!info.isDirectory() || info.isSymbolicLink() || (info.mode & 0o077) !== 0)
    throw new Error("Outbox reservations directory is unsafe"); }
function cacheForDirectory(dir, identity) {
    assertReservationsDirectory(identity);
    const current = directoryProtocolCaches.get(dir);
    if (current !== undefined && current.dev === identity.dev && current.ino === identity.ino)
        return current;
    const replacement = { dev: identity.dev, ino: identity.ino, cursor: 0, retiredMarkers: new Map(), retiredByGeneration: new Map(), retiredReservations: new Map() };
    directoryProtocolCaches.set(dir, replacement);
    return replacement;
}
function markerIdentity(info) {
    if (!info.isFile() || info.isSymbolicLink() || (info.mode & 0o077) !== 0)
        return undefined;
    const values = [info.mode, info.size, info.dev, info.ino, info.nlink, info.ctimeMs, info.mtimeMs];
    if (values.some((value) => typeof value !== "number" || !Number.isFinite(value)))
        return undefined;
    return { mode: info.mode, size: info.size, dev: info.dev, ino: info.ino, nlink: info.nlink, ctimeMs: info.ctimeMs, mtimeMs: info.mtimeMs };
}
function sameMarkerIdentity(left, right) { return left.mode === right.mode && left.size === right.size && left.dev === right.dev && left.ino === right.ino && left.nlink === right.nlink && left.ctimeMs === right.ctimeMs && left.mtimeMs === right.mtimeMs; }
function sameMarkerInode(left, right) { return left.mode === right.mode && left.size === right.size && left.dev === right.dev && left.ino === right.ino; }
function isPublisherTempUnlink(left, right) { return sameMarkerInode(left, right) && left.mtimeMs === right.mtimeMs && left.nlink === 2 && right.nlink === 1 && right.ctimeMs >= left.ctimeMs; }
function cacheRetirement(dir, generation, reservation, identity) {
    const cache = directoryProtocolCaches.get(dir);
    if (cache === undefined)
        return;
    const canonical = canonicalStringify(reservation);
    const existing = cache.retiredReservations.get(reservation.reservationId);
    if (existing !== undefined && (existing.canonical !== canonical || existing.generation !== generation))
        throw new Error("Outbox admission reservation was retired more than once");
    const generationCanonical = cache.retiredByGeneration.get(generation);
    if (generationCanonical !== undefined && generationCanonical !== canonical)
        throw new Error("Outbox admission retirement marker changed");
    if (identity === undefined)
        cache.retiredMarkers.delete(generation);
    else
        cache.retiredMarkers.set(generation, identity);
    cache.retiredByGeneration.set(generation, canonical);
    cache.retiredReservations.set(reservation.reservationId, { generation, canonical });
}
function bumpGenerationHint(dir, generation) { const cache = directoryProtocolCaches.get(dir); if (cache !== undefined)
    cache.cursor = Math.max(cache.cursor, generation); }
export function admissionLockName(generation) { if (!Number.isSafeInteger(generation) || generation < 0 || generation >= ADMISSION_GENERATION_LIMIT)
    throw new Error("Outbox admission generation is out of bounds"); return `admission.${generation.toString().padStart(16, "0")}.lock`; }
export function admissionRetirementName(generation) { if (!Number.isSafeInteger(generation) || generation < 0 || generation >= ADMISSION_GENERATION_LIMIT)
    throw new Error("Outbox admission generation is out of bounds"); return `admission.${generation.toString().padStart(16, "0")}.retired`; }
export function isAdmissionProtocolArtifact(name) { return ADMISSION_LOCK.test(name) || ADMISSION_RETIREMENT.test(name) || ADMISSION_RETIREMENT_TEMP.test(name); }
async function syncDirectory(fs, path) { const handle = await fs.open(path, "r"); try {
    await handle.sync();
}
finally {
    await handle.close();
} }
async function durableRemove(fs, path, dir) { try {
    await fs.rm(path);
}
catch (error) {
    if (!errno(error, "ENOENT"))
        throw error;
} await syncDirectory(fs, dir); }
async function readExactPrivateBytes(fs, path, label) {
    const info = await fs.lstat(path);
    if (!info.isFile() || info.isSymbolicLink() || (info.mode & 0o077) !== 0 || info.size < 1 || info.size > 1_048_576)
        throw new Error(`${label} is unsafe`);
    let handle;
    let bytes;
    let opened;
    try {
        handle = await fs.open(path, fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW);
        opened = await handle.stat();
        if (!opened.isFile() || opened.isSymbolicLink() || opened.dev !== info.dev || opened.ino !== info.ino || opened.size !== info.size || (opened.mode & 0o077) !== 0)
            throw new Error(`${label} inode changed`);
        bytes = await handle.readFile();
    }
    finally {
        await handle?.close().catch(() => undefined);
    }
    const after = await fs.lstat(path);
    if (!after.isFile() || after.isSymbolicLink() || after.dev !== info.dev || after.ino !== info.ino || after.size !== info.size || (after.mode & 0o077) !== 0 || bytes.length !== info.size)
        throw new Error(`${label} path changed`);
    return { bytes, identity: markerIdentity(after) };
}
async function readPrivateJson(fs, path) {
    return JSON.parse(Buffer.from((await readExactPrivateBytes(fs, path, "Outbox admission protocol file")).bytes).toString("utf8"));
}
function retirementValue(generation, reservation) {
    const value = { version: 1, kind: "admission_lock_retired", generation, reservation, auditHash: "" };
    value.auditHash = hashWithout(value, "auditHash");
    return value;
}
function validateRetirement(input, generation, validateReservation) {
    if (!record(input) || !exactKeys(input, RETIREMENT_KEYS) || input.version !== 1 || input.kind !== "admission_lock_retired" || input.generation !== generation)
        throw new Error("Outbox admission retirement marker is malformed");
    const reservation = validateReservation(input.reservation);
    const value = { version: 1, kind: "admission_lock_retired", generation, reservation, auditHash: input.auditHash };
    if (typeof value.auditHash !== "string" || value.auditHash !== hashWithout(value, "auditHash"))
        throw new Error("Outbox admission retirement marker is malformed");
    return value;
}
async function readExactRetirement(fs, file, generation, validateReservation) {
    const exact = await readExactPrivateBytes(fs, file, "Outbox admission retirement marker");
    const text = Buffer.from(exact.bytes).toString("utf8");
    const parsed = validateRetirement(JSON.parse(text), generation, validateReservation);
    if (text !== canonicalStringify(parsed))
        throw new Error("Outbox admission retirement marker is not canonical");
    return { marker: parsed, identity: exact.identity };
}
async function durableRetirement(fs, dir, generation, validateReservation) {
    const dirInfo = await fs.lstat(dir);
    cacheForDirectory(dir, dirInfo);
    const file = join(dir, admissionRetirementName(generation));
    let first;
    try {
        first = await readExactRetirement(fs, file, generation, validateReservation);
    }
    catch (error) {
        if (errno(error, "ENOENT"))
            return undefined;
        throw error;
    }
    await syncDirectory(fs, dir);
    const dirAfter = await fs.lstat(dir);
    assertReservationsDirectory(dirAfter);
    if (dirAfter.dev !== dirInfo.dev || dirAfter.ino !== dirInfo.ino)
        throw new Error("Outbox reservations directory changed");
    const second = await readExactRetirement(fs, file, generation, validateReservation);
    if (canonicalStringify(second.marker) !== canonicalStringify(first.marker))
        throw new Error("Outbox admission retirement marker readback changed");
    if (first.identity !== undefined && second.identity !== undefined && !sameMarkerInode(first.identity, second.identity))
        throw new Error("Outbox admission retirement marker readback changed");
    cacheRetirement(dir, generation, second.marker.reservation, second.identity);
    return second.marker;
}
async function validateCachedRetirement(fs, dir, cache, generation, validateReservation) {
    const cached = cache.retiredMarkers.get(generation);
    const file = join(dir, admissionRetirementName(generation));
    if (cached !== undefined) {
        let current;
        try {
            current = await fs.lstat(file);
        }
        catch (error) {
            if (errno(error, "ENOENT"))
                throw new Error("Outbox admission retirement disappeared during scan");
            throw error;
        }
        const identity = markerIdentity(current);
        if (identity !== undefined) {
            if (sameMarkerIdentity(cached, identity))
                return;
            if (isPublisherTempUnlink(cached, identity)) {
                const exact = await readExactRetirement(fs, file, generation, validateReservation);
                const canonical = cache.retiredByGeneration.get(generation);
                if (canonical === undefined || canonicalStringify(exact.marker.reservation) !== canonical || exact.identity === undefined || !sameMarkerIdentity(identity, exact.identity))
                    throw new Error("Outbox admission retirement marker changed");
                cache.retiredMarkers.set(generation, exact.identity);
                return;
            }
            throw new Error("Outbox admission retirement marker changed");
        }
    }
    const marker = await durableRetirement(fs, dir, generation, validateReservation);
    if (marker === undefined)
        throw new Error("Outbox admission retirement disappeared during scan");
}
function sameNameSet(left, right) {
    if (left.length !== right.length)
        return false;
    const sortedLeft = [...left].sort();
    const sortedRight = [...right].sort();
    return sortedLeft.every((name, index) => name === sortedRight[index]);
}
async function validateColdRetirementSnapshot(fs, dir, directory, names, generations, validateReservation) {
    if (generations.length === 0)
        return;
    const validated = new Map();
    for (const generation of generations) {
        const file = join(dir, admissionRetirementName(generation));
        let exact;
        try {
            exact = await readExactRetirement(fs, file, generation, validateReservation);
        }
        catch (error) {
            if (errno(error, "ENOENT"))
                throw new Error("Outbox admission retirement disappeared during scan");
            throw error;
        }
        validated.set(generation, exact);
    }
    await syncDirectory(fs, dir);
    const beforeReadback = await fs.lstat(dir);
    assertReservationsDirectory(beforeReadback);
    if (beforeReadback.dev !== directory.dev || beforeReadback.ino !== directory.ino)
        throw new Error("Outbox reservations directory changed during scan");
    const readbackNames = await fs.readdir(dir);
    const afterReadback = await fs.lstat(dir);
    assertReservationsDirectory(afterReadback);
    if (afterReadback.dev !== directory.dev || afterReadback.ino !== directory.ino || !sameNameSet(names, readbackNames))
        throw new Error("Outbox reservations directory name set changed during scan");
    for (const generation of generations) {
        const first = validated.get(generation);
        const file = join(dir, admissionRetirementName(generation));
        let current;
        try {
            current = await fs.lstat(file);
        }
        catch (error) {
            if (errno(error, "ENOENT"))
                throw new Error("Outbox admission retirement disappeared during scan");
            throw error;
        }
        const identity = markerIdentity(current);
        if (first.identity !== undefined && identity !== undefined) {
            if (sameMarkerIdentity(first.identity, identity))
                continue;
            if (!isPublisherTempUnlink(first.identity, identity))
                throw new Error("Outbox admission retirement marker changed during scan");
            const second = await readExactRetirement(fs, file, generation, validateReservation);
            if (canonicalStringify(second.marker) !== canonicalStringify(first.marker) || second.identity === undefined || !sameMarkerIdentity(identity, second.identity))
                throw new Error("Outbox admission retirement marker changed during scan");
            first.identity = second.identity;
        }
        else {
            const second = await readExactRetirement(fs, file, generation, validateReservation);
            if (canonicalStringify(second.marker) !== canonicalStringify(first.marker))
                throw new Error("Outbox admission retirement marker changed during scan");
        }
    }
    for (const [generation, exact] of validated)
        cacheRetirement(dir, generation, exact.marker.reservation, exact.identity);
}
async function cleanupRetirementTemps(fs, dir, generation, validateReservation, alreadyValidated = false, knownNames) {
    if (!alreadyValidated && await durableRetirement(fs, dir, generation, validateReservation) === undefined)
        return;
    let removed = false;
    const names = knownNames ?? await fs.readdir(dir);
    for (const name of names) {
        const match = ADMISSION_RETIREMENT_TEMP.exec(name);
        if (match !== null && generationFrom(match) === generation) {
            try {
                await fs.rm(join(dir, name));
                removed = true;
            }
            catch (error) {
                if (!errno(error, "ENOENT"))
                    throw error;
            }
        }
    }
    if (removed)
        await syncDirectory(fs, dir);
    if (removed && await durableRetirement(fs, dir, generation, validateReservation) === undefined)
        throw new Error("Outbox admission retirement disappeared during cleanup");
}
async function scanAdmissionStateOnce(fs, dir, validateReservation) {
    const initialDirectory = await fs.lstat(dir);
    const cache = cacheForDirectory(dir, initialDirectory);
    const names = await fs.readdir(dir);
    const afterRead = await fs.lstat(dir);
    assertReservationsDirectory(afterRead);
    if (afterRead.dev !== initialDirectory.dev || afterRead.ino !== initialDirectory.ino)
        throw new Error("Outbox reservations directory changed during scan");
    if (names.includes("admission.lock"))
        throw new Error("Outbox legacy admission lock is unsafe");
    const retired = new Set();
    const locks = new Map();
    const temps = new Set();
    for (const name of names) {
        let match = ADMISSION_RETIREMENT.exec(name);
        if (match !== null) {
            const generation = generationFrom(match);
            if (generation >= ADMISSION_GENERATION_LIMIT)
                throw new Error("Outbox admission generation limit reached");
            retired.add(generation);
            continue;
        }
        match = ADMISSION_LOCK.exec(name);
        if (match !== null) {
            const generation = generationFrom(match);
            if (generation >= ADMISSION_GENERATION_LIMIT)
                throw new Error("Outbox admission generation limit reached");
            const list = locks.get(generation) ?? [];
            list.push(name);
            locks.set(generation, list);
            continue;
        }
        match = ADMISSION_RETIREMENT_TEMP.exec(name);
        if (match !== null) {
            const generation = generationFrom(match);
            if (generation >= ADMISSION_GENERATION_LIMIT)
                throw new Error("Outbox admission generation limit reached");
            temps.add(generation);
            continue;
        }
        if (name.startsWith("admission.") || name.startsWith(".admission."))
            throw new Error("Outbox admission protocol artifact is malformed");
    }
    let cursor = cache.cursor;
    if (!Number.isSafeInteger(cursor) || cursor < 0 || cursor >= ADMISSION_GENERATION_LIMIT)
        throw new Error("Outbox admission generation limit reached");
    for (let generation = 0; generation < cursor; generation += 1) {
        if (!retired.has(generation))
            throw new Error("Outbox admission generation history is not contiguous");
        await validateCachedRetirement(fs, dir, cache, generation, validateReservation);
    }
    const coldGenerations = [];
    while (retired.has(cursor)) {
        coldGenerations.push(cursor);
        cursor += 1;
        if (cursor >= ADMISSION_GENERATION_LIMIT)
            throw new Error("Outbox admission generation limit reached");
    }
    await validateColdRetirementSnapshot(fs, dir, initialDirectory, names, coldGenerations, validateReservation);
    for (const generation of retired)
        if (generation > cursor)
            throw new Error("Outbox admission generation history has a gap");
    for (const generation of temps) {
        if (generation < cursor)
            await cleanupRetirementTemps(fs, dir, generation, validateReservation, true, names).catch(() => undefined);
        else if (generation > cursor)
            throw new Error("Outbox admission retirement temp is beyond the generation cursor");
    }
    let active;
    for (const [generation, namesForGeneration] of locks) {
        if (namesForGeneration.length !== 1)
            throw new Error("Outbox admission generation has ambiguous locks");
        const file = join(dir, namesForGeneration[0]);
        if (generation < cursor) {
            if (!retired.has(generation))
                throw new Error("Outbox admission lock predates contiguous retirement history");
            await durableRemove(fs, file, dir).catch(() => undefined);
            continue;
        }
        if (generation > cursor)
            throw new Error("Outbox admission lock is beyond the contiguous generation cursor");
        const reservation = validateReservation(await readPrivateJson(fs, file));
        if (active !== undefined)
            throw new Error("Outbox admission has multiple active generations");
        active = { generation, file, reservation };
    }
    const finalDirectory = await fs.lstat(dir);
    assertReservationsDirectory(finalDirectory);
    if (finalDirectory.dev !== initialDirectory.dev || finalDirectory.ino !== initialDirectory.ino)
        throw new Error("Outbox reservations directory changed during scan");
    cache.cursor = cursor;
    return { cursor, active, retiredReservations: cache.retiredReservations };
}
async function scanAdmissionState(fs, dir, validateReservation) {
    for (let attempt = 0; attempt < 4; attempt += 1) {
        try {
            return await scanAdmissionStateOnce(fs, dir, validateReservation);
        }
        catch (error) {
            if (!(error instanceof Error) || error.message !== "Outbox reservations directory name set changed during scan" || attempt === 3)
                throw error;
        }
    }
    throw new Error("Outbox reservations directory changed during scan");
}
function assertNotRetired(state, reservation) {
    const retired = state.retiredReservations.get(reservation.reservationId);
    if (retired === undefined)
        return;
    if (retired.canonical !== canonicalStringify(reservation))
        throw new Error("Outbox admission retirement reservation collision");
    throw new Error("Outbox admission reservation is already retired");
}
export async function publishAdmissionRetirement(fs, dir, generation, reservation, validateReservation) {
    const file = join(dir, admissionRetirementName(generation));
    const expected = retirementValue(generation, reservation);
    const existing = await durableRetirement(fs, dir, generation, validateReservation);
    if (existing !== undefined) {
        if (!sameReservation(existing.reservation, reservation))
            throw new Error("Outbox admission retirement marker collision");
        return;
    }
    const temp = join(dir, `.admission.${generation.toString().padStart(16, "0")}.retired.tmp-${process.pid}-${randomBytes(16).toString("hex")}`);
    const handle = await fs.open(temp, "wx", 0o600);
    try {
        await handle.writeFile(canonicalStringify(expected), "utf8");
        await handle.sync();
    }
    catch (error) {
        await handle.close().catch(() => undefined);
        await fs.rm(temp, { force: true }).catch(() => undefined);
        throw error;
    }
    await handle.close();
    await fs.chmod(temp, 0o600);
    try {
        await fs.link(temp, file);
    }
    catch (error) {
        if (!errno(error, "EEXIST"))
            throw error;
    }
    await syncDirectory(fs, dir);
    const winner = await durableRetirement(fs, dir, generation, validateReservation);
    if (winner === undefined || !sameReservation(winner.reservation, reservation))
        throw new Error("Outbox admission retirement marker collision");
    await cleanupRetirementTemps(fs, dir, generation, validateReservation, true).catch(() => undefined);
    bumpGenerationHint(dir, generation + 1);
}
export async function activeAdmissionLocks(fs, dir, validateReservation) {
    const state = await scanAdmissionState(fs, dir, validateReservation);
    return state.active === undefined ? [] : [state.active];
}
export async function acquireAdmissionGeneration(input) {
    const delay = input.busyDelayMs ?? 5;
    const attempts = input.maxAttempts ?? 400;
    const now = input.now ?? Date.now;
    const deadline = input.busyDeadlineMs === undefined ? undefined : now() + input.busyDeadlineMs;
    for (let attempt = 0; attempt < attempts; attempt += 1) {
        const state = await scanAdmissionState(input.fs, input.dir, input.validateReservation);
        assertNotRetired(state, input.reservation);
        const generation = state.cursor;
        const file = join(input.dir, admissionLockName(generation));
        if (state.active === undefined) {
            try {
                await input.fs.link(input.reservationFile, file);
                await syncDirectory(input.fs, input.dir);
                const retired = await durableRetirement(input.fs, input.dir, generation, input.validateReservation);
                if (retired !== undefined) {
                    const current = input.validateReservation(await readPrivateJson(input.fs, file));
                    if (!sameReservation(current, input.reservation))
                        throw new Error("Outbox retired admission generation lock collision");
                    await durableRemove(input.fs, file, input.dir);
                    bumpGenerationHint(input.dir, generation + 1);
                    continue;
                }
                bumpGenerationHint(input.dir, generation);
                return { generation, file, reservation: input.reservation };
            }
            catch (error) {
                if (!errno(error, "EEXIST"))
                    throw error;
                continue;
            }
        }
        const existing = state.active.reservation;
        if (sameReservation(existing, input.reservation)) {
            await syncDirectory(input.fs, input.dir);
            const retired = await durableRetirement(input.fs, input.dir, state.active.generation, input.validateReservation);
            if (retired !== undefined) {
                await durableRemove(input.fs, state.active.file, input.dir);
                bumpGenerationHint(input.dir, state.active.generation + 1);
                continue;
            }
            bumpGenerationHint(input.dir, state.active.generation);
            return { generation: state.active.generation, file: state.active.file, reservation: input.reservation };
        }
        let completed = false;
        try {
            completed = await input.durableProof(existing);
        }
        catch { /* ambiguous durable state is never reclamation authority */ }
        if (completed) {
            await publishAdmissionRetirement(input.fs, input.dir, state.active.generation, existing, input.validateReservation);
            await durableRemove(input.fs, state.active.file, input.dir).catch(() => undefined);
            continue;
        }
        let abandoned = false;
        try {
            abandoned = input.abandoned === undefined ? false : await input.abandoned(existing);
        }
        catch { /* ambiguous liveness is never reclamation authority */ }
        if (abandoned) {
            await publishAdmissionRetirement(input.fs, input.dir, state.active.generation, existing, input.validateReservation);
            await durableRemove(input.fs, state.active.file, input.dir).catch(() => undefined);
            await durableRemove(input.fs, join(input.dir, `${existing.reservationId}.json`), input.dir).catch(() => undefined);
            continue;
        }
        if (deadline !== undefined && now() >= deadline)
            break;
        await new Promise((resolveWait) => setTimeout(resolveWait, delay));
    }
    throw new Error("Outbox admission is busy");
}
export async function retireOwnedAdmissionLock(input) {
    const state = await scanAdmissionState(input.fs, input.dir, input.validateReservation);
    const canonical = canonicalStringify(input.reservation);
    const retired = state.retiredReservations.get(input.reservation.reservationId);
    if (retired !== undefined) {
        if (retired.canonical !== canonical)
            throw new Error("Outbox admission retirement reservation collision");
        const marker = await durableRetirement(input.fs, input.dir, retired.generation, input.validateReservation);
        if (marker === undefined || !sameReservation(marker.reservation, input.reservation))
            throw new Error("Outbox admission retirement marker disappeared or changed");
        return;
    }
    if (state.active !== undefined && sameReservation(state.active.reservation, input.reservation)) {
        await publishAdmissionRetirement(input.fs, input.dir, state.active.generation, state.active.reservation, input.validateReservation);
        await durableRemove(input.fs, state.active.file, input.dir);
        return;
    }
    if (input.requireOwnership === true)
        throw new Error("Outbox admission lock ownership changed");
    await syncDirectory(input.fs, input.dir);
}
//# sourceMappingURL=reservation-protocol.js.map