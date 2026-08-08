#!/usr/bin/env node
import { readFile } from "node:fs/promises";
import { homedir } from "node:os";
import { pathToFileURL } from "node:url";
import { parseArgs } from "node:util";
import { containsSecret } from "./secret-scan.js";
import { loadConfig as loadRuntimeConfig } from "../config.js";
import { MemoryClientError } from "../clients/http.js";
import { ImportApprovalMismatchError, ImportInfrastructureError, ImportValidationError, applyHermesImport as applyImport, planHermesImport as planImport, } from "./import-hermes.js";
import { initializeDestination as initializeMemoryDestination, } from "./init.js";
import { AdminQdrantClient } from "./qdrant-admin.js";
import { memoryStatus as readMemoryStatus } from "./status.js";
const ADMIN_HOST_ENVIRONMENT = "PI_QDRANT_MEMORY_HOST";
const PLAN_ID_PATTERN = /^[a-f0-9]{64}$/;
const COLLECTION_PATTERN = /^[A-Za-z0-9_-]{1,255}$/;
const RAW_CONTROL_PATTERN = /[\u0000-\u001f\u007f-\u009f]/u;
const DISPLAY_REDACTED = "[redacted]";
const TOP_LEVEL_HELP = `Usage: pi-qdrant-memory <command> [options]

Commands:
  init             initialize the configured destination
  status           inspect configured administrative dependencies
  import-hermes    dry-run or apply an approved Hermes import

Run a command with --help for command-specific options.`;
const INIT_HELP = `Usage: pi-qdrant-memory init [--json]

Host rule: set PI_QDRANT_MEMORY_HOST explicitly to prime or pi.`;
const STATUS_HELP = `Usage: pi-qdrant-memory status [--json]

Host rule: set PI_QDRANT_MEMORY_HOST explicitly to prime or pi.`;
const IMPORT_HELP = `Usage:
  pi-qdrant-memory import-hermes --target-host prime|pi --dry-run [options]
  pi-qdrant-memory import-hermes --target-host prime|pi --approve <plan-id> [options]

Options:
  --source-url <url>
  --source-collection <collection>
  --source-model <model>
  --json
  --help`;
class CliInputError extends Error {
    constructor() {
        super("invalid arguments or configuration");
        this.name = "CliInputError";
    }
}
class CliConfigError extends Error {
    constructor() {
        super("invalid arguments or configuration");
        this.name = "CliConfigError";
    }
}
function sourceClientProjection(client) {
    return {
        collectionInfo: (collection, signal) => client.collectionInfo(collection, signal),
        scroll: (collection, offset, limit, signal) => client.scroll(collection, offset, limit, signal),
    };
}
function destinationClientProjection(client) {
    return {
        collectionInfo: (collection, signal) => client.collectionInfo(collection, signal),
        upsert: (collection, points, signal) => client.upsert(collection, points, signal),
    };
}
function createDefaultImportClients(config, sourceUrl, fetchImpl) {
    const common = {
        timeoutMs: config.retrieval.timeoutMs,
        ...(fetchImpl === undefined ? {} : { fetchImpl }),
    };
    const sourceClient = new AdminQdrantClient({
        ...common,
        baseUrl: sourceUrl,
        ...(config.admin.source.apiKey === undefined
            ? {}
            : { apiKey: config.admin.source.apiKey }),
    });
    const destinationClient = new AdminQdrantClient({
        ...common,
        baseUrl: config.qdrant.url,
        ...(config.admin.destinationApiKey === undefined
            ? {}
            : { apiKey: config.admin.destinationApiKey }),
    });
    return {
        source: sourceClientProjection(sourceClient),
        destination: destinationClientProjection(destinationClient),
    };
}
export function defaultCliDependencies(options = {}) {
    const env = options.env ?? process.env;
    const home = options.homeDir ?? homedir();
    const readTextFile = options.readTextFile ?? ((path) => readFile(path, "utf8"));
    const fetchImpl = options.fetchImpl;
    const configDependencies = {
        env,
        homeDir: home,
        ...(env.XDG_CONFIG_HOME === undefined || env.XDG_CONFIG_HOME === ""
            ? {}
            : { xdgConfigHome: env.XDG_CONFIG_HOME }),
        readTextFile,
    };
    return {
        env,
        loadConfig: (host) => loadRuntimeConfig(host, configDependencies),
        initialize: (config) => initializeMemoryDestination(config, {
            ...(fetchImpl === undefined ? {} : { fetchImpl }),
        }),
        status: (config) => readMemoryStatus(config, {
            ...(fetchImpl === undefined ? {} : { fetchImpl }),
        }),
        plan: planImport,
        apply: applyImport,
        createImportClients: (config, sourceUrl) => createDefaultImportClients(config, sourceUrl, fetchImpl),
        writeStdout: options.writeStdout ?? ((value) => process.stdout.write(value)),
        writeStderr: options.writeStderr ?? ((value) => process.stderr.write(value)),
    };
}
function parseSimpleCommand(args) {
    try {
        const parsed = parseArgs({
            args: [...args],
            strict: true,
            allowPositionals: false,
            options: {
                json: { type: "boolean" },
                help: { type: "boolean", short: "h" },
            },
        });
        return { json: parsed.values.json ?? false, help: parsed.values.help ?? false };
    }
    catch {
        throw new CliInputError();
    }
}
function parseImportCommand(args) {
    try {
        const parsed = parseArgs({
            args: [...args],
            strict: true,
            allowPositionals: false,
            options: {
                json: { type: "boolean" },
                help: { type: "boolean", short: "h" },
                "target-host": { type: "string" },
                "dry-run": { type: "boolean" },
                approve: { type: "string" },
                "source-url": { type: "string" },
                "source-collection": { type: "string" },
                "source-model": { type: "string" },
            },
        });
        return {
            json: parsed.values.json ?? false,
            help: parsed.values.help ?? false,
            dryRun: parsed.values["dry-run"] ?? false,
            ...(parsed.values["target-host"] === undefined
                ? {}
                : { targetHost: parsed.values["target-host"] }),
            ...(parsed.values.approve === undefined
                ? {}
                : { approve: parsed.values.approve }),
            ...(parsed.values["source-url"] === undefined
                ? {}
                : { sourceUrl: parsed.values["source-url"] }),
            ...(parsed.values["source-collection"] === undefined
                ? {}
                : { sourceCollection: parsed.values["source-collection"] }),
            ...(parsed.values["source-model"] === undefined
                ? {}
                : { sourceModel: parsed.values["source-model"] }),
        };
    }
    catch {
        throw new CliInputError();
    }
}
function explicitAdministrativeHost(env) {
    const value = env[ADMIN_HOST_ENVIRONMENT];
    if (value !== "prime" && value !== "pi")
        throw new CliInputError();
    return value;
}
function targetHost(value) {
    if (value !== "prime" && value !== "pi")
        throw new CliInputError();
    return value;
}
function collectionIdentifier(value) {
    if (!COLLECTION_PATTERN.test(value) || containsSecret(value))
        throw new CliInputError();
    return value;
}
function modelIdentifier(value) {
    if (value.trim() !== value ||
        value.length === 0 ||
        value.length > 256 ||
        /[\u0000-\u001f\u007f]/.test(value) ||
        containsSecret(value)) {
        throw new CliInputError();
    }
    return value;
}
function sourceUrl(value) {
    if (value.trim() !== value ||
        value.length === 0 ||
        RAW_CONTROL_PATTERN.test(value) ||
        containsSecret(value)) {
        throw new CliInputError();
    }
    let parsed;
    try {
        parsed = new URL(value);
    }
    catch {
        throw new CliInputError();
    }
    if ((parsed.protocol !== "http:" && parsed.protocol !== "https:") ||
        parsed.username !== "" ||
        parsed.password !== "" ||
        value.includes("?") ||
        value.includes("#") ||
        parsed.search !== "" ||
        parsed.hash !== "") {
        throw new CliInputError();
    }
    const normalized = parsed.href.replace(/\/+$/, "");
    if (normalized.length === 0 || containsSecret(normalized))
        throw new CliInputError();
    return normalized;
}
function isSystemIoError(error) {
    return (typeof error === "object" &&
        error !== null &&
        "code" in error &&
        typeof error.code === "string");
}
async function configured(host, deps) {
    try {
        return await deps.loadConfig(host);
    }
    catch (error) {
        if (isSystemIoError(error)) {
            throw new ImportInfrastructureError("configuration unavailable");
        }
        throw new CliConfigError();
    }
}
function escapeDisplayString(value) {
    let escaped = "";
    for (const character of value) {
        const code = character.codePointAt(0);
        if (character === "\\") {
            escaped += "\\\\";
        }
        else if (code !== undefined &&
            ((code >= 0x00 && code <= 0x1f) || (code >= 0x7f && code <= 0x9f))) {
            escaped += `\\u${code.toString(16).toUpperCase().padStart(4, "0")}`;
        }
        else {
            escaped += character;
        }
    }
    return escaped;
}
function displayString(value) {
    return containsSecret(value) ? DISPLAY_REDACTED : escapeDisplayString(value);
}
function displayOptionalString(value) {
    return value === null ? null : displayString(value);
}
function displayCountRecord(counts) {
    return Object.fromEntries(Object.entries(counts).map(([key, count]) => [escapeDisplayString(key), count]));
}
function safeInitialize(result) {
    return {
        created: result.created,
        collection: displayString(result.collection),
        dimension: result.dimension,
        distance: displayString(result.distance),
    };
}
function safeStatus(status) {
    const collection = (value) => ({
        collection: displayString(value.collection),
        exists: value.exists,
        dimension: value.dimension,
        distance: displayOptionalString(value.distance),
        pointCount: value.pointCount,
        healthy: value.healthy,
        keyConfigured: value.keyConfigured,
    });
    return {
        destinationExists: status.destinationExists,
        destination: collection(status.destination),
        source: collection(status.source),
        embeddings: {
            model: displayString(status.embeddings.model),
            dimension: status.embeddings.dimension,
            healthy: status.embeddings.healthy,
            keyConfigured: status.embeddings.keyConfigured,
        },
        qdrant: {
            healthy: status.qdrant.healthy,
            destinationHealthy: status.qdrant.destinationHealthy,
            sourceHealthy: status.qdrant.sourceHealthy,
        },
    };
}
function safePlan(plan) {
    return {
        planId: displayString(plan.planId),
        transformVersion: plan.transformVersion,
        targetHost: displayString(plan.targetHost),
        sourceCollection: displayString(plan.sourceCollection),
        destinationCollection: displayString(plan.destinationCollection),
        rejected: displayCountRecord(plan.rejected),
        report: {
            accepted: plan.report.accepted,
            rejected: plan.report.rejected,
            bySourceType: displayCountRecord(plan.report.bySourceType),
            byProjectLabel: displayCountRecord(plan.report.byProjectLabel),
        },
    };
}
function output(deps, json, command, value) {
    const projection = { command, ...value };
    deps.writeStdout(json
        ? `${JSON.stringify(projection)}\n`
        : `${command}\n${JSON.stringify(value, null, 2)}\n`);
}
function help(deps, json, usage) {
    deps.writeStdout(json ? `${JSON.stringify({ usage })}\n` : `${usage}\n`);
}
function requestedJson(args) {
    return args.some((value) => value === "--json");
}
function topLevelHelpRequest(args) {
    let helpRequested = false;
    for (const value of args) {
        if (value === "--help" || value === "-h") {
            helpRequested = true;
        }
        else if (value !== "--json") {
            return false;
        }
    }
    return helpRequested;
}
function failure(deps, json, message) {
    deps.writeStderr(json ? `${JSON.stringify({ error: message })}\n` : `${message}\n`);
}
function exitForError(error) {
    if (error instanceof CliInputError ||
        error instanceof CliConfigError ||
        error instanceof ImportValidationError ||
        (error instanceof MemoryClientError && error.category === "configuration")) {
        return 2;
    }
    if (error instanceof ImportInfrastructureError)
        return 1;
    return 1;
}
async function runSimple(command, args, deps) {
    const parsed = parseSimpleCommand(args);
    if (parsed.help) {
        help(deps, parsed.json, command === "init" ? INIT_HELP : STATUS_HELP);
        return 0;
    }
    const host = explicitAdministrativeHost(deps.env);
    const config = await configured(host, deps);
    if (command === "init") {
        const result = await deps.initialize(config);
        output(deps, parsed.json, "init", {
            host: displayString(host),
            ...safeInitialize(result),
        });
    }
    else {
        const result = await deps.status(config);
        output(deps, parsed.json, "status", {
            host: displayString(host),
            ...safeStatus(result),
        });
    }
    return 0;
}
async function runImport(args, deps) {
    const parsed = parseImportCommand(args);
    if (parsed.help) {
        help(deps, parsed.json, IMPORT_HELP);
        return 0;
    }
    const host = targetHost(parsed.targetHost);
    const hasApproval = parsed.approve !== undefined;
    if (parsed.dryRun === hasApproval)
        throw new CliInputError();
    if (hasApproval && !PLAN_ID_PATTERN.test(parsed.approve)) {
        throw new CliInputError();
    }
    const sourceUrlOverride = parsed.sourceUrl === undefined
        ? undefined
        : sourceUrl(parsed.sourceUrl);
    const sourceCollectionOverride = parsed.sourceCollection === undefined
        ? undefined
        : collectionIdentifier(parsed.sourceCollection);
    const sourceModelOverride = parsed.sourceModel === undefined
        ? undefined
        : modelIdentifier(parsed.sourceModel);
    const config = await configured(host, deps);
    const resolvedSourceUrl = sourceUrl(sourceUrlOverride ?? config.admin.source.url);
    const resolvedSourceCollection = collectionIdentifier(sourceCollectionOverride ?? config.admin.source.collection);
    const destinationCollection = collectionIdentifier(config.qdrant.collection);
    const configuredModel = modelIdentifier(config.embeddings.model);
    const clients = deps.createImportClients(config, resolvedSourceUrl);
    const importOptions = {
        sourceIdentity: resolvedSourceUrl,
        sourceCollection: resolvedSourceCollection,
        destinationCollection,
        targetHost: host,
        configuredModel,
        configuredDimension: config.embeddings.dimension,
        ...(sourceModelOverride === undefined
            ? {}
            : { declaredSourceModel: sourceModelOverride }),
    };
    if (parsed.dryRun) {
        const plan = await deps.plan(importOptions, clients);
        output(deps, parsed.json, "import-hermes", { mode: "dry-run", ...safePlan(plan) });
        return 0;
    }
    const result = await deps.apply({ ...importOptions, approvedPlanId: parsed.approve }, clients);
    output(deps, parsed.json, "import-hermes", {
        mode: "apply",
        targetHost: displayString(host),
        sourceCollection: displayString(resolvedSourceCollection),
        destinationCollection: displayString(destinationCollection),
        planId: displayString(result.planId),
        upserted: result.upserted,
        batches: result.batches,
    });
    return 0;
}
export async function main(args, deps = defaultCliDependencies()) {
    const json = requestedJson(args);
    try {
        if (topLevelHelpRequest(args)) {
            help(deps, json, TOP_LEVEL_HELP);
            return 0;
        }
        const command = args[0];
        const commandArgs = args.slice(1);
        if (command === "init" || command === "status") {
            return await runSimple(command, commandArgs, deps);
        }
        if (command === "import-hermes") {
            return await runImport(commandArgs, deps);
        }
        throw new CliInputError();
    }
    catch (error) {
        if (error instanceof ImportApprovalMismatchError) {
            failure(deps, json, "source changed; run dry-run again");
            return 2;
        }
        const exit = exitForError(error);
        failure(deps, json, exit === 2 ? "invalid arguments or configuration" : "operation failed");
        return exit;
    }
}
if (import.meta.url === pathToFileURL(process.argv[1] ?? "").href) {
    process.exitCode = await main(process.argv.slice(2), defaultCliDependencies());
}
//# sourceMappingURL=cli.js.map