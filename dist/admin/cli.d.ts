#!/usr/bin/env node
import type { HostId, RuntimeConfig } from "../types.js";
import { type ImportClients, type ImportOptions } from "./import-hermes.js";
import type { ImportPlan } from "./import-plan.js";
import { type InitializeDestinationResult } from "./init.js";
import { type MemoryStatus } from "./status.js";
export interface CliDependencies {
    env: Record<string, string | undefined>;
    loadConfig(host: HostId): Promise<RuntimeConfig>;
    initialize(config: RuntimeConfig): Promise<InitializeDestinationResult>;
    status(config: RuntimeConfig): Promise<MemoryStatus>;
    plan(options: ImportOptions, clients: ImportClients): Promise<ImportPlan>;
    apply(options: ImportOptions & {
        approvedPlanId: string;
    }, clients: ImportClients): Promise<{
        planId: string;
        upserted: number;
        batches: number;
    }>;
    createImportClients(config: RuntimeConfig, sourceUrl: string): ImportClients;
    writeStdout(value: string): void;
    writeStderr(value: string): void;
}
export interface DefaultCliDependencyOptions {
    env?: Record<string, string | undefined>;
    homeDir?: string;
    readTextFile?(path: string): Promise<string>;
    fetchImpl?: typeof fetch;
    writeStdout?(value: string): void;
    writeStderr?(value: string): void;
}
export declare function defaultCliDependencies(options?: DefaultCliDependencyOptions): CliDependencies;
export declare function main(args: readonly string[], deps?: CliDependencies): Promise<number>;
