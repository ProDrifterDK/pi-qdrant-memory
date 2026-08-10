#!/usr/bin/env node
import type { HostId, RuntimeConfig } from "../types.js";
import { type InitializeDestinationResult } from "./init.js";
import { type MemoryStatus } from "./status.js";
export interface CliDependencies {
    env: Record<string, string | undefined>;
    loadConfig(host: HostId): Promise<RuntimeConfig>;
    initialize(config: RuntimeConfig): Promise<InitializeDestinationResult>;
    status(config: RuntimeConfig): Promise<MemoryStatus>;
    writeStdout(value: string): void;
    writeStderr(value: string): void;
}
export interface DefaultCliDependencyOptions {
    env?: Record<string, string | undefined>;
    homeDir?: string;
    readTextFile?(path: string): Promise<string>;
    writeStdout?(value: string): void;
    writeStderr?(value: string): void;
}
export declare function defaultCliDependencies(options?: DefaultCliDependencyOptions): CliDependencies;
export declare function main(args: readonly string[], deps?: CliDependencies): Promise<number>;
