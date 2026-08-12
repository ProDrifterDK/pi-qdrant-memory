import type { AuthorizedDestination, RuntimeConfig } from "../types.js";
export interface EmbeddingsClientOptions {
    baseUrl: string;
    model: string;
    dimension: number;
    queryPrefix: string;
    apiKey?: string;
    timeoutMs: number;
    fetchImpl?: typeof fetch;
}
export interface DocumentEmbeddingClient {
    /** Canonical endpoint pinned at client construction, never caller options. */
    readonly endpoint: string;
    embedDocument(input: {
        model: string;
        text: string;
        signal?: AbortSignal;
    }): Promise<readonly number[]>;
}
export declare class EmbeddingsClient implements DocumentEmbeddingClient {
    #private;
    readonly endpoint: string;
    constructor(options: EmbeddingsClientOptions);
    /** Real-brand check: only genuine EmbeddingsClient instances pass; forged prototypes and structural objects fail. */
    static isValid(value: unknown): value is EmbeddingsClient;
    /** Production-bound: exact brand and NO injected transport (fetchImpl test seams fail); flag is private state. */
    static isProductionBound(value: unknown): value is EmbeddingsClient;
    /** Opaque per-instance frozen transport identity (empty object; never options). */
    get transport(): object;
    private request;
    embedQuery(query: string, signal?: AbortSignal): Promise<number[]>;
    /** Task 7 document embeddings deliberately omit queryPrefix and accept BGE-M3 only. */
    embedDocument(input: {
        model: string;
        text: string;
        signal?: AbortSignal;
    }): Promise<readonly number[]>;
}
/**
 * Nominal endpoint/client pairing; only the REAL EmbeddingsClient brand may be
 * bound — arbitrary structural objects (including real-write/fake-read mixes)
 * fail closed. The emitted-JS constructor requires the module issuer.
 */
export declare class ValidatedEmbeddingDocumentClient {
    #private;
    readonly endpoint: string;
    /** Public constructor is unusable without the module-private issuer symbol. */
    constructor(endpoint: string, client: DocumentEmbeddingClient, issuer: symbol);
    /** Exposed validating operation only; issuance stays module-private. */
    static isValid(value: unknown): value is ValidatedEmbeddingDocumentClient;
    embedDocument(input: {
        model: string;
        text: string;
        signal?: AbortSignal;
    }): Promise<readonly number[]>;
    static bind(input: {
        endpoint: string;
        client: DocumentEmbeddingClient;
    }): ValidatedEmbeddingDocumentClient;
}
/** Explicit validated seam used by production clients and endpoint-pinned fakes. */
export declare function bindEmbeddingDocumentClient(input: {
    endpoint: string;
    client: DocumentEmbeddingClient;
}): ValidatedEmbeddingDocumentClient;
export interface EmbeddingDestinationFactoryInput {
    endpoint: string;
    destination: AuthorizedDestination;
    client: ValidatedEmbeddingDocumentClient;
    egressMode: RuntimeConfig["privacy"]["egressMode"];
    coordinationPolicyHash: string;
    coordinationPolicyEpoch: number;
    nodeId?: string;
}
export interface EmbeddingDestinationFactory {
    bind(destination: AuthorizedDestination): BoundEmbeddingDestination;
}
/**
 * Nominal, frozen, privately branded bound embedding destination. It snapshots
 * endpoint/destination/coordination identity and binds embed once; forged
 * prototypes and monkeypatched statics fail the brand check.
 */
export declare class BoundEmbeddingDestination {
    #private;
    readonly endpoint: string;
    readonly destination: AuthorizedDestination;
    readonly coordination: {
        readonly policyHash: string;
        readonly policyEpoch: number;
    };
    /** Public constructor is unusable without the module-private issuer symbol. */
    constructor(input: {
        endpoint: string;
        destination: AuthorizedDestination;
        coordination: {
            policyHash: string;
            policyEpoch: number;
        };
        embed: (input: {
            model: string;
            text: string;
            signal?: AbortSignal;
        }) => Promise<readonly number[]>;
    }, issuer: symbol);
    /** Exposed validating operation only; issuance stays module-private. */
    static isValid(value: unknown): value is BoundEmbeddingDestination;
    embed(input: {
        model: string;
        text: string;
        signal?: AbortSignal;
    }): Promise<readonly number[]>;
}
/** Captures immutable endpoint/client/destination snapshots; a later caller mutation cannot retarget egress. */
export declare function createEmbeddingDestinationFactory(input: EmbeddingDestinationFactoryInput): EmbeddingDestinationFactory;
export declare function bindEmbeddingDestination(factory: EmbeddingDestinationFactory, destination: AuthorizedDestination): BoundEmbeddingDestination;
