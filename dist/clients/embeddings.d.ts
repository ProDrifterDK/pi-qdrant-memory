import type { AuthorizedDestination, RuntimeConfig } from "../types.js";
import type { BoundEmbeddingDestination } from "../outbox/delivery.js";
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
    readonly endpoint: string;
    private readonly options;
    constructor(options: EmbeddingsClientOptions);
    private request;
    embedQuery(query: string, signal?: AbortSignal): Promise<number[]>;
    /** Task 7 document embeddings deliberately omit queryPrefix and accept BGE-M3 only. */
    embedDocument(input: {
        model: string;
        text: string;
        signal?: AbortSignal;
    }): Promise<readonly number[]>;
}
/** Nominal endpoint/client pairing; raw structural embedding clients cannot enter a factory. */
export declare class ValidatedEmbeddingDocumentClient {
    #private;
    readonly endpoint: string;
    private constructor();
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
    nodeId?: string;
}
export interface EmbeddingDestinationFactory {
    bind(destination: AuthorizedDestination): BoundEmbeddingDestination;
}
/** Captures immutable endpoint/client/destination snapshots; a later caller mutation cannot retarget egress. */
export declare function createEmbeddingDestinationFactory(input: EmbeddingDestinationFactoryInput): EmbeddingDestinationFactory;
export declare function bindEmbeddingDestination(factory: EmbeddingDestinationFactory, destination: AuthorizedDestination): BoundEmbeddingDestination;
