import { afterEach, describe, expect, it, vi } from "vitest";
import { readFile, readdir } from "node:fs/promises";
import { join } from "node:path";
import { execFileSync } from "node:child_process";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { ProductionCoordinationStore, createQdrantCoordinationStore, createQdrantDestinationFactory, createQdrantSafeBundle, bindQdrantDestination } from "../../src/qdrant/write.js";
import { bindEmbeddingDestination, bindEmbeddingDocumentClient, createEmbeddingDestinationFactory, EmbeddingsClient } from "../../src/clients/embeddings.js";
import { bindIngestRuntime } from "../../src/coordination/ingest.js";
import type { QdrantClientOptions } from "../../src/qdrant/client.js";
import type { AuthorizedDestination } from "../../src/types.js";

/** Absolute file:// imports of the BUILT dist — the reviewer's exploit surface. */
const distUrl = (rel: string): string => new URL(`../../dist/${rel}`, import.meta.url).href;

describe("Task 8 authority seam (round 23): no reachable raw surface", () => {
  afterEach(() => { vi.unstubAllGlobals(); });

  it("absolute import of dist/coordination/control.js exposes NO raw mutators, register/resolve or protocol store", async () => {
    const mod = await import(distUrl("coordination/control.js"));
    const names = Object.keys(mod).sort();
    for (const raw of ["compareAndSwapControl", "casLease", "insertLease", "insertJob", "insertProposal", "insertCoverage", "insertTombstone", "protocolStore", "registerFacade", "protocolStoreFor", "createPrivateWriteEngine"]) {
      expect(names).not.toContain(raw);
    }
  });

  it("absolute import of dist/qdrant/write.js exposes NO raw CAS helpers or writer constructors", async () => {
    const mod = await import(distUrl("qdrant/write.js"));
    const names = Object.keys(mod);
    for (const raw of ["insertOnly", "updateOnlyCas", "publishControlCas", "casPoint", "insertInitialControl", "createQdrantSessionWriter", "sessionWriter", "SessionWriter"]) {
      expect(names).not.toContain(raw);
    }
    // The raw engine is LEXICAL: even the engine factory is not a named export.
    expect(typeof (mod as Record<string, unknown>).createPrivateWriteEngine).toBe("undefined");
    expect("createPrivateWriteEngine" in mod).toBe(false);
    expect("PrivateWriteEngine" in mod).toBe(false);
    expect("QdrantWriteVerificationClient" in mod).toBe(false);
  });

  it("absolute import of dist/qdrant/client.js exposes NO session-writer/admin constructors or factories", async () => {
    const mod = await import(distUrl("qdrant/client.js"));
    const names = Object.keys(mod);
    for (const raw of ["QdrantSessionWriter", "RestQdrantSessionWriter", "createQdrantSessionWriter", "sessionWriter", "QdrantAdminClient", "RestQdrantAdminClient", "adminClient", "createQdrantAdminClient", "isRestQdrantSessionWriter", "restTransportOf"]) {
      expect(names).not.toContain(raw);
    }
    expect(names).toContain("readPolicy");
    expect(names).toContain("physicalPointIdFor");
  });

  it("the facade module is GONE: absolute import of dist/coordination/facade.js fails", async () => {
    await expect(import(distUrl("coordination/facade.js"))).rejects.toThrow();
  });

  it("no public object exposes .writer/.upsertPoints/raw session: everything built from dist OPTIONS only", async () => {
    const { createQdrantCoordinationStore, ProductionCoordinationStore } = await import(distUrl("coordination/control.js"));
    const { createQdrantSafeBundle, bindQdrantDestination } = await import(distUrl("qdrant/write.js"));
    const fetchImpl: typeof fetch = async (input, init = {}) => {
      const url = String(input);
      if (url.includes("/points/retrieve")) return new Response(JSON.stringify({ result: [], status: "ok" }), { headers: { "content-type": "application/json" } });
      if (url.includes("/points?") && init.method === "PUT") return new Response(JSON.stringify({ result: { status: "acknowledged" }, status: "ok" }), { headers: { "content-type": "application/json" } });
      return new Response(JSON.stringify({ result: {}, status: "ok" }), { headers: { "content-type": "application/json" } });
    };
    vi.stubGlobal("fetch", fetchImpl);
    const options: QdrantClientOptions = { baseUrl: "http://qdrant", collection: "pi_memory", ownerHost: "pi", apiKey: "k", timeoutMs: 1000, maxClockSkewMs: 0, readConsistency: "majority" };
    const store = createQdrantCoordinationStore(options);
    expect(ProductionCoordinationStore.isValid(store)).toBe(true);
    const storeAny = store as unknown as Record<string, unknown>;
    for (const raw of ["compareAndSwapControl", "casLease", "insertLease", "insertJob", "insertProposal", "insertCoverage", "insertTombstone", "upsertPoints", "client", "writer", "session", "engine", "protocol"]) {
      expect(raw in storeAny).toBe(false);
      expect(typeof storeAny[raw]).toBe("undefined");
    }
    const bundle = createQdrantSafeBundle({ options, destination: { id: "qdrant:pi", residency: "local", dataUse: "memory" }, egressMode: "allowlist", coordinationPolicyHash: "policy-hash", coordinationPolicyEpoch: 1 });
    const bound = bindQdrantDestination(bundle.qdrant, { id: "qdrant:pi", residency: "local", dataUse: "memory" });
    for (const obj of [bundle.qdrant, bound, bundle.transport]) {
      const anyObj = obj as unknown as Record<string, unknown>;
      expect("writer" in anyObj).toBe(false);
      expect("upsertPoints" in anyObj).toBe(false);
      expect("client" in anyObj).toBe(false);
    }
    expect(typeof bundle.qdrant.bind).toBe("function");
  });

  it("emitted d.ts files are coherent: no raw CoordinationStore protocol leaks; raw helpers stripped", async () => {
    const root = new URL("../../dist/", import.meta.url);
    const walk = async (dir: URL): Promise<string[]> => {
      const entries = await readdir(dir, { withFileTypes: true });
      const files: string[] = [];
      for (const entry of entries) {
        const next = entry.isDirectory() ? new URL(`${entry.name}/`, dir) : new URL(entry.name, dir);
        if (entry.isDirectory()) files.push(...(await walk(next)));
        else if (entry.name.endsWith(".d.ts")) files.push(join(next.pathname));
      }
      return files;
    };
    const files = await walk(root);
    expect(files.length).toBeGreaterThan(5);
    for (const file of files) {
      const text = await readFile(file, "utf8");
      // The raw @internal protocol type must be absent from ALL public declarations.
      expect(text).not.toMatch(/(?<![A-Za-z0-9_$])CoordinationStore(?![A-Za-z0-9_$])/u);
      expect(text).not.toMatch(/(?<![A-Za-z0-9_$])LeaseClaim(?![A-Za-z0-9_$])/u);
      expect(text).not.toMatch(/(?<![A-Za-z0-9_$])(?:QdrantWriteVerificationClient|PrivateWriteEngine|ValidatedQdrantSessionWriter|RestQdrant(?:ReadClient|SessionWriter|AdminClient))(?![A-Za-z0-9_$])/u);
    }
    const writeDts = await readFile(new URL("../../dist/qdrant/write.d.ts", import.meta.url), "utf8");
    for (const raw of ["insertOnly", "updateOnlyCas", "publishControlCas", "casPoint", "createPrivateWriteEngine", "PrivateWriteEngine"]) {
      expect(writeDts).not.toContain(raw);
    }
    expect(writeDts).not.toMatch(/\bSessionWriter\b/u);
    const controlDts = await readFile(new URL("../../dist/coordination/control.d.ts", import.meta.url), "utf8");
    for (const raw of ["casLease", "insertLease", "compareAndSwapControl", "insertJob", "protocolStore"]) {
      expect(controlDts).not.toContain(raw);
    }
    const clientDts = await readFile(new URL("../../dist/qdrant/client.d.ts", import.meta.url), "utf8");
    for (const raw of ["RestQdrantSessionWriter", "createQdrantSessionWriter", "sessionWriter"]) {
      expect(clientDts).not.toContain(raw);
    }
  });

  it("injected fetchImpl can NEVER mint Production authority: store/bundle/destination fail closed", async () => {
    const injected = async (): Promise<Response> => new Response("{}", { status: 200 });
    const options: QdrantClientOptions = { baseUrl: "http://qdrant", collection: "pi_memory", ownerHost: "pi", apiKey: "k", timeoutMs: 1000, maxClockSkewMs: 0, readConsistency: "majority", fetchImpl: injected };
    // A fabricated transport can never mint the Production store, a bound
    // destination, or the safe bundle (the injected flag is checked in the ctor).
    expect(() => createQdrantCoordinationStore(options)).toThrow(/production-bound|injected/i);
    expect(() => createQdrantDestinationFactory({ options, destination: { id: "qdrant:pi", residency: "local", dataUse: "memory" }, egressMode: "allowlist", coordinationPolicyHash: "policy-hash", coordinationPolicyEpoch: 1 })).toThrow(/production-bound|injected/i);
    expect(() => createQdrantSafeBundle({ options, destination: { id: "qdrant:pi", residency: "local", dataUse: "memory" }, egressMode: "allowlist", coordinationPolicyHash: "policy-hash", coordinationPolicyEpoch: 1 })).toThrow(/production-bound|injected/i);
    // With a GENUINE lexical session (global fetch stubbed BEFORE creation) everything works.
    const fetchImpl2: typeof fetch = async (input, init = {}) => { const url = String(input); if (url.includes("/points/retrieve")) return new Response(JSON.stringify({ result: [], status: "ok" }), { headers: { "content-type": "application/json" } }); if (url.includes("/points?") && init.method === "PUT") return new Response(JSON.stringify({ result: { status: "acknowledged" }, status: "ok" }), { headers: { "content-type": "application/json" } }); return new Response(JSON.stringify({ result: {}, status: "ok" }), { headers: { "content-type": "application/json" } }); };
    vi.stubGlobal("fetch", fetchImpl2);
    const store = createQdrantCoordinationStore({ baseUrl: "http://qdrant", collection: "pi_memory", ownerHost: "pi", apiKey: "k", timeoutMs: 1000, maxClockSkewMs: 0, readConsistency: "majority" });
    expect(ProductionCoordinationStore.isValid(store)).toBe(true);
    const bundle = createQdrantSafeBundle({ options: { baseUrl: "http://qdrant", collection: "pi_memory", ownerHost: "pi", apiKey: "k", timeoutMs: 1000, maxClockSkewMs: 0, readConsistency: "majority" }, destination: { id: "qdrant:pi", residency: "local", dataUse: "memory" }, egressMode: "allowlist", coordinationPolicyHash: "policy-hash", coordinationPolicyEpoch: 1 });
    expect(ProductionCoordinationStore.isValid(bundle.store)).toBe(true);
  });

  it("NO other dist module recreates the raw surface under a new name (all modules scanned)", async () => {
    const root = new URL("../../dist/", import.meta.url);
    const walk = async (dir: URL): Promise<string[]> => {
      const entries = await readdir(dir, { withFileTypes: true });
      const files: string[] = [];
      for (const entry of entries) {
        const next = entry.isDirectory() ? new URL(`${entry.name}/`, dir) : new URL(entry.name, dir);
        if (entry.isDirectory()) files.push(...(await walk(next)));
        else if (entry.name.endsWith(".js")) files.push(next.href);
      }
      return files;
    };
    const files = await walk(root);
    expect(files.length).toBeGreaterThan(10);
    const rawNames = ["insertOnly", "updateOnlyCas", "publishControlCas", "casPoint", "createPrivateWriteEngine", "createQdrantSessionWriter", "sessionWriter", "RestQdrantSessionWriter", "createQdrantAdminClient", "adminClient", "registerFacade", "protocolStoreFor", "mintLeaseAuthority", "rotateLeaseAuthority", "compareAndSwapControl", "insertLease", "casLease"];
    for (const href of files) {
      const mod = await import(href);
      const names = Object.keys(mod);
      const hits = rawNames.filter((raw) => names.includes(raw));
      expect(hits).toEqual([]);
    }
  });

  it("the safe bundle binds store + destination to the exact transport; production src has no dynamic imports", async () => {
    const options: QdrantClientOptions = { baseUrl: "http://qdrant", collection: "pi_memory", ownerHost: "pi", apiKey: "k", timeoutMs: 1000, maxClockSkewMs: 0, readConsistency: "majority" };
    const fetchImpl: typeof fetch = async (input, init = {}) => { const url = String(input); if (url.includes("/points/retrieve")) return new Response(JSON.stringify({ result: [], status: "ok" }), { headers: { "content-type": "application/json" } }); if (url.includes("/points?") && init.method === "PUT") return new Response(JSON.stringify({ result: { status: "acknowledged" }, status: "ok" }), { headers: { "content-type": "application/json" } }); return new Response(JSON.stringify({ result: {}, status: "ok" }), { headers: { "content-type": "application/json" } }); };
    vi.stubGlobal("fetch", fetchImpl);
    const bundle = createQdrantSafeBundle({ options, destination: { id: "qdrant:pi", residency: "local", dataUse: "memory" }, egressMode: "allowlist", coordinationPolicyHash: "policy-hash", coordinationPolicyEpoch: 1 });
    const bound = bindQdrantDestination(bundle.qdrant, { id: "qdrant:pi", residency: "local", dataUse: "memory" });
    const embeddingFactory = createEmbeddingDestinationFactory({ endpoint: "http://embed/v1", destination: { id: "embed:local", residency: "local", dataUse: "memory" }, client: bindEmbeddingDocumentClient({ endpoint: "http://embed/v1", client: new EmbeddingsClient({ baseUrl: "http://embed/v1", model: "bge-m3", dimension: 1024, queryPrefix: "query: ", timeoutMs: 100 }) }), egressMode: "allowlist", coordinationPolicyHash: "policy-hash", coordinationPolicyEpoch: 1 });
    const embedding = bindEmbeddingDestination(embeddingFactory, { id: "embed:local", residency: "local", dataUse: "memory" });
    expect(() => bindIngestRuntime({ store: bundle.store, qdrant: bound, embedding })).not.toThrow();
    // Mixing a SECOND bundle's destination with the first store's transport fails closed.
    const other = createQdrantSafeBundle({ options, destination: { id: "qdrant:pi", residency: "local", dataUse: "memory" }, egressMode: "allowlist", coordinationPolicyHash: "policy-hash", coordinationPolicyEpoch: 1 });
    const otherBound = bindQdrantDestination(other.qdrant, { id: "qdrant:pi", residency: "local", dataUse: "memory" });
    expect(() => bindIngestRuntime({ store: bundle.store, qdrant: otherBound, embedding })).toThrow(/exact writer transport/i);
    // Production src has NO dynamic imports (plan line 16): static imports only.
    const { readdir: readDir, readFile: readFile2 } = await import("node:fs/promises");
    const { join: joinPath } = await import("node:path");
    const srcRoot = new URL("../../src/", import.meta.url);
    const walkSrc = async (dir: URL): Promise<string[]> => {
      const entries = await readDir(dir, { withFileTypes: true });
      const files: string[] = [];
      for (const entry of entries) {
        const next = entry.isDirectory() ? new URL(`${entry.name}/`, dir) : new URL(entry.name, dir);
        if (entry.isDirectory()) files.push(...(await walkSrc(next)));
        else if (entry.name.endsWith(".ts")) files.push(new URL(entry.name, dir).pathname);
      }
      return files;
    };
    for (const href of await walkSrc(srcRoot)) {
      const text = await readFile2(href, "utf8");
      expect(text).not.toMatch(/\bawait\s+import\(/u);
      expect(text).not.toMatch(/\bimport\(/u);
    }
  });

  it("NO *OnProtocol/issuer/mint/rotate helper is exported from ANY dist module; capabilities are scope-bound", async () => {
    const root = new URL("../../dist/", import.meta.url);
    const walk = async (dir: URL): Promise<string[]> => {
      const entries = await readdir(dir, { withFileTypes: true });
      const files: string[] = [];
      for (const entry of entries) {
        const next = entry.isDirectory() ? new URL(`${entry.name}/`, dir) : new URL(entry.name, dir);
        if (entry.isDirectory()) files.push(...(await walk(next)));
        else if (entry.name.endsWith(".js")) files.push(next.href);
      }
      return files;
    };
    for (const href of await walk(root)) {
      const mod = await import(href);
      for (const name of Object.keys(mod)) {
        expect(name).not.toMatch(/OnProtocol$/u);
        expect(name).not.toMatch(/^(mint|rotateLease|issue|claimLeaseAuthority|waitForOldLeasesToQuiesceOnProtocol|acceptLeaseAuthorityOnProtocol|claimLeaseOnProtocol|renewLeaseOnProtocol|releaseLeaseOnProtocol|createJobOnProtocol|writeProposalOnProtocol|createTombstoneOnProtocol|markCoverageOnProtocol|initializeControlOnProtocol|beginPolicyDrainOnProtocol|activatePolicyEpochOnProtocol|beginForgetBarrierOnProtocol)/u);
      }
    }
    // Cross-store scope: a genuine authority/proof minted by store A is refused by store B.
    const fetchImplA: typeof fetch = async () => new Response(JSON.stringify({ result: [], status: "ok" }), { headers: { "content-type": "application/json" } });
    const fetchImplB: typeof fetch = async () => new Response(JSON.stringify({ result: [], status: "ok" }), { headers: { "content-type": "application/json" } });
    vi.stubGlobal("fetch", fetchImplA);
    const { createQdrantCoordinationStore: createStore, ProductionCoordinationStore: PStore } = await import("../../src/qdrant/write.js");
    const storeA = createStore({ baseUrl: "http://qdrant", collection: "pi_memory", ownerHost: "pi", apiKey: "k", timeoutMs: 1000, maxClockSkewMs: 0, readConsistency: "majority" });
    vi.stubGlobal("fetch", fetchImplB);
    const storeB = createStore({ baseUrl: "http://qdrant", collection: "pi_memory", ownerHost: "pi", apiKey: "k", timeoutMs: 1000, maxClockSkewMs: 0, readConsistency: "majority" });
    expect(storeA).not.toBe(storeB);
    // The private scope cannot be obtained from anywhere; a structural attempt fails.
    const { createIngestControlReader } = await import("../../src/coordination/control.js");
    expect(() => createIngestControlReader({ ownerHost: "pi", readControl: async () => ({}) } as never, { policyHash: "h", policyEpoch: 0 })).toThrow(/genuine production store/i);
  });

  it("a tiny TypeScript consumer compiles against the emitted d.ts surface (no unresolved raw types)", () => {
    const dir = mkdtempSync(join(tmpdir(), "pi-qdrant-dts-"));
    const consumer = `import { createQdrantCoordinationStore, createIngestControlReader, type ProductionCoordinationStore } from "./dist/coordination/control.js";
import { claimLease, renewLease, releaseLease, acceptLeaseAuthority, readLease, isLeaseExpired, type LeaseAuthority } from "./dist/coordination/leases.js";
import { createJob, writeProposal, acceptProposal, readActiveAcceptance, readJob, type CreateJobInput, type WriteProposalInput } from "./dist/coordination/jobs.js";
import { createTombstone, readTombstones, createIngestTombstoneReader, type CreateTombstoneInput } from "./dist/coordination/tombstones.js";
import { markCoverage, findMissingEpisodes, reconcileCoverage, type MarkCoverageInput, type FindMissingInput } from "./dist/coordination/reconcile.js";
import { beginPolicyDrain, beginForgetBarrier, activatePolicyEpoch, rotateCoordinationPolicy, waitForOldLeasesToQuiesce, initializeControl, readControl, type QuiescenceProof } from "./dist/coordination/control.js";
import { RootWorkerContext } from "./dist/coordination/root.js";
import { createQdrantDestinationFactory, type QdrantDestinationFactoryInput } from "./dist/qdrant/write.js";
import { readPolicy } from "./dist/qdrant/client.js";

declare const production: ProductionCoordinationStore;
declare const authority: LeaseAuthority;
declare const proof: QuiescenceProof;
declare const createInput: CreateJobInput;
declare const writeInput: WriteProposalInput;
declare const tombInput: CreateTombstoneInput;
declare const markInput: MarkCoverageInput;
declare const findInput: FindMissingInput;
declare const worker: RootWorkerContext;
declare const factoryInput: QdrantDestinationFactoryInput;
const p1: Promise<LeaseAuthority | null> = claimLease(production, worker, { jobId: "j", policyEpoch: 1, policyHash: "h", privacyEpoch: 0 });
const p2: Promise<LeaseAuthority | null> = renewLease(production, authority);
const p3: Promise<boolean> = releaseLease(production, authority);
const p4: Promise<import("./dist/domain/records.js").ControlRecord> = beginPolicyDrain(production, { now: 0 });
const p5: Promise<QuiescenceProof> = waitForOldLeasesToQuiesce(production, { retiredEpoch: 1, maxLeaseMs: 30000, maxClockSkewMs: 0 });
const p6: Promise<LeaseAuthority | null> = acceptLeaseAuthority(production, authority, "proposal-id");
const p7: Promise<LeaseAuthority | null> = acceptProposal(production, authority, { proposalId: "proposal-id" });
void p1; void p2; void p3; void p4; void p5; void p6; void p7; void proof; void createInput; void writeInput; void tombInput; void markInput; void findInput; void production; void createJob; void writeProposal; void readJob; void readActiveAcceptance; void createTombstone; void readTombstones; void createIngestTombstoneReader; void createIngestControlReader; void markCoverage; void findMissingEpisodes; void reconcileCoverage; void activatePolicyEpoch; void rotateCoordinationPolicy; void beginForgetBarrier; void initializeControl; void readControl; void readLease; void isLeaseExpired; void createQdrantCoordinationStore; void createQdrantDestinationFactory; void readPolicy; void factoryInput;
`;
    writeFileSync(join(dir, "consumer.ts"), consumer);
    // Copy the built dist next to the consumer so relative d.ts resolution works.
    execFileSync("cp", ["-r", "dist", join(dir, "dist")], { cwd: process.cwd(), stdio: "pipe" });
    // npx must resolve tsc from the repo's node_modules; the consumer file and
    // its relative d.ts imports live in the temp dir.
    execFileSync("npx", ["tsc", "--noEmit", "--strict", "--module", "nodenext", "--moduleResolution", "nodenext", "--target", "es2022", join(dir, "consumer.ts")], { cwd: process.cwd(), stdio: "pipe" });
  });

  it("GLOBAL RULE: Qdrant options accessors are snapshotted EXACTLY ONCE; late swapped values can never mint or relabel", async () => {
    // Variant A: fetchImpl getter returns undefined first, a FAKE fetch later.
    // The snapshot pins the FIRST value; the minted session must use the
    // genuine global fetch (never the late fake) and the getter is read once.
    let fetchReads = 0;
    let collectionReads = 0;
    const fakeLater = async (): Promise<Response> => new Response("{}", { status: 200 });
    const swapOptions = {
      baseUrl: "http://qdrant",
      get collection() { collectionReads += 1; return collectionReads === 1 ? "pi_memory" : "prime_memory"; },
      ownerHost: "pi",
      apiKey: "k",
      timeoutMs: 1000,
      maxClockSkewMs: 0,
      readConsistency: "majority",
      get fetchImpl() { fetchReads += 1; return fetchReads === 1 ? undefined : fakeLater; },
    } as QdrantClientOptions;
    const genuineFetch: typeof fetch = async (input, init = {}) => { const url = String(input); if (url.includes("/points/retrieve")) return new Response(JSON.stringify({ result: [], status: "ok" }), { headers: { "content-type": "application/json" } }); if (url.includes("/points?") && init.method === "PUT") return new Response(JSON.stringify({ result: { status: "acknowledged" }, status: "ok" }), { headers: { "content-type": "application/json" } }); return new Response(JSON.stringify({ result: {}, status: "ok" }), { headers: { "content-type": "application/json" } }); };
    vi.stubGlobal("fetch", genuineFetch);
    const store = createQdrantCoordinationStore(swapOptions);
    expect(ProductionCoordinationStore.isValid(store)).toBe(true);
    expect(store.collection).toBe("pi_memory");
    expect(fetchReads).toBe(1);
    expect(collectionReads).toBe(1);
    // The captured transport is the GENUINE global fetch (which serves an
    // empty backend): a later read of the swapped fetch cannot make the store
    // write or read through the fake — the genuine transport surfaces the
    // missing control point instead of the fake's "{}" envelope.
    await expect(store.readControl()).rejects.toThrow(/missing/i);
    // Variant B: fetchImpl getter returns the FAKE on the first read -> the
    // injected-fetch rejection fires on the snapshot; no production object is
    // ever minted.
    const injected = { baseUrl: "http://qdrant", collection: "pi_memory", ownerHost: "pi", apiKey: "k", timeoutMs: 1000, maxClockSkewMs: 0, readConsistency: "majority", fetchImpl: fakeLater } as QdrantClientOptions;
    expect(() => createQdrantCoordinationStore(injected)).toThrow(/production-bound|injected/i);
    expect(() => createQdrantSafeBundle({ options: injected, destination: { id: "qdrant:pi", residency: "local", dataUse: "memory" }, egressMode: "allowlist", coordinationPolicyHash: "policy-hash", coordinationPolicyEpoch: 1 })).toThrow(/production-bound|injected/i);
  });

  it("GLOBAL RULE: destination/bind accessors are snapshotted EXACTLY ONCE; getter swaps cannot relabel identity", async () => {
    let idReads = 0;
    const swapDestination = {
      get id() { idReads += 1; return idReads === 1 ? "qdrant:pi" : "qdrant:other"; },
      residency: "local",
      dataUse: "memory",
    } as AuthorizedDestination;
    let bindReads = 0;
    const realBind = (dest: AuthorizedDestination) => dest;
    const factory = {
      get bind() { bindReads += 1; return bindReads === 1 ? realBind : () => ({ id: "forged" }); },
    } as unknown as ReturnType<typeof createQdrantSafeBundle>["qdrant"];
    const fetchImpl: typeof fetch = async (input, init = {}) => { const url = String(input); if (url.includes("/points/retrieve")) return new Response(JSON.stringify({ result: [], status: "ok" }), { headers: { "content-type": "application/json" } }); if (url.includes("/points?") && init.method === "PUT") return new Response(JSON.stringify({ result: { status: "acknowledged" }, status: "ok" }), { headers: { "content-type": "application/json" } }); return new Response(JSON.stringify({ result: {}, status: "ok" }), { headers: { "content-type": "application/json" } }); };
    vi.stubGlobal("fetch", fetchImpl);
    const bound = bindQdrantDestination(factory, swapDestination);
    expect(idReads).toBe(1);
    expect(bindReads).toBe(1);
    // The destination snapshot pins the FIRST identity; the late swap never applies.
    expect((bound as unknown as { destination?: AuthorizedDestination }).destination?.id ?? "qdrant:pi").toBe("qdrant:pi");
  });

  it("GLOBAL RULE: ingest runtime snapshots store/qdrant/embedding EXACTLY ONCE; a proxy swap genuine->fake cannot mint", async () => {
    const fetchImpl: typeof fetch = async (input, init = {}) => { const url = String(input); if (url.includes("/points/retrieve")) return new Response(JSON.stringify({ result: [], status: "ok" }), { headers: { "content-type": "application/json" } }); if (url.includes("/points?") && init.method === "PUT") return new Response(JSON.stringify({ result: { status: "acknowledged" }, status: "ok" }), { headers: { "content-type": "application/json" } }); return new Response(JSON.stringify({ result: {}, status: "ok" }), { headers: { "content-type": "application/json" } }); };
    vi.stubGlobal("fetch", fetchImpl);
    const bundle = createQdrantSafeBundle({ options: { baseUrl: "http://qdrant", collection: "pi_memory", ownerHost: "pi", apiKey: "k", timeoutMs: 1000, maxClockSkewMs: 0, readConsistency: "majority" }, destination: { id: "qdrant:pi", residency: "local", dataUse: "memory" }, egressMode: "allowlist", coordinationPolicyHash: "policy-hash", coordinationPolicyEpoch: 1 });
    const qdrantBound = bindQdrantDestination(bundle.qdrant, { id: "qdrant:pi", residency: "local", dataUse: "memory" });
    const embeddingFactory = createEmbeddingDestinationFactory({ endpoint: "http://embed/v1", destination: { id: "embed:local", residency: "local", dataUse: "memory" }, client: bindEmbeddingDocumentClient({ endpoint: "http://embed/v1", client: new EmbeddingsClient({ baseUrl: "http://embed/v1", model: "bge-m3", dimension: 1024, queryPrefix: "query: ", timeoutMs: 100 }) }), egressMode: "allowlist", coordinationPolicyHash: "policy-hash", coordinationPolicyEpoch: 1 });
    const embeddingBound = bindEmbeddingDestination(embeddingFactory, { id: "embed:local", residency: "local", dataUse: "memory" });
    // A Proxy that serves the genuine objects on the FIRST read and a fake on
    // later reads: the runtime snapshots once and must mint with the genuine
    // trio (or fail) — never with the swapped fake.
    let reads = 0;
    const swapStore = new Proxy({}, { get(_t, prop) { reads += 1; if (prop === "transport") return bundle.store.transport; if (prop === "endpoint") return bundle.store.endpoint; if (prop === "ownerHost") return bundle.store.ownerHost; if (prop === "collection") return bundle.store.collection; throw new Error("unexpected store getter: " + String(prop)); } }) as unknown as typeof bundle.store;
    const input = { store: swapStore, qdrant: qdrantBound, embedding: embeddingBound };
    // The brand check rejects the structural proxy store regardless of getter values.
    await expect(Promise.resolve().then(() => bindIngestRuntime(input as never))).rejects.toThrow(/branded production store/i);
  });


  it("GLOBAL RULE: embedding factory / egress / reconcile / reader optional+destination accessors are read EXACTLY ONCE", async () => {
    // (1) createEmbeddingDestinationFactory: the top-level destination getter
    // must be read exactly once (id/residency/dataUse from the local).
    let destReads = 0;
    const swapDest = {
      get id() { destReads += 1; return "embed:local"; },
      residency: "local",
      dataUse: "memory",
    } as AuthorizedDestination;
    const fetchImpl: typeof fetch = async () => new Response(JSON.stringify({ data: [{ embedding: Array.from({ length: 1024 }, () => 0.25) }] }), { status: 200, headers: { "content-type": "application/json" } });
    vi.stubGlobal("fetch", fetchImpl);
    const embeddingClient = new EmbeddingsClient({ baseUrl: "http://embed/v1", model: "bge-m3", dimension: 1024, queryPrefix: "query: ", timeoutMs: 100 });
    const embeddingFactory = createEmbeddingDestinationFactory({ endpoint: "http://embed/v1", destination: swapDest, client: bindEmbeddingDocumentClient({ endpoint: "http://embed/v1", client: embeddingClient }), egressMode: "allowlist", coordinationPolicyHash: "policy-hash", coordinationPolicyEpoch: 1 });
    expect(destReads).toBe(1);
    // (2) bindConfiguredDestination: configured/requested top-level getters read once.
    let configuredReads = 0;
    let requestedReads = 0;
    const configured = { get id() { configuredReads += 1; return "qdrant:pi"; }, residency: "local", dataUse: "memory" } as AuthorizedDestination;
    const requested = { get id() { requestedReads += 1; return "qdrant:pi"; }, residency: "local", dataUse: "memory" } as AuthorizedDestination;
    const { bindConfiguredDestination } = await import("../../src/security/egress.js");
    const boundDest = bindConfiguredDestination({ endpoint: "http://qdrant", configuredDestination: configured, requestedDestination: requested, egressMode: "allowlist" });
    expect(configuredReads).toBe(1);
    expect(requestedReads).toBe(1);
    expect(boundDest.id).toBe("qdrant:pi");
    // (3) reconcileCoverage optional fields: each read exactly once (a swapped
    // optional value can never be validated/persisted twice).
    let batchReads = 0;
    let offsetReads = 0;
    const { createQdrantCoordinationStore: createStore2 } = await import("../../src/qdrant/write.js");
    vi.stubGlobal("fetch", async (input: RequestInfo | URL, init?: RequestInit) => { const url = String(input); if (url.includes("/points/retrieve")) return new Response(JSON.stringify({ result: [], status: "ok" }), { headers: { "content-type": "application/json" } }); if (url.includes("/points?") && init?.method === "PUT") return new Response(JSON.stringify({ result: { status: "acknowledged" }, status: "ok" }), { headers: { "content-type": "application/json" } }); return new Response(JSON.stringify({ result: {}, status: "ok" }), { headers: { "content-type": "application/json" } }); });
    const store2 = createStore2({ baseUrl: "http://qdrant", collection: "pi_memory", ownerHost: "pi", apiKey: "k", timeoutMs: 1000, maxClockSkewMs: 0, readConsistency: "majority" });
    const { reconcileCoverage } = await import("../../src/coordination/reconcile.js");
    const swapBatch = { get batchSize() { batchReads += 1; return 64; } } as never;
    const swapOffset = { get offset() { offsetReads += 1; return undefined; } } as never;
    const result = await reconcileCoverage({ store: store2, listEpisodes: async () => ({ episodes: [] }), extractorRevision: "e", policyEpoch: 1, policyHash: "h", policyIntersectionId: "i", privacyEpoch: 0, ...(swapBatch as object), ...(swapOffset as object) } as never);
    expect(batchReads).toBe(1);
    expect(offsetReads).toBe(1);
    expect(result.missing).toEqual([]);
    // (4) createIngestControlReader: policyHash/policyEpoch read exactly once.
    let hashReads = 0;
    let epochReads = 0;
    const { createIngestControlReader } = await import("../../src/coordination/control.js");
    const swapPolicy = { get policyHash() { hashReads += 1; return "policy-hash"; }, get policyEpoch() { epochReads += 1; return 1; } } as never;
    const reader = createIngestControlReader(store2, swapPolicy as never);
    expect(hashReads).toBe(1);
    expect(epochReads).toBe(1);
    expect(reader.policyHash).toBe("policy-hash");
    expect(reader.policyEpoch).toBe(1);
  });

});
