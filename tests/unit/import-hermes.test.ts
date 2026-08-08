import { describe, expect, it, vi } from "vitest";
import { MemoryClientError } from "../../src/clients/http.js";
import {
  ImportApprovalMismatchError,
  applyHermesImport,
  planHermesImport,
  type ImportClients,
  type ImportOptions,
} from "../../src/admin/import-hermes.js";
import {
  defaultCliDependencies,
  main,
  type CliDependencies,
} from "../../src/admin/cli.js";
import type { ImportPlan } from "../../src/admin/import-plan.js";
import type { AdminCollectionInfo, AdminPoint } from "../../src/admin/qdrant-admin.js";
import type { HostId, RuntimeConfig } from "../../src/types.js";

const PLAN_ID = "a".repeat(64);

function point(id: string | number, text = `safe memory ${id}`): AdminPoint {
  return {
    id,
    vector: [0.1, 0.2, 0.3],
    payload: { text, model: "bge-m3", source_type: "note" },
  };
}

function info(overrides: Partial<AdminCollectionInfo> = {}): AdminCollectionInfo {
  return { dimension: 3, distance: "Cosine", pointCount: null, ...overrides };
}

function options(overrides: Partial<ImportOptions> = {}): ImportOptions {
  return {
    sourceIdentity: "http://source.test",
    sourceCollection: "hermes_memory",
    destinationCollection: "pi_memory",
    targetHost: "prime",
    configuredModel: "bge-m3",
    configuredDimension: 3,
    declaredSourceModel: "bge-m3",
    ...overrides,
  };
}

function importerClients(pages: Array<{ points: AdminPoint[]; nextOffset?: string | number }>): {
  clients: ImportClients;
  sourceInfo: ReturnType<typeof vi.fn>;
  destinationInfo: ReturnType<typeof vi.fn>;
  scroll: ReturnType<typeof vi.fn>;
  upsert: ReturnType<typeof vi.fn>;
} {
  let page = 0;
  const sourceInfo = vi.fn(async () => info());
  const destinationInfo = vi.fn(async () => info());
  const scroll = vi.fn(async () => pages[page++] ?? { points: [] });
  const upsert = vi.fn(async () => undefined);
  return {
    clients: {
      source: { collectionInfo: sourceInfo, scroll },
      destination: { collectionInfo: destinationInfo, upsert },
    },
    sourceInfo,
    destinationInfo,
    scroll,
    upsert,
  };
}

function config(host: HostId = "prime"): RuntimeConfig {
  return {
    host,
    enabled: true,
    autoRecall: true,
    configPath: "/private/config.json",
    qdrant: {
      url: "http://destination.test",
      collection: "pi_memory",
      apiKey: "runtime-key-must-not-administer",
    },
    embeddings: {
      baseUrl: "http://embeddings.test/v1",
      model: "bge-m3",
      dimension: 3,
      queryPrefix: "search_query: ",
    },
    retrieval: {
      topK: 5,
      candidatesPerLane: 20,
      minScore: 0.35,
      projectBoost: 0.05,
      contextBudgetChars: 1200,
      toolResultBudgetChars: 8000,
      hardContextCharBudget: 16000,
      timeoutMs: 2500,
    },
    admin: {
      destinationApiKey: "admin-key",
      source: {
        url: "http://source.test",
        collection: "hermes_memory",
        schema: "hermes-qdrant-memory-v0.9-compatible",
        apiKey: "source-key",
      },
    },
  };
}

function safePlan(extra: Partial<ImportPlan> = {}): ImportPlan {
  return {
    planId: PLAN_ID,
    transformVersion: 1,
    targetHost: "prime",
    sourceCollection: "hermes_memory",
    destinationCollection: "pi_memory",
    accepted: [point("accepted-secret", "password=hunter2long")],
    rejected: { secret: 1 },
    report: {
      accepted: 1,
      rejected: 1,
      bySourceType: { note: 1 },
      byProjectLabel: { global: 1 },
    },
    ...extra,
  };
}

interface CliHarness {
  deps: CliDependencies;
  stdout: string[];
  stderr: string[];
  loadConfig: ReturnType<typeof vi.fn>;
  initialize: ReturnType<typeof vi.fn>;
  status: ReturnType<typeof vi.fn>;
  plan: ReturnType<typeof vi.fn>;
  apply: ReturnType<typeof vi.fn>;
  createImportClients: ReturnType<typeof vi.fn>;
}

function cliHarness(overrides: Partial<CliDependencies> = {}): CliHarness {
  const stdout: string[] = [];
  const stderr: string[] = [];
  const loadConfig = vi.fn(async (host: HostId) => config(host));
  const initialize = vi.fn(async (value: RuntimeConfig) => ({
    created: true,
    collection: value.qdrant.collection,
    dimension: value.embeddings.dimension,
    distance: "Cosine" as const,
  }));
  const status = vi.fn(async () => ({
    destinationExists: true,
    destination: {
      endpoint: "https://user:password@destination.private/path",
      collection: "pi_memory",
      exists: true,
      dimension: 3,
      distance: "Cosine",
      pointCount: 4,
      healthy: true,
      keyConfigured: true,
    },
    source: {
      endpoint: "https://source.private/token=abcdefghijklmnop",
      collection: "hermes_memory",
      exists: true,
      dimension: 3,
      distance: "Cosine",
      pointCount: 5,
      healthy: true,
      keyConfigured: true,
    },
    embeddings: {
      endpoint: "https://embeddings.private",
      model: "bge-m3",
      dimension: 3,
      healthy: true,
      keyConfigured: false,
    },
    qdrant: { healthy: true, destinationHealthy: true, sourceHealthy: true },
  }));
  const plan = vi.fn(async () => safePlan());
  const apply = vi.fn(async () => ({ planId: PLAN_ID, upserted: 1, batches: 1 }));
  const createImportClients = vi.fn(() => importerClients([{ points: [] }]).clients);
  const deps: CliDependencies = {
    env: { PI_QDRANT_MEMORY_HOST: "prime" },
    loadConfig,
    initialize,
    status,
    plan,
    apply,
    createImportClients,
    writeStdout: (value) => stdout.push(value),
    writeStderr: (value) => stderr.push(value),
    ...overrides,
  };
  return { deps, stdout, stderr, loadConfig, initialize, status, plan, apply, createImportClients };
}

describe("planHermesImport", () => {
  it("validates both collection contracts before scrolling exact pages of 256", async () => {
    const first = point("first");
    const second = point(2);
    const harness = importerClients([
      { points: [first], nextOffset: "string-offset" },
      { points: [second], nextOffset: 42 },
      { points: [] },
    ]);

    const plan = await planHermesImport(options(), harness.clients);

    expect(plan.report.accepted).toBe(2);
    expect(harness.sourceInfo).toHaveBeenCalledOnce();
    expect(harness.destinationInfo).toHaveBeenCalledOnce();
    expect(harness.scroll.mock.calls).toEqual([
      ["hermes_memory", undefined, 256, undefined],
      ["hermes_memory", "string-offset", 256, undefined],
      ["hermes_memory", 42, 256, undefined],
    ]);
    expect(harness.upsert).not.toHaveBeenCalled();
  });

  it.each([
    [info({ dimension: 4 }), info(), "dimension"],
    [info(), info({ dimension: 4 }), "dimension"],
    [info({ distance: "Dot" }), info(), "distance"],
    [info(), info({ distance: "Euclid" }), "distance"],
  ])("rejects an incompatible source/destination contract before scroll", async (source, destination, word) => {
    const harness = importerClients([{ points: [point("never-read")] }]);
    harness.sourceInfo.mockResolvedValue(source);
    harness.destinationInfo.mockResolvedValue(destination);
    await expect(planHermesImport(options(), harness.clients)).rejects.toThrow(word);
    expect(harness.scroll).not.toHaveBeenCalled();
    expect(harness.upsert).not.toHaveBeenCalled();
  });

  it("redacts collection metadata failures", async () => {
    const harness = importerClients([]);
    harness.sourceInfo.mockRejectedValue(new Error("password=hunter2long https://private/path"));
    let message = "";
    try {
      await planHermesImport(options(), harness.clients);
    } catch (error: unknown) {
      message = error instanceof Error ? error.message : String(error);
    }
    expect(message).toBe("source collection metadata unavailable");
    expect(message).not.toContain("hunter2long");
    expect(harness.scroll).not.toHaveBeenCalled();
  });

  it.each([
    [
      [{ points: [], nextOffset: "repeat" }, { points: [], nextOffset: "repeat" }],
      "repeated",
    ],
    [
      [
        { points: [], nextOffset: 7 },
        { points: [], nextOffset: "7" },
        { points: [], nextOffset: 7 },
      ],
      "cyclic",
    ],
  ])("detects %s pagination offsets without looping", async (pages) => {
    const harness = importerClients(pages as Array<{ points: AdminPoint[]; nextOffset?: string | number }>);
    await expect(planHermesImport(options(), harness.clients)).rejects.toThrow(/pagination offset/i);
    expect(harness.scroll).toHaveBeenCalledTimes(pages.length);
    expect(harness.upsert).not.toHaveBeenCalled();
  });

  it("is a zero-write dry-run and serializes only content-free report projections", async () => {
    const secret = "password=hunter2long";
    const harness = importerClients([{ points: [point("secret-source-id", secret)] }]);
    const plan = await planHermesImport(options(), harness.clients);
    const reportJson = JSON.stringify({
      planId: plan.planId,
      rejected: plan.rejected,
      report: plan.report,
    });
    expect(plan.report).toMatchObject({ accepted: 0, rejected: 1 });
    expect(reportJson).not.toContain(secret);
    expect(reportJson).not.toContain("secret-source-id");
    expect(harness.upsert).not.toHaveBeenCalled();
  });
});

describe("applyHermesImport", () => {
  it.each(["", "A".repeat(64), "a".repeat(63), "g".repeat(64)])(
    "rejects invalid approval %j before any I/O",
    async (approvedPlanId) => {
      const harness = importerClients([{ points: [point("never-read")] }]);
      await expect(applyHermesImport({ ...options(), approvedPlanId }, harness.clients)).rejects.toThrow(/approval/i);
      expect(harness.sourceInfo).not.toHaveBeenCalled();
      expect(harness.destinationInfo).not.toHaveBeenCalled();
      expect(harness.scroll).not.toHaveBeenCalled();
      expect(harness.upsert).not.toHaveBeenCalled();
    },
  );

  it("rejects a well-formed but wrong approval before any destination write", async () => {
    const harness = importerClients([{ points: [point("source-1")] }]);
    await expect(
      applyHermesImport(
        { ...options(), approvedPlanId: "f".repeat(64) },
        harness.clients,
      ),
    ).rejects.toThrow("source changed; run dry-run again");
    expect(harness.sourceInfo).toHaveBeenCalledOnce();
    expect(harness.destinationInfo).toHaveBeenCalledOnce();
    expect(harness.scroll).toHaveBeenCalledOnce();
    expect(harness.upsert).not.toHaveBeenCalled();
  });

  it.each([
    ["vector", (value: AdminPoint) => { value.vector[0] = 0.777; }],
    ["relevant payload", (value: AdminPoint) => { value.payload.source_type = "changed"; }],
  ])("replans completely and performs zero writes after a %s change", async (_label, mutate) => {
    const sourcePoint = point("source-1");
    const harness = importerClients([]);
    harness.scroll.mockImplementation(async () => ({ points: [sourcePoint] }));
    const dry = await planHermesImport(options(), harness.clients);
    mutate(sourcePoint);

    await expect(
      applyHermesImport({ ...options(), approvedPlanId: dry.planId }, harness.clients),
    ).rejects.toThrow("source changed; run dry-run again");
    expect(harness.sourceInfo).toHaveBeenCalledTimes(2);
    expect(harness.destinationInfo).toHaveBeenCalledTimes(2);
    expect(harness.scroll).toHaveBeenCalledTimes(2);
    expect(harness.upsert).not.toHaveBeenCalled();
  });

  it.each([
    [0, []],
    [1, [1]],
    [64, [64]],
    [65, [64, 1]],
    [129, [64, 64, 1]],
  ])("upserts %i accepted points in nonempty batches no larger than 64", async (count, sizes) => {
    const points = Array.from({ length: count }, (_, index) => point(index));
    const harness = importerClients([]);
    harness.scroll.mockImplementation(async () => ({ points }));
    const dry = await planHermesImport(options(), harness.clients);
    const result = await applyHermesImport(
      { ...options(), approvedPlanId: dry.planId },
      harness.clients,
    );

    expect(result).toEqual({ planId: dry.planId, upserted: count, batches: sizes.length });
    expect(harness.upsert.mock.calls.map((call) => call[1].length)).toEqual(sizes);
    expect(harness.upsert.mock.calls.every((call) => call[1].length > 0 && call[1].length <= 64)).toBe(true);
  });

  it("does not mutate or retain source objects and retries deterministic IDs/run IDs", async () => {
    const sourcePoints = [
      point("b"),
      { ...point(7), payload: { ...point(7).payload, tags: ["tag"] } },
    ];
    const before = structuredClone(sourcePoints);
    const source = {
      collectionInfo: vi.fn(async () => info()),
      scroll: vi.fn(async () => ({ points: sourcePoints })),
    };
    Object.freeze(source);
    const firstBatches: AdminPoint[][] = [];
    const secondBatches: AdminPoint[][] = [];
    const firstClients: ImportClients = {
      source,
      destination: {
        collectionInfo: vi.fn(async () => info()),
        upsert: vi.fn(async (_collection, batch) => { firstBatches.push(structuredClone(batch)); }),
      },
    };
    const dry = await planHermesImport(options(), firstClients);
    await applyHermesImport({ ...options(), approvedPlanId: dry.planId }, firstClients);
    const secondClients: ImportClients = {
      source,
      destination: {
        collectionInfo: vi.fn(async () => info()),
        upsert: vi.fn(async (_collection, batch) => { secondBatches.push(structuredClone(batch)); }),
      },
    };
    await applyHermesImport({ ...options(), approvedPlanId: dry.planId }, secondClients);

    expect(sourcePoints).toEqual(before);
    expect(firstBatches).toEqual(secondBatches);
    expect(firstBatches.flat().every((value) => value.payload.import_run_id === dry.planId)).toBe(true);
    expect(firstBatches.flat().map((value) => value.id)).toEqual(dry.accepted.map((value) => value.id));
    expect(Object.keys(source).sort()).toEqual(["collectionInfo", "scroll"]);
  });
});

describe("administrative CLI", () => {
  it.each(["init", "status"])("resolves %s only from the explicit administrative host environment", async (command) => {
    const harness = cliHarness();
    const exit = await main([command, "--json"], harness.deps);
    expect(exit).toBe(0);
    expect(harness.loadConfig).toHaveBeenCalledWith("prime");
    expect(harness.stderr).toEqual([]);
  });

  it.each(["init", "status"])("fails closed for %s without PI_QDRANT_MEMORY_HOST even with process markers", async (command) => {
    const harness = cliHarness({
      env: { PRIME_AGENT: "1", PI_AGENT_DIR: "/ambiguous/private/path" },
    });
    const exit = await main([command], harness.deps);
    expect(exit).toBe(2);
    expect(harness.loadConfig).not.toHaveBeenCalled();
    expect(harness.stderr.join("\n")).not.toContain("ambiguous/private");
  });

  it("prints only a safe init projection", async () => {
    const harness = cliHarness();
    expect(await main(["init", "--json"], harness.deps)).toBe(0);
    const output = harness.stdout.join("");
    expect(JSON.parse(output)).toMatchObject({ command: "init", host: "prime", collection: "pi_memory" });
    expect(output).not.toContain("config.json");
    expect(output).not.toContain("key");
  });

  it("prints only a safe status projection without endpoints", async () => {
    const harness = cliHarness();
    expect(await main(["status", "--json"], harness.deps)).toBe(0);
    const output = harness.stdout.join("");
    expect(JSON.parse(output)).toMatchObject({
      command: "status",
      host: "prime",
      destination: { collection: "pi_memory", exists: true },
      source: { collection: "hermes_memory", exists: true },
    });
    expect(output).not.toContain("endpoint");
    expect(output).not.toContain("password");
    expect(output).not.toContain("private");
  });

  it("requires an exact import target and one exact mode", async () => {
    for (const args of [
      ["import-hermes", "--dry-run"],
      ["import-hermes", "--target-host", "other", "--dry-run"],
      ["import-hermes", "--target-host", "prime"],
      ["import-hermes", "--target-host", "prime", "--dry-run", "--approve", PLAN_ID],
      ["import-hermes", "--target-host", "prime", "--approve", "short"],
    ]) {
      const harness = cliHarness();
      expect(await main(args, harness.deps)).toBe(2);
      expect(harness.loadConfig).not.toHaveBeenCalled();
      expect(harness.plan).not.toHaveBeenCalled();
      expect(harness.apply).not.toHaveBeenCalled();
    }
  });

  it("uses strict per-command parsing and rejects credential flags", async () => {
    for (const args of [
      ["init", "--target-host", "prime"],
      ["status", "extra"],
      ["import-hermes", "--target-host", "prime", "--dry-run", "--api-key", "do-not-echo"],
      ["import-hermes", "--target-host", "prime", "--dry-run", "--unknown"],
    ]) {
      const harness = cliHarness();
      expect(await main(args, harness.deps)).toBe(2);
      expect(harness.loadConfig).not.toHaveBeenCalled();
      expect(harness.stderr.join("")).not.toContain("do-not-echo");
    }
  });

  it("applies strict credential-free source overrides without mutating config", async () => {
    const harness = cliHarness();
    expect(await main([
      "import-hermes",
      "--target-host", "pi",
      "--source-url", "https://source.example/qdrant/",
      "--source-collection", "alternate_source",
      "--source-model", "bge-m3",
      "--dry-run",
      "--json",
    ], harness.deps)).toBe(0);
    expect(harness.loadConfig).toHaveBeenCalledWith("pi");
    expect(harness.createImportClients).toHaveBeenCalledWith(
      expect.objectContaining({ host: "pi" }),
      "https://source.example/qdrant",
    );
    expect(harness.plan).toHaveBeenCalledWith(
      expect.objectContaining({
        sourceIdentity: "https://source.example/qdrant",
        sourceCollection: "alternate_source",
        declaredSourceModel: "bge-m3",
        targetHost: "pi",
      }),
      expect.any(Object),
    );
    expect((await harness.loadConfig.mock.results[0]?.value).admin.source.collection).toBe("hermes_memory");
  });

  it.each([
    "https://user:password@source.example",
    "https://source.example/?api_key=secret",
    "file:///private/source",
    "not a url",
  ])("rejects unsafe source URL override %s before dependencies run", async (sourceUrl) => {
    const harness = cliHarness();
    expect(await main([
      "import-hermes", "--target-host", "prime", "--source-url", sourceUrl, "--dry-run",
    ], harness.deps)).toBe(2);
    expect(harness.loadConfig).not.toHaveBeenCalled();
    expect(harness.stderr.join("")).not.toContain(sourceUrl);
  });

  it.each([
    { name: "newline", control: "\n" },
    { name: "carriage return", control: "\r" },
    { name: "tab", control: "\t" },
    { name: "DEL", control: "\u007f" },
    { name: "C1 start", control: "\u0080" },
    { name: "C1 end", control: "\u009f" },
  ])(
    "rejects a raw $name control in source URL before config or network work",
    async ({ control }) => {
      const harness = cliHarness();
      const unsafeUrl = `https://source.example/a${control}b`;
      expect(await main([
        "import-hermes", "--target-host", "prime", "--source-url", unsafeUrl, "--dry-run",
      ], harness.deps)).toBe(2);
      expect(harness.loadConfig).not.toHaveBeenCalled();
      expect(harness.createImportClients).not.toHaveBeenCalled();
      expect(harness.plan).not.toHaveBeenCalled();
    },
  );

  it("uses one parsed normalized source URL for both plan identity and client base URL", async () => {
    const harness = cliHarness();
    expect(await main([
      "import-hermes",
      "--target-host", "prime",
      "--source-url", "HTTP://Source.Example:80/a/../qdrant///",
      "--dry-run",
    ], harness.deps)).toBe(0);
    const normalized = "http://source.example/qdrant";
    expect(harness.createImportClients).toHaveBeenCalledWith(expect.any(Object), normalized);
    expect(harness.plan).toHaveBeenCalledWith(
      expect.objectContaining({ sourceIdentity: normalized }),
      expect.any(Object),
    );
    expect(harness.createImportClients.mock.calls[0]?.[1]).toBe(
      harness.plan.mock.calls[0]?.[0].sourceIdentity,
    );
  });

  it.each([
    ["--source-collection", "../private"],
    ["--source-collection", "password=hunter2long"],
    ["--source-model", " token=abcdefghijklmnop"],
    ["--source-model", "password=hunter2long"],
  ])("rejects unsafe %s override without echoing its value", async (flag, value) => {
    const harness = cliHarness();
    expect(await main([
      "import-hermes", "--target-host", "prime", flag, value, "--dry-run",
    ], harness.deps)).toBe(2);
    expect(harness.loadConfig).not.toHaveBeenCalled();
    expect(harness.stderr.join("")).not.toContain(value);
  });

  it("never serializes ImportPlan.accepted in JSON or human dry-run output", async () => {
    for (const extra of [[], ["--json"]]) {
      const harness = cliHarness();
      expect(await main([
        "import-hermes", "--target-host", "prime", "--dry-run", ...extra,
      ], harness.deps)).toBe(0);
      const output = `${harness.stdout.join("")}\n${harness.stderr.join("")}`;
      expect(output).not.toContain("accepted-secret");
      expect(output).not.toContain("password=hunter2long");
      expect(output).not.toContain('"accepted":[');
      expect(output).toContain(PLAN_ID);
    }
  });

  it("injectively escapes aggregate keys and dry-run projection strings in JSON and human output", async () => {
    const actualDel = "type\u007f";
    const literalDelEscape = String.raw`type\u007F`;
    const actualC1 = "type\u0085";
    const literalC1Escape = String.raw`type\u0085`;
    const actualNewline = "line\nbreak";
    const maliciousPlan = safePlan({
      sourceCollection: "source\u007fcollection",
      destinationCollection: "password=hunter2long",
      rejected: { [actualC1]: 3, [literalC1Escape]: 4 },
      report: {
        accepted: 3,
        rejected: 7,
        bySourceType: {
          [actualDel]: 1,
          [literalDelEscape]: 2,
          [actualNewline]: 3,
        },
        byProjectLabel: { [actualC1]: 3, [literalC1Escape]: 4 },
      },
    });

    for (const json of [false, true]) {
      const harness = cliHarness({ plan: vi.fn(async () => maliciousPlan) });
      expect(await main([
        "import-hermes", "--target-host", "prime", "--dry-run", ...(json ? ["--json"] : []),
      ], harness.deps)).toBe(0);
      const stdout = harness.stdout.join("");
      expect(stdout).not.toContain("hunter2long");
      expect(stdout).not.toContain(actualDel);
      expect(stdout).not.toContain(actualC1);
      expect(stdout).not.toContain("\r");
      expect(stdout).not.toContain("\t");
      expect(stdout).toContain(String.raw`type\\u007F`);
      expect(stdout).toContain(String.raw`type\\\\u007F`);
      expect(stdout).toContain(String.raw`type\\u0085`);
      expect(stdout).toContain(String.raw`type\\\\u0085`);
      expect(stdout).toContain(String.raw`line\\u000Abreak`);
      if (json) {
        expect(stdout.slice(0, -1)).not.toMatch(/[\u0000-\u001f\u007f-\u009f]/u);
        const parsed = JSON.parse(stdout) as {
          sourceCollection: string;
          destinationCollection: string;
          report: { bySourceType: Record<string, number> };
        };
        expect(parsed.sourceCollection).toBe(String.raw`source\u007Fcollection`);
        expect(parsed.destinationCollection).toBe("[redacted]");
        expect(parsed.report.bySourceType).toEqual({
          [String.raw`type\u007F`]: 1,
          [String.raw`type\\u007F`]: 2,
          [String.raw`line\u000Abreak`]: 3,
        });
      }
    }
  });

  it("redacts credential-shaped config strings and escapes controls in init/status projections", async () => {
    const maliciousConfig = config();
    maliciousConfig.qdrant.collection = "password=hunter2long";
    maliciousConfig.admin.source.collection = "source\u007fcollection";
    maliciousConfig.embeddings.model = "token=abcdefghijklmnop";

    const initHarness = cliHarness({
      loadConfig: vi.fn(async () => maliciousConfig),
      initialize: vi.fn(async () => ({
        created: false,
        collection: maliciousConfig.qdrant.collection,
        dimension: 3,
        distance: "Cos\u0085ine" as "Cosine",
      })),
    });
    expect(await main(["init", "--json"], initHarness.deps)).toBe(0);
    const initOutput = initHarness.stdout.join("");
    expect(initOutput).not.toContain("hunter2long");
    expect(initOutput).not.toContain("\u0085");
    expect(JSON.parse(initOutput)).toMatchObject({
      collection: "[redacted]",
      distance: String.raw`Cos\u0085ine`,
    });

    const baseStatus = await cliHarness().deps.status(config());
    baseStatus.destination.collection = maliciousConfig.qdrant.collection;
    baseStatus.destination.distance = "Cos\u007fine";
    baseStatus.source.collection = maliciousConfig.admin.source.collection;
    baseStatus.source.distance = "Cos\u009fine";
    baseStatus.embeddings.model = maliciousConfig.embeddings.model;
    const statusHarness = cliHarness({
      loadConfig: vi.fn(async () => maliciousConfig),
      status: vi.fn(async () => baseStatus),
    });
    expect(await main(["status", "--json"], statusHarness.deps)).toBe(0);
    const statusOutput = statusHarness.stdout.join("");
    expect(statusOutput).not.toContain("hunter2long");
    expect(statusOutput).not.toContain("abcdefghijklmnop");
    expect(statusOutput.slice(0, -1)).not.toMatch(/[\u0000-\u001f\u007f-\u009f]/u);
    expect(JSON.parse(statusOutput)).toMatchObject({
      destination: {
        collection: "[redacted]",
        distance: String.raw`Cos\u007Fine`,
      },
      source: {
        collection: String.raw`source\u007Fcollection`,
        distance: String.raw`Cos\u009Fine`,
      },
      embeddings: { model: "[redacted]" },
    });
  });

  it("escapes every apply result string before JSON output", async () => {
    const harness = cliHarness({
      apply: vi.fn(async () => ({
        planId: "plan\u007fidentifier",
        upserted: 1,
        batches: 1,
      })),
    });
    expect(await main([
      "import-hermes", "--target-host", "prime", "--approve", PLAN_ID, "--json",
    ], harness.deps)).toBe(0);
    const output = harness.stdout.join("");
    expect(output.slice(0, -1)).not.toMatch(/[\u0000-\u001f\u007f-\u009f]/u);
    expect(JSON.parse(output)).toMatchObject({
      planId: String.raw`plan\u007Fidentifier`,
      sourceCollection: "hermes_memory",
      destinationCollection: "pi_memory",
    });
  });

  it("runs approved apply and prints only the safe result", async () => {
    const harness = cliHarness();
    expect(await main([
      "import-hermes", "--target-host", "prime", "--approve", PLAN_ID, "--json",
    ], harness.deps)).toBe(0);
    expect(harness.apply).toHaveBeenCalledWith(
      expect.objectContaining({ targetHost: "prime", approvedPlanId: PLAN_ID }),
      expect.any(Object),
    );
    expect(JSON.parse(harness.stdout.join(""))).toMatchObject({
      command: "import-hermes",
      mode: "apply",
      planId: PLAN_ID,
      upserted: 1,
      batches: 1,
    });
    expect(harness.stdout.join("")).not.toContain("accepted-secret");
  });

  it.each([
    { args: ["--help"] },
    { args: ["--help", "--json"] },
    { args: ["--json", "--help"] },
    { args: ["init", "--help"] },
    { args: ["status", "--help"] },
    { args: ["import-hermes", "--help"] },
    { args: ["import-hermes", "--json", "--help"] },
  ])("returns zero for help $args without loading configuration", async ({ args }) => {
    const harness = cliHarness();
    expect(await main(args, harness.deps)).toBe(0);
    expect(harness.loadConfig).not.toHaveBeenCalled();
    expect(harness.createImportClients).not.toHaveBeenCalled();
    if (args.includes("--json")) {
      expect(JSON.parse(harness.stdout.join(""))).toHaveProperty("usage");
    }
  });

  it("maps invalid/config/mismatch to 2, infrastructure to 1, and redacts caught messages", async () => {
    const configFailure = cliHarness({
      loadConfig: vi.fn(async () => { throw new Error("/private/config password=hunter2long"); }),
    });
    expect(await main(["init"], configFailure.deps)).toBe(2);

    const infrastructure = cliHarness({
      plan: vi.fn(async () => { throw new MemoryClientError("network", "https://private source text"); }),
    });
    expect(await main(["import-hermes", "--target-host", "prime", "--dry-run"], infrastructure.deps)).toBe(1);

    const configIoFailure = cliHarness({
      loadConfig: vi.fn(async () => {
        throw Object.assign(new Error("/private/config permission denied"), { code: "EACCES" });
      }),
    });
    expect(await main(["status"], configIoFailure.deps)).toBe(1);

    const mismatch = cliHarness({
      apply: vi.fn(async () => { throw new ImportApprovalMismatchError(); }),
    });
    expect(await main(["import-hermes", "--target-host", "prime", "--approve", PLAN_ID], mismatch.deps)).toBe(2);
    expect(mismatch.stderr.join("").trim()).toBe("source changed; run dry-run again");

    const allOutput = [configFailure, infrastructure, configIoFailure, mismatch]
      .flatMap((value) => [...value.stdout, ...value.stderr])
      .join("\n");
    expect(allOutput).not.toContain("hunter2long");
    expect(allOutput).not.toContain("https://private");
    expect(allOutput).not.toContain("/private/config");
  });

  it("uses SOURCE key for reads, ADMIN key for destination metadata/writes, and never runtime key", async () => {
    const calls: Array<{ url: string; method: string; key: string | null; body?: unknown }> = [];
    const fetchImpl = (async (url: string, init: RequestInit = {}) => {
      const method = init.method ?? "GET";
      calls.push({
        url,
        method,
        key: new Headers(init.headers).get("api-key"),
        ...(typeof init.body === "string" ? { body: JSON.parse(init.body) } : {}),
      });
      if (method === "GET") {
        return new Response(JSON.stringify({
          result: {
            points_count: 1,
            config: { params: { vectors: { size: 3, distance: "Cosine" } } },
            payload_schema: {},
          },
          status: "ok",
        }), { status: 200, headers: { "content-type": "application/json" } });
      }
      if (url.endsWith("/points/scroll")) {
        return new Response(JSON.stringify({
          result: { points: [point("one")], next_page_offset: null },
          status: "ok",
        }), { status: 200, headers: { "content-type": "application/json" } });
      }
      if (url.endsWith("/points?wait=true")) {
        return new Response(JSON.stringify({
          result: { status: "completed", operation_id: 1 },
          status: "ok",
        }), { status: 200, headers: { "content-type": "application/json" } });
      }
      throw new Error("unexpected mocked request");
    }) as typeof fetch;
    const stdout: string[] = [];
    const stderr: string[] = [];
    const deps = defaultCliDependencies({
      env: {
        PI_QDRANT_MEMORY_HOST: "prime",
        PI_QDRANT_MEMORY_QDRANT_URL: "http://destination.test",
        PI_QDRANT_MEMORY_QDRANT_COLLECTION: "pi_memory",
        PI_QDRANT_MEMORY_SOURCE_QDRANT_URL: "http://source.test",
        PI_QDRANT_MEMORY_SOURCE_QDRANT_COLLECTION: "hermes_memory",
        PI_QDRANT_MEMORY_EMBEDDING_DIMENSION: "3",
        PI_QDRANT_MEMORY_EMBEDDING_MODEL: "bge-m3",
        PI_QDRANT_MEMORY_QDRANT_API_KEY: "runtime-key-must-not-administer",
        PI_QDRANT_MEMORY_ADMIN_QDRANT_API_KEY: "admin-key",
        PI_QDRANT_MEMORY_SOURCE_QDRANT_API_KEY: "source-key",
      },
      homeDir: "/home/tester",
      readTextFile: async () => { throw Object.assign(new Error("missing"), { code: "ENOENT" }); },
      fetchImpl,
      writeStdout: (value) => stdout.push(value),
      writeStderr: (value) => stderr.push(value),
    });

    expect(await main([
      "import-hermes", "--target-host", "prime", "--source-model", "bge-m3", "--dry-run", "--json",
    ], deps)).toBe(0);
    const approvedPlanId = (JSON.parse(stdout.join("")) as { planId: string }).planId;
    stdout.length = 0;
    expect(await main([
      "import-hermes", "--target-host", "prime", "--source-model", "bge-m3", "--approve", approvedPlanId, "--json",
    ], deps)).toBe(0);
    expect(stderr).toEqual([]);

    const sourceCalls = calls.filter((call) => call.url.startsWith("http://source.test/"));
    const destinationCalls = calls.filter((call) => call.url.startsWith("http://destination.test/"));
    expect(sourceCalls.length).toBeGreaterThan(0);
    expect(destinationCalls.length).toBeGreaterThan(0);
    expect(sourceCalls.every((call) => call.key === "source-key")).toBe(true);
    expect(destinationCalls.every((call) => call.key === "admin-key")).toBe(true);
    expect(calls.every((call) => call.key !== "runtime-key-must-not-administer")).toBe(true);
    expect(sourceCalls.some((call) => call.method === "PUT")).toBe(false);
    expect(destinationCalls.some((call) => call.url.endsWith("/points?wait=true") && call.method === "PUT")).toBe(true);
  });
});
