// @effect-diagnostics nodeBuiltinImport:off -- the fake CLI runs the configured statusLine command as the real one does.
import * as NodeChildProcess from "node:child_process";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as PtyAdapter from "@t3tools/shared/PtyAdapter";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";

import {
  antigravityStatusLineEnvironment,
  parseAntigravityStatusLine,
  readAntigravityStatusLine,
  supportsAntigravityStatusLine,
} from "./antigravityStatusLine.ts";

// The 1.5.12 payload, with the fields this reader ignores.
const payload = {
  cwd: "/tmp/workspace",
  session_id: "",
  model: { id: "gemini", display_name: "Gemini", effort: "high" },
  context_window: { used_percentage: 0, current_usage: {} },
  quota: {
    "gemini-5h": {
      remaining_fraction: 0.75,
      reset_time: "2026-11-19T17:00:00Z",
      reset_in_seconds: 18_000,
    },
    "3p-weekly": { remaining_fraction: 1, reset_time: "2026-11-26T12:00:00Z" },
    "gemini-argon": { reset_time: "2026-11-19T17:00:00Z" },
  },
  agent_state: "initializing",
  plan_tier: "Google AI Ultra",
  email: "someone@example.com",
};

describe("parseAntigravityStatusLine", () => {
  it("reads quota buckets, e-mail, and plan, inferring each window from its id", () => {
    expect(parseAntigravityStatusLine(JSON.stringify(payload))).toEqual({
      email: "someone@example.com",
      planTier: "Google AI Ultra",
      buckets: [
        {
          bucketId: "gemini-5h",
          remainingFraction: 0.75,
          resetTime: "2026-11-19T17:00:00Z",
          window: "5h",
        },
        {
          bucketId: "3p-weekly",
          remainingFraction: 1,
          resetTime: "2026-11-26T12:00:00Z",
          window: "weekly",
        },
        { bucketId: "gemini-argon", resetTime: "2026-11-19T17:00:00Z" },
      ],
    });
  });

  it("treats a payload before sign-in completes as no account, and rejects non-JSON", () => {
    expect(
      parseAntigravityStatusLine(JSON.stringify({ agent_state: "authenticating", email: " " })),
    ).toEqual({ email: undefined, planTier: undefined, buckets: [] });
    expect(parseAntigravityStatusLine("not json")).toBeUndefined();
    expect(parseAntigravityStatusLine(JSON.stringify({ quota: "full" }))).toBeUndefined();
  });
});

describe("supportsAntigravityStatusLine", () => {
  it.each([
    ["the 1.4 runtime with its harness", "/r/agy_acp_server.par", "/r/localharness_external", null],
    ["an agy link to the 1.4 runtime", "/r/agy_acp_server.par", null, null],
    ["the Windows 1.4 runtime", "C:\\r\\agy_acp_server.exe", null, null],
    ["a CLI before 1.5.0", "/bin/antigravity", null, "1.4.9"],
  ] as const)("rejects %s", (_name, resolvedPath, harnessPath, version) => {
    expect(supportsAntigravityStatusLine({ harnessPath, version }, resolvedPath)).toBe(false);
  });

  it.each([
    ["1.5.0", "1.5.0"],
    ["1.5.12", "1.5.12"],
    ["an unversioned PATH CLI", null],
    ["a non-semver build", "dev"],
  ] as const)("accepts %s", (_name, version) => {
    expect(supportsAntigravityStatusLine({ harnessPath: null, version }, "/bin/agy")).toBe(true);
  });
});

describe("antigravityStatusLineEnvironment", () => {
  it("drops thread attribution and ambient API keys in any case, and pins the runtime", () => {
    expect(
      antigravityStatusLineEnvironment({
        PATH: "/usr/bin",
        T3CODE_THREAD_ID: "thread-1",
        gemini_api_key: "ambient",
        GOOGLE_APPLICATION_CREDENTIALS: "/key.json",
      }),
    ).toEqual({ PATH: "/usr/bin", AGY_CLI_DISABLE_AUTO_UPDATE: "1", ELECTRON_RUN_AS_NODE: "1" });
  });
});

describe.skipIf(HostProcess.Platform.defaultValue() === "win32")(
  "readAntigravityStatusLine",
  () => {
    // Stands in for the CLI: runs the statusLine command the reader configured, as the CLI does.
    const fakeCli = (behavior: {
      readonly emit?: ReadonlyArray<unknown>;
      readonly exitAfterMs?: number;
    }) => {
      const spawned: Array<{ readonly cwd: string; killed: number }> = [];
      const adapter = PtyAdapter.PtyAdapter.of({
        spawn: (input) =>
          Effect.sync(() => {
            const settings = JSON.parse(input.args?.[1] ?? "{}");
            for (const status of behavior.emit ?? []) {
              NodeChildProcess.execFileSync("/bin/sh", ["-c", settings.statusLine.command], {
                input: JSON.stringify(status),
                env: input.env,
              });
            }
            const record = { cwd: input.cwd, killed: 0 };
            spawned.push(record);
            let onExit: ((event: PtyAdapter.PtyExitEvent) => void) | undefined;
            if (behavior.exitAfterMs !== undefined) {
              setTimeout(() => onExit?.({ exitCode: 0, signal: null }), behavior.exitAfterMs);
            }
            return {
              pid: 4242,
              write: () => {},
              resize: () => {},
              kill: () => {
                record.killed += 1;
              },
              onData: () => () => {},
              onExit: (callback) => {
                onExit = callback;
                return () => {};
              },
            };
          }),
      });
      return { spawned, layer: Layer.succeed(PtyAdapter.PtyAdapter, adapter) };
    };
    const read = readAntigravityStatusLine({
      executablePath: "/bin/antigravity",
      nodeExecutable: process.execPath,
      environment: { PATH: process.env.PATH },
    });

    it.live("returns the first payload with a plan, then stops the CLI and removes its files", () =>
      Effect.gen(function* () {
        const cli = fakeCli({ emit: [{ agent_state: "authenticating" }, payload] });
        const result = yield* read.pipe(Effect.provide(cli.layer));
        expect(result._tag === "read" && result.statusLine.planTier).toBe("Google AI Ultra");
        expect(result._tag === "read" && result.statusLine.buckets).toHaveLength(3);
        expect(cli.spawned[0]?.killed).toBe(1);
        expect(yield* (yield* FileSystem.FileSystem).exists(cli.spawned[0]!.cwd)).toBe(false);
      }).pipe(Effect.provide(NodeServices.layer)),
    );

    it.live("keeps quota reported before the CLI exits without a plan", () =>
      Effect.gen(function* () {
        const { plan_tier: _plan, ...withoutPlan } = payload;
        const cli = fakeCli({ emit: [withoutPlan], exitAfterMs: 50 });
        const result = yield* read.pipe(Effect.provide(cli.layer));
        expect(result._tag).toBe("read");
        expect(result._tag === "read" && result.statusLine.planTier).toBeUndefined();
      }).pipe(Effect.provide(NodeServices.layer)),
    );

    it.live("fails when the CLI exits without quota or cannot start", () =>
      Effect.gen(function* () {
        const silent = fakeCli({ emit: [{ agent_state: "authenticating" }], exitAfterMs: 50 });
        expect((yield* read.pipe(Effect.provide(silent.layer)))._tag).toBe("failed");
        const failing = Layer.succeed(
          PtyAdapter.PtyAdapter,
          PtyAdapter.PtyAdapter.of({
            spawn: () => Effect.fail(new PtyAdapter.PtySpawnError({ adapter: "test" })),
          }),
        );
        expect((yield* read.pipe(Effect.provide(failing)))._tag).toBe("failed");
      }).pipe(Effect.provide(NodeServices.layer)),
    );
  },
);
