import * as HostProcess from "@t3tools/shared/HostProcess";
import * as PtyAdapter from "@t3tools/shared/PtyAdapter";
import { compareSemverVersions, parseSemver } from "@t3tools/shared/semver";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

// 1.5.0 is the first CLI with `--settings`; its `statusLine` command receives quota on stdin.
const MIN_CLI_VERSION = "1.5.0";
const TIMEOUT_MS = 15_000;
// The plan tier lands about two seconds after quota; it names the account's plan in Limits.
const PLAN_GRACE_MS = 3_000;
const POLL_INTERVAL = "250 millis";
// The legacy ACP runtime, which writes acp_token.json instead and unpacks about 1 GB per launch.
const LEGACY_RUNTIME = /(?:^|[\\/])agy_acp_server(?:\.exe)?$|\.par$/iu;
const REMOVED_ENVIRONMENT_KEYS = new Set([
  // Keeps listeners of this process out of the agent's Servers panel.
  "T3CODE_THREAD_ID",
  // An ambient key would switch the CLI away from the account T3 signed in.
  "GEMINI_API_KEY",
  "GOOGLE_API_KEY",
  "GOOGLE_APPLICATION_CREDENTIALS",
  "GOOGLE_GENAI_USE_VERTEXAI",
  // An inherited pipe whose owner exited makes Electron-as-Node print Crashpad errors.
  "CHROME_CRASHPAD_PIPE_NAME",
]);

const Payload = Schema.Struct({
  email: Schema.optional(Schema.String),
  plan_tier: Schema.optional(Schema.String),
  quota: Schema.optional(
    Schema.Record(
      Schema.String,
      Schema.Struct({
        remaining_fraction: Schema.optional(Schema.Number),
        reset_time: Schema.optional(Schema.String),
      }),
    ),
  ),
});
const decodePayload = Schema.decodeUnknownOption(Schema.fromJsonString(Payload));
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

export interface AntigravityStatusLine {
  readonly email: string | undefined;
  readonly planTier: string | undefined;
  /** Shaped like `retrieveUserQuotaSummary` buckets, so both read the same way. */
  readonly buckets: ReadonlyArray<{
    readonly bucketId: string;
    readonly remainingFraction?: number;
    readonly resetTime?: string;
    readonly window?: string;
  }>;
}

export type AntigravityStatusLineRead =
  | { readonly _tag: "unsupported" }
  | { readonly _tag: "failed" }
  | {
      readonly _tag: "read";
      readonly statusLine: AntigravityStatusLine;
      readonly observedAt: number;
    };

export function supportsAntigravityStatusLine(
  executable: { readonly harnessPath: string | null; readonly version: string | null },
  resolvedPath: string,
): boolean {
  if (executable.harnessPath !== null || LEGACY_RUNTIME.test(resolvedPath)) return false;
  return (
    executable.version === null ||
    parseSemver(executable.version) === null ||
    compareSemverVersions(executable.version, MIN_CLI_VERSION) >= 0
  );
}

const nonEmpty = (value: string | undefined) => (value?.trim() ? value.trim() : undefined);

export function parseAntigravityStatusLine(text: string): AntigravityStatusLine | undefined {
  const payload = decodePayload(text);
  if (Option.isNone(payload)) return undefined;
  const { email, plan_tier, quota } = payload.value;
  return {
    email: nonEmpty(email),
    planTier: nonEmpty(plan_tier),
    buckets: Object.entries(quota ?? {}).map(([bucketId, bucket]) => {
      const window = bucketId.endsWith("-5h")
        ? "5h"
        : bucketId.endsWith("-weekly")
          ? "weekly"
          : undefined;
      return {
        bucketId,
        ...(bucket.remaining_fraction === undefined
          ? {}
          : { remainingFraction: bucket.remaining_fraction }),
        ...(bucket.reset_time === undefined ? {} : { resetTime: bucket.reset_time }),
        ...(window === undefined ? {} : { window }),
      };
    }),
  };
}

export function antigravityStatusLineEnvironment(base: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  return {
    ...Object.fromEntries(
      Object.entries(base).filter(([key]) => !REMOVED_ENVIRONMENT_KEYS.has(key.toUpperCase())),
    ),
    // The probe must not replace the runtime T3 installed and leases.
    AGY_CLI_DISABLE_AUTO_UPDATE: "1",
    // The capture runs under T3's Node, which is Electron in the desktop app.
    ELECTRON_RUN_AS_NODE: "1",
  };
}

const captureSource = (output: string) => {
  const target = encodeJson(output);
  return [
    'import * as fs from "node:fs";',
    'let text = "";',
    'process.stdin.setEncoding("utf8").on("data", (chunk) => { text += chunk; });',
    'process.stdin.on("end", () => {',
    "  if (!text.trim()) return;",
    `  const temp = ${target} + "." + process.pid;`,
    "  fs.writeFileSync(temp, text);",
    `  fs.renameSync(temp, ${target});`,
    "});",
  ].join("\n");
};

/**
 * Starts the Antigravity CLI in a headless terminal with a temporary `statusLine` command that
 * records each status payload, and returns the first one with quota. Sends no prompt and reads
 * the CLI's own credential, which 1.5+ keeps in the OS keyring rather than in acp_token.json.
 */
export const readAntigravityStatusLine = Effect.fn("readAntigravityStatusLine")(
  function* (input: {
    readonly executablePath: string;
    readonly nodeExecutable: string;
    readonly environment: NodeJS.ProcessEnv;
  }) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const pty = yield* PtyAdapter.PtyAdapter;
    const platform = yield* HostProcess.Platform;
    const quote = (value: string) =>
      platform === "win32" ? `"${value}"` : `'${value.replaceAll("'", `'\\''`)}'`;

    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-agy-status-" });
    const output = path.join(directory, "status.json");
    const capture = path.join(directory, "capture.mjs");
    yield* fs.writeFileString(capture, captureSource(output));
    const settings = encodeJson({
      statusLine: {
        type: "command",
        command: `${quote(input.nodeExecutable)} ${quote(capture)}`,
        enabled: true,
      },
    });
    const exited = yield* Deferred.make<void>();
    // Released before the temp directory, so the CLI stops before its capture script is removed.
    const child = yield* Effect.acquireRelease(
      pty.spawn({
        shell: input.executablePath,
        args: ["--settings", settings],
        cwd: directory,
        cols: 120,
        rows: 30,
        env: antigravityStatusLineEnvironment(input.environment),
      }),
      (child) => Effect.sync(() => child.kill()),
    );
    // Drain the screen so a full terminal buffer cannot stall the CLI.
    child.onData(() => {});
    child.onExit(() => Deferred.doneUnsafe(exited, Exit.void));

    const startedAt = yield* Clock.currentTimeMillis;
    let latest: { statusLine: AntigravityStatusLine; observedAt: number } | undefined;
    let quotaSeenAt: number | undefined;
    while (true) {
      const now = yield* Clock.currentTimeMillis;
      const text = yield* fs.readFileString(output).pipe(Effect.option);
      const statusLine = Option.isSome(text) ? parseAntigravityStatusLine(text.value) : undefined;
      if (statusLine !== undefined && statusLine.buckets.length > 0) {
        latest = { statusLine, observedAt: now };
        quotaSeenAt ??= now;
      }
      if (
        latest !== undefined &&
        (latest.statusLine.planTier !== undefined || now - (quotaSeenAt ?? now) >= PLAN_GRACE_MS)
      ) {
        return latest;
      }
      if (now - startedAt >= TIMEOUT_MS || (yield* Deferred.isDone(exited))) return latest;
      yield* Deferred.await(exited).pipe(Effect.timeoutOption(POLL_INTERVAL));
    }
  },
  Effect.scoped,
  Effect.map((latest): AntigravityStatusLineRead =>
    latest === undefined ? { _tag: "failed" } : { _tag: "read", ...latest },
  ),
  Effect.orElseSucceed((): AntigravityStatusLineRead => ({ _tag: "failed" })),
);
