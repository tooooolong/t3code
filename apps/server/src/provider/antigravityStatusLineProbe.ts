// @effect-diagnostics nodeBuiltinImport:off -- node-pty, realpath, and atomic temp capture require Node builtins.
import * as NodeFS from "node:fs";
import * as NodeModule from "node:module";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as HostProcess from "@t3tools/shared/HostProcess";
import { compareSemverVersions, parseSemver } from "@t3tools/shared/semver";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

const MIN_STATUS_LINE_CLI_VERSION = "1.5.0";
const STATUS_LINE_POLL_INTERVAL_MS = 150;
const STATUS_LINE_PLAN_GRACE_MS = 2_500;
const STATUS_LINE_TIMEOUT_MS = 15_000;
const STATUS_LINE_EXIT_WAIT_MS = 500;

const LEGACY_EXECUTABLE_PATTERN = /^agy_acp_server(?:\.(?:par|exe))?$|\.par$/iu;
const LEGACY_HARNESS_NAMES = ["localharness_external", "localharness_external.exe"] as const;

const requireForNodePty = NodeModule.createRequire(import.meta.url);
let didEnsureSpawnHelperExecutable = false;

const decodeUnknownJson = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Unknown));
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

export interface AntigravityStatusLineBucket {
  readonly bucketId: string;
  readonly remainingFraction?: number;
  readonly remainingAmount?: number;
  readonly resetTime?: string;
  readonly window?: string;
  readonly displayName?: string;
  readonly disabled?: boolean;
}

export interface AntigravityStatusLineSnapshot {
  readonly version?: string;
  readonly agentState?: string;
  readonly email?: string;
  readonly planTier?: string;
  readonly buckets: ReadonlyArray<AntigravityStatusLineBucket>;
}

export type AntigravityStatusLineReadResult =
  | { readonly _tag: "unsupported" }
  | {
      readonly _tag: "ok";
      readonly snapshot: AntigravityStatusLineSnapshot;
      readonly requestedAt: number;
    }
  | { readonly _tag: "failed" };

function isLegacyBundleTarget(executablePath: string): boolean {
  let resolved = executablePath;
  try {
    resolved = NodeFS.realpathSync(executablePath);
  } catch {}
  const baseName = resolved.split(/[\\/]/u).at(-1) ?? "";
  if (LEGACY_EXECUTABLE_PATTERN.test(baseName)) return true;
  const directory = NodePath.dirname(resolved);
  for (const harnessName of LEGACY_HARNESS_NAMES) {
    try {
      if (NodeFS.statSync(NodePath.join(directory, harnessName)).isFile()) {
        return true;
      }
    } catch {}
  }
  return false;
}

/**
 * Only the native Go Antigravity CLI (`>= 1.5.0`, no `localharness_external` sibling even behind
 * symlinks) supports `--settings` + `statusLine`. Legacy 1.4.x PyInstaller bundles unpack ~1 GB
 * per launch and write `acp_token.json` instead, so they must never run the PTY probe.
 */
export function supportsAntigravityStatusLineQuota(executable: {
  readonly executablePath?: string;
  readonly harnessPath: string | null;
  readonly version: string | null;
}): boolean {
  if (executable.harnessPath !== null) return false;
  if (executable.executablePath !== undefined && isLegacyBundleTarget(executable.executablePath)) {
    return false;
  }
  if (executable.version === null) return true;
  if (parseSemver(executable.version) === null) return true;
  return compareSemverVersions(executable.version, MIN_STATUS_LINE_CLI_VERSION) >= 0;
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function nonEmptyString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : undefined;
}

function finiteNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function inferBucketWindow(bucketId: string, explicit: string | undefined): string | undefined {
  if (explicit) return explicit;
  if (bucketId.endsWith("-5h")) return "5h";
  if (bucketId.endsWith("-weekly")) return "weekly";
  return undefined;
}

/**
 * Tolerantly parses the JSON payload piped to Antigravity's `statusLine.command`.
 * Accepts both snake_case (1.5.x) and camelCase keys, plus `remaining_amount`-only buckets.
 */
export function parseAntigravityStatusLineSnapshot(
  raw: unknown,
  requestedAt: number,
): AntigravityStatusLineSnapshot | undefined {
  const record = asRecord(raw);
  if (!record) return undefined;
  const version = nonEmptyString(record.version);
  const agentState = nonEmptyString(record.agent_state ?? record.agentState);
  const email = nonEmptyString(record.email);
  const planTier = nonEmptyString(record.plan_tier ?? record.planTier);
  const quotaRecord = asRecord(record.quota);
  const buckets: AntigravityStatusLineBucket[] = [];
  if (quotaRecord) {
    for (const [bucketId, entry] of Object.entries(quotaRecord)) {
      if (!bucketId.trim()) continue;
      const bucket = asRecord(entry);
      if (!bucket) continue;
      const disabled = bucket.disabled === true;
      const remainingFraction = finiteNumber(bucket.remaining_fraction ?? bucket.remainingFraction);
      const remainingAmount = finiteNumber(bucket.remaining_amount ?? bucket.remainingAmount);
      if (remainingFraction === undefined && remainingAmount === undefined) continue;
      const explicitReset = nonEmptyString(bucket.reset_time ?? bucket.resetTime);
      const resetInSeconds = finiteNumber(bucket.reset_in_seconds ?? bucket.resetInSeconds);
      const computedReset =
        resetInSeconds !== undefined && resetInSeconds >= 0
          ? Option.map(
              DateTime.make(requestedAt + resetInSeconds * 1_000),
              DateTime.formatIso,
            ).pipe(Option.getOrUndefined)
          : undefined;
      const resetTime = explicitReset ?? computedReset;
      const window = inferBucketWindow(bucketId, nonEmptyString(bucket.window));
      const displayName = nonEmptyString(bucket.display_name ?? bucket.displayName);
      buckets.push({
        bucketId,
        ...(remainingFraction !== undefined ? { remainingFraction } : {}),
        ...(remainingAmount !== undefined ? { remainingAmount } : {}),
        ...(resetTime ? { resetTime } : {}),
        ...(window ? { window } : {}),
        ...(displayName ? { displayName } : {}),
        ...(disabled ? { disabled: true } : {}),
      });
    }
  }
  return {
    ...(version ? { version } : {}),
    ...(agentState ? { agentState } : {}),
    ...(email ? { email } : {}),
    ...(planTier ? { planTier } : {}),
    buckets,
  };
}

function ensureNodePtySpawnHelperExecutable(platform: NodeJS.Platform, arch: NodeJS.Architecture) {
  if (didEnsureSpawnHelperExecutable || platform === "win32") return;
  didEnsureSpawnHelperExecutable = true;
  try {
    const packageDir = NodePath.dirname(requireForNodePty.resolve("node-pty/package.json"));
    for (const candidate of [
      NodePath.join(packageDir, "build", "Release", "spawn-helper"),
      NodePath.join(packageDir, "build", "Debug", "spawn-helper"),
      NodePath.join(packageDir, "prebuilds", `${platform}-${arch}`, "spawn-helper"),
    ]) {
      try {
        const stat = NodeFS.statSync(candidate);
        if (stat.isFile() && (stat.mode & 0o111) === 0) {
          NodeFS.chmodSync(candidate, stat.mode | 0o755);
        }
      } catch {}
    }
  } catch {}
}

function quoteCommandArg(value: string, platform: NodeJS.Platform): string {
  if (platform === "win32") {
    return `"${value.replaceAll('"', '\\"')}"`;
  }
  return `'${value.replaceAll("'", "'\\''")}'`;
}

const strippedProbeEnvKeys = new Set([
  "T3CODE_THREAD_ID",
  "CHROME_CRASHPAD_PIPE_NAME",
  "GEMINI_API_KEY",
  "GOOGLE_API_KEY",
  "GOOGLE_APPLICATION_CREDENTIALS",
  "GOOGLE_GENAI_USE_VERTEXAI",
  "GOOGLE_CLOUD_PROJECT",
  "GOOGLE_CLOUD_LOCATION",
  "GOOGLE_CLOUD_QUOTA_PROJECT",
  "GCLOUD_PROJECT",
  "CLOUDSDK_CORE_PROJECT",
]);

export function buildAntigravityStatusLineProbeEnvironment(input: {
  readonly baseEnv: NodeJS.ProcessEnv;
  readonly profileDirectory: string;
  readonly tempDir: string;
  readonly platform: NodeJS.Platform;
  readonly gcpProject?: string;
  readonly gcpLocation?: string;
}): Record<string, string> {
  const env: Record<string, string> = {};
  for (const [key, value] of Object.entries(input.baseEnv)) {
    if (value === undefined) continue;
    if (strippedProbeEnvKeys.has(key.toUpperCase())) continue;
    env[key] = value;
  }
  env.GEMINI_HOME = input.profileDirectory;
  env.AGY_ACP_FORCE_FILE_STORAGE = "1";
  env.AGY_CLI_DISABLE_AUTO_UPDATE = "1";
  env.ELECTRON_RUN_AS_NODE = "1";
  if (input.gcpProject) env.GOOGLE_CLOUD_PROJECT = input.gcpProject;
  if (input.gcpLocation) env.GOOGLE_CLOUD_LOCATION = input.gcpLocation;
  env.TERM = env.TERM && env.TERM !== "dumb" ? env.TERM : "xterm-256color";
  if (input.platform === "win32") {
    env.TEMP = input.tempDir;
    env.TMP = input.tempDir;
  } else {
    env.TMPDIR = input.tempDir;
  }
  return env;
}

function makeCaptureScriptSource(outFile: string): string {
  const tempPrefix = encodeJson(`${outFile}.tmp.`);
  const targetPath = encodeJson(outFile);
  return [
    'import * as fs from "node:fs";',
    'let data = "";',
    'process.stdin.setEncoding("utf8");',
    'process.stdin.on("data", (chunk) => { data += chunk; });',
    'process.stdin.on("end", () => {',
    "  const trimmed = data.trim();",
    "  if (!trimmed) return;",
    `  const tmp = ${tempPrefix} + process.pid;`,
    "  try {",
    "    fs.writeFileSync(tmp, trimmed);",
    `    fs.renameSync(tmp, ${targetPath});`,
    "  } catch {}",
    "});",
  ].join("\n");
}

function terminatePtyProcess(term: import("node-pty").IPty, platform: NodeJS.Platform) {
  if (platform === "win32") {
    // Match NodePtyAdapter: private `_agent.kill()` cancels startup before the first output byte.
    if (
      "_agent" in term &&
      typeof term._agent === "object" &&
      term._agent !== null &&
      "kill" in term._agent &&
      typeof term._agent.kill === "function"
    ) {
      try {
        term._agent.kill();
      } catch {}
    }
    try {
      term.kill();
    } catch {}
    return;
  }
  if (Number.isInteger(term.pid) && term.pid > 0) {
    try {
      process.kill(-term.pid, "SIGTERM");
    } catch {}
  }
  try {
    term.kill();
  } catch {}
}

function readSnapshotFile(
  outFile: string,
  requestedAt: number,
): AntigravityStatusLineSnapshot | undefined {
  try {
    if (!NodeFS.existsSync(outFile)) return undefined;
    const raw = NodeFS.readFileSync(outFile, "utf8");
    const decoded = decodeUnknownJson(raw);
    return Option.isSome(decoded)
      ? parseAntigravityStatusLineSnapshot(decoded.value, requestedAt)
      : undefined;
  } catch {
    return undefined;
  }
}

function finalizeSnapshot(
  snapshot: AntigravityStatusLineSnapshot | undefined,
  observedAt: number,
): AntigravityStatusLineReadResult {
  if (snapshot && snapshot.buckets.length > 0) {
    return { _tag: "ok", snapshot, requestedAt: observedAt };
  }
  // Treat slow authentication or silent timeouts as retryable failures so existing limits are kept.
  return { _tag: "failed" };
}

/**
 * Runs a short-lived headless PTY instance of the native Antigravity CLI (`--settings` with a
 * temporary `statusLine.command`) to read live quota buckets, email, and plan tier without
 * touching OS keychains or sending any LLM prompt.
 */
export const readAntigravityStatusLine = Effect.fn("AntigravityStatusLine.read")(function* (input: {
  readonly executablePath: string;
  readonly nodeExecutablePath: string;
  readonly profileDirectory: string;
  readonly baseEnv: NodeJS.ProcessEnv;
  readonly gcpProject?: string;
  readonly gcpLocation?: string;
  readonly platform?: NodeJS.Platform;
  readonly architecture?: NodeJS.Architecture;
  readonly timeoutMs?: number;
  readonly planGraceMs?: number;
}): Effect.fn.Return<AntigravityStatusLineReadResult> {
  const platform = input.platform ?? (yield* HostProcess.Platform);
  const architecture = input.architecture ?? (yield* HostProcess.Architecture);
  const timeoutMs = input.timeoutMs ?? STATUS_LINE_TIMEOUT_MS;
  const planGraceMs = input.planGraceMs ?? STATUS_LINE_PLAN_GRACE_MS;

  return yield* Effect.scoped(
    Effect.gen(function* () {
      const exitDeferred = yield* Deferred.make<void>();
      let exited = false;

      const session = yield* Effect.acquireRelease(
        Effect.sync(() => {
          ensureNodePtySpawnHelperExecutable(platform, architecture);
          const nodePty = requireForNodePty("node-pty") as typeof import("node-pty");
          const tempDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-agy-status-"));
          try {
            const outFile = NodePath.join(tempDir, "latest.json");
            const captureScript = NodePath.join(tempDir, "capture.mjs");
            NodeFS.writeFileSync(captureScript, makeCaptureScriptSource(outFile), { mode: 0o600 });

            const statusCommand = `${quoteCommandArg(input.nodeExecutablePath, platform)} ${quoteCommandArg(captureScript, platform)}`;
            const settings = encodeJson({
              statusLine: {
                type: "command",
                command: statusCommand,
                enabled: true,
              },
            });
            const env = buildAntigravityStatusLineProbeEnvironment({
              baseEnv: input.baseEnv,
              profileDirectory: input.profileDirectory,
              tempDir,
              platform,
              ...(input.gcpProject ? { gcpProject: input.gcpProject } : {}),
              ...(input.gcpLocation ? { gcpLocation: input.gcpLocation } : {}),
            });
            const term = nodePty.spawn(input.executablePath, ["--settings", settings], {
              name: "xterm-256color",
              cols: 120,
              rows: 30,
              cwd: tempDir,
              env,
            });
            term.onData(() => {});
            term.onExit(() => {
              exited = true;
              Deferred.doneUnsafe(exitDeferred, Exit.void);
            });
            return { tempDir, outFile, term };
          } catch (error) {
            try {
              NodeFS.rmSync(tempDir, { recursive: true, force: true });
            } catch {}
            throw error;
          }
        }),
        ({ tempDir, term }) =>
          Effect.gen(function* () {
            yield* Effect.sync(() => {
              terminatePtyProcess(term, platform);
            });
            if (!exited) {
              yield* Deferred.await(exitDeferred).pipe(
                Effect.timeoutOption(STATUS_LINE_EXIT_WAIT_MS),
              );
            }
            yield* Effect.sync(() => {
              try {
                NodeFS.rmSync(tempDir, { recursive: true, force: true });
              } catch {}
            });
          }),
      );

      const startedAt = yield* Clock.currentTimeMillis;
      let latestSnapshot: AntigravityStatusLineSnapshot | undefined;
      let latestObservedAt = startedAt;
      let quotaFirstSeenAt: number | undefined;

      while (true) {
        const now = yield* Clock.currentTimeMillis;
        const parsed = readSnapshotFile(session.outFile, now);
        if (parsed) {
          latestSnapshot = parsed;
          latestObservedAt = now;
        }

        if (latestSnapshot && latestSnapshot.buckets.length > 0) {
          quotaFirstSeenAt ??= now;
          if (latestSnapshot.planTier || now - quotaFirstSeenAt >= planGraceMs) {
            return {
              _tag: "ok",
              snapshot: latestSnapshot,
              requestedAt: latestObservedAt,
            } as const;
          }
        }

        if (exited || now - startedAt >= timeoutMs) {
          return finalizeSnapshot(latestSnapshot, latestObservedAt);
        }

        yield* Deferred.await(exitDeferred).pipe(
          Effect.timeoutOption(STATUS_LINE_POLL_INTERVAL_MS),
        );
      }
    }),
  ).pipe(
    Effect.catchDefect(() => Effect.succeed<AntigravityStatusLineReadResult>({ _tag: "failed" })),
  );
});
