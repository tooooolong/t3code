import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import {
  buildAntigravityStatusLineProbeEnvironment,
  parseAntigravityStatusLineSnapshot,
  supportsAntigravityStatusLineQuota,
} from "./antigravityStatusLineProbe.ts";

describe("supportsAntigravityStatusLineQuota", () => {
  it.each([
    [
      "legacy 1.4.x with harness",
      { harnessPath: "/bin/localharness_external", version: "1.4.5" },
      false,
    ],
    [
      "harness sibling even with 1.5+ version",
      { harnessPath: "/bin/localharness_external", version: "1.5.12" },
      false,
    ],
    [
      "harness sibling with unknown version",
      { harnessPath: "/bin/localharness_external", version: null },
      false,
    ],
    ["native CLI older than 1.5.0", { harnessPath: null, version: "1.4.9" }, false],
    ["native CLI at 1.5.0", { harnessPath: null, version: "1.5.0" }, true],
    ["native CLI at 1.5.12", { harnessPath: null, version: "1.5.12" }, true],
    ["native CLI at 2.0.0", { harnessPath: null, version: "2.0.0" }, true],
    ["native CLI without parsed version", { harnessPath: null, version: null }, true],
    ["native CLI with non-semver build tag", { harnessPath: null, version: "dev-local" }, true],
  ] as const)("gates %s -> %s", (_label, executable, expected) => {
    expect(supportsAntigravityStatusLineQuota(executable)).toBe(expected);
  });

  it.effect("rejects an agy symlink pointing to a legacy bundle or harness sibling", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped();
      const legacyDir = path.join(root, "legacy");
      const binDir = path.join(root, "bin");
      yield* fs.makeDirectory(legacyDir, { recursive: true });
      yield* fs.makeDirectory(binDir, { recursive: true });

      const parPath = path.join(legacyDir, "agy_acp_server.par");
      const harnessPath = path.join(legacyDir, "localharness_external");
      yield* fs.writeFileString(parPath, "#!/bin/sh\n");
      yield* fs.writeFileString(harnessPath, "#!/bin/sh\n");
      const symlinkPath = path.join(binDir, "agy");
      yield* fs.symlink(parPath, symlinkPath);

      expect(
        supportsAntigravityStatusLineQuota({
          executablePath: symlinkPath,
          harnessPath: null,
          version: null,
        }),
      ).toBe(false);

      const nativeDir = path.join(root, "native");
      yield* fs.makeDirectory(nativeDir, { recursive: true });
      const nativePath = path.join(nativeDir, "antigravity");
      yield* fs.writeFileString(nativePath, "#!/bin/sh\n");
      const nativeLink = path.join(binDir, "agy-native");
      yield* fs.symlink(nativePath, nativeLink);

      expect(
        supportsAntigravityStatusLineQuota({
          executablePath: nativeLink,
          harnessPath: null,
          version: null,
        }),
      ).toBe(true);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

describe("buildAntigravityStatusLineProbeEnvironment", () => {
  it("uses the instance profileDirectory, strips ambient credentials, and applies instance GCP settings", () => {
    const env = buildAntigravityStatusLineProbeEnvironment({
      baseEnv: {
        HOME: "/Users/custom",
        AGY_ACCOUNT: "work@example.com",
        T3CODE_THREAD_ID: "thread-1",
        CHROME_CRASHPAD_PIPE_NAME: "/tmp/crashpad",
        GEMINI_API_KEY: "ambient-key",
        GOOGLE_CLOUD_PROJECT: "ambient-proj",
      },
      profileDirectory: "/state/providers/antigravity/instance-a",
      tempDir: "/tmp/probe-scratch",
      platform: "darwin",
      gcpProject: "work-proj",
      gcpLocation: "us-central1",
    });

    expect(env.GEMINI_HOME).toBe("/state/providers/antigravity/instance-a");
    expect(env.TMPDIR).toBe("/tmp/probe-scratch");
    expect(env.HOME).toBe("/Users/custom");
    expect(env.AGY_ACCOUNT).toBe("work@example.com");
    expect(env.AGY_CLI_DISABLE_AUTO_UPDATE).toBe("1");
    expect(env.GOOGLE_CLOUD_PROJECT).toBe("work-proj");
    expect(env.GOOGLE_CLOUD_LOCATION).toBe("us-central1");
    expect(env.T3CODE_THREAD_ID).toBeUndefined();
    expect(env.CHROME_CRASHPAD_PIPE_NAME).toBeUndefined();
    expect(env.GEMINI_API_KEY).toBeUndefined();
  });
});

describe("parseAntigravityStatusLineSnapshot", () => {
  const requestedAt = Date.parse("2026-11-19T12:00:00.000Z");

  it("parses snake_case 1.5.x statusLine payloads and infers windows", () => {
    const parsed = parseAntigravityStatusLineSnapshot(
      {
        version: "1.5.12",
        agent_state: "idle",
        email: "user@example.com",
        plan_tier: "Google AI Ultra",
        quota: {
          "3p-5h": {
            remaining_fraction: 1,
            reset_time: "2026-11-19T17:00:00Z",
            reset_in_seconds: 18000,
          },
          "gemini-weekly": {
            remaining_fraction: 0.75,
            reset_in_seconds: 3600,
            display_name: "Gemini",
          },
          "credits-balance": {
            remaining_amount: 42,
            display_name: "Credits",
          },
          "disabled-bucket": {
            remaining_fraction: 0,
            disabled: true,
          },
          "missing-fraction": {
            reset_time: "2026-11-19T17:00:00Z",
          },
        },
      },
      requestedAt,
    );

    expect(parsed).toEqual({
      version: "1.5.12",
      agentState: "idle",
      email: "user@example.com",
      planTier: "Google AI Ultra",
      buckets: [
        {
          bucketId: "3p-5h",
          remainingFraction: 1,
          resetTime: "2026-11-19T17:00:00Z",
          window: "5h",
        },
        {
          bucketId: "gemini-weekly",
          remainingFraction: 0.75,
          resetTime: "2026-11-19T13:00:00.000Z",
          window: "weekly",
          displayName: "Gemini",
        },
        {
          bucketId: "credits-balance",
          remainingAmount: 42,
          displayName: "Credits",
        },
        {
          bucketId: "disabled-bucket",
          remainingFraction: 0,
          disabled: true,
        },
      ],
    });
  });

  it("tolerates camelCase fields and rejects non-object inputs", () => {
    expect(parseAntigravityStatusLineSnapshot(null, requestedAt)).toBeUndefined();
    expect(parseAntigravityStatusLineSnapshot([], requestedAt)).toBeUndefined();
    expect(parseAntigravityStatusLineSnapshot("idle", requestedAt)).toBeUndefined();

    expect(
      parseAntigravityStatusLineSnapshot(
        {
          agentState: "authenticating",
          planTier: "Google AI Pro",
          quota: {
            custom: {
              window: "5h",
              remainingFraction: 0.4,
              resetTime: "2026-11-19T15:00:00Z",
              displayName: "Custom Pool",
            },
          },
        },
        requestedAt,
      ),
    ).toEqual({
      agentState: "authenticating",
      planTier: "Google AI Pro",
      buckets: [
        {
          bucketId: "custom",
          remainingFraction: 0.4,
          resetTime: "2026-11-19T15:00:00Z",
          window: "5h",
          displayName: "Custom Pool",
        },
      ],
    });
  });
});
