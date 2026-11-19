// @effect-diagnostics nodeBuiltinImport:off -- node:sqlite reads the native session databases read-only.
import * as NodeSqlite from "node:sqlite";

import * as Effect from "effect/Effect";

import { antigravityManagedPath, antigravityProfileRoot } from "./AntigravityProfilePaths.ts";

const NATIVE_SESSION_ID = /^[\w-]+$/;
const APP_DATA_DIR = "antigravity-acp";
const CONVERSATION_SEGMENTS = [APP_DATA_DIR, "conversations"] as const;
const CONVERSATION_SUMMARIES_SEGMENTS = [APP_DATA_DIR, "conversation_summaries.db"] as const;

// StepStatus enum values in Antigravity's trajectory database (exa.cortex_pb).
const STEP_STATUS_PENDING = 1;
const STEP_STATUS_RUNNING = 2;
const STEP_STATUS_GENERATING = 8;

export type AntigravityContinuationActivity = "active" | "idle" | "unknown";

const readSqlite = <A>(file: string, read: (database: NodeSqlite.DatabaseSync) => A) =>
  Effect.try({
    try: () => {
      const database = new NodeSqlite.DatabaseSync(file, { readOnly: true });
      try {
        database.exec("PRAGMA busy_timeout = 100");
        return read(database);
      } finally {
        database.close();
      }
    },
    catch: () => undefined,
  });

const hasTable = (database: NodeSqlite.DatabaseSync, table: string): boolean =>
  database.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(table) !==
  undefined;

const resolveManagedFile = (
  profileDirectory: string,
  segments: ReadonlyArray<string>,
): Effect.Effect<string | undefined> =>
  Effect.tryPromise({
    try: async () => {
      const root = await antigravityProfileRoot(profileDirectory);
      if (root === undefined) return undefined;
      const entry = await antigravityManagedPath(root, segments, "file");
      return entry?.path;
    },
    catch: () => undefined,
  }).pipe(Effect.orElseSucceed(() => undefined));

const readConversationStepsActivity = (
  profileDirectory: string,
  sessionId: string,
  query: (database: NodeSqlite.DatabaseSync) => boolean,
): Effect.Effect<AntigravityContinuationActivity> => {
  if (!NATIVE_SESSION_ID.test(sessionId)) return Effect.succeed("unknown");
  return resolveManagedFile(profileDirectory, [...CONVERSATION_SEGMENTS, `${sessionId}.db`]).pipe(
    Effect.flatMap((file) =>
      file === undefined
        ? Effect.succeed<AntigravityContinuationActivity>("unknown")
        : readSqlite<AntigravityContinuationActivity>(file, (database) =>
            hasTable(database, "steps") ? (query(database) ? "active" : "idle") : "unknown",
          ),
    ),
    Effect.orElseSucceed((): AntigravityContinuationActivity => "unknown"),
  );
};

const readConversationSummariesActivity = (
  profileDirectory: string,
  query: (database: NodeSqlite.DatabaseSync) => boolean,
): Effect.Effect<AntigravityContinuationActivity> =>
  resolveManagedFile(profileDirectory, CONVERSATION_SUMMARIES_SEGMENTS).pipe(
    Effect.flatMap((file) =>
      file === undefined
        ? Effect.succeed<AntigravityContinuationActivity>("unknown")
        : readSqlite<AntigravityContinuationActivity>(file, (database) =>
            hasTable(database, "conversation_summaries")
              ? query(database)
                ? "active"
                : "idle"
              : "unknown",
          ),
    ),
    Effect.orElseSucceed((): AntigravityContinuationActivity => "unknown"),
  );

/**
 * Checks whether an Antigravity session still has native background work (a running background
 * command/timer step in its trajectory DB, or a non-idle root/child conversation in
 * `conversation_summaries.db`) so idle session release does not terminate it before callbacks land.
 */
export const hasAntigravityPendingBackgroundWork = Effect.fn(
  "AntigravityBackgroundWork.hasPendingBackgroundWork",
)(function* (profileDirectory: string, sessionId: string): Effect.fn.Return<boolean> {
  if (!NATIVE_SESSION_ID.test(sessionId)) return false;

  const hasRunningBackgroundStep = yield* readConversationStepsActivity(
    profileDirectory,
    sessionId,
    (database) =>
      database
        .prepare("SELECT 1 FROM steps WHERE status = ? AND task_details IS NOT NULL LIMIT 1")
        .get(STEP_STATUS_RUNNING) !== undefined,
  );
  if (hasRunningBackgroundStep === "active") return true;

  const hasActiveSummary = yield* readConversationSummariesActivity(
    profileDirectory,
    (database) =>
      database
        .prepare(
          `SELECT 1 FROM conversation_summaries
           WHERE (conversation_id = ? AND (status = 'CASCADE_RUN_STATUS_RUNNING' OR not_fully_idle != 0))
              OR (parent_conversation_id = ? AND killed = 0 AND (status = 'CASCADE_RUN_STATUS_RUNNING' OR not_fully_idle != 0))
           LIMIT 1`,
        )
        .get(sessionId, sessionId) !== undefined,
  );
  return hasActiveSummary === "active";
});

/**
 * Checks whether the root Antigravity session is still actively running a turn
 * (`CASCADE_RUN_STATUS_RUNNING`, or a generating/pending/running foreground step).
 * Returns `"unknown"` when neither native database can be read so callers fall back to ACP state.
 */
export const isAntigravityContinuationStillActive = Effect.fn(
  "AntigravityBackgroundWork.isContinuationStillActive",
)(function* (
  profileDirectory: string,
  sessionId: string,
): Effect.fn.Return<AntigravityContinuationActivity> {
  if (!NATIVE_SESSION_ID.test(sessionId)) return "unknown";

  const summaryState = yield* readConversationSummariesActivity(
    profileDirectory,
    (database) =>
      database
        .prepare(
          `SELECT 1 FROM conversation_summaries
           WHERE conversation_id = ? AND status = 'CASCADE_RUN_STATUS_RUNNING'
           LIMIT 1`,
        )
        .get(sessionId) !== undefined,
  );
  if (summaryState === "active") return "active";

  const stepsState = yield* readConversationStepsActivity(
    profileDirectory,
    sessionId,
    (database) =>
      database
        .prepare(
          "SELECT 1 FROM steps WHERE status = ? OR (status IN (?, ?) AND task_details IS NULL) LIMIT 1",
        )
        .get(STEP_STATUS_GENERATING, STEP_STATUS_PENDING, STEP_STATUS_RUNNING) !== undefined,
  );
  if (stepsState === "active") return "active";
  if (summaryState === "unknown" && stepsState === "unknown") return "unknown";
  return "idle";
});
