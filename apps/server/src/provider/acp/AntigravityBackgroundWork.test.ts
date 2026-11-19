import * as NodeSqlite from "node:sqlite";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import {
  hasAntigravityPendingBackgroundWork,
  isAntigravityContinuationStillActive,
} from "./AntigravityBackgroundWork.ts";

const STEP_STATUS_PENDING = 1;
const STEP_STATUS_RUNNING = 2;
const STEP_STATUS_DONE = 3;
const STEP_STATUS_CANCELED = 6;
const STEP_STATUS_ERROR = 7;
const STEP_STATUS_GENERATING = 8;

const createStepsDb = (
  filePath: string,
  rows: ReadonlyArray<{
    readonly idx: number;
    readonly status: number;
    readonly taskDetails?: Uint8Array | null;
  }>,
) => {
  const database = new NodeSqlite.DatabaseSync(filePath);
  try {
    database.exec(
      "CREATE TABLE IF NOT EXISTS steps (idx INTEGER PRIMARY KEY, step_type INTEGER NOT NULL DEFAULT 0, status INTEGER NOT NULL DEFAULT 0, task_details BLOB)",
    );
    database.exec("DELETE FROM steps");
    const insert = database.prepare(
      "INSERT INTO steps (idx, step_type, status, task_details) VALUES (?, 132, ?, ?)",
    );
    for (const row of rows) {
      insert.run(row.idx, row.status, row.taskDetails ?? null);
    }
  } finally {
    database.close();
  }
};

const createSummariesDb = (
  filePath: string,
  rows: ReadonlyArray<{
    readonly conversationId: string;
    readonly status: string;
    readonly notFullyIdle: number;
    readonly killed?: number;
    readonly parentConversationId?: string;
  }>,
) => {
  const database = new NodeSqlite.DatabaseSync(filePath);
  try {
    database.exec(
      `CREATE TABLE IF NOT EXISTS conversation_summaries (
        conversation_id TEXT PRIMARY KEY,
        status TEXT NOT NULL DEFAULT '',
        parent_conversation_id TEXT NOT NULL DEFAULT '',
        not_fully_idle NUMERIC NOT NULL DEFAULT 0,
        killed NUMERIC NOT NULL DEFAULT 0
      )`,
    );
    database.exec("DELETE FROM conversation_summaries");
    const insert = database.prepare(
      `INSERT INTO conversation_summaries (
        conversation_id, status, parent_conversation_id, not_fully_idle, killed
      ) VALUES (?, ?, ?, ?, ?)`,
    );
    for (const row of rows) {
      insert.run(
        row.conversationId,
        row.status,
        row.parentConversationId ?? "",
        row.notFullyIdle,
        row.killed ?? 0,
      );
    }
  } finally {
    database.close();
  }
};

describe("AntigravityBackgroundWork", () => {
  it.effect("detects running background tasks and active child conversations", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const profileDirectory = yield* fs.makeTempDirectoryScoped();
      const sessionId = "3863d8b2-14c8-46ec-b009-03b95ae13aa6";
      const appDataDir = path.join(profileDirectory, "antigravity-acp");
      const conversationsDir = path.join(appDataDir, "conversations");
      yield* fs.makeDirectory(conversationsDir, { recursive: true });
      const stepsDb = path.join(conversationsDir, `${sessionId}.db`);
      const summariesDb = path.join(appDataDir, "conversation_summaries.db");

      // Missing files degrade safely: background work is false, continuation activity is unknown.
      assert.isFalse(yield* hasAntigravityPendingBackgroundWork(profileDirectory, sessionId));
      assert.strictEqual(
        yield* isAntigravityContinuationStillActive(profileDirectory, sessionId),
        "unknown",
      );

      // A running background command (status = 2 with task_details) pins background work,
      // while not counting as an active foreground step for continuation debounce.
      createStepsDb(stepsDb, [
        { idx: 1, status: STEP_STATUS_DONE, taskDetails: null },
        { idx: 2, status: STEP_STATUS_RUNNING, taskDetails: Buffer.from("task-2") },
      ]);
      assert.isTrue(yield* hasAntigravityPendingBackgroundWork(profileDirectory, sessionId));
      assert.strictEqual(
        yield* isAntigravityContinuationStillActive(profileDirectory, sessionId),
        "idle",
      );

      // Once the background command completes, cancels, or errors, background work clears.
      for (const terminalStatus of [STEP_STATUS_DONE, STEP_STATUS_CANCELED, STEP_STATUS_ERROR]) {
        createStepsDb(stepsDb, [
          { idx: 2, status: terminalStatus, taskDetails: Buffer.from("task-2") },
        ]);
        assert.isFalse(yield* hasAntigravityPendingBackgroundWork(profileDirectory, sessionId));
      }

      // An actively generating step (status = 8) or pending/running foreground step
      // (status IN (1, 2), no task_details) marks continuation as still active.
      createStepsDb(stepsDb, [{ idx: 3, status: STEP_STATUS_GENERATING, taskDetails: null }]);
      assert.strictEqual(
        yield* isAntigravityContinuationStillActive(profileDirectory, sessionId),
        "active",
      );

      createStepsDb(stepsDb, [{ idx: 4, status: STEP_STATUS_PENDING, taskDetails: null }]);
      assert.strictEqual(
        yield* isAntigravityContinuationStillActive(profileDirectory, sessionId),
        "active",
      );

      createStepsDb(stepsDb, [{ idx: 4, status: STEP_STATUS_RUNNING, taskDetails: null }]);
      assert.strictEqual(
        yield* isAntigravityContinuationStillActive(profileDirectory, sessionId),
        "active",
      );

      createStepsDb(stepsDb, [{ idx: 4, status: STEP_STATUS_DONE, taskDetails: null }]);
      assert.strictEqual(
        yield* isAntigravityContinuationStillActive(profileDirectory, sessionId),
        "idle",
      );

      // Root CASCADE_RUN_STATUS_RUNNING keeps continuation active even while all persisted steps
      // are DONE (e.g. during pre-first-token LLM thinking before the next step row is written).
      createSummariesDb(summariesDb, [
        {
          conversationId: sessionId,
          status: "CASCADE_RUN_STATUS_RUNNING",
          notFullyIdle: 1,
        },
      ]);
      assert.strictEqual(
        yield* isAntigravityContinuationStillActive(profileDirectory, sessionId),
        "active",
      );

      // Running or non-idle child subagent in conversation_summaries.db pins background work,
      // but does NOT keep the root's continuation active once the root itself is IDLE.
      createSummariesDb(summariesDb, [
        {
          conversationId: sessionId,
          status: "CASCADE_RUN_STATUS_IDLE",
          notFullyIdle: 0,
        },
        {
          conversationId: "child-subagent-1",
          parentConversationId: sessionId,
          status: "CASCADE_RUN_STATUS_RUNNING",
          notFullyIdle: 1,
          killed: 0,
        },
      ]);
      assert.isTrue(yield* hasAntigravityPendingBackgroundWork(profileDirectory, sessionId));
      assert.strictEqual(
        yield* isAntigravityContinuationStillActive(profileDirectory, sessionId),
        "idle",
      );

      // Killed or idle children do not pin background work.
      createSummariesDb(summariesDb, [
        {
          conversationId: sessionId,
          status: "CASCADE_RUN_STATUS_IDLE",
          notFullyIdle: 0,
        },
        {
          conversationId: "child-subagent-1",
          parentConversationId: sessionId,
          status: "CASCADE_RUN_STATUS_RUNNING",
          notFullyIdle: 1,
          killed: 1,
        },
        {
          conversationId: "child-subagent-2",
          parentConversationId: sessionId,
          status: "CASCADE_RUN_STATUS_IDLE",
          notFullyIdle: 0,
          killed: 0,
        },
      ]);
      assert.isFalse(yield* hasAntigravityPendingBackgroundWork(profileDirectory, sessionId));

      // Root not_fully_idle flag with IDLE status pins background work (e.g. a chained background
      // task is running), while allowing the root continuation turn itself to settle.
      createSummariesDb(summariesDb, [
        {
          conversationId: sessionId,
          status: "CASCADE_RUN_STATUS_IDLE",
          notFullyIdle: 1,
        },
      ]);
      assert.isTrue(yield* hasAntigravityPendingBackgroundWork(profileDirectory, sessionId));
      assert.strictEqual(
        yield* isAntigravityContinuationStillActive(profileDirectory, sessionId),
        "idle",
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("rejects invalid session ids and symlinked databases", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const profileDirectory = yield* fs.makeTempDirectoryScoped();
      const outsideDirectory = yield* fs.makeTempDirectoryScoped();
      const sessionId = "symlink-session-1";
      const appDataDir = path.join(profileDirectory, "antigravity-acp");
      const conversationsDir = path.join(appDataDir, "conversations");
      yield* fs.makeDirectory(conversationsDir, { recursive: true });

      const foreignDb = path.join(outsideDirectory, "foreign.db");
      createStepsDb(foreignDb, [
        { idx: 1, status: STEP_STATUS_RUNNING, taskDetails: Buffer.from("task-1") },
        { idx: 2, status: STEP_STATUS_GENERATING, taskDetails: null },
      ]);
      yield* fs.symlink(foreignDb, path.join(conversationsDir, `${sessionId}.db`));

      const foreignSummariesDb = path.join(outsideDirectory, "foreign-summaries.db");
      createSummariesDb(foreignSummariesDb, [
        {
          conversationId: sessionId,
          status: "CASCADE_RUN_STATUS_RUNNING",
          notFullyIdle: 1,
        },
      ]);
      yield* fs.symlink(foreignSummariesDb, path.join(appDataDir, "conversation_summaries.db"));

      assert.isFalse(yield* hasAntigravityPendingBackgroundWork(profileDirectory, "../escape"));
      assert.isFalse(yield* hasAntigravityPendingBackgroundWork(profileDirectory, sessionId));
      assert.strictEqual(
        yield* isAntigravityContinuationStillActive(profileDirectory, sessionId),
        "unknown",
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
