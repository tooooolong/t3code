import * as NodeSqlite from "node:sqlite";

import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  ChatFileAttachment,
  MessageId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import { resolveSelfInvocation } from "@t3tools/shared/nodeRuntime";
import { htmlRenderFromToolItem } from "@t3tools/shared/toolOutput";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { ChildProcessSpawner } from "effect/process";
import type * as EffectAcpSchema from "effect-acp/compat";
import * as EffectAcpErrors from "effect-acp/errors";

import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import type * as AcpSessionRuntime from "@t3tools/provider-acp/server/AcpSessionRuntime";
import { makeAntigravityAcpRuntime } from "../../provider/acp/AntigravityAcpSupport.ts";
import { normalizeAntigravitySessionUpdate } from "../../provider/acp/AntigravityProtocol.ts";
import { ANTIGRAVITY_SIGN_IN_REQUIRED_MESSAGE } from "../../provider/antigravityAuthSupport.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import type * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import {
  makeAntigravityAcpAdapterFlavor,
  makeAntigravityAdapterV2,
} from "./AntigravityAdapterV2.ts";

const flavor = makeAntigravityAcpAdapterFlavor({
  instanceId: ProviderInstanceId.make("antigravity-test"),
  random: undefined as never,
  fileSystem: undefined as never,
  path: undefined as never,
  host: undefined as never,
  selfInvocation: undefined as never,
  makeRuntime: () => Effect.die("not spawned in this test"),
  withProcess: (_stop, task) => task,
  defaultModel: Effect.undefined,
});

function permissionRequest(
  toolCallId: string,
  options: ReadonlyArray<EffectAcpSchema.PermissionOption>,
): EffectAcpSchema.RequestPermissionRequest {
  return {
    sessionId: "session-1",
    options,
    toolCall: { toolCallId, title: "Which branch?" },
  };
}

describe("AntigravityAdapterV2 flavor", () => {
  it("keeps a successful subagent launch running until the parent turn becomes idle", () => {
    const batch = flavor.extractSubagentUpdate?.({
      toolCallId: "trajectory:4",
      title: "Running start_subagent",
      kind: "other",
      status: "completed",
      data: { rawOutput: "Started two agents." },
    });
    assert.equal(batch?.status, "running");
    assert.equal(batch?.prompt, "Started two agents.");
    assert.isNull(batch?.result);
    assert.isTrue(flavor.subagentsIdleOnTurnCompletion);

    // Permission prompts carry the launch; later updates for the same call do not.
    const launch = flavor.extractSubagentUpdate?.({
      toolCallId: "35bea692fa95447fbdae6333fec5515f",
      title: "Run invoke_subagent?",
      kind: "other",
      status: "pending",
      data: {
        rawInput: {
          Subagents: [{ TypeName: "research", Role: "Readme Reader", Prompt: "Read README.md" }],
        },
      },
    });
    assert.equal(launch?.title, "Readme Reader");
    assert.equal(launch?.prompt, "Read README.md");
  });

  it("routes interaction_* permission requests to the question card", () => {
    const question = flavor.extractPermissionQuestion?.(
      permissionRequest("interaction_1", [
        { optionId: "main", name: "main", kind: "allow_once" },
        { optionId: "dev", name: "dev", kind: "allow_once" },
      ]),
    );
    assert.isDefined(question);
    assert.deepEqual(
      question?.question.options.map((option) => option.label),
      ["main", "dev"],
    );
    assert.deepEqual(question?.respond({ interaction_1: "dev" }), {
      outcome: { outcome: "selected", optionId: "dev" },
    });
    assert.isUndefined(question?.respond({ interaction_1: "nope" }));
    assert.isUndefined(
      flavor.extractPermissionQuestion?.(
        permissionRequest("tool_1", [{ optionId: "allow", name: "Allow", kind: "allow_once" }]),
      ),
    );
  });

  it("keeps the question card to the agent's own options so a typed answer cannot be sent", () => {
    const question = flavor.extractPermissionQuestion?.(
      permissionRequest("interaction_2", [
        { optionId: "opt-main", name: "main", kind: "allow_once" },
        { optionId: "opt-dev", name: "dev", kind: "allow_once" },
      ]),
    );
    assert.strictEqual(question?.question.allowCustomAnswer, false);
    assert.deepEqual(
      question?.question.options.map((option) => option.value),
      ["opt-main", "opt-dev"],
    );
    assert.deepEqual(question?.respond({ interaction_2: "opt-dev" }), {
      outcome: { outcome: "selected", optionId: "opt-dev" },
    });
  });
});

const layerSession = Layer.mergeAll(
  NodeServices.layer,
  NodeCrypto.layer,
  IdAllocator.layer,
  McpProviderSessions.layer,
  TestProviderHost.layer().pipe(
    Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
  ),
);

describe("AntigravityAdapterV2 compaction", () => {
  it.live.each([false, true])(
    "negotiates native goals (%s) without enabling manual compaction",
    (nativeGoals) =>
      Effect.gen(function* () {
        const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const path = yield* Path.Path;
        const crypto = yield* SecureRandom.SecureRandom;
        const mockAgentPath = yield* path.fromFileUrl(
          new URL("../../../scripts/acp-mock-agent.ts", import.meta.url),
        );
        const instanceId = ProviderInstanceId.make("antigravity-compaction-test");
        const goalSupport = yield* Ref.make(false);
        const startupMilestone = yield* Deferred.make<void>();
        const consumeStartupCommands = yield* Deferred.make<void>();
        const adapter = yield* makeAntigravityAdapterV2({
          instanceId,
          selfInvocation: yield* resolveSelfInvocation(),
          makeRuntime: (input) =>
            makeAntigravityAcpRuntime({
              ...input,
              childProcessSpawner,
              spawn: {
                command: process.execPath,
                args: [mockAgentPath],
                cwd: input.cwd,
                env: { T3_ACP_ANTIGRAVITY: "1" },
              },
            }).pipe(
              Effect.provideService(SecureRandom.SecureRandom, crypto),
              Effect.map((runtime) => ({
                ...runtime,
                drainEvents: Deferred.succeed(startupMilestone, undefined).pipe(
                  Effect.andThen(runtime.drainEvents),
                ),
              })),
            ),
          withProcess: (_stop, task) => task,
          defaultModel: Effect.undefined,
          supportsNativeGoals: Ref.get(goalSupport).pipe(
            Effect.tap(() => Deferred.succeed(startupMilestone, undefined)),
          ),
          onSessionEvent: (event) =>
            event._tag === "AvailableCommandsUpdated"
              ? Deferred.await(consumeStartupCommands).pipe(
                  Effect.andThen(Ref.set(goalSupport, nativeGoals)),
                )
              : Effect.void,
        });
        const opening = yield* adapter
          .openSession({
            threadId: ThreadId.make("thread-antigravity-compaction"),
            providerSessionId: ProviderSessionId.make("provider-session-antigravity-compaction"),
            modelSelection: { instanceId, model: "gemini-test-low" },
            runtimePolicy: ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
              runtimeMode: "full-access",
              interactionMode: "default",
              cwd: process.cwd(),
            }),
          })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(startupMilestone);
        yield* Deferred.succeed(consumeStartupCommands, undefined);
        const session = yield* Fiber.join(opening);
        assert.isUndefined(session.compactThread);
        assert.equal(session.providerSession.capabilities.turns.supportsNativeGoals, nativeGoals);
        assert.equal((yield* adapter.getCapabilities()).turns.supportsNativeGoals, nativeGoals);
      }).pipe(Effect.provide(layerSession)),
  );
});

describe("AntigravityAdapterV2 session open", () => {
  it.effect.each([
    {
      name: "browser sign-in",
      cause: new EffectAcpErrors.AcpTransportError({
        detail: ANTIGRAVITY_SIGN_IN_REQUIRED_MESSAGE,
        cause: undefined,
      }),
      requiresSignIn: true,
    },
    {
      name: "ACP authentication required",
      cause: EffectAcpErrors.AcpRequestError.authRequired(),
      requiresSignIn: true,
    },
    {
      name: "transport failure",
      cause: new EffectAcpErrors.AcpTransportError({
        detail: "Connection closed",
        cause: undefined,
      }),
      requiresSignIn: undefined,
    },
  ] as const)("classifies $name during session open", ({ cause, requiresSignIn }) =>
    Effect.gen(function* () {
      const instanceId = ProviderInstanceId.make("antigravity-sign-in-test");
      const adapter = yield* makeAntigravityAdapterV2({
        instanceId,
        selfInvocation: yield* resolveSelfInvocation(),
        makeRuntime: () => Effect.fail(cause),
        withProcess: (_stop, task) => task,
        defaultModel: Effect.undefined,
      });
      const threadId = ThreadId.make("thread-antigravity-sign-in");
      const error = yield* adapter
        .openSession({
          threadId,
          providerSessionId: ProviderSessionId.make("provider-session-antigravity-sign-in"),
          modelSelection: { instanceId, model: "gemini-test-low" },
          runtimePolicy: ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
            runtimeMode: "full-access",
            interactionMode: "default",
            cwd: process.cwd(),
          }),
        })
        .pipe(Effect.flip);

      assert.equal(error._tag, "ProviderAdapterOpenSessionError");
      assert.equal(
        error._tag === "ProviderAdapterOpenSessionError" && error.requiresSignIn,
        requiresSignIn,
      );
    }).pipe(Effect.provide(layerSession)),
  );
});

describe("AntigravityAdapterV2 client file system", () => {
  it.effect("serves confined reads and leaves edits to the agent's diff tools", () =>
    Effect.gen(function* () {
      const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const host = yield* ProviderHost.ProviderHost;
      const mockAgentPath = yield* path.fromFileUrl(
        new URL("../../../scripts/acp-mock-agent.ts", import.meta.url),
      );
      type RuntimeService = AcpSessionRuntime.AcpSessionRuntime["Service"];
      // effect-acp keeps the last handler registered per method; so does this.
      let readTextFile: Parameters<RuntimeService["handleReadTextFile"]>[0] | undefined;
      let writeTextFile: Parameters<RuntimeService["handleWriteTextFile"]>[0] | undefined;
      const logs = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-antigravity-log-" });
      const requestLog = path.join(logs, "requests.ndjson");
      const crypto = yield* SecureRandom.SecureRandom;
      const instanceId = ProviderInstanceId.make("antigravity-containment-test");
      const adapter = yield* makeAntigravityAdapterV2({
        instanceId,
        selfInvocation: yield* resolveSelfInvocation(),
        makeRuntime: (input) =>
          makeAntigravityAcpRuntime({
            ...input,
            childProcessSpawner,
            spawn: {
              command: process.execPath,
              args: [mockAgentPath],
              cwd: input.cwd,
              env: { T3_ACP_ANTIGRAVITY: "1", T3_ACP_REQUEST_LOG_PATH: requestLog },
            },
          }).pipe(
            Effect.provideService(SecureRandom.SecureRandom, crypto),
            Effect.map((runtime): RuntimeService => ({
              ...runtime,
              handleReadTextFile: (handler) =>
                Effect.sync(() => {
                  readTextFile = handler;
                }).pipe(Effect.andThen(runtime.handleReadTextFile(handler))),
              handleWriteTextFile: (handler) =>
                Effect.sync(() => {
                  writeTextFile = handler;
                }).pipe(Effect.andThen(runtime.handleWriteTextFile(handler))),
            })),
          ),
        withProcess: (_stop, task) => task,
        defaultModel: Effect.undefined,
      });
      const workspace = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-antigravity-workspace-",
      });
      const outside = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-antigravity-outside-",
      });
      const outsideFile = path.join(outside, "secret.txt");
      yield* fileSystem.writeFileString(outsideFile, "secret");
      const attachment = path.join(host.paths.attachmentsDir, "pasted.txt");
      yield* fileSystem.writeFileString(attachment, "pasted");

      const threadId = ThreadId.make("thread-antigravity-containment");
      const runtimePolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
        runtimeMode: "full-access",
        interactionMode: "default",
        cwd: workspace,
      });
      const modelSelection = { instanceId, model: "gemini-test-low" } as const;
      const session = yield* adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make("provider-session-antigravity-containment"),
        modelSelection,
        runtimePolicy,
      });
      yield* session.ensureThread({ threadId, modelSelection, runtimePolicy });
      const initialize = (yield* fileSystem.readFileString(requestLog))
        .trim()
        .split("\n")
        .map(
          (line) =>
            JSON.parse(line) as {
              method?: string;
              params?: { clientCapabilities?: { fs?: unknown }; clientInfo?: { name?: string } };
            },
        )
        .find((request) => request.method === "initialize");
      assert.deepEqual(initialize?.params?.clientCapabilities?.fs, {
        readTextFile: true,
        writeTextFile: false,
      });
      assert.equal(initialize?.params?.clientInfo?.name, "zed");
      assert.isUndefined(writeTextFile);
      if (readTextFile === undefined) {
        return yield* Effect.die("Antigravity sessions must serve client file reads");
      }
      const context = (method: string) => ({ requestId: `test-${method}`, method });

      const insidePath = path.join(workspace, "src", "inside.ts");
      yield* fileSystem.makeDirectory(path.dirname(insidePath), { recursive: true });
      yield* fileSystem.writeFileString(insidePath, "inside");
      const pasted = yield* readTextFile(
        { sessionId: "mock-session-1", path: attachment },
        context("fs/read_text_file"),
      );
      assert.equal(pasted.content, "pasted");
      yield* fileSystem.writeFileString(insidePath, "\uFEFFfirst\né中🙂\nlast");
      const sliced = yield* readTextFile(
        { sessionId: "mock-session-1", path: insidePath, line: 2, limit: 1 },
        context("fs/read_text_file"),
      );
      assert.equal(sliced.content, "é中🙂");
      yield* fileSystem.writeFileString(insidePath, "inside");
      for (const [name, bytes] of [
        ["image.png", new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a])],
        ["data.bin", new Uint8Array([0x61, 0, 0x62])],
      ] as const) {
        const binaryPath = path.join(workspace, name);
        yield* fileSystem.writeFile(binaryPath, bytes);
        const binaryRead = yield* readTextFile(
          { sessionId: "mock-session-1", path: binaryPath },
          context("fs/read_text_file"),
        ).pipe(Effect.flip);
        assert.propertyVal(binaryRead, "code", -32602, `${name} is refused as binary`);
      }

      const outsideRead = yield* readTextFile(
        { sessionId: "mock-session-1", path: outsideFile },
        context("fs/read_text_file"),
      ).pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(outsideRead));
      const linkPath = path.join(workspace, "linked-secret.txt");
      yield* fileSystem.symlink(outsideFile, linkPath);
      const linkedRead = yield* readTextFile(
        { sessionId: "mock-session-1", path: linkPath },
        context("fs/read_text_file"),
      ).pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(linkedRead), "a read through an escaping symlink is denied");
      const insideLink = path.join(workspace, "inside-link.ts");
      yield* fileSystem.symlink(insidePath, insideLink);
      const viaInsideLink = yield* readTextFile(
        { sessionId: "mock-session-1", path: insideLink },
        context("fs/read_text_file"),
      );
      assert.equal(viaInsideLink.content, "inside");
    }).pipe(Effect.provide(layerSession)),
  );
});

describe("AntigravityAdapterV2 workspace changes", () => {
  it.effect("confines file requests to the workspace of the turn in progress", () =>
    Effect.gen(function* () {
      const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const crypto = yield* SecureRandom.SecureRandom;
      const mockAgentPath = yield* path.fromFileUrl(
        new URL("../../../scripts/acp-mock-agent.ts", import.meta.url),
      );
      type RuntimeService = AcpSessionRuntime.AcpSessionRuntime["Service"];
      const promptReceived = yield* Deferred.make<Parameters<RuntimeService["prompt"]>[0]>();
      const host = yield* ProviderHost.ProviderHost;
      const document = ChatFileAttachment.make({
        type: "file",
        id: "native-pdf",
        name: "diagram.pdf",
        mimeType: "application/pdf",
        sizeBytes: 9,
      });
      const documentPath = host.resolveAttachmentPath(document)!;
      yield* fileSystem.writeFileString(documentPath, "%PDF-1.7");
      let readTextFile: Parameters<RuntimeService["handleReadTextFile"]>[0] | undefined;
      const instanceId = ProviderInstanceId.make("antigravity-workspace-change-test");
      const adapter = yield* makeAntigravityAdapterV2({
        instanceId,
        selfInvocation: yield* resolveSelfInvocation(),
        makeRuntime: (input) =>
          makeAntigravityAcpRuntime({
            ...input,
            childProcessSpawner,
            spawn: {
              command: process.execPath,
              args: [mockAgentPath],
              cwd: input.cwd,
              env: { T3_ACP_ANTIGRAVITY: "1", T3_ACP_HANG_PROMPT_FOREVER: "1" },
            },
          }).pipe(
            Effect.provideService(SecureRandom.SecureRandom, crypto),
            Effect.map((runtime): RuntimeService => ({
              ...runtime,
              prompt: (request) =>
                Deferred.succeed(promptReceived, request).pipe(
                  Effect.andThen(runtime.prompt(request)),
                ),
              start: () =>
                runtime.start().pipe(
                  Effect.map((started) => ({
                    ...started,
                    initializeResult: {
                      ...started.initializeResult,
                      agentCapabilities: {
                        ...started.initializeResult.agentCapabilities,
                        promptCapabilities: { image: true, audio: true, embeddedContext: true },
                      },
                    },
                  })),
                ),
              handleReadTextFile: (handler) =>
                Effect.sync(() => {
                  readTextFile = handler;
                }).pipe(Effect.andThen(runtime.handleReadTextFile(handler))),
            })),
          ),
        withProcess: (_stop, task) => task,
        defaultModel: Effect.undefined,
      });
      const workspaceA = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-ag-a-" });
      const workspaceB = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-ag-b-" });
      yield* fileSystem.writeFileString(path.join(workspaceA, "a.txt"), "from a");
      yield* fileSystem.writeFileString(path.join(workspaceB, "b.txt"), "from b");
      const policyFor = (cwd: string) =>
        ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
          runtimeMode: "full-access",
          interactionMode: "default",
          cwd,
        });
      const threadId = ThreadId.make("thread-antigravity-workspace-change");
      const modelSelection = { instanceId, model: "gemini-test-low" } as const;
      const session = yield* adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make("provider-session-antigravity-workspace-change"),
        modelSelection,
        runtimePolicy: policyFor(workspaceA),
      });
      const providerThread = yield* session.ensureThread({
        threadId,
        modelSelection,
        runtimePolicy: policyFor(workspaceA),
      });
      const now = yield* DateTime.now;
      yield* session
        .startTurn({
          appThread: {
            createdBy: "user",
            creationSource: "web",
            id: threadId,
            projectId: ProjectId.make("project-antigravity-workspace-change"),
            title: "Antigravity workspace change",
            providerInstanceId: instanceId,
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: workspaceB,
            activeProviderThreadId: providerThread.id,
            lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
            forkedFrom: null,
            createdAt: now,
            updatedAt: now,
            archivedAt: null,
            settledOverride: null,
            settledAt: null,
            lastVisitedAt: null,
            deletedAt: null,
          },
          threadId,
          runId: RunId.make("run-antigravity-workspace-change"),
          runOrdinal: 1,
          providerTurnOrdinal: 1,
          attemptId: RunAttemptId.make("attempt-antigravity-workspace-change"),
          rootNodeId: NodeId.make("node-antigravity-workspace-change"),
          providerThread,
          message: {
            createdBy: "user",
            creationSource: "web",
            messageId: MessageId.make("message-antigravity-workspace-change"),
            text: "read b.txt",
            attachments: [document],
          },
          modelSelection,
          runtimePolicy: policyFor(workspaceB),
        })
        .pipe(Effect.forkScoped);
      yield* session.events.pipe(
        Stream.filter((event) => event.type === "provider_turn.updated"),
        Stream.runHead,
      );
      const sent = yield* Deferred.await(promptReceived);
      assert.deepEqual(
        sent.prompt.filter((block) => block.type === "resource_link"),
        [
          {
            type: "resource_link",
            uri: (yield* path.toFileUrl(documentPath)).href,
            name: "diagram.pdf",
            mimeType: "application/pdf",
            size: 8,
          },
        ],
      );
      assert.include(JSON.stringify(sent.prompt), JSON.stringify(documentPath).slice(1, -1));
      if (readTextFile === undefined) {
        return yield* Effect.die("Antigravity sessions must serve client file requests");
      }
      const context = { requestId: "test-read", method: "fs/read_text_file" };
      const fromB = yield* readTextFile(
        { sessionId: "mock-session-1", path: path.join(workspaceB, "b.txt") },
        context,
      );
      assert.equal(fromB.content, "from b");
      const fromA = yield* readTextFile(
        { sessionId: "mock-session-1", path: path.join(workspaceA, "a.txt") },
        context,
      ).pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(fromA), "the previous workspace is no longer readable");
    }).pipe(Effect.provide(layerSession)),
  );
});

describe("AntigravityAdapterV2 html_render pages", () => {
  it.effect("shows the page html_render published when the agent reports only a summary", () =>
    Effect.gen(function* () {
      const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const path = yield* Path.Path;
      const crypto = yield* SecureRandom.SecureRandom;
      const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
      const mockAgentPath = yield* path.fromFileUrl(
        new URL("../../../scripts/acp-mock-agent.ts", import.meta.url),
      );
      type RuntimeService = AcpSessionRuntime.AcpSessionRuntime["Service"];
      let sessionUpdate: Parameters<RuntimeService["handleSessionUpdate"]>[0] | undefined;
      const instanceId = ProviderInstanceId.make("antigravity-html-render-test");
      const threadId = ThreadId.make("thread-antigravity-html-render");
      const page = (name: string) => ({
        attachmentId: `${threadId}-${name}-html`,
        title: "Revenue",
        height: 240,
      });
      // Long enough that Antigravity bounds the html the adapter sees.
      const rows = Array.from({ length: 500 }, (_, row) => `<tr><td>Row ${row}</td></tr>`);
      const report = { html: `<table>${rows.join("")}</table>`, title: "Revenue", height: 240 };
      const failed = { ...report, html: "<p>Revenue</p>" };
      const errored = { ...report, html: "<p>Revenue error</p>" };
      const adapter = yield* makeAntigravityAdapterV2({
        instanceId,
        selfInvocation: yield* resolveSelfInvocation(),
        makeRuntime: (input) =>
          makeAntigravityAcpRuntime({
            ...input,
            childProcessSpawner,
            spawn: {
              command: process.execPath,
              args: [mockAgentPath],
              cwd: input.cwd,
              env: { T3_ACP_ANTIGRAVITY: "1" },
            },
          }).pipe(
            Effect.provideService(SecureRandom.SecureRandom, crypto),
            Effect.map((runtime): RuntimeService => ({
              ...runtime,
              handleSessionUpdate: (handler) =>
                Effect.sync(() => {
                  sessionUpdate = handler;
                }).pipe(Effect.andThen(runtime.handleSessionUpdate(handler))),
              // agy_acp_server 1.3/1.4 without a permission prompt: the call is
              // titled "Running <tool>" and completes with the step's summary
              // in place of the MCP result.
              prompt: () =>
                Effect.gen(function* () {
                  const send = (update: EffectAcpSchema.SessionUpdate) =>
                    sessionUpdate === undefined
                      ? Effect.die("the adapter must handle session updates")
                      : sessionUpdate(
                          normalizeAntigravitySessionUpdate({
                            sessionId: "mock-session-1",
                            update,
                          }),
                        );
                  const calls = [
                    ["published", report],
                    ["unpublished", { ...report, html: "<p>Costs</p>" }],
                    ["failed", failed],
                    ["error", errored],
                  ] as const;
                  for (const [toolCallId, rawInput] of calls) {
                    yield* send({
                      sessionUpdate: "tool_call",
                      toolCallId,
                      title: "Running html_render",
                      kind: "other",
                      status: "in_progress",
                      rawInput,
                    });
                  }
                  yield* mcpSessions.recordHtmlRender(threadId, report, page("report"));
                  yield* mcpSessions.recordHtmlRender(threadId, failed, page("failed"));
                  yield* mcpSessions.recordHtmlRender(threadId, errored, page("error"));
                  for (const [toolCallId, status] of [
                    ["unpublished", "completed"],
                    ["failed", "failed"],
                    ["published", "completed"],
                    ["published", "completed"],
                  ] as const) {
                    yield* send({
                      sessionUpdate: "tool_call_update",
                      toolCallId,
                      status,
                      rawOutput: `Summary of ${toolCallId}`,
                    });
                  }
                  yield* send({
                    sessionUpdate: "tool_call_update",
                    toolCallId: "error",
                    status: "completed",
                    rawOutput: { result: { isError: true, content: [] } },
                  });
                  for (const [name, rawInput] of [
                    ["failed", failed],
                    ["error", errored],
                  ] as const) {
                    const toolCallId = `${name}-retry`;
                    yield* send({
                      sessionUpdate: "tool_call",
                      toolCallId,
                      title: "Running html_render",
                      kind: "other",
                      status: "in_progress",
                      rawInput,
                    });
                    yield* mcpSessions.recordHtmlRender(threadId, rawInput, page(toolCallId));
                    yield* send({
                      sessionUpdate: "tool_call_update",
                      toolCallId: name,
                      status: name === "failed" ? "failed" : "completed",
                      rawOutput:
                        name === "failed"
                          ? "Summary of failed"
                          : { result: { isError: true, content: [] } },
                    });
                    yield* send({
                      sessionUpdate: "tool_call_update",
                      toolCallId,
                      status: "completed",
                      rawOutput: `Summary of ${toolCallId}`,
                    });
                  }
                  yield* mcpSessions.recordHtmlRender(threadId, report, page("replacement"));
                  yield* send({
                    sessionUpdate: "tool_call_update",
                    toolCallId: "published",
                    status: "completed",
                    rawOutput: { htmlRender: page("replacement") },
                  });
                  yield* send({
                    sessionUpdate: "tool_call_update",
                    toolCallId: "published",
                    status: "completed",
                    rawOutput: "Summary of published",
                  });
                  return { stopReason: "end_turn" as const };
                }),
            })),
          ),
        withProcess: (_stop, task) => task,
        defaultModel: Effect.undefined,
      });
      const runtimePolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
        runtimeMode: "full-access",
        interactionMode: "default",
        cwd: process.cwd(),
      });
      const modelSelection = { instanceId, model: "gemini-test-low" } as const;
      const session = yield* adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make("provider-session-antigravity-html-render"),
        modelSelection,
        runtimePolicy,
      });
      const providerThread = yield* session.ensureThread({
        threadId,
        modelSelection,
        runtimePolicy,
      });
      const now = yield* DateTime.now;
      yield* session.startTurn({
        appThread: {
          createdBy: "user",
          creationSource: "web",
          id: threadId,
          projectId: ProjectId.make("project-antigravity-html-render"),
          title: "Antigravity html_render",
          providerInstanceId: instanceId,
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: process.cwd(),
          activeProviderThreadId: providerThread.id,
          lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
          forkedFrom: null,
          createdAt: now,
          updatedAt: now,
          archivedAt: null,
          settledOverride: null,
          settledAt: null,
          lastVisitedAt: null,
          deletedAt: null,
        },
        threadId,
        runId: RunId.make("run-antigravity-html-render"),
        runOrdinal: 1,
        providerTurnOrdinal: 1,
        attemptId: RunAttemptId.make("attempt-antigravity-html-render"),
        rootNodeId: NodeId.make("node-antigravity-html-render"),
        providerThread,
        message: {
          createdBy: "user",
          creationSource: "web",
          messageId: MessageId.make("message-antigravity-html-render"),
          text: "chart the revenue",
          attachments: [],
        },
        modelSelection,
        runtimePolicy,
      });
      const events = Array.from(
        yield* session.events.pipe(
          Stream.takeUntil((event) => event.type === "turn.terminal"),
          Stream.runCollect,
        ),
      );
      const tool = (nativeId: string) => {
        const event = events.findLast(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.nativeItemRef?.nativeId === nativeId,
        );
        if (event?.type !== "turn_item.updated" || event.turnItem.type !== "dynamic_tool") {
          return undefined;
        }
        const item = event.turnItem;
        return {
          status: item.status,
          toolName: item.toolName,
          page: htmlRenderFromToolItem(item),
          output: item.output,
        };
      };

      assert.deepEqual(tool("published"), {
        status: "completed",
        toolName: "t3-code.html_render",
        page: page("replacement"),
        output: { htmlRender: page("replacement") },
      });
      assert.deepEqual(tool("unpublished"), {
        status: "completed",
        toolName: "t3-code.html_render",
        page: undefined,
        output: "Summary of unpublished",
      });
      assert.deepEqual(tool("failed"), {
        status: "failed",
        toolName: "t3-code.html_render",
        page: undefined,
        output: "Summary of failed",
      });
      assert.deepEqual(tool("error"), {
        status: "completed",
        toolName: "t3-code.html_render",
        page: undefined,
        output: [],
      });
      assert.deepEqual(
        events.flatMap((event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "dynamic_tool" &&
          event.turnItem.nativeItemRef?.nativeId === "published" &&
          event.turnItem.status === "completed"
            ? [htmlRenderFromToolItem(event.turnItem)]
            : [],
        ),
        [page("report"), page("report"), page("replacement"), page("replacement")],
      );
      for (const name of ["failed-retry", "error-retry"]) {
        assert.deepEqual(tool(name), {
          status: "completed",
          toolName: "t3-code.html_render",
          page: page(name),
          output: { htmlRender: page(name) },
        });
      }
      assert.strictEqual(yield* mcpSessions.takeHtmlRender(threadId, report), undefined);
      assert.strictEqual(yield* mcpSessions.takeHtmlRender(threadId, failed), undefined);
      assert.strictEqual(yield* mcpSessions.takeHtmlRender(threadId, errored), undefined);
    }).pipe(Effect.provide(layerSession)),
  );
});

describe("AntigravityAdapterV2 subagent reads", () => {
  it.effect("completes a read T3 served when the agent later fails it with a step error", () =>
    Effect.gen(function* () {
      const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const crypto = yield* SecureRandom.SecureRandom;
      const mockAgentPath = yield* path.fromFileUrl(
        new URL("../../../scripts/acp-mock-agent.ts", import.meta.url),
      );
      const workspace = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-antigravity-reads-",
      });
      const outside = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-antigravity-reads-outside-",
      });
      const served = path.join(workspace, "served.ts");
      yield* fileSystem.writeFileString(served, "export {};");
      const denied = path.join(outside, "secret.txt");
      yield* fileSystem.writeFileString(denied, "secret");
      const unseen = path.join(workspace, "unseen.ts");
      const retried = path.join(workspace, "retried.ts");
      const stepError = 'Model produced invalid output. ("model output error")';
      type RuntimeService = AcpSessionRuntime.AcpSessionRuntime["Service"];
      let sessionUpdate: Parameters<RuntimeService["handleSessionUpdate"]>[0] | undefined;
      let readTextFile: Parameters<RuntimeService["handleReadTextFile"]>[0] | undefined;
      const instanceId = ProviderInstanceId.make("antigravity-subagent-reads-test");
      const threadId = ThreadId.make("thread-antigravity-subagent-reads");
      const adapter = yield* makeAntigravityAdapterV2({
        instanceId,
        selfInvocation: yield* resolveSelfInvocation(),
        makeRuntime: (input) =>
          makeAntigravityAcpRuntime({
            ...input,
            childProcessSpawner,
            spawn: {
              command: process.execPath,
              args: [mockAgentPath],
              cwd: input.cwd,
              env: { T3_ACP_ANTIGRAVITY: "1" },
            },
          }).pipe(
            Effect.provideService(SecureRandom.SecureRandom, crypto),
            Effect.map((runtime): RuntimeService => ({
              ...runtime,
              handleSessionUpdate: (handler) =>
                Effect.sync(() => {
                  sessionUpdate = handler;
                }).pipe(Effect.andThen(runtime.handleSessionUpdate(handler))),
              handleReadTextFile: (handler) =>
                Effect.sync(() => {
                  readTextFile = handler;
                }).pipe(Effect.andThen(runtime.handleReadTextFile(handler))),
              // agy_acp_server announces a subagent's client_view_file on the parent session,
              // never completes it, then fails every open call with the next step error.
              prompt: () =>
                Effect.gen(function* () {
                  if (sessionUpdate === undefined || readTextFile === undefined) {
                    return yield* Effect.die("the adapter must handle updates and reads");
                  }
                  const send = sessionUpdate;
                  const handleRead = readTextFile;
                  const update = (update: EffectAcpSchema.SessionUpdate) =>
                    send(
                      normalizeAntigravitySessionUpdate({ sessionId: "mock-session-1", update }),
                    );
                  // The agent's own view_file never reaches T3, so it must not take a served read.
                  const reads = [
                    ["call_native", served, "Running view_file"],
                    ["call_served", served, "Running client_view_file"],
                    ["call_missing", retried, "Running client_view_file"],
                    ["call_retried", retried, "Running client_view_file"],
                    ["call_denied", denied, "Running client_view_file"],
                    ["call_unseen", unseen, "Running client_view_file"],
                  ] as const;
                  for (const [toolCallId, target, title] of reads) {
                    yield* update({
                      sessionUpdate: "tool_call",
                      toolCallId,
                      title,
                      kind: "read",
                      status: "in_progress",
                      rawInput: { absolute_path: target },
                      locations: [{ path: target }],
                    });
                  }
                  const serve = (target: string) =>
                    handleRead(
                      { sessionId: "subagent-conversation", path: target },
                      { requestId: `read-${target}`, method: "fs/read_text_file" },
                    ).pipe(Effect.exit);
                  yield* serve(served);
                  yield* serve(retried);
                  yield* fileSystem.writeFileString(retried, "export {};").pipe(Effect.orDie);
                  yield* serve(retried);
                  yield* serve(denied);
                  for (const [toolCallId] of reads) {
                    yield* update({
                      sessionUpdate: "tool_call_update",
                      toolCallId,
                      status: "failed",
                      rawOutput: stepError,
                    });
                  }
                  return { stopReason: "end_turn" as const };
                }),
            })),
          ),
        withProcess: (_stop, task) => task,
        defaultModel: Effect.undefined,
      });
      const runtimePolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
        runtimeMode: "full-access",
        interactionMode: "default",
        cwd: workspace,
      });
      const modelSelection = { instanceId, model: "gemini-test-low" } as const;
      const session = yield* adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make("provider-session-antigravity-subagent-reads"),
        modelSelection,
        runtimePolicy,
      });
      const providerThread = yield* session.ensureThread({
        threadId,
        modelSelection,
        runtimePolicy,
      });
      const now = yield* DateTime.now;
      yield* session.startTurn({
        appThread: {
          createdBy: "user",
          creationSource: "web",
          id: threadId,
          projectId: ProjectId.make("project-antigravity-subagent-reads"),
          title: "Antigravity subagent reads",
          providerInstanceId: instanceId,
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: workspace,
          activeProviderThreadId: providerThread.id,
          lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
          forkedFrom: null,
          createdAt: now,
          updatedAt: now,
          archivedAt: null,
          settledOverride: null,
          settledAt: null,
          lastVisitedAt: null,
          deletedAt: null,
        },
        threadId,
        runId: RunId.make("run-antigravity-subagent-reads"),
        runOrdinal: 1,
        providerTurnOrdinal: 1,
        attemptId: RunAttemptId.make("attempt-antigravity-subagent-reads"),
        rootNodeId: NodeId.make("node-antigravity-subagent-reads"),
        providerThread,
        message: {
          createdBy: "user",
          creationSource: "web",
          messageId: MessageId.make("message-antigravity-subagent-reads"),
          text: "review the codebase",
          attachments: [],
        },
        modelSelection,
        runtimePolicy,
      });
      const events = Array.from(
        yield* session.events.pipe(
          Stream.takeUntil((event) => event.type === "turn.terminal"),
          Stream.runCollect,
        ),
      );
      const read = (nativeId: string) => {
        const event = events.findLast(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.nativeItemRef?.nativeId === nativeId,
        );
        return event?.type === "turn_item.updated" && event.turnItem.type === "dynamic_tool"
          ? { status: event.turnItem.status, output: event.turnItem.output }
          : undefined;
      };

      const firstEvent = (nativeId: string, status: string) =>
        events.findIndex(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.nativeItemRef?.nativeId === nativeId &&
            event.turnItem.status === status,
        );
      // T3 completes a served read itself instead of leaving it running until the flush.
      assert.isAbove(firstEvent("call_served", "completed"), -1);
      assert.isBelow(firstEvent("call_served", "completed"), firstEvent("call_native", "failed"));
      const failed = { status: "failed" as const, output: stepError };
      assert.deepEqual(read("call_served"), { status: "completed", output: undefined });
      assert.deepEqual(read("call_retried"), { status: "completed", output: undefined });
      assert.deepEqual(read("call_native"), failed);
      assert.deepEqual(read("call_missing"), failed);
      assert.deepEqual(read("call_denied"), failed);
      assert.deepEqual(read("call_unseen"), failed);
    }).pipe(Effect.provide(layerSession)),
  );
});

describe("AntigravityAdapterV2 runtime mode ceiling", () => {
  const modesSetFor = (pickedMode: string) =>
    Effect.gen(function* () {
      const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const path = yield* Path.Path;
      const crypto = yield* SecureRandom.SecureRandom;
      const mockAgentPath = yield* path.fromFileUrl(
        new URL("../../../scripts/acp-mock-agent.ts", import.meta.url),
      );
      type RuntimeService = AcpSessionRuntime.AcpSessionRuntime["Service"];
      const modesSet: Array<string> = [];
      const instanceId = ProviderInstanceId.make("antigravity-ceiling-test");
      const adapter = yield* makeAntigravityAdapterV2({
        instanceId,
        selfInvocation: yield* resolveSelfInvocation(),
        makeRuntime: (input) =>
          makeAntigravityAcpRuntime({
            ...input,
            childProcessSpawner,
            spawn: {
              command: process.execPath,
              args: [mockAgentPath],
              cwd: input.cwd,
              env: { T3_ACP_ANTIGRAVITY: "1" },
            },
          }).pipe(
            Effect.provideService(SecureRandom.SecureRandom, crypto),
            Effect.map((runtime): RuntimeService => ({
              ...runtime,
              setMode: (modeId) =>
                Effect.sync(() => {
                  modesSet.push(modeId);
                }).pipe(Effect.andThen(runtime.setMode(modeId))),
            })),
          ),
        withProcess: (_stop, task) => task,
        defaultModel: Effect.undefined,
      });
      const threadId = ThreadId.make("thread-antigravity-ceiling");
      yield* adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make("provider-session-antigravity-ceiling"),
        modelSelection: {
          instanceId,
          model: "gemini-test-low",
          options: [{ id: "_t3/session-mode", value: pickedMode }],
        },
        runtimePolicy: ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
          runtimeMode: "approval-required",
          interactionMode: "default",
          cwd: process.cwd(),
          maxRuntimeMode: "auto-accept-edits",
        }),
      });
      return modesSet;
    }).pipe(Effect.provide(layerSession), Effect.scoped);

  it.effect("does not apply a picked session mode above the policy ceiling", () =>
    Effect.gen(function* () {
      assert.deepEqual(yield* modesSetFor("yolo"), ["default"]);
      // A mode under the ceiling is still the user's to pick.
      assert.deepEqual(yield* modesSetFor("auto_edit"), ["default", "auto_edit"]);
    }),
  );
});

describe("AntigravityAdapterV2 session system prompt", () => {
  it.effect.each(["1.4.5", "1.4.1"])(
    "carries PR linking once in the %s session setup or once per turn",
    (agentVersion) =>
      Effect.gen(function* () {
        const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const crypto = yield* SecureRandom.SecureRandom;
        const mockAgentPath = yield* path.fromFileUrl(
          new URL("../../../scripts/acp-mock-agent.ts", import.meta.url),
        );
        const logs = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-antigravity-log-" });
        const requestLog = path.join(logs, "requests.ndjson");
        type RuntimeService = AcpSessionRuntime.AcpSessionRuntime["Service"];
        const promptReceived = yield* Deferred.make<Parameters<RuntimeService["prompt"]>[0]>();
        const instanceId = ProviderInstanceId.make("antigravity-system-prompt-test");
        const adapter = yield* makeAntigravityAdapterV2({
          instanceId,
          selfInvocation: yield* resolveSelfInvocation(),
          makeRuntime: (input) =>
            makeAntigravityAcpRuntime({
              ...input,
              childProcessSpawner,
              spawn: {
                command: process.execPath,
                args: [mockAgentPath],
                cwd: input.cwd,
                env: {
                  T3_ACP_ANTIGRAVITY: "1",
                  T3_ACP_AGENT_VERSION: agentVersion,
                  T3_ACP_REQUEST_LOG_PATH: requestLog,
                  T3_ACP_HANG_PROMPT_FOREVER: "1",
                },
              },
            }).pipe(
              Effect.provideService(SecureRandom.SecureRandom, crypto),
              Effect.map((runtime): RuntimeService => ({
                ...runtime,
                prompt: (request) =>
                  Deferred.succeed(promptReceived, request).pipe(
                    Effect.andThen(runtime.prompt(request)),
                  ),
              })),
            ),
          withProcess: (_stop, task) => task,
          defaultModel: Effect.undefined,
        });
        const workspace = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-ag-sp-" });
        const runtimePolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
          runtimeMode: "full-access",
          interactionMode: "default",
          cwd: workspace,
        });
        const threadId = ThreadId.make("thread-antigravity-system-prompt");
        const modelSelection = { instanceId, model: "gemini-test-low" } as const;
        const session = yield* adapter.openSession({
          threadId,
          providerSessionId: ProviderSessionId.make("provider-session-antigravity-system-prompt"),
          modelSelection,
          runtimePolicy,
        });
        const providerThread = yield* session.ensureThread({
          threadId,
          modelSelection,
          runtimePolicy,
        });
        const now = yield* DateTime.now;
        yield* session
          .startTurn({
            appThread: {
              createdBy: "user",
              creationSource: "web",
              id: threadId,
              projectId: ProjectId.make("project-antigravity-system-prompt"),
              title: "Antigravity system prompt",
              providerInstanceId: instanceId,
              modelSelection,
              runtimeMode: "full-access",
              interactionMode: "default",
              branch: null,
              worktreePath: null,
              activeProviderThreadId: providerThread.id,
              lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
              forkedFrom: null,
              createdAt: now,
              updatedAt: now,
              archivedAt: null,
              settledOverride: null,
              settledAt: null,
              lastVisitedAt: null,
              deletedAt: null,
            },
            threadId,
            runId: RunId.make("run-antigravity-system-prompt"),
            runOrdinal: 1,
            providerTurnOrdinal: 1,
            attemptId: RunAttemptId.make("attempt-antigravity-system-prompt"),
            rootNodeId: NodeId.make("node-antigravity-system-prompt"),
            providerThread,
            message: {
              createdBy: "user",
              creationSource: "web",
              messageId: MessageId.make("message-antigravity-system-prompt"),
              text: "open a PR",
              attachments: [],
            },
            modelSelection,
            runtimePolicy,
          })
          .pipe(Effect.forkScoped);
        const sent = JSON.stringify((yield* Deferred.await(promptReceived)).prompt);
        const sessionNew = (yield* fileSystem.readFileString(requestLog))
          .trim()
          .split("\n")
          .map((line) => JSON.parse(line) as { method?: string; params?: { _meta?: unknown } })
          .find((request) => request.method === "session/new");
        const append = JSON.stringify(sessionNew?.params?._meta ?? null);
        // Identity stays per turn either way.
        assert.include(sent, "through the Antigravity harness, as gemini-test-low");
        if (agentVersion === "1.4.5") {
          assert.include(append, "pull_request_linking");
          assert.notInclude(sent, "<pull_request_linking>");
        } else {
          assert.notInclude(append, "systemPrompt");
          assert.include(sent, "<pull_request_linking>");
        }
      }).pipe(Effect.provide(layerSession)),
  );
});

describe("AntigravityAdapterV2 post-settle background wakeups", () => {
  const writeStepsDb = (
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

  const openPostSettleHarness = Effect.fn("openPostSettleHarness")(function* (options: {
    readonly name: string;
    readonly runtimeMode: "full-access" | "approval-required";
    readonly onPrompt?: (input: {
      readonly sendUpdate: (
        update: EffectAcpSchema.SessionUpdate,
      ) => Effect.Effect<void, EffectAcpErrors.AcpError>;
    }) => Effect.Effect<EffectAcpSchema.PromptResponse, EffectAcpErrors.AcpError>;
    readonly onCancel?: Effect.Effect<void>;
  }) {
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const crypto = yield* SecureRandom.SecureRandom;
    const mockAgentPath = yield* path.fromFileUrl(
      new URL("../../../scripts/acp-mock-agent.ts", import.meta.url),
    );
    const workspace = yield* fileSystem.makeTempDirectoryScoped({
      prefix: `t3-ag-wake-${options.name}-`,
    });
    const profileDirectory = yield* fileSystem.makeTempDirectoryScoped({
      prefix: `t3-ag-profile-${options.name}-`,
    });
    const conversationsDir = path.join(profileDirectory, "antigravity-acp", "conversations");
    yield* fileSystem.makeDirectory(conversationsDir, { recursive: true });

    type RuntimeService = AcpSessionRuntime.AcpSessionRuntime["Service"];
    let sessionUpdateHandler: Parameters<RuntimeService["handleSessionUpdate"]>[0] | undefined;
    let requestPermissionHandler:
      | Parameters<RuntimeService["handleRequestPermission"]>[0]
      | undefined;
    let terminationHandler: ((error: EffectAcpErrors.AcpError) => Effect.Effect<void>) | undefined;
    let onIncomingRequest:
      | ((requestId: string, method: string, payload: unknown) => Effect.Effect<void>)
      | undefined;
    let onOutgoingResponse: ((requestId: string) => Effect.Effect<void>) | undefined;
    const continuationOffers =
      yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
    const instanceId = ProviderInstanceId.make(`antigravity-wake-${options.name}`);
    const threadId = ThreadId.make(`thread-antigravity-wake-${options.name}`);
    const sessionId = "mock-session-1";

    const sendUpdate = (update: EffectAcpSchema.SessionUpdate) => {
      if (sessionUpdateHandler === undefined) {
        return Effect.die("sessionUpdateHandler not registered");
      }
      return sessionUpdateHandler(normalizeAntigravitySessionUpdate({ sessionId, update }));
    };

    const adapter = yield* makeAntigravityAdapterV2({
      instanceId,
      selfInvocation: yield* resolveSelfInvocation(),
      profileDirectory,
      continuationRequests: {
        offer: (request) => Queue.offer(continuationOffers, request).pipe(Effect.asVoid),
      },
      makeRuntime: (input) => {
        terminationHandler = input.onTermination;
        onIncomingRequest = input.onIncomingRequest;
        onOutgoingResponse = input.onOutgoingResponse;
        return makeAntigravityAcpRuntime({
          ...input,
          childProcessSpawner,
          spawn: {
            command: process.execPath,
            args: [mockAgentPath],
            cwd: input.cwd,
            env: { T3_ACP_ANTIGRAVITY: "1" },
          },
        }).pipe(
          Effect.provideService(SecureRandom.SecureRandom, crypto),
          Effect.map((runtime): RuntimeService => ({
            ...runtime,
            handleSessionUpdate: (handler) =>
              Effect.sync(() => {
                sessionUpdateHandler = handler;
              }).pipe(Effect.andThen(runtime.handleSessionUpdate(handler))),
            handleRequestPermission: (handler) =>
              Effect.sync(() => {
                requestPermissionHandler = handler;
              }).pipe(Effect.andThen(runtime.handleRequestPermission(handler))),
            prompt: () =>
              options.onPrompt === undefined
                ? Effect.succeed({ stopReason: "end_turn" as const })
                : options.onPrompt({ sendUpdate }),
            ...(options.onCancel === undefined
              ? {}
              : {
                  cancel: options.onCancel.pipe(Effect.andThen(runtime.cancel)),
                }),
          })),
        );
      },
      withProcess: (_stop, task) => task,
      defaultModel: Effect.undefined,
    });

    const runtimePolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
      runtimeMode: options.runtimeMode,
      interactionMode: "default",
      cwd: workspace,
    });
    const modelSelection = { instanceId, model: "gemini-test-low" } as const;
    const session = yield* adapter.openSession({
      threadId,
      providerSessionId: ProviderSessionId.make(`provider-session-wake-${options.name}`),
      modelSelection,
      runtimePolicy,
    });
    const providerThread = yield* session.ensureThread({
      threadId,
      modelSelection,
      runtimePolicy,
    });
    const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
    yield* session.events.pipe(
      Stream.runForEach((event) => Queue.offer(events, event)),
      Effect.forkScoped,
    );

    const now = yield* DateTime.now;
    let ordinal = 0;
    const makeTurn = (
      kind: "user" | "continuation",
      text = "run tests",
      runtimeModeOverride?: ProviderAdapter.ProviderAdapterV2RuntimePolicy["runtimeMode"],
    ) => {
      ordinal += 1;
      const turnPolicy =
        runtimeModeOverride === undefined
          ? runtimePolicy
          : ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
              runtimeMode: runtimeModeOverride,
              interactionMode: "default",
              cwd: workspace,
            });
      return {
        appThread: {
          createdBy: "user" as const,
          creationSource: "web" as const,
          id: threadId,
          projectId: ProjectId.make(`project-wake-${options.name}`),
          title: `Antigravity wake ${options.name}`,
          providerInstanceId: instanceId,
          modelSelection,
          runtimeMode: turnPolicy.runtimeMode,
          interactionMode: "default" as const,
          branch: null,
          worktreePath: workspace,
          activeProviderThreadId: providerThread.id,
          lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
          forkedFrom: null,
          createdAt: now,
          updatedAt: now,
          archivedAt: null,
          settledOverride: null,
          settledAt: null,
          lastVisitedAt: null,
          deletedAt: null,
        },
        threadId,
        runId: RunId.make(`run-wake-${options.name}-${ordinal}`),
        runOrdinal: ordinal,
        providerTurnOrdinal: ordinal,
        attemptId: RunAttemptId.make(`attempt-wake-${options.name}-${ordinal}`),
        rootNodeId: NodeId.make(`node-wake-${options.name}-${ordinal}`),
        providerThread,
        message:
          kind === "continuation"
            ? {
                createdBy: "agent" as const,
                creationSource: "provider" as const,
                messageId: MessageId.make(`message-wake-${options.name}-${ordinal}`),
                text: "",
                attachments: [],
              }
            : {
                createdBy: "user" as const,
                creationSource: "web" as const,
                messageId: MessageId.make(`message-wake-${options.name}-${ordinal}`),
                text,
                attachments: [],
              },
        modelSelection,
        runtimePolicy: turnPolicy,
      };
    };

    const takeUntilTerminal = Effect.gen(function* () {
      const collected: Array<ProviderAdapter.ProviderAdapterV2Event> = [];
      while (true) {
        const event = yield* Queue.take(events);
        collected.push(event);
        if (event.type === "turn.terminal") return collected;
      }
    });

    const requestPermission = (
      request: EffectAcpSchema.RequestPermissionRequest,
      id = "perm-1",
    ) => {
      if (requestPermissionHandler === undefined) {
        return Effect.die("requestPermissionHandler not registered");
      }
      const handle = requestPermissionHandler;
      // Mirrors real ACP transport ordering: onIncomingRequest before handler, onOutgoingResponse after.
      return (onIncomingRequest?.(id, "session/request_permission", request) ?? Effect.void).pipe(
        Effect.andThen(
          handle(request, {
            requestId: id,
            method: "session/request_permission",
          }),
        ),
        Effect.tap(() => onOutgoingResponse?.(id) ?? Effect.void),
      );
    };

    const terminateRuntime = () => {
      if (terminationHandler === undefined) {
        return Effect.die("terminationHandler not registered");
      }
      return terminationHandler(
        new EffectAcpErrors.AcpTransportError({
          detail: "Antigravity process exited",
          cause: "exit",
        }),
      );
    };

    return {
      session,
      sessionId,
      providerThread,
      events,
      continuationOffers,
      stepsDbPath: path.join(conversationsDir, `${sessionId}.db`),
      summariesDbPath: path.join(profileDirectory, "antigravity-acp", "conversation_summaries.db"),
      sendUpdate,
      requestPermission,
      terminateRuntime,
      makeTurn,
      takeUntilTerminal,
    };
  });

  it.effect(
    "auto-approves full-access post-settle permission, opens continuation, and waits for active SQLite generation",
    () =>
      Effect.gen(function* () {
        const harness = yield* openPostSettleHarness({
          name: "full-access",
          runtimeMode: "full-access",
        });

        // Turn 1 settles while a background command is still running in Antigravity's DB.
        yield* harness.session.startTurn(harness.makeTurn("user"));
        const turn1Events = yield* harness.takeUntilTerminal;
        assert.isTrue(
          turn1Events.some(
            (event) => event.type === "turn.terminal" && event.status === "completed",
          ),
        );

        writeStepsDb(harness.stepsDbPath, [
          { idx: 1, status: 2, taskDetails: Buffer.from("task-723") },
        ]);
        assert.isTrue(yield* harness.session.hasPendingBackgroundWork!);

        // Background command finishes and Antigravity wakes up with thinking + edit_file permission.
        writeStepsDb(harness.stepsDbPath, [
          { idx: 1, status: 3, taskDetails: Buffer.from("task-723") },
          { idx: 2, status: 8, taskDetails: null },
        ]);
        yield* harness.sendUpdate({
          sessionUpdate: "agent_thought_chunk",
          content: { type: "text", text: "Background tests finished; checking mutation." },
        });
        yield* harness.sendUpdate({
          sessionUpdate: "tool_call",
          toolCallId: "edit-1",
          title: "Edit ads.go",
          kind: "edit",
          status: "pending",
        });

        // Previously this threw -32603 ("No active ACP turn") even in full-access mode.
        const permissionResponse = yield* harness.requestPermission({
          sessionId: harness.sessionId,
          toolCall: { toolCallId: "edit-1", kind: "edit", title: "Edit ads.go" },
          options: [
            { optionId: "allow-once", name: "Allow once", kind: "allow_once" },
            { optionId: "reject-once", name: "Deny", kind: "reject_once" },
          ],
        });
        assert.deepEqual(permissionResponse, {
          outcome: { outcome: "selected", optionId: "allow-once" },
        });

        yield* harness.sendUpdate({
          sessionUpdate: "tool_call_update",
          toolCallId: "edit-1",
          status: "completed",
        });

        // Completed tool offers a continuation run.
        const offer = yield* Queue.take(harness.continuationOffers);
        assert.equal(offer.driver, "antigravity");

        yield* harness.session.startTurn(harness.makeTurn("continuation"));
        const terminalFiber = yield* harness.takeUntilTerminal.pipe(Effect.forkScoped);

        // SQLite step status = 8 (GENERATING) prevents premature 3s debounce settlement.
        yield* TestClock.adjust("3100 millis");
        assert.isUndefined(terminalFiber.pollUnsafe());

        // Agent emits its final message and SQLite step transitions to DONE (3).
        yield* harness.sendUpdate({
          sessionUpdate: "agent_message_chunk",
          content: { type: "text", text: "Mutation check passed." },
        });
        writeStepsDb(harness.stepsDbPath, [
          { idx: 1, status: 3, taskDetails: Buffer.from("task-723") },
          { idx: 2, status: 3, taskDetails: null },
        ]);

        yield* TestClock.adjust("3100 millis");
        const turn2Events = yield* Fiber.join(terminalFiber);
        assert.isTrue(
          turn2Events.some(
            (event) => event.type === "turn.terminal" && event.status === "completed",
          ),
        );
        assert.isTrue(
          turn2Events.some(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.type === "assistant_message" &&
              event.turnItem.text.includes("Mutation check passed."),
          ),
        );
        assert.isFalse(yield* harness.session.hasPendingBackgroundWork!);

        // Chained background command inside a continuation turn: Antigravity leaves the backgrounded
        // tool_call in `in_progress` without a terminal update, while SQLite records task_details.
        yield* harness.sendUpdate({
          sessionUpdate: "agent_message_chunk",
          content: { type: "text", text: "Launching second Docker suite..." },
        });
        yield* Queue.take(harness.continuationOffers);
        yield* harness.session.startTurn(harness.makeTurn("continuation"));
        yield* harness.sendUpdate({
          sessionUpdate: "tool_call",
          toolCallId: "bg-cmd-2",
          title: "Run TestMonitorAccountStatusSuite",
          kind: "execute",
          status: "in_progress",
        });
        writeStepsDb(harness.stepsDbPath, [
          { idx: 1, status: 3, taskDetails: Buffer.from("task-723") },
          { idx: 2, status: 3, taskDetails: null },
          { idx: 3, status: 2, taskDetails: Buffer.from("task-740") },
        ]);

        // Run 3 settles cleanly despite bg-cmd-2 staying in_progress, while pinning background work.
        const turn3TerminalFiber = yield* harness.takeUntilTerminal.pipe(Effect.forkScoped);
        yield* TestClock.adjust("3100 millis");
        const turn3Events = yield* Fiber.join(turn3TerminalFiber);
        assert.isTrue(
          turn3Events.some(
            (event) => event.type === "turn.terminal" && event.status === "completed",
          ),
        );
        assert.isTrue(yield* harness.session.hasPendingBackgroundWork!);
      }).pipe(Effect.provide(layerSession)),
  );

  it.effect(
    "opens a continuation run and pauses debounce for post-settle approval requests in approval-required mode",
    () =>
      Effect.gen(function* () {
        const harness = yield* openPostSettleHarness({
          name: "approval-required",
          runtimeMode: "approval-required",
        });

        yield* harness.session.startTurn(harness.makeTurn("user"));
        yield* harness.takeUntilTerminal;

        // Post-settle permission arrives with no prior buffered wake frames (drainedWakeCount === 0).
        const permissionFiber = yield* harness
          .requestPermission({
            sessionId: harness.sessionId,
            toolCall: { toolCallId: "edit-ask-1", kind: "edit", title: "Edit ads.go" },
            options: [
              { optionId: "allow-once", name: "Allow once", kind: "allow_once" },
              { optionId: "reject-once", name: "Deny", kind: "reject_once" },
            ],
          })
          .pipe(Effect.forkScoped);

        // awaitRequestTurn offers a continuation run automatically.
        const offer = yield* Queue.take(harness.continuationOffers);
        assert.equal(offer.driver, "antigravity");

        yield* harness.session.startTurn(harness.makeTurn("continuation"));

        // Wait until the approval runtime request is projected on the continuation turn.
        let pendingRequestId:
          | Parameters<typeof harness.session.respondToRuntimeRequest>[0]["requestId"]
          | undefined;
        while (pendingRequestId === undefined) {
          const event = yield* Queue.take(harness.events);
          if (
            event.type === "runtime_request.updated" &&
            event.runtimeRequest.status === "pending"
          ) {
            pendingRequestId = event.runtimeRequest.id;
          }
        }

        // Advancing past 3s debounce while approval is pending must NOT finalize the turn.
        const terminalFiber = yield* harness.takeUntilTerminal.pipe(Effect.forkScoped);
        yield* TestClock.adjust("6000 millis");
        assert.isUndefined(terminalFiber.pollUnsafe());

        // User approves the request; permission resolves and the turn settles after debounce.
        yield* harness.session.respondToRuntimeRequest({
          requestId: pendingRequestId,
          decision: "accept",
        });
        assert.deepEqual(yield* Fiber.join(permissionFiber), {
          outcome: { outcome: "selected", optionId: "allow-once" },
        });

        yield* TestClock.adjust("3100 millis");
        const turn2Events = yield* Fiber.join(terminalFiber);
        assert.isTrue(
          turn2Events.some(
            (event) => event.type === "turn.terminal" && event.status === "completed",
          ),
        );

        // Post-settle interaction_* question also opens a continuation run and waits for answer.
        const questionFiber = yield* harness
          .requestPermission(
            {
              sessionId: harness.sessionId,
              toolCall: { toolCallId: "interaction_wake_1", title: "Which target?" },
              options: [
                { optionId: "unit", name: "Unit tests", kind: "allow_once" },
                { optionId: "all", name: "All tests", kind: "allow_once" },
              ],
            },
            "perm-question-1",
          )
          .pipe(Effect.forkScoped);

        yield* Queue.take(harness.continuationOffers);
        yield* harness.session.startTurn(harness.makeTurn("continuation"));

        let questionRequestId:
          | Parameters<typeof harness.session.respondToRuntimeRequest>[0]["requestId"]
          | undefined;
        while (questionRequestId === undefined) {
          const event = yield* Queue.take(harness.events);
          if (
            event.type === "runtime_request.updated" &&
            event.runtimeRequest.status === "pending"
          ) {
            questionRequestId = event.runtimeRequest.id;
          }
        }

        const questionTerminalFiber = yield* harness.takeUntilTerminal.pipe(Effect.forkScoped);
        yield* TestClock.adjust("6000 millis");
        assert.isUndefined(questionTerminalFiber.pollUnsafe());

        yield* harness.session.respondToRuntimeRequest({
          requestId: questionRequestId,
          answers: { interaction_wake_1: "all" },
        });
        assert.deepEqual(yield* Fiber.join(questionFiber), {
          outcome: { outcome: "selected", optionId: "all" },
        });

        yield* TestClock.adjust("3100 millis");
        const turn3Events = yield* Fiber.join(questionTerminalFiber);
        assert.isTrue(
          turn3Events.some(
            (event) => event.type === "turn.terminal" && event.status === "completed",
          ),
        );
      }).pipe(Effect.provide(layerSession)),
  );

  it.effect("cancels late full-access permission requests after an explicit Stop", () =>
    Effect.gen(function* () {
      const promptHold = yield* Deferred.make<void>();
      const harness = yield* openPostSettleHarness({
        name: "stop-quarantine",
        runtimeMode: "full-access",
        onPrompt: () =>
          Deferred.await(promptHold).pipe(Effect.as({ stopReason: "cancelled" as const })),
        onCancel: Deferred.succeed(promptHold, undefined).pipe(Effect.asVoid),
      });

      const turn1 = harness.makeTurn("user");
      yield* harness.session.startTurn(turn1);

      let providerTurnId:
        | Parameters<typeof harness.session.interruptTurn>[0]["providerTurnId"]
        | undefined;
      while (providerTurnId === undefined) {
        const event = yield* Queue.take(harness.events);
        if (event.type === "provider_turn.updated" && event.providerTurn.status === "running") {
          providerTurnId = event.providerTurn.id;
        }
      }

      yield* harness.session.interruptTurn({
        providerThread: harness.providerThread,
        providerTurnId,
        requestRuntimeRestart: true,
      });
      yield* harness.takeUntilTerminal;

      const latePermission = yield* harness.requestPermission({
        sessionId: harness.sessionId,
        toolCall: { toolCallId: "late-edit", kind: "edit", title: "Late edit" },
        options: [
          { optionId: "allow-once", name: "Allow once", kind: "allow_once" },
          { optionId: "reject-once", name: "Deny", kind: "reject_once" },
        ],
      });
      assert.deepEqual(latePermission, { outcome: { outcome: "cancelled" } });
    }).pipe(Effect.provide(layerSession)),
  );

  it.effect(
    "still settles normal turns with invoke_subagent immediately and fails continuation on runtime exit",
    () =>
      Effect.gen(function* () {
        const harness = yield* openPostSettleHarness({
          name: "subagent-and-exit",
          runtimeMode: "full-access",
          onPrompt: ({ sendUpdate }) =>
            Effect.gen(function* () {
              yield* sendUpdate({
                sessionUpdate: "tool_call",
                toolCallId: "subagent-call-1",
                title: "Running start_subagent",
                kind: "other",
                status: "completed",
                rawOutput: "Started helper.",
              });
              return { stopReason: "end_turn" as const };
            }),
        });

        // Normal turn settles immediately despite extractSubagentUpdate keeping status "running".
        yield* harness.session.startTurn(harness.makeTurn("user"));
        const turn1Events = yield* harness.takeUntilTerminal;
        assert.isTrue(
          turn1Events.some(
            (event) => event.type === "turn.terminal" && event.status === "completed",
          ),
        );
        assert.isTrue(
          turn1Events.some(
            (event) => event.type === "subagent.updated" && event.subagent.status === "idle",
          ),
        );

        // Post-settle text wake opens a continuation run, then runtime dies mid-continuation.
        yield* harness.sendUpdate({
          sessionUpdate: "agent_message_chunk",
          content: { type: "text", text: "Subagent finished, continuing..." },
        });
        yield* Queue.take(harness.continuationOffers);
        yield* harness.session.startTurn(harness.makeTurn("continuation"));

        const terminalFiber = yield* harness.takeUntilTerminal.pipe(Effect.forkScoped);
        yield* harness.terminateRuntime();
        const turn2Events = yield* Fiber.join(terminalFiber);
        assert.isTrue(
          turn2Events.some((event) => event.type === "turn.terminal" && event.status === "failed"),
        );

        // Stale RUNNING rows left on disk after runtime exit must not keep background work pinned.
        const summariesDb = new NodeSqlite.DatabaseSync(harness.summariesDbPath);
        try {
          summariesDb.exec(
            `CREATE TABLE IF NOT EXISTS conversation_summaries (
              conversation_id TEXT PRIMARY KEY,
              status TEXT NOT NULL DEFAULT '',
              parent_conversation_id TEXT NOT NULL DEFAULT '',
              not_fully_idle NUMERIC NOT NULL DEFAULT 0,
              killed NUMERIC NOT NULL DEFAULT 0
            )`,
          );
          summariesDb
            .prepare(
              "INSERT INTO conversation_summaries (conversation_id, status, not_fully_idle) VALUES (?, 'CASCADE_RUN_STATUS_RUNNING', 1)",
            )
            .run(harness.sessionId);
        } finally {
          summariesDb.close();
        }
        assert.isFalse(yield* harness.session.hasPendingBackgroundWork ?? Effect.succeed(false));
      }).pipe(Effect.provide(layerSession)),
  );

  it.effect(
    "settles a continuation that starts a subagent and classifies native trajectory errors",
    () =>
      Effect.gen(function* () {
        const harness = yield* openPostSettleHarness({
          name: "continuation-subagent-and-error",
          runtimeMode: "full-access",
        });

        yield* harness.session.startTurn(harness.makeTurn("user"));
        yield* harness.takeUntilTerminal;

        // Continuation launches a subagent (projected as running until finalizeTurn) and finishes.
        yield* harness.sendUpdate({
          sessionUpdate: "tool_call",
          toolCallId: "cont-subagent-1",
          title: "Running start_subagent",
          kind: "other",
          status: "completed",
          rawOutput: "Started helper.",
        });
        yield* Queue.take(harness.continuationOffers);
        yield* harness.session.startTurn(harness.makeTurn("continuation"));

        const subagentTerminal = yield* harness.takeUntilTerminal.pipe(Effect.forkScoped);
        yield* TestClock.adjust("3100 millis");
        const cont1Events = yield* Fiber.join(subagentTerminal);
        assert.isTrue(
          cont1Events.some(
            (event) => event.type === "turn.terminal" && event.status === "completed",
          ),
        );

        // Next continuation writes an Antigravity QUOTA_EXHAUSTED error step (step_type = 17).
        const stepsDb = new NodeSqlite.DatabaseSync(harness.stepsDbPath);
        try {
          stepsDb.exec(
            "CREATE TABLE IF NOT EXISTS steps (idx INTEGER PRIMARY KEY, step_type INTEGER NOT NULL DEFAULT 0, status INTEGER NOT NULL DEFAULT 0, task_details BLOB, step_payload BLOB)",
          );
          stepsDb
            .prepare(
              "INSERT INTO steps (idx, step_type, status, task_details, step_payload) VALUES (1, 17, 7, NULL, ?)",
            )
            .run(
              Buffer.from(
                "Error 429, Message: quota hit [reason:QUOTA_EXHAUSTED], Status: RESOURCE_EXHAUSTED",
              ),
            );
        } finally {
          stepsDb.close();
        }
        yield* harness.sendUpdate({
          sessionUpdate: "agent_message_chunk",
          content: { type: "text", text: "Quota reached" },
        });
        yield* Queue.take(harness.continuationOffers);
        yield* harness.session.startTurn(harness.makeTurn("continuation"));
        const errTerminal = yield* harness.takeUntilTerminal.pipe(Effect.forkScoped);
        yield* TestClock.adjust("3100 millis");
        const cont2Events = yield* Fiber.join(errTerminal);
        const terminal = cont2Events.find((event) => event.type === "turn.terminal");
        assert.strictEqual(terminal?.status, "failed");
        assert.strictEqual(terminal?.failure?.class, "usage_limit");
      }).pipe(Effect.provide(layerSession)),
  );

  it.effect(
    "keeps late approval requests bound to their continuation turn when a user turn preempts",
    () =>
      Effect.gen(function* () {
        const harness = yield* openPostSettleHarness({
          name: "late-approval-ownership",
          runtimeMode: "approval-required",
        });

        yield* harness.session.startTurn(harness.makeTurn("user"));
        yield* harness.takeUntilTerminal;

        // Background work requests edit permission while idle under approval-required.
        const latePermissionFiber = yield* harness
          .requestPermission(
            {
              sessionId: harness.sessionId,
              toolCall: { toolCallId: "late-edit-1", kind: "edit", title: "Edit file" },
              options: [
                { optionId: "allow-once", name: "Allow once", kind: "allow_once" },
                { optionId: "reject-once", name: "Deny", kind: "reject_once" },
              ],
            },
            "perm-late-ownership",
          )
          .pipe(Effect.forkScoped);
        yield* Queue.take(harness.continuationOffers);

        // A normal user message with full-access preempts before the continuation attaches.
        yield* harness.session.startTurn(harness.makeTurn("user", "quick question", "full-access"));
        const userEvents = yield* harness.takeUntilTerminal;
        assert.isFalse(userEvents.some((event) => event.type === "runtime_request.updated"));
        assert.isUndefined(latePermissionFiber.pollUnsafe());

        // Once the continuation attaches, it owns the approval under the original approval-required policy.
        yield* harness.session.startTurn(harness.makeTurn("continuation"));
        let approvalRequestId:
          | Parameters<typeof harness.session.respondToRuntimeRequest>[0]["requestId"]
          | undefined;
        while (approvalRequestId === undefined) {
          const event = yield* Queue.take(harness.events);
          if (
            event.type === "runtime_request.updated" &&
            event.runtimeRequest.status === "pending"
          ) {
            approvalRequestId = event.runtimeRequest.id;
          }
        }
        yield* harness.session.respondToRuntimeRequest({
          requestId: approvalRequestId,
          decision: "accept",
        });
        assert.deepEqual(yield* Fiber.join(latePermissionFiber), {
          outcome: { outcome: "selected", optionId: "allow-once" },
        });
        const terminalFiber = yield* harness.takeUntilTerminal.pipe(Effect.forkScoped);
        yield* TestClock.adjust("3100 millis");
        const contEvents = yield* Fiber.join(terminalFiber);
        assert.isTrue(
          contEvents.some(
            (event) => event.type === "turn.terminal" && event.status === "completed",
          ),
        );
      }).pipe(Effect.provide(layerSession)),
  );

  it.effect(
    "falls back to ACP tool liveness when native DB is missing and fails continuation on active timeout",
    () =>
      Effect.gen(function* () {
        const harness = yield* openPostSettleHarness({
          name: "unknown-db-and-timeout",
          runtimeMode: "full-access",
        });

        yield* harness.session.startTurn(harness.makeTurn("user"));
        yield* harness.takeUntilTerminal;

        // Native DB is absent -> unknown -> in_progress foreground tool keeps continuation open past 3s.
        yield* harness.sendUpdate({
          sessionUpdate: "agent_message_chunk",
          content: { type: "text", text: "Running verification..." },
        });
        yield* Queue.take(harness.continuationOffers);
        yield* harness.session.startTurn(harness.makeTurn("continuation"));
        yield* harness.sendUpdate({
          sessionUpdate: "tool_call",
          toolCallId: "fg-cmd-1",
          title: "Running npm test",
          kind: "execute",
          status: "in_progress",
        });
        const toolTerminal = yield* harness.takeUntilTerminal.pipe(Effect.forkScoped);
        yield* TestClock.adjust("6000 millis");
        assert.isUndefined(toolTerminal.pollUnsafe());

        yield* harness.sendUpdate({
          sessionUpdate: "tool_call_update",
          toolCallId: "fg-cmd-1",
          status: "completed",
        });
        yield* TestClock.adjust("3100 millis");
        const cont1Events = yield* Fiber.join(toolTerminal);
        assert.isTrue(
          cont1Events.some(
            (event) => event.type === "turn.terminal" && event.status === "completed",
          ),
        );

        // When native DB stays active past the 15-minute cap, fail explicitly instead of completing.
        const stepsDb = new NodeSqlite.DatabaseSync(harness.stepsDbPath);
        try {
          stepsDb.exec(
            "CREATE TABLE IF NOT EXISTS steps (idx INTEGER PRIMARY KEY, step_type INTEGER NOT NULL DEFAULT 0, status INTEGER NOT NULL DEFAULT 0, task_details BLOB)",
          );
          stepsDb
            .prepare(
              "INSERT INTO steps (idx, step_type, status, task_details) VALUES (1, 132, 8, NULL)",
            )
            .run();
        } finally {
          stepsDb.close();
        }
        yield* harness.sendUpdate({
          sessionUpdate: "agent_message_chunk",
          content: { type: "text", text: "Working..." },
        });
        yield* Queue.take(harness.continuationOffers);
        yield* harness.session.startTurn(harness.makeTurn("continuation"));
        const timeoutTerminal = yield* harness.takeUntilTerminal.pipe(Effect.forkScoped);
        yield* TestClock.adjust("16 minutes");
        yield* TestClock.adjust("3100 millis");
        const cont2Events = yield* Fiber.join(timeoutTerminal);
        assert.isTrue(
          cont2Events.some((event) => event.type === "turn.terminal" && event.status === "failed"),
        );
      }).pipe(Effect.provide(layerSession)),
  );
});
