import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it, vi } from "@effect/vitest";
import {
  ApprovalRequestId,
  EventId,
  RuntimeItemId,
  RuntimeRequestId,
  ThreadId,
  TurnId,
  type ProviderRuntimeEvent,
  type ProviderSendTurnInput,
  type ProviderSession,
  type ProviderSessionStartInput,
  type ProviderTurnStartResult,
} from "@t3tools/contracts";
import { Effect, Fiber, Layer, Option, PubSub, Ref, Stream } from "effect";

import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import { ProviderSessionRuntimeRepositoryLive } from "../../persistence/Layers/ProviderSessionRuntime.ts";
import { ProviderSessionRuntimeRepository } from "../../persistence/Services/ProviderSessionRuntime.ts";
import { AnalyticsService } from "../../telemetry/Services/AnalyticsService.ts";
import { type ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import { ProviderAdapterRegistry } from "../Services/ProviderAdapterRegistry.ts";
import { ProviderService } from "../Services/ProviderService.ts";
import { ProviderSessionDirectoryLive } from "./ProviderSessionDirectory.ts";
import { makeProviderServiceLive } from "./ProviderService.ts";

const asEventId = (value: string): EventId => EventId.makeUnsafe(value);
const asThreadId = (value: string): ThreadId => ThreadId.makeUnsafe(value);
const asTurnId = (value: string): TurnId => TurnId.makeUnsafe(value);
const asRuntimeItemId = (value: string): RuntimeItemId => RuntimeItemId.makeUnsafe(value);
const asRuntimeRequestId = (value: string): RuntimeRequestId =>
  RuntimeRequestId.makeUnsafe(value);
const asApprovalRequestId = (value: string): ApprovalRequestId =>
  ApprovalRequestId.makeUnsafe(value);
const sleep = (ms: number) =>
  Effect.promise(() => new Promise<void>((resolve) => setTimeout(resolve, ms)));

function makeCcbEvent(threadId: ThreadId, turnId: TurnId, index: number): ProviderRuntimeEvent[] {
  const createdAt = new Date(1_800_000_000_000 + index).toISOString();
  return [
    {
      type: "turn.started",
      eventId: asEventId("ccb-msg-evt-turn-started"),
      provider: "ccb",
      threadId,
      turnId,
      createdAt,
      payload: { model: "claude-sonnet-4-6" },
    },
    {
      type: "content.delta",
      eventId: asEventId("ccb-msg-evt-assistant-delta"),
      provider: "ccb",
      threadId,
      turnId,
      createdAt,
      payload: { streamKind: "assistant_text", delta: "I will update the project." },
    },
    {
      type: "content.delta",
      eventId: asEventId("ccb-msg-evt-reasoning-delta"),
      provider: "ccb",
      threadId,
      turnId,
      createdAt,
      payload: { streamKind: "reasoning_text", delta: "Inspect, edit, verify." },
    },
    {
      type: "turn.tasks.updated",
      eventId: asEventId("ccb-msg-evt-tasks"),
      provider: "ccb",
      threadId,
      turnId,
      createdAt,
      payload: {
        tasks: [
          { task: "Inspect files", status: "completed" },
          { task: "Apply edit", status: "inProgress" },
        ],
      },
    },
    {
      type: "item.started",
      eventId: asEventId("ccb-msg-evt-command-started"),
      provider: "ccb",
      threadId,
      turnId,
      itemId: asRuntimeItemId("ccb-tool-bash"),
      createdAt,
      payload: {
        itemType: "command_execution",
        status: "inProgress",
        title: "Bash",
        data: { command: "bun run test" },
      },
    },
    {
      type: "item.completed",
      eventId: asEventId("ccb-msg-evt-command-completed"),
      provider: "ccb",
      threadId,
      turnId,
      itemId: asRuntimeItemId("ccb-tool-bash"),
      createdAt,
      payload: {
        itemType: "command_execution",
        status: "completed",
        title: "Bash",
        detail: "tests passed",
      },
    },
    {
      type: "item.started",
      eventId: asEventId("ccb-msg-evt-file-started"),
      provider: "ccb",
      threadId,
      turnId,
      itemId: asRuntimeItemId("ccb-tool-edit"),
      createdAt,
      payload: {
        itemType: "file_change",
        status: "inProgress",
        title: "Edit",
        data: { file_path: "src/app.ts" },
      },
    },
    {
      type: "item.completed",
      eventId: asEventId("ccb-msg-evt-file-completed"),
      provider: "ccb",
      threadId,
      turnId,
      itemId: asRuntimeItemId("ccb-tool-edit"),
      createdAt,
      payload: {
        itemType: "file_change",
        status: "completed",
        title: "Edit",
      },
    },
    {
      type: "item.completed",
      eventId: asEventId("ccb-msg-evt-mcp-completed"),
      provider: "ccb",
      threadId,
      turnId,
      itemId: asRuntimeItemId("ccb-tool-mcp"),
      createdAt,
      payload: {
        itemType: "mcp_tool_call",
        status: "completed",
        title: "MCP search",
      },
    },
    {
      type: "item.completed",
      eventId: asEventId("ccb-msg-evt-agent-completed"),
      provider: "ccb",
      threadId,
      turnId,
      itemId: asRuntimeItemId("ccb-tool-task"),
      createdAt,
      payload: {
        itemType: "collab_agent_tool_call",
        status: "completed",
        title: "Task",
      },
    },
    {
      type: "item.completed",
      eventId: asEventId("ccb-msg-evt-web-completed"),
      provider: "ccb",
      threadId,
      turnId,
      itemId: asRuntimeItemId("ccb-tool-web"),
      createdAt,
      payload: {
        itemType: "web_search",
        status: "completed",
        title: "WebSearch",
      },
    },
    {
      type: "request.opened",
      eventId: asEventId("ccb-msg-evt-approval-opened"),
      provider: "ccb",
      threadId,
      turnId,
      requestId: asRuntimeRequestId("ccb-approval-1"),
      createdAt,
      payload: {
        requestType: "file_change_approval",
        detail: "Edit src/app.ts",
      },
    },
    {
      type: "request.resolved",
      eventId: asEventId("ccb-msg-evt-approval-resolved"),
      provider: "ccb",
      threadId,
      turnId,
      requestId: asRuntimeRequestId("ccb-approval-1"),
      createdAt,
      payload: {
        requestType: "file_change_approval",
        decision: "accept",
      },
    },
    {
      type: "turn.diff.updated",
      eventId: asEventId("ccb-msg-evt-diff"),
      provider: "ccb",
      threadId,
      turnId,
      createdAt,
      payload: { unifiedDiff: "diff --git a/src/app.ts b/src/app.ts" },
    },
    {
      type: "item.completed",
      eventId: asEventId("ccb-msg-evt-compact"),
      provider: "ccb",
      threadId,
      turnId,
      itemId: asRuntimeItemId("ccb-compact-1"),
      createdAt,
      payload: {
        itemType: "context_compaction",
        status: "completed",
        title: "Context compact requested",
      },
    },
    {
      type: "turn.completed",
      eventId: asEventId("ccb-msg-evt-turn-completed"),
      provider: "ccb",
      threadId,
      turnId,
      createdAt,
      payload: { state: "completed", stopReason: "success" },
    },
  ];
}

function makeFakeCcbAdapter() {
  const sessions = new Map<ThreadId, ProviderSession>();
  const eventPubSub = Effect.runSync(PubSub.unbounded<ProviderRuntimeEvent>());

  const startSession = vi.fn((input: ProviderSessionStartInput) =>
    Effect.sync(() => {
      const now = new Date().toISOString();
      const session: ProviderSession = {
        provider: "ccb",
        status: "ready",
        runtimeMode: input.runtimeMode,
        threadId: input.threadId,
        cwd: input.cwd ?? process.cwd(),
        resumeCursor: input.resumeCursor ?? {
          ccbSessionId: `ccb-session-${String(input.threadId)}`,
          turnCount: 0,
        },
        createdAt: now,
        updatedAt: now,
        ...(input.modelSelection?.provider === "ccb" ? { model: input.modelSelection.model } : {}),
      };
      sessions.set(input.threadId, session);
      return session;
    }),
  );

  const sendTurn = vi.fn(
    (input: ProviderSendTurnInput): Effect.Effect<ProviderTurnStartResult, ProviderAdapterError> =>
      Effect.gen(function* () {
        const turnId = asTurnId(`turn-${String(input.threadId)}`);
        for (const event of makeCcbEvent(input.threadId, turnId, 1)) {
          yield* PubSub.publish(eventPubSub, event);
        }
        return {
          threadId: input.threadId,
          turnId,
          resumeCursor: {
            ccbSessionId: `ccb-session-${String(input.threadId)}`,
            turnCount: 1,
          },
        };
      }),
  );
  const respondToRequest = vi.fn(() => Effect.void);

  const adapter: ProviderAdapterShape<ProviderAdapterError> = {
    provider: "ccb",
    capabilities: {
      sessionModelSwitch: "in-session",
      supportsThreadCompaction: true,
    },
    startSession,
    sendTurn,
    interruptTurn: vi.fn(() => Effect.void),
    respondToRequest,
    respondToUserInput: vi.fn(() => Effect.void),
    stopSession: vi.fn((threadId) =>
      Effect.sync(() => {
        sessions.delete(threadId);
      }),
    ),
    listSessions: vi.fn(() => Effect.sync(() => Array.from(sessions.values()))),
    hasSession: vi.fn((threadId) => Effect.sync(() => sessions.has(threadId))),
    readThread: vi.fn((threadId) =>
      Effect.succeed({
        threadId,
        turns: [{ id: asTurnId("turn-1"), items: [] }],
      }),
    ),
    rollbackThread: vi.fn((threadId) => Effect.succeed({ threadId, turns: [] })),
    compactThread: vi.fn(() => Effect.void),
    stopAll: vi.fn(() =>
      Effect.sync(() => {
        sessions.clear();
      }),
    ),
    streamEvents: Stream.fromPubSub(eventPubSub),
  };

  return { adapter, startSession, sendTurn, respondToRequest };
}

function makeLayer(ccb: ReturnType<typeof makeFakeCcbAdapter>) {
  const runtimeRepositoryLayer = ProviderSessionRuntimeRepositoryLive.pipe(
    Layer.provide(SqlitePersistenceMemory),
  );
  const directoryLayer = ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepositoryLayer));
  const registry = Layer.succeed(ProviderAdapterRegistry, {
    getByProvider: (provider) =>
      provider === "ccb"
        ? Effect.succeed(ccb.adapter)
        : Effect.die(`unexpected provider ${provider}`),
    listProviders: () => Effect.succeed(["ccb" as const]),
  });

  return Layer.mergeAll(
    makeProviderServiceLive().pipe(
      Layer.provide(registry),
      Layer.provide(directoryLayer),
      Layer.provide(AnalyticsService.layerTest),
    ),
    directoryLayer,
    runtimeRepositoryLayer,
    NodeServices.layer,
  );
}

it.effect("ProviderServiceLive defaults to CCB and routes AI coding message events", () =>
  Effect.gen(function* () {
    const ccb = makeFakeCcbAdapter();
    const threadId = asThreadId("thread-ccb-provider-message");

    yield* Effect.gen(function* () {
      const provider = yield* ProviderService;
      const runtimeRepository = yield* ProviderSessionRuntimeRepository;
      const eventsRef = yield* Ref.make<Array<ProviderRuntimeEvent>>([]);
      const eventsFiber = yield* Stream.runForEach(provider.streamEvents, (event) =>
        Ref.update(eventsRef, (current) => [...current, event]),
      ).pipe(Effect.forkChild);
      yield* sleep(50);

      const session = yield* provider.startSession(threadId, {
        threadId,
        cwd: "/tmp/ccb-provider-message",
        runtimeMode: "full-access",
        modelSelection: {
          provider: "ccb",
          model: "claude-sonnet-4-6",
        },
      });
      assert.equal(session.provider, "ccb");
      assert.equal(ccb.startSession.mock.calls[0]?.[0]?.provider, "ccb");

      const turn = yield* provider.sendTurn({
        threadId,
        input: "Implement the feature, update files, run checks, and summarize.",
        attachments: [],
        modelSelection: {
          provider: "ccb",
          model: "claude-sonnet-4-6",
        },
      });
      assert.equal(turn.threadId, threadId);
      assert.equal(ccb.sendTurn.mock.calls.length, 1);

      yield* sleep(50);
      const events = yield* Ref.get(eventsRef);
      yield* Fiber.interrupt(eventsFiber);
      const eventTypes = new Set(events.map((event) => event.type));
      assert.equal(eventTypes.has("turn.started"), true);
      assert.equal(eventTypes.has("content.delta"), true);
      assert.equal(eventTypes.has("turn.tasks.updated"), true);
      assert.equal(eventTypes.has("item.started"), true);
      assert.equal(eventTypes.has("item.completed"), true);
      assert.equal(eventTypes.has("request.opened"), true);
      assert.equal(eventTypes.has("request.resolved"), true);
      assert.equal(eventTypes.has("turn.diff.updated"), true);
      assert.equal(eventTypes.has("turn.completed"), true);
      assert.equal(
        events.some(
          (event) =>
            event.type === "content.delta" && event.payload.streamKind === "reasoning_text",
        ),
        true,
      );
      for (const itemType of [
        "command_execution",
        "file_change",
        "mcp_tool_call",
        "collab_agent_tool_call",
        "web_search",
        "context_compaction",
      ]) {
        assert.equal(
          events.some(
            (event) => event.type === "item.completed" && event.payload.itemType === itemType,
          ),
          true,
        );
      }

      yield* provider.respondToRequest({
        threadId,
        requestId: asApprovalRequestId("ccb-approval-1"),
        decision: "accept",
      });
      assert.equal(ccb.respondToRequest.mock.calls.length, 1);

      const runtime = yield* runtimeRepository.getByThreadId({ threadId });
      assert.equal(Option.isSome(runtime), true);
      if (Option.isSome(runtime)) {
        assert.equal(runtime.value.providerName, "ccb");
        assert.deepEqual(runtime.value.resumeCursor, {
          ccbSessionId: `ccb-session-${String(threadId)}`,
          turnCount: 1,
        });
      }
    }).pipe(Effect.provide(makeLayer(ccb)));
  }),
);
