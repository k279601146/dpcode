import { assert, it, vi } from "@effect/vitest";
import { Effect, Fiber, Stream } from "effect";
import { ThreadId } from "@t3tools/contracts";
import { mkdtempSync, mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { CcbAdapter } from "../Services/CcbAdapter.ts";
import { makeCcbAdapterLive, type CcbAdapterLiveOptions } from "./CcbAdapter.ts";

function makeFakeBridge(messages: ReadonlyArray<Record<string, unknown>>): CcbAdapterLiveOptions {
  return {
    bridgeModule: {
      createDpcodeCcbSession: vi.fn(async () => ({
        sessionId: "ccb-session-test",
        async *submitMessage() {
          for (const message of messages) {
            yield message;
          }
        },
        interrupt: vi.fn(),
        resetAbortController: vi.fn(),
        getAbortSignal: () => new AbortController().signal,
        getMessages: () => messages,
        setModel: vi.fn(),
      })),
    },
  };
}

function makeFakeVendorBridge(): CcbAdapterLiveOptions {
  const root = mkdtempSync(join(tmpdir(), "dpcode-ccb-adapter-"));
  const sourceDir = join(root, "src");
  const dpcodeDir = join(sourceDir, "dpcode");
  const bundlePath = join(root, "bridge-bundle.mjs");

  mkdirSync(dpcodeDir, { recursive: true });
  writeFileSync(join(dpcodeDir, "bridge.ts"), "export {}\n");
  writeFileSync(join(sourceDir, "QueryEngine.ts"), "export {}\n");
  writeFileSync(join(root, "package.json"), '{ "type": "module" }\n');
  writeFileSync(join(root, "bun.lock"), "\n");

  return {
    vendorPath: root,
    bridgeBundlePath: bundlePath,
    runBridgeBuild: vi.fn(async (input) => {
      writeFileSync(
        input.outputPath,
        `
export async function createDpcodeCcbSession() {
  return {
    sessionId: "ccb-bundled-session",
    async *submitMessage() {
      yield { type: "result", subtype: "success", is_error: false };
    },
    interrupt() {},
    resetAbortController() {},
    getAbortSignal() { return new AbortController().signal; },
    getMessages() { return []; },
    setModel() {}
  };
}
`,
      );
    }),
  };
}

const layer = (options: CcbAdapterLiveOptions) => it.layer(makeCcbAdapterLive(options));

layer(
  makeFakeBridge([
    {
      type: "assistant",
      message: {
        content: [{ type: "text", text: "hello from ccb" }],
      },
    },
    {
      type: "result",
      subtype: "success",
      is_error: false,
    },
  ]),
)("CcbAdapterLive", (it) => {
  it.effect("starts a CCB session and maps assistant/result events", () =>
    Effect.gen(function* () {
      const adapter = yield* CcbAdapter;
      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.take(6),
        Stream.runCollect,
        Effect.forkDetach,
      );
      const threadId = ThreadId.makeUnsafe("thread-ccb-test");

      const session = yield* adapter.startSession({
        threadId,
        provider: "ccb",
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: {
          provider: "ccb",
          model: "claude-sonnet-4-6",
        },
      });
      const turn = yield* adapter.sendTurn({
        threadId,
        input: "say hello",
        modelSelection: {
          provider: "ccb",
          model: "claude-sonnet-4-6",
        },
      });

      const events = Array.from(yield* Fiber.join(eventsFiber));
      assert.equal(session.provider, "ccb");
      assert.equal(turn.threadId, threadId);
      assert.ok(events.some((event) => event.type === "session.started"));
      assert.ok(events.some((event) => event.type === "turn.started"));
      assert.ok(
        events.some(
          (event) =>
            event.type === "content.delta" &&
            event.payload.streamKind === "assistant_text" &&
            event.payload.delta === "hello from ccb",
        ),
      );
      assert.ok(events.some((event) => event.type === "turn.completed"));
    }),
  );
});

layer(makeFakeVendorBridge())("CcbAdapterLive bridge bundling", (it) => {
  it.effect("bundles the CCB source bridge before importing it under Node", () =>
    Effect.gen(function* () {
      const adapter = yield* CcbAdapter;
      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkDetach,
      );
      const threadId = ThreadId.makeUnsafe("thread-ccb-bundled-test");

      const session = yield* adapter.startSession({
        threadId,
        provider: "ccb",
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });

      const events = Array.from(yield* Fiber.join(eventsFiber));
      assert.equal(session.resumeCursor.ccbSessionId, "ccb-bundled-session");
      assert.ok(events.some((event) => event.type === "session.started"));
      assert.ok(events.some((event) => event.type === "session.state.changed"));
    }),
  );
});

layer(
  makeFakeBridge([
    {
      type: "assistant",
      message: {
        content: [
          { type: "thinking", thinking: "planning edit" },
          { type: "tool_use", id: "tool-write-1", name: "Write", input: { file_path: "src/app.ts" } },
        ],
      },
    },
    {
      type: "user",
      message: {
        content: [{ type: "tool_result", tool_use_id: "tool-write-1", content: "ok" }],
      },
    },
    {
      type: "result",
      subtype: "success",
      is_error: false,
    },
  ]),
)("CcbAdapterLive coding task events", (it) => {
  it.effect("maps reasoning, tool use, tool result, and turn completion events", () =>
    Effect.gen(function* () {
      const adapter = yield* CcbAdapter;
      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.take(8),
        Stream.runCollect,
        Effect.forkDetach,
      );
      const threadId = ThreadId.makeUnsafe("thread-ccb-coding-events-test");

      yield* adapter.startSession({
        threadId,
        provider: "ccb",
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({
        threadId,
        input: "edit a file",
      });

      const events = Array.from(yield* Fiber.join(eventsFiber));
      assert.ok(
        events.some(
          (event) =>
            event.type === "content.delta" &&
            event.payload.streamKind === "reasoning_text" &&
            event.payload.delta === "planning edit",
        ),
      );
      assert.ok(
        events.some(
          (event) =>
            event.type === "item.started" &&
            event.payload.itemType === "file_change" &&
            event.payload.title === "Write",
        ),
      );
      assert.ok(
        events.some(
          (event) =>
            event.type === "item.completed" &&
            event.payload.itemType === "file_change" &&
            event.payload.status === "completed",
        ),
      );
      assert.ok(events.some((event) => event.type === "turn.completed"));
    }),
  );
});
