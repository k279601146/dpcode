import { assert, it, vi } from "@effect/vitest";
import { Effect, Fiber, Stream } from "effect";
import { ThreadId } from "@t3tools/contracts";

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
