import { assert, it, vi } from "@effect/vitest";
import { Effect, Fiber, Stream } from "effect";
import { ApprovalRequestId, ThreadId, type ProviderRuntimeEvent } from "@t3tools/contracts";
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
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

function makeControllableFakeBridge(): CcbAdapterLiveOptions & {
  interrupt: ReturnType<typeof vi.fn>;
  setModel: ReturnType<typeof vi.fn>;
} {
  const interrupt = vi.fn();
  const setModel = vi.fn();
  return {
    interrupt,
    setModel,
    bridgeModule: {
      createDpcodeCcbSession: vi.fn(async () => ({
        sessionId: "ccb-session-controllable",
        async *submitMessage() {
          yield { type: "result", subtype: "success", is_error: false };
        },
        interrupt,
        resetAbortController: vi.fn(),
        getAbortSignal: () => new AbortController().signal,
        getMessages: () => [{ type: "assistant", text: "cached transcript" }],
        setModel,
      })),
    },
  };
}

function makeApprovalFakeBridge(): CcbAdapterLiveOptions & {
  requestTool: (
    tool: Record<string, unknown>,
    input: Record<string, unknown>,
  ) => Promise<Record<string, unknown>>;
} {
  let requestTool:
    | ((
        tool: Record<string, unknown>,
        input: Record<string, unknown>,
      ) => Promise<Record<string, unknown>>)
    | undefined;
  return {
    get requestTool() {
      if (!requestTool) {
        throw new Error("CCB approval bridge was not initialized.");
      }
      return requestTool;
    },
    bridgeModule: {
      createDpcodeCcbSession: vi.fn(
        async (input: {
          canUseTool: (
            tool: Record<string, unknown>,
            input: Record<string, unknown>,
          ) => Promise<Record<string, unknown>>;
        }) => {
          requestTool = input.canUseTool;
          return {
            sessionId: "ccb-session-approval",
            async *submitMessage() {
              yield { type: "result", subtype: "success", is_error: false };
            },
            interrupt: vi.fn(),
            resetAbortController: vi.fn(),
            getAbortSignal: () => new AbortController().signal,
            getMessages: () => [],
            setModel: vi.fn(),
          };
        },
      ),
    },
  };
}

function makePermissionToolEventFakeBridge(): CcbAdapterLiveOptions {
  return {
    bridgeModule: {
      createDpcodeCcbSession: vi.fn(
        async (input: {
          canUseTool: (...args: ReadonlyArray<unknown>) => Promise<Record<string, unknown>>;
        }) => ({
          sessionId: "ccb-session-permission-tool-event",
          async *submitMessage() {
            await input.canUseTool(
              { name: "Write" },
              { file_path: "index.html" },
              {},
              {},
              "tool-write-permission-1",
            );
            yield {
              type: "user",
              message: {
                content: [
                  {
                    type: "tool_result",
                    tool_use_id: "tool-write-permission-1",
                    content: "created index.html",
                  },
                ],
              },
            };
            yield { type: "result", subtype: "success", is_error: false };
          },
          interrupt: vi.fn(),
          resetAbortController: vi.fn(),
          getAbortSignal: () => new AbortController().signal,
          getMessages: () => [],
          setModel: vi.fn(),
        }),
      ),
    },
  };
}

function makePermissionOnlyToolEventFakeBridge(): CcbAdapterLiveOptions {
  return {
    bridgeModule: {
      createDpcodeCcbSession: vi.fn(
        async (input: {
          canUseTool: (...args: ReadonlyArray<unknown>) => Promise<Record<string, unknown>>;
        }) => ({
          sessionId: "ccb-session-permission-only-tool-event",
          async *submitMessage() {
            await input.canUseTool(
              { name: "Edit" },
              { file_path: "index.html", old_string: "Hello", new_string: "Hello DPCode" },
              {},
              {},
              "tool-edit-permission-only-1",
            );
            yield { type: "result", subtype: "success", is_error: false };
          },
          interrupt: vi.fn(),
          resetAbortController: vi.fn(),
          getAbortSignal: () => new AbortController().signal,
          getMessages: () => [],
          setModel: vi.fn(),
        }),
      ),
    },
  };
}

function makeCompactFakeBridge(): CcbAdapterLiveOptions & {
  submitPrompts: string[];
} {
  const submitPrompts: string[] = [];
  return {
    submitPrompts,
    bridgeModule: {
      createDpcodeCcbSession: vi.fn(async () => ({
        sessionId: "ccb-session-compact",
        async *submitMessage(prompt: string | ReadonlyArray<unknown>) {
          if (typeof prompt !== "string") {
            throw new Error("Expected compact prompt to be a string.");
          }
          submitPrompts.push(prompt);
          if (prompt === "/compact") {
            yield {
              type: "system",
              subtype: "compact_boundary",
              uuid: "compact-boundary-1",
              compact_metadata: { trigger: "manual" },
            };
          }
          yield { type: "result", subtype: "success", is_error: false };
        },
        interrupt: vi.fn(),
        resetAbortController: vi.fn(),
        getAbortSignal: () => new AbortController().signal,
        getMessages: () => [{ type: "system", subtype: "compact_boundary" }],
        setModel: vi.fn(),
      })),
    },
  };
}

function makeMultiTurnFakeBridge(): CcbAdapterLiveOptions & {
  createSession: ReturnType<typeof vi.fn>;
  submitPrompts: Array<string | ReadonlyArray<unknown>>;
} {
  const submitPrompts: Array<string | ReadonlyArray<unknown>> = [];
  const createSession = vi.fn(async () => ({
    sessionId: "ccb-session-multi-turn",
    async *submitMessage(prompt: string | ReadonlyArray<unknown>) {
      submitPrompts.push(prompt);
      yield { type: "result", subtype: "success", is_error: false };
    },
    interrupt: vi.fn(),
    resetAbortController: vi.fn(),
    getAbortSignal: () => new AbortController().signal,
    getMessages: () => [{ type: "assistant", content: "multi-turn transcript" }],
    setModel: vi.fn(),
  }));

  return {
    createSession,
    submitPrompts,
    bridgeModule: {
      createDpcodeCcbSession: createSession,
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

function makeTestAttachmentsDir(): string {
  const dir = join(mkdtempSync(join(tmpdir(), "dpcode-ccb-attachments-")), "attachments");
  mkdirSync(dir, { recursive: true });
  return dir;
}

const layer = (options: CcbAdapterLiveOptions) => {
  (options as { attachmentsDir?: string }).attachmentsDir ??= makeTestAttachmentsDir();
  return it.layer(makeCcbAdapterLive(options));
};

function waitForTranscript(path: string): Promise<void> {
  const deadline = Date.now() + 2_000;
  const poll = (resolve: () => void, reject: (error: Error) => void): void => {
    try {
      const parsed = JSON.parse(readFileSync(path, "utf8"));
      if (Array.isArray(parsed)) {
        resolve();
        return;
      }
    } catch {
      // Keep polling until the adapter has flushed the transcript.
    }
    if (Date.now() >= deadline) {
      reject(new Error(`Timed out waiting for CCB transcript: ${path}`));
      return;
    }
    setTimeout(() => poll(resolve, reject), 10);
  };
  return new Promise(poll);
}

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
        Stream.filter(
          (event) =>
            event.type === "session.started" ||
            event.type === "turn.started" ||
            event.type === "content.delta" ||
            event.type === "turn.completed",
        ),
        Stream.take(4),
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

const inputBridge = (() => {
  const submitInputs: Array<unknown> = [];
  return {
    submitInputs,
    attachmentsDir: makeTestAttachmentsDir(),
    bridgeModule: {
      createDpcodeCcbSession: vi.fn(async () => ({
        sessionId: "ccb-session-input",
        async *submitMessage(prompt: unknown) {
          submitInputs.push(prompt);
          yield { type: "result", subtype: "success", is_error: false };
        },
        interrupt: vi.fn(),
        resetAbortController: vi.fn(),
        getAbortSignal: () => new AbortController().signal,
        getMessages: () => [],
        setModel: vi.fn(),
      })),
    },
  } satisfies CcbAdapterLiveOptions & {
    submitInputs: Array<unknown>;
    attachmentsDir: string;
  };
})();

layer(inputBridge)("CcbAdapterLive turn input", (it) => {
  it.effect("builds CCB content blocks for text, images, skills, and mentions", () =>
    Effect.gen(function* () {
      const adapter = yield* CcbAdapter;
      const threadId = ThreadId.makeUnsafe("thread-ccb-input-blocks-test");
      const attachmentId = "ccb-image-input";
      writeFileSync(join(inputBridge.attachmentsDir, `${attachmentId}.png`), Buffer.from("png"));

      const completedFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.type === "turn.completed" && event.threadId === threadId),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkDetach,
      );

      yield* adapter.startSession({
        threadId,
        provider: "ccb",
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({
        threadId,
        input: "Use @plan(outline the work)",
        attachments: [
          {
            type: "image",
            id: attachmentId,
            name: "input.png",
            mimeType: "image/png",
            sizeBytes: 3,
          },
        ],
        skills: [{ name: "verify", path: "ccb://bundled/verify" }],
        mentions: [{ name: "README.md", path: "README.md" }],
      });
      yield* Fiber.join(completedFiber);

      const input = inputBridge.submitInputs[0];
      assert.ok(Array.isArray(input));
      const blocks = input as Array<Record<string, unknown>>;
      assert.equal(blocks[0]?.type, "text");
      assert.match(String(blocks[0]?.text), /Use the "plan" agent/);
      assert.match(String(blocks[0]?.text), /verify \(ccb:\/\/bundled\/verify\)/);
      assert.match(String(blocks[0]?.text), /README\.md \(README\.md\)/);
      const image = blocks.find((block) => block.type === "image") as
        | { source?: { media_type?: string; data?: string } }
        | undefined;
      assert.equal(image?.source?.media_type, "image/png");
      assert.equal(image?.source?.data, Buffer.from("png").toString("base64"));
    }),
  );
});

layer({
  attachmentsDir: makeTestAttachmentsDir(),
  bridgeModule: {
    createDpcodeCcbSession: vi.fn(async () => ({
      sessionId: "ccb-session-invalid-attachment",
      async *submitMessage() {
        yield { type: "result", subtype: "success", is_error: false };
      },
      interrupt: vi.fn(),
      resetAbortController: vi.fn(),
      getAbortSignal: () => new AbortController().signal,
      getMessages: () => [],
      setModel: vi.fn(),
    })),
  },
})("CcbAdapterLive attachment validation", (it) => {
  it.effect("fails clearly for missing image attachment files", () =>
    Effect.gen(function* () {
      const adapter = yield* CcbAdapter;
      const threadId = ThreadId.makeUnsafe("thread-ccb-invalid-attachment-test");

      yield* adapter.startSession({
        threadId,
        provider: "ccb",
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });

      let detail = "";
      yield* adapter
        .sendTurn({
          threadId,
          input: "inspect this image",
          attachments: [
            {
              type: "image",
              id: "missing-image",
              name: "missing.png",
              mimeType: "image/png",
              sizeBytes: 1,
            },
          ],
        })
        .pipe(
          Effect.catch((error) =>
            Effect.sync(() => {
              detail = error.detail;
            }),
          ),
        );

      assert.match(detail, /Invalid attachment id 'missing-image'/);
    }),
  );
});

const permissionModeBridge = (() => {
  const modes: string[] = [];
  return {
    modes,
    bridgeModule: {
      createDpcodeCcbSession: vi.fn(async () => ({
        sessionId: "ccb-session-permission-mode",
        async *submitMessage() {
          yield { type: "result", subtype: "success", is_error: false };
        },
        interrupt: vi.fn(),
        resetAbortController: vi.fn(),
        getAbortSignal: () => new AbortController().signal,
        getMessages: () => [],
        setModel: vi.fn(),
        setPermissionMode: vi.fn((mode: string) => {
          modes.push(mode);
        }),
      })),
    },
  } satisfies CcbAdapterLiveOptions & { modes: string[] };
})();

layer(permissionModeBridge)("CcbAdapterLive permission mode switching", (it) => {
  it.effect("maps plan/default interaction modes onto the CCB session permission mode", () =>
    Effect.gen(function* () {
      const adapter = yield* CcbAdapter;
      const threadId = ThreadId.makeUnsafe("thread-ccb-permission-mode-test");

      yield* adapter.startSession({
        threadId,
        provider: "ccb",
        cwd: process.cwd(),
        runtimeMode: "full-access",
        providerOptions: { ccb: { permissionMode: "acceptEdits" } },
      });
      const firstCompleted = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.type === "turn.completed" && event.threadId === threadId),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkDetach,
      );
      yield* adapter.sendTurn({
        threadId,
        input: "plan it",
        interactionMode: "plan",
      });
      yield* Fiber.join(firstCompleted);
      const secondCompleted = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.type === "turn.completed" && event.threadId === threadId),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkDetach,
      );
      yield* adapter.sendTurn({
        threadId,
        input: "implement it",
        interactionMode: "default",
      });
      yield* Fiber.join(secondCompleted);

      assert.deepEqual(permissionModeBridge.modes, ["plan", "acceptEdits"]);
    }),
  );
});

layer(
  makeFakeBridge([
    {
      type: "stream_event",
      event: {
        type: "content_block_delta",
        index: 0,
        delta: { type: "text_delta", text: "我先创建项目。" },
      },
    },
    {
      type: "assistant",
      message: {
        content: [
          {
            type: "tool_use",
            id: "tool-command-1",
            name: "PowerShell",
            input: { command: "npx create-next-app nextjs-project --yes" },
          },
        ],
      },
    },
    {
      type: "user",
      message: {
        content: [
          {
            type: "tool_result",
            tool_use_id: "tool-command-1",
            content: "created nextjs-project",
          },
        ],
      },
    },
    {
      type: "stream_event",
      event: {
        type: "content_block_delta",
        index: 1,
        delta: { type: "text_delta", text: "项目已经创建完成。" },
      },
    },
    { type: "result", subtype: "success", is_error: false },
  ]),
)("CcbAdapterLive app-style streaming", (it) => {
  it.effect("splits assistant text around tools and asks checkpointing to refresh changed files", () =>
    Effect.gen(function* () {
      const adapter = yield* CcbAdapter;
      const threadId = ThreadId.makeUnsafe("thread-ccb-app-style-streaming-test");
      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter(
          (event) =>
            (event.type === "content.delta" &&
              event.payload.streamKind === "assistant_text") ||
            (event.type === "item.completed" &&
              event.payload.itemType === "command_execution") ||
            event.type === "turn.diff.updated" ||
            event.type === "turn.completed",
        ),
        Stream.take(5),
        Stream.runCollect,
        Effect.forkDetach,
      );

      yield* adapter.startSession({
        threadId,
        provider: "ccb",
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({
        threadId,
        input: "开发一个简单的Next.js项目",
      });

      const events = Array.from(yield* Fiber.join(eventsFiber));
      const textEvents = events.filter(
        (event): event is Extract<ProviderRuntimeEvent, { type: "content.delta" }> =>
          event.type === "content.delta" && event.payload.streamKind === "assistant_text",
      );
      assert.equal(textEvents.length, 2);
      assert.notEqual(String(textEvents[0]?.itemId), String(textEvents[1]?.itemId));
      assert.ok(events.some((event) => event.type === "turn.diff.updated"));
      assert.ok(events.some((event) => event.type === "turn.completed"));
    }),
  );
});

layer(
  makeFakeBridge([
    {
      type: "stream_event",
      event: {
        type: "content_block_start",
        index: 1,
        content_block: {
          type: "mcp_tool_use",
          id: "tool-mcp-1",
          name: "lookup",
          input: { query: "issue status" },
        },
      },
    },
    {
      type: "stream_event",
      event: {
        type: "content_block_start",
        index: 2,
        content_block: {
          type: "server_tool_use",
          id: "tool-web-1",
          name: "web_search",
          input: { query: "DPCode CCB events" },
        },
      },
    },
    {
      type: "assistant",
      message: {
        content: [
          {
            type: "tool_use",
            id: "tool-read-1",
            name: "ReadFile",
            input: { file_path: "src/app.ts" },
          },
          {
            type: "tool_use",
            id: "tool-grep-1",
            name: "Grep",
            input: { pattern: "ccb", path: "apps/server/src" },
          },
          {
            type: "tool_use",
            id: "tool-glob-1",
            name: "Glob",
            input: { pattern: "**/*.ts", path: "apps/server/src" },
          },
          {
            type: "tool_use",
            id: "tool-edit-1",
            name: "Edit",
            input: { file_path: "src/app.ts", old_string: "old", new_string: "new" },
          },
        ],
      },
    },
    {
      type: "result",
      subtype: "success",
      is_error: false,
    },
  ]),
)("CcbAdapterLive tool display classification", (it) => {
  it.effect("classifies CCB mcp, web, read-only, and file-edit tool rows for existing UI types", () =>
    Effect.gen(function* () {
      const adapter = yield* CcbAdapter;
      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter(
          (event) =>
            event.type === "item.started" ||
            event.type === "item.completed" ||
            event.type === "turn.completed",
        ),
        Stream.take(13),
        Stream.runCollect,
        Effect.forkDetach,
      );
      const threadId = ThreadId.makeUnsafe("thread-ccb-tool-display-classification-test");

      yield* adapter.startSession({
        threadId,
        provider: "ccb",
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({
        threadId,
        input: "exercise tool display classification",
      });

      const events = Array.from(yield* Fiber.join(eventsFiber));
      const started = events.filter((event) => event.type === "item.started");
      assert.ok(
        started.some(
          (event) =>
            event.type === "item.started" &&
            event.payload.itemType === "mcp_tool_call" &&
            event.payload.title === "MCP tool call",
        ),
      );
      assert.ok(
        started.some(
          (event) =>
            event.type === "item.started" &&
            event.payload.itemType === "web_search" &&
            event.payload.title === "Web search",
        ),
      );
      assert.ok(
        started.some(
          (event) =>
            event.type === "item.started" &&
            event.payload.itemType === "dynamic_tool_call" &&
            event.payload.title === "Read file" &&
            (event.payload.data as { filePath?: string }).filePath === "src/app.ts",
        ),
      );
      assert.ok(
        started.some(
          (event) =>
            event.type === "item.started" &&
            event.payload.itemType === "dynamic_tool_call" &&
            event.payload.title === "Search files" &&
            (event.payload.data as { filePath?: string }).filePath === "apps/server/src",
        ),
      );
      assert.ok(
        started.some(
          (event) =>
            event.type === "item.started" &&
            event.payload.itemType === "file_change" &&
            event.payload.title === "File change" &&
            (event.payload.data as { files?: string[]; input?: unknown }).files?.includes(
              "src/app.ts",
            ) &&
            (event.payload.data as { input?: { oldStringLength?: number; newStringLength?: number } })
              .input?.oldStringLength === 3 &&
            (event.payload.data as { input?: { oldStringLength?: number; newStringLength?: number } })
              .input?.newStringLength === 3 &&
            JSON.stringify(event.payload.data).includes("old_string") === false &&
            JSON.stringify(event.payload.data).includes("new_string") === false,
        ),
      );
      assert.ok(
        events.some(
          (event) =>
            event.type === "item.completed" &&
            event.payload.itemType === "file_change" &&
            event.payload.status === "failed",
        ),
      );
      assert.ok(events.some((event) => event.type === "turn.completed"));
    }),
  );
});

layer(
  makeFakeBridge([
    {
      type: "stream_event",
      event: {
        type: "content_block_delta",
        delta: { type: "text_delta", text: "You are 27." },
      },
    },
    {
      type: "assistant",
      message: {
        content: [{ type: "text", text: "You are 27." }],
      },
    },
    {
      type: "result",
      subtype: "success",
      is_error: false,
    },
  ]),
)("CcbAdapterLive assistant streaming", (it) => {
  it.effect("does not repeat the final assistant message after streamed deltas", () =>
    Effect.gen(function* () {
      const adapter = yield* CcbAdapter;
      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.type === "content.delta" || event.type === "turn.completed"),
        Stream.take(2),
        Stream.runCollect,
        Effect.forkDetach,
      );
      const threadId = ThreadId.makeUnsafe("thread-ccb-stream-dedup-test");

      yield* adapter.startSession({
        threadId,
        provider: "ccb",
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({
        threadId,
        input: "say my age",
      });

      const events = Array.from(yield* Fiber.join(eventsFiber));
      const assistantDeltas = events.filter(
        (event) => event.type === "content.delta" && event.payload.streamKind === "assistant_text",
      );
      assert.equal(assistantDeltas.length, 1);
      assert.equal(assistantDeltas[0]?.payload.delta, "You are 27.");
      assert.ok(events.some((event) => event.type === "turn.completed"));
    }),
  );
});

layer(
  makeFakeBridge([
    {
      type: "stream_event",
      event: {
        type: "content_block_start",
        index: 1,
        content_block: {
          type: "tool_use",
          id: "tool-write-1",
          name: "Write",
          input: {},
        },
      },
    },
    {
      type: "stream_event",
      event: {
        type: "content_block_delta",
        index: 1,
        delta: {
          type: "input_json_delta",
          partial_json:
            '{"file_path":"index.html","content":"<!DOCTYPE html><html><body>hi</body></html>"}',
        },
      },
    },
    {
      type: "stream_event",
      event: {
        type: "content_block_stop",
        index: 1,
      },
    },
    {
      type: "assistant",
      message: {
        content: [
          { type: "text", text: "I wrote index.html." },
          {
            type: "tool_use",
            id: "tool-write-1",
            name: "Write",
            input: {
              file_path: "index.html",
              content: "<!DOCTYPE html><html><body>hi</body></html>",
            },
          },
        ],
      },
    },
    {
      type: "user",
      message: {
        content: [
          {
            type: "tool_result",
            tool_use_id: "tool-write-1",
            content:
              '{"file_path":"index.html","content":"<!DOCTYPE html><html><body>hi</body></html>"}',
          },
        ],
      },
    },
    {
      type: "result",
      subtype: "success",
      is_error: false,
    },
  ]),
)("CcbAdapterLive streamed tool blocks", (it) => {
  it.effect("keeps streamed tool JSON out of assistant text and emits tool lifecycle data", () =>
    Effect.gen(function* () {
      const adapter = yield* CcbAdapter;
      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter(
          (event) =>
            event.type === "content.delta" ||
            event.type === "item.started" ||
            event.type === "item.updated" ||
            event.type === "item.completed" ||
            event.type === "turn.completed",
        ),
        Stream.take(7),
        Stream.runCollect,
        Effect.forkDetach,
      );
      const threadId = ThreadId.makeUnsafe("thread-ccb-streamed-tool-json-test");

      yield* adapter.startSession({
        threadId,
        provider: "ccb",
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({
        threadId,
        input: "write a file",
      });

      const events = Array.from(yield* Fiber.join(eventsFiber));
      const assistantDeltas = events.filter(
        (event) => event.type === "content.delta" && event.payload.streamKind === "assistant_text",
      );
      assert.deepEqual(
        assistantDeltas.map((event) => (event.type === "content.delta" ? event.payload.delta : "")),
        ["I wrote index.html."],
      );
      assert.equal(
        assistantDeltas.some(
          (event) =>
            event.type === "content.delta" &&
            (event.payload.delta.includes("file_path") ||
              event.payload.delta.includes("<!DOCTYPE html>")),
        ),
        false,
      );
      assert.equal(events.filter((event) => event.type === "item.started").length, 1);
      assert.ok(
        events.some(
          (event) =>
            event.type === "item.updated" &&
            event.payload.itemType === "file_change" &&
            (event.payload.data as { files?: string[] }).files?.[0] === "index.html" &&
            JSON.stringify(event.payload.data).includes("<!DOCTYPE html>") === false,
        ),
      );
      assert.ok(
        events.some(
          (event) =>
            event.type === "item.completed" &&
            event.payload.itemType === "file_change" &&
            (event.payload.data as { toolName?: string; operation?: string }).toolName === "Write" &&
            (event.payload.data as { operation?: string }).operation === "write" &&
            JSON.stringify(event.payload.data).includes("<!DOCTYPE html>") === false,
        ),
      );
      assert.equal(
        events.some(
          (event) => event.type === "content.delta" && event.payload.streamKind === "file_change_output",
        ),
        false,
      );
      assert.ok(events.some((event) => event.type === "turn.completed"));
    }),
  );
});

layer(
  makeFakeBridge([
    {
      type: "assistant",
      message: {
        content: [
          {
            type: "tool_use",
            id: "tool-todo-1",
            name: "TodoWrite",
            input: {
              todos: [
                { content: "Inspect CCB events", status: "completed" },
                {
                  content: "Normalize message rendering",
                  activeForm: "Normalizing message rendering",
                  status: "in_progress",
                },
              ],
            },
          },
          {
            type: "tool_use",
            id: "tool-plan-1",
            name: "ExitPlanMode",
            input: { plan: "1. Inspect events\n2. Patch adapter\n3. Verify" },
          },
        ],
        usage: {
          input_tokens: 10,
          output_tokens: 5,
          cache_read_input_tokens: 2,
        },
      },
    },
    { type: "system", subtype: "init", model: "claude-sonnet-4-6" },
    {
      type: "system",
      subtype: "task_started",
      task_id: "task-ccb-1",
      description: "Run subtask",
      task_type: "agent",
    },
    {
      type: "system",
      subtype: "task_progress",
      task_id: "task-ccb-1",
      description: "Subtask is working",
      summary: "Inspecting",
      usage: { input_tokens: 20, output_tokens: 10 },
      last_tool_name: "Read",
    },
    {
      type: "system",
      subtype: "task_notification",
      task_id: "task-ccb-1",
      status: "completed",
      summary: "Subtask done",
      usage: { input_tokens: 25, output_tokens: 15 },
    },
    {
      type: "tool_progress",
      tool_use_id: "tool-bash-1",
      tool_name: "Bash",
      elapsed_time_seconds: 1.5,
    },
    {
      type: "tool_use_summary",
      summary: "Read files and updated adapter",
      preceding_tool_use_ids: ["tool-todo-1"],
    },
    {
      type: "auth_status",
      isAuthenticating: false,
      output: ["ready"],
    },
    {
      type: "rate_limit_event",
      rate_limit_info: {
        status: "allowed_warning",
        utilization: 0.82,
        resetsAt: 1_800_000_000,
      },
    },
    {
      type: "result",
      subtype: "success",
      is_error: false,
      usage: { input_tokens: 30, output_tokens: 12 },
    },
  ]),
)("CcbAdapterLive canonical message rendering", (it) => {
  it.effect("normalizes CCB plan, task, telemetry, auth, rate limit, and usage messages", () =>
    Effect.gen(function* () {
      const adapter = yield* CcbAdapter;
      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter(
          (event) =>
            event.type === "item.started" ||
            event.type === "turn.tasks.updated" ||
            event.type === "turn.proposed.completed" ||
            event.type === "session.configured" ||
            event.type === "task.started" ||
            event.type === "task.progress" ||
            event.type === "task.completed" ||
            event.type === "tool.progress" ||
            event.type === "tool.summary" ||
            event.type === "auth.status" ||
            event.type === "account.rate-limits.updated" ||
            event.type === "thread.token-usage.updated" ||
            event.type === "turn.completed",
        ),
        Stream.take(17),
        Stream.runCollect,
        Effect.forkDetach,
      );
      const threadId = ThreadId.makeUnsafe("thread-ccb-canonical-rendering-test");

      yield* adapter.startSession({
        threadId,
        provider: "ccb",
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({
        threadId,
        input: "exercise ccb event rendering",
      });

      const events = Array.from(yield* Fiber.join(eventsFiber));
      assert.ok(
        events.some(
          (event) =>
            event.type === "turn.tasks.updated" &&
            event.payload.tasks[1]?.task === "Normalizing message rendering" &&
            event.payload.tasks[1]?.status === "inProgress",
        ),
      );
      assert.ok(
        events.some(
          (event) =>
            event.type === "turn.proposed.completed" &&
            event.payload.planMarkdown.includes("Patch adapter"),
        ),
      );
      assert.ok(events.some((event) => event.type === "session.configured"));
      assert.ok(events.some((event) => event.type === "task.started"));
      assert.ok(
        events.some(
          (event) =>
            event.type === "task.progress" &&
            event.payload.description === "Subtask is working" &&
            event.payload.lastToolName === "Read",
        ),
      );
      assert.ok(
        events.some(
          (event) =>
            event.type === "task.completed" &&
            event.payload.status === "completed" &&
            event.payload.summary === "Subtask done",
        ),
      );
      assert.ok(events.some((event) => event.type === "tool.progress"));
      assert.ok(events.some((event) => event.type === "tool.summary"));
      assert.ok(events.some((event) => event.type === "auth.status"));
      assert.ok(
        events.some(
          (event) =>
            event.type === "account.rate-limits.updated" &&
            (event.payload.rateLimits as { status?: string }).status === "allowed_warning",
        ),
      );
      assert.ok(
        events.some(
          (event) =>
            event.type === "thread.token-usage.updated" &&
            event.payload.usage.usedTokens === 42,
        ),
      );
      assert.ok(events.some((event) => event.type === "turn.completed"));
    }),
  );
});

layer(
  makeFakeBridge([
    {
      type: "system",
      subtype: "session_state_changed",
      state: "requires_action",
    },
    {
      type: "system",
      subtype: "api_retry",
      attempt: 2,
      max_retries: 5,
      retry_delay_ms: 250,
      error: "rate_limit",
    },
    {
      type: "system",
      subtype: "local_command_output",
      content: "local command output\n",
    },
    {
      type: "system",
      subtype: "post_turn_summary",
      uuid: "post-turn-summary-1",
      summarizes_uuid: "assistant-1",
      status_category: "review_ready",
      status_detail: "Ready for review",
      title: "Review ready",
      description: "Updated the adapter",
      recent_action: "Mapped CCB events",
      needs_action: "Review changes",
      artifact_urls: ["https://example.test/artifact"],
    },
    {
      type: "system",
      subtype: "elicitation_complete",
      mcp_server_name: "github",
      elicitation_id: "elicitation-1",
    },
    {
      type: "streamlined_text",
      uuid: "streamlined-1",
      text: "Streamlined answer.",
    },
    {
      type: "streamlined_tool_use_summary",
      tool_summary: "Read 2 files and edited 1 file",
    },
    {
      type: "prompt_suggestion",
      suggestion: "Run focused verification",
    },
    {
      type: "result",
      subtype: "success",
      is_error: false,
    },
  ]),
)("CcbAdapterLive additional SDK display messages", (it) => {
  it.effect("maps CCB session state, retry, local output, summaries, elicitation, and streamlined messages", () =>
    Effect.gen(function* () {
      const adapter = yield* CcbAdapter;
      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter(
          (event) =>
            event.type === "session.state.changed" ||
            event.type === "runtime.warning" ||
            event.type === "content.delta" ||
            event.type === "item.updated" ||
            event.type === "user-input.resolved" ||
            event.type === "item.completed" ||
            event.type === "tool.summary" ||
            event.type === "thread.metadata.updated" ||
            event.type === "turn.completed",
        ),
        Stream.take(12),
        Stream.runCollect,
        Effect.forkDetach,
      );
      const threadId = ThreadId.makeUnsafe("thread-ccb-additional-sdk-display-test");

      yield* adapter.startSession({
        threadId,
        provider: "ccb",
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({
        threadId,
        input: "exercise additional sdk messages",
      });

      const events = Array.from(yield* Fiber.join(eventsFiber));
      assert.ok(
        events.some(
          (event) =>
            event.type === "session.state.changed" &&
            event.payload.state === "waiting" &&
            event.payload.reason === "session_state:requires_action",
        ),
      );
      assert.ok(
        events.some(
          (event) =>
            event.type === "runtime.warning" &&
            event.payload.message.includes("CCB API request retry"),
        ),
      );
      assert.ok(
        events.some(
          (event) =>
            event.type === "content.delta" &&
            event.payload.streamKind === "assistant_text" &&
            event.payload.delta.includes("local command output"),
        ),
      );
      assert.ok(
        events.some(
          (event) =>
            event.type === "item.updated" &&
            event.payload.itemType === "dynamic_tool_call" &&
            event.payload.title === "Review ready" &&
            (event.payload.data as { needsAction?: string }).needsAction === "Review changes",
        ),
      );
      assert.ok(
        events.some(
          (event) =>
            event.type === "user-input.resolved" &&
            (event.payload.answers as { elicitationId?: string }).elicitationId === "elicitation-1",
        ),
      );
      assert.ok(
        events.some(
          (event) =>
            event.type === "item.completed" &&
            event.payload.itemType === "assistant_message" &&
            event.payload.detail === "Streamlined answer.",
        ),
      );
      assert.ok(
        events.some(
          (event) =>
            event.type === "tool.summary" &&
            event.payload.summary === "Read 2 files and edited 1 file",
        ),
      );
      assert.ok(
        events.some(
          (event) =>
            event.type === "thread.metadata.updated" &&
            (event.payload.metadata as { promptSuggestion?: string }).promptSuggestion ===
              "Run focused verification",
        ),
      );
      assert.ok(events.some((event) => event.type === "turn.completed"));
    }),
  );
});

const lifecycleBridge = makeControllableFakeBridge();
layer(lifecycleBridge)("CcbAdapterLive lifecycle", (it) => {
  it.effect("lists, reads, compacts, interrupts, and stops sessions", () =>
    Effect.gen(function* () {
      const adapter = yield* CcbAdapter;
      const threadId = ThreadId.makeUnsafe("thread-ccb-lifecycle-test");

      const session = yield* adapter.startSession({
        threadId,
        provider: "ccb",
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const completedFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.type === "turn.completed"),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkDetach,
      );

      assert.equal(yield* adapter.hasSession(threadId), true);
      assert.equal((yield* adapter.listSessions()).length, 1);

      const turn = yield* adapter.sendTurn({
        threadId,
        input: "run lifecycle",
        modelSelection: {
          provider: "ccb",
          model: "claude-opus-4-7",
        },
      });
      assert.equal(turn.threadId, threadId);
      assert.equal(lifecycleBridge.setModel.mock.calls[0]?.[0], "claude-opus-4-7");
      yield* Fiber.join(completedFiber);

      const transcript = yield* adapter.readThread(threadId);
      assert.equal(transcript.threadId, threadId);
      assert.ok(transcript.turns.length > 0);

      yield* adapter.compactThread!(threadId);
      yield* adapter.interruptTurn(threadId);
      assert.ok(lifecycleBridge.interrupt.mock.calls.length > 0);

      yield* adapter.stopSession(threadId);
      assert.equal(yield* adapter.hasSession(threadId), false);
      assert.equal(session.resumeCursor.ccbSessionId, "ccb-session-controllable");
    }),
  );
});

const multiTurnBridge = makeMultiTurnFakeBridge();
layer(multiTurnBridge)("CcbAdapterLive multi-turn", (it) => {
  it.effect("reuses the same CCB session for multiple turns in one thread", () =>
    Effect.gen(function* () {
      const adapter = yield* CcbAdapter;
      const threadId = ThreadId.makeUnsafe("thread-ccb-multi-turn-test");

      yield* adapter.startSession({
        threadId,
        provider: "ccb",
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const firstCompletedFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.type === "turn.completed"),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkDetach,
      );
      yield* adapter.sendTurn({
        threadId,
        input: "first message",
      });
      yield* Fiber.join(firstCompletedFiber);

      const secondCompletedFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.type === "turn.completed"),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkDetach,
      );
      yield* adapter.sendTurn({
        threadId,
        input: "second message",
      });
      yield* Fiber.join(secondCompletedFiber);

      assert.equal(multiTurnBridge.createSession.mock.calls.length, 1);
      const promptText = (prompt: string | ReadonlyArray<unknown> | undefined) =>
        typeof prompt === "string" ? prompt : JSON.stringify(prompt);
      assert.match(promptText(multiTurnBridge.submitPrompts[0]), /first message/);
      assert.match(promptText(multiTurnBridge.submitPrompts[1]), /second message/);
      assert.equal((yield* adapter.listSessions())[0]?.provider, "ccb");
    }),
  );
});

const approvalBridge = makeApprovalFakeBridge();
layer(approvalBridge)("CcbAdapterLive approvals", (it) => {
  it.effect("opens and resolves approval requests through canUseTool", () =>
    Effect.gen(function* () {
      const adapter = yield* CcbAdapter;
      const threadId = ThreadId.makeUnsafe("thread-ccb-approval-test");
      const session = yield* adapter.startSession({
        threadId,
        provider: "ccb",
        cwd: process.cwd(),
        runtimeMode: "approval-required",
      });

      const decisionFiber = yield* Effect.promise(() =>
        approvalBridge.requestTool({ name: "Bash" }, { command: "pwd" }),
      ).pipe(Effect.forkDetach);

      const openedEventFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.type === "request.opened"),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkDetach,
      );

      const openedEvents = Array.from(yield* Fiber.join(openedEventFiber));
      const opened = openedEvents.find(
        (event): event is Extract<ProviderRuntimeEvent, { type: "request.opened" }> =>
          event.type === "request.opened",
      );
      assert.ok(opened);
      assert.equal(opened.type, "request.opened");
      assert.equal(opened.threadId, threadId);
      assert.equal(opened.payload.detail, "Bash");
      assert.equal(opened.payload.requestType, "command_execution_approval");

      const resolvedEventFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.type === "request.resolved"),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkDetach,
      );

      yield* adapter.respondToRequest(
        threadId,
        ApprovalRequestId.makeUnsafe(opened.requestId),
        "accept",
      );

      const result = yield* Fiber.join(decisionFiber);
      const requestEvents = Array.from(yield* Fiber.join(resolvedEventFiber));
      const resolved = requestEvents.find(
        (event): event is Extract<ProviderRuntimeEvent, { type: "request.resolved" }> =>
          event.type === "request.resolved",
      );
      assert.equal(resolved?.type, "request.resolved");
      assert.equal(resolved?.payload.requestType, "command_execution_approval");
      assert.equal(result.behavior, "allow");
      assert.equal(session.resumeCursor.ccbSessionId, "ccb-session-approval");
    }),
  );
});

const compactBridge = makeCompactFakeBridge();
layer(compactBridge)("CcbAdapterLive compact", (it) => {
  it.effect("runs CCB native /compact and maps the compact boundary", () =>
    Effect.gen(function* () {
      const adapter = yield* CcbAdapter;
      const threadId = ThreadId.makeUnsafe("thread-ccb-compact-test");
      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter(
          (event) =>
            event.type === "turn.started" ||
            event.type === "item.completed" ||
            event.type === "turn.completed",
        ),
        Stream.take(3),
        Stream.runCollect,
        Effect.forkDetach,
      );

      yield* adapter.startSession({
        threadId,
        provider: "ccb",
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.compactThread!(threadId);

      const events = Array.from(yield* Fiber.join(eventsFiber));
      assert.deepEqual(compactBridge.submitPrompts, ["/compact"]);
      assert.ok(events.some((event) => event.type === "turn.started"));
      assert.ok(
        events.some(
          (event) =>
            event.type === "item.completed" &&
            event.payload.itemType === "context_compaction" &&
            event.payload.status === "completed",
        ),
      );
      assert.ok(events.some((event) => event.type === "turn.completed"));
    }),
  );
});

layer({
  bridgeModule: {
    createDpcodeCcbSession: vi.fn(async () => ({
      sessionId: "ccb-session-discovery",
      async *submitMessage() {
        yield { type: "result", subtype: "success", is_error: false };
      },
      interrupt: vi.fn(),
      resetAbortController: vi.fn(),
      getAbortSignal: () => new AbortController().signal,
      getMessages: () => [],
      setModel: vi.fn(),
    })),
    listDpcodeCcbCommands: vi.fn(async () => [
      { name: "compact", description: "Compact context" },
      { name: "review" },
    ]),
    listDpcodeCcbSkills: vi.fn(async () => [
      {
        name: "verify",
        description: "Run focused verification",
        path: "ccb://bundled/verify",
        enabled: true,
        scope: "bundled",
        displayName: "Verify",
        shortDescription: "Run focused verification",
      },
    ]),
    listDpcodeCcbAgents: vi.fn(async () => [
      {
        name: "review",
        displayName: "Review",
        description: "Review changes",
        model: "sonnet",
      },
    ]),
    listDpcodeCcbMcpStatus: vi.fn(async () => ({
      servers: [{ name: "filesystem", transport: "stdio", scope: "project", enabled: true }],
      errors: [],
    })),
  },
})("CcbAdapterLive discovery", (it) => {
  it.effect("lists CCB native commands and skills from the bridge", () =>
    Effect.gen(function* () {
      const adapter = yield* CcbAdapter;

      const commands = yield* adapter.listCommands!({
        provider: "ccb",
        cwd: process.cwd(),
      });
      const skills = yield* adapter.listSkills!({
        provider: "ccb",
        cwd: process.cwd(),
      });
      const agents = yield* adapter.listAgents!();
      const capabilities = yield* adapter.getComposerCapabilities!();

      assert.deepEqual(
        commands.commands.map((command) => command.name),
        ["compact", "review"],
      );
      assert.equal(skills.skills[0]?.name, "verify");
      assert.equal(skills.skills[0]?.path, "ccb://bundled/verify");
      assert.equal(skills.skills[0]?.interface?.displayName, "Verify");
      assert.equal(agents.agents[0]?.name, "review");
      assert.equal(agents.agents[0]?.displayName, "Review");
      assert.equal(agents.agents[0]?.model, "sonnet");
      assert.equal(capabilities.supportsSkillDiscovery, true);
    }),
  );

  it.effect("emits CCB MCP status without blocking session startup", () =>
    Effect.gen(function* () {
      const adapter = yield* CcbAdapter;
      const threadId = ThreadId.makeUnsafe("thread-ccb-mcp-status-test");
      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.type === "session.started" || event.type === "mcp.status.updated"),
        Stream.take(2),
        Stream.runCollect,
        Effect.forkDetach,
      );

      yield* adapter.startSession({
        threadId,
        provider: "ccb",
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });

      const events = Array.from(yield* Fiber.join(eventsFiber));
      assert.ok(events.some((event) => event.type === "session.started"));
      assert.ok(
        events.some(
          (event) =>
            event.type === "mcp.status.updated" &&
            (event.payload.status as { servers?: Array<{ name?: string }> }).servers?.[0]?.name ===
              "filesystem",
        ),
      );
    }),
  );
});

layer({
  bridgeModule: {
    createDpcodeCcbSession: vi.fn(async () => ({
      sessionId: "ccb-session-mcp-error",
      async *submitMessage() {
        yield { type: "result", subtype: "success", is_error: false };
      },
      interrupt: vi.fn(),
      resetAbortController: vi.fn(),
      getAbortSignal: () => new AbortController().signal,
      getMessages: () => [],
      setModel: vi.fn(),
    })),
    listDpcodeCcbMcpStatus: vi.fn(async () => {
      throw new Error("bad mcp config");
    }),
  },
})("CcbAdapterLive MCP errors", (it) => {
  it.effect("surfaces MCP status errors without blocking turns", () =>
    Effect.gen(function* () {
      const adapter = yield* CcbAdapter;
      const threadId = ThreadId.makeUnsafe("thread-ccb-mcp-error-test");
      const statusFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.type === "mcp.status.updated"),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkDetach,
      );

      yield* adapter.startSession({
        threadId,
        provider: "ccb",
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const turn = yield* adapter.sendTurn({
        threadId,
        input: "still works",
      });

      const events = Array.from(yield* Fiber.join(statusFiber));
      assert.equal(turn.threadId, threadId);
      assert.ok(
        (events[0]?.payload.status as { errors?: string[] }).errors?.some((error) =>
          error.includes("bad mcp config"),
        ),
      );
    }),
  );
});

layer({
  bridgeModule: {
    createDpcodeCcbSession: vi.fn(async (input: { initialMessages?: unknown }) => ({
      sessionId: input.initialMessages ? "ccb-session-resumed" : "ccb-session-original",
      async *submitMessage() {
        yield { type: "result", subtype: "success", is_error: false };
      },
      interrupt: vi.fn(),
      resetAbortController: vi.fn(),
      getAbortSignal: () => new AbortController().signal,
      getMessages: () => [{ role: "assistant", content: "persisted transcript" }],
      setModel: vi.fn(),
    })),
  },
})("CcbAdapterLive resume", (it) => {
  it.effect("persists transcript messages and injects them when resuming", () =>
    Effect.gen(function* () {
      const adapter = yield* CcbAdapter;
      const threadId = ThreadId.makeUnsafe(`thread-ccb-resume-test-${crypto.randomUUID()}`);
      const completedFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.type === "turn.completed" && event.threadId === threadId),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkDetach,
      );

      yield* adapter.startSession({
        threadId,
        provider: "ccb",
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({
        threadId,
        input: "persist history",
      });
      yield* Fiber.join(completedFiber);

      const cursor = (yield* adapter.listSessions()).find(
        (entry) => entry.threadId === threadId,
      )?.resumeCursor;
      assert.ok(cursor);
      assert.equal(typeof cursor.transcriptPath, "string");
      yield* Effect.promise(() => waitForTranscript(cursor.transcriptPath as string));
      yield* adapter.stopSession(threadId);

      const resumed = yield* adapter.startSession({
        threadId,
        provider: "ccb",
        cwd: process.cwd(),
        runtimeMode: "full-access",
        resumeCursor: cursor,
      });

      assert.equal(resumed.resumeCursor.ccbSessionId, "ccb-session-resumed");
      assert.equal(resumed.resumeCursor.turnCount, 1);
      assert.equal(resumed.resumeCursor.transcriptPath, cursor.transcriptPath);
    }),
  );
});

layer(makePermissionToolEventFakeBridge())("CcbAdapterLive permission tool events", (it) => {
  it.effect("emits a tool lifecycle row from canUseTool when the SDK stream omits tool_use", () =>
    Effect.gen(function* () {
      const adapter = yield* CcbAdapter;
      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter(
          (event) =>
            event.type === "item.started" ||
            event.type === "item.updated" ||
            event.type === "content.delta" ||
            event.type === "item.completed" ||
            event.type === "turn.completed",
        ),
        Stream.take(5),
        Stream.runCollect,
        Effect.forkDetach,
      );
      const threadId = ThreadId.makeUnsafe("thread-ccb-permission-tool-event-test");

      yield* adapter.startSession({
        threadId,
        provider: "ccb",
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({
        threadId,
        input: "write a file",
      });

      const events = Array.from(yield* Fiber.join(eventsFiber));
      const toolStarted = events.find((event) => event.type === "item.started");
      assert.equal(toolStarted?.type, "item.started");
      if (toolStarted?.type !== "item.started") {
        throw new Error("Expected CCB permission preflight to emit item.started.");
      }
      assert.equal(toolStarted.payload.itemType, "file_change");
      assert.equal(toolStarted.payload.title, "File change");
      assert.deepEqual(toolStarted.payload.data.files, ["index.html"]);

      assert.ok(
        events.some(
          (event) =>
            event.type === "item.updated" &&
            event.payload.itemType === "file_change" &&
            event.payload.status === "inProgress",
        ),
      );
      assert.equal(
        events.some(
          (event) => event.type === "content.delta" && event.payload.streamKind === "file_change_output",
        ),
        false,
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

layer(makePermissionOnlyToolEventFakeBridge())("CcbAdapterLive permission-only tool events", (it) => {
  it.effect("fails tool/file rows when CCB omits the tool_result message", () =>
    Effect.gen(function* () {
      const adapter = yield* CcbAdapter;
      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter(
          (event) =>
            event.type === "item.updated" ||
            event.type === "item.completed" ||
            event.type === "turn.completed",
        ),
        Stream.take(3),
        Stream.runCollect,
        Effect.forkDetach,
      );
      const threadId = ThreadId.makeUnsafe("thread-ccb-permission-only-tool-event-test");

      yield* adapter.startSession({
        threadId,
        provider: "ccb",
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({
        threadId,
        input: "edit a file without emitting tool_result",
      });

      const events = Array.from(yield* Fiber.join(eventsFiber));
      assert.ok(
        events.some(
          (event) =>
            event.type === "item.updated" &&
            event.payload.itemType === "file_change" &&
            event.payload.status === "inProgress" &&
            event.payload.data.files.includes("index.html"),
        ),
      );
      assert.ok(
        events.some(
          (event) =>
            event.type === "item.completed" &&
            event.payload.itemType === "file_change" &&
            event.payload.status === "failed" &&
            event.payload.data.files.includes("index.html"),
        ),
      );
      assert.ok(
        events.some(
          (event) =>
            event.type === "turn.completed" &&
            event.payload.state === "failed" &&
            event.payload.errorMessage.includes("did not emit a tool result"),
        ),
      );
    }),
  );
});

layer(
  makeFakeBridge([
    {
      type: "assistant",
      message: {
        content: [
          {
            type: "tool_use",
            id: "tool-bash-error-1",
            name: "Bash",
            input: { command: "exit 1" },
          },
        ],
      },
    },
    {
      type: "result",
      subtype: "error_during_execution",
      is_error: true,
      stop_reason: "tool_error",
      total_cost_usd: 0.125,
      usage: { input_tokens: 8, output_tokens: 2 },
      modelUsage: {
        "claude-sonnet-4-6": {
          inputTokens: 8,
          outputTokens: 2,
          cacheReadInputTokens: 0,
          cacheCreationInputTokens: 0,
          webSearchRequests: 0,
          costUSD: 0.125,
          contextWindow: 200_000,
          maxOutputTokens: 16_384,
        },
      },
      errors: ["command failed", "exit code 1"],
    },
  ]),
)("CcbAdapterLive result finalization", (it) => {
  it.effect("propagates result stop reason, model usage, errors, cost, and pending tool failure", () =>
    Effect.gen(function* () {
      const adapter = yield* CcbAdapter;
      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter(
          (event) =>
            event.type === "item.started" ||
            event.type === "item.completed" ||
            event.type === "thread.token-usage.updated" ||
            event.type === "turn.completed" ||
            event.type === "runtime.error",
        ),
        Stream.take(5),
        Stream.runCollect,
        Effect.forkDetach,
      );
      const threadId = ThreadId.makeUnsafe("thread-ccb-result-finalization-test");

      yield* adapter.startSession({
        threadId,
        provider: "ccb",
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({
        threadId,
        input: "exercise result finalization",
      });

      const events = Array.from(yield* Fiber.join(eventsFiber));
      assert.ok(
        events.some(
          (event) =>
            event.type === "item.completed" &&
            event.payload.itemType === "command_execution" &&
            event.payload.status === "failed" &&
            (event.payload.data as { command?: string }).command === "exit 1",
        ),
      );
      assert.ok(
        events.some(
          (event) =>
            event.type === "thread.token-usage.updated" &&
            event.payload.usage.usedTokens === 10,
        ),
      );
      const completed = events.find(
        (event): event is Extract<ProviderRuntimeEvent, { type: "turn.completed" }> =>
          event.type === "turn.completed",
      );
      assert.equal(completed?.type, "turn.completed");
      assert.equal(completed?.payload.state, "failed");
      assert.equal(completed?.payload.stopReason, "tool_error");
      assert.equal(completed?.payload.totalCostUsd, 0.125);
      assert.equal(completed?.payload.errorMessage, "command failed\nexit code 1");
      assert.equal(
        (completed?.payload.modelUsage as { "claude-sonnet-4-6"?: { costUSD?: number } })[
          "claude-sonnet-4-6"
        ]?.costUSD,
        0.125,
      );
      assert.ok(
        events.some(
          (event) =>
            event.type === "runtime.error" &&
            event.payload.message === "command failed\nexit code 1",
        ),
      );
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
        Stream.filter(
          (event) =>
            event.type === "content.delta" ||
            event.type === "item.started" ||
            event.type === "item.completed" ||
            event.type === "turn.completed",
        ),
        Stream.take(4),
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
            event.payload.title === "File change",
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
