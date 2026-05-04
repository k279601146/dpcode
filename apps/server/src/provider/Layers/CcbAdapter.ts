/**
 * CcbAdapterLive - In-process Claude Code Best provider adapter.
 *
 * Keeps the CCB integration behind a narrow bridge so DPcode can follow both
 * upstream projects with most compatibility work isolated here.
 *
 * @module CcbAdapterLive
 */
import {
  DEFAULT_MODEL_BY_PROVIDER,
  EventId,
  type CanonicalItemType,
  type CanonicalRequestType,
  type ProviderApprovalDecision,
  type ProviderComposerCapabilities,
  type ProviderListAgentsResult,
  type ProviderListCommandsResult,
  type ProviderListModelsResult,
  type ProviderListSkillsResult,
  type ProviderRuntimeEvent,
  type ProviderSendTurnInput,
  type ProviderSession,
  type ThreadTokenUsageSnapshot,
  type RuntimeContentStreamKind,
  ProviderItemId,
  RuntimeItemId,
  RuntimeRequestId,
  RuntimeTaskId,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import {
  DateTime,
  Effect,
  Fiber,
  Layer,
  Queue,
  Random,
  Stream,
} from "effect";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { mkdir, readFile, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolveCcbVendorPath } from "../ccbVendorPath.ts";
import { CcbAdapter, type CcbAdapterShape } from "../Services/CcbAdapter.ts";
import { withProviderPlanModePrompt } from "../planMode.ts";
import { spawn } from "node:child_process";

const PROVIDER = "ccb" as const;
const CCB_MODEL_DISCOVERY_TIMEOUT_MS = 5_000;
const CCB_BRIDGE_CACHE_VERSION = "v3";
const CCB_BRIDGE_MACRO_DEFINES: Readonly<Record<string, string>> = {
  "MACRO.VERSION": JSON.stringify("2.1.888"),
  "MACRO.BUILD_TIME": JSON.stringify(new Date().toISOString()),
  "MACRO.FEEDBACK_CHANNEL": JSON.stringify(""),
  "MACRO.ISSUES_EXPLAINER": JSON.stringify(""),
  "MACRO.NATIVE_PACKAGE_URL": JSON.stringify(""),
  "MACRO.PACKAGE_URL": JSON.stringify(""),
  "MACRO.VERSION_CHANGELOG": JSON.stringify(""),
};
const DEFAULT_CCB_VENDOR_PATH = resolveCcbVendorPath({
  baseDir: dirname(fileURLToPath(import.meta.url)),
});

type CcbBridgeModule = {
  createDpcodeCcbSession(input: {
    cwd: string;
    model?: string;
    fallbackModel?: string;
    permissionMode?: string;
    openAiBaseUrl?: string;
    openAiApiKey?: string;
    initialMessages?: unknown[];
    canUseTool: (...args: ReadonlyArray<unknown>) => Promise<Record<string, unknown>>;
  }): Promise<CcbSessionHandle>;
  listDpcodeCcbCommands?(cwd: string): Promise<ReadonlyArray<{ name: string; description?: string }>>;
  listDpcodeCcbSkills?(
    cwd: string,
  ): Promise<
    ReadonlyArray<{
      name: string;
      description?: string;
      path: string;
      enabled: boolean;
      scope?: string;
      displayName?: string;
      shortDescription?: string;
    }>
  >;
  listDpcodeCcbMcpStatus?(cwd: string): Promise<{
    servers: ReadonlyArray<{
      name: string;
      transport: string;
      scope?: string;
      enabled: boolean;
    }>;
    errors: ReadonlyArray<string>;
  }>;
};

type CcbSessionHandle = {
  sessionId: string;
  submitMessage(
    prompt: string,
    options?: { uuid?: string; isMeta?: boolean },
  ): AsyncGenerator<Record<string, unknown>, void, unknown>;
  interrupt(): void;
  resetAbortController(): void;
  getAbortSignal(): AbortSignal;
  getMessages(): readonly unknown[];
  setModel(model: string): void;
};

type PendingApproval = {
  readonly detail: string;
  readonly input: Record<string, unknown>;
  readonly resolve: (decision: ProviderApprovalDecision) => void;
  readonly promise: Promise<ProviderApprovalDecision>;
};

type PendingToolItem = {
  readonly itemId: RuntimeItemId;
  readonly itemType: CanonicalItemType;
  readonly title: string;
  readonly toolName: string;
  readonly input: Record<string, unknown>;
  readonly detail?: string;
  readonly lastInputFingerprint?: string;
};

type CcbStreamToolItem = PendingToolItem & {
  readonly itemId: RuntimeItemId;
  readonly partialInputJson: string;
};

type CcbSessionContext = {
  session: ProviderSession;
  readonly handle: CcbSessionHandle;
  readonly transcriptPath: string;
  readonly pendingApprovals: Map<string, PendingApproval>;
  readonly pendingToolItems: Map<string, PendingToolItem>;
  readonly streamToolItemsByIndex: Map<number, CcbStreamToolItem>;
  readonly turns: Array<{ id: TurnId; items: Array<unknown> }>;
  readonly streamedAssistantTextByTurnId: Map<string, string>;
  activeTurnId: TurnId | undefined;
  streamFiber: Fiber.Fiber<void, ProviderAdapterError> | undefined;
  stopped: boolean;
};

export interface CcbAdapterLiveOptions {
  readonly vendorPath?: string;
  readonly bridgeModule?: CcbBridgeModule;
  readonly bridgeBundlePath?: string;
  readonly runBridgeBuild?: (input: CcbBridgeBuildInput) => Promise<void>;
}

export interface CcbBridgeBuildInput {
  readonly vendorPath: string;
  readonly entryPath: string;
  readonly outputPath: string;
}

function asObject(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === "object" ? (value as Record<string, unknown>) : undefined;
}

function asString(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function asArray(value: unknown): ReadonlyArray<unknown> | undefined {
  return Array.isArray(value) ? value : undefined;
}

function asNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function trimOrNull(value: unknown): string | null {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : null;
}

function toMessage(cause: unknown, fallback: string): string {
  return cause instanceof Error && cause.message.trim() ? cause.message : fallback;
}

function rawMethod(message: Record<string, unknown>): string {
  const type = asString(message.type) ?? "message";
  const subtype = asString(message.subtype);
  const event = asObject(message.event);
  const eventType = asString(event?.type);
  return [type, subtype ?? eventType].filter(Boolean).join(".");
}

function contentText(value: unknown): string {
  if (typeof value === "string") return value;
  const blocks = asArray(value);
  if (!blocks) return "";
  return blocks
    .map((block) => {
      const obj = asObject(block);
      return asString(obj?.text) ?? "";
    })
    .filter(Boolean)
    .join("\n");
}

function ccbToolItemType(toolName: string): CanonicalItemType {
  const normalized = toolName.toLowerCase();
  if (normalized === "todowrite" || normalized.includes("todo")) return "plan";
  if (
    normalized === "bash" ||
    normalized.includes("command") ||
    normalized.includes("shell") ||
    normalized.includes("terminal")
  ) {
    return "command_execution";
  }
  if (
    normalized === "write" ||
    normalized === "edit" ||
    normalized === "multiedit" ||
    normalized === "notebookedit" ||
    normalized.includes("file") ||
    normalized.includes("patch") ||
    normalized.includes("replace") ||
    normalized.includes("create") ||
    normalized.includes("delete")
  ) {
    return "file_change";
  }
  if (
    normalized === "task" ||
    normalized === "agent" ||
    normalized.includes("agent") ||
    normalized.includes("subagent") ||
    normalized.includes("sub-agent")
  ) {
    return "collab_agent_tool_call";
  }
  if (normalized.includes("mcp")) return "mcp_tool_call";
  if (normalized === "webfetch" || normalized === "websearch" || normalized.includes("web search"))
    return "web_search";
  if (normalized.includes("image")) return "image_view";
  return "dynamic_tool_call";
}

function isCcbReadOnlyToolName(toolName: string): boolean {
  const normalized = toolName.toLowerCase();
  return (
    normalized === "read" ||
    normalized.includes("read file") ||
    normalized.includes("view") ||
    normalized === "grep" ||
    normalized === "glob" ||
    normalized.includes("search")
  );
}

function classifyCcbRequestType(toolName: string): CanonicalRequestType {
  if (isCcbReadOnlyToolName(toolName)) {
    return "file_read_approval";
  }
  const itemType = ccbToolItemType(toolName);
  if (itemType === "command_execution") {
    return "command_execution_approval";
  }
  if (itemType === "file_change") {
    return "file_change_approval";
  }
  return "dynamic_tool_call";
}

function ccbToolTitle(itemType: CanonicalItemType, toolName: string): string {
  switch (itemType) {
    case "plan":
      return "Plan";
    case "command_execution":
      return "Command run";
    case "file_change":
      return "File change";
    case "mcp_tool_call":
      return "MCP tool call";
    case "collab_agent_tool_call":
      return "Subagent task";
    case "web_search":
      return "Web search";
    case "image_view":
      return "Image view";
    case "dynamic_tool_call":
      return "Tool call";
    default:
      return toolName;
  }
}

function tryParseJsonRecord(value: string): Record<string, unknown> | undefined {
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed)
      ? (parsed as Record<string, unknown>)
      : undefined;
  } catch {
    return undefined;
  }
}

function toolInputFingerprint(input: Record<string, unknown>): string | undefined {
  try {
    return JSON.stringify(input);
  } catch {
    return undefined;
  }
}

function firstToolString(...values: ReadonlyArray<unknown>): string | undefined {
  for (const value of values) {
    if (typeof value === "string" && value.trim().length > 0) {
      return value.trim();
    }
  }
  return undefined;
}

function filePathFromToolInput(input: Record<string, unknown>): string | undefined {
  return firstToolString(
    input.file_path,
    input.filePath,
    input.path,
    input.filename,
    input.notebook_path,
  );
}

function filesFromToolInput(input: Record<string, unknown>): ReadonlyArray<string> {
  const candidates = [
    filePathFromToolInput(input),
    ...(["files", "file_paths", "paths"] as const).flatMap((key) =>
      Array.isArray(input[key])
        ? input[key].filter((value): value is string => typeof value === "string" && value.trim().length > 0)
        : [],
    ),
  ];
  return Array.from(new Set(candidates.filter((value): value is string => value !== undefined)));
}

function commandFromToolInput(input: Record<string, unknown>): string | undefined {
  return firstToolString(input.command, input.cmd);
}

function summarizeCcbToolRequest(
  toolName: string,
  input: Record<string, unknown>,
): string | undefined {
  const command = commandFromToolInput(input);
  if (command) {
    return `${toolName}: ${command.slice(0, 400)}`;
  }

  const filePath = filePathFromToolInput(input);
  if (filePath) {
    return `${toolName}: ${filePath}`;
  }

  if (Object.keys(input).length === 0) {
    return undefined;
  }

  const serialized = JSON.stringify(input);
  return serialized.length <= 400
    ? `${toolName}: ${serialized}`
    : `${toolName}: ${serialized.slice(0, 397)}...`;
}

function ccbToolResultStreamKind(
  itemType: CanonicalItemType,
): Extract<RuntimeContentStreamKind, "command_output" | "file_change_output"> | undefined {
  switch (itemType) {
    case "command_execution":
      return "command_output";
    case "file_change":
      return "file_change_output";
    default:
      return undefined;
  }
}

function ccbToolData(
  toolName: string,
  input: Record<string, unknown>,
  result?: unknown,
): Record<string, unknown> {
  const command = commandFromToolInput(input);
  const filePath = filePathFromToolInput(input);
  const files = filesFromToolInput(input);
  return {
    toolName,
    input,
    ...(command ? { command } : {}),
    ...(filePath ? { filePath } : {}),
    ...(files.length > 0 ? { files } : {}),
    ...(result !== undefined ? { result } : {}),
  };
}

function extractCcbTextContent(value: unknown): string {
  if (typeof value === "string") {
    return value;
  }

  if (Array.isArray(value)) {
    return value.map(extractCcbTextContent).filter(Boolean).join("");
  }

  const record = asObject(value);
  if (!record) {
    return "";
  }

  return (
    asString(record.text) ??
    asString(record.content) ??
    asString(record.output) ??
    asString(record.result) ??
    extractCcbTextContent(record.content)
  );
}

function toolResultText(block: Record<string, unknown>): string {
  return extractCcbTextContent(block.content);
}

function normalizeCcbTodoStatus(value: unknown): "pending" | "inProgress" | "completed" {
  if (value === "completed") return "completed";
  if (value === "inProgress" || value === "in_progress") return "inProgress";
  return "pending";
}

function normalizeCcbTodoTasks(input: Record<string, unknown>):
  | {
      readonly tasks: ReadonlyArray<{
        readonly task: string;
        readonly status: "pending" | "inProgress" | "completed";
      }>;
    }
  | undefined {
  const todos = Array.isArray(input.todos) ? input.todos : undefined;
  if (!todos) return undefined;

  const tasks = todos
    .map((entry) => {
      const todo = asObject(entry);
      if (!todo) return undefined;
      const status = normalizeCcbTodoStatus(todo.status);
      const content = trimOrNull(todo.content);
      const activeForm = trimOrNull(todo.activeForm);
      const task = status === "inProgress" ? (activeForm ?? content) : (content ?? activeForm);
      return task ? { task, status } : undefined;
    })
    .filter(
      (
        task,
      ): task is {
        readonly task: string;
        readonly status: "pending" | "inProgress" | "completed";
      } => task !== undefined,
    );

  return tasks.length > 0 ? { tasks } : undefined;
}

function extractExitPlanModePlan(value: unknown): string | undefined {
  const record = asObject(value);
  const plan = trimOrNull(record?.plan);
  return plan ?? undefined;
}

function normalizeCcbTokenUsage(value: unknown): ThreadTokenUsageSnapshot | undefined {
  const usage = asObject(value);
  if (!usage) return undefined;

  const inputTokens =
    (asNumber(usage.input_tokens) ?? asNumber(usage.inputTokens) ?? 0) +
    (asNumber(usage.cache_creation_input_tokens) ?? asNumber(usage.cacheCreationInputTokens) ?? 0) +
    (asNumber(usage.cache_read_input_tokens) ?? asNumber(usage.cacheReadInputTokens) ?? 0);
  const outputTokens = asNumber(usage.output_tokens) ?? asNumber(usage.outputTokens) ?? 0;
  const cachedInputTokens =
    asNumber(usage.cache_read_input_tokens) ?? asNumber(usage.cacheReadInputTokens);
  const reasoningOutputTokens =
    asNumber(usage.reasoning_output_tokens) ?? asNumber(usage.reasoningOutputTokens);
  const toolUses = asNumber(usage.tool_uses) ?? asNumber(usage.toolUses);
  const durationMs = asNumber(usage.duration_ms) ?? asNumber(usage.durationMs);
  const totalProcessedTokens =
    asNumber(usage.total_tokens) ??
    asNumber(usage.totalTokens) ??
    (inputTokens + outputTokens > 0 ? inputTokens + outputTokens : undefined);
  if (totalProcessedTokens === undefined || totalProcessedTokens <= 0) {
    return undefined;
  }

  const maxTokens =
    asNumber(usage.model_context_window) ??
    asNumber(usage.modelContextWindow) ??
    asNumber(usage.context_window) ??
    asNumber(usage.contextWindow);
  const usedTokens =
    maxTokens !== undefined ? Math.min(totalProcessedTokens, maxTokens) : totalProcessedTokens;

  return {
    usedTokens,
    totalProcessedTokens,
    lastUsedTokens: usedTokens,
    ...(maxTokens !== undefined && maxTokens > 0 ? { maxTokens } : {}),
    ...(inputTokens > 0 ? { inputTokens, lastInputTokens: inputTokens } : {}),
    ...(outputTokens > 0 ? { outputTokens, lastOutputTokens: outputTokens } : {}),
    ...(cachedInputTokens !== undefined ? { cachedInputTokens } : {}),
    ...(reasoningOutputTokens !== undefined
      ? {
          reasoningOutputTokens,
          lastReasoningOutputTokens: reasoningOutputTokens,
        }
      : {}),
    ...(toolUses !== undefined ? { toolUses } : {}),
    ...(durationMs !== undefined ? { durationMs } : {}),
  };
}

function ccbTaskStatus(value: unknown): "completed" | "failed" | "stopped" {
  return value === "failed" || value === "stopped" ? value : "completed";
}

function recordAssistantTextDelta(
  context: CcbSessionContext,
  turnId: TurnId,
  delta: string,
): void {
  const key = String(turnId);
  context.streamedAssistantTextByTurnId.set(
    key,
    `${context.streamedAssistantTextByTurnId.get(key) ?? ""}${delta}`,
  );
}

function assistantFinalTextDelta(
  context: CcbSessionContext,
  turnId: TurnId,
  finalText: string,
): string {
  const streamedText = context.streamedAssistantTextByTurnId.get(String(turnId)) ?? "";
  if (streamedText.length === 0) {
    return finalText;
  }
  if (finalText === streamedText || finalText.trim() === streamedText.trim()) {
    return "";
  }
  if (finalText.startsWith(streamedText)) {
    return finalText.slice(streamedText.length);
  }
  if (streamedText.endsWith(finalText)) {
    return "";
  }
  return finalText;
}

function modelFromInput(input: ProviderSendTurnInput): string | undefined {
  return input.modelSelection?.provider === PROVIDER ? input.modelSelection.model : undefined;
}

function buildPromptText(input: ProviderSendTurnInput): string {
  return withProviderPlanModePrompt({
    text: input.input?.trim() ?? "",
    interactionMode: input.interactionMode,
  });
}

function normalizeCcbOpenAiBaseUrl(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  if (!trimmed) return undefined;
  return trimmed.replace(/\/+$/, "");
}

function ccbOpenAiModelUrl(baseUrl: string): string {
  return `${normalizeCcbOpenAiBaseUrl(baseUrl)}/models`;
}

function ccbBridgeEntryPath(vendorPath: string): string {
  return resolve(vendorPath, "src/dpcode/bridge.ts");
}

function defaultCcbBridgeBundlePath(vendorPath: string): string {
  const cacheKey = createHash("sha1")
    .update(resolve(vendorPath))
    .update(CCB_BRIDGE_CACHE_VERSION)
    .digest("hex");
  return resolve(tmpdir(), "dpcode-ccb-bridge", cacheKey, "bridge.mjs");
}

type CcbResumeCursor = {
  ccbSessionId?: string;
  transcriptPath?: string;
  turnCount?: number;
};

function asCcbResumeCursor(value: unknown): CcbResumeCursor | undefined {
  const cursor = asObject(value);
  if (!cursor) return undefined;
  return {
    ...(typeof cursor.ccbSessionId === "string" ? { ccbSessionId: cursor.ccbSessionId } : {}),
    ...(typeof cursor.transcriptPath === "string" ? { transcriptPath: cursor.transcriptPath } : {}),
    ...(typeof cursor.turnCount === "number" ? { turnCount: cursor.turnCount } : {}),
  };
}

function ccbTranscriptPath(threadId: ThreadId, sessionId: string): string {
  return resolve(tmpdir(), "dpcode-ccb-transcripts", String(threadId), `${sessionId}.json`);
}

async function readCcbTranscript(transcriptPath: string | undefined): Promise<unknown[] | undefined> {
  if (!transcriptPath) return undefined;
  try {
    const raw = await readFile(transcriptPath, "utf8");
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : undefined;
  } catch {
    return undefined;
  }
}

async function writeCcbTranscript(transcriptPath: string, messages: readonly unknown[]): Promise<void> {
  await mkdir(dirname(transcriptPath), { recursive: true });
  await writeFile(transcriptPath, JSON.stringify(messages), "utf8");
}

function ccbBridgeFreshnessInputs(vendorPath: string): ReadonlyArray<string> {
  return [
    ccbBridgeEntryPath(vendorPath),
    resolve(vendorPath, "src/QueryEngine.ts"),
    resolve(vendorPath, "package.json"),
    resolve(vendorPath, "bun.lock"),
  ];
}

async function isCcbBridgeBundleFresh(
  bundlePath: string,
  watchedPaths: ReadonlyArray<string>,
): Promise<boolean> {
  try {
    const bundleStat = await stat(bundlePath);
    for (const watchedPath of watchedPaths) {
      const watchedStat = await stat(watchedPath);
      if (watchedStat.mtimeMs > bundleStat.mtimeMs) {
        return false;
      }
    }
    return true;
  } catch {
    return false;
  }
}

async function runDefaultCcbBridgeBuild(input: CcbBridgeBuildInput): Promise<void> {
  const args = [
    "build",
    input.entryPath,
    "--target=node",
    "--format=esm",
    ...Object.entries(CCB_BRIDGE_MACRO_DEFINES).flatMap(([key, value]) => [
      "--define",
      `${key}=${value}`,
    ]),
    `--outfile=${input.outputPath}`,
  ];

  await new Promise<void>((resolvePromise, reject) => {
    const child = spawn("bun", args, {
      cwd: input.vendorPath,
      shell: process.platform === "win32",
      stdio: ["ignore", "pipe", "pipe"],
    });
    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];

    child.stdout.on("data", (chunk: Buffer) => stdout.push(chunk));
    child.stderr.on("data", (chunk: Buffer) => stderr.push(chunk));
    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) {
        resolvePromise();
        return;
      }
      const output = Buffer.concat([...stdout, ...stderr]).toString("utf8").trim();
      reject(
        new Error(
          [
            `Failed to bundle CCB bridge with bun build (exit ${code ?? "unknown"}).`,
            output,
          ]
            .filter(Boolean)
            .join("\n"),
        ),
      );
    });
  });
}

async function resolveCcbBridgeModuleUrl(
  input: {
    readonly vendorPath: string;
    readonly bridgeBundlePath?: string;
    readonly runBridgeBuild?: (buildInput: CcbBridgeBuildInput) => Promise<void>;
  },
): Promise<string> {
  const entryPath = ccbBridgeEntryPath(input.vendorPath);
  const outputPath = input.bridgeBundlePath ?? defaultCcbBridgeBundlePath(input.vendorPath);
  const watchedPaths = ccbBridgeFreshnessInputs(input.vendorPath);

  if (!existsSync(entryPath)) {
    throw new Error(`CCB bridge entry not found: ${entryPath}`);
  }

  if (!(await isCcbBridgeBundleFresh(outputPath, watchedPaths))) {
    await mkdir(dirname(outputPath), { recursive: true });
    await (input.runBridgeBuild ?? runDefaultCcbBridgeBuild)({
      vendorPath: input.vendorPath,
      entryPath,
      outputPath,
    });
  }

  const outputStat = await stat(outputPath);
  const outputUrl = pathToFileURL(outputPath);
  outputUrl.searchParams.set("mtime", String(outputStat.mtimeMs));
  return outputUrl.href;
}

async function fetchCcbModels(input: {
  readonly baseUrl: string;
  readonly apiKey: string;
}): Promise<ProviderListModelsResult> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), CCB_MODEL_DISCOVERY_TIMEOUT_MS);
  try {
    const response = await fetch(ccbOpenAiModelUrl(input.baseUrl), {
      headers: input.apiKey ? { Authorization: `Bearer ${input.apiKey}` } : undefined,
      signal: controller.signal,
    });
    if (!response.ok) {
      throw new Error(`CCB model discovery failed: HTTP ${response.status}`);
    }
    const payload = (await response.json()) as { data?: Array<{ id?: unknown }> };
    const models = (payload.data ?? [])
      .map((model) => (typeof model.id === "string" ? model.id.trim() : ""))
      .filter(Boolean)
      .map((slug) => ({ slug, name: ccbModelName(slug) }));

    return {
      models,
      source: input.baseUrl,
      cached: false,
    } satisfies ProviderListModelsResult;
  } catch (cause) {
    if (cause instanceof Error && cause.name === "AbortError") {
      throw new Error(
        `CCB model discovery timed out after ${CCB_MODEL_DISCOVERY_TIMEOUT_MS}ms: ${ccbOpenAiModelUrl(
          input.baseUrl,
        )}`,
      );
    }
    throw cause;
  } finally {
    clearTimeout(timeout);
  }
}

function ccbModelName(slug: string): string {
  return slug
    .split(/[/:]/)
    .filter(Boolean)
    .at(-1)
    ?.replace(/[-_]+/g, " ")
    .replace(/\b\w/g, (char) => char.toUpperCase()) ?? slug;
}

function approvalDecisionToCcb(
  decision: ProviderApprovalDecision,
  input: Record<string, unknown>,
): Record<string, unknown> {
  switch (decision) {
    case "accept":
    case "acceptForSession":
      return {
        behavior: "allow",
        updatedInput: input,
        decisionReason: { type: "mode", mode: "default" },
      };
    case "cancel":
    case "decline":
      return {
        behavior: "deny",
        message: "Declined by user",
        decisionReason: { type: "mode", mode: "default" },
      };
  }
}

function makeCcbAdapter(options?: CcbAdapterLiveOptions) {
  return Effect.gen(function* () {
    const sessions = new Map<ThreadId, CcbSessionContext>();
    const runtimeEventQueue = yield* Queue.unbounded<ProviderRuntimeEvent>();
    const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
    const nextEventId = Effect.map(Random.nextUUIDv4, (id) => EventId.makeUnsafe(id));

    let bridgeModulePromise: Promise<CcbBridgeModule> | undefined;
    const loadBridgeModule = () =>
      Effect.tryPromise({
        try: async () => {
          if (options?.bridgeModule) return options.bridgeModule;
          const vendorPath = options?.vendorPath ?? DEFAULT_CCB_VENDOR_PATH;
          bridgeModulePromise ??= resolveCcbBridgeModuleUrl({
            vendorPath,
            ...(options?.bridgeBundlePath ? { bridgeBundlePath: options.bridgeBundlePath } : {}),
            ...(options?.runBridgeBuild ? { runBridgeBuild: options.runBridgeBuild } : {}),
          }).then((moduleUrl) => import(moduleUrl) as Promise<CcbBridgeModule>);
          return bridgeModulePromise;
        },
        catch: (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "ccb/bridge/load",
            detail: toMessage(cause, "Failed to load CCB bridge"),
            cause,
          }),
      });

    const makeStamp = () => Effect.all({ eventId: nextEventId, createdAt: nowIso });
    const offer = (event: ProviderRuntimeEvent): Effect.Effect<void> =>
      Queue.offer(runtimeEventQueue, event).pipe(Effect.asVoid);

    const requireSession = (threadId: ThreadId) =>
      Effect.sync(() => sessions.get(threadId)).pipe(
        Effect.flatMap((context) =>
          context && !context.stopped
            ? Effect.succeed(context)
            : Effect.fail(
                new ProviderAdapterSessionNotFoundError({
                  provider: PROVIDER,
                  threadId,
                }),
              ),
        ),
      );

    const emitSessionState = (
      context: CcbSessionContext,
      state: "starting" | "ready" | "running" | "stopped" | "error",
      reason?: string,
    ) =>
      Effect.gen(function* () {
        const stamp = yield* makeStamp();
        yield* offer({
          type: "session.state.changed",
          eventId: stamp.eventId,
          provider: PROVIDER,
          threadId: context.session.threadId,
          createdAt: stamp.createdAt,
          payload: {
            state,
            ...(reason ? { reason } : {}),
          },
          providerRefs: {
            providerThreadId: context.handle.sessionId,
          },
        });
      });

    const emitRuntimeError = (context: CcbSessionContext, message: string, detail?: unknown) =>
      Effect.gen(function* () {
        const stamp = yield* makeStamp();
        yield* offer({
          type: "runtime.error",
          eventId: stamp.eventId,
          provider: PROVIDER,
          threadId: context.session.threadId,
          ...(context.activeTurnId ? { turnId: context.activeTurnId } : {}),
          createdAt: stamp.createdAt,
          payload: {
            message,
            class: "provider_error",
            ...(detail !== undefined ? { detail } : {}),
          },
          providerRefs: {
            providerThreadId: context.handle.sessionId,
          },
        });
      });

    const emitMcpStatus = (context: CcbSessionContext, status: unknown) =>
      Effect.gen(function* () {
        const stamp = yield* makeStamp();
        yield* offer({
          type: "mcp.status.updated",
          eventId: stamp.eventId,
          provider: PROVIDER,
          threadId: context.session.threadId,
          createdAt: stamp.createdAt,
          payload: { status },
          providerRefs: {
            providerThreadId: context.handle.sessionId,
          },
        });
      });

    const emitContentDelta = (
      context: CcbSessionContext,
      turnId: TurnId,
      streamKind: RuntimeContentStreamKind,
      delta: string,
      raw: ProviderRuntimeEvent["raw"],
      itemId?: RuntimeItemId,
    ) =>
      Effect.gen(function* () {
        if (delta.length === 0) {
          return;
        }
        const stamp = yield* makeStamp();
        yield* offer({
          type: "content.delta",
          eventId: stamp.eventId,
          provider: PROVIDER,
          threadId: context.session.threadId,
          turnId,
          ...(itemId ? { itemId } : {}),
          createdAt: stamp.createdAt,
          payload: {
            streamKind,
            delta,
          },
          raw,
          providerRefs: {
            providerThreadId: context.handle.sessionId,
            ...(itemId ? { providerItemId: ProviderItemId.makeUnsafe(String(itemId)) } : {}),
          },
        });
        if (streamKind === "assistant_text") {
          recordAssistantTextDelta(context, turnId, delta);
        }
      });

    const emitThreadTokenUsage = (
      context: CcbSessionContext,
      turnId: TurnId,
      usage: unknown,
      raw: ProviderRuntimeEvent["raw"],
    ) =>
      Effect.gen(function* () {
        const normalizedUsage = normalizeCcbTokenUsage(usage);
        if (!normalizedUsage) {
          return;
        }
        const stamp = yield* makeStamp();
        yield* offer({
          type: "thread.token-usage.updated",
          eventId: stamp.eventId,
          provider: PROVIDER,
          threadId: context.session.threadId,
          turnId,
          createdAt: stamp.createdAt,
          payload: { usage: normalizedUsage },
          raw,
          providerRefs: { providerThreadId: context.handle.sessionId },
        });
      });

    const emitTodoTasksUpdated = (
      context: CcbSessionContext,
      turnId: TurnId,
      toolInput: Record<string, unknown>,
      raw: ProviderRuntimeEvent["raw"],
      providerItemId?: string,
    ) =>
      Effect.gen(function* () {
        const tasksPayload = normalizeCcbTodoTasks(toolInput);
        if (!tasksPayload) {
          return;
        }
        const stamp = yield* makeStamp();
        yield* offer({
          type: "turn.tasks.updated",
          eventId: stamp.eventId,
          provider: PROVIDER,
          threadId: context.session.threadId,
          turnId,
          createdAt: stamp.createdAt,
          payload: tasksPayload,
          raw,
          providerRefs: {
            providerThreadId: context.handle.sessionId,
            ...(providerItemId ? { providerItemId: ProviderItemId.makeUnsafe(providerItemId) } : {}),
          },
        });
      });

    const emitProposedPlanCompleted = (
      context: CcbSessionContext,
      turnId: TurnId,
      planMarkdown: string,
      raw: ProviderRuntimeEvent["raw"],
      providerItemId?: string,
    ) =>
      Effect.gen(function* () {
        const trimmed = planMarkdown.trim();
        if (trimmed.length === 0) {
          return;
        }
        const stamp = yield* makeStamp();
        yield* offer({
          type: "turn.proposed.completed",
          eventId: stamp.eventId,
          provider: PROVIDER,
          threadId: context.session.threadId,
          turnId,
          createdAt: stamp.createdAt,
          payload: { planMarkdown: trimmed },
          raw,
          providerRefs: {
            providerThreadId: context.handle.sessionId,
            ...(providerItemId ? { providerItemId: ProviderItemId.makeUnsafe(providerItemId) } : {}),
          },
        });
      });

    const emitAssistantMessageCompleted = (
      context: CcbSessionContext,
      turnId: TurnId,
      text: string,
      raw: ProviderRuntimeEvent["raw"],
      providerItemId?: string,
    ) =>
      Effect.gen(function* () {
        const detail = text.trim();
        if (detail.length === 0) {
          return;
        }
        const itemId = RuntimeItemId.makeUnsafe(providerItemId ?? crypto.randomUUID());
        const stamp = yield* makeStamp();
        yield* offer({
          type: "item.completed",
          eventId: stamp.eventId,
          provider: PROVIDER,
          threadId: context.session.threadId,
          turnId,
          itemId,
          createdAt: stamp.createdAt,
          payload: {
            itemType: "assistant_message",
            status: "completed",
            title: "Assistant message",
            detail,
          },
          raw,
          providerRefs: {
            providerThreadId: context.handle.sessionId,
            providerItemId: ProviderItemId.makeUnsafe(String(itemId)),
          },
        });
      });

    const emitToolLifecycle = (
      context: CcbSessionContext,
      turnId: TurnId,
      lifecycle: "item.started" | "item.updated" | "item.completed",
      tool: PendingToolItem & { readonly itemId: RuntimeItemId },
      raw: ProviderRuntimeEvent["raw"],
      status: "inProgress" | "completed" | "failed",
      result?: unknown,
    ) =>
      Effect.gen(function* () {
        const stamp = yield* makeStamp();
        yield* offer({
          type: lifecycle,
          eventId: stamp.eventId,
          provider: PROVIDER,
          threadId: context.session.threadId,
          turnId,
          itemId: tool.itemId,
          createdAt: stamp.createdAt,
          payload: {
            itemType: tool.itemType,
            status,
            title: tool.title,
            ...(tool.detail ? { detail: tool.detail } : {}),
            data: ccbToolData(tool.toolName, tool.input, result),
          },
          raw,
          providerRefs: {
            providerThreadId: context.handle.sessionId,
            providerItemId: ProviderItemId.makeUnsafe(String(tool.itemId)),
          },
        });
      });

    const mapSdkMessage = (
      context: CcbSessionContext,
      turnId: TurnId,
      message: Record<string, unknown>,
    ): Effect.Effect<void> =>
      Effect.gen(function* () {
        const messageType = asString(message.type);
        const method = rawMethod(message);
        const raw = {
          source: "ccb.sdk.message" as const,
          method,
          messageType,
          payload: message,
        };

        if (messageType === "stream_event") {
          const event = asObject(message.event);
          const eventType = asString(event?.type);
          const blockIndex = typeof event?.index === "number" ? event.index : undefined;
          if (eventType === "content_block_start") {
            const blockObj = asObject(event?.content_block);
            const blockType = asString(blockObj?.type);
            if (
              blockIndex === undefined ||
              (blockType !== "tool_use" &&
                blockType !== "server_tool_use" &&
                blockType !== "mcp_tool_use")
            ) {
              return;
            }

            const itemId = RuntimeItemId.makeUnsafe(asString(blockObj?.id) ?? crypto.randomUUID());
            const toolName = asString(blockObj?.name) ?? "Tool";
            const input = asObject(blockObj?.input) ?? {};
            const itemType = ccbToolItemType(toolName);
            const detail = summarizeCcbToolRequest(toolName, input);
            const lastInputFingerprint =
              Object.keys(input).length > 0 ? toolInputFingerprint(input) : undefined;
            const tool: CcbStreamToolItem = {
              itemId,
              itemType,
              title: ccbToolTitle(itemType, toolName),
              toolName,
              input,
              partialInputJson: "",
              ...(detail ? { detail } : {}),
              ...(lastInputFingerprint ? { lastInputFingerprint } : {}),
            };

            context.turns.at(-1)?.items.push(blockObj);
            context.streamToolItemsByIndex.set(blockIndex, tool);
            context.pendingToolItems.set(String(itemId), tool);
            yield* emitToolLifecycle(context, turnId, "item.started", tool, raw, "inProgress");
            if (toolName === "TodoWrite") {
              yield* emitTodoTasksUpdated(context, turnId, input, raw, String(itemId));
            }
            if (toolName === "ExitPlanMode") {
              const planMarkdown = extractExitPlanModePlan(input);
              if (planMarkdown) {
                yield* emitProposedPlanCompleted(context, turnId, planMarkdown, raw, String(itemId));
              }
            }
            return;
          }

          if (eventType === "content_block_delta") {
            const delta = asObject(event?.delta);
            const deltaType = asString(delta?.type);
            if (deltaType === "input_json_delta") {
              if (blockIndex === undefined) {
                return;
              }
              const tool = context.streamToolItemsByIndex.get(blockIndex);
              const partialJson = asString(delta?.partial_json);
              if (!tool || partialJson === undefined) {
                return;
              }

              const partialInputJson = `${tool.partialInputJson}${partialJson}`;
              const parsedInput = tryParseJsonRecord(partialInputJson);
              const detail = parsedInput
                ? summarizeCcbToolRequest(tool.toolName, parsedInput)
                : tool.detail;
              const nextFingerprint =
                parsedInput && Object.keys(parsedInput).length > 0
                  ? toolInputFingerprint(parsedInput)
                  : undefined;
              const nextTool: CcbStreamToolItem = {
                ...tool,
                partialInputJson,
                ...(parsedInput ? { input: parsedInput } : {}),
                ...(detail ? { detail } : {}),
                ...(nextFingerprint ? { lastInputFingerprint: nextFingerprint } : {}),
              };

              context.streamToolItemsByIndex.set(blockIndex, nextTool);
              context.pendingToolItems.set(String(nextTool.itemId), nextTool);
              if (
                parsedInput &&
                nextFingerprint &&
                tool.lastInputFingerprint !== nextFingerprint
              ) {
                yield* emitToolLifecycle(
                  context,
                  turnId,
                  "item.updated",
                  nextTool,
                  raw,
                  "inProgress",
                );
                if (nextTool.toolName === "TodoWrite") {
                  yield* emitTodoTasksUpdated(
                    context,
                    turnId,
                    parsedInput,
                    raw,
                    String(nextTool.itemId),
                  );
                }
                if (nextTool.toolName === "ExitPlanMode") {
                  const planMarkdown = extractExitPlanModePlan(parsedInput);
                  if (planMarkdown) {
                    yield* emitProposedPlanCompleted(
                      context,
                      turnId,
                      planMarkdown,
                      raw,
                      String(nextTool.itemId),
                    );
                  }
                }
              }
              return;
            }

            const text =
              deltaType === "thinking_delta"
                ? (asString(delta?.thinking) ?? asString(delta?.text) ?? "")
                : deltaType === "text_delta" || deltaType === undefined
                  ? (asString(delta?.text) ?? "")
                  : "";
            if (text.length > 0) {
              yield* emitContentDelta(
                context,
                turnId,
                deltaType?.includes("thinking") ? "reasoning_text" : "assistant_text",
                text,
                raw,
              );
            }
          }
          if (eventType === "content_block_stop" && blockIndex !== undefined) {
            context.streamToolItemsByIndex.delete(blockIndex);
          }
          return;
        }

        if (messageType === "assistant") {
          const content = asArray(asObject(message.message)?.content) ?? [];
          const finalTextBlocks: string[] = [];
          for (const block of content) {
            const blockObj = asObject(block);
            const blockType = asString(blockObj?.type);
            if (blockType === "text") {
              const text = asString(blockObj?.text) ?? "";
              if (text.length > 0) {
                finalTextBlocks.push(text);
              }
            } else if (blockType === "thinking") {
              const text = asString(blockObj?.thinking) ?? asString(blockObj?.text) ?? "";
              if (text.length > 0) {
                yield* emitContentDelta(context, turnId, "reasoning_text", text, raw);
              }
            } else if (
              blockType === "tool_use" ||
              blockType === "server_tool_use" ||
              blockType === "mcp_tool_use"
            ) {
              const itemId = RuntimeItemId.makeUnsafe(asString(blockObj?.id) ?? crypto.randomUUID());
              const toolName = asString(blockObj?.name) ?? "Tool";
              const itemType = ccbToolItemType(toolName);
              const input =
                asObject(blockObj?.input) ??
                context.pendingToolItems.get(String(itemId))?.input ??
                {};
              const detail = summarizeCcbToolRequest(toolName, input);
              const inputFingerprint =
                Object.keys(input).length > 0 ? toolInputFingerprint(input) : undefined;
              const existing = context.pendingToolItems.get(String(itemId));
              const tool: PendingToolItem & { readonly itemId: RuntimeItemId } = {
                itemId,
                itemType,
                title: ccbToolTitle(itemType, toolName),
                toolName,
                input,
                ...(detail ? { detail } : {}),
                ...(inputFingerprint ? { lastInputFingerprint: inputFingerprint } : {}),
              };
              context.turns.at(-1)?.items.push(block);
              context.pendingToolItems.set(String(itemId), tool);
              if (!existing) {
                yield* emitToolLifecycle(context, turnId, "item.started", tool, raw, "inProgress");
              } else if (
                inputFingerprint &&
                existing.lastInputFingerprint !== inputFingerprint
              ) {
                yield* emitToolLifecycle(context, turnId, "item.updated", tool, raw, "inProgress");
              }
              if (toolName === "TodoWrite") {
                yield* emitTodoTasksUpdated(context, turnId, input, raw, String(itemId));
              }
              if (toolName === "ExitPlanMode") {
                const planMarkdown = extractExitPlanModePlan(input);
                if (planMarkdown) {
                  yield* emitProposedPlanCompleted(context, turnId, planMarkdown, raw, String(itemId));
                }
              }
            }
          }
          const assistantProviderItemId =
            asString(asObject(message.message)?.id) ?? asString(message.uuid);
          const finalText = finalTextBlocks.join("\n");
          const finalDelta = assistantFinalTextDelta(context, turnId, finalText);
          if (finalDelta.length > 0) {
            yield* emitContentDelta(
              context,
              turnId,
              "assistant_text",
              finalDelta,
              raw,
              assistantProviderItemId
                ? RuntimeItemId.makeUnsafe(assistantProviderItemId)
                : undefined,
            );
          }
          yield* emitAssistantMessageCompleted(
            context,
            turnId,
            finalText,
            raw,
            assistantProviderItemId,
          );
          const messageUsage = asObject(message.message)?.usage ?? message.usage;
          if (messageUsage) {
            yield* emitThreadTokenUsage(context, turnId, messageUsage, raw);
          }
          return;
        }

        if (messageType === "user") {
          const content = asObject(message.message)?.content;
          const blocks = asArray(content);
          const toolResults = blocks
            ?.map(asObject)
            .filter((block): block is Record<string, unknown> => block?.type === "tool_result");
          if (toolResults?.length) {
            for (const block of toolResults) {
              const providerToolUseId = asString(block.tool_use_id);
              const stored = providerToolUseId
                ? context.pendingToolItems.get(providerToolUseId)
                : undefined;
              if (providerToolUseId) {
                context.pendingToolItems.delete(providerToolUseId);
              }
              const itemId = RuntimeItemId.makeUnsafe(providerToolUseId ?? crypto.randomUUID());
              const resultText = toolResultText(block);
              const tool: PendingToolItem & { readonly itemId: RuntimeItemId } = {
                itemId,
                itemType: stored?.itemType ?? "dynamic_tool_call",
                title: stored?.title ?? "Tool call",
                toolName: stored?.toolName ?? stored?.title ?? "Tool",
                input: stored?.input ?? {},
                ...(stored?.detail
                  ? { detail: stored.detail }
                  : resultText
                    ? { detail: resultText }
                    : {}),
                ...(stored?.lastInputFingerprint
                  ? { lastInputFingerprint: stored.lastInputFingerprint }
                  : {}),
              };
              context.turns.at(-1)?.items.push(block);
              yield* emitToolLifecycle(
                context,
                turnId,
                "item.updated",
                tool,
                raw,
                block.is_error === true ? "failed" : "inProgress",
                block,
              );
              const streamKind = ccbToolResultStreamKind(tool.itemType);
              if (streamKind && resultText.length > 0) {
                yield* emitContentDelta(context, turnId, streamKind, resultText, raw, itemId);
              }
              yield* emitToolLifecycle(
                context,
                turnId,
                "item.completed",
                tool,
                raw,
                block.is_error === true ? "failed" : "completed",
                block,
              );
            }
            return;
          }

          const stamp = yield* makeStamp();
          yield* offer({
            type: "item.completed",
            eventId: stamp.eventId,
            provider: PROVIDER,
            threadId: context.session.threadId,
            turnId,
            createdAt: stamp.createdAt,
            payload: {
              itemType: "user_message",
              status: "completed",
              data: message,
              ...(contentText(content) ? { detail: contentText(content) } : {}),
            },
            raw,
            providerRefs: { providerThreadId: context.handle.sessionId },
          });
          return;
        }

        if (messageType === "system") {
          const subtype = asString(message.subtype);
          const stamp = yield* makeStamp();
          const base = {
            eventId: stamp.eventId,
            provider: PROVIDER,
            threadId: context.session.threadId,
            turnId,
            createdAt: stamp.createdAt,
            raw,
            providerRefs: { providerThreadId: context.handle.sessionId },
          } satisfies Omit<ProviderRuntimeEvent, "type" | "payload">;

          switch (subtype) {
            case "init":
              yield* offer({
                ...base,
                type: "session.configured",
                payload: { config: message },
              });
              return;
            case "status":
              yield* offer({
                ...base,
                type: "session.state.changed",
                payload: {
                  state: asString(message.status) === "compacting" ? "waiting" : "running",
                  reason: `status:${asString(message.status) ?? "active"}`,
                  detail: message,
                },
              });
              return;
            case "compact_boundary": {
              const itemId = RuntimeItemId.makeUnsafe(asString(message.uuid) ?? crypto.randomUUID());
              yield* offer({
                ...base,
                type: "thread.state.changed",
                payload: {
                  state: "compacted",
                  detail: message,
                },
              });
              yield* offer({
                ...base,
                type: "item.completed",
                itemId,
                payload: {
                  itemType: "context_compaction",
                  status: "completed",
                  title: "Context compacted",
                  data: message,
                },
                providerRefs: {
                  providerThreadId: context.handle.sessionId,
                  providerItemId: ProviderItemId.makeUnsafe(String(itemId)),
                },
              });
              return;
            }
            case "hook_started":
              yield* offer({
                ...base,
                type: "hook.started",
                payload: {
                  hookId: asString(message.hook_id) ?? "hook",
                  hookName: asString(message.hook_name) ?? "Hook",
                  hookEvent: asString(message.hook_event) ?? "event",
                },
              });
              return;
            case "hook_progress":
              yield* offer({
                ...base,
                type: "hook.progress",
                payload: {
                  hookId: asString(message.hook_id) ?? "hook",
                  ...(asString(message.output) ? { output: asString(message.output) } : {}),
                  ...(asString(message.stdout) ? { stdout: asString(message.stdout) } : {}),
                  ...(asString(message.stderr) ? { stderr: asString(message.stderr) } : {}),
                },
              });
              return;
            case "hook_response":
              yield* offer({
                ...base,
                type: "hook.completed",
                payload: {
                  hookId: asString(message.hook_id) ?? "hook",
                  outcome:
                    message.outcome === "error" || message.outcome === "cancelled"
                      ? message.outcome
                      : "success",
                  ...(asString(message.output) ? { output: asString(message.output) } : {}),
                  ...(asString(message.stdout) ? { stdout: asString(message.stdout) } : {}),
                  ...(asString(message.stderr) ? { stderr: asString(message.stderr) } : {}),
                  ...(asNumber(message.exit_code) !== undefined ? { exitCode: asNumber(message.exit_code) } : {}),
                },
              });
              return;
            case "task_started": {
              const taskId = asString(message.task_id) ?? asString(message.uuid) ?? crypto.randomUUID();
              yield* offer({
                ...base,
                type: "task.started",
                payload: {
                  taskId: RuntimeTaskId.makeUnsafe(taskId),
                  ...(asString(message.description) ? { description: asString(message.description) } : {}),
                  ...(asString(message.task_type) ? { taskType: asString(message.task_type) } : {}),
                },
              });
              return;
            }
            case "task_progress": {
              if (message.usage) {
                yield* emitThreadTokenUsage(context, turnId, message.usage, raw);
              }
              const taskId = asString(message.task_id) ?? asString(message.uuid) ?? crypto.randomUUID();
              yield* offer({
                ...base,
                type: "task.progress",
                payload: {
                  taskId: RuntimeTaskId.makeUnsafe(taskId),
                  description: asString(message.description) ?? "Task progress",
                  ...(asString(message.summary) ? { summary: asString(message.summary) } : {}),
                  ...(message.usage ? { usage: message.usage } : {}),
                  ...(asString(message.last_tool_name) ? { lastToolName: asString(message.last_tool_name) } : {}),
                },
              });
              return;
            }
            case "task_notification": {
              if (message.usage) {
                yield* emitThreadTokenUsage(context, turnId, message.usage, raw);
              }
              const taskId = asString(message.task_id) ?? asString(message.uuid) ?? crypto.randomUUID();
              yield* offer({
                ...base,
                type: "task.completed",
                payload: {
                  taskId: RuntimeTaskId.makeUnsafe(taskId),
                  status: ccbTaskStatus(message.status),
                  ...(asString(message.summary) ? { summary: asString(message.summary) } : {}),
                  ...(message.usage ? { usage: message.usage } : {}),
                },
              });
              return;
            }
            case "files_persisted":
              yield* offer({
                ...base,
                type: "files.persisted",
                payload: {
                  files: (asArray(message.files) ?? [])
                    .map(asObject)
                    .filter((file): file is Record<string, unknown> => file !== undefined)
                    .map((file) => ({
                      filename: asString(file.filename) ?? "file",
                      fileId: asString(file.file_id) ?? asString(file.fileId) ?? "file",
                    })),
                  failed: (asArray(message.failed) ?? [])
                    .map(asObject)
                    .filter((file): file is Record<string, unknown> => file !== undefined)
                    .map((file) => ({
                      filename: asString(file.filename) ?? "file",
                      error: asString(file.error) ?? "Failed to persist file",
                    })),
                },
              });
              return;
            default:
              yield* offer({
                ...base,
                type: "runtime.warning",
                payload: {
                  message: `Unhandled CCB system message subtype '${subtype ?? "unknown"}'.`,
                  detail: message,
                },
              });
              return;
          }
        }

        if (messageType === "tool_progress") {
          const stamp = yield* makeStamp();
          yield* offer({
            type: "tool.progress",
            eventId: stamp.eventId,
            provider: PROVIDER,
            threadId: context.session.threadId,
            turnId,
            createdAt: stamp.createdAt,
            payload: {
              ...(asString(message.tool_use_id) ? { toolUseId: asString(message.tool_use_id) } : {}),
              ...(asString(message.tool_name) ? { toolName: asString(message.tool_name) } : {}),
              ...(asNumber(message.elapsed_time_seconds) !== undefined
                ? { elapsedSeconds: asNumber(message.elapsed_time_seconds) }
                : {}),
              ...(asString(message.task_id) ? { summary: `task:${asString(message.task_id)}` } : {}),
            },
            raw,
            providerRefs: { providerThreadId: context.handle.sessionId },
          });
          return;
        }

        if (messageType === "tool_use_summary") {
          const summary = asString(message.summary);
          if (!summary) {
            return;
          }
          const stamp = yield* makeStamp();
          yield* offer({
            type: "tool.summary",
            eventId: stamp.eventId,
            provider: PROVIDER,
            threadId: context.session.threadId,
            turnId,
            createdAt: stamp.createdAt,
            payload: {
              summary,
              ...(asArray(message.preceding_tool_use_ids)?.every((value) => typeof value === "string")
                ? { precedingToolUseIds: message.preceding_tool_use_ids as string[] }
                : {}),
            },
            raw,
            providerRefs: { providerThreadId: context.handle.sessionId },
          });
          return;
        }

        if (messageType === "auth_status") {
          const stamp = yield* makeStamp();
          yield* offer({
            type: "auth.status",
            eventId: stamp.eventId,
            provider: PROVIDER,
            threadId: context.session.threadId,
            turnId,
            createdAt: stamp.createdAt,
            payload: {
              ...(typeof message.isAuthenticating === "boolean"
                ? { isAuthenticating: message.isAuthenticating }
                : {}),
              ...(asArray(message.output)?.every((value) => typeof value === "string")
                ? { output: message.output as string[] }
                : {}),
              ...(asString(message.error) ? { error: asString(message.error) } : {}),
            },
            raw,
            providerRefs: { providerThreadId: context.handle.sessionId },
          });
          return;
        }

        if (messageType === "rate_limit_event") {
          const stamp = yield* makeStamp();
          yield* offer({
            type: "account.rate-limits.updated",
            eventId: stamp.eventId,
            provider: PROVIDER,
            threadId: context.session.threadId,
            turnId,
            createdAt: stamp.createdAt,
            payload: { rateLimits: message },
            raw,
            providerRefs: { providerThreadId: context.handle.sessionId },
          });
          return;
        }

        if (messageType === "result") {
          yield* Effect.tryPromise({
            try: () => writeCcbTranscript(context.transcriptPath, context.handle.getMessages()),
            catch: (cause) =>
              new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "session/transcript/write",
                detail: toMessage(cause, "Failed to persist CCB transcript"),
                cause,
              }),
          }).pipe(
            Effect.catch((error) =>
              emitRuntimeError(context, error.detail, error.cause),
            ),
          );
          const stamp = yield* makeStamp();
          const isError = message.is_error === true;
          if (message.usage) {
            yield* emitThreadTokenUsage(context, turnId, message.usage, raw);
          }
          for (const [pendingKey, tool] of context.pendingToolItems) {
            yield* emitToolLifecycle(
              context,
              turnId,
              "item.completed",
              tool,
              {
                ...raw,
                method: `${method}.pending-tool-complete`,
              },
              isError ? "failed" : "completed",
              message,
            );
            context.pendingToolItems.delete(pendingKey);
          }
          yield* offer({
            type: "turn.completed",
            eventId: stamp.eventId,
            provider: PROVIDER,
            threadId: context.session.threadId,
            turnId,
            createdAt: stamp.createdAt,
            payload: {
              state: isError ? "failed" : "completed",
              stopReason: asString(message.subtype),
              usage: message.usage,
              ...(typeof message.total_cost_usd === "number"
                ? { totalCostUsd: message.total_cost_usd }
                : {}),
              ...(isError ? { errorMessage: asString(message.result) ?? "CCB turn failed" } : {}),
            },
            raw,
            providerRefs: { providerThreadId: context.handle.sessionId },
          });
          context.streamedAssistantTextByTurnId.delete(String(turnId));
          context.streamToolItemsByIndex.clear();
          if (isError) {
            yield* emitRuntimeError(context, asString(message.result) ?? "CCB turn failed", message);
          }
        }
      });

    const startSession: CcbAdapterShape["startSession"] = (input) =>
      Effect.gen(function* () {
        if (input.provider !== undefined && input.provider !== PROVIDER) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
          });
        }

        const bridge = yield* loadBridgeModule();
        const createdAt = yield* nowIso;
        const threadId = input.threadId;
        const pendingApprovals = new Map<string, PendingApproval>();
        const runtimeMode = input.runtimeMode;
        const resumeCursor = asCcbResumeCursor(input.resumeCursor);
        let contextRef: CcbSessionContext | undefined;
        const initialMessages = yield* Effect.tryPromise({
          try: () => readCcbTranscript(resumeCursor?.transcriptPath),
          catch: (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "session/resume",
              detail: toMessage(cause, "Failed to read CCB transcript"),
              cause,
            }),
        });

        const handle = yield* Effect.tryPromise({
          try: () =>
            bridge.createDpcodeCcbSession({
              cwd: input.cwd ?? process.cwd(),
              ...(input.modelSelection?.provider === PROVIDER ? { model: input.modelSelection.model } : {}),
              ...(input.providerOptions?.ccb?.fallbackModel
                ? { fallbackModel: input.providerOptions.ccb.fallbackModel }
                : {}),
              ...(input.providerOptions?.ccb?.permissionMode
                ? { permissionMode: input.providerOptions.ccb.permissionMode }
                : {}),
              ...(input.providerOptions?.ccb?.openAiBaseUrl
                ? { openAiBaseUrl: input.providerOptions.ccb.openAiBaseUrl }
                : {}),
              ...(input.providerOptions?.ccb?.openAiApiKey
                ? { openAiApiKey: input.providerOptions.ccb.openAiApiKey }
                : {}),
              ...(initialMessages ? { initialMessages } : {}),
              canUseTool: async (...args: ReadonlyArray<unknown>) => {
                const [tool, toolInput, , , toolUseId] = args;
                const inputObject = asObject(toolInput) ?? {};
                const context = contextRef;
                const toolName = asString(asObject(tool)?.name) ?? "Tool";
                const requestType = classifyCcbRequestType(toolName);
                if (context?.activeTurnId) {
                  const itemId = RuntimeItemId.makeUnsafe(asString(toolUseId) ?? crypto.randomUUID());
                  const pendingKey = String(itemId);
                  if (!context.pendingToolItems.has(pendingKey)) {
                    const itemType = ccbToolItemType(toolName);
                    const detail = summarizeCcbToolRequest(toolName, inputObject);
                    const inputFingerprint =
                      Object.keys(inputObject).length > 0
                        ? toolInputFingerprint(inputObject)
                        : undefined;
                    const toolItem: PendingToolItem & { readonly itemId: RuntimeItemId } = {
                      itemId,
                      itemType,
                      title: ccbToolTitle(itemType, toolName),
                      toolName,
                      input: inputObject,
                      ...(detail ? { detail } : {}),
                      ...(inputFingerprint ? { lastInputFingerprint: inputFingerprint } : {}),
                    };
                    context.pendingToolItems.set(pendingKey, toolItem);
                    const raw = {
                      source: "ccb.sdk.permission" as const,
                      method: "canUseTool",
                      payload: { tool, input: toolInput, toolUseId },
                    };
                    const startedAt = new Date().toISOString();
                    await Effect.runPromise(Queue.offer(runtimeEventQueue, {
                        type: "item.started",
                        eventId: EventId.makeUnsafe(crypto.randomUUID()),
                        provider: PROVIDER,
                        threadId,
                        turnId: context.activeTurnId,
                        itemId,
                        createdAt: startedAt,
                        payload: {
                          itemType,
                          status: "inProgress",
                          title: toolItem.title,
                          ...(detail ? { detail } : {}),
                          data: ccbToolData(toolName, inputObject),
                        },
                        raw,
                        providerRefs: {
                          providerThreadId: context.handle.sessionId,
                          providerItemId: ProviderItemId.makeUnsafe(String(itemId)),
                        },
                    } satisfies ProviderRuntimeEvent));
                    await Effect.runPromise(Queue.offer(runtimeEventQueue, {
                      type: "item.updated",
                      eventId: EventId.makeUnsafe(crypto.randomUUID()),
                      provider: PROVIDER,
                      threadId,
                      turnId: context.activeTurnId,
                      itemId,
                      createdAt: startedAt,
                      payload: {
                        itemType,
                        status: "inProgress",
                        title: toolItem.title,
                        ...(detail ? { detail } : {}),
                        data: ccbToolData(toolName, inputObject),
                      },
                      raw,
                      providerRefs: {
                        providerThreadId: context.handle.sessionId,
                        providerItemId: ProviderItemId.makeUnsafe(String(itemId)),
                      },
                    } satisfies ProviderRuntimeEvent));
                  }
                }
                if (runtimeMode === "full-access") {
                  return approvalDecisionToCcb("accept", inputObject);
                }

                const requestId = crypto.randomUUID();
                let resolveDecision!: (decision: ProviderApprovalDecision) => void;
                const decisionPromise = new Promise<ProviderApprovalDecision>((resolve) => {
                  resolveDecision = resolve;
                });
                pendingApprovals.set(requestId, {
                  detail: asString(asObject(tool)?.name) ?? "CCB tool request",
                  input: inputObject,
                  resolve: resolveDecision,
                  promise: decisionPromise,
                });

                const stamp = {
                  eventId: EventId.makeUnsafe(crypto.randomUUID()),
                  createdAt: new Date().toISOString(),
                };
                await Effect.runPromise(Queue.offer(runtimeEventQueue, {
                  type: "request.opened",
                  eventId: stamp.eventId,
                  provider: PROVIDER,
                  threadId,
                  requestId: RuntimeRequestId.makeUnsafe(requestId),
                  createdAt: stamp.createdAt,
                  payload: {
                    requestType,
                    detail: toolName,
                    args: inputObject,
                  },
                  raw: {
                    source: "ccb.sdk.permission",
                    method: "canUseTool",
                    payload: { tool, input: toolInput, toolUseId },
                  },
                } satisfies ProviderRuntimeEvent));

                const resolved = await decisionPromise;
                return approvalDecisionToCcb(resolved, inputObject);
              },
            }),
          catch: (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "session/start",
              detail: toMessage(cause, "Failed to create CCB session"),
              cause,
            }),
        });
        const transcriptPath =
          resumeCursor?.transcriptPath ?? ccbTranscriptPath(threadId, handle.sessionId);

        const session: ProviderSession = {
          provider: PROVIDER,
          status: "ready",
          runtimeMode: input.runtimeMode,
          ...(input.cwd ? { cwd: input.cwd } : {}),
          ...(input.modelSelection?.provider === PROVIDER ? { model: input.modelSelection.model } : {}),
          threadId,
          resumeCursor: {
            ccbSessionId: handle.sessionId,
            transcriptPath,
            turnCount: resumeCursor?.turnCount ?? 0,
          },
          createdAt,
          updatedAt: createdAt,
        };

        const context: CcbSessionContext = {
          session,
          handle,
          transcriptPath,
          pendingApprovals,
          pendingToolItems: new Map(),
          streamToolItemsByIndex: new Map(),
          turns: [],
          streamedAssistantTextByTurnId: new Map(),
          activeTurnId: undefined,
          streamFiber: undefined,
          stopped: false,
        };
        sessions.set(threadId, context);
        contextRef = context;

        const stamp = yield* makeStamp();
        yield* offer({
          type: "session.started",
          eventId: stamp.eventId,
          provider: PROVIDER,
          threadId,
          createdAt: stamp.createdAt,
          payload: {
            message: "CCB session started",
            resume: session.resumeCursor,
            resumed: initialMessages !== undefined,
          },
          providerRefs: {
            providerThreadId: handle.sessionId,
          },
        });
        yield* emitSessionState(context, "ready");

        if (bridge.listDpcodeCcbMcpStatus) {
          const mcpStatus = yield* Effect.tryPromise({
            try: () => bridge.listDpcodeCcbMcpStatus!(input.cwd ?? process.cwd()),
            catch: (cause) =>
              new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "mcp/status",
                detail: toMessage(cause, "Failed to load CCB MCP status"),
                cause,
              }),
          }).pipe(
            Effect.catch((error) =>
              Effect.succeed({
                servers: [],
                errors: [error.detail],
              }),
            ),
          );

          yield* emitMcpStatus(context, mcpStatus);
        }

        return session;
      });

    const sendTurn: CcbAdapterShape["sendTurn"] = (input) =>
      Effect.gen(function* () {
        const context = yield* requireSession(input.threadId);
        if (modelFromInput(input)) {
          context.handle.setModel(modelFromInput(input)!);
        }
        context.handle.resetAbortController();

        const turnId = TurnId.makeUnsafe(yield* Random.nextUUIDv4);
        const startedAt = yield* nowIso;
        context.activeTurnId = turnId;
        context.turns.push({ id: turnId, items: [] });
        context.session = {
          ...context.session,
          status: "running",
          activeTurnId: turnId,
          resumeCursor: {
            ccbSessionId: context.handle.sessionId,
            transcriptPath: context.transcriptPath,
            turnCount: context.turns.length,
          },
          updatedAt: startedAt,
        };

        const stamp = yield* makeStamp();
        yield* offer({
          type: "turn.started",
          eventId: stamp.eventId,
          provider: PROVIDER,
          threadId: input.threadId,
          turnId,
          createdAt: stamp.createdAt,
          payload: {
            model: modelFromInput(input) ?? context.session.model ?? DEFAULT_MODEL_BY_PROVIDER.ccb,
          },
          providerRefs: { providerThreadId: context.handle.sessionId },
        });
        yield* emitSessionState(context, "running");

        const prompt = buildPromptText(input);
        const runStream = Effect.tryPromise({
          try: async () => {
            try {
              for await (const message of context.handle.submitMessage(prompt, { uuid: turnId })) {
                await Effect.runPromise(mapSdkMessage(context, turnId, message));
              }
              context.activeTurnId = undefined;
              context.session = {
                ...context.session,
                status: "ready",
                activeTurnId: undefined,
                resumeCursor: {
                  ccbSessionId: context.handle.sessionId,
                  transcriptPath: context.transcriptPath,
                  turnCount: context.turns.length,
                },
                updatedAt: new Date().toISOString(),
              };
              try {
                await writeCcbTranscript(context.transcriptPath, context.handle.getMessages());
              } catch (cause) {
                await Effect.runPromise(
                  emitRuntimeError(
                    context,
                    toMessage(cause, "Failed to persist CCB transcript"),
                    cause,
                  ),
                );
              }
              await Effect.runPromise(emitSessionState(context, "ready"));
            } catch (cause) {
              await Effect.runPromise(emitRuntimeError(context, toMessage(cause, "CCB turn failed"), cause));
              const errorStamp = {
                eventId: EventId.makeUnsafe(crypto.randomUUID()),
                createdAt: new Date().toISOString(),
              };
              await Effect.runPromise(Queue.offer(runtimeEventQueue, {
                type: context.handle.getAbortSignal().aborted ? "turn.aborted" : "turn.completed",
                eventId: errorStamp.eventId,
                provider: PROVIDER,
                threadId: input.threadId,
                turnId,
                createdAt: errorStamp.createdAt,
                payload:
                  context.handle.getAbortSignal().aborted
                    ? { reason: "interrupted" }
                    : { state: "failed", errorMessage: toMessage(cause, "CCB turn failed") },
                providerRefs: { providerThreadId: context.handle.sessionId },
              } satisfies ProviderRuntimeEvent));
              context.activeTurnId = undefined;
              context.session = {
                ...context.session,
                status: "error",
                activeTurnId: undefined,
                lastError: toMessage(cause, "CCB turn failed"),
                updatedAt: new Date().toISOString(),
              };
            }
          },
          catch: (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "turn/start",
              detail: toMessage(cause, "CCB turn failed"),
              cause,
            }),
        });
        context.streamFiber = yield* runStream.pipe(Effect.forkDetach);

        return {
          threadId: input.threadId,
          turnId,
          resumeCursor: context.session.resumeCursor,
        };
      });

    const interruptTurn: CcbAdapterShape["interruptTurn"] = (threadId) =>
      Effect.gen(function* () {
        const context = yield* requireSession(threadId);
        context.handle.interrupt();
        const stamp = yield* makeStamp();
        if (context.activeTurnId) {
          yield* offer({
            type: "turn.aborted",
            eventId: stamp.eventId,
            provider: PROVIDER,
            threadId,
            turnId: context.activeTurnId,
            createdAt: stamp.createdAt,
            payload: { reason: "interrupted" },
            providerRefs: { providerThreadId: context.handle.sessionId },
          });
        }
      });

    const respondToRequest: CcbAdapterShape["respondToRequest"] = (threadId, requestId, decision) =>
      Effect.gen(function* () {
        const context = yield* requireSession(threadId);
        const pending = context.pendingApprovals.get(requestId);
        if (!pending) {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "item/requestApproval/decision",
            detail: `Unknown pending approval request: ${requestId}`,
          });
        }
        context.pendingApprovals.delete(requestId);
        pending.resolve(decision);
        const stamp = yield* makeStamp();
        yield* offer({
          type: "request.resolved",
          eventId: stamp.eventId,
          provider: PROVIDER,
          threadId,
          requestId: RuntimeRequestId.makeUnsafe(requestId),
          createdAt: stamp.createdAt,
          payload: {
            requestType: "dynamic_tool_call",
            decision,
            resolution: { detail: pending.detail },
          },
          raw: {
            source: "ccb.sdk.permission",
            method: "canUseTool.response",
            payload: { decision },
          },
          providerRefs: { providerThreadId: context.handle.sessionId },
        });
      });

    const respondToUserInput: CcbAdapterShape["respondToUserInput"] = (threadId, requestId) =>
      Effect.fail(
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "item/tool/respondToUserInput",
          detail: `CCB adapter has no pending structured user input request: ${requestId} on ${threadId}`,
        }),
      );

    const stopSession: CcbAdapterShape["stopSession"] = (threadId) =>
      Effect.gen(function* () {
        const context = yield* requireSession(threadId);
        context.stopped = true;
        context.handle.interrupt();
        if (context.streamFiber) {
          yield* Fiber.interrupt(context.streamFiber).pipe(Effect.asVoid);
        }
        sessions.delete(threadId);
        yield* emitSessionState(context, "stopped");
      });

    const readThread: CcbAdapterShape["readThread"] = (threadId) =>
      Effect.gen(function* () {
        const context = yield* requireSession(threadId);
        return {
          threadId,
          cwd: context.session.cwd ?? null,
          turns: context.turns.length
            ? context.turns.map((turn) => ({ id: turn.id, items: [...turn.items] }))
            : [
                {
                  id: TurnId.makeUnsafe("ccb-transcript"),
                  items: [...context.handle.getMessages()],
                },
              ],
        };
      });

    const rollbackThread: CcbAdapterShape["rollbackThread"] = (threadId, numTurns) =>
      Effect.gen(function* () {
        const context = yield* requireSession(threadId);
        context.turns.splice(Math.max(0, context.turns.length - numTurns));
        context.session = {
          ...context.session,
          resumeCursor: {
            ccbSessionId: context.handle.sessionId,
            transcriptPath: context.transcriptPath,
            turnCount: context.turns.length,
          },
          updatedAt: yield* nowIso,
        };
        return yield* readThread(threadId);
      });

    const compactThread: NonNullable<CcbAdapterShape["compactThread"]> = (threadId) =>
      Effect.gen(function* () {
        const context = yield* requireSession(threadId);
        if (context.activeTurnId) {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "thread/compact",
            detail: "Cannot compact a CCB thread while another turn is running.",
          });
        }

        const turnId = TurnId.makeUnsafe(yield* Random.nextUUIDv4);
        const stamp = yield* makeStamp();
        context.activeTurnId = turnId;
        context.turns.push({ id: turnId, items: [] });
        context.session = {
          ...context.session,
          status: "running",
          activeTurnId: turnId,
          updatedAt: stamp.createdAt,
        };

        yield* offer({
          type: "turn.started",
          eventId: stamp.eventId,
          provider: PROVIDER,
          threadId,
          turnId,
          createdAt: stamp.createdAt,
          payload: {
            model: context.session.model ?? DEFAULT_MODEL_BY_PROVIDER.ccb,
          },
          providerRefs: {
            providerThreadId: context.handle.sessionId,
          },
        });
        yield* emitSessionState(context, "running");

        yield* Effect.tryPromise({
          try: async () => {
            try {
              for await (const message of context.handle.submitMessage("/compact", { uuid: turnId })) {
                await Effect.runPromise(mapSdkMessage(context, turnId, message));
              }
              context.activeTurnId = undefined;
              context.session = {
                ...context.session,
                status: "ready",
                activeTurnId: undefined,
                resumeCursor: {
                  ccbSessionId: context.handle.sessionId,
                  transcriptPath: context.transcriptPath,
                  turnCount: context.turns.length,
                },
                updatedAt: new Date().toISOString(),
              };
              try {
                await writeCcbTranscript(context.transcriptPath, context.handle.getMessages());
              } catch (cause) {
                await Effect.runPromise(
                  emitRuntimeError(
                    context,
                    toMessage(cause, "Failed to persist CCB transcript"),
                    cause,
                  ),
                );
              }
              await Effect.runPromise(emitSessionState(context, "ready"));
            } catch (cause) {
              await Effect.runPromise(
                emitRuntimeError(context, toMessage(cause, "CCB compact failed"), cause),
              );
              const errorStamp = {
                eventId: EventId.makeUnsafe(crypto.randomUUID()),
                createdAt: new Date().toISOString(),
              };
              await Effect.runPromise(Queue.offer(runtimeEventQueue, {
                type: context.handle.getAbortSignal().aborted ? "turn.aborted" : "turn.completed",
                eventId: errorStamp.eventId,
                provider: PROVIDER,
                threadId,
                turnId,
                createdAt: errorStamp.createdAt,
                payload:
                  context.handle.getAbortSignal().aborted
                    ? { reason: "interrupted" }
                    : { state: "failed", errorMessage: toMessage(cause, "CCB compact failed") },
                providerRefs: { providerThreadId: context.handle.sessionId },
              } satisfies ProviderRuntimeEvent));
              context.activeTurnId = undefined;
              context.session = {
                ...context.session,
                status: "error",
                activeTurnId: undefined,
                lastError: toMessage(cause, "CCB compact failed"),
                updatedAt: new Date().toISOString(),
              };
            }
          },
          catch: (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "thread/compact",
              detail: toMessage(cause, "CCB compact failed"),
              cause,
            }),
        });
      });

    const listModels: NonNullable<CcbAdapterShape["listModels"]> = (input) =>
      Effect.tryPromise({
        try: async () => {
          const baseUrl =
            normalizeCcbOpenAiBaseUrl(input.ccbOpenAiBaseUrl) ??
            normalizeCcbOpenAiBaseUrl(process.env.OPENAI_BASE_URL);
          const apiKey = input.ccbOpenAiApiKey?.trim() || process.env.OPENAI_API_KEY || "";
          if (!baseUrl) {
            return {
              models: [
                { slug: "claude-sonnet-4-6", name: "Claude Sonnet 4.6" },
                { slug: "claude-opus-4-7", name: "Claude Opus 4.7" },
                { slug: "claude-haiku-4-5", name: "Claude Haiku 4.5" },
              ],
              source: PROVIDER,
              cached: false,
            } satisfies ProviderListModelsResult;
          }

          return fetchCcbModels({ baseUrl, apiKey });
        },
        catch: (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "models/list",
            detail: toMessage(cause, "Failed to list CCB models"),
            cause,
          }),
      });

    const listCommands: NonNullable<CcbAdapterShape["listCommands"]> = (input) =>
      Effect.gen(function* () {
        const bridge = yield* loadBridgeModule();
        if (!bridge.listDpcodeCcbCommands) {
          return { commands: [], source: PROVIDER, cached: false } satisfies ProviderListCommandsResult;
        }
        const commands = yield* Effect.tryPromise({
          try: () => bridge.listDpcodeCcbCommands!(input.cwd),
          catch: (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "commands/list",
              detail: toMessage(cause, "Failed to list CCB commands"),
              cause,
            }),
        });
        return {
          commands: commands.map((command) => ({
            name: command.name,
            ...(command.description ? { description: command.description } : {}),
          })),
          source: PROVIDER,
          cached: false,
        } satisfies ProviderListCommandsResult;
      });

    const listSkills: NonNullable<CcbAdapterShape["listSkills"]> = (input) =>
      Effect.gen(function* () {
        const bridge = yield* loadBridgeModule();
        if (!bridge.listDpcodeCcbSkills) {
          return { skills: [], source: PROVIDER, cached: false } satisfies ProviderListSkillsResult;
        }
        const skills = yield* Effect.tryPromise({
          try: () => bridge.listDpcodeCcbSkills!(input.cwd),
          catch: (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "skills/list",
              detail: toMessage(cause, "Failed to list CCB skills"),
              cause,
            }),
        });
        return {
          skills: skills.map((skill) => ({
            name: skill.name,
            ...(skill.description ? { description: skill.description } : {}),
            path: skill.path,
            enabled: skill.enabled,
            ...(skill.scope ? { scope: skill.scope } : {}),
            ...((skill.displayName || skill.shortDescription)
              ? {
                  interface: {
                    ...(skill.displayName ? { displayName: skill.displayName } : {}),
                    ...(skill.shortDescription ? { shortDescription: skill.shortDescription } : {}),
                  },
                }
              : {}),
          })),
          source: PROVIDER,
          cached: false,
        } satisfies ProviderListSkillsResult;
      });

    const listAgents: NonNullable<CcbAdapterShape["listAgents"]> = () =>
      Effect.succeed({ agents: [], source: PROVIDER, cached: false } satisfies ProviderListAgentsResult);

    const composerCapabilities: ProviderComposerCapabilities = {
      provider: PROVIDER,
      supportsSkillMentions: true,
      supportsSkillDiscovery: true,
      supportsNativeSlashCommandDiscovery: true,
      supportsPluginMentions: true,
      supportsPluginDiscovery: false,
      supportsRuntimeModelList: true,
      supportsThreadCompaction: true,
      supportsThreadImport: true,
    };

    return {
      provider: PROVIDER,
      capabilities: {
        sessionModelSwitch: "in-session",
        supportsSkillMentions: true,
        supportsSkillDiscovery: true,
        supportsNativeSlashCommandDiscovery: true,
        supportsPluginMentions: true,
        supportsPluginDiscovery: false,
        supportsRuntimeModelList: true,
      },
      startSession,
      sendTurn,
      interruptTurn,
      respondToRequest,
      respondToUserInput,
      stopSession,
      listSessions: () => Effect.sync(() => Array.from(sessions.values()).map((entry) => entry.session)),
      hasSession: (threadId) => Effect.sync(() => sessions.has(threadId)),
      readThread,
      rollbackThread,
      compactThread,
      stopAll: () =>
        Effect.gen(function* () {
          for (const threadId of Array.from(sessions.keys())) {
            yield* stopSession(threadId).pipe(Effect.catch(() => Effect.void));
          }
        }).pipe(Effect.zipRight(Queue.shutdown(runtimeEventQueue))),
      streamEvents: Stream.fromQueue(runtimeEventQueue),
      getComposerCapabilities: () => Effect.succeed(composerCapabilities),
      listCommands,
      listSkills,
      listPlugins: () =>
        Effect.succeed({
          marketplaces: [],
          marketplaceLoadErrors: [],
          remoteSyncError: null,
          featuredPluginIds: [],
          source: PROVIDER,
          cached: false,
        }),
      readPlugin: () =>
        Effect.fail(
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "plugin/read",
            detail: "CCB plugin detail discovery is not available yet.",
          }),
        ),
      listModels,
      listAgents,
    } satisfies CcbAdapterShape;
  });
}

export const CcbAdapterLive = Layer.effect(CcbAdapter, makeCcbAdapter());

export function makeCcbAdapterLive(options?: CcbAdapterLiveOptions) {
  return Layer.effect(CcbAdapter, makeCcbAdapter(options));
}
