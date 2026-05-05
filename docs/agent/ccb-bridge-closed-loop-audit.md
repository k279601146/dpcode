# DP Code x CCB Bridge Closed-Loop Audit

## Summary

This audit checks whether DP Code is using CCB's native capabilities end to end, instead of only wrapping the first entry point or compensating with prompts. The main boundary is:

1. DP Code server adapter (`apps/server/src/provider/Layers/CcbAdapter.ts`)
2. DP Code CCB vendor bridge (`CCB-claude-best-t3code/src/dpcode/bridge.ts`)
3. CCB native runtime (`QueryEngine`, `AgentTool`, MCP, skills, task events)

The highest-risk pattern is a partial bridge: the main CCB turn receives a capability or policy, but named subagents, fork/resume agents, background tasks, transcript resume, or DP Code runtime event projection do not.

## Execution Status

Implementation completed for the audit items in this document:

- P0 selected skills now flow through a structured CCB turn option and are expanded by CCB's native prompt slash-command skill path. DP Code no longer relies on selected-skill prompt text for correctness.
- P0/P2 selected skills are checked at native invocation time; disabled or hidden skills are rejected even if stale DP Code UI state submits them.
- P1 MCP resources are passed into `QueryEngine`, live session MCP status is exposed through the bridge handle, and DP Code emits live tools/resources/errors when available.
- P1 named agents with `requiredMcpServers` now surface failed, pending, and auth-required MCP server details in the tool error instead of only saying tools are missing.
- P1 background task terminal notifications now preserve command/cwd/error/final message/output/output path/worktree/URL details.
- P1 transcript resume read failures are explicit instead of silently starting a fresh CCB context.
- P1 permission-mode switches now use a refreshed tool pool source on each CCB turn, so mode-dependent tool visibility is not frozen at session startup.
- P2 workspace cwd is expressed through CCB native cwd/session binding only; DP Code no longer injects the cwd into the CCB system prompt as a correctness mechanism.

Remaining non-blocking follow-up:

- If CCB exposes a stable telemetry assertion surface for `was_discovered` or skill invocation analytics, add a telemetry-specific selected-skill test. Current coverage asserts the native prompt slash-command expansion and permission metadata path.

## Current Guardrails Already Present

- Windows shell capability now has a native permission expression in `toolPermissionContext.alwaysDenyRules.session`, not only a post-filtered `tools` array. This protects main and named subagent `assembleToolPool()` paths.
- `AgentTool` now detects sidechains that end after `user tool_result` or a final assistant `tool_use`, and fails instead of returning stale interim text as `completed`.
- `CcbAdapter` already marks `tool_result.is_error === true` as failed tool lifecycle rows and can allow the main turn to recover later.
- `CcbAdapter` already fails pending tool rows when the final `result` arrives without matching `tool_result`.

## Findings

### P0: Selected skills are prompt text, not a native CCB skill invocation

Subsystem: skills / workflow / context

Status: resolved. DP Code passes `selectedSkills` as structured turn options; CCB resolves them to native prompt slash-command skills, loads skill content, merges allowed tools, records invocation state, and rejects disabled/hidden skills.

Evidence:
- DP Code lists CCB skills through `listDpcodeCcbSkills()`, which classifies CCB prompt commands as skills and preserves name/path/enabled/scope metadata.
- When a user selects skills for a turn, `buildPromptText()` appends plain text: `DPCode selected these skills for this turn. Use them when relevant:` plus name/path.
- That path does not call CCB `SkillTool`, does not enqueue a slash command, and does not mark a skill as invoked/discovered in CCB's native skill lifecycle.

Impact:
- A selected skill may not load its `SKILL.md` or bundled files unless the model independently chooses the right CCB mechanism.
- Skill discovery telemetry and `was_discovered` style CCB behavior can diverge from DP Code UI state.
- This is the same class of issue as the prior Bash problem: DP Code exposes a CCB capability in UI, but execution falls back to prompt compliance.

Fix strategy:
- Add a native selected-skill bridge path. Preferred options, in order:
  1. Extend the CCB bridge with a structured `selectedSkills` turn option that converts each selected skill into the same internal command/attachment shape CCB uses for slash-command skills.
  2. If CCB has no stable public helper, add a narrow CCB-side helper that resolves skill command metadata and returns the native attachment/input expected by `processUserInput`.
- Keep prompt text only as non-authoritative display context, or remove it once native invocation is in place.

Regression tests:
- Fake a selected skill and assert `submitMessage()` receives a structured CCB-native skill invocation, not only appended prompt text.
- CCB-side test: selected skill loads skill content and records invocation/discovery metadata.
- DP Code adapter test: selected skill appears in the transcript as native skill context and survives resume.

### P1: MCP live connection errors/resources are not fully surfaced through DP Code

Subsystem: MCP / tools / context

Status: resolved for the bridge scope. Live MCP tools/resources/errors are exposed from the CCB session handle and emitted by DP Code; MCP resources are passed into `QueryEngine`; required-MCP agent failures now include failed/pending/auth-required server details.

Evidence:
- `setupDpcodeCcbMcp()` collects live MCP `clients`, `tools`, `commands`, `resources`, and `errors`, and writes them into CCB `appState.mcp`.
- `QueryEngine` receives `mcpClients` and initial assembled tools, but its `ProcessUserInputContext.options.mcpResources` is set to `{}`.
- DP Code emits MCP status through `listDpcodeCcbMcpStatus()`, which reads config enabled/disabled state and config errors, not the live `setupDpcodeCcbMcp()` connection errors collected during session startup.

Impact:
- MCP tools may work, but MCP resources and live connection failures can be invisible or incomplete in DP Code.
- If an MCP server partially connects, the user may see configured status rather than actual runtime tool/resource availability.
- Agents relying on MCP resources or `requiredMcpServers` can fail in ways that look like model behavior rather than bridge state.

Fix strategy:
- Extend `DpcodeCcbSession` or startup result with live MCP status from `setupDpcodeCcbMcp()` including connected/failed clients, tools, commands, resources, and errors.
- Pass MCP resources into `QueryEngine` or the relevant CCB context using the native field rather than leaving `mcpResources: {}`.
- Emit DP Code `mcp.status.updated` from live session setup, and keep config-only status as a separate diagnostic source if needed.

Regression tests:
- Fake one successful and one failed MCP server; assert DP Code emits live failed status and error message.
- Fake MCP resources; assert CCB query context receives resources and DP Code can reference them.
- Named Agent with `requiredMcpServers` should fail with a visible CCB/DP Code error when the server is unavailable.

### P1: Background task completion loses terminal detail

Subsystem: workflow / task events / state

Status: resolved. `task_notification` completion now preserves command/cwd/error/final message/output/full output/output path/worktree/URLs/last tool details.

Evidence:
- `CcbAdapter` maps `task_started` and `task_progress` with command, cwd, output, outputPath, URLs, usage, and lastToolName.
- `task_notification` maps only `taskId`, normalized status, summary, and usage.
- CCB background agents and shell tasks can include final message/error/output/worktree path/output file through `enqueueAgentNotification()` and shell task notification paths.

Impact:
- Final failed/killed background tasks can lose the concrete error or output path at the DP Code event layer.
- A task row may show terminal status but not enough detail for the user or main agent to inspect recovery context.
- This particularly affects long-running Agent, PowerShell/Bash, monitor, and TaskOutput workflows.

Fix strategy:
- Expand `task_notification` mapping to preserve CCB fields already parsed elsewhere: `error`, `finalMessage`, `output`, `fullOutput`, `outputPath`, `worktreePath`, `worktreeBranch`, `urls`, `lastToolName`, `command`, and `cwd`.
- Reuse the same extraction helpers used by `task_started` and `task_progress`.
- Ensure `failed`, `killed`, and `stopped` statuses keep diagnostic detail.

Regression tests:
- Fake `task_notification` with `status=failed`, `error`, and `output_path`; assert DP Code `task.completed` includes all fields.
- Fake background Agent completion with worktree path; assert task completion carries worktree metadata.
- Fake killed task with partial output; assert partial output remains visible.

### P1: Transcript resume failures are silent

Subsystem: context / resume / state

Status: resolved. Malformed or unreadable resume transcripts now fail resume visibly instead of silently starting a fresh CCB context.

Evidence:
- `readCcbTranscript()` catches all read/parse errors and returns `undefined`.
- `startSession()` treats `undefined` as a fresh CCB session and still reuses the same resume cursor path when present.
- `writeCcbTranscript()` persists `context.handle.getMessages()` on final result and after stream completion.

Impact:
- A corrupt or unreadable transcript can silently start a fresh CCB `QueryEngine` with no prior messages.
- The UI may show a resumed session while CCB context is actually missing, causing partial analysis or repeated work.
- Tool ids and prior tool_result continuity can be lost without a clear runtime error.

Fix strategy:
- Make resume transcript read failures explicit when a resume cursor path was provided.
- If the file is missing, malformed, or not an array, emit a `runtime.error` and either fail session resume or mark `resumeCursor` as discarded with a visible warning.
- Keep the current best-effort behavior only when no resume cursor is provided.

Regression tests:
- Resume with malformed JSON: assert `startSession` reports a resume error or warning and does not silently claim normal resume.
- Resume with valid transcript: assert `initialMessages` is passed unchanged.
- Resume with missing transcript path: assert visible warning or explicit fresh-session state.

### P1: Permission mode changes are local state only unless tool pool is refreshed

Subsystem: tools / permissions / state

Status: resolved. `QueryEngine` now accepts a `refreshTools` source and DP Code reassembles the CCB tool pool from current permission context/MCP tools for each turn.

Evidence:
- `CcbAdapter.sendTurn()` maps DP Code interaction mode to `setPermissionMode("plan")` or the base permission mode.
- The CCB bridge `setPermissionMode()` mutates `appState.toolPermissionContext.mode`.
- `QueryEngine` was created with a `tools` array assembled at session startup.

Impact:
- CCB native permission checks see the updated mode, but any mode-dependent tool visibility computed into the initial `tools` array may remain stale.
- This is less severe than the Bash subagent issue because runtime permission context still changes, but it can still create prompt/tool definition mismatch across plan/default transitions.

Fix strategy:
- Add a bridge-level `refreshTools()` or pass a QueryEngine option that reassembles `assembleToolPool(appState.toolPermissionContext, appState.mcp.tools)` whenever permission mode changes.
- Ensure named subagents, fork resume, and async resume use the same refreshed source.

Regression tests:
- Start default, switch to plan, assert mode-specific tools visible to model match CCB permission mode.
- Switch back to default, assert tools are reintroduced if CCB would normally expose them.

### P2: Commands and skills discovery is split from invocation semantics

Subsystem: commands / skills / workflow

Status: resolved for selected skills. Listing still uses native CCB command discovery, and selected-skill invocation now goes through CCB native skill command expansion rather than prompt decoration.

Evidence:
- DP Code lists commands through `listDpcodeCcbCommands()` and filters out skill commands.
- DP Code lists skills through `listDpcodeCcbSkills()` and maps CCB commands to skill metadata.
- Turn input currently uses plain prompt composition for selected skills/mentions.

Impact:
- Listing is native enough for UI discovery, but invocation semantics are incomplete.
- Disabled/hidden skill behavior can drift if UI selection bypasses CCB command execution rules.

Fix strategy:
- Treat command/skill selection as native CCB command invocation rather than prompt decoration.
- Preserve enabled/hidden checks at invocation time, not only list time.

Regression tests:
- Disabled skill cannot be invoked even if sent from DP Code UI.
- Hidden skill does not appear and cannot be selected through stale UI state.

### P2: Workspace cwd is expressed twice

Subsystem: context / cwd / workflow

Status: resolved. Workspace cwd is now passed through native CCB cwd/session binding and no longer injected into the DP Code CCB system prompt.

Evidence:
- `runWithBoundCcbCwd()` and CCB bridge `bindDpcodeCcbSessionCwdSync()` set cwd/original cwd/project root and call `process.chdir()`.
- `buildCcbDpcodeSystemPrompt()` also appends workspace instructions describing the active project directory.

Impact:
- Native cwd handling is present, so this is not a correctness blocker.
- The prompt layer can become stale or redundant if cwd changes mid-session or if future multi-root support is added.

Fix strategy:
- Keep native cwd as the source of truth.
- Treat workspace prompt text as optional UX context only; do not rely on it for file operation correctness.
- If cwd changes become supported, update prompt context from the same source as CCB cwd state or remove it.

Regression tests:
- Start session with cwd; assert CCB cwd state and `process.cwd()` are bound correctly without depending on prompt text.
- Resume session; assert cwd is rebound before CCB commands, skills, agents, and MCP discovery.

## Coverage Matrix

| Area | Current status | Risk |
| --- | --- | --- |
| Main tool pool | Uses CCB `assembleToolPool()` plus defensive DP Code normalization | Low |
| Named subagent tool pool | Uses independent CCB `assembleToolPool()` and now receives Bash deny rule through permission context | Low |
| Fork/resume agent tools | Fork resume can use exact parent tools; non-fork resume reassembles from permission context | Medium |
| Runtime permission approvals | `canUseTool` opens DP Code approvals and returns CCB decisions | Medium |
| Skill listing | Uses CCB command discovery | Low |
| Skill invocation | Structured selected-skill turn option expands through CCB native prompt slash-command skill path | Low |
| Commands listing | Uses CCB command discovery | Low |
| MCP tools | Session setup populates CCB appState and initial tool pool | Medium |
| MCP resources/status | Live tools/resources/errors are surfaced and MCP resources are passed into CCB query context | Low |
| Transcript persistence | Writes full `getMessages()` on result/completion | Medium |
| Transcript resume | Read/parse failures are explicit resume errors | Low |
| Background task progress | Started/progress events are rich | Low |
| Background task terminal state | Completion preserves terminal diagnostics and worktree/output details | Low |
| Incomplete subagent result | Now detected and failed | Low |
| Missing tool_result | Adapter fails pending tool rows | Low |

## Recommended Fix Order

All recommended fix-order items have been implemented in the bridge scope:

1. P0 selected skills native invocation bridge.
2. P1 live MCP status/resources bridge.
3. P1 transcript resume failure visibility.
4. P1 task_notification terminal detail preservation.
5. P1 tool pool refresh on permission mode changes.
6. P2 cleanup of prompt-only workspace cwd guidance once native bridges cover correctness.

## Test Command Policy

- Use `bun run test` for targeted tests.
- Do not use `bun test`.
- Do not run `bun fmt`, `bun lint`, or `bun typecheck` unless explicitly requested.
