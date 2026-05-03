<proposed_plan>
# CCB → DPcode 原生 AI 编程助手集成计划

## Summary

- DPcode 与 CCB 都保留为可持续跟随 GitHub 上游的开源来源：`Emanuele-web04/dpcode` 和 `claude-code-best/claude-code`。
- 新项目以 DPcode 为产品外壳，以 CCB 为内置 Agent 大脑，但两者之间通过清晰的 adapter/bridge 边界集成，避免把 CCB 代码“改散”到 DPcode 各处。
- 默认运行路径彻底移除对外部 `codex app-server`、`codex` binary、`claude cli` 的依赖，改为 Server 内直接调用 CCB 的 QueryEngine/Agent Loop。
- 后续同步上游时，优先更新原始 DPcode/CCB 源码目录，再只处理 bridge 层兼容差异，最大限度降低合并成本。

## Upstream-Safe Architecture

- 保持 `CCB-claude-best-t3code` 作为独立 vendor/upstream 目录，不在 DPcode 业务代码中直接穿插修改 CCB 内部文件。
- 对 CCB 的必要改动只允许集中在极少数“导出/入口补丁”中，例如暴露 `QueryEngine`、初始化 helper、SDK event 类型；禁止在 CCB 核心 Agent Loop、工具实现、MCP 逻辑里写 DPcode 专属逻辑。
- DPcode 侧新增 `CcbAdapter` 和 `ccb bridge` 模块，负责协议翻译、会话管理、审批桥接、事件映射、配置注入。
- 建立 `upstream patches` 目录或补丁记录文档，记录所有对 CCB/DPcode 上游源码的必要偏离，方便未来 rebase、subtree pull 或手动同步。
- 合并后的新项目采用“三层边界”：
  - `CCB source`：尽量原样同步上游。
  - `DPcode source`：尽量原样同步上游 UI/server 架构。
  - `integration bridge`：承接两边差异，是主要维护面。

## Key Changes

- 新增 `ProviderKind = "ccb"`，并将默认 Provider 切换为 `ccb`。
- `apps/server` 新增 `CcbAdapter`，实现现有 `ProviderAdapterShape`：
  - `startSession` 创建 CCB engine session。
  - `sendTurn` 调 CCB `QueryEngine.submitMessage()`。
  - `interruptTurn` 使用 `AbortController` 中断。
  - `respondToRequest` 将 DPcode 审批结果回传给 CCB `canUseTool`。
  - `compactThread` 接入 CCB AutoCompact/SnipCompact 能力。
- CCB 继续负责 Agent Loop、工具系统、Prompt 注入、自动压缩、SessionMemory、MCP 客户端、模型调用。
- DPcode Server 继续负责 SQLite session、WebSocket、权限路由、事件持久化、前端协议。
- DPcode Web 尽量不感知 CCB 内部，只继续消费标准 `ProviderRuntimeEvent` 和 orchestration events。

## Event And Session Mapping

- CCB assistant text、reasoning、tool use、tool result、error、compact boundary 统一翻译为 DPcode runtime events。
- CCB 工具审批通过 DPcode 现有 approval UI 展示，用户决策再回到 CCB。
- `resumeCursor` 使用 CCB 原生 session/transcript 信息，例如 `{ ccbSessionId, transcriptPath, turnCount }`，不再依赖 Codex provider thread id。
- MCP 状态、工具列表、Slash commands、Skills 由 CCB 发现，DPcode 只做展示与调用入口映射。

## Sync Strategy

- 后续同步 DPcode 上游时，优先保留 DPcode 原有架构更新，再重新套用 `ccb provider` 和 bridge 层。
- 后续同步 CCB 上游时，优先完整替换或 rebase `CCB-claude-best-t3code`，再修复少量导出补丁和 adapter 类型兼容。
- 所有跨项目耦合必须落在 bridge 层，避免同步上游时同时冲突 DPcode UI、server orchestration、CCB Agent Loop 三个区域。
- 对外部项目新增的 breaking changes，先写兼容 shim，不直接大规模重构两边核心代码。

## Test Plan

- `CcbAdapter` 单元测试：session start/send/interrupt/stop/resume。
- 事件映射测试：assistant delta、reasoning、tool lifecycle、approval、compact、error。
- 权限测试：`read-only`、`workspace-write`、`danger-full-access` 与 CCB tool permission 对齐。
- 恢复测试：重启 DPcode 后能用 CCB transcript/resumeCursor 继续会话。
- MCP 测试：MCP 加载失败不阻塞普通会话，状态正确回传前端。
- 最终验证按项目规则执行：用户明确要求后，再集中运行一次 `bun fmt`、`bun lint`、`bun typecheck`；测试使用 `bun run test`，不运行 `bun test`。

## Assumptions

- 当前实际 CCB 路径为 `D:\AIkaifa\dpcode\CCB-claude-best-t3code`，计划以此为准。
- 合并后项目目标不是 fork 后彻底脱离上游，而是形成可长期同步上游的集成项目。
- CCB 的业务智能保持原汁原味；DPcode 主要提供产品化外壳、持久化、WebSocket、审批 UI 和多会话管理。
</proposed_plan>