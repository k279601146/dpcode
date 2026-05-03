# CCB 原生嵌入 DPcode 计划
使用UTF-8编码读取文件
## Summary

- 将 `CCB-claude-best-t3code` 保持为独立上游源码目录，作为可同步的 vendor 源；DPcode 只在 `apps/server` 新增薄适配层，不直接改写 CCB 核心逻辑。
- 新增 DPcode 原生 `ccb` Provider，默认 Provider 从外部 `codex app-server` 切到内嵌 CCB；旧 `codex`/外部 Claude SDK 适配器先下线出默认路径，后续可作为 legacy 删除。
- 集成边界采用 CCB 的 `QueryEngine`/SDK-style async stream，而不是 spawn CLI 或 JSON-RPC app-server；由 DPcode Server 把 CCB 流事件翻译成现有 `ProviderRuntimeEvent`/orchestration 事件。
- Web 层尽量少改：继续消费 DPcode 协议事件，只补齐 CCB 工具、审批、模型、Skill/Command 发现所需展示字段。

## Key Changes

- 工程结构：
  - 将根目录 `CCB-claude-best-t3code` 纳入 workspace 或通过 TS path alias 暴露为内部依赖，命名为 `@dpcode/ccb-engine` 或等价内部路径。
  - 保持 CCB 自身 `.git`/上游同步边界；DPcode 的改动集中在 `apps/server/src/provider/Layers/CcbAdapter.ts` 和少量桥接模块。
  - 新增 `apps/server/src/provider/ccb/*`，封装 CCB 初始化、AppState、工具权限、MCP 配置、事件转换、会话恢复。

- Provider/API 合同：
  - 在 `packages/contracts` 中新增 `ProviderKind = "ccb"`，新增 `CcbModelSelection`，并将 `DEFAULT_PROVIDER_KIND` 改为 `"ccb"`。
  - 新增或复用 `ProviderComposerCapabilities`：声明 CCB 支持 skills、native slash commands、runtime model list、approval requests、native compact。
  - `ProviderSession.resumeCursor` 使用 `{ ccbSessionId, transcriptPath?, turnCount? }`，不再依赖外部 provider thread id。
  - `ProviderRuntimeEvent` 不做大规模重写；只补充缺失的 CCB-specific payload 字段，例如 compact metadata、MCP status、permission detail。

- Server 集成：
  - 实现 `CcbAdapter`，满足现有 `ProviderAdapterShape`：`startSession` 创建 CCB `QueryEngine`，`sendTurn` 调用 `submitMessage`，`interruptTurn` 触发 `AbortController`，`compactThread` 调 CCB compact 命令或 compact service。
  - `canUseTool` 桥接 DPcode 审批：CCB 请求工具权限时创建 `ProviderRuntimeRequestOpenedEvent`，等待 `respondToRequest` 的 Deferred 决策，再返回 allow/deny。
  - CCB SDK/QueryEngine 输出统一转换为 DPcode runtime events：assistant text delta、reasoning delta、tool start/update/complete、approval open/resolved、turn complete/error。
  - MCP 由 CCB 管理连接与工具发现；DPcode 只负责传入 cwd/settings/env，并把 MCP 状态投影为 runtime status event。
  - Provider registry 默认注册 `CcbAdapter`，并让新会话默认走 `ccb`；移除 `CodexAppServerManager` 在主路径中的依赖。

- CCB 桥接策略：
  - 优先复用 CCB `QueryEngine`，避免重建 Agent Loop、工具系统、AutoCompact、SessionMemory、MCP 客户端。
  - 对 CCB 需要 CLI 初始化的部分抽出 `createCcbEngineSession()` 桥接函数，统一构造 tools、commands、mcpClients、agents、AppState、system prompt 和 permission context。
  - 对必须改动 CCB 的地方采用最小导出补丁：只导出 QueryEngine 入口、初始化 helper、类型，不修改业务逻辑。

## Test Plan
rg 在这台环境里被系统拒绝执行了，切到 PowerShell 原生命令
- 单元测试：
  - `CcbAdapter` session lifecycle：start/send/interrupt/stop/list/readThread。
  - CCB stream → `ProviderRuntimeEvent` 映射：文本 delta、reasoning、tool lifecycle、tool result、turn complete、error。
  - 审批桥：allow/deny/on-failure/on-request/runtimeMode 映射与 Deferred 释放。
  - resumeCursor：重启 server 后能恢复 CCB transcript/session，并继续下一轮。
  - compact：AutoCompact/SnipCompact 事件能被记录，前端 transcript 不丢历史边界。

- 集成测试：
  - 通过现有 orchestration reactor 启动默认 `ccb` session，发送一轮消息并收到 assistant streaming。
  - 工具调用审批从 WebSocket 命令 `thread.approval.respond` 回到 CCB `canUseTool`。
  - 文件读写/命令类工具在 `read-only`、`workspace-write`、`danger-full-access` 下行为符合 DPcode runtime mode。
  - MCP 配置加载失败不阻塞普通会话，状态以 warning/error event 暴露。

- 最终验证：
  - 按项目规则，只有在用户明确要求实现与验证时运行最终一次 `bun fmt`、`bun lint`、`bun typecheck`。
  - 不运行 `bun test`；需要测试时使用 `bun run test` 或定向 Vitest 命令。

## Assumptions

- 以当前实际路径 `D:\AIkaifa\dpcode\CCB-claude-best-t3code` 为 CCB 源目录。
- 目标是默认路径完全不依赖外部 `codex app-server`、`codex` binary 或 `claude` CLI。
- 为便于未来同步上游，CCB 核心目录尽量保持原样；DPcode 侧通过桥接层吸收协议差异。
- 旧 `codex`/`claudeAgent` Provider 可暂时保留代码但不作为默认入口；完成 CCB parity 后再做删除清理。
