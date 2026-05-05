# Explore Agent工作流程

Explore Agent是Claude Code中专门用于代码库探索的只读代理，设计为快速、高效的文件搜索工具。

## 核心定义与配置

Explore Agent作为内置代理定义在`exploreAgent.ts`中 [1](#7-0) ：


## 系统提示词与行为约束

Explore Agent使用严格的只读系统提示词 [2](#7-1) ：

- **只读模式**：严禁创建、修改、删除文件
- **搜索专长**：文件模式匹配和正则表达式搜索
- **工具限制**：只能使用`FILE_READ_TOOL_NAME`、`GLOB_TOOL_NAME`、`GREP_TOOL_NAME`和只读的`BASH_TOOL_NAME`
- **性能优化**：强调并行工具调用和高效搜索

## 执行路径

作为命名Agent，Explore Agent通过`AgentTool.call()`的标准路径执行 [3](#7-2) ：

1. **入口解析** - `AgentTool.call()`接收`subagent_type: "Explore"`
2. **权限过滤** - 检查Agent访问权限
3. **工具组装** - 通过`assembleToolPool()`独立组装只读工具池
4. **核心执行** - `runAgent()`执行探索任务
5. **结果汇总** - `finalizeAgentTool()`格式化返回结果

## 工具池独立组装

Explore Agent不继承父Agent的工具限制，而是获得独立的只读工具池 [4](#7-3) ：

```typescript
const workerPermissionContext = {
  ...appState.toolPermissionContext,
  mode: selectedAgent.permissionMode ?? 'acceptEdits'
}
const workerTools = assembleToolPool(workerPermissionContext, appState.mcp.tools)
```

## 输出处理机制

### 大输出持久化
当Explore Agent输出超过2000字节时，系统自动触发持久化 [5](#7-4) ：

- 完整输出保存到`~/.claude/projects/{project}/tool-results/{toolUseId}.txt`
- 返回2000字节预览和文件路径
- 用户可通过`FileReadTool`或`BashTool`读取完整结果

### 一次性代理优化
Explore Agent被归类为"一次性内置代理" [6](#7-5) ，在输出格式化时省略agentId和usage块以节省token。

## 在Plan Mode中的使用

在Plan Mode的Phase 1阶段，系统明确要求使用Explore Agent进行代码库探索 [7](#7-6) ：

- 可并行启动多个Explore Agent
- 每个Agent负责特定的搜索焦点
- 为后续的Plan Agent提供探索结果

## 性能特性

- **模型选择**：外部用户使用haiku模型以获得更快的响应速度
- **并行能力**：支持多个Explore Agent并行执行不同搜索任务
- **缓存优化**：`omitClaudeMd: true`避免加载不必要的规则文件
- **工具效率**：基于ripgrep的高性能文件搜索

Explore Agent通过这种设计实现了快速、安全的代码库探索，为复杂的代码分析任务提供了基础支撑。

## Notes

- Explore Agent的输出会持久化到`~/.claude/projects/{project}/{sessionId}/subagents/`目录
- 系统通过多层过滤机制处理`node_modules`等目录，即使不遵守`.gitignore`
- Explore Agent是Plan Mode工作流程中的关键组件，负责初始代码库理解


**File:** docs/agent/sub-agents.mdx (L11-34)
```text
一条 `Agent(prompt="修复 bug")` 调用的完整路径：

```
AI 生成 tool_use: { prompt: "修复 bug", subagent_type: "Explore" }
  ↓
AgentTool.call()                              ← 入口（AgentTool.tsx:387）
  ├── 解析 effectiveType（fork vs 命名 agent vs GP 回退）
  ├── filterDeniedAgents()                    ← 仅命名 Agent 路径执行：权限过滤
  ├── 检查 requiredMcpServers                 ← MCP 依赖验证（最长等 30s）
  ├── assembleToolPool(workerPermissionContext) ← 独立组装工具池
  ├── createAgentWorktree()                   ← 可选 worktree 隔离
  ↓
runAgent()                                    ← 核心执行（runAgent.ts）
  ├── getAgentSystemPrompt()                  ← 构建 agent 专属 system prompt
  ├── initializeAgentMcpServers()             ← agent 级 MCP 服务器
  ├── executeSubagentStartHooks()             ← Hook 注入
  ├── query()                                 ← 进入标准 agentic loop
  │   ├── 消息流逐条 yield
  │   └── recordSidechainTranscript()         ← JSONL 持久化（~/.claude/projects/{project}/{session}/subagents/）
  ↓
finalizeAgentTool()                           ← 结果汇总
  ├── 提取文本内容 + usage 统计
  └── mapToolResultToToolResultBlockParam()   ← 格式化为 tool_result
```
```

**File:** docs/agent/sub-agents.mdx (L105-119)
```text
命名 Agent 的工具池组装逻辑：

```typescript
const workerPermissionContext = {
  ...appState.toolPermissionContext,
  mode: selectedAgent.permissionMode ?? 'acceptEdits'
}
const workerTools = assembleToolPool(workerPermissionContext, appState.mcp.tools)
```

关键设计决策：
- **权限模式独立**：子 Agent 使用 `selectedAgent.permissionMode`（默认 `acceptEdits`），不受父 Agent 当前模式的限制
- **MCP 工具继承**：`appState.mcp.tools` 包含所有已连接的 MCP 工具，子 Agent 自动获得
- **Agent 级 MCP 服务器**：`runAgent()` 中的 `initializeAgentMcpServers()` 可以为特定 Agent 额外连接专属 MCP 服务器

```


当Explore Agent输出过大时，Claude Code会自动将完整结果保存到文件并显示预览。这是通过工具结果持久化机制实现的。

## 大输出处理机制

### 1. 自动持久化触发
当工具输出超过预设阈值时，系统会调用`persistToolResult`函数将内容保存到磁盘 [1](#6-0) ：

```typescript
export async function persistToolResult(
  content: NonNullable<ToolResultBlockParam['content']>,
  toolUseId: string,
): Promise<PersistedToolResult | PersistToolResultError>
```

### 2. 存储位置
文件保存在工具结果目录中，路径通过`getToolResultPath`生成 [2](#6-1) ：

```typescript
export function getToolResultPath(id: string, isJson: boolean): string {
  const ext = isJson ? 'json' : 'txt'
  return join(getToolResultsDir(), `${id}.${ext}`)
}
```

### 3. 预览生成
系统会生成2000字节的预览 [3](#6-2) ，并构建包含文件路径的消息 [4](#6-3) 。

### 4. Explore Agent特定处理
Explore Agent作为内置的一次性Agent，其输出处理遵循特殊优化 [5](#6-4) 。

## 读取完整输出的方法

### 使用FileRead工具
```bash
# 读取保存的完整输出
FileReadTool /path/to/.claude/projects/{project}/{sessionId}/tool-results/{agentId}.txt
```

### 使用Bash工具
```bash
# 直接查看文件内容
BashTool cat /path/to/.claude/projects/{project}/{sessionId}/tool-results/{agentId}.txt

# 或者使用tail查看最新内容
BashTool tail -f /path/to/.claude/projects/{project}/{sessionId}/tool-results/{agentId}.txt
```

## 输出大小限制

- **预览大小**: 2000字节 [3](#6-2) 
- **最大持久化大小**: 64MB [6](#6-5) 
- **Bash工具限制**: 超过限制时会自动后台化 [7](#6-6) 

## 实际应用建议

1. **查找文件路径**: 从Agent输出消息中获取保存的文件路径
2. **分段读取**: 对于超大文件，使用FileRead的offset和limit参数分段读取
3. **搜索特定内容**: 使用Grep工具在保存的文件中搜索关键信息

这种机制确保了即使Explore Agent产生大量输出（如完整的代码库分析），系统也能稳定运行并让你访问完整结果。

## Notes

- 工具结果目录通常位于`~/.claude/projects/{project}/tool-results/`
- Explore Agent使用haiku模型以提高速度，但可能产生更多详细输出
- 可以通过环境变量调整某些大小限制阈值

