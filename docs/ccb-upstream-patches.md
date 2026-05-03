# CCB Upstream Patch Record

DPcode keeps `CCB-claude-best-t3code` as a vendor/upstream source tree. Any
local CCB changes must stay small, documented here, and focused on exposing a
stable integration boundary.

## Current Patches

- `CCB-claude-best-t3code/src/dpcode/bridge.ts`
  - Adds a DPcode-only shim that creates a CCB `QueryEngine` session and exposes
    the narrow methods DPcode needs: submit, interrupt, reset abort controller,
    read messages, and set model.
  - The shim intentionally does not modify CCB Agent Loop, tools, API clients,
    MCP logic, prompt construction, compaction, or memory extraction.

## Sync Notes

- When syncing CCB upstream, restore this file if the vendor tree is replaced.
- If CCB changes `QueryEngineConfig`, update only this shim and the DPcode
  `CcbAdapter` bridge. Avoid writing DPcode-specific behavior into CCB core.

