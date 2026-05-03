import { type ModelSelection } from "@t3tools/contracts";
import { useQuery } from "@tanstack/react-query";
import { useMemo } from "react";
import { getCustomModelsByProvider, type AppSettings } from "../appSettings";
import { resolveRuntimeAwareDefaultModelSelection } from "../lib/defaultModelSelection";
import { providerModelsQueryOptions } from "../lib/providerDiscoveryReactQuery";

type DefaultModelSelectionSettings = Pick<
  AppSettings,
  | "ccbOpenAiApiKey"
  | "ccbOpenAiBaseUrl"
  | "claudeBinaryPath"
  | "codexBinaryPath"
  | "customCcbModels"
  | "customClaudeModels"
  | "customCodexModels"
  | "customGeminiModels"
  | "customOpenCodeModels"
  | "defaultProvider"
  | "geminiBinaryPath"
  | "openCodeBinaryPath"
>;

function providerDefaultModelsQueryOptions(settings: DefaultModelSelectionSettings) {
  switch (settings.defaultProvider) {
    case "ccb":
      return providerModelsQueryOptions({
        provider: "ccb",
        ccbOpenAiBaseUrl: settings.ccbOpenAiBaseUrl,
        ccbOpenAiApiKey: settings.ccbOpenAiApiKey,
      });
    case "codex":
      return providerModelsQueryOptions({
        provider: "codex",
        binaryPath: settings.codexBinaryPath || null,
      });
    case "claudeAgent":
      return providerModelsQueryOptions({
        provider: "claudeAgent",
        binaryPath: settings.claudeBinaryPath || null,
      });
    case "gemini":
      return providerModelsQueryOptions({
        provider: "gemini",
        binaryPath: settings.geminiBinaryPath || null,
      });
    case "opencode":
      return providerModelsQueryOptions({
        provider: "opencode",
        binaryPath: settings.openCodeBinaryPath || null,
      });
  }
}

export function useAppDefaultModelSelection(
  settings: DefaultModelSelectionSettings,
): ModelSelection {
  const customModelsByProvider = useMemo(() => getCustomModelsByProvider(settings), [settings]);
  const runtimeModelsQuery = useQuery(providerDefaultModelsQueryOptions(settings));

  return useMemo(
    () =>
      resolveRuntimeAwareDefaultModelSelection({
        provider: settings.defaultProvider,
        customModelsByProvider,
        runtimeModels: runtimeModelsQuery.data?.models ?? null,
      }),
    [customModelsByProvider, runtimeModelsQuery.data?.models, settings.defaultProvider],
  );
}
