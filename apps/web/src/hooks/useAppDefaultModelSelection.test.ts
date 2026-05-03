import { describe, expect, it } from "vitest";
import { getCustomModelsByProvider } from "../appSettings";
import { resolveRuntimeAwareDefaultModelSelection } from "../lib/defaultModelSelection";

describe("resolveRuntimeAwareDefaultModelSelection", () => {
  const customModelsByProvider = getCustomModelsByProvider({
    customCcbModels: [],
    customCodexModels: [],
    customClaudeModels: [],
    customGeminiModels: [],
    customOpenCodeModels: [],
  });

  it("prefers the first runtime model exposed by CCB discovery", () => {
    expect(
      resolveRuntimeAwareDefaultModelSelection({
        provider: "ccb",
        customModelsByProvider,
        runtimeModels: [
          { slug: "openai/gpt-4.1" },
          { slug: "claude-sonnet-4-6" },
        ],
      }),
    ).toEqual({
      provider: "ccb",
      model: "openai/gpt-4.1",
    });
  });

  it("falls back to the provider default when runtime discovery is empty", () => {
    expect(
      resolveRuntimeAwareDefaultModelSelection({
        provider: "ccb",
        customModelsByProvider,
        runtimeModels: [],
      }),
    ).toEqual({
      provider: "ccb",
      model: "claude-sonnet-4-6",
    });
  });
});
