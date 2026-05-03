import { type ModelSelection, type ProviderKind } from "@t3tools/contracts";
import { getCustomModelsByProvider, resolveAppModelSelection } from "../appSettings";

type RuntimeModelDescriptor = {
  slug: string;
};

export function resolveRuntimeAwareDefaultModelSelection(input: {
  provider: ProviderKind;
  customModelsByProvider: ReturnType<typeof getCustomModelsByProvider>;
  runtimeModels?: ReadonlyArray<RuntimeModelDescriptor> | null;
}): ModelSelection {
  const runtimeModel =
    input.runtimeModels?.find((model) => model.slug.trim().length > 0)?.slug.trim() ?? null;

  return {
    provider: input.provider,
    model:
      runtimeModel ?? resolveAppModelSelection(input.provider, input.customModelsByProvider, null),
  };
}
