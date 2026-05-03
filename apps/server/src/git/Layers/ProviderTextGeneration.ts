import { Effect, Layer } from "effect";

import { parseOpenCodeModelSlug } from "../../provider/opencodeRuntime.ts";
import {
  CodexTextGeneration,
  OpenCodeTextGeneration,
  type TextGenerationShape,
  TextGeneration,
} from "../Services/TextGeneration.ts";

const TITLE_MAX_LENGTH = 60;

function compactWhitespace(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

function localTitleFromMessage(message: string): string {
  const compacted = compactWhitespace(message);
  if (!compacted) return "New thread";
  if (compacted.length <= TITLE_MAX_LENGTH) return compacted;
  return `${compacted.slice(0, TITLE_MAX_LENGTH - 1).trimEnd()}...`;
}

function shouldUseOpenCode(input: {
  readonly model?: string;
  readonly modelSelection?: { provider: string };
}): boolean {
  if (input.modelSelection?.provider === "opencode") {
    return true;
  }
  if (input.modelSelection?.provider === "codex") {
    return false;
  }
  return parseOpenCodeModelSlug(input.model) !== null;
}

const makeProviderTextGeneration = Effect.gen(function* () {
  const codexTextGeneration = yield* CodexTextGeneration;
  const openCodeTextGeneration = yield* OpenCodeTextGeneration;

  const resolveImplementation = (input: {
    readonly model?: string;
    readonly modelSelection?: { provider: string };
  }): TextGenerationShape =>
    shouldUseOpenCode(input) ? openCodeTextGeneration : codexTextGeneration;

  const ccbTextGeneration: TextGenerationShape = {
    generateCommitMessage: (input) => codexTextGeneration.generateCommitMessage(input),
    generatePrContent: (input) => codexTextGeneration.generatePrContent(input),
    generateDiffSummary: (input) => codexTextGeneration.generateDiffSummary(input),
    generateBranchName: (input) =>
      Effect.succeed({
        branch: localTitleFromMessage(input.message)
          .toLowerCase()
          .replace(/[^a-z0-9]+/g, "-")
          .replace(/^-+|-+$/g, "")
          .slice(0, 48) || "ccb-thread",
      }),
    generateThreadTitle: (input) =>
      Effect.succeed({
        title: localTitleFromMessage(input.message),
      }),
  };

  const resolveProviderImplementation = (input: {
    readonly model?: string;
    readonly modelSelection?: { provider: string };
  }): TextGenerationShape =>
    input.modelSelection?.provider === "ccb" ? ccbTextGeneration : resolveImplementation(input);

  return {
    generateCommitMessage: (input) =>
      resolveProviderImplementation(input).generateCommitMessage(input),
    generatePrContent: (input) => resolveProviderImplementation(input).generatePrContent(input),
    generateDiffSummary: (input) => resolveProviderImplementation(input).generateDiffSummary(input),
    generateBranchName: (input) => resolveProviderImplementation(input).generateBranchName(input),
    generateThreadTitle: (input) => resolveProviderImplementation(input).generateThreadTitle(input),
  } satisfies TextGenerationShape;
});

export const ProviderTextGenerationLive = Layer.effect(TextGeneration, makeProviderTextGeneration);
