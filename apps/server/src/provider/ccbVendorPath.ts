import { existsSync } from "node:fs";
import { resolve } from "node:path";

const CCB_VENDOR_DIRECTORY = "CCB-claude-best-t3code";
const SOURCE_RELATIVE_VENDOR_PATH = "../../../../../CCB-claude-best-t3code";
const BUNDLED_RELATIVE_VENDOR_PATH = "../../../CCB-claude-best-t3code";

function trimToNull(value: string | undefined): string | null {
  const trimmed = value?.trim();
  return trimmed && trimmed.length > 0 ? trimmed : null;
}

export interface ResolveCcbVendorPathInput {
  readonly baseDir: string;
  readonly cwd?: string;
  readonly env?: NodeJS.ProcessEnv;
  readonly exists?: (candidate: string) => boolean;
}

export function listCcbVendorPathCandidates(input: ResolveCcbVendorPathInput): ReadonlyArray<string> {
  const cwd = input.cwd ?? process.cwd();
  const env = input.env ?? process.env;
  const envCandidates = [trimToNull(env.DPCODE_CCB_VENDOR_PATH), trimToNull(env.CCB_VENDOR_PATH)]
    .filter((candidate): candidate is string => candidate !== null)
    .map((candidate) => resolve(cwd, candidate));
  const candidates = [
    ...envCandidates,
    resolve(cwd, CCB_VENDOR_DIRECTORY),
    resolve(cwd, "..", CCB_VENDOR_DIRECTORY),
    resolve(input.baseDir, SOURCE_RELATIVE_VENDOR_PATH),
    resolve(input.baseDir, BUNDLED_RELATIVE_VENDOR_PATH),
  ];

  return Array.from(new Set(candidates));
}

export function resolveCcbVendorPath(input: ResolveCcbVendorPathInput): string {
  const exists = input.exists ?? existsSync;
  const candidates = listCcbVendorPathCandidates(input);
  return candidates.find((candidate) => exists(candidate)) ?? candidates[0] ?? resolve(input.baseDir);
}
