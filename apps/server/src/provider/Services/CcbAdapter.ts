/**
 * CcbAdapter - Claude Code Best implementation of the generic provider adapter contract.
 *
 * This service owns the in-process CCB QueryEngine lifecycle and emits
 * canonical provider runtime events for DPcode orchestration.
 *
 * @module CcbAdapter
 */
import { ServiceMap } from "effect";

import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "./ProviderAdapter.ts";

export interface CcbAdapterShape extends ProviderAdapterShape<ProviderAdapterError> {
  readonly provider: "ccb";
}

export class CcbAdapter extends ServiceMap.Service<CcbAdapter, CcbAdapterShape>()(
  "t3/provider/Services/CcbAdapter",
) {}

