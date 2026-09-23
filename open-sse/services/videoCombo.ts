/**
 * Video Combo Strategy Execution
 *
 * Executes a full Combo strategy for video generation requests. Expands combo
 * targets via resolveComboTargets(), filters to video-capable targets, runs
 * each target via handleVideoGeneration() using a priority strategy, provides
 * per-credential resolution, and returns the first success or last failure.
 *
 * Mirrors imageCombo.ts (#9239) — video generation had no combo integration
 * at all (unlike chat and images), so a rate-limited/failed video provider
 * had no automatic fallback. This closes that gap.
 */
import { getComboByName, getCombos } from "@/lib/db/combos";
import { resolveComboTargets } from "@omniroute/open-sse/services/combo.ts";
import { parseVideoModel, getVideoProvider } from "@omniroute/open-sse/config/videoRegistry.ts";
import { resolveVideoCredentialProvider } from "@omniroute/open-sse/handlers/videoGeneration/googleFlow.ts";
import {
  getProviderCredentialsWithQuotaPreflight,
  clearRecoveredProviderState,
} from "@/sse/services/auth";
import { isAllRateLimitedCredentials } from "@/app/api/v1/_shared/rateLimit";
import { handleVideoGeneration } from "@omniroute/open-sse/handlers/videoGeneration.ts";
import { attachOmniRouteMetaHeaders } from "@/domain/omnirouteResponseMeta";
import { generateRequestId } from "@/shared/utils/requestId";
import { calculateModalCost } from "@/lib/usage/costCalculator";
import { toJsonErrorPayload } from "@/shared/utils/upstreamError";
import { HTTP_STATUS } from "@omniroute/open-sse/config/constants.ts";
import { errorResponse } from "@omniroute/open-sse/utils/error.ts";
import * as logger from "@/sse/utils/logger";

/**
 * Caller-facing shape of handleVideoGeneration(). The handler is untyped and
 * returns a wide inferred union across providers, so we narrow it to the two
 * discriminated arms this strategy actually consumes.
 */
type VideoGenerationResult =
  | { success: true; data?: unknown; status?: number; error?: string }
  | { success: false; data?: unknown; status?: number; error?: string };

/**
 * Execute a full combo strategy for a video generation request.
 *
 * 1. Resolve combo targets via resolveComboTargets.
 * 2. Filter to video-capable targets (those that resolve in the video registry).
 * 3. Iterate targets in priority order; for each target, resolve credentials and
 *    call handleVideoGeneration. Return the first success or the last failure.
 * 4. Attach combo name, selected target, and fallback count to response headers.
 */
export async function executeVideoCombo(
  comboName: string,
  body: Record<string, unknown>,
  auth: {
    request: Request;
    policy: { apiKeyInfo?: { id?: string; name?: string } | null };
  },
  startTime: number,
  log: typeof logger
): Promise<Response> {
  // 1. Resolve combo targets
  const combo = await getComboByName(comboName);
  if (!combo) {
    // Model name is not a combo; the caller should handle this as a direct model
    return errorResponse(HTTP_STATUS.BAD_REQUEST, `Combo not found: ${comboName}`);
  }

  const allCombos = await getCombos();
  const targets = resolveComboTargets(combo as never, allCombos as never);
  if (!targets || targets.length === 0) {
    return errorResponse(HTTP_STATUS.BAD_REQUEST, `Combo "${comboName}" has no usable targets`);
  }

  // 2. Filter to video-capable targets. videoRegistry.ts is single-purpose
  // (video providers/models only), so a resolvable provider is sufficient —
  // no separate modality-entry lookup is needed here (unlike images, whose
  // registry mixes modalities and needs getImageModelEntry to disambiguate).
  const videoTargets = targets.filter((t) => {
    if (!t.modelStr) return false;
    return parseVideoModel(t.modelStr).provider !== null;
  });

  if (videoTargets.length === 0) {
    return errorResponse(
      HTTP_STATUS.BAD_REQUEST,
      `No video-capable targets in combo "${comboName}"`
    );
  }

  // 3. Iterate targets in priority order (first healthy target wins)
  let lastError: { status: number; error: string } | null = null;
  let successResult: { data: unknown; provider: string; model: string } | null = null;
  let fallbackCount = 0;
  let selectedProvider = "";
  let selectedModel = "";

  for (const target of videoTargets) {
    const { provider: targetProvider, model: targetModel } = parseVideoModel(target.modelStr);
    if (!targetProvider) {
      lastError = { status: 400, error: `Invalid video model: ${target.modelStr}` };
      fallbackCount += 1;
      continue;
    }
    void targetModel;

    // Resolve credentials. Mirrors the authType branching in
    // videos/generations/route.ts: providers with authType "none" (e.g. the
    // free veoaifree-web wrapper) run without a stored credential rather than
    // being skipped for lacking one.
    const providerConfig = getVideoProvider(targetProvider);
    let credentials: unknown = null;

    if (!providerConfig || providerConfig.authType !== "none") {
      credentials = await getProviderCredentialsWithQuotaPreflight(
        resolveVideoCredentialProvider(targetProvider)
      );
      if (!credentials) {
        lastError = { status: 400, error: `No credentials for video provider: ${targetProvider}` };
        fallbackCount += 1;
        continue;
      }
      if (isAllRateLimitedCredentials(credentials)) {
        lastError = { status: 429, error: `[${targetProvider}] All accounts rate limited` };
        fallbackCount += 1;
        continue;
      }
    } else {
      // authType "none" — local/free provider, no credential required. Still
      // attempt a best-effort connection lookup (e.g. a base-URL override)
      // but never skip the target for lacking one.
      const localCredentials = await getProviderCredentialsWithQuotaPreflight(targetProvider);
      if (localCredentials && isAllRateLimitedCredentials(localCredentials)) {
        lastError = { status: 429, error: `[${targetProvider}] All accounts rate limited` };
        fallbackCount += 1;
        continue;
      }
      credentials = localCredentials || null;
    }

    // Execute video generation for this target
    const result = (await handleVideoGeneration({
      body: { ...body, model: target.modelStr },
      credentials,
      log,
    })) as VideoGenerationResult;

    if (result.success) {
      await clearRecoveredProviderState(credentials);
      selectedProvider = targetProvider;
      selectedModel = target.modelStr;
      successResult = {
        data: result.data,
        provider: targetProvider,
        model: target.modelStr,
      };
      break;
    }

    // Classify the failure. Only 400 (malformed request — the prompt/model
    // spec itself, which fails identically against every target) stops
    // iteration. 401/403/429/5xx describe THIS provider's account/quota
    // state, not the request's validity — e.g. Novita's "insufficient
    // balance" (403) says nothing about whether agnes or veo-free would
    // succeed — so all of those fall through to the next target. This is a
    // deliberate deviation from imageCombo.ts, which treats 403 as terminal;
    // that conflates "this account is out of credit" with "this request is
    // fundamentally invalid," which defeats the whole point of a combo:
    // auto-rotating past a provider that hit a limit.
    const status = result.status || 500;
    const error = typeof result.error === "string" ? result.error : "Video generation failed";

    if (status === 400) {
      return errorResponse(status, `[${targetProvider}] ${error}`);
    }

    lastError = { status, error: `[${targetProvider}] ${error}` };
    fallbackCount += 1;
  }

  // 4. Build response
  if (successResult) {
    const seconds = Number(body.duration) || 0;
    const costUsd = await calculateModalCost("video", selectedProvider, selectedModel, {
      seconds,
    });

    const headers = new Headers({ "Content-Type": "application/json" });
    attachOmniRouteMetaHeaders(headers, {
      provider: selectedProvider,
      model: selectedModel,
      costUsd,
      latencyMs: Date.now() - startTime,
      requestId: generateRequestId(),
      strategy: "priority",
      fallbackAttempts: fallbackCount,
    });

    // NOTE: unlike imageCombo.ts, this returns successResult.data as-is
    // ({created, data:[...]}) rather than unwrapping one more ".data" level —
    // that extra unwrap in imageCombo.ts strips the "created" field and
    // diverges from the direct (non-combo) route's response contract, which
    // returns the handler's payload verbatim. Matching the direct video
    // route (videos/generations/route.ts: `JSON.stringify(result.data)`)
    // keeps combo-routed and direct-routed video responses byte-identical.
    return new Response(JSON.stringify(successResult.data), {
      status: 200,
      headers,
    });
  }

  // All targets failed — return the last error
  const errorPayload = toJsonErrorPayload(
    lastError?.error || "All combo targets failed",
    "Video combo targets all failed"
  );
  return new Response(JSON.stringify(errorPayload), {
    status: lastError?.status || 502,
    headers: { "Content-Type": "application/json" },
  });
}
