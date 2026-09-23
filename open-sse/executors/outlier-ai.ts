/**
 * OutlierAIExecutor — Outlier Playground Chat via streaming turn endpoint
 *
 * Session-based authentication via cookies (_session + _csrf) extracted from
 * /auth/callback after Google OAuth login. Requests require the X-CSRF-Token
 * header. Stateless proxy uses a fixed conversationId per credential set.
 *
 * Auth: _session + _csrf cookies + X-CSRF-Token header
 * API: POST /internal/experts/assistant/conversations/{conversationId}/turn-streaming
 */

import { BaseExecutor, type ExecuteInput } from "./base.ts";
import { makeExecutorErrorResult } from "../utils/error.ts";

const OUTLIER_BASE_URL = "https://playground.outlier.ai";

interface OutlierCredentials {
  _session?: string;
  _csrf?: string;
  csrfToken?: string;
  "X-CSRF-Token"?: string;
  conversationId?: string;
}

function parseCsrfToken(providerSpecificData: unknown): string | null {
  if (!providerSpecificData || typeof providerSpecificData !== "object") return null;
  const data = providerSpecificData as Record<string, unknown>;

  // Try multiple possible keys for CSRF token
  return (
    (typeof data.csrfToken === "string" ? data.csrfToken.trim() : null) ||
    (typeof data["X-CSRF-Token"] === "string" ? data["X-CSRF-Token"].trim() : null) ||
    (typeof data["csrf-token"] === "string" ? data["csrf-token"].trim() : null) ||
    null
  );
}

function extractConversationId(providerSpecificData: unknown): string | null {
  if (!providerSpecificData || typeof providerSpecificData !== "object") return null;
  const data = providerSpecificData as Record<string, unknown>;
  return typeof data.conversationId === "string" ? data.conversationId.trim() : null;
}

function extractModelConfig(providerSpecificData: unknown): { model: string; modelId?: string } {
  if (!providerSpecificData || typeof providerSpecificData !== "object") {
    return { model: "claude-haiku-4-5-20251001", modelId: "695eb23bebfe65cbcfb1c911" };
  }
  const data = providerSpecificData as Record<string, unknown>;
  const model =
    typeof data.modelName === "string" ? data.modelName.trim() : "claude-haiku-4-5-20251001";
  const modelId = typeof data.modelId === "string" ? data.modelId.trim() : undefined;
  return { model, modelId };
}

export class OutlierAIExecutor extends BaseExecutor {
  constructor() {
    super("outlier-ai", { id: "outlier-ai", baseUrl: OUTLIER_BASE_URL });
  }

  async execute(input: ExecuteInput) {
    const { credentials, body, signal } = input;

    if (!credentials?.providerSpecificData) {
      return makeExecutorErrorResult(
        400,
        "outlier-ai: Missing credentials. Store cookie + CSRF in providerSpecificData.",
        body,
        OUTLIER_BASE_URL
      );
    }

    let data: Record<string, unknown>;
    if (typeof credentials.providerSpecificData === "string") {
      try {
        data = JSON.parse(credentials.providerSpecificData);
      } catch {
        return makeExecutorErrorResult(
          400,
          "outlier-ai: Invalid provider data format",
          body,
          OUTLIER_BASE_URL
        );
      }
    } else if (typeof credentials.providerSpecificData === "object") {
      data = credentials.providerSpecificData as Record<string, unknown>;
    } else {
      return makeExecutorErrorResult(
        400,
        "outlier-ai: Invalid provider-specific data",
        body,
        OUTLIER_BASE_URL
      );
    }

    // Support both: cookie in providerSpecificData OR apiKey as fallback for backwards compatibility
    let cookieString = typeof data.cookie === "string" ? data.cookie.trim() : "";
    if (!cookieString && typeof credentials.apiKey === "string") {
      cookieString = credentials.apiKey.trim();
    }
    if (!cookieString) {
      return makeExecutorErrorResult(
        400,
        "outlier-ai: Missing session cookies in providerSpecificData.cookie",
        body,
        OUTLIER_BASE_URL
      );
    }

    const csrfToken = parseCsrfToken(data);
    if (!csrfToken) {
      return makeExecutorErrorResult(
        400,
        "outlier-ai: Missing X-CSRF-Token in providerSpecificData",
        body,
        OUTLIER_BASE_URL
      );
    }

    const conversationId = extractConversationId(data);
    if (!conversationId) {
      return makeExecutorErrorResult(
        400,
        "outlier-ai: Missing conversationId. Create a conversation at playground.outlier.ai/chat and paste the ID from the URL (playground.outlier.ai/conversation/{id})",
        body,
        OUTLIER_BASE_URL
      );
    }

    // Extract the first message text from the OpenAI-format body
    const bodyRecord =
      typeof body === "object" && body !== null ? (body as Record<string, unknown>) : {};
    const messages = Array.isArray(bodyRecord.messages) ? bodyRecord.messages : [];
    const lastMessage = messages.at(-1);
    const messageContent =
      typeof lastMessage === "object" && lastMessage !== null
        ? (lastMessage as Record<string, unknown>).content
        : "";
    const promptText =
      typeof messageContent === "string" ? messageContent : JSON.stringify(messageContent);

    // Get configurable model from credentials, fallback to Claude Haiku
    const { model, modelId } = extractModelConfig(data);

    // Build the Outlier turn request
    const turnRequest: Record<string, unknown> = {
      prompt: {
        model,
        turnType: "Text",
        text: promptText,
        images: [],
        files: [],
        modelWasSwitched: false,
        isMysteryModel: false,
        turnMode: "Normal",
      },
      model,
      turnMode: "Normal",
      turnType: "Text",
      isMysteryModel: false,
    };

    // Add modelId only if provided (Outlier may auto-detect from model name)
    if (modelId) {
      (turnRequest.prompt as Record<string, unknown>).modelId = modelId;
      turnRequest.modelId = modelId;
    }

    const url = new URL(
      `${OUTLIER_BASE_URL}/internal/experts/assistant/conversations/${conversationId}/turn-streaming`
    );

    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36",
      Origin: OUTLIER_BASE_URL,
      Referer: `${OUTLIER_BASE_URL}/chat`,
      "X-CSRF-Token": csrfToken,
      Cookie: cookieString,
    };

    try {
      const response = await fetch(url.toString(), {
        method: "POST",
        headers,
        body: JSON.stringify(turnRequest),
        signal,
      });

      return {
        response,
        url: url.toString(),
        headers,
        transformedBody: turnRequest,
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown error";
      return makeExecutorErrorResult(
        500,
        `outlier-ai request failed: ${message}`,
        body,
        url.toString()
      );
    }
  }
}

export default OutlierAIExecutor;
