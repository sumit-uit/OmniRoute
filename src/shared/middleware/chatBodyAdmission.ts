/**
 * Process-local bounded admission for POST /v1/chat/completions.
 *
 * Large chat bodies amplify into multiple transient representations while they are parsed,
 * translated, compressed, and dispatched. A heap snapshot alone cannot prevent two healthy
 * requests from entering that allocation-heavy path together. This module reserves process-
 * local heavyweight capacity before parsing and enforces the hard limit against bytes read,
 * not an untrusted Content-Length header.
 *
 * Process-wide admission budget (#10110): ALL requests — every API key, every
 * session — contend for ONE global heavyweight budget, so the documented
 * "in one process" bound holds against fake-credential sharding. Per-request
 * session identity is used only as a fairness scheduling key: waiters are
 * grouped per session and served round-robin against the shared budget, so one
 * connection's burst cannot starve others (#9654).
 */

import { CORS_HEADERS } from "../utils/cors";
import { createHash } from "crypto";
import { resolveFeatureFlag } from "../utils/featureFlags";

function parsePositiveInt(value: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(String(value), 10);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function parseNonNegativeInt(value: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(String(value), 10);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : fallback;
}

export const CHAT_LARGE_BODY_BYTES = parsePositiveInt(
  process.env.OMNIROUTE_CHAT_LARGE_BODY_BYTES,
  256 * 1024
);

export const CHAT_HARD_MAX_BODY_BYTES = parsePositiveInt(
  process.env.OMNIROUTE_CHAT_HARD_MAX_BODY_BYTES,
  50 * 1024 * 1024
);

export const CHAT_MAX_HEAVY_IN_FLIGHT = parsePositiveInt(
  resolveFeatureFlag("CHAT_MAX_HEAVY_IN_FLIGHT"),
  1
);

/**
 * How long a heavy request waits for heavyweight capacity before giving up with a
 * retryable 503. Agent loops (OpenCode, Claude Code, Cursor…) fan out sub-requests
 * that routinely land on the admission gate together; an immediate 503 makes the
 * client burn its retry budget in seconds and the agent dies mid-task. A short
 * bounded wait serializes the burst instead. `0` (legacy) rejects immediately.
 */
export const CHAT_ADMISSION_QUEUE_MAX_MS = parseNonNegativeInt(
  process.env.OMNIROUTE_CHAT_ADMISSION_QUEUE_MS,
  2000
);

/**
 * Queued-bytes budget for the admission wait (#9654 / U3). A parked waiter holds a
 * fully-buffered request body; several large coding-agent bodies (~750 KB) waiting at
 * once is exactly the heap-amplification scenario chatBodyAdmission was built to stop
 * (#4380). Each lane's controller charges every parked waiter's buffered size against
 * this budget and rejects over-budget waits immediately (retryable 503) instead of
 * parking. Bytes are released when a waiter wakes, aborts, or times out.
 */
export const CHAT_ADMISSION_MAX_QUEUED_BYTES = parsePositiveInt(
  process.env.OMNIROUTE_CHAT_ADMISSION_MAX_QUEUED_BYTES,
  4 * 1024 * 1024
);

export const CHAT_HEAVY_MESSAGE_COUNT = parsePositiveInt(
  process.env.OMNIROUTE_CHAT_HEAVY_MESSAGE_COUNT,
  200
);
export const CHAT_HEAVY_TOOL_COUNT = parsePositiveInt(
  process.env.OMNIROUTE_CHAT_HEAVY_TOOL_COUNT,
  64
);
export const CHAT_HEAVY_ESTIMATED_TOKENS = parsePositiveInt(
  process.env.OMNIROUTE_CHAT_HEAVY_ESTIMATED_TOKENS,
  32_000
);
/**
 * Optional per-deployment history cap. `0` (the default) disables it.
 *
 * A fixed message count is a *deployment policy*, not a universal property of a chat request:
 * the same 900-message conversation is trivial on a 16 GB host and fatal in a 1 GB container.
 * Enforcing one here rejected conversations before OmniRoute's own compression pipeline — the
 * component that exists precisely to make them servable — ever ran, and returned a terminal 413
 * that no client can retry its way out of. Message count is also not an input the caller fully
 * controls: translation from other protocols expands a single turn into several `messages[]`
 * entries, so the metric an operator caps is partly manufactured by OmniRoute itself.
 *
 * What actually bounds heap growth is the heavyweight lease below (bounded concurrency through
 * the allocation-heavy path) plus the heap-pressure shed in the chat handler. Both remain in
 * force for every request, including large ones. Constrained deployments that still want a hard
 * ceiling opt in with `OMNIROUTE_CHAT_HARD_MAX_MESSAGES`.
 */
export const CHAT_HARD_MAX_MESSAGES = parsePositiveInt(
  process.env.OMNIROUTE_CHAT_HARD_MAX_MESSAGES,
  0
);

export interface ChatAdmissionLease {
  readonly released: boolean;
  release(): void;
}

/** A parked waiter, grouped by fairness key for round-robin dispatch. */
interface AdmissionWaiter {
  readonly key: string;
  readonly resolve: () => void;
}

/**
 * Process-local heavyweight reservation. The capacity check and increment execute in one
 * synchronous JavaScript turn, making acquisition atomic within an OmniRoute process.
 * Unavailable capacity is a bounded wait (see `acquireHeavyWithin`) and only then a
 * retryable 503, so short agent bursts serialize instead of killing the client's
 * retry budget.
 */
export class ChatAdmissionController {
  #activeHeavy = 0;
  #queuedBytes = 0;
  /** Per-key FIFOs. A key groups one client's waiters so they are served
   * round-robin against the shared budget instead of monopolizing a strict
   * FIFO (see #dispatchFair). */
  #queues = new Map<string, AdmissionWaiter[]>();
  /** Keys in creation order; #fairCursor scans them round-robin. */
  #fairKeys: string[] = [];
  #fairCursor = 0;

  constructor(
    readonly maxHeavyInFlight = 1,
    readonly maxQueuedBytes = CHAT_ADMISSION_MAX_QUEUED_BYTES
  ) {
    if (!Number.isSafeInteger(maxHeavyInFlight) || maxHeavyInFlight < 1) {
      throw new RangeError("maxHeavyInFlight must be a positive integer");
    }
    if (!Number.isSafeInteger(maxQueuedBytes) || maxQueuedBytes < 0) {
      throw new RangeError("maxQueuedBytes must be a non-negative integer");
    }
  }

  get activeHeavy(): number {
    return this.#activeHeavy;
  }

  /** Total buffered bytes currently parked across all queues (heap valve accounting). */
  get queuedBytes(): number {
    return this.#queuedBytes;
  }

  /** Total waiters parked across all keys (diagnostics). */
  get waitingCount(): number {
    let total = 0;
    for (const queue of this.#queues.values()) total += queue.length;
    return total;
  }

  /** Per-key waiter depths (diagnostics) — opaque scheduler keys, never raw credentials. */
  get waitersByKey(): ReadonlyArray<{ key: string; waiting: number }> {
    const out: Array<{ key: string; waiting: number }> = [];
    for (const [key, queue] of this.#queues) out.push({ key, waiting: queue.length });
    return out;
  }

  tryAcquireHeavy(): ChatAdmissionLease | null {
    if (this.#activeHeavy >= this.maxHeavyInFlight) return null;
    this.#activeHeavy += 1;
    let released = false;
    return {
      get released() {
        return released;
      },
      release: () => {
        if (released) return;
        released = true;
        this.#activeHeavy = Math.max(0, this.#activeHeavy - 1);
        this.#dispatchFair();
      },
    };
  }

  /**
   * Wait up to `timeoutMs` for heavyweight capacity, retrying atomically on each
   * release. Resolves `null` when the deadline expires with no capacity freed, in
   * which case the caller answers the retryable 503. `timeoutMs <= 0` is the
   * legacy immediate-reject path.
   *
   * Waiters are grouped by `sessionKey` and served round-robin across keys
   * (#dispatchFair), so one client's burst cannot starve another's bounded wait
   * while every key contends for the SAME process-wide budget.
   *
   * When `signal` aborts while parked (client disconnect), the waiter is removed
   * from its queue immediately and the promise resolves `null` early instead of
   * parking for the full `timeoutMs` — the caller's 503 is dropped on the dead
   * connection, so no capacity is consumed and the freed slot never wakes a
   * waiter the client no longer needs. A signal that is already aborted never
   * parks at all.
   *
   * `queuedBytes` is the buffered body size this waiter will hold while parked;
   * it is charged against `maxQueuedBytes` so a burst of large bodies cannot
   * amplify the heap (#4380). An over-budget wait is rejected immediately with
   * `null` (retryable 503) and never parks; the charge is released on wake,
   * abort, or timeout.
   */
  async acquireHeavyWithin(
    timeoutMs: number,
    signal?: AbortSignal,
    queuedBytes = 0,
    sessionKey = "default"
  ): Promise<ChatAdmissionLease | null> {
    const deadline = Date.now() + Math.max(0, Math.floor(timeoutMs));
    for (;;) {
      if (signal?.aborted) return null;
      const lease = this.tryAcquireHeavy();
      if (lease) return lease;
      const remaining = deadline - Date.now();
      if (remaining <= 0) return null;
      // Heap valve: refuse to park when the queued-bytes budget is exhausted.
      if (queuedBytes > 0 && this.#queuedBytes + queuedBytes > this.maxQueuedBytes) {
        return null;
      }
      this.#queuedBytes += queuedBytes;
      // Park into this key's FIFO (creating the key on first use).
      let queue = this.#queues.get(sessionKey);
      if (!queue) {
        queue = [];
        this.#queues.set(sessionKey, queue);
        this.#fairKeys.push(sessionKey);
      }
      const lane = queue;
      let resolveParked: (() => void) | null = null;
      const waiter: AdmissionWaiter = {
        key: sessionKey,
        resolve: () => resolveParked?.(),
      };
      const parked = new Promise<void>((resolve) => {
        resolveParked = () => resolve();
        lane.push(waiter);
      });
      let deadlineTimer: ReturnType<typeof setTimeout> | null = null;
      const races: Array<Promise<boolean>> = [
        parked.then(() => false),
        new Promise<boolean>((resolve) => {
          deadlineTimer = setTimeout(() => resolve(true), remaining);
        }),
      ];
      let onAbort: (() => void) | null = null;
      if (signal) {
        races.push(
          new Promise<boolean>((resolve) => {
            const listener = () => resolve(true);
            onAbort = listener;
            signal.addEventListener("abort", listener, { once: true });
            // Already-aborted signals must settle without parking.
            if (signal.aborted) resolve(true);
          })
        );
      }
      const timedOut = await Promise.race(races);
      // The waiter has left its queue (wake, abort, or timeout) — release its charge.
      this.#queuedBytes = Math.max(0, this.#queuedBytes - queuedBytes);
      this.#removeWaiter(waiter);
      // Cancel the deadline timer when abort/release wins; a fired timer is a no-op.
      if (deadlineTimer) clearTimeout(deadlineTimer);
      if (onAbort) signal?.removeEventListener("abort", onAbort);
      if (timedOut) return null;
    }
  }

  /** Remove a parked waiter from its key's queue, dropping empty keys. Idempotent. */
  #removeWaiter(waiter: AdmissionWaiter): void {
    const queue = this.#queues.get(waiter.key);
    if (!queue) return;
    const index = queue.indexOf(waiter);
    if (index >= 0) queue.splice(index, 1);
    if (queue.length === 0) this.#removeFairKey(waiter.key);
  }

  #removeFairKey(key: string): void {
    this.#queues.delete(key);
    const index = this.#fairKeys.indexOf(key);
    if (index < 0) return;
    this.#fairKeys.splice(index, 1);
    if (index < this.#fairCursor) this.#fairCursor -= 1;
    if (this.#fairKeys.length === 0) this.#fairCursor = 0;
  }

  /**
   * Round-robin dispatch across per-key queues (#9654 fairness, #10110 global
   * budget). Called on every release; wakes exactly ONE waiter — the head of
   * the next key in rotation — so the freed slot is claimed atomically by the
   * woken waiter's re-loop. A strict FIFO would let one client's burst consume
   * every freed slot; rotating the cursor gives each contending key a turn.
   */
  #dispatchFair(): void {
    if (this.#fairKeys.length === 0) return;
    for (let i = 0; i < this.#fairKeys.length; i++) {
      const key = this.#fairKeys[this.#fairCursor % this.#fairKeys.length];
      this.#fairCursor += 1;
      const queue = this.#queues.get(key);
      if (!queue || queue.length === 0) continue;
      const waiter = queue.shift() as AdmissionWaiter;
      if (queue.length === 0) this.#removeFairKey(key);
      waiter.resolve();
      return;
    }
  }
}

const defaultAdmissionController = new ChatAdmissionController(CHAT_MAX_HEAVY_IN_FLIGHT);

/**
 * Process-wide byte-level admission budget (#10110).
 *
 * Every request — every session, every API key — admits against ONE global
 * ChatAdmissionController, so `CHAT_MAX_HEAVY_IN_FLIGHT` and
 * `CHAT_ADMISSION_MAX_QUEUED_BYTES` are enforced process-wide, exactly as
 * documented in docs/reference/ENVIRONMENT.md. The pre-#10110 design minted a
 * per-session controller per request, multiplying the process bound by up to
 * 64 lanes and letting unauthenticated fake credentials shard capacity.
 *
 * Per-request session identity survives ONLY as a fairness scheduling key:
 * waiters are grouped per key and served round-robin against the shared
 * budget (ChatAdmissionController#dispatchFair), preserving the #9654
 * guarantee that one connection's burst cannot starve others — without any
 * per-key capacity being allocated.
 */

export function resolveSessionId(request: Request): string {
  // Fairness scheduling key ONLY (never a capacity shard): hashed so raw key
  // material never appears in diagnostics. Reuses the internal-bypass auth
  // extraction: bearer token from Authorization, x-api-key (Anthropic-style),
  // or Google API key header.
  const authHeader = request.headers.get("authorization") || "";
  const bearerMatch = /^bearer\s+(\S+)$/i.exec(authHeader.trim());
  if (bearerMatch) {
    return "key_" + createHash("sha256").update(bearerMatch[1]).digest("hex").slice(0, 16);
  }
  const xApiKey = request.headers.get("x-api-key") || "";
  if (xApiKey.trim().length > 0) {
    return "key_" + createHash("sha256").update(xApiKey.trim()).digest("hex").slice(0, 16);
  }
  const xGoogApiKey = request.headers.get("x-goog-api-key") || "";
  if (xGoogApiKey.trim().length > 0) {
    return "key_" + createHash("sha256").update(xGoogApiKey.trim()).digest("hex").slice(0, 16);
  }
  return "anonymous";
}

export class PerConnectionAdmissionController {
  readonly #controller: ChatAdmissionController;

  constructor(
    readonly maxHeavyInFlight = 1,
    // Deprecated pre-#10110 lane-eviction knobs: accepted for API
    // compatibility and ignored — there are no per-session lanes to evict.
    _opts?: { maxSessions?: number; sessionTtlMs?: number }
  ) {
    this.#controller = new ChatAdmissionController(maxHeavyInFlight);
  }

  /** Returns the process-global budget — the same instance for every session. */
  getController(_sessionId: string): ChatAdmissionController {
    return this.#controller;
  }

  /**
   * Process-wide aggregate snapshot for observability: global totals plus
   * per-key waiter depths. Keys are opaque scheduler keys, never raw
   * credentials.
   */
  snapshot(): {
    activeHeavy: number;
    queuedBytes: number;
    waiting: number;
    lanes: ReadonlyArray<{ key: string; waiting: number }>;
  } {
    return {
      activeHeavy: this.#controller.activeHeavy,
      queuedBytes: this.#controller.queuedBytes,
      waiting: this.#controller.waitingCount,
      lanes: this.#controller.waitersByKey,
    };
  }

  get activeHeavy(): number {
    return this.#controller.activeHeavy;
  }

  get queuedBytes(): number {
    return this.#controller.queuedBytes;
  }

  get waitingCount(): number {
    return this.#controller.waitingCount;
  }

  /** No per-session state to clean; kept for API compatibility. */
  dispose(): void {
    // Intentionally empty: the process-global controller owns no session state.
  }
}

export const perConnectionAdmissionController = new PerConnectionAdmissionController(
  CHAT_MAX_HEAVY_IN_FLIGHT
);

export type ChatRequestAdmission =
  | { admit: true; request: Request; lease: ChatAdmissionLease | null }
  | { admit: false; response: Response };

export type ChatStructureAdmission =
  { admit: true; lease: ChatAdmissionLease | null } | { admit: false; response: Response };

function rejectionResponse(status: 413 | 503, hardMaxBytes: number): Response {
  const isPayload = status === 413;
  const headers: Record<string, string> = {
    ...CORS_HEADERS,
    "Content-Type": "application/json",
  };
  if (!isPayload) headers["Retry-After"] = "2";
  return new Response(
    JSON.stringify({
      error: {
        message: isPayload
          ? `Request body too large for chat completions (max ${Math.floor(
              hardMaxBytes / (1024 * 1024)
            )} MB).`
          : "Chat admission capacity is temporarily unavailable. Retry shortly.",
        type: isPayload ? "payload_too_large" : "server_error",
        code: isPayload ? "PAYLOAD_TOO_LARGE" : "chat_admission_busy",
      },
    }),
    { status, headers }
  );
}

function structuralRejectionResponse(status: 413 | 503, maxMessages: number): Response {
  const historyLimit = status === 413;
  const headers: Record<string, string> = {
    ...CORS_HEADERS,
    "Content-Type": "application/json",
  };
  if (!historyLimit) headers["Retry-After"] = "1";

  return new Response(
    JSON.stringify({
      error: {
        message: historyLimit
          ? `Chat history exceeds the ${maxMessages}-message limit; compact the conversation and retry.`
          : "Structurally heavy chat request capacity is busy; retry shortly.",
        type: historyLimit ? "payload_too_large" : "server_error",
        code: historyLimit ? "chat_history_too_large" : "chat_admission_busy",
        reason: historyLimit ? "message_limit" : "structure_limit",
      },
    }),
    { status, headers }
  );
}

type TokenEstimate = { tokens: number; exhausted: boolean };

function conservativeStringTokens(value: string, remaining: number): number {
  let tokens = 0;
  for (const character of value) {
    tokens += character.codePointAt(0)! < 0x80 ? 0.25 : 1;
    if (tokens >= remaining) return remaining;
  }
  return tokens;
}

function estimateStructureTokens(value: unknown, limit: number): TokenEstimate {
  let tokens = 0;
  let visited = 0;
  const maxNodes = 10_000;
  const stack: Array<{ value: unknown; depth: number }> = [{ value, depth: 0 }];
  while (stack.length > 0 && tokens < limit && visited < maxNodes) {
    const current = stack.pop();
    if (!current) break;
    visited += 1;
    if (typeof current.value === "string") {
      tokens += conservativeStringTokens(current.value, limit - tokens);
      continue;
    }
    if (!current.value || typeof current.value !== "object") continue;
    if (current.depth >= 12) return { tokens, exhausted: true };

    const remainingNodes = maxNodes - visited - stack.length;
    if (Array.isArray(current.value)) {
      if (current.value.length > remainingNodes) return { tokens, exhausted: true };
      for (const child of current.value) stack.push({ value: child, depth: current.depth + 1 });
      continue;
    }

    let children = 0;
    for (const key in current.value) {
      if (!Object.hasOwn(current.value, key)) continue;
      children += 1;
      if (children > remainingNodes) return { tokens, exhausted: true };
      tokens += conservativeStringTokens(key, limit - tokens);
      if (tokens >= limit) return { tokens: limit, exhausted: false };
      stack.push({
        value: (current.value as Record<string, unknown>)[key],
        depth: current.depth + 1,
      });
    }
  }
  return { tokens, exhausted: stack.length > 0 && tokens < limit };
}

export async function admitChatStructure(
  body: unknown,
  lease: ChatAdmissionLease | null,
  options: {
    controller?: ChatAdmissionController;
    sessionId?: string;
    maxMessages?: number;
    heavyMessages?: number;
    heavyTools?: number;
    heavyTokens?: number;
    queueMs?: number;
    signal?: AbortSignal;
  } = {}
): Promise<ChatStructureAdmission> {
  if (!body || typeof body !== "object" || Array.isArray(body)) return { admit: true, lease };

  const record = body as Record<string, unknown>;
  const messages = Array.isArray(record.messages) ? record.messages : [];
  const tools = Array.isArray(record.tools) ? record.tools : [];
  const maxMessages = options.maxMessages ?? CHAT_HARD_MAX_MESSAGES;
  // Opt-in only: `0`/unset means no history cap, so oversized conversations reach the
  // compression pipeline and the bounded heavyweight path instead of a terminal 413.
  if (maxMessages > 0 && messages.length > maxMessages) {
    return { admit: false, response: structuralRejectionResponse(413, maxMessages) };
  }

  const heavyMessages = options.heavyMessages ?? CHAT_HEAVY_MESSAGE_COUNT;
  const heavyTools = options.heavyTools ?? CHAT_HEAVY_TOOL_COUNT;
  const heavyTokens = options.heavyTokens ?? CHAT_HEAVY_ESTIMATED_TOKENS;
  const countHeavy = messages.length >= heavyMessages || tools.length >= heavyTools;
  if (!countHeavy && lease) return { admit: true, lease };

  const messageEstimate = estimateStructureTokens(messages, heavyTokens);
  const toolEstimate = messageEstimate.exhausted
    ? { tokens: 0, exhausted: true }
    : estimateStructureTokens(tools, heavyTokens - messageEstimate.tokens);
  const estimatedTokens = Math.min(heavyTokens, messageEstimate.tokens + toolEstimate.tokens);
  const heavy =
    countHeavy ||
    messageEstimate.exhausted ||
    toolEstimate.exhausted ||
    estimatedTokens >= heavyTokens;
  if (!heavy || lease) return { admit: true, lease };

  const controller =
    options.controller ??
    (options.sessionId
      ? perConnectionAdmissionController.getController(options.sessionId)
      : defaultAdmissionController);
  // Structural-only waits happen on byte-light bodies (a byte-heavy body already
  // holds the byte-stage lease), so the conservative 256KB weight bounds the
  // parsed JSON the waiter keeps resident while parked.
  const acquired = await controller.acquireHeavyWithin(
    options.queueMs ?? 0,
    options.signal,
    CHAT_LARGE_BODY_BYTES,
    options.sessionId
  );
  return acquired
    ? { admit: true, lease: acquired }
    : { admit: false, response: structuralRejectionResponse(503, maxMessages) };
}

function parseContentLength(header: string | null): number | null {
  if (header === null || !/^(0|[1-9]\d*)$/.test(header.trim())) return null;
  const parsed = Number(header);
  return Number.isSafeInteger(parsed) ? parsed : null;
}

function rebuildRequest(request: Request, body: Uint8Array): Request {
  const headers = new Headers(request.headers);
  // The inbound value may be absent or dishonest. Let the runtime derive the correct value.
  headers.delete("content-length");
  return new Request(request.url, {
    method: request.method,
    headers,
    body,
    signal: request.signal,
    duplex: "half",
  } as RequestInit & { duplex: "half" });
}

/**
 * Internal self-loop bypass marker for the vision-bridge describe call (and any
 * other trusted in-process sub-request). An external client cannot spoof it:
 * it is honored ONLY when combined with a trusted self-loop credential — the
 * local-mode `sk_omniroute` sentinel or the operator-configured env key
 * (`OMNIROUTE_API_KEY` / `ROUTER_API_KEY`, #1350) so REQUIRE_API_KEY=true
 * deployments can run the describe sub-request.
 */
export const ADMISSION_BYPASS_HEADER = "x-omniroute-admission-bypass";
const ADMISSION_BYPASS_VALUE = "internal";
const SELF_LOOP_KEY = "sk_omniroute";

/**
 * Resolve the bearer credential used by trusted in-process self-loop
 * sub-requests (the vision-bridge describe call).
 *
 * Local mode uses the `sk_omniroute` sentinel. Deployments that force API key
 * auth (`REQUIRE_API_KEY=true`) reject that sentinel with 401, so they must use
 * a real key — the persistent env-var key (#1350, `OMNIROUTE_API_KEY` /
 * `ROUTER_API_KEY`) is the natural choice because it always validates and
 * survives restarts. Falls back to the sentinel when no env key is configured
 * so local-mode behavior is unchanged.
 */
export function resolveSelfLoopBearer(): string {
  return (
    process.env.OMNIROUTE_API_KEY?.trim() || process.env.ROUTER_API_KEY?.trim() || SELF_LOOP_KEY
  );
}

/**
 * Sentinel lease returned by the admission byte stage for an internal self-loop
 * sub-request (the vision-bridge describe call). The parent request already holds
 * the single heavyweight lease, so the describe call must never reserve again —
 * but a NON-NULL lease is still required so the route's later structural stage
 * (`admitChatStructure`) treats the body as covered. With `lease: null` the
 * structural stage classifies the base64-heavy describe body as "heavy" and tries
 * to acquire the busy capacity, returning 503 `chat_admission_busy` anyway — the
 * gap that kept the Zoo Code / api-key describe call failing even after the byte
 * stage was bypassed. Release is a no-op; capacity was never reserved.
 */
const NULL_LEASE: ChatAdmissionLease = {
  get released() {
    return true;
  },
  release() {
    // No-op: the sentinel never reserved heavyweight capacity.
  },
};

/**
 * True when the request is a trusted in-process self-loop sub-request that must
 * not consume a heavyweight admission lease. The describe call runs WHILE the
 * parent request already holds the single heavyweight lease (`CHAT_MAX_HEAVY_IN_FLIGHT=1`),
 * so without this bypass it is rejected with 503 `chat_admission_busy` and the
 * image is never described (#vision-bridge self-loop).
 */
function isInternalAdmissionBypass(request: Request): boolean {
  const bypass =
    request.headers.get(ADMISSION_BYPASS_HEADER)?.trim().toLowerCase() === ADMISSION_BYPASS_VALUE;
  if (!bypass) return false;

  // Credential gate: the bypass only applies to trusted self-loop credentials —
  // the local `sk_omniroute` sentinel OR the operator-configured env key
  // (`OMNIROUTE_API_KEY` / `ROUTER_API_KEY`, #1350) so REQUIRE_API_KEY=true
  // deployments can still run the vision-bridge describe sub-request. The env
  // key is a secret like any other API key, so honoring it here does not widen
  // the attack surface: a third-party that holds it can already call every API.
  const auth = request.headers.get("authorization") || "";
  const match = /^bearer\s+(\S+)$/i.exec(auth.trim());
  if (!match) return false;
  return match[1].trim().toLowerCase() === resolveSelfLoopBearer().toLowerCase();
}

/**
 * Reserve heavyweight capacity and ingest the body with a hard byte bound before JSON
 * parsing. Missing/invalid Content-Length is sniffed only up to the heavyweight threshold;
 * a lease is acquired atomically before retaining bytes at or beyond that threshold.
 *
 * Internal self-loop sub-requests (vision-bridge describe calls) bypass the lease
 * reservation — they run inside a parent request that already holds the lease.
 */
export async function admitChatRequest(
  request: Request,
  options: {
    controller?: ChatAdmissionController;
    sessionId?: string;
    largeBodyBytes?: number;
    hardMaxBytes?: number;
    queueMs?: number;
  } = {}
): Promise<ChatRequestAdmission> {
  const sessionId = options.sessionId ?? resolveSessionId(request);
  const controller =
    options.controller ?? perConnectionAdmissionController.getController(sessionId);
  const largeBodyBytes = options.largeBodyBytes ?? CHAT_LARGE_BODY_BYTES;
  const hardMaxBytes = options.hardMaxBytes ?? CHAT_HARD_MAX_BODY_BYTES;
  const queueMs = options.queueMs ?? 0;
  const internalBypass = isInternalAdmissionBypass(request);
  const contentLength = parseContentLength(request.headers.get("content-length"));

  // Internal self-loop: skip the heavyweight reservation entirely (the parent
  // request already holds the single lease) but still enforce the hard byte bound.
  if (internalBypass) {
    const contentLengthHeader = request.headers.get("content-length");
    if (contentLength !== null && contentLength > hardMaxBytes) {
      return { admit: false, response: rejectionResponse(413, hardMaxBytes) };
    }
    // Sniff bytes for the hard bound without reserving a lease.
    const reader = request.body?.getReader();
    if (!reader) return { admit: true, request, lease: NULL_LEASE };
    const chunks: Uint8Array[] = [];
    let totalBytes = 0;
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        totalBytes += value.byteLength;
        if (totalBytes > hardMaxBytes) {
          await reader.cancel("chat request exceeds hard body limit").catch(() => undefined);
          return { admit: false, response: rejectionResponse(413, hardMaxBytes) };
        }
        chunks.push(value);
      }
    } catch (error) {
      throw error;
    } finally {
      reader.releaseLock();
    }
    const body = new Uint8Array(totalBytes);
    let offset = 0;
    for (const chunk of chunks) {
      body.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return { admit: true, request: rebuildRequest(request, body), lease: NULL_LEASE };
  }

  if (contentLength !== null && contentLength > hardMaxBytes) {
    return { admit: false, response: rejectionResponse(413, hardMaxBytes) };
  }

  let lease: ChatAdmissionLease | null = null;
  const reserve = async (bytes = 0): Promise<boolean> => {
    if (lease) return true;
    lease = await controller.acquireHeavyWithin(queueMs, request.signal, bytes, sessionId);
    return lease !== null;
  };

  // A known-large declaration can reserve before ingestion. Unknown lengths are boundedly
  // sniffed below; this avoids consuming scarce heavyweight capacity for small chunked bodies.
  if (
    contentLength !== null &&
    contentLength >= largeBodyBytes &&
    !(await reserve(Math.min(contentLength, hardMaxBytes)))
  ) {
    return { admit: false, response: rejectionResponse(503, hardMaxBytes) };
  }

  const reader = request.body?.getReader();
  if (!reader) return { admit: true, request, lease };

  const chunks: Uint8Array[] = [];
  let totalBytes = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      totalBytes += value.byteLength;
      if (totalBytes > hardMaxBytes) {
        await reader.cancel("chat request exceeds hard body limit").catch(() => undefined);
        lease?.release();
        return { admit: false, response: rejectionResponse(413, hardMaxBytes) };
      }
      if (totalBytes >= largeBodyBytes && !(await reserve(totalBytes))) {
        await reader.cancel("chat admission capacity unavailable").catch(() => undefined);
        return { admit: false, response: rejectionResponse(503, hardMaxBytes) };
      }
      chunks.push(value);
    }
  } catch (error) {
    lease?.release();
    throw error;
  } finally {
    reader.releaseLock();
  }

  const body = new Uint8Array(totalBytes);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return { admit: true, request: rebuildRequest(request, body), lease };
}

/** Release a lease if a handler rejects; otherwise bind it to the returned response lifecycle. */
export async function releaseChatAdmissionAfterHandler(
  responsePromise: Promise<Response>,
  lease: ChatAdmissionLease | null
): Promise<Response> {
  try {
    return releaseChatAdmissionWhenDone(await responsePromise, lease);
  } catch (error) {
    lease?.release();
    throw error;
  }
}

/** Hold a heavyweight lease through an SSE response without buffering the response body. */
export function releaseChatAdmissionWhenDone(
  response: Response,
  lease: ChatAdmissionLease | null
): Response {
  if (!lease) return response;
  const isStreaming = response.headers.get("content-type")?.includes("text/event-stream");
  if (!isStreaming || !response.body) {
    lease.release();
    return response;
  }

  const reader = response.body.getReader();
  const body = new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        const { done, value } = await reader.read();
        if (done) {
          lease.release();
          controller.close();
        } else {
          controller.enqueue(value);
        }
      } catch (error) {
        lease.release();
        controller.error(error);
      }
    },
    async cancel(reason) {
      lease.release();
      await reader.cancel(reason).catch(() => undefined);
    },
  });

  return new Response(body, {
    status: response.status,
    statusText: response.statusText,
    headers: response.headers,
  });
}
