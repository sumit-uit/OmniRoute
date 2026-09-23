---
title: "Admission lanes — two lane systems, what gates each, where each reports"
status: active
lastUpdated: 2026-08-09
---

# Admission lanes (#9654) — two lane systems, what gates each, where each reports

OmniRoute has **two** process-local lane systems with different scopes. They are
complementary; operators should know which one they are looking at.

## 1. Byte-level per-connection lanes (`chatBodyAdmission.ts`)

- **Scope:** the buffered-body/heap path for `POST /v1/chat/completions`. Guards
  against heap amplification from large coding-agent bodies (#4380).
- **Gate:** **always on.** **Process-wide budget** (`CHAT_MAX_HEAVY_IN_FLIGHT`, default `1`).
  Per-request session identity (hashed API key or `anonymous`) is used **only as a fairness
  scheduling key** — waiters are grouped per session and served round-robin against the
  **shared** budget, so one connection's burst cannot starve others (#9654, #10110).
  There are **no per-session lanes**; the pre-#10110 design multiplied the process bound
  by up to 64 lanes and is no longer in effect.
- **Tuning:**
  - `OMNIROUTE_CHAT_MAX_HEAVY_IN_FLIGHT` — process-wide heavyweight concurrency (default `1`)
  - `OMNIROUTE_CHAT_ADMISSION_QUEUE_MS` — queue-wait before 503 (default 2000)
  - `OMNIROUTE_CHAT_ADMISSION_MAX_QUEUED_BYTES` — queued-bytes heap valve (default 4 MB)
  - `OMNIROUTE_CHAT_VIRTUAL_TTL_MS` — deprecated no-op since #10110 (accepted for compat)
  - `OMNIROUTE_CHAT_VIRTUAL_MAX_SESSIONS` — deprecated no-op since #10110 (accepted for compat)
- **Reports:** not in `GET /api/monitoring/health` today; observable via
  `PerConnectionAdmissionController.snapshot()` (opaque scheduler keys, activeHeavy, queuedBytes, waiting, lanes).

## 2. Adaptive runtime virtual lanes (`open-sse/services/admission`)

- **Scope:** tenant-key admission for provider dispatch — queue cost, latency-guided
  limit adaptation, lane queueing, and lane metrics.
- **Gate:** **opt-in.** Disabled unless `OMNIROUTE_CHAT_VIRTUAL_LANES=true`. Without it,
  the adaptive controller keeps the shared queue behavior (criterion 1 of #9654 only
  holds once an operator enables lanes).
- **Tuning:** `OMNIROUTE_CHAT_VIRTUAL_LANES` + adaptive config (`maxQueueCount`,
  `maxQueueCost`, `defaultMaxWaitMs`, …).
- **Reports:** `GET /api/monitoring/health` → `adaptiveAdmission` → `laneCount`,
  `laneQueuedCount`, `laneQueuedCost`, `laneTenants` (opaque lane IDs, never raw keys).

## Which one is showing in a dashboard

- `adaptiveAdmission.laneCount` / `laneTenants` → **adaptive virtual lanes** (system 2).
- A health payload with **no** `adaptiveAdmission.lane*` fields usually means
  `OMNIROUTE_CHAT_VIRTUAL_LANES` is unset — the byte-level lanes (system 1) are still
  active, but nothing under `adaptiveAdmission` will report lane data until it is enabled.

## Why both exist

The byte-level lanes bound the memory-heavy parse/compress path; the adaptive lanes
bound dispatch cost per tenant. #9654's criterion 1 ("one session's burst does not 503
another") is enforced by system 1 unconditionally and by system 2 once opt-in is enabled.
