# TPP: Robust Backfill Implementation

## Status
- **Phase**: Complete — all phases done
- **Last updated**: 2026-02-20 (Session 6)
- **Branch**: `master` on `mackid1993/imessage-test`
- **Next step**: Move to `_done/`

---

## Tribal Knowledge (READ FIRST)

- **NEVER modify `rustpush/` (vendored)**. All Rust changes go in `pkg/rustpushgo/src/lib.rs`.
- `runCloudSyncController` intentionally has **NO** `defer recover()` — a panic must crash the process. See TPP-backfill-reply-and-panics.md.
- `purgeCloudMessagesByPortalID` **hard-deletes** rows (tombstone path). `deleteLocalChatByPortalID` **soft-deletes** (Beeper/iPhone delete path). This difference is intentional.
- `deleted_chats.json` was deliberately removed. Don't bring it back. The replacement is UUID-based echo detection via `hasMessageUUID`.
- **`pending_cloud_deletion` is a STALE DESIGN.** The docs describe it but we don't delete from CloudKit anymore, so it should never be implemented. Don't build it.
- Failed approaches (don't repeat): deleted_portal DB table, timestamp-based echo detection, blanket is_from_me block, nuking recoverableMessageDeleteZone, `pending_cloud_deletion`. See `docs/deletion-echo-suppression.md`.
- `RECONNECT_WINDOW_MS` is already **30 seconds** (not 10). Docs that say 10s are stale.
- The `isStaleForDeletedChat` compile error and `loginLog` unused variable documented in the deletion-resurrection report have **already been fixed**. The `skipPortals` check in `createPortalsFromCloudSync` replaced `isStaleForDeletedChat`.
- **bridgev2 `doForwardBackfill` calls `FetchMessages` exactly once** — it does NOT loop on `HasMore: true` for forward backfill. Only backward backfill (`DoBackwardsBackfill`) paginates. Do not return `HasMore: true` with `Forward: true`.
- **DM portals can have `cloud_message` rows without any `cloud_chat` entry.** DMs are resolved from message data via `resolveConversationID`. Never delete messages solely based on missing `cloud_chat` — it will destroy DM data.
- **Forward backfill with uncached attachments hangs.** Each `compileBatchMessage` call for an uncached attachment triggers a sequential 90s CloudKit download. For large portals this creates multi-hour stalls. Solution: `preUploadChunkAttachments` parallel-downloads uncached attachments per chunk BEFORE the conversion loop.
- **Failed forward chunking approaches** (don't repeat): returning `HasMore: true` on forward (bridgev2 ignores it, only 5k of 27k delivered); skipping uncached attachments (user rejected — missing data).
- **Delayed re-syncs MUST run inline in `runCloudSyncController`**, not in a goroutine. The `defer cancel()` on the cancellable context fires when the function returns — if re-syncs are in a goroutine, the context dies before they run. This caused all 3 delayed re-syncs to fail with "context canceled", silently breaking portal discovery for late-arriving CloudKit data.
- **`forwardBackfillSem` acquire must check `ctx.Done()`**. Without it, portals waiting for a semaphore slot block the portal event loop indefinitely → "Portal event channel is still full" → events dropped.
- **APNS delivers UUIDs as UPPERCASE, CloudKit stores lowercase.** All `cloud_message` GUID comparisons must use `UPPER()` on both sides. The `hasMessageUUID` query was case-sensitive — caused APNS messages to bypass echo detection entirely.
- **UUID echo detection must NOT drop messages for non-deleted portals.** The `isDeletedPortal` check handles truly-deleted portals. For portals that don't exist yet (forward backfill hasn't run), messages should pass through with `CreatePortal: true`; bridgev2 deduplicates by message ID.
- **Never use context timeouts on the forward backfill chunk download path.** The `chunkAttachmentTimeout` (Session 4) cancelled downloads mid-stream, causing forward backfill to complete prematurely. On the APNs buffer flush, `IsStoredMessage` bridge DB checks then silently dropped outgoing echoes at DEBUG level (the messages were already in Matrix from CloudKit, but attachments were incomplete). Per-download 90s timeouts (bounded by construction) are sufficient; wall-clock deadlines on the chunk are harmful.
- **`handleMessage` drop points must log at INFO level.** The `wasUnsent` and `IsStoredMessage` checks previously logged at DEBUG, making silent drops invisible. Upgraded in `e0c966e`.

---

## Goal

Close the remaining gaps in backfill reliability. Cross-referencing the three design documents against the actual codebase revealed that several documented bugs are already fixed. This TPP addresses the real remaining issues: attachment reliability, large-portal performance, and housekeeping.

---

## Weak-Point Analysis

### Source documents
- `docs/cloudkit-backfill-infrastructure.md`
- `docs/cloudkit-deletion-resurrection-report.md`
- `docs/deletion-echo-suppression.md`

### Already fixed (documented bugs that no longer exist in code)

| Doc Bug | Status |
|---------|--------|
| `isStaleForDeletedChat` compile error | **Fixed.** Replaced with `skipPortals` check. |
| `loginLog` unused variable | **Fixed.** No longer present. |
| `cloudSyncDone` set on sync failure | **Fixed.** Retry loop holds APNs gate closed. |
| `IsStoredMessage` 10s window | **Fixed.** Already 30s. |
| `pending_cloud_deletion` not implemented | **Stale design.** We don't delete from CloudKit anymore. Not needed. |

### Still open

**W1. Failed attachments silently dropped, permanently lost after `fwd_backfill_done=1`** (Medium)

When `safeCloudDownloadAttachment` times out or errors, the attachment is silently skipped. Once `fwd_backfill_done=1` is set (via `CompleteCallback`), `preUploadCloudAttachments` skips the entire portal on restart — including failed attachments. They're permanently lost from the forward backfill path.

**W2. Large-portal first-backfill stall (27k+ messages)** (Medium)

Forward `FetchMessages` returns all messages in one batch (`HasMore: false`). `compileBatchMessage × N` + `BatchSend` for tens of thousands of messages takes 2+ minutes, blocking the portal event loop. The "event handling is taking long" warning fires every 30s. Forward chunking (returning `HasMore: true` with 5k batches) would mitigate this.

**W3. Orphaned `cloud_message` rows after unresolved CloudKit tombstones** (Low)

When a tombstone's `record_name` was never stored locally in `cloud_chat`, no soft-delete of messages occurs. These rows don't cause resurrection (portals need a `cloud_chat` entry), but accumulate as dead storage.

**W4. `cloud_attachment_cache` grows unbounded** (Low)

No cleanup on portal deletion. Not cleared by `clearAllData`. Each entry is small but accumulates indefinitely.

**W5. `preUploadCloudAttachments` ignores context cancellation** (Low)

Called with `context.Background()`. All 32 download goroutines run to completion (or 90s timeout) even if `Disconnect()` is called. Worst case: 48 minutes of leaked work.

**W6. `uploaded.Add(1)` counts attempts, not successes** (Trivial)

Counter increments even when download/upload failed. Misleading log output.

**W7. `donePorals` typo** (Trivial)

Variable named `donePorals` (missing 't'). Cosmetic only.

**W8. Leaked goroutines on CloudKit download stall** (Low — accepted risk)

Bounded to 32 concurrent leaks. No fix without Rust-side cancellation. Document and accept.

**W9. `findAndDeleteCloudChatByIdentifier` CloudKit watermark risk** (Low — accepted risk)

May consume server-side cursor. Accept and document.

**W10. Delayed re-syncs killed by premature context cancellation** (Critical — found in Session 3)

Task 4's `defer cancel()` fires when `runCloudSyncController` returns (~1s after launching the delayed re-sync goroutine). All 3 re-syncs (15s/60s/3min) get "context canceled" immediately. Portals whose CloudKit data arrives after bootstrap are never discovered — permanently missing.

**W11. Forward backfill semaphore blocks portal event loop** (Medium — found in Session 3)

`forwardBackfillSem <- struct{}{}` blocks with no context/timeout check. When 3 slots are occupied by slow portals (downloading hundreds of attachments), queued portals block their event loops → "Portal event channel is still full" → incoming events dropped.

**W12. `preUploadChunkAttachments` no overall timeout** (Medium — found in Session 4)

No deadline on the per-chunk attachment pre-download. On slow systems, 32 concurrent downloads each hitting 90s CloudKit timeouts = indefinite blocking. Added 5-minute per-chunk timeout + dynamic safety-net.

**W13. APNS outgoing messages dropped by UUID echo check** (High — found in Session 4)

Two bugs: (1) `hasMessageUUID` case-sensitive comparison missed APNS uppercase UUIDs vs CloudKit lowercase — fixed with `UPPER()`. (2) `handleMessage` dropped known-UUID messages when no portal existed, even for non-deleted portals still being created. Removed over-aggressive drop.

---

## Task Breakdown

### Phase 1: Attachment Reliability (Medium)

- [x] **Task 1**: Track failed attachment downloads. Added `failedAttachmentEntry` struct with retry count, `recordAttachmentFailure` helper, `failedAttachments sync.Map` on `IMClient`. Downloads/uploads record failures with attempt count. `preUploadCloudAttachments` retries failed attachments (even for done portals) up to `maxAttachmentRetries=3`, then abandons permanently corrupted records. Fixed `uploaded.Add(1)` to only count successes. Fixed `donePorals` → `donePortals`. Added `failed` count to completion log.
  - Files: `pkg/connector/client.go`

- [x] **Task 2**: Forward backfill internal chunking. Since bridgev2 `doForwardBackfill` calls `FetchMessages` exactly once (no HasMore loop), implemented internal chunking: `FetchMessages` loops over `listOldestMessages` / `listForwardMessages` in 5,000-row chunks, advancing a cursor. Added `preUploadChunkAttachments` — parallel-downloads uncached attachments (up to 32 concurrent) per chunk BEFORE the sequential conversion loop, preventing 90s CloudKit download stalls.
  - Files: `pkg/connector/client.go`, `pkg/connector/cloud_backfill_store.go`

### Phase 2: Cleanup & Housekeeping (Low)

- [x] **Task 3**: `cloud_attachment_cache` pruning via `pruneOrphanedAttachmentCache`. Uses `json_each` + `json_extract` to find record_names referenced by live messages. Also added `cloud_attachment_cache` to `clearAllData`. Called from new `runPostSyncHousekeeping` after bootstrap.
  - Files: `pkg/connector/cloud_backfill_store.go`, `pkg/connector/sync_controller.go`

- [x] **Task 4**: Cancellable context for `preUploadCloudAttachments`. Replaced `context.Background()` with `context.WithCancel` derived from `stopChan` in `runCloudSyncController`. Added `ctx.Done()` select on semaphore acquire so goroutines waiting for slots exit immediately on shutdown.
  - Files: `pkg/connector/sync_controller.go`, `pkg/connector/client.go`

- [x] **Task 5**: Orphaned `cloud_message` cleanup via `deleteOrphanedMessages`. Deletes rows where `deleted=TRUE` AND `portal_id` has no matching `cloud_chat` entry. **Critical restriction**: DM portals legitimately have messages without `cloud_chat` rows, so only already-soft-deleted rows are cleaned up. Called from `runPostSyncHousekeeping`.
  - Files: `pkg/connector/cloud_backfill_store.go`, `pkg/connector/sync_controller.go`

### Phase 3: Documentation (Low)

- [x] **Task 6**: Update docs. The referenced doc files (`docs/cloudkit-deletion-resurrection-report.md`, `docs/cloudkit-backfill-infrastructure.md`, `docs/deletion-echo-suppression.md`) don't exist — they were research notes that informed the TPP but were never created as standalone files. All corrections (stale bug references, `pending_cloud_deletion` stale design, `RECONNECT_WINDOW_MS` 30s, accepted risks W8/W9) are captured in this TPP's Tribal Knowledge section, which serves as the authoritative reference.

---

## Required Reading

- `docs/cloudkit-backfill-infrastructure.md` — full architecture reference
- `docs/cloudkit-deletion-resurrection-report.md` — bug catalog (partially stale)
- `docs/deletion-echo-suppression.md` — echo suppression layers (parts are stale)
- `_todo/TPP-backfill-reply-and-panics.md` — prior session context (panic hardening)

---

## Implementation Log

### Session 1 (2026-02-20) — Research only, no code changes
- Read all three reference documents + existing TPP (backfill-reply-and-panics)
- Cross-referenced docs against actual codebase (full exploration of sync_controller.go, client.go, cloud_backfill_store.go, lib.rs)
- **Key finding**: 4 of 7 documented bugs already fixed in code but docs are stale
- **Key finding**: `pending_cloud_deletion` is stale design — we don't delete from CloudKit anymore. Not needed.
- Completed weak-point analysis (9 open items after removing fixed/stale)
- Created task breakdown (6 tasks across 3 phases)

### Session 2 (2026-02-20) — Phases 1 + 2 Implementation
- **Task 1 DONE**: Failed attachment retry with `failedAttachments sync.Map` + `maxAttachmentRetries=3` cap. Fixed `uploaded.Add(1)` counter + `donePorals` typo.
- **Task 2 iteration 1**: External chunking with `HasMore: true` — BROKEN. bridgev2 only calls `FetchMessages` once for forward, so only first 5k of 27k messages delivered.
- **Task 2 iteration 2**: Reverted to single-batch — rejected by user, still needs chunking.
- **Task 2 iteration 3 (FINAL)**: Internal chunking loop within `FetchMessages`. Loops over `listOldestMessages`/`listForwardMessages` in 5k-row chunks, accumulates all messages into one response.
- **Task 3 DONE**: `cloud_attachment_cache` pruning using `json_each`/`json_extract`. Added to `clearAllData`. Runs after bootstrap via `runPostSyncHousekeeping`.
- **Task 4 DONE**: Cancellable context from `stopChan`. Semaphore acquire checks `ctx.Done()` for prompt shutdown.
- **Task 5 iteration 1**: `deleteOrphanedMessages` deleted ALL rows without `cloud_chat` — BROKE DM portals (they legitimately have no `cloud_chat`).
- **Task 5 iteration 2 (FINAL)**: Restricted to `deleted=TRUE` rows only. Safe for DMs.

### Session 3 (2026-02-20) — Bugfixes + critical re-sync fix
- **Forward backfill hang fix**: Large portals hung because uncached attachments triggered sequential 90s CloudKit downloads during `compileBatchMessage`. Added `preUploadChunkAttachments` — parallel pre-downloads (up to 32 concurrent) per chunk BEFORE the conversion loop. All downloads become cache hits.
- **W10 FIX (critical)**: Delayed re-syncs (15s/60s/3min) were all broken — `defer cancel()` on the cancellable context (from Task 4) fired when `runCloudSyncController` returned, killing the context before the goroutine ran. Fix: moved re-syncs inline (no goroutine) so they complete before `defer cancel()`. This was the root cause of portals never filling in for users whose CloudKit data arrived after bootstrap.
- **W11 FIX**: Added `ctx.Done()` select on `forwardBackfillSem` acquire in FetchMessages. Prevents "Portal event channel is still full" when all 3 semaphore slots are taken by slow portals.
- **What's next**: Deploy this fix, then Phase 3 (doc updates).

### Session 4 (2026-02-20) — Slow-system hang + APNS outgoing message fix
- **W12 FIX**: `preUploadChunkAttachments` had no overall timeout — on slow systems, attachment downloads could block indefinitely. Added 5-minute per-chunk timeout with graceful degradation (continues with partial cache). Also increased safety-net timeout from fixed 10s to dynamic `30s + 2s×pending` (capped at 5min).
- **W13 FIX**: Outgoing messages (is_from_me) sent from iPhone were missing in Matrix when delivered via APNS. Two bugs:
  1. `hasMessageUUID` used case-sensitive comparison (`guid=$2`) while APNS delivers uppercase UUIDs and CloudKit stores lowercase. Fixed with `UPPER(guid)=UPPER($2)`.
  2. UUID echo detection at `handleMessage` dropped messages with known UUIDs when `createPortal=true` but no portal MXID existed, treating them as "stale echo of deleted chat." This was wrong — non-deleted portals that just haven't been created by forward backfill yet were also caught. Removed the over-aggressive drop; `isDeletedPortal` check already handles truly-deleted portals. bridgev2 deduplicates by message ID.
- **What's next**: Deploy, then Phase 3 (doc updates).

### Session 5 (2026-02-20) — Resumable forward backfill + outgoing APNs diagnostics
- **Resumable forward backfill (plan implemented)**: Re-engineered forward backfill to be fully resumable with no timeout. 7 changes across 3 files:
  1. Removed `chunkAttachmentTimeout` constant and `context.WithTimeout` — replaced with parent ctx and progress logging (every 30s via ticker + every 10 downloads inline)
  2. Added `backfillProgressAt int64` atomic field to `IMClient` — stamped after each download, read by safety-net
  3. Persisted attachment failures to SQLite (`cloud_attachment_failure` table with login_id, record_name, retries, last_error)
  4. Wired persistent failures: `recordAttachmentFailure` persists to SQLite, success deletes from SQLite, startup loads from SQLite
  5. Added bridge DB dedup filter in FetchMessages forward path — filters out messages already in Matrix
  6. Conditional `markForwardBackfillDone` — only marks done if no retryable failures remain
  7. Max-retry abandon — after `maxAttachmentRetries` (3) persistent failures, skips download and sends message text-only
  - Safety-net rewritten: heartbeat-aware stall detector with 3-min timeout that extends while `backfillProgressAt` advances
  - Committed `b368e21`, pushed to master
- **W13 RESOLVED**: Outgoing (is_from_me) messages now appear on fresh pull. Root cause was the `chunkAttachmentTimeout` from Session 4 — the 5-minute context cancellation caused forward backfill to complete prematurely, leaving outgoing messages in a state where the `IsStoredMessage` bridge DB check silently dropped them at DEBUG level. The Session 5 resumable backfill changes (`b368e21`) fixed this by removing the chunk timeout entirely and replacing the fixed safety-net with a heartbeat-aware stall detector.
  - Diagnostic logging (`e0c966e`) also added: upgraded `wasUnsent` and `IsStoredMessage` drop logs from Debug→Info, added content message entry log and no-content warning. These are observational only — no behavioral change.
