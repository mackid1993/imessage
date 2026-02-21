# TPP: Backfill Panic Hardening + Reply OBJ Fix

## Status
- **Phase**: Review — all tasks implemented; awaiting live validation
- **Last updated**: 2026-02-20 (Session 4)
- **Branch**: `refactor` on `mackid1993/imessage-test`

---

## Tribal Knowledge (READ FIRST)

- **NEVER modify `rustpush/` (vendored)**. All Rust changes go in `pkg/rustpushgo/src/lib.rs`.
- `runtime/debug` is already imported in both `client.go` and `sync_controller.go`.
- `sync_messages_fallback` must be a **free function** placed OUTSIDE `impl Client { }`.
- `PCSKeys` has no `Clone` derive — use refs + `AssertUnwindSafe` in `catch_unwind`.
- `runCloudSyncController` intentionally has **NO** `defer recover()`. A panic there must crash the process so the bridge restarts and re-runs CloudKit sync cleanly. Swallowing the panic would leave `setCloudSyncDone()` uncalled → APNs buffer permanently blocked → all incoming messages dropped. Task 3b in previous sessions added it, then commit `1cd94c0` deliberately removed it. Do not add it back.

### Session 4 Infrastructure Work (2026-02-20) — NOT in original TPP scope

This session made substantial ad-hoc backfill infrastructure changes. All committed and pushed to `mackid1993/refactor`. A full reference document was written at `docs/cloudkit-backfill-infrastructure.md`.

**Commits (all on `refactor`):**
- `abdc45f` — `safeCloudDownloadAttachment`: 90s per-download timeout (goroutine + channel + select). Prevents bridge hang when CloudKit FFI stalls on network or "Ford chunks". Inner goroutine is leaked until Rust unblocks but bounded to ≤32.
- `2447a74` — `preUploadCloudAttachments`: first attempt at skip-on-restart using `GetExistingPortalByKey`. Superseded by next commit.
- `8befecd` — `fwd_backfill_done BOOLEAN` added to `cloud_chat` (migration). Set by `FetchMessages` via `CompleteCallback` (and immediately on empty-result path). `preUploadCloudAttachments` queries `getForwardBackfillDonePortals()` and skips portals in the done-set entirely. Fixes the restart re-upload storm AND the interrupted-backfill edge case (MXID-existence was wrong; this is correct).
- `18d2b19` — `cloud_attachment_cache` table added: persists `record_name → content_json` (full `*event.MessageEventContent` JSON including mxc:// URI). Loaded at startup in `preUploadCloudAttachments` before the pending list is built. Removed the 5-minute overall pre-upload timeout (the 90s per-download cap is sufficient; removing the wall-clock timeout ensures complete cache population so FetchMessages always gets 100% cache hits).

**Net effect:** On a warm restart (all portals backfilled, all attachments cached), `preUploadCloudAttachments` returns after two DB queries with zero CloudKit calls.

**Known remaining issue:** For portals with 27k+ messages, the "event handling is taking long" warning (bridgev2 internal, fires every 30s) still appears on first backfill. This is inherent: `compileBatchMessage × N` + `BatchSend` HTTP for 27k events takes 2+ minutes and is entirely in bridgev2/homeserver code. After `fwd_backfill_done=1` is set, subsequent restarts use anchor/catch-up mode and are fast. The warning is a one-time cost per portal.

---

## Goal

Fix two bugs:
1. Reply messages show an "OBJ" box (U+FFFC) at the start of the body in Matrix.
2. Unprotected panic paths in the backfill pipeline that crash the bridge or silently drop pages.

---

## Task Breakdown

- [x] **Task 1**: Strip U+FFFC in both `cloudRowToBackfillMessages` and `convertMessage`
- [x] **Task 2**: Per-record fallback in `lib.rs` when `sync_messages` panics
- [x] **Task 3**: Add `defer recover()` to unprotected goroutines (NOT `runCloudSyncController` — see Tribal Knowledge)

---

## Task 1: Strip U+FFFC — COMPLETE

Both edits in `pkg/connector/client.go`:
- Backfill path: `body = strings.TrimLeft(body, "\ufffc \n")` after subject block
- APNs real-time path: `text := strings.TrimLeft(ptrStringOr(msg.Text, ""), "\ufffc \n")`

---

## Task 2: Per-record fallback in `pkg/rustpushgo/src/lib.rs` — COMPLETE

Wrapped `sync_messages` call in `tokio::task::spawn` inside `cloud_sync_messages`. On `JoinErr` (panic), calls `sync_messages_fallback` which replicates `sync_records` logic for `messageManateeZone` but handles `None` proto fields gracefully and wraps `from_record_encrypted` in `catch_unwind`. Skips malformed individual records without losing the whole page.

Key implementation details:
- `sync_messages_fallback` is a **free function** outside `impl Client`
- Uses `rustpush::cloudkit::{FetchRecordChangesOperation, CloudKitSession, NO_ASSETS}` and `rustpush::cloud_messages::{CloudMessage, MESSAGES_SERVICE}`
- `PCSKeys` has no `Clone` — refs + `AssertUnwindSafe` required
- On fallback failure: returns empty done page (vs. crashing the sync)

---

## Task 3: `defer recover()` — COMPLETE (with caveat)

Added to:
- **Attachment download goroutine** in `client.go` (~line 2738) ✅
- **Delayed re-sync goroutine** in `sync_controller.go` (~line 394) ✅
- `runCloudSyncController` — added then **intentionally removed** (commit `1cd94c0`). See Tribal Knowledge.

---

## Review Checklist

- [ ] Reply messages show no OBJ box in real-time APNs messages (live test needed)
- [ ] Reply messages show no OBJ box in CloudKit backfill (live test needed)
- [x] Panic recovery logs in zerolog format with stack trace
- [x] `cargo check` in `pkg/rustpushgo/` passes
- [x] `go build ./pkg/connector/...` passes
- [ ] `make build` end-to-end (requires Rust toolchain)
- [x] 90s per-download timeout in `safeCloudDownloadAttachment`
- [x] `fwd_backfill_done` persisted; restart re-upload storm eliminated
- [x] `cloud_attachment_cache` persisted; mxc URIs survive restarts
- [x] `docs/cloudkit-backfill-infrastructure.md` written

---

## Implementation Log

### Session 1 (2026-02-19)
Full investigation. Task 1 implemented (both U+FFFC strips in client.go).

### Session 2 (2026-02-19)
Task 1 confirmed. Discovered `rustpush/` is vendored — never modify. Task 2 redesigned: per-record fallback in `lib.rs`. Task 3 not yet started.

### Session 3 (2026-02-20)
Task 2 implemented (`tokio::task::spawn` + `sync_messages_fallback`). Task 3 implemented (attachment goroutine + delayed resync goroutine; `runCloudSyncController` was added then removed). `cargo check` and `go build` both pass. All tasks complete.

### Session 4 (2026-02-20)
Ad-hoc infrastructure work (see Tribal Knowledge section above). No TPP tasks changed. Reference document written at `docs/cloudkit-backfill-infrastructure.md`.
