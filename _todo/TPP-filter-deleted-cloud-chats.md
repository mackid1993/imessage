# TPP: Filter Deleted/Empty Cloud Chats + Bidirectional Delete Sync

## Status
- **Phase**: Review & Refinement
- **Last updated**: 2026-02-16 (Session 31)
- **Branch**: `refactor` on `mackid1993/imessage-test`
- **HEAD**: `cfbf779`
- **Base**: `origin/backfill` (`83d86ec`)

## Goal
1. **Stop spam portals**: Deleted chats stay deleted — no recreation from APNs echoes or CloudKit re-sync
2. **Bidirectional delete**: Beeper delete → iCloud delete, iCloud delete → Beeper delete
3. **Respect iDevice recycle bin**: Don't nuke Apple's "Recently Deleted" — just filter tombstoned chats

## THE FIX (Session 29)

### Root cause: tombstone ID mismatch
CloudKit tombstones (deleted chats) only carry the `record_name` (e.g., `p:ABC123`).
The FFI layer set `cloud_chat_id = record_name` for tombstones. But `ingestCloudChats`
passed this to `deleteChatBatch` which did:
```sql
DELETE FROM cloud_chat WHERE cloud_chat_id IN (...)
```
The `cloud_chat` table stores the actual chat identifier (`iMessage;-;user@example.com`)
as `cloud_chat_id`, NOT the record_name. **The SQL matched nothing** — tombstoned chats
stayed in local DB → `createPortalsFromCloudSync` resurrected them as zombies.

### Solution: tombstone-based filtering (no DB table needed)
1. **`ingestCloudChats`**: look up portal_ids by `record_name`, delete by `record_name`,
   queue bridge portal deletion, mark in `recentlyDeletedPortals`
2. **`cloudSyncDone` gate**: tombstones always processed BEFORE APNs can create portals
3. **`recentlyDeletedPortals`**: blocks APNs echoes for tombstoned chats
4. **On restart**: CloudKit sync re-delivers tombstones → repopulates in-memory set
5. **Removed `deleted_portal` table**: redundant — tombstones ARE the authoritative signal

## Verified in Production
- 36,044 CloudKit records, 806 tombstones, 29 chats
- No zombie portals after restart
- Tombstones correctly filtered

## Tribal Knowledge

### Failed approaches (DON'T repeat these)
- **`deleted_portal` DB table** — Portal ID mismatch between delete event and CloudKit sync made lookups unreliable.
- **Nuking `recoverableMessageDeleteZone`** — Wipes the user's iPhone "Recently Deleted" for ALL chats.
- **`deleted_chats.json`** — Early attempt, replaced by DB.
- **`isStaleForDeletedChat` timestamp check** — APNs `"e"` field is DELIVERY timestamp, not send time.
- **Blanket `is_from_me` block** — Blocks legitimate new outgoing messages from Messages.app.
- **Deleting tombstoned chats by `cloud_chat_id`** — For tombstones, FFI sets cloud_chat_id = record_name. Must delete by `record_name` column.
- **`purgeCloudMessagesByPortalID` in tombstone handler** — Hard-deletes cloud_message rows, destroying UUID echo detection data. Use `deleteLocalChatByPortalID` (soft-delete, `deleted=TRUE`) instead.
- **Setting `cloudSyncDone` on sync failure** — Opens APNs gate with no echo detection data populated. Gate must stay closed until sync succeeds.

### Key architectural insights
- **NEVER modify vendored `rustpush/`** — all changes go in `pkg/rustpushgo/src/lib.rs` or Go code
- **CloudKit tombstones are the authoritative "deleted" signal** — not a local DB table
- **`cloudSyncDone` gate MUST stay closed until sync succeeds** — ensures tombstones populate `recentlyDeletedPortals` and `cloud_message` has UUID data BEFORE APNs can create portals. Messages for existing portals still deliver while gate is closed.
- **Tombstones only carry `record_name`** — no chat_identifier, no participants, no group_id. Must look up portal_id via cloud_chat table before deleting.
- **Soft-delete cloud_message, NEVER hard-delete** — `deleteLocalChatByPortalID` marks `deleted=TRUE` preserving UUIDs. `hasMessageUUID` ignores the deleted flag, so soft-deleted UUIDs still block echoes. `purgeCloudMessagesByPortalID` exists but should NOT be called during deletion flows.
- **Always record `pending_cloud_deletion`** — even with empty chatIdentifier. `loadPendingDeletionPortalIDs` uses portal_id to populate `recentlyDeletedPortals` on restart.
- **Beeper deletes run CloudKit scan synchronously** — `deleteFromApple(..., true)`. Must finish before returning so a restart can't leave CloudKit records alive.
- `PurgeRecoverableZones` FFI exists but should NOT be called — respects user's iPhone recycle bin

### Key files
- `pkg/connector/client.go` — handleMessage (UUID echo detection), handleChatDelete, HandleMatrixDeleteChat, deleteFromApple, runFullCloudKitDeletion, FetchMessages
- `pkg/connector/cloud_backfill_store.go` — hasMessageUUID, persistMessageUUID, deleteLocalChatByPortalID, lookupPortalIDsByRecordNames, deleteChatsByRecordNames, pending_cloud_deletion
- `pkg/connector/sync_controller.go` — ingestCloudChats (tombstone handling), retryPendingCloudDeletions, createPortalsFromCloudSync, loadPendingDeletionPortalIDs, pruneRecentlyDeletedPortals

## Session 30: Echo suppression overhaul

### Problem
Deleted portals kept resurrecting from APNs echoes after bridge restart.
Three compounding bugs:

1. **Post-restart gap**: `recentlyDeletedPortals` in-memory only → empty after restart.
2. **Send-time UUID gap**: `persistMessageUUID` not called at send time.
3. **Fresh backfill gap**: echo check gated on `recentlyDeletedPortals` → never fired on fresh DB.

### Fixes
1. `loadPendingDeletionPortalIDs` populates `recentlyDeletedPortals` on startup.
2. `HandleMatrixMessage` + `handleMatrixFile` call `persistMessageUUID` at send time.
3. `handleMessage` checks `hasMessageUUID` + `GetExistingPortalByKey` whenever `createPortal=true` — independent of `recentlyDeletedPortals`.
4. `skipPortals` broadened to ALL `recentlyDeletedPortals` entries, not just tombstones.

### Design principle
All echo suppression is UUID-based: known UUIDs blocked, fresh UUIDs allowed.
No unconditional drops. `isTombstone` retained for logging only.

## Session 31: Deletion flow deep dive & bug sweep

### Problem
Deep dive audit of the entire deletion flow found 9 bugs (2 high, 4 medium,
3 low/trivial) across all three delete paths.

### Bugs fixed (`cfbf779`)

| # | Bug | Severity | Fix |
|---|-----|----------|-----|
| 1 | `cloudSyncDone` set on sync failure opens resurrection window | HIGH | Removed `setCloudSyncDone()` from error branch — gate stays closed until sync succeeds |
| 2 | Tombstone `purgeCloudMessagesByPortalID` destroys echo detection data | HIGH | Replaced with `deleteLocalChatByPortalID` (soft-delete preserves UUIDs) |
| 3 | `handleChatDelete` skips `pending_cloud_deletion` when chatIdentifier empty | MEDIUM | Always record — portal_id alone enough for restart protection |
| 4 | `HandleMatrixDeleteChat` passes `synchronous: false` | MEDIUM | Changed to `true` — Beeper deletes finish CloudKit cleanup before returning |
| 5 | `recentlyDeletedPortals` unbounded memory growth | LOW | Added `pruneRecentlyDeletedPortals()` with 24h TTL after bootstrap sync |
| 6 | Full-scan delete functions missing panic recovery | MEDIUM | Switched to `safeCloudSyncChats`/`safeCloudSyncMessages` wrappers |
| 7 | Post-sync soft-delete no-op for tombstoned portals | MEDIUM | Fixed by bug 2 — tombstones now soft-delete instead of hard-delete |
| 8 | `processedMsgUUIDs` dead code / memory leak | LOW | Removed field, mutex, and all writes (written but never read) |
| 9 | Duplicate comment in tombstone handler | TRIVIAL | Cleaned up in bug 2 rewrite |

### Documentation
- `docs/deletion-echo-suppression.md` — comprehensive 4-layer architecture doc
- `docs/cloudkit-deletion-resurrection-report.md` — deep dive analysis (pre-fix snapshot)

## Remaining Work
1. **PR from `refactor` → `master`**
2. **Move TPP to `_done/`**
