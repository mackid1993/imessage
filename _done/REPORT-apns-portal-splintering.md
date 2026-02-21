# Report: APNs Portal Splintering Fix

**Date**: 2026-02-20
**Commits**: `c046eef` through `2eaf224` (6 commits)
**Files changed**: `pkg/connector/client.go`, `pkg/connector/sync_controller.go`, `pkg/connector/cloud_backfill_store.go`, `pkg/rustpushgo/src/lib.rs`

---

## Problem

Group chat messages delivered via APNs were creating duplicate portals in Matrix.
A user would see "iMessage Group Chat" appearing twice — one correct portal (from
CloudKit sync) and one splintered portal (from APNs).

**Root cause**: APNs uses a `sender_guid` UUID to identify conversations. CloudKit
uses a separate `gid` (group_id) UUID. These are different UUIDs for the same group.
The APNs path (`makePortalKey`) created portals as `gid:<sender_guid>`, while
CloudKit sync created them as `gid:<cloudkit_gid>`. Two different portal IDs for
the same conversation = duplicate rooms.

**Secondary cause**: Apple sets `cv_name` (group name) to `Some("")` on DM APNs
payloads. The `makePortalKey` function treated any non-nil `groupName` as a group,
so DMs with empty names were misclassified as groups and got `gid:` portal IDs
instead of `tel:` portal IDs.

---

## Solution

### Architectural change: CloudKit is the sole creator of group portals

The APNs path **never** creates `gid:` portals. Only CloudKit sync creates them,
because CloudKit has the authoritative chat records with correct group_id and
participant data.

**Flow**:
1. APNs message arrives → Rust writes it to CloudKit (`save_messages`)
2. Go `handleMessage` receives the message
3. If the message targets an **existing** portal → deliver immediately (no change)
4. If the message would create a **new** `gid:` portal → signal the background
   CloudKit sync loop and return. The message is safe in CloudKit.
5. Background sync imports the message with the correct portal_id from cloud_chat
   records, creates the portal, and delivers via backfill.
6. Subsequent APNs messages with the same sender_guid resolve instantly via
   `gidAliases` cache or participant matching.

### Pre-flush CloudKit sync (backfill window)

During bootstrap, APNs messages are buffered in `pendingPortalMsgs`. Before
replaying them, `flushPendingPortalMsgs` now runs a CloudKit sync pass to import
any messages the Rust side wrote to CloudKit during the backfill window. Messages
already in `cloud_message` (with correct portal assignment) are skipped in the
replay via `hasMessageUUID` check.

### Background sync with on-demand trigger

`runPeriodicCloudSync` now listens on both a 5-minute ticker AND a
`cloudSyncTrigger` channel. When `handleMessage` defers a group message, it sends
on the trigger channel (non-blocking). The sync loop picks it up immediately,
imports the message from CloudKit, and creates the portal.

### Other fixes

- **`makePortalKey`**: Check `*groupName != ""` instead of `groupName != nil`.
  Added cloud_chat fallback for groups with empty cv_name and ≤2 external
  participants.
- **`resolveConversationID`**: Fixed ordering. Was: getChatPortalID → DM sender →
  UUID→gid. The DM-before-UUID order routed group messages to DM portals. Now:
  getChatPortalID → structured DM format → UUID→gid → DM sender (last resort).
- **Status 200 PCS acks**: Downgraded from WARN to DEBUG — these are delivery
  acknowledgments, not errors.
- **`getMessagePortalID`**: New helper to look up portal_id for a message UUID
  in cloud_message.

---

## Trade-offs

| | Before | After |
|---|---|---|
| Group portal creation | APNs + CloudKit (both create) | CloudKit only |
| New group latency | Instant (wrong portal) | Seconds (triggered sync) to 5 min (periodic poll) |
| Duplicate portals | Yes (sender_guid ≠ gid) | No |
| Message loss risk | None | Negligible (only if CloudKit write fails AND bridge crashes) |

The latency trade-off only affects the **first message in a genuinely new group**
that has no portal yet. After bootstrap, all groups have portals and APNs delivers
instantly. For existing groups with mismatched sender_guids, the first message
triggers a sync (~500ms), caches the alias, and all subsequent messages resolve
instantly.

---

## Verification

1. `make build` compiles clean
2. After deployment, check logs for:
   - `"Deferring new group portal to background CloudKit sync"` — APNs path
     correctly deferring instead of creating
   - `"CloudKit sync starting" source=triggered` — on-demand sync firing
   - `"Periodic CloudKit sync starting"` — every 5 minutes
   - `"Pre-flush CloudKit sync"` — at flush time during bootstrap
   - `"Flush complete: some messages already imported via CloudKit"` — dedup working
3. No duplicate group chat rooms in Matrix
4. All messages delivered (check both APNs-only and CloudKit-synced messages)
5. Send a message from iPhone during bridge bootstrap — should arrive at the
   correct portal, not a splintered "Group Chat"
